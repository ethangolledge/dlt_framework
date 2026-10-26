"""SQL databases (SQL Server, Postgres, ...) through dlt's ``sql_table``."""

from abc import ABC
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Any, ClassVar

import dlt
from dlt.extract.resource import DltResource
from dlt.sources.sql_database import sql_table
from dlt.sources.sql_database.helpers import TableBackend
from sqlalchemy import Table, make_url
from sqlalchemy.sql import Select

from dlt_framework.core.backfill import RowBackfill
from dlt_framework.core.cursor import TimeCursor
from dlt_framework.core.errors import DefinitionError
from dlt_framework.core.formats import AwareDatetime
from dlt_framework.core.resource import Resource
from dlt_framework.core.source import Extraction, Source
from dlt_framework.core.windows import RowWindow, TimeWindow, ValueWindow, Window


class Backend(ABC):
    """How ``sql_table`` reads rows and types them; ``name`` is dlt's name for it."""

    name: ClassVar[TableBackend]

    def requires(self) -> dict[str, str]:
        """Modules it imports when it reads, mapped to the extra that installs them."""
        return {}

    def validate(self, resource: Resource, config: dict[str, Any]) -> None:  # noqa: B027
        """Refuse a resource this backend would read wrongly; ``config`` is its ``sql_table``
        options."""

    def backend_kwargs(self, credentials: Any) -> dict[str, Any]:
        """``sql_table`` ``backend_kwargs`` it needs; a resource's own override these."""
        return {}


@dataclass(frozen=True, slots=True)
class SqlAlchemy(Backend):
    """Python dicts; slowest, fewest dependencies."""

    name: ClassVar[TableBackend] = "sqlalchemy"


@dataclass(frozen=True, slots=True)
class PyArrow(Backend):
    """Exact types, Arrow batches, 20-30x faster than ``SqlAlchemy``: the choice for real tables."""

    name: ClassVar[TableBackend] = "pyarrow"

    def requires(self) -> dict[str, str]:
        return {"pyarrow": "sql", "numpy": "sql"}


@dataclass(frozen=True, slots=True)
class ConnectorX(Backend):
    """Fastest (Rust, no SQLAlchemy), with costs: it ignores ``chunk_size`` (whole result in
    memory) unless ``backend_kwargs={"return_type": "arrow_stream"}``, drops timezones from
    tz-aware timestamps, and reads its own URL scheme, derived here from the credentials unless
    ``backend_kwargs={"conn": ...}`` is given. The ``connectorx`` extra."""

    name: ClassVar[TableBackend] = "connectorx"

    def requires(self) -> dict[str, str]:
        return {"connectorx": "connectorx", "pyarrow": "connectorx"}

    def validate(self, resource: Resource, config: dict[str, Any]) -> None:
        streams = config.get("backend_kwargs", {}).get("return_type") == "arrow_stream"
        if resource.chunk is not None and not streams:
            raise DefinitionError(
                f"{resource.name!r}: connectorx reads the whole result at once, so a chunk "
                "cannot bound it; set backend_kwargs={'return_type': 'arrow_stream'}"
            )
        cursor = resource.cursor
        if isinstance(cursor, TimeCursor) and isinstance(cursor.format, AwareDatetime):
            raise DefinitionError(
                f"{resource.name!r}: connectorx returns tz-aware timestamps without their "
                "timezone, so this cursor would compare wrongly; use PyArrow(), or a naive column "
                "with NaiveDatetime()"
            )

    def backend_kwargs(self, credentials: Any) -> dict[str, Any]:
        """connectorx reads ``mssql://`` where SQLAlchemy reads ``mssql+pymssql://``."""
        url = make_url(str(credentials))
        conn = url.set(drivername=url.get_backend_name(), query={})
        return {"conn": conn.render_as_string(hide_password=False)}


@dataclass(frozen=True, kw_only=True)
class SqlSource(Source[dict[str, Any]]):
    """Each resource maps to ``sql_table`` keyword arguments plus these framework keys:

    - ``table``: the upstream table name when it differs from the resource name.
    - ``order_by``: columns giving a stable row order; required for ``RowBackfill``, where each
      window becomes ``ORDER BY ... OFFSET ... LIMIT ...``.

    ``backend`` (``PyArrow()``, ``ConnectorX()``, ``SqlAlchemy()``) decides how rows are read and
    typed. Credentials resolve from ``sources.<name>.credentials``, e.g.
    ``SOURCES__ERP__CREDENTIALS`` for a source named ``erp``; SQL Server URLs are
    ``mssql+pymssql://`` (the ``mssql`` extra). Cursors become ``WHERE`` clauses; a SQL Server
    ``rowversion`` cursor is a ``ValueCursor`` with ``format=RowVersion()``.
    """

    windows: ClassVar[tuple[type[Window], ...]] = (TimeWindow, ValueWindow, RowWindow)

    backend: Backend

    def requires(self) -> dict[str, str]:
        return self.backend.requires()

    def validate(self, resource: Resource, config: dict[str, Any]) -> None:
        super().validate(resource, config)
        self.backend.validate(resource, config)
        if resource.cursor is not None and resource.cursor.fallbacks:
            raise DefinitionError(
                f"{resource.name!r}: fallback cursors cannot become a WHERE clause; coalesce "
                "upstream (a view or computed column) and track that column instead"
            )
        if isinstance(resource.backfill, RowBackfill):
            if not config.get("order_by"):
                raise DefinitionError(f"{resource.name!r}: RowBackfill needs order_by columns")
            if "query_adapter_callback" in config:
                raise DefinitionError(f"{resource.name!r}: RowBackfill owns query_adapter_callback")

    def resource(
        self, resource: Resource, config: dict[str, Any], extraction: Extraction
    ) -> DltResource:
        window = extraction.window
        options = {key: value for key, value in config.items() if key not in ("table", "order_by")}
        credentials = dlt.secrets[f"sources.{self.name}.credentials"]
        if isinstance(window, RowWindow):
            options["query_adapter_callback"] = self._page(config["order_by"], window)
        if kwargs := {
            **self.backend.backend_kwargs(credentials),
            **options.get("backend_kwargs", {}),
        }:
            options["backend_kwargs"] = kwargs
        return sql_table(
            credentials=credentials,
            table=config.get("table", resource.name),
            incremental=resource.cursor.incremental(window) if resource.cursor else None,
            backend=self.backend.name,
            defer_table_reflect=True,
            **options,
        ).with_name(resource.name)

    @staticmethod
    def _page(order_by: Sequence[str], window: RowWindow) -> Callable[[Select[Any], Table], Any]:
        def page(query: Select[Any], table: Table) -> Select[Any]:
            columns = [table.c[column] for column in order_by]
            return query.order_by(*columns).offset(window.offset).limit(window.size)

        return page

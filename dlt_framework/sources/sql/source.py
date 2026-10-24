"""SQL databases (SQL Server, Postgres, ...) through dlt's ``sql_table``."""

from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Any, ClassVar, Literal

import dlt
from dlt.extract.resource import DltResource
from dlt.sources.sql_database import sql_table
from sqlalchemy import Table, make_url
from sqlalchemy.sql import Select

from dlt_framework.core.backfill import RowBackfill
from dlt_framework.core.cursor import TimeCursor
from dlt_framework.core.errors import DefinitionError
from dlt_framework.core.formats import AwareDatetime
from dlt_framework.core.resource import Resource
from dlt_framework.core.source import Source
from dlt_framework.core.windows import RowWindow, TimeWindow, ValueWindow, Window

Backend = Literal["sqlalchemy", "pyarrow", "connectorx"]
# What each backend imports when it reads, mapped to the extra that installs it.
_BACKEND_MODULES: dict[str, dict[str, str]] = {
    "sqlalchemy": {},
    "pyarrow": {"pyarrow": "sql", "numpy": "sql"},
    "connectorx": {"connectorx": "connectorx", "pyarrow": "connectorx"},
}


@dataclass(frozen=True, kw_only=True)
class SqlSource(Source[dict[str, Any]]):
    """Each resource maps to ``sql_table`` keyword arguments plus these framework keys:

    - ``table``: the upstream table name when it differs from the resource name.
    - ``order_by``: columns giving a stable row order; required for ``RowBackfill``, where each
      window becomes ``ORDER BY ... OFFSET ... LIMIT ...``.

    ``backend`` decides how rows are read and typed:

    - ``pyarrow``: exact types, Arrow batches; the default choice for real tables.
    - ``connectorx``: fastest (Rust, no SQLAlchemy). It ignores ``chunk_size`` (whole result in
      memory), drops timezones from tz-aware timestamps, and reads its own URL scheme, derived
      here from the credentials unless ``backend_kwargs={"conn": ...}`` is given. Needs the
      ``connectorx`` extra.
    - ``sqlalchemy``: Python dicts; slowest, fewest dependencies.

    Credentials resolve from ``sources.<name>.credentials``, e.g. ``SOURCES__ERP__CREDENTIALS``
    for a source named ``erp``. Cursors become ``WHERE`` clauses; a SQL Server ``rowversion``
    cursor is a ``ValueCursor`` with ``format=RowVersion()``.
    """

    windows: ClassVar[tuple[type[Window], ...]] = (TimeWindow, ValueWindow, RowWindow)

    backend: Backend

    def requires(self) -> dict[str, str]:
        return _BACKEND_MODULES[self.backend]

    def validate(self, resource: Resource, config: dict[str, Any]) -> None:
        super().validate(resource, config)
        cursor = resource.cursor
        if (
            self.backend == "connectorx"
            and isinstance(cursor, TimeCursor)
            and isinstance(cursor.format, AwareDatetime)
        ):
            raise DefinitionError(
                f"{resource.name!r}: connectorx returns tz-aware timestamps without their "
                "timezone, so this cursor would compare wrongly; use pyarrow, or a naive column "
                "with NaiveDatetime()"
            )
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
        self, resource: Resource, config: dict[str, Any], window: Window | None
    ) -> DltResource:
        options = {key: value for key, value in config.items() if key not in ("table", "order_by")}
        credentials = dlt.secrets[f"sources.{self.name}.credentials"]
        if isinstance(window, RowWindow):
            options["query_adapter_callback"] = self._page(config["order_by"], window)
        if self.backend == "connectorx":
            options["backend_kwargs"] = {
                "conn": self._connectorx_url(credentials),
                **options.get("backend_kwargs", {}),
            }
        return sql_table(
            credentials=credentials,
            table=config.get("table", resource.name),
            incremental=resource.cursor.incremental(window) if resource.cursor else None,
            backend=self.backend,
            defer_table_reflect=True,
            **options,
        ).with_name(resource.name)

    @staticmethod
    def _connectorx_url(credentials: Any) -> str:
        """connectorx reads ``mssql://`` where SQLAlchemy reads ``mssql+pyodbc://...?driver=``."""
        url = make_url(str(credentials))
        return url.set(drivername=url.get_backend_name(), query={}).render_as_string(
            hide_password=False
        )

    @staticmethod
    def _page(order_by: Sequence[str], window: RowWindow) -> Callable[[Select[Any], Table], Any]:
        def page(query: Select[Any], table: Table) -> Select[Any]:
            columns = [table.c[column] for column in order_by]
            return query.order_by(*columns).offset(window.offset).limit(window.size)

        return page

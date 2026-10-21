"""SQL databases (SQL Server, Postgres, ...) through dlt's ``sql_table``."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Any, ClassVar

import dlt
from dlt.extract.resource import DltResource
from dlt.sources.sql_database import sql_table
from sqlalchemy import Table
from sqlalchemy.sql import Select

from dlt_framework.core.backfill import RowBackfill
from dlt_framework.core.errors import DefinitionError
from dlt_framework.core.resource import Resource
from dlt_framework.core.source import Source
from dlt_framework.core.windows import RowWindow, TimeWindow, ValueWindow, Window


@dataclass(frozen=True, kw_only=True)
class SqlSource(Source[dict[str, Any]]):
    """Each resource maps to ``sql_table`` keyword arguments plus these framework keys:

    - ``table``: the upstream table name when it differs from the resource name.
    - ``order_by``: columns giving a stable row order; required for ``RowBackfill``, where each
      window becomes ``ORDER BY ... OFFSET ... LIMIT ...``.

    Credentials resolve from ``sources.<name>.credentials``, e.g. ``SOURCES__ERP__CREDENTIALS``
    for a source named ``erp``. Cursors become ``WHERE`` clauses.
    """

    windows: ClassVar[tuple[type[Window], ...]] = (TimeWindow, ValueWindow, RowWindow)

    def validate(self, resource: Resource, config: dict[str, Any]) -> None:
        super().validate(resource, config)
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
        if isinstance(window, RowWindow):
            options["query_adapter_callback"] = self._page(config["order_by"], window)
        return sql_table(
            credentials=dlt.secrets[f"sources.{self.name}.credentials"],
            table=config.get("table", resource.name),
            incremental=resource.cursor.incremental(window) if resource.cursor else None,
            defer_table_reflect=True,
            **options,
        ).with_name(resource.name)

    @staticmethod
    def _page(order_by: Sequence[str], window: RowWindow) -> Callable[[Select[Any], Table], Any]:
        def page(query: Select[Any], table: Table) -> Select[Any]:
            columns = [table.c[column] for column in order_by]
            return query.order_by(*columns).offset(window.offset).limit(window.size)

        return page

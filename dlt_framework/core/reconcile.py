"""``pipeline reconcile``: whether the destination holds every record, counted, not assumed.

Backfill coverage says which windows loaded; this counts what the tables hold. Per resource:
its rows, and its distinct primary keys (fewer means duplicates); per declared backfill window,
the rows whose cursor falls in it, bounded exactly as the backfill bounded its extract. With
``--source``, the source's own count for each, where it can give one (``Source.count``): a
window where the two differ has missing or extra rows. Nothing is extracted or loaded.
"""

import uuid
from collections.abc import Iterator, Sequence
from datetime import UTC, datetime
from typing import Any

from dlt_framework.core.monitoring.records import ReconcileRecord, ResourceRecord
from dlt_framework.core.resource import Resource
from dlt_framework.core.run import IncrementalRun
from dlt_framework.core.windows import Window

_PER_QUERY = 200  # windows counted in one statement


def reconcile(run: IncrementalRun, source_counts: bool) -> list[ReconcileRecord]:
    """Counts for every resource of ``run``'s pipeline; ``source_counts`` asks upstream too,
    once the destination's connection is closed."""
    definition = run.definition
    pipeline = run.dlt_pipeline()
    run.settings.retry("sync", pipeline.sync_destination)
    if definition.source.name not in pipeline.schemas:
        return []  # nothing loaded yet
    schema = pipeline.schemas[definition.source.name]
    counted: list[tuple[Resource, Window | None, int, int | None]] = []
    with pipeline.sql_client() as client:
        for resource in run.resources:
            if resource.name not in schema.tables:
                continue  # never loaded
            table = client.make_qualified_table_name(resource.name)
            total = _scalar(client, f"SELECT COUNT(*) FROM {table}")
            counted.append((resource, None, total, _distinct_keys(client, table, resource)))
            if (column := _cursor_column(schema, resource)) is not None:
                windows = _windows(resource)
                rows = _counts(client, table, column, resource, windows)
                counted.extend(
                    (resource, window, n, None) for window, n in zip(windows, rows, strict=True)
                )
    reconcile_id, now = uuid.uuid4().hex, datetime.now(UTC)
    return [
        ReconcileRecord(
            reconcile_id=reconcile_id,
            resource_id=ResourceRecord.key(definition.name, resource.name),
            at=now,
            window_kind=window.kind if window else "all",
            window_start=window.span()[0] if window else None,
            window_end=window.span()[1] if window else None,
            destination_rows=rows,
            distinct_keys=keys,
            source_rows=definition.source.count(resource, window) if source_counts else None,
        )
        for resource, window, rows, keys in counted
    ]


def _distinct_keys(client: Any, table: str, resource: Resource) -> int | None:
    keys = getattr(resource.write, "primary_key", None)
    if not keys:
        return None
    columns = ", ".join(client.escape_column_name(key) for key in keys)
    return _scalar(client, f"SELECT COUNT(*) FROM (SELECT DISTINCT {columns} FROM {table}) k")


def _scalar(client: Any, sql: str) -> int:
    rows = client.execute_sql(sql)
    return int(rows[0][0])


def _cursor_column(schema: Any, resource: Resource) -> str | None:
    """The cursor's column in the destination table, when the rows carry it (a filesystem
    cursor is the file's date, which they do not)."""
    if resource.cursor is None or resource.backfill is None or resource.backfill.window.positional:
        return None
    column = schema.naming.normalize_path(resource.cursor.tracked_path)
    return column if column in schema.tables[resource.name]["columns"] else None


def _windows(resource: Resource) -> list[Window]:
    assert resource.backfill is not None
    return list(resource.backfill.windows(resource.cursor, None, None))


def _counts(
    client: Any, table: str, column: str, resource: Resource, windows: Sequence[Window]
) -> Iterator[int]:
    """Rows per window, ``_PER_QUERY`` windows a statement, each bounded as dlt bounds the
    backfill's extract: the cursor's rendering of ``[start, end)``."""
    assert resource.cursor is not None
    quoted = client.escape_column_name(column)
    for first in range(0, len(windows), _PER_QUERY):
        batch = windows[first : first + _PER_QUERY]
        parts: list[str] = []
        args: list[Any] = []
        for index, window in enumerate(batch):
            bounds = resource.cursor.bounds(window)
            assert bounds is not None
            parts.append(
                f"SELECT {index} AS w, COUNT(*) AS n FROM {table} "
                f"WHERE {quoted} >= %s AND {quoted} < %s"
            )
            args.extend(bounds)
        found = dict(client.execute_sql(" UNION ALL ".join(parts), *args))
        yield from (int(found[index]) for index in range(len(batch)))

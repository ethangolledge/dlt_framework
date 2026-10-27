"""Is all the data there, exactly once: backfill coverage and its gaps, holes and re-reads in
the incremental cursor, windows loaded twice, and what reconciliation counted.

These read all of history, not the scope: a gap from last year is still a gap today.
"""

from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from itertools import pairwise
from typing import Any

import polars as pl

from dashboard.model.facts import View
from dlt_framework.core.monitoring import Coverage, ResourceRecord
from dlt_framework.core.windows import WINDOWS


@dataclass(frozen=True, slots=True)
class Covered:
    resource_id: str
    kind: str  # the window kind: time, value, rows
    fraction: float | None  # None while a row history is open-ended
    gaps: list[tuple[str, str | None]]  # as window edges, ready for --from / --to
    cells: list[bool | None]  # the ribbon: each cell covered, missing, or past the known end
    commands: list[str]  # the backfill that fills each gap


def coverage(view: View, cells: int = 60) -> list[Covered]:
    """Each declared backfill's coverage, as a ribbon of ``cells`` and its gaps' commands."""
    out = []
    for resource in _declared(view):
        found = Coverage.recorded(view.frames.loads, resource)
        if found is None:
            continue
        window = WINDOWS[resource["backfill_window"]]
        edge = window.format_edge
        pipeline, name = resource["pipeline_id"], resource["resource"]
        gaps = [(edge(low), None if high is None else edge(high)) for low, high in found.gaps]
        out.append(
            Covered(
                resource_id=resource["resource_id"],
                kind=window.kind,
                fraction=found.fraction,
                gaps=gaps,
                cells=_ribbon(found, cells),
                commands=[
                    f"pipeline backfill {pipeline} --resource {name} --from {low}"
                    + (f" --to {high}" if high is not None else "")
                    for low, high in gaps
                ],
            )
        )
    return out


def _ribbon(found: Coverage, cells: int) -> list[bool | None]:
    """Whether the middle of each of ``cells`` equal parts of the history is covered."""
    start = _number(found.start)
    end = None if found.end is None else _number(found.end)
    covered = [(_number(low), _number(high)) for low, high in found.covered]
    if end is None:  # an open-ended row history: as far as it has been read
        end = max((high for _, high in covered), default=start + 1)
    out: list[bool | None] = []
    for index in range(cells):
        middle = start + (end - start) * (index + 0.5) / cells
        out.append(any(low <= middle < high for low, high in covered))
    return out


def _number(edge: Any) -> float:
    return edge.timestamp() if isinstance(edge, datetime) else float(edge)


@dataclass(frozen=True, slots=True)
class Break:
    """Two consecutive increments whose cursors do not meet."""

    resource_id: str
    kind: str  # hole: the next began after the last ended; reread: before it; reset: went back
    at: datetime
    previous_last: str
    next_start: str


def cursor_breaks(view: View) -> list[Break]:
    """Where an incremental cursor skipped (a **hole**: state jumped past rows never loaded),
    re-read (a **reread**: duplicates under ``Append``, expected with ``lag``), or went backwards
    (a **reset**: state was dropped or rebuilt). Consecutive increments normally meet exactly:
    each starts where the one before ended."""
    loads = view.frames.loads.filter(
        (pl.col("window_kind") == "increment") & pl.col("cursor_start_value").is_not_null()
    ).unique(["load_id", "resource_id"])
    kinds = {
        row["resource_id"]: row["cursor_kind"]
        for row in view.frames.resources.iter_rows(named=True)
    }
    found = []
    for key, own in loads.sort("at").group_by("resource_id", maintain_order=True):
        resource_id = str(key[0])
        parse = _parser(kinds.get(resource_id))
        rows = own.select("at", "cursor_start_value", "cursor_last_value").rows()
        for (_, _, last_a), (at_b, start_b, last_b) in pairwise(rows):
            if last_a is None or start_b is None:
                continue
            previous, begun = parse(last_a), parse(start_b)
            if begun > previous:
                found.append(Break(resource_id, "hole", at_b, last_a, start_b))
            elif last_b is not None and parse(last_b) < previous:
                found.append(Break(resource_id, "reset", at_b, last_a, start_b))
            elif begun < previous:
                found.append(Break(resource_id, "reread", at_b, last_a, start_b))
    return found


def _parser(cursor_kind: str | None) -> Callable[[str], Any]:
    """Cursor values as recorded (readable text) back to comparable values: integers for value
    cursors, instants for time cursors (UTC when the text has no zone), else the text."""
    if cursor_kind == "ValueCursor":
        return int
    if cursor_kind == "TimeCursor":
        return _instant
    return str


def _instant(text: str) -> datetime:
    moment = datetime.fromisoformat(text.replace("Z", "+00:00"))
    return moment if moment.tzinfo else moment.replace(tzinfo=UTC)


def double_loads(view: View) -> pl.DataFrame:
    """Backfill windows loaded more than once: wasted work, and duplicates under ``Append``."""
    loads = view.frames.loads.filter(pl.col("window_kind") != "increment").unique(
        ["load_id", "resource_id"]
    )
    return (
        loads.group_by("resource_id", "window_kind", "window_start", "window_end")
        .agg(times=pl.len(), rows=pl.col("rows").sum(), last=pl.col("at").max())
        .filter(pl.col("times") > 1)
        .sort("times", descending=True)
    )


def empty_windows(view: View) -> pl.DataFrame:
    """Backfill windows that loaded no rows while the windows either side loaded some: a source
    outage, or a filter that did not reach upstream."""
    loads = (
        view.frames.loads.filter(pl.col("window_kind").is_in(["time", "value"]))
        .unique(["resource_id", "window_kind", "window_start", "window_end"], keep="last")
        .sort("resource_id", "window_start")
    )
    return loads.filter(
        (pl.col("rows") == 0)
        & (pl.col("rows").shift(1).over("resource_id") > 0)
        & (pl.col("rows").shift(-1).over("resource_id") > 0)
    ).select("resource_id", "window_kind", "window_start", "window_end", "at")


def reconciled(view: View) -> pl.DataFrame:
    """Each resource's latest reconciliation: rows, duplicate keys, and the windows whose count
    differs from the source."""
    frame = view.frames.reconciliations
    if frame.is_empty():
        return pl.DataFrame(schema={"resource_id": pl.String, "rows": pl.Int64})
    latest = frame.sort("at").group_by("resource_id").agg(pl.col("reconcile_id").last())
    rows = frame.join(latest, on=["resource_id", "reconcile_id"])
    whole = rows.filter(pl.col("window_kind") == "all").select(
        "resource_id",
        "at",
        rows="destination_rows",
        duplicates=pl.col("destination_rows") - pl.col("distinct_keys"),
        source_rows="source_rows",
    )
    differ = (
        rows.filter(
            pl.col("source_rows").is_not_null()
            & (pl.col("source_rows") != pl.col("destination_rows"))
        )
        .group_by("resource_id")
        .agg(
            differing=pl.len(),
            missing=(pl.col("source_rows") - pl.col("destination_rows")).clip(0).sum(),
        )
    )
    return whole.join(differ, on="resource_id", how="left").with_columns(
        pl.col("differing", "missing").fill_null(0)
    )


def _declared(view: View) -> list[dict[str, Any]]:
    pipelines = view.scope.pipelines or tuple(view.pipelines)
    return [
        row
        for pipeline in pipelines
        for row in ResourceRecord.declared(view.frames.resources, pipeline)
    ]

"""What history is missing, and how to fill it.

Every missing range is a ``Missing``: its resource, its edges, why it is missing, and the
command that fills it (``None`` when there is none to run). They come from:

- **gaps** in a declared backfill's history: never loaded, or failed in a backfill run;
- **empty** backfill windows between windows that had rows: an outage, or a filter that did not
  reach upstream;
- **off** windows, whose rows differ from the source in the latest reconciliation;
- **holes** in an incremental cursor: an increment began after the one before it ended;
- **resets**, where a cursor went back, and windows loaded **twice**: no rows missing, but
  duplicates under ``Append``.

These read all of history, not the range: a gap from last year is still a gap today.
"""

from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from itertools import pairwise
from typing import Any

import polars as pl

from dashboard.model.facts import View
from dashboard.model.incidents import cause, rerun
from dashboard.units import edge
from dlt_framework.core.monitoring import Coverage, ResourceRecord
from dlt_framework.core.windows import WINDOWS

FIXABLE = ("gap", "failed", "empty", "off", "hole")  # kinds with rows missing


@dataclass(frozen=True, slots=True)
class Missing:
    resource_id: str
    kind: str  # gap, failed, empty, off, hole, reset, twice
    start: str | None
    end: str | None
    why: str
    command: str | None

    @property
    def pipeline(self) -> str:
        return self.resource_id.split(".", 1)[0]


@dataclass(frozen=True, slots=True)
class History:
    """A resource's declared backfill: how far it is loaded, and what is missing."""

    resource_id: str
    window: str  # the window type, e.g. TimeWindows
    start: str
    end: str | None  # None: open-ended until a short window
    fraction: float | None
    cells: list[bool]  # the ribbon: each equal part of the history loaded or not
    missing: list[Missing]


def histories(view: View, cells: int = 60) -> list[History]:
    """Each declared backfill in scope, with the gaps, empty and off windows in it."""
    out = []
    failed = _failed_windows(view)
    for resource in _declared(view):
        found = Coverage.recorded(view.frames.loads, resource)
        if found is None:
            continue
        window = WINDOWS[resource["backfill_window"]]
        resource_id = resource["resource_id"]
        missing = []
        for low, high in found.gaps:
            start, end = window.format_edge(low), None if high is None else window.format_edge(high)
            broke = _stopped_in(failed.get(resource_id, []), window.parse_edge, low, high)
            if broke is not None:
                stopped = _window(broke["error_window"])
                at = edge(stopped[0]) if stopped else "a window"
                kind, why = (
                    "failed",
                    f"backfill {broke['run_id'][:8]} failed at {at} ({cause(broke)}); "
                    "nothing after it was loaded",
                )
            elif high is None:
                kind, why = "gap", "not reached yet: history ends at the first short window"
            else:
                kind, why = "gap", "never loaded"
            missing.append(
                Missing(resource_id, kind, start, end, why, command(resource, start, end))
            )
        missing += _empty(view, resource) + _off(view, resource) + _twice(view, resource)
        out.append(
            History(
                resource_id=resource_id,
                window=resource["backfill_window"],
                start=window.format_edge(found.start),
                end=None if found.end is None else window.format_edge(found.end),
                fraction=found.fraction,
                cells=_ribbon(found, cells),
                missing=missing,
            )
        )
    return out


def holes(view: View) -> list[Missing]:
    """Holes and resets in every incremental cursor in scope, with the backfill that reloads a
    hole where the resource declares one."""
    loads = view.frames.loads.filter(
        (pl.col("window_kind") == "increment") & pl.col("cursor_start_value").is_not_null()
    ).unique(["load_id", "resource_id"])
    resources = {row["resource_id"]: row for row in _latest_resources(view)}
    found = []
    for key, own in loads.sort("at").group_by("resource_id", maintain_order=True):
        resource_id = str(key[0])
        if resource_id not in resources:
            continue  # not in scope
        resource = resources[resource_id]
        parse = _parser(resource["cursor_kind"])
        rows = own.select("at", "cursor_start_value", "cursor_last_value").rows()
        for (_, _, last_a), (at_b, start_b, last_b) in pairwise(rows):
            if last_a is None or start_b is None:
                continue
            previous, begun = parse(last_a), parse(start_b)
            when = f"{at_b:%Y-%m-%d %H:%M}Z"
            if begun > previous:
                reload = command(resource, last_a, start_b) if resource["backfill_window"] else None
                why = f"the increment at {when} began after the one before ended"
                found.append(
                    Missing(
                        resource_id,
                        "hole",
                        last_a,
                        start_b,
                        why if reload else f"{why}; no backfill declared to reload it",
                        reload,
                    )
                )
            elif last_b is not None and parse(last_b) < previous:
                found.append(
                    Missing(
                        resource_id,
                        "reset",
                        last_a,
                        start_b,
                        f"the cursor went back at {when}: its state was reset; "
                        "under Append, rows since are loaded twice",
                        None,
                    )
                )
    return found


def jobs(view: View) -> pl.DataFrame:
    """Backfill runs in range, newest first: what each loaded, and where a failed one stopped."""
    runs = view.mode("backfill").runs
    windows = (
        view.frames.loads.filter(pl.col("window_kind") != "increment")
        .unique(["resource_id", "window_start", "window_end", "run_id"])
        .group_by("run_id")
        .agg(windows=pl.len(), resources=pl.col("resource_id").unique().sort().str.join(", "))
    )
    return (
        runs.join(windows, on="run_id", how="left")
        .with_columns(pl.col("windows").fill_null(0))
        .with_columns(
            resume=pl.struct(pl.all()).map_elements(
                lambda run: rerun(run) if run["outcome"] == "failed" else None,
                return_dtype=pl.String,
            )
        )
        .sort("started_at", descending=True)
    )


def command(resource: dict[str, Any], start: str | None, end: str | None) -> str:
    """The backfill that loads ``[start, end)`` of ``resource``."""
    pipeline, name = resource["pipeline_id"], resource["resource"]
    text = f"pipeline backfill {pipeline} --resource {name}"
    return text + (f" --from {start}" if start else "") + (f" --to {end}" if end else "")


def _window(text: str | None) -> tuple[str, str] | None:
    """``[start, end)`` as its two edges."""
    if not text or not text.startswith("["):
        return None
    low, _, high = text.strip("[)").partition(",")
    return low.strip(), high.strip()


def _stopped_in(
    failed: list[tuple[str, dict[str, Any]]], parse: Callable[[str], Any], low: Any, high: Any
) -> dict[str, Any] | None:
    """The latest failed backfill run that stopped at a window inside ``[low, high)``."""
    for at, run in sorted(failed, key=lambda item: item[1]["started_at"], reverse=True):
        try:
            point = parse(at)
        except ValueError:
            continue
        if low <= point and (high is None or point < high):
            return run
    return None


def _failed_windows(view: View) -> dict[str, list[tuple[str, dict[str, Any]]]]:
    """Per resource, the window each failed backfill run stopped at, and the run."""
    out: dict[str, list[tuple[str, dict[str, Any]]]] = {}
    broken = view.history.filter((pl.col("mode") == "backfill") & (pl.col("outcome") == "failed"))
    for run in broken.iter_rows(named=True):
        edges = _window(run["error_window"])
        if edges is None:
            continue
        for name in str(run["error_resource"] or "").split(", "):
            out.setdefault(f"{run['pipeline_id']}.{name}", []).append((edges[0], run))
    return out


def _empty(view: View, resource: dict[str, Any]) -> list[Missing]:
    loads = (
        view.frames.loads.filter(
            (pl.col("resource_id") == resource["resource_id"])
            & (pl.col("window_kind") != "increment")
        )
        .unique(["window_start", "window_end"], keep="last")
        .sort("window_start")
    )
    empty = loads.filter(
        (pl.col("rows") == 0) & (pl.col("rows").shift(1) > 0) & (pl.col("rows").shift(-1) > 0)
    )
    return [
        Missing(
            resource["resource_id"],
            "empty",
            row["window_start"],
            row["window_end"],
            "loaded no rows, between windows that had some",
            command(resource, row["window_start"], row["window_end"]),
        )
        for row in empty.iter_rows(named=True)
    ]


def _off(view: View, resource: dict[str, Any]) -> list[Missing]:
    """Windows whose rows differ from the source in the resource's latest reconciliation."""
    own = view.frames.reconciliations.filter(pl.col("resource_id") == resource["resource_id"])
    if own.is_empty():
        return []
    latest = own.filter(pl.col("reconcile_id") == own.sort("at")["reconcile_id"][-1])
    differ = latest.filter(
        (pl.col("window_kind") != "all")
        & pl.col("source_rows").is_not_null()
        & (pl.col("source_rows") != pl.col("destination_rows"))
    ).sort("window_start")
    return [
        Missing(
            resource["resource_id"],
            "off",
            row["window_start"],
            row["window_end"],
            f"{row['destination_rows']:,} rows here, {row['source_rows']:,} at the source "
            f"(reconciled {row['at']:%Y-%m-%d})",
            command(resource, row["window_start"], row["window_end"]),
        )
        for row in differ.iter_rows(named=True)
    ]


def _twice(view: View, resource: dict[str, Any]) -> list[Missing]:
    twice = (
        view.frames.loads.filter(
            (pl.col("resource_id") == resource["resource_id"])
            & (pl.col("window_kind") != "increment")
        )
        .unique(["load_id"])
        .group_by("window_start", "window_end")
        .agg(times=pl.len())
        .filter(pl.col("times") > 1)
        .sort("window_start")
    )
    under = "duplicates under Append" if resource["write_mode"] == "Append" else "wasted work"
    return [
        Missing(
            resource["resource_id"],
            "twice",
            row["window_start"],
            row["window_end"],
            f"loaded {row['times']} times: {under}",
            None,
        )
        for row in twice.iter_rows(named=True)
    ]


def duplicates(view: View) -> pl.DataFrame:
    """Each resource's latest reconciliation where its keys are not unique."""
    frame = view.frames.reconciliations.filter(pl.col("window_kind") == "all")
    if frame.is_empty():
        return pl.DataFrame(schema={"resource_id": pl.String, "duplicates": pl.Int64})
    return (
        frame.sort("at")
        .group_by("resource_id")
        .last()
        .with_columns(duplicates=pl.col("destination_rows") - pl.col("distinct_keys"))
        .filter(pl.col("duplicates") > 0)
        .select("resource_id", "duplicates", "destination_rows", "at")
    )


def _ribbon(found: Coverage, cells: int) -> list[bool]:
    """Whether the middle of each of ``cells`` equal parts of the history is loaded."""
    start = _number(found.start)
    covered = [(_number(low), _number(high)) for low, high in found.covered]
    end = (
        max((high for _, high in covered), default=start + 1)
        if found.end is None
        else _number(found.end)
    )
    return [
        any(low <= start + (end - start) * (index + 0.5) / cells < high for low, high in covered)
        for index in range(cells)
    ]


def _number(value: Any) -> float:
    return value.timestamp() if isinstance(value, datetime) else float(value)


def _parser(cursor_kind: str | None) -> Callable[[str], Any]:
    """Cursor values as recorded back to comparable values."""
    if cursor_kind == "ValueCursor":
        return int
    if cursor_kind == "TimeCursor":
        return _instant
    return str


def _instant(text: str) -> datetime:
    moment = datetime.fromisoformat(text.replace("Z", "+00:00"))
    return moment if moment.tzinfo else moment.replace(tzinfo=UTC)


def _latest_resources(view: View) -> list[dict[str, Any]]:
    pipelines = view.scope.pipelines or tuple(view.pipelines)
    return [
        row
        for pipeline in pipelines
        for row in ResourceRecord.declared(view.frames.resources, pipeline)
    ]


def _declared(view: View) -> list[dict[str, Any]]:
    return [row for row in _latest_resources(view) if row["backfill_window"]]


def edges(item: Missing) -> str:
    """``2026-10-04 00:00 → 2026-10-05 00:00``."""
    return f"{edge(item.start)} → {edge(item.end) if item.end else 'open'}"

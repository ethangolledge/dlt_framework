"""``pipeline status``: per resource, the last run, where its cursor is, and backfill coverage.

Built only from monitoring frames, so it never connects to the destination or reads dlt state.
"""

from typing import Any

import polars as pl

from dlt_framework.core.monitoring.coverage import Coverage
from dlt_framework.core.monitoring.records import ResourceRecord
from dlt_framework.core.pipeline import Pipeline
from dlt_framework.core.resource import Resource

COLUMNS = (
    "pipeline",
    "resource",
    "last_run",
    "mode",
    "outcome",
    "last_success",
    "rows",
    "cursor",
    "backfill",
    "gaps",
    "first_gap",
)
_NONE = "-"


def status(definition: Pipeline, runs: pl.DataFrame, loads: pl.DataFrame) -> list[dict[str, str]]:
    own = runs.filter(pl.col("pipeline_id") == definition.name).sort("at")
    succeeded = own.filter(pl.col("event") == "succeeded")
    common = {
        "pipeline": definition.name,
        **_last_run(own),
        "last_success": _moment(succeeded) if succeeded.height else _NONE,
    }
    return [
        {**common, "resource": resource.name, **_resource(definition, resource, loads)}
        for resource in definition.source.resources
    ]


def _last_run(runs: pl.DataFrame) -> dict[str, str]:
    """When the latest run started, its mode, and its outcome. A run that never reported one is
    ``lost`` (killed, e.g. for OOM) or still running."""
    if not runs.height:
        return dict.fromkeys(("last_run", "mode", "outcome"), _NONE)
    latest = runs.filter(pl.col("run_id") == runs["run_id"][-1])
    outcome = latest["event"][-1]
    return {
        "last_run": _moment(latest.head(1)),
        "mode": latest["mode"][-1],
        "outcome": "lost or running" if outcome == "started" else outcome,
    }


def _resource(definition: Pipeline, resource: Resource, loads: pl.DataFrame) -> dict[str, str]:
    resource_id = ResourceRecord.key(definition.name, resource.name)
    increments = loads.filter(
        (pl.col("resource_id") == resource_id) & (pl.col("window_kind") == "increment")
    ).sort("at")
    latest = increments.tail(1).to_dicts()
    row = {
        "rows": str(latest[0]["rows"]) if latest else _NONE,
        "cursor": (latest[0]["cursor_last_value"] or _NONE) if latest else _NONE,
        "backfill": _NONE,
        "gaps": _NONE,
        "first_gap": _NONE,
    }
    if resource.backfill is None:
        return row
    coverage = Coverage.of(loads, resource_id, resource.backfill, resource.cursor)
    gaps = coverage.gaps
    fraction = coverage.fraction
    edge = resource.backfill.window.format_edge
    row["backfill"] = f"{fraction:.0%}" if fraction is not None else "open: rows not exhausted"
    row["gaps"] = str(len(gaps))
    if gaps:
        start, end = gaps[0]
        row["first_gap"] = f"[{edge(start)}, {_NONE if end is None else edge(end)})"
    return row


def _moment(frame: pl.DataFrame) -> str:
    at: Any = frame["at"][-1]
    return f"{at:%Y-%m-%d %H:%M}Z"  # monitoring stores UTC

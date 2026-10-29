"""The browser app's data: the model's facts as plain JSON values.

``summary`` is what every page needs at once: pipelines (what each is, and its state now),
every run in brief, incidents, missing data, backfill jobs, and the checks. ``detail`` is one
run in full (its error, steps, memory, loads, tables), read only when that run is opened.
Times are ISO 8601 in UTC. Runs and incidents cover all history, so the page can filter by any
range; states and checks are judged over the last ``DAYS`` days, and say so.
"""

from dataclasses import asdict
from datetime import datetime
from typing import Any

import polars as pl

from dashboard.model import gaps, incidents, performance, pipelines, triage
from dashboard.model.facts import BROKEN, View
from dlt_framework.core.monitoring import Frames
from dlt_framework.core.monitoring.health import Signal

SAMPLES = 240  # memory samples kept per run, at most


def summary(frames: Frames, now: datetime, signals: list[Signal] | None = None) -> dict[str, Any]:
    judged = triage.judge(frames, now, signals)
    everything = judged.everything
    states = {status.pipeline: status for status in judged.statuses}
    return {
        "generated": _time(now),
        "days": triage.DAYS,
        "lateness": judged.lateness,
        "pipelines": [
            _pipeline(everything, name, states.get(name), judged.last_success.get(name))
            for name in everything.pipelines
        ],
        "runs": [_run(run) for run in everything.history.iter_rows(named=True)],
        "incidents": [_incident(item) for item in judged.incidents],
        "missing": [_missing(item) for item in judged.missing],
        "histories": [_history(item) for item in judged.histories],
        "jobs": [
            {"run": job["run_id"], "windows": job["windows"], "resume": job["resume"]}
            for job in gaps.jobs(everything).iter_rows(named=True)
        ],
        "duplicates": _rows(gaps.duplicates(everything)),
        "findings": [asdict(item) for item in judged.report.findings],
        "unchecked": [
            {"title": check.title, "needs": lacks} for check, lacks in judged.report.skipped
        ],
        "memory": _rows(performance.memory(judged.production)),
        "steps": _rows(performance.step_profile(judged.production)),
        "slow_tables": _rows(performance.slow_tables(judged.production).head(15)),
    }


def detail(frames: Frames, run_id: str) -> dict[str, Any]:
    """One run in full: what its records hold beyond the summary."""

    def mine(frame: pl.DataFrame) -> pl.DataFrame:
        return frame.filter(pl.col("run_id") == run_id)

    runs = frames.runs.filter((pl.col("run_id") == run_id) & (pl.col("event") != "started"))
    error = runs.sort("at").tail(1).to_dicts()
    samples = mine(frames.samples).sort("at")
    kept = _evenly(samples.height, SAMPLES)
    return {
        "run": run_id,
        "error": {
            key.removeprefix("error_"): value
            for key, value in (error[0] if error else {}).items()
            if key.startswith("error_") or key == "exit_code"
        },
        "steps": [
            {
                "step": step["step"],
                "attempt": step["attempt"],
                "started": _time(step["started_at"]),
                "ended": _time(step["at"]),
                "seconds": step["duration_s"],
                "cpu": step["cpu_s"],
                "outcome": step["outcome"],
                "cause": step["error_cause_type"],
            }
            for step in mine(frames.steps).sort("started_at").iter_rows(named=True)
        ],
        "samples": [
            [_time(row["at"]), row["rss_mb"], row["step"], row["memory_limit_mb"]]
            for index, row in enumerate(samples.iter_rows(named=True))
            if index in kept
        ],
        "loads": [
            {
                "resource": load["resource_id"].split(".", 1)[1],
                "kind": load["window_kind"],
                "rows": load["rows"],
                "window": [load["window_start"], load["window_end"]],
                "cursor": [load["cursor_start_value"], load["cursor_last_value"]],
                "extracted": [_time(load["extract_first_at"]), _time(load["extract_last_at"])],
            }
            for load in mine(frames.loads).sort("at").iter_rows(named=True)
        ],
        "tables": [
            {"table": row["table"], "rows": row["rows"], "seconds": row["load_s"]}
            for row in mine(frames.tables).iter_rows(named=True)
        ],
    }


def _pipeline(
    view: View, name: str, status: triage.Status | None, last_success: datetime | None
) -> dict[str, Any]:
    about = pipelines.about(view, name)
    loads = view.frames.loads.filter(pl.col("resource_id").str.starts_with(f"{name}."))
    loaded = {
        row["resource_id"].split(".", 1)[1]: row
        for row in loads.group_by("resource_id")
        .agg(rows=pl.col("rows").sum(), loads=pl.len(), last=pl.col("at").max())
        .iter_rows(named=True)
    }
    tables = (
        view.frames.tables.filter(pl.col("resource_id").str.starts_with(f"{name}."))
        .group_by("table")
        .agg(rows=pl.col("rows").sum(), last=pl.col("at").max())
        .sort("table")
    )
    return {
        "id": name,
        "state": status.level if status else "idle",
        "headline": status.headline if status else f"no runs in the last {triage.DAYS} days",
        "concerns": [asdict(concern) for concern in (status.concerns if status else ())],
        "source_type": about.source_type,
        "source": about.source,
        "destination": about.destination,
        "dataset": about.dataset,
        "last_success": _time(last_success),
        "resources": [
            {
                "name": row["resource"],
                "write": row["write_mode"],
                "cursor": row["cursor_path"],
                "cursor_kind": row["cursor_kind"],
                "cursor_start": row["cursor_start"],
                "history": [row["backfill_start"], row["backfill_end"]]
                if row["backfill_window"]
                else None,
                "window": row["backfill_window"],
                "empty": row["empty"],
                "rows": loaded.get(row["resource"], {}).get("rows"),
                "loads": loaded.get(row["resource"], {}).get("loads"),
                "last_load": _time(loaded.get(row["resource"], {}).get("last")),
                "cursor_path": [
                    [_time(at), value]
                    for at, value in loads.filter(
                        (pl.col("resource_id") == f"{name}.{row['resource']}")
                        & pl.col("cursor_last_value").is_not_null()
                        & (pl.col("window_kind") == "increment")
                    )
                    .sort("at")
                    .select("at", "cursor_last_value")
                    .iter_rows()
                ],
            }
            for row in about.resources
        ],
        "tables": [
            {"table": row["table"], "rows": row["rows"], "last": _time(row["last"])}
            for row in tables.iter_rows(named=True)
        ],
    }


def _run(run: dict[str, Any]) -> dict[str, Any]:
    broken = run["outcome"] in BROKEN
    return {
        "id": run["run_id"],
        "pipeline": run["pipeline_id"],
        "mode": run["mode"],
        "started": _time(run["started_at"]),
        "ended": _time(run["ended_at"]),
        "outcome": run["outcome"],
        "empty": run["empty"],
        "seconds": run["duration_s"],
        "rows": run["rows"],
        "peak_mb": run["peak_rss_mb"],
        "available_mb": run["available_mb"],
        "limit": bool(run["memory_limit_mb"]),
        "version": run["code_version"],
        "host": run["host"],
        "cpu_s": run["cpu_seconds"],
        "cause": incidents.cause(run) if broken else None,
        "detail": incidents.detail(run) if broken else None,
        "rerun": incidents.rerun(run) if broken else None,
    }


def _incident(item: incidents.Incident) -> dict[str, Any]:
    return {
        "pipeline": item.pipeline,
        "mode": item.mode,
        "open": item.open,
        "started": _time(item.started_at),
        "resolved": _time(item.resolved_at),
        "runs": [run["run_id"] for run in item.runs],
        "causes": [[name, count] for name, count in item.causes],
        "latest": item.latest["run_id"],
        "rerun": incidents.rerun(item.latest),
    }


def _missing(item: gaps.Missing) -> dict[str, Any]:
    return {
        "resource": item.resource_id,
        "pipeline": item.pipeline,
        "kind": item.kind,
        "start": item.start,
        "end": item.end,
        "why": item.why,
        "command": item.command,
        "fixable": item.kind in gaps.FIXABLE,
    }


def _history(item: gaps.History) -> dict[str, Any]:
    return {
        "resource": item.resource_id,
        "window": item.window,
        "start": item.start,
        "end": item.end,
        "fraction": item.fraction,
        "cells": item.cells,
    }


def _rows(frame: pl.DataFrame) -> list[dict[str, Any]]:
    return [
        {key: _time(value) if isinstance(value, datetime) else value for key, value in row.items()}
        for row in frame.iter_rows(named=True)
    ]


def _time(moment: datetime | None) -> str | None:
    return None if moment is None else moment.isoformat().replace("+00:00", "Z")


def _evenly(count: int, most: int) -> set[int]:
    """Indexes of at most ``most`` of ``count`` items, evenly spread, first and last kept."""
    if count <= most:
        return set(range(count))
    step = (count - 1) / (most - 1)
    return {round(index * step) for index in range(most)}

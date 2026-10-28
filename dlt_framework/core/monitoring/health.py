"""``pipeline health``: what is wrong now, judged from monitoring frames alone.

Runs record facts; this derives signals from them, so no threshold lives in loader code. Each
signal compares a pipeline's latest run with its own history, so "slow" and "late" mean slow
and late for that pipeline. Thresholds are stated in ``[health]`` and can be overridden per
pipeline (``[pipelines.<name>.health]``).
"""

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

import polars as pl

from dlt_framework.core.monitoring.coverage import Coverage
from dlt_framework.core.monitoring.records import Frames, ReconcileRecord, ResourceRecord
from dlt_framework.core.settings import HealthSettings
from dlt_framework.core.windows import WINDOWS

COLUMNS = ("pipeline", "resource", "event", "level", "detail")
_ENDED = ("succeeded", "failed", "lost")
_RETRY_RUNS = (3, 5)  # retries in 3 of the last 5 runs is a trend, not a blip
_MIN_HISTORY = 5  # successes needed before "slow" or "unusual rows" can be judged
_REGULAR = 0.8  # share of gaps within half to double their median for a derived schedule


@dataclass(frozen=True, slots=True)
class Signal:
    pipeline: str
    resource: str | None
    event: str
    level: str  # "error": broken now; "warning": heading there
    detail: str

    def row(self) -> dict[str, str]:
        return {
            "pipeline": self.pipeline,
            "resource": self.resource or "-",
            "event": self.event,
            "level": self.level,
            "detail": self.detail,
        }


def health(
    name: str,
    settings: HealthSettings,
    frames: Frames,
    now: datetime | None = None,
) -> list[Signal]:
    """Signals for pipeline ``name``'s incremental runs, its backfill coverage, and its latest
    reconciliation, from the frames alone: its resources are those its latest run declared."""
    now = now or datetime.now(UTC)
    runs, loads, steps, reconciliations = (
        frames.runs,
        frames.loads,
        frames.steps,
        frames.reconciliations,
    )
    own = runs.filter((pl.col("dlt_pipeline") == name) & pl.col("event").is_in(_ENDED))
    own = own.sort("at").tail(settings.history_runs)
    signals: list[Signal] = []

    def signal(event: str, level: str, detail: str, resource: str | None = None) -> None:
        signals.append(Signal(name, resource, event, level, detail))

    successes = own.filter(pl.col("event") == "succeeded")
    if own.height:
        latest = own.row(-1, named=True)
        if latest["event"] == "failed":
            signal("run_failed", "error", _failure(latest, own))
        elif latest["event"] == "lost":
            signal("run_lost", "error", _lost(latest))
        if (pressure := _pressure(latest)) is not None and pressure > 1 - settings.memory_headroom:
            signal("memory_high", "warning", f"peak memory {pressure:.0%} of what was available")
    if (late := _late(successes, settings, now)) is not None:
        signal("run_stale", "error", late)
    durations = successes["duration_s"].drop_nulls().to_list()  # older records may have none
    if len(durations) > _MIN_HISTORY:
        *history, latest_s = durations
        p95 = pl.Series(history).quantile(0.95)
        if p95 and latest_s > p95 * settings.slow_after:
            signal("run_slow", "warning", f"took {latest_s:.0f}s; usually under {p95:.0f}s (p95)")
    if (retried := _retried(steps, own)) is not None:
        signal("retries_rising", "warning", retried)
    for resource in ResourceRecord.declared(frames.resources, name):
        resource_id, resource_name = resource["resource_id"], resource["resource"]
        if (
            unusual := _rows(loads, successes, resource_id, resource["empty"], settings)
        ) is not None:
            signal("rows_anomaly", "warning", unusual, resource_name)
        if (mismatch := _reconciled(reconciliations, resource_id)) is not None:
            signal("reconcile_mismatch", "warning", mismatch, resource_name)
        if (coverage := Coverage.recorded(loads, resource)) is not None:
            gaps = coverage.gaps
            edge = WINDOWS[resource["backfill_window"]].format_edge
            if gaps and gaps[0][1] is not None:  # an open end is history not yet reached
                first = f"[{edge(gaps[0][0])}, {edge(gaps[0][1])})"
                signal(
                    "coverage_gap", "warning", f"{len(gaps)} gap(s), first {first}", resource_name
                )
    return signals


def _failure(latest: dict[str, Any], runs: pl.DataFrame) -> str:
    streak = 0
    for event in reversed(runs["event"].to_list()):
        if event == "succeeded":
            break
        streak += 1
    where = latest["error_resource"] or latest["error_step"] or "run"
    lines = (latest["error_message"] or "").splitlines()
    message = lines[0][:200] if lines else ""
    times = f" ({streak} runs in a row)" if streak > 1 else ""
    return f"{latest['error_type']} in {where}{times}: {message}"


def _lost(latest: dict[str, Any]) -> str:
    seen = f" during {latest['error_step']}" if latest["error_step"] else ""
    peak = f", peak {latest['peak_rss_mb']:.0f} MB" if latest["peak_rss_mb"] else ""
    return f"killed{seen}{peak}; no end was recorded"


def _pressure(run: dict[str, Any]) -> float | None:
    peak = run.get("peak_rss_mb")
    limit = run.get("memory_limit_mb") or run.get("host_memory_mb")
    return peak / limit if peak and limit else None


def _late(successes: pl.DataFrame, settings: HealthSettings, now: datetime) -> str | None:
    if not successes.height:
        return None
    expected = settings.expect_every_s
    if not expected:
        gaps = successes["at"].diff().drop_nulls().dt.total_seconds()
        if gaps.len() < _MIN_HISTORY:
            return None  # too few runs to know the schedule: state expect_every_s
        expected = float(gaps.median())  # type: ignore[arg-type]
        agree = gaps.is_between(expected / 2, expected * 2).mean()
        if agree is None or float(agree) < _REGULAR:  # type: ignore[arg-type]
            return None  # runs by hand, or bursts: no schedule to be late against
    last = successes["at"][-1]
    allowed = timedelta(seconds=expected * settings.stale_after_intervals)
    if now - last <= allowed:
        return None
    every = timedelta(seconds=round(expected))
    return f"last success {last:%Y-%m-%d %H:%M}Z; expected every {every}"


def _retried(steps: pl.DataFrame, runs: pl.DataFrame) -> str | None:
    needed, of = _RETRY_RUNS
    recent = runs["run_id"].tail(of)
    retried = steps.filter(pl.col("run_id").is_in(recent.implode()) & (pl.col("attempt") > 1))
    if retried["run_id"].n_unique() < needed:
        return None
    most = retried.group_by("step").len().sort("len")["step"][-1]
    runs_retried = retried["run_id"].n_unique()
    return f"{runs_retried} of the last {recent.len()} runs retried a step, most often {most}"


def _rows(
    loads: pl.DataFrame,
    successes: pl.DataFrame,
    resource_id: str,
    empty: str,
    settings: HealthSettings,
) -> str | None:
    """Rows of the latest successful run against the median of the runs before it."""
    per_run = (
        loads.filter(
            (pl.col("resource_id") == resource_id) & (pl.col("window_kind") == "increment")
        )
        .select("run_id", "rows")
        .join(successes.select("run_id", "at"), on="run_id")  # ordered by when the run ended
        .group_by("run_id", "at")
        .agg(pl.col("rows").sum())
        .sort("at")
    )
    if per_run.height <= _MIN_HISTORY:
        return None
    *history, latest = per_run["rows"].to_list()
    median = float(pl.Series(history).median())  # type: ignore[arg-type]
    if latest == 0 and median > 0 and empty != "allow":
        return f"no rows; usually {median:.0f}"
    if median > 0 and latest > median * settings.rows_spike:
        return f"{latest} rows; usually {median:.0f}"
    return None


def _reconciled(reconciliations: pl.DataFrame, resource_id: str) -> str | None:
    """Duplicate keys, or windows where the source counts differently, in the latest
    reconciliation of the resource."""
    own = reconciliations.filter(pl.col("resource_id") == resource_id)
    if own.is_empty():
        return None
    latest = own.filter(pl.col("reconcile_id") == own.sort("at")["reconcile_id"][-1])
    problems = []
    for whole in latest.filter(pl.col("window_kind") == "all").iter_rows(named=True):
        keys, rows = whole["distinct_keys"], whole["destination_rows"]
        if keys is not None and keys < rows:
            problems.append(f"{rows - keys} duplicate keys")
    differ = latest.filter(
        pl.col("source_rows").is_not_null() & (pl.col("source_rows") != pl.col("destination_rows"))
    ).sort("window_start", nulls_last=False)
    if differ.height:
        first = differ.row(0, named=True)
        span = ReconcileRecord.window(
            first["window_kind"], first["window_start"], first["window_end"]
        )
        problems.append(
            f"{differ.height} count(s) differ from the source, first {span}: "
            f"{first['destination_rows']} loaded, {first['source_rows']} upstream"
        )
    return "; ".join(problems) or None

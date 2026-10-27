"""The morning glance: KPIs against the period before, the run matrix, and each pipeline's
state, so a reader knows in seconds whether anything needs opening."""

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

import polars as pl

from dashboard.model.facts import Scope, View, bucket_of
from dashboard.units import age, change, human, seconds

OK = ("succeeded", "retried")


@dataclass(frozen=True, slots=True)
class Kpi:
    title: str
    value: str
    style: str
    trend: list[float | None]  # one value a bucket, for the card's sparkline
    note: str  # the change against the period before, or what it names


def kpis(view: View, count: int = 24) -> list[Kpi]:
    runs, before = view.runs, view.scope.previous().keep(view.history)
    ended = runs.filter(pl.col("outcome") != "running")
    ok = ended.filter(pl.col("outcome").is_in(OK)).height
    rate = ok / ended.height if ended.height else None
    failed = runs.filter(pl.col("outcome") == "failed")
    lost = runs.filter(pl.col("outcome") == "lost")
    stale = freshness(view).head(1).to_dicts()
    return [
        Kpi(
            "RUNS",
            f"{runs.height}",
            "info",
            per_bucket(view, runs, pl.len(), count),
            change(runs.height, before.height),
        ),
        Kpi(
            "SUCCESS",
            "-" if rate is None else f"{rate:.1%}",
            "ok" if rate is None or rate >= 0.99 else "warn" if rate >= 0.9 else "error",
            per_bucket(view, ended, pl.col("outcome").is_in(OK).mean(), count),
            f"{ok} of {ended.height} ended",
        ),
        Kpi(
            "FAILED",
            f"{failed.height}",
            "error" if failed.height else "ok",
            per_bucket(view, failed, pl.len(), count),
            _names(failed),
        ),
        Kpi(
            "LOST",
            f"{lost.height}",
            "error" if lost.height else "ok",
            per_bucket(view, lost, pl.len(), count),
            _names(lost),
        ),
        Kpi(
            "ROWS",
            human(_rows(runs)),
            "info",
            per_bucket(view, runs, pl.col("rows").sum(), count),
            change(_rows(runs), _rows(before)),
        ),
        Kpi(
            "OLDEST DATA",
            age(stale[0]["age"]) if stale else "-",
            _freshness_style(stale[0]["age"] if stale else timedelta(0)),
            [],
            stale[0]["pipeline_id"] if stale else "no success yet",
        ),
    ]


def per_bucket(view: View, runs: pl.DataFrame, measure: pl.Expr, count: int) -> list[float | None]:
    """``measure`` of the runs started in each of ``count`` buckets of the scope."""
    if runs.is_empty():
        return [None] * count
    found = dict(
        runs.group_by(bucket_of("started_at", view.scope, count).alias("bucket"))
        .agg(measure.alias("value"))
        .iter_rows()
    )
    return [found.get(index) for index in range(count)]


def matrix(view: View, count: int) -> list[tuple[str, list[str | None]]]:
    """Per pipeline, the worst outcome of the runs started in each bucket (``None``: no run)."""
    runs = view.runs
    out = []
    for pipeline in sorted(runs["pipeline_id"].unique().drop_nulls()):
        mine = runs.filter(pl.col("pipeline_id") == pipeline).with_columns(
            bucket=bucket_of("started_at", view.scope, count)
        )
        worst = dict(
            mine.group_by("bucket").agg(pl.col("outcome").sort_by("rank").first()).iter_rows()
        )
        out.append((pipeline, [worst.get(index) for index in range(count)]))
    return out


def freshness(view: View) -> pl.DataFrame:
    """Each pipeline's last success and its age at ``now``, oldest first: the data that is most
    behind. Pipelines with no success at all come first."""
    history = view.history
    if view.scope.pipelines:
        history = history.filter(pl.col("pipeline_id").is_in(list(view.scope.pipelines)))
    return (
        history.filter(pl.col("mode") == "run")
        .group_by("pipeline_id")
        .agg(last_success=pl.col("ended_at").filter(pl.col("outcome").is_in(OK)).max())
        .with_columns(age=pl.lit(view.now) - pl.col("last_success"))
        .sort("age", descending=True, nulls_last=False)
    )


def pipelines(view: View, points: int = 12) -> list[dict[str, Any]]:
    """One row per pipeline in scope: its latest run, its last ``points`` durations and rows, its
    peak memory share, and its failures in scope."""
    rows = []
    for pipeline, own in sorted(
        view.runs.group_by("pipeline_id"), key=lambda group: str(group[0][0])
    ):
        own = own.sort("started_at")
        latest = own.row(-1, named=True)
        rows.append(
            {
                "pipeline": pipeline[0],
                "outcome": latest["outcome"],
                "age": view.now - latest["started_at"],
                "durations": own["duration_s"].tail(points).to_list(),
                "rows": own["rows"].tail(points).to_list(),
                "memory": latest["memory_share"],
                "runs": own.height,
                "failures": own.filter(pl.col("outcome").is_in(["failed", "lost"])).height,
            }
        )
    return rows


def ticker(view: View, count: int = 8) -> list[tuple[datetime, str, str]]:
    """The latest run events, newest first: ``(when, outcome, text)``."""
    events = []
    for run in view.runs.head(count).iter_rows(named=True):
        rows = human(run["rows"] or 0)
        what = {
            "succeeded": f"succeeded, {rows} rows in {seconds(run['duration_s'])}",
            "retried": f"succeeded after retries, {rows} rows",
            "failed": f"failed in {run['error_step'] or 'run'}: {run['error_cause_type'] or ''}",
            "lost": "was killed (lost)",
            "running": f"running for {seconds(run['duration_s'])}",
        }[run["outcome"]]
        events.append(
            (
                run["started_at"],
                run["outcome"],
                f"{run['pipeline_id']} {what} (run {run['run_id'][:8]})",
            )
        )
    return events


def _rows(runs: pl.DataFrame) -> float:
    return float(runs.select(pl.col("rows").sum()).item() or 0)


def _freshness_style(behind: timedelta | None) -> str:
    """Red when a pipeline never succeeded, amber when its data is over a day old."""
    if behind is None:
        return "error"
    return "ok" if behind < timedelta(days=1) else "warn"


def _names(runs: pl.DataFrame) -> str:
    names = sorted(set(runs["pipeline_id"].drop_nulls()))
    return (
        ", ".join(names[:2]) + (f" +{len(names) - 2}" if len(names) > 2 else "")
        if names
        else "none"
    )


def scope_label(scope: Scope) -> str:
    return f"{scope.start:%Y-%m-%d %H:%M} → {scope.end:%Y-%m-%d %H:%M}Z"

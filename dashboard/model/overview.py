"""The morning glance: four numbers against the period before, and the run matrix."""

from dataclasses import dataclass
from datetime import timedelta

import polars as pl

from dashboard.model.facts import Scope, View, bucket_of
from dashboard.units import age, change, human

OK = ("succeeded", "retried")


@dataclass(frozen=True, slots=True)
class Kpi:
    title: str
    value: str
    style: str
    trend: list[float | None]  # one value a bucket, for the card's sparkline
    note: str  # the change against the period before, or what it names


def kpis(view: View, count: int = 24) -> list[Kpi]:
    """Four numbers, each against the period before: is it working, how much ran, how much
    landed, and how far behind the stalest data is."""
    runs, before = view.runs, view.scope.previous().keep(view.history)
    rate, ended = _success(runs), runs.filter(pl.col("outcome") != "running")
    stale = freshness(view).head(1).to_dicts()
    return [
        Kpi(
            "SUCCESS",
            "-" if rate is None else f"{rate:.1%}",
            "ok" if rate is None or rate >= 0.99 else "warn" if rate >= 0.9 else "error",
            per_bucket(view, ended, pl.col("outcome").is_in(OK).mean(), count),
            _points(rate, _success(before)),
        ),
        Kpi(
            "RUNS",
            f"{runs.height}",
            "bright",
            per_bucket(view, runs, pl.len(), count),
            change(runs.height, before.height),
        ),
        Kpi(
            "ROWS",
            human(_rows(runs)),
            "bright",
            per_bucket(view, runs, pl.col("rows").sum(), count),
            change(_rows(runs), _rows(before)),
        ),
        Kpi(
            "STALEST",
            age(stale[0]["age"]) if stale else "-",
            _freshness_style(stale[0]["age"] if stale else timedelta(0)),
            [],
            stale[0]["pipeline_id"] if stale else "no success yet",
        ),
    ]


def _success(runs: pl.DataFrame) -> float | None:
    ended = runs.filter(pl.col("outcome") != "running")
    return ended.filter(pl.col("outcome").is_in(OK)).height / ended.height if ended.height else None


def _points(now: float | None, before: float | None) -> str:
    """A rate's change in percentage points: ``▼1.2pt``."""
    if now is None or before is None:
        return "-"
    delta = (now - before) * 100
    return f"{'▲' if delta > 0.05 else '▼' if delta < -0.05 else '='}{abs(delta):.1f}pt"


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


def _rows(runs: pl.DataFrame) -> float:
    return float(runs.select(pl.col("rows").sum()).item() or 0)


def _freshness_style(behind: timedelta | None) -> str:
    """Red when a pipeline never succeeded, amber when its data is over a day old."""
    if behind is None:
        return "error"
    return "ok" if behind < timedelta(days=1) else "warn"


def scope_label(scope: Scope) -> str:
    return f"{scope.start:%m-%d %H:%M} → {scope.end:%m-%d %H:%M}Z"

"""Failures as patterns: one recurring bug or many problems, how long pipelines take to
recover, and the failed and lost runs themselves."""

from typing import Any

import polars as pl

from dashboard.model.facts import View

BROKEN = ("failed", "lost")


def failed(view: View) -> pl.DataFrame:
    """Failed and lost runs in scope, newest first, with every error field."""
    return view.runs.filter(pl.col("outcome").is_in(BROKEN))


def by_cause(view: View) -> pl.DataFrame:
    """Failures grouped by root cause and where they were raised: how often, in which
    pipelines, and when first and last. A lost run's cause is ``killed``."""
    return (
        failed(view)
        .with_columns(
            cause=pl.when(pl.col("outcome") == "lost")
            .then(pl.lit("killed"))
            .otherwise(pl.coalesce("error_cause_type", pl.lit("unknown"))),
            where=pl.coalesce("error_where", "error_step", pl.lit("-")),
        )
        .group_by("cause", "where")
        .agg(
            count=pl.len(),
            pipelines=pl.col("pipeline_id").unique().sort().str.join(", "),
            first=pl.col("started_at").min(),
            last=pl.col("started_at").max(),
        )
        .sort("count", "last", descending=True)
    )


def recovery(view: View) -> pl.DataFrame:
    """Per pipeline: how many failure streaks, the median time from a streak's first failure to
    the next success, and since when it has been failing if it still is."""
    rows = []
    for key, own in view.history.filter(pl.col("mode") == "run").group_by("pipeline_id"):
        streak_start = None
        recoveries = []
        for outcome, started, ended in (
            own.sort("started_at").select("outcome", "started_at", "ended_at").iter_rows()
        ):
            if outcome in BROKEN:
                streak_start = streak_start or started
            elif outcome in ("succeeded", "retried") and streak_start is not None:
                recoveries.append((ended - streak_start).total_seconds())
                streak_start = None
        rows.append(
            {
                "pipeline_id": str(key[0]),
                "streaks": len(recoveries) + (streak_start is not None),
                "median_recovery_s": pl.Series(recoveries, dtype=pl.Float64).median()
                if recoveries
                else None,
                "failing_since": streak_start,
            }
        )
    return pl.DataFrame(
        rows,
        schema={
            "pipeline_id": pl.String,
            "streaks": pl.Int64,
            "median_recovery_s": pl.Float64,
            "failing_since": pl.Datetime("us", "UTC"),
        },
    ).sort("failing_since", "streaks", descending=True, nulls_last=True)


def next_step(run: dict[str, Any]) -> str:
    """The command to run after reading a failure: rerun once the cause is fixed. A failed
    backfill window resumes from its logged edge."""
    pipeline = run["pipeline_id"]
    if run.get("mode") == "backfill" and run.get("error_window") and run.get("error_resource"):
        window = str(run["error_window"])
        start = window.split("[", 1)[-1].split(",", 1)[0].strip()
        return f"pipeline backfill {pipeline} --resource {run['error_resource']} --from {start}"
    return f"pipeline run {pipeline}"

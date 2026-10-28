"""How far behind each pipeline's data is."""

import polars as pl

from dashboard.model.facts import View

OK = ("succeeded", "retried")


def freshness(view: View) -> pl.DataFrame:
    """Each pipeline's last successful incremental run and its age at ``now``, oldest first:
    the data most behind. A pipeline that never succeeded has no age, and comes first."""
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

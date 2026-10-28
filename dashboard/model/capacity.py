"""Memory: how big a container each pipeline needs, from the peaks its runs reached."""

import polars as pl

from dashboard.model.facts import View

SIZING_MARGIN = 1.25  # recommended memory: the p99 peak plus a quarter
SIZING_STEP_MB = 256  # rounded up to this


def memory(view: View) -> pl.DataFrame:
    """Per pipeline: peak memory p50/p95/p99 and max, the limit it ran under (or the host's
    memory), its share, and a recommended size: p99 plus a margin, rounded up."""
    runs = view.runs.filter(pl.col("peak_rss_mb").is_not_null())
    if runs.is_empty():
        return pl.DataFrame(schema={"pipeline_id": pl.String, "p99_mb": pl.Float64})
    return (
        runs.group_by("pipeline_id")
        .agg(
            p50_mb=pl.col("peak_rss_mb").median(),
            p95_mb=pl.col("peak_rss_mb").quantile(0.95),
            p99_mb=pl.col("peak_rss_mb").quantile(0.99),
            max_mb=pl.col("peak_rss_mb").max(),
            limit_mb=pl.col("memory_limit_mb").drop_nulls().last(),
            host_mb=pl.col("host_memory_mb").drop_nulls().last(),
            runs=pl.len(),
        )
        .with_columns(available_mb=pl.coalesce("limit_mb", "host_mb"))
        .with_columns(
            share=pl.col("p99_mb") / pl.col("available_mb"),
            recommended_mb=(pl.col("p99_mb") * SIZING_MARGIN / SIZING_STEP_MB).ceil()
            * SIZING_STEP_MB,
        )
        .sort("share", descending=True, nulls_last=True)
    )

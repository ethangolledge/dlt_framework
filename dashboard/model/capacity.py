"""Memory, CPU, and concurrency: how big a container each pipeline needs, which step sets the
peak, how much CPU sits idle, and what runs at the same time."""

from dataclasses import replace
from datetime import timedelta

import polars as pl

from dashboard.model.facts import Scope, View, bucket_of, buckets

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


def memory_by_step(view: View) -> pl.DataFrame:
    """Per pipeline and step: the highest memory sampled while it ran. The step with the highest
    is the one to chunk or tune."""
    samples = view.samples.filter(pl.col("step").is_not_null()).join(
        view.runs.select("run_id", "pipeline_id"), on="run_id"
    )
    return (
        samples.group_by("pipeline_id", "step")
        .agg(peak_mb=pl.col("rss_mb").max(), p50_mb=pl.col("rss_mb").median())
        .sort("pipeline_id", "peak_mb", descending=[False, True])
    )


def cpu(view: View) -> pl.DataFrame:
    """Per pipeline: the share of the host's cores its runs used, median and p95 across samples.
    Low use while steps wait means room for more workers."""
    samples = view.samples.join(view.runs.select("run_id", "pipeline_id", "cpu_count"), on="run_id")
    return (
        samples.with_columns(used=pl.col("cpu_percent") / (pl.col("cpu_count").fill_null(1) * 100))
        .group_by("pipeline_id")
        .agg(
            p50=pl.col("used").median(),
            p95=pl.col("used").quantile(0.95),
            cores=pl.col("cpu_count").max(),
        )
        .sort("p95", descending=True)
    )


def fitted(view: View) -> Scope:
    """The scope narrowed to when its runs ran: an axis spans the runs, not the quiet after."""
    if view.runs.is_empty():
        return view.scope
    first, last = view.runs.select(pl.col("started_at").min(), pl.col("ended_at").max()).row(0)
    start = max(view.scope.start, first)
    end = min(view.scope.end, max(last or first, start + timedelta(minutes=1)))
    return replace(view.scope, start=start, end=end)


def concurrency(view: View, count: int) -> pl.DataFrame:
    """For each of ``count`` buckets of the scope: the most runs active at the same moment, and
    their summed peak memory then, against the host. A run counts only while it actually runs,
    so a bucket holding many short runs one after another shows one at a time, not all."""
    runs = view.runs.filter(pl.col("ended_at").is_not_null()).select(
        "started_at", "ended_at", pl.col("peak_rss_mb").fill_null(0.0)
    )
    host = view.runs["host_memory_mb"].drop_nulls().max()
    scope = fitted(view)
    starts = runs.with_columns(bucket=bucket_of("started_at", scope, count))
    rows = []
    for index, start in enumerate(buckets(scope, count)):
        most, memory = 0, 0.0
        for moment in starts.filter(pl.col("bucket") == index)[
            "started_at"
        ]:  # peaks begin at a start
            active = runs.filter((pl.col("started_at") <= moment) & (pl.col("ended_at") > moment))
            if active.height > most:
                most, memory = active.height, float(active["peak_rss_mb"].sum())
        if not most:  # nothing started here: what carried over from before
            active = runs.filter((pl.col("started_at") <= start) & (pl.col("ended_at") > start))
            most, memory = active.height, float(active["peak_rss_mb"].sum())
        rows.append({"start": start, "runs": most, "memory_mb": memory, "host_mb": host})
    return pl.DataFrame(
        rows,
        schema={
            "start": pl.Datetime("us", "UTC"),
            "runs": pl.Int64,
            "memory_mb": pl.Float64,
            "host_mb": pl.Float64,
        },
    )


def headroom(view: View, days: int) -> list[tuple[str, list[float | None]]]:
    """Per pipeline, its highest memory share on each of the last ``days`` days."""
    start = view.now.replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=days - 1)
    runs = view.history.filter(pl.col("started_at") >= start, pl.col("memory_share").is_not_null())
    out = []
    for key, own in sorted(runs.group_by("pipeline_id"), key=lambda group: str(group[0][0])):
        daily = dict(
            own.with_columns(
                day=((pl.col("started_at") - pl.lit(start)).dt.total_days()).cast(pl.Int64)
            )
            .group_by("day")
            .agg(pl.col("memory_share").max())
            .iter_rows()
        )
        out.append((str(key[0]), [daily.get(day) for day in range(days)]))
    return out

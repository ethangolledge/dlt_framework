"""Where time and memory go: memory per pipeline, steps busy or waiting, what holds each load
up, slow tables, retries, and what changed between code versions.

A high percentile is only shown as one with ``TOP_RUNS`` runs behind it; below that, a p95 or p99
is the maximum in disguise, so the maximum is shown and named (``top`` says which).
"""

import polars as pl

from dashboard.model.facts import OK, View

TOP_RUNS = 20
SIZING_MARGIN = 1.25  # recommended memory: the top peak plus a quarter
SIZING_STEP_MB = 256  # rounded up to this


def _top(column: str, quantile: float, name: str) -> list[pl.Expr]:
    """``name`` and ``top``: the quantile once there are ``TOP_RUNS`` values, else the max."""
    enough = pl.len() >= TOP_RUNS
    label = f"p{round(quantile * 100)}"
    return [
        pl.when(enough)
        .then(pl.col(column).quantile(quantile))
        .otherwise(pl.col(column).max())
        .alias(name),
        pl.when(enough).then(pl.lit(label)).otherwise(pl.lit("max")).alias("top"),
    ]


def memory(view: View) -> pl.DataFrame:
    """Per pipeline: peak memory p50, top and max, the limit it ran under (or the host's memory),
    its share, and a recommended size: the top peak plus a margin, rounded up."""
    runs = view.runs.filter(pl.col("peak_rss_mb").is_not_null())
    if runs.is_empty():
        return pl.DataFrame(schema={"pipeline_id": pl.String, "top_mb": pl.Float64})
    return (
        runs.group_by("pipeline_id")
        .agg(
            *_top("peak_rss_mb", 0.99, "top_mb"),
            p50_mb=pl.col("peak_rss_mb").median(),
            max_mb=pl.col("peak_rss_mb").max(),
            limit_mb=pl.col("memory_limit_mb").drop_nulls().last(),
            host_mb=pl.col("host_memory_mb").drop_nulls().last(),
            runs=pl.len(),
        )
        .with_columns(available_mb=pl.coalesce("limit_mb", "host_mb"))
        .with_columns(
            share=pl.col("top_mb") / pl.col("available_mb"),
            recommended_mb=(pl.col("top_mb") * SIZING_MARGIN / SIZING_STEP_MB).ceil()
            * SIZING_STEP_MB,
        )
        .sort("share", "max_mb", descending=True, nulls_last=True)
    )


def step_profile(view: View) -> pl.DataFrame:
    """Per pipeline and step: how long it usually takes (p50, top), its share of run time, how
    much of that was computing (``cpu_share``), and how often it was retried."""
    steps = view.steps.join(view.runs.select("run_id", "pipeline_id"), on="run_id")
    if steps.is_empty():
        return pl.DataFrame(schema={"pipeline_id": pl.String, "step": pl.String})
    per_run = steps.group_by("pipeline_id", "run_id", "step").agg(
        seconds=pl.col("duration_s").sum(),
        cpu=pl.col("cpu_s").sum(),
        attempts=pl.col("attempt").max(),
    )
    totals = per_run.group_by("pipeline_id").agg(total=pl.col("seconds").sum())
    return (
        per_run.group_by("pipeline_id", "step")
        .agg(
            *_top("seconds", 0.95, "top_s"),
            p50_s=pl.col("seconds").median(),
            seconds=pl.col("seconds").sum(),
            cpu_share=pl.col("cpu").sum() / pl.col("seconds").sum(),
            retried_runs=(pl.col("attempts") > 1).sum(),
            runs=pl.len(),
        )
        .join(totals, on="pipeline_id")
        .with_columns(share=pl.col("seconds") / pl.col("total"))
        .drop("total")
        .sort("pipeline_id", "seconds", descending=[False, True])
    )


def critical_path(view: View) -> pl.DataFrame:
    """Per pipeline and resource: in how many of the loads it shared with others it was the last
    to finish extracting, and its median share of those loads' extract time. The resource that
    usually finishes last holds the others up: split it out, or chunk it."""
    loads = view.loads.filter(pl.col("extract_first_at").is_not_null())
    shared = (
        loads.group_by("load_id").agg(pl.len().alias("resources")).filter(pl.col("resources") > 1)
    )
    loads = loads.join(shared, on="load_id")
    if loads.is_empty():
        return pl.DataFrame(
            schema={"pipeline_id": pl.String, "resource": pl.String, "last": pl.UInt32}
        )
    unit = loads.group_by("load_id").agg(
        unit_start=pl.col("extract_first_at").min(), unit_end=pl.col("extract_last_at").max()
    )
    return (
        loads.join(unit, on="load_id")
        .with_columns(
            last=pl.col("extract_last_at") == pl.col("unit_end"),
            share=(pl.col("extract_last_at") - pl.col("extract_first_at")).dt.total_seconds(
                fractional=True
            )
            / (pl.col("unit_end") - pl.col("unit_start")).dt.total_seconds(fractional=True),
        )
        .group_by("pipeline_id", "resource")
        .agg(last=pl.col("last").sum(), loads=pl.len(), share=pl.col("share").median())
        .filter(pl.col("last") > 0)
        .sort("last", descending=True)
    )


def slow_tables(view: View) -> pl.DataFrame:
    """Destination tables by load time: median and top seconds, rows a load second. A table that
    takes long for its rows (a merge without an index, a wide staging table) leads the list."""
    tables = view.tables.join(view.runs.select("run_id", "pipeline_id"), on="run_id")
    if tables.is_empty():
        return pl.DataFrame(
            schema={"pipeline_id": pl.String, "table": pl.String, "p50_s": pl.Float64}
        )
    return (
        tables.group_by("pipeline_id", "table")
        .agg(
            *_top("load_s", 0.95, "top_s"),
            p50_s=pl.col("load_s").median(),
            rows=pl.col("rows").median(),
            rows_per_s=pl.col("rows_per_load_s").median(),
            retries=pl.col("retries").sum(),
            loads=pl.len(),
        )
        .sort("top_s", descending=True, nulls_last=True)
    )


def by_version(view: View) -> pl.DataFrame:
    """Median duration and rows of successful runs per pipeline and code version, in the order
    versions first appeared: a step up after a release shows here."""
    ok = view.history.filter(pl.col("outcome").is_in(OK))
    return (
        ok.group_by("pipeline_id", "code_version")
        .agg(
            first=pl.col("started_at").min(),
            p50_s=pl.col("duration_s").median(),
            rows=pl.col("rows").median(),
            runs=pl.len(),
        )
        .sort("pipeline_id", "first")
    )

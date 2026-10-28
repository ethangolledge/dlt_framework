"""Where the time goes: steps busy or waiting, what holds each load up, slow tables,
retries, and what changed between code versions."""

import polars as pl

from dashboard.model.facts import View


def step_profile(view: View) -> pl.DataFrame:
    """Per pipeline and step: how long it usually takes (p50, p95), its share of run time, how
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
            p50_s=pl.col("seconds").median(),
            p95_s=pl.col("seconds").quantile(0.95),
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
    """Destination tables by load time: median and p95 seconds, rows a load second. A table that
    takes long for its rows (a merge without an index, a wide staging table) leads the list."""
    tables = view.tables.join(view.runs.select("run_id", "pipeline_id"), on="run_id")
    if tables.is_empty():
        return pl.DataFrame(
            schema={"pipeline_id": pl.String, "table": pl.String, "p50_s": pl.Float64}
        )
    return (
        tables.group_by("pipeline_id", "table")
        .agg(
            p50_s=pl.col("load_s").median(),
            p95_s=pl.col("load_s").quantile(0.95),
            rows=pl.col("rows").median(),
            rows_per_s=pl.col("rows_per_load_s").median(),
            retries=pl.col("retries").sum(),
            loads=pl.len(),
        )
        .sort("p95_s", descending=True, nulls_last=True)
    )


def by_version(view: View) -> pl.DataFrame:
    """Median duration and rows of successful runs per pipeline and code version, in the order
    versions first appeared: a step up after a release shows here."""
    ok = view.history.filter(pl.col("outcome").is_in(["succeeded", "retried"]))
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

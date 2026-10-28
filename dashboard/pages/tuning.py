"""Tuning: what would make the pipelines faster, cheaper, or steadier?

The checks' findings first, each with its evidence and how many runs it rests on, and the
change to make. Then the evidence itself, a panel for each kind the records hold (memory,
steps, tables, releases, what runs at once, a profile), and last the checks that could not
run, with the records they need.
"""

import polars as pl

from dashboard.model import capacity, checks, performance
from dashboard.model.checks import Finding, Report
from dashboard.model.facts import View
from dashboard.pages.base import Context, clear, header, nature_style, page, styled, two_lines
from dashboard.render.primitives import (
    Column,
    bar,
    bars,
    chart,
    gauge,
    legend,
    panel,
    share,
    side_by_side,
    table,
    time_axis,
)
from dashboard.render.text import Block, Line, join, span, vstack
from dashboard.theme import LEVELS, level_style
from dashboard.units import human, mb, seconds

ROWS = 10


@page("tuning", "Tuning")
def draw(context: Context) -> Block:
    view, size = context.view, context.size
    report = checks.run(view)
    modes = " and ".join(view.scope.modes) if view.scope.modes else "runs and backfills"
    return vstack(
        header("tuning", context, modes),
        findings(report.findings, size),
        sizing(view, size),
        steps(view, size),
        side_by_side([slow_tables(view, share(size, 2)), releases(view, share(size, 2))], size)
        if _has_both(view)
        else slow_tables(view, size) or releases(view, size),
        concurrency(view, size),
        profile(view, size),
        unchecked(report, size),
    )


def findings(found: list[Finding], size: int) -> Block:
    if not found:
        return clear("FINDINGS", size, "every check that could run found nothing to change")
    lines: Block = []
    for item in found:
        text, style = LEVELS[item.level]
        lines += two_lines(
            span(text[0], style),
            item.pipeline,
            join(span(item.title, style), span(f"   {item.evidence}", "dim")),
            span(f"→ {item.action}", styled("text", item.also or None)),
            size - 4,
        )
    worst = "warn" if any(item.level == "warning" for item in found) else "info"
    return panel("FINDINGS", lines, size, accent=worst)


def sizing(view: View, size: int) -> Block:
    found = capacity.memory(view)
    if found.is_empty() or "p50_mb" not in found.columns:
        return []
    columns = [
        Column("pipeline", 22),
        Column("runs", 5, "right"),
        Column("p50", 6, "right"),
        Column("p99", 6, "right"),
        Column("max", 6, "right"),
        Column("p99 of", 12),
        Column("", 26),
        Column("give it", 8, "right"),
    ]
    rows: list[list[Line | str]] = []
    for row in found.iter_rows(named=True):
        portion = row["share"]
        against = (
            f"{mb(row['limit_mb'])} limit" if row["limit_mb"] else f"{mb(row['host_mb'])} host"
        )
        rows.append(
            [
                row["pipeline_id"],
                str(row["runs"]),
                mb(row["p50_mb"]),
                mb(row["p99_mb"]),
                mb(row["max_mb"]),
                span(against if row["available_mb"] else "unknown", "muted"),
                gauge(portion, 26, level_style(portion)) if portion is not None else span(""),
                span(mb(row["recommended_mb"]), styled("accent", "p99 + 25%, rounded up")),
            ]
        )
    return panel("MEMORY", table(columns, rows), size)


def steps(view: View, size: int) -> Block:
    found = performance.step_profile(view)
    if found.is_empty() or "p50_s" not in found.columns:
        return []
    found = found.sort("p95_s", descending=True)
    columns = [
        Column("pipeline", 22),
        Column("step", 10),
        Column("runs", 5, "right"),
        Column("p50", 7, "right"),
        Column("p95", 7, "right"),
        Column("share of run", 21),
        Column("computing", 17),
        Column("retried", 7, "right"),
    ]
    rows: list[list[Line | str]] = []
    for row in found.head(ROWS).iter_rows(named=True):
        cpu = row["cpu_share"] or 0.0
        nature = "busy" if cpu >= 0.6 else "waiting" if cpu <= 0.2 else "mixed"
        rows.append(
            [
                row["pipeline_id"],
                row["step"],
                str(row["runs"]),
                seconds(row["p50_s"]),
                seconds(row["p95_s"]),
                join(bar(row["share"] or 0.0, 15, "muted"), span(f" {row['share'] or 0:>4.0%}")),
                join(bar(min(cpu, 1.0), 11, nature_style(nature)), span(f" {cpu:>4.0%}")),
                span(f"{row['retried_runs']}", "warn" if row["retried_runs"] else "dim"),
            ]
        )
    more = found.height - ROWS
    key = legend([("█", "accent", "busy"), ("█", "info", "waiting"), ("█", "warn", "mixed")])
    tail = [span(f"+{more} more", "dim")] if more > 0 else []
    return panel("STEPS", [*table(columns, rows), *tail, key], size)


def _has_both(view: View) -> bool:
    return bool(slow_tables(view, 60)) and bool(releases(view, 60))


def slow_tables(view: View, size: int) -> Block:
    found = performance.slow_tables(view)
    if found.is_empty() or "p95_s" not in found.columns:
        return []
    rows = [
        (
            row["table"],
            row["p95_s"] or 0.0,
            f"p95 {seconds(row['p95_s'])} · {human(row['rows_per_s'] or 0)}/s",
            "warn",
        )
        for row in found.head(8).iter_rows(named=True)
    ]
    return panel("SLOW TABLES", bars(rows, size - 4), size, accent="warn")


def releases(view: View, size: int) -> Block:
    """Median run per code version, where a pipeline has run more than one."""
    found = performance.by_version(view.mode("run"))
    several = found.group_by("pipeline_id").len().filter(pl.col("len") > 1)["pipeline_id"]
    found = found.filter(pl.col("pipeline_id").is_in(several.implode()))
    if found.is_empty():
        return []
    rows: list[list[Line | str]] = []
    for key, own in found.group_by("pipeline_id", maintain_order=True):
        before = None
        for row in own.iter_rows(named=True):
            delta: float | None = None if not before else (row["p50_s"] - before) / before
            rows.append(
                [
                    str(key[0]),
                    row["code_version"] or "-",
                    f"{row['first']:%m-%d %H:%M}",
                    str(row["runs"]),
                    seconds(row["p50_s"]),
                    span(
                        "" if delta is None else f"{delta:+.0%}",
                        "error" if delta is not None and delta > 0.2 else "muted",
                    ),
                ]
            )
            before = row["p50_s"]
    columns = [
        Column("pipeline", 18),
        Column("version", 9),
        Column("since", 11),
        Column("runs", 5, "right"),
        Column("p50", 7, "right"),
        Column("change", 7, "right"),
    ]
    return panel("RELEASES", table(columns, rows), size)


def concurrency(view: View, size: int) -> Block:
    """How many runs were active at once, and their peak memory summed against the host."""
    if view.runs.height < 2:
        return []
    half = share(size, 2)
    found = capacity.concurrency(view, half - 12)
    if found.select(pl.col("runs").max()).item() < 2:
        return []  # never two at once: nothing to see
    active = chart(
        [(found["runs"].cast(pl.Float64).to_list(), "info")],
        half - 4,
        5,
        label=lambda v: f"{v:.0f}",
    )
    host = found.select(pl.col("host_mb").max()).item()
    memory = chart(
        [(found["memory_mb"].to_list(), "accent")],
        half - 4,
        5,
        label=mb,
        limit=None if host is None else (float(host), "error"),
    )
    scope = capacity.fitted(view)
    axis = join(span(" " * 8), time_axis(scope.start, scope.end, half - 12))
    return side_by_side(
        [
            panel("RUNNING AT ONCE", [*active, axis], half),
            panel("THEIR MEMORY", [*memory, axis], half),
        ],
        size,
    )


def profile(view: View, size: int) -> Block:
    """The latest profiled run: its busiest functions per step."""
    profiles = view.frames.profiles.filter(pl.col("run_id").is_in(view.runs["run_id"].implode()))
    if profiles.is_empty():
        return []
    run_id = profiles.sort("at")["run_id"][-1]
    lines: Block = []
    for key, own in profiles.filter(pl.col("run_id") == run_id).group_by(
        "step", maintain_order=True
    ):
        rows = [
            (
                row["function"].split(":", 1)[-1],
                row["total_share"],
                f"{row['total_share']:.0%} total {row['own_share']:.0%} own",
                "accent" if row["file"].startswith("dlt_framework") else "info",
            )
            for row in own.sort("total_samples", descending=True).head(6).iter_rows(named=True)
        ]
        lines += [span(str(key[0]).upper(), "info"), *bars(rows, size - 4, label=44)]
    key_ = legend([("█", "accent", "this framework"), ("█", "info", "dlt and libraries")])
    return panel(f"PROFILE · run {run_id[:8]}", [*lines, key_], size)


def unchecked(report: Report, size: int) -> Block:
    if not report.skipped:
        return []
    lines = [
        join(span(f"{check.title:<56.56}", "muted"), span("needs " + ", ".join(lacks), "dim"))
        for check, lacks in report.skipped
    ]
    lines.append(span("older monitoring records lack these; runs since record them", "dim"))
    return panel("NOT CHECKED", lines, size, style="dim", accent="muted")

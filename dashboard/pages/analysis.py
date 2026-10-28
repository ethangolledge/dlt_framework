"""The pages for digging in, across pipelines: what to do, failures, completeness, speed,
capacity, and profiles.

Each opens with its findings, the answer, and then shows the evidence behind them.
"""

from statistics import median
from typing import Any

import polars as pl

from dashboard.model import capacity, completeness, failures, overview, performance, triage
from dashboard.model.facts import View
from dashboard.pages.base import (
    Context,
    clear,
    empty,
    nature_style,
    page,
    styled,
    to_pipeline,
    to_run,
    todo,
)
from dashboard.render.primitives import (
    Column,
    banner,
    bar,
    bars,
    chart,
    gauge,
    heatmap,
    legend,
    panel,
    share,
    side_by_side,
    sparkline,
    table,
)
from dashboard.render.text import Block, Line, join, span, vstack
from dashboard.theme import level_style
from dashboard.units import edge, first_line, human, mb, seconds

ROWS = 10  # rows a ranked panel lists before saying how many more


def findings(context: Context, *areas: str) -> Block:
    """The page's answer: what the triage found in its areas, with what to do."""
    found = [item for item in triage.actions(context.view, context.signals) if item.area in areas]
    if not found:
        return clear("FINDINGS", context.size, "nothing found")
    worst = "error" if any(item.level == "error" for item in found) else "warn"
    return panel("FINDINGS", todo(found, context.size - 4), context.size, accent=worst)


def _more(found: pl.DataFrame | list[Any]) -> Block:
    count = len(found) - ROWS
    return [span(f"+{count} more", "dim")] if count > 0 else []


def _scope(view: View) -> str:
    return overview.scope_label(view.scope)


# ---- actions -------------------------------------------------------------------------------


@page("actions", "Actions")
def draw_actions(context: Context) -> Block:
    view, size = context.view, context.size
    found = triage.actions(view, context.signals)
    fix = [item for item in found if item.level != "info"]
    tune = [item for item in found if item.level == "info"]
    return vstack(
        banner("actions", size, _scope(view)),
        panel("FIX", todo(fix, size - 4), size, accent="error" if fix else "ok")
        if fix
        else clear("FIX", size, "nothing is failing or at risk"),
        panel("TUNE", todo(tune, size - 4), size) if tune else [],
    )


# ---- failures ------------------------------------------------------------------------------


@page("failures", "Failures")
def draw_failures(context: Context) -> Block:
    view, size = context.view, context.size
    return vstack(
        banner("failures", size, _scope(view)),
        findings(context, "reliability"),
        causes(view, size),
        recovery(view, size),
        listing(view, size),
    )


def causes(view: View, size: int) -> Block:
    found = failures.by_cause(view)
    if found.is_empty():
        return clear("CAUSES", size, "nothing failed in this range")
    columns = [
        Column("count", 5, "right"),
        Column("cause", 24),
        Column("where", 52),
        Column("pipelines", 28),
        Column("last", 11),
    ]
    rows = [
        [
            span(str(row["count"]), "error"),
            row["cause"],
            span(row["where"], "muted"),
            row["pipelines"],
            f"{row['last']:%m-%d %H:%M}",
        ]
        for row in found.iter_rows(named=True)
    ]
    return panel("CAUSES", table(columns, rows), size, accent="error")


def recovery(view: View, size: int) -> Block:
    found = failures.recovery(view).filter(pl.col("streaks") > 0)
    if found.is_empty():
        return []
    columns = [
        Column("pipeline", 22),
        Column("streaks", 7, "right"),
        Column("recovers in", 12, "right"),
        Column("failing since", 20),
    ]
    rows: list[list[Line | str]] = [
        [
            span(row["pipeline_id"], styled("text", to=to_pipeline(row["pipeline_id"]))),
            str(row["streaks"]),
            seconds(row["median_recovery_s"]),
            span(
                f"{row['failing_since']:%Y-%m-%d %H:%M}Z" if row["failing_since"] else "recovered",
                "error" if row["failing_since"] else "ok",
            ),
        ]
        for row in found.iter_rows(named=True)
    ]
    return panel("RECOVERY", table(columns, rows), size)


def _cause(run: dict[str, Any]) -> str:
    if run["outcome"] == "lost":
        return "killed: no end was reported"
    return f"{run['error_cause_type']}: {first_line(run['error_message'])}"


def listing(view: View, size: int) -> Block:
    found = failures.failed(view)
    if found.is_empty():
        return []
    columns = [
        Column("started", 11),
        Column("pipeline", 18),
        Column("outcome", 7),
        Column("step", 9),
        Column("cause", 60),
        Column("run", 9),
    ]
    rows = [
        [
            f"{row['started_at']:%m-%d %H:%M}",
            row["pipeline_id"],
            span(row["outcome"], "error"),
            row["error_step"] or "-",
            span(_cause(row), "muted"),
            span(row["run_id"][:8], styled("info", "open this run", to_run(row["run_id"]))),
        ]
        for row in found.head(ROWS).iter_rows(named=True)
    ]
    return panel("FAILED RUNS", [*table(columns, rows), *_more(found)], size)


# ---- completeness --------------------------------------------------------------------------


@page("completeness", "Completeness")
def draw_completeness(context: Context) -> Block:
    view, size = context.view, context.size
    half = share(size, 2)
    return vstack(
        banner("completeness", size, "all history"),
        findings(context, "completeness"),
        coverage(view, size),
        breaks(view, size),
        side_by_side([reconciliation(view, half), doubles(view, half)], size),
    )


def coverage(view: View, size: int) -> Block:
    found = completeness.coverage(view, cells=max(20, size - 4 - 30 - 18))
    if not found:
        return empty("COVERAGE", size, "no resource declares a backfill")
    lines = []
    for item in found:
        ribbon = join(
            *(span("█" if cell else "░", "ok" if cell else "error") for cell in item.cells)
        )
        portion = "open" if item.fraction is None else f"{item.fraction:.0%}"
        lines.append(
            join(
                span(f"{item.resource_id:<28.28} ", "text"),
                span("▕", "dim"),
                ribbon,
                span("▏", "dim"),
                span(f" {portion:>5} {len(item.gaps)} gap(s)", "warn" if item.gaps else "ok"),
            )
        )
    key = legend([("█", "ok", "loaded"), ("░", "error", "missing")])
    return panel("COVERAGE", [*lines, key], size)


_BREAKS = {"hole": "error", "reset": "error", "reread": "warn"}


def breaks(view: View, size: int) -> Block:
    found = completeness.cursor_breaks(view)
    if not found:
        return clear("CURSOR", size, "every increment began where the one before ended")
    columns = [
        Column("when", 11),
        Column("resource", 28),
        Column("kind", 7),
        Column("ended at", 20),
        Column("next began at", 20),
    ]
    rows: list[list[Line | str]] = [
        [
            f"{item.at:%m-%d %H:%M}",
            item.resource_id,
            span(item.kind, _BREAKS[item.kind]),
            edge(item.previous_last),
            edge(item.next_start),
        ]
        for item in found[-ROWS:]
    ]
    key = legend(
        [
            ("hole", "error", "rows may be missing"),
            ("reread", "warn", "duplicates under Append"),
            ("reset", "error", "state went back"),
        ]
    )
    return panel("CURSOR", [*table(columns, rows), key], size, accent="error")


def reconciliation(view: View, size: int) -> Block:
    found = completeness.reconciled(view)
    if found.is_empty() or "duplicates" not in found.columns:
        return empty("RECONCILE", size, "run `pipeline reconcile NAME --source` to count")
    columns = [
        Column("resource", 24),
        Column("rows", 8, "right"),
        Column("source", 8, "right"),
        Column("dupes", 6, "right"),
        Column("off", 4, "right"),
    ]
    rows = [
        [
            row["resource_id"],
            human(row["rows"]),
            "-" if row["source_rows"] is None else human(row["source_rows"]),
            span(str(row["duplicates"] or 0), "error" if row["duplicates"] else "dim"),
            span(
                str(row["differing"]),
                styled("error" if row["differing"] else "dim", "windows off the source"),
            ),
        ]
        for row in found.iter_rows(named=True)
    ]
    return panel("RECONCILE", table(columns, rows), size)


def _window(row: dict[str, Any]) -> str:
    return f"[{edge(row['window_start'])}, {edge(row['window_end'])})"


def doubles(view: View, size: int) -> Block:
    twice, holes = completeness.double_loads(view), completeness.empty_windows(view)
    lines = [
        join(
            span("✖ ", "warn"),
            span(f"loaded {row['times']} times  ", "warn"),
            span(f"{row['resource_id']} {_window(row)}", "text"),
        )
        for row in twice.head(5).iter_rows(named=True)
    ]
    lines += [
        join(
            span("◌ ", "warn"),
            span("empty  ", styled("warn", "empty between windows that had rows")),
            span(f"{row['resource_id']} {_window(row)}", "text"),
        )
        for row in holes.head(5).iter_rows(named=True)
    ]
    if not lines:
        return clear("WINDOWS", size, "none loaded twice, none suspiciously empty")
    return panel("WINDOWS", lines, size, accent="warn")


# ---- speed ---------------------------------------------------------------------------------


@page("speed", "Speed")
def draw_speed(context: Context) -> Block:
    view, size = context.view, context.size
    half = share(size, 2)
    return vstack(
        banner("speed", size, _scope(view)),
        findings(context, "performance", "concurrency", "destination"),
        steps(view, size),
        side_by_side([durations(view, half), versions(view, half)], size),
        side_by_side([slow_tables(view, half), critical_path(view, half)], size),
        retries(view, size),
    )


def steps(view: View, size: int) -> Block:
    found = performance.step_profile(view)
    if found.is_empty() or "p50_s" not in found.columns:
        return empty("STEPS", size, "no steps recorded in this range")
    found = found.sort("p95_s", descending=True)
    columns = [
        Column("pipeline", 18),
        Column("step", 10),
        Column("p50", 7, "right"),
        Column("p95", 7, "right"),
        Column("share of run", 21),
        Column("computing", 17),
        Column("retried", 8, "right"),
    ]
    rows = []
    for row in found.head(ROWS).iter_rows(named=True):
        cpu = row["cpu_share"] or 0.0
        nature = "busy" if cpu >= 0.6 else "waiting" if cpu <= 0.2 else "mixed"
        rows.append(
            [
                row["pipeline_id"],
                row["step"],
                seconds(row["p50_s"]),
                seconds(row["p95_s"]),
                join(bar(row["share"] or 0.0, 15, "muted"), span(f" {row['share'] or 0:>4.0%}")),
                join(bar(min(cpu, 1.0), 11, nature_style(nature)), span(f" {cpu:>4.0%}")),
                span(
                    f"{row['retried_runs']}/{row['runs']}", "warn" if row["retried_runs"] else "dim"
                ),
            ]
        )
    key = legend([("█", "accent", "busy"), ("█", "info", "waiting"), ("█", "warn", "mixed")])
    return panel("STEPS", [*table(columns, rows), *_more(found), key], size)


def durations(view: View, size: int) -> Block:
    found = performance.durations(view)
    if not found:
        return empty("DURATION", size, "no runs in this range")
    rows = []
    for pipeline, values in sorted(found.items()):
        known = [value for value in values if value is not None]
        p50 = median(known) if known else None
        rows.append(
            join(
                span(f"{pipeline:<18.18}", "text"),
                sparkline(values, size - 4 - 18 - 18, "info"),
                span(f"  p50 {seconds(p50):>7}", "muted"),
            )
        )
    return panel("DURATION", rows, size)


def versions(view: View, size: int) -> Block:
    found = performance.by_version(view)
    if found.is_empty():
        return empty("RELEASES", size, "no successful runs yet")
    rows = []
    for key, own in found.group_by("pipeline_id", maintain_order=True):
        before = None
        for row in own.iter_rows(named=True):
            delta: float | None = None if not before else (row["p50_s"] - before) / before
            rows.append(
                [
                    str(key[0]),
                    row["code_version"] or "-",
                    f"{row['first']:%m-%d %H:%M}",
                    seconds(row["p50_s"]),
                    span(
                        "" if delta is None else f"{delta:+.0%}",
                        "error" if delta is not None and delta > 0.2 else "muted",
                    ),
                    str(row["runs"]),
                ]
            )
            before = row["p50_s"]
    columns = [
        Column("pipeline", 16),
        Column("version", 9),
        Column("since", 11),
        Column("p50", 7, "right"),
        Column("change", 7, "right"),
        Column("runs", 5, "right"),
    ]
    return panel("RELEASES", table(columns, rows), size)


def slow_tables(view: View, size: int) -> Block:
    found = performance.slow_tables(view)
    if found.is_empty() or "p95_s" not in found.columns:
        return empty("SLOW TABLES", size, "no destination tables in this range")
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


def critical_path(view: View, size: int) -> Block:
    found = performance.critical_path(view)
    if found.is_empty() or "loads" not in found.columns:
        return empty("FINISHES LAST", size, "no loads extracted several resources together")
    rows = [
        (
            f"{row['pipeline_id']}.{row['resource']}",
            row["last"] / row["loads"],
            f"last in {row['last']}/{row['loads']}",
            "accent",
        )
        for row in found.head(8).iter_rows(named=True)
    ]
    return panel("FINISHES LAST", bars(rows, size - 4, label=26), size, accent="accent")


def retries(view: View, size: int) -> Block:
    found = performance.retries(view)
    if found.is_empty():
        return clear("RETRIES", size, "no step needed a retry")
    columns = [
        Column("pipeline", 16),
        Column("step", 9),
        Column("cause", 16),
        Column("tries", 5, "right"),
        Column("runs", 5, "right"),
        Column("last", 11),
    ]
    rows = [
        [
            row["pipeline_id"],
            row["step"],
            span(row["error_cause_type"] or "-", "warn"),
            str(row["attempts"]),
            str(row["runs"]),
            f"{row['last']:%m-%d %H:%M}",
        ]
        for row in found.head(8).iter_rows(named=True)
    ]
    return panel("RETRIES", table(columns, rows), size, accent="warn")


# ---- capacity ------------------------------------------------------------------------------


@page("capacity", "Capacity")
def draw_capacity(context: Context) -> Block:
    view, size = context.view, context.size
    half = share(size, 2)
    return vstack(
        banner("capacity", size, _scope(view)),
        findings(context, "memory"),
        sizing(view, size),
        headroom(view, size),
        side_by_side([by_step(view, half), cpu(view, half)], size),
        concurrency(view, size),
    )


def sizing(view: View, size: int) -> Block:
    found = capacity.memory(view)
    if found.is_empty() or "p50_mb" not in found.columns:
        return empty("MEMORY", size, "no peaks recorded in this range")
    columns = [
        Column("pipeline", 18),
        Column("p50", 6, "right"),
        Column("p99", 6, "right"),
        Column("max", 6, "right"),
        Column("p99 of limit", 26),
        Column("give it", 8, "right"),
    ]
    rows = []
    for row in found.iter_rows(named=True):
        portion = row["share"]
        limit = mb(row["available_mb"]) + ("" if row["limit_mb"] else " (host)")
        rows.append(
            [
                row["pipeline_id"],
                mb(row["p50_mb"]),
                mb(row["p99_mb"]),
                mb(row["max_mb"]),
                gauge(portion, 26, styled(level_style(portion), f"of {limit}"))
                if portion is not None
                else span("-", "muted"),
                span(mb(row["recommended_mb"]), styled("accent", "p99 + 25%, rounded up")),
            ]
        )
    return panel("MEMORY", table(columns, rows), size)


def by_step(view: View, size: int) -> Block:
    found = capacity.memory_by_step(view)
    if found.is_empty():
        return empty("BY STEP", size, "no samples carried a step in this range")
    rows = [
        (f"{row['pipeline_id']} {row['step']}", row["peak_mb"], mb(row["peak_mb"]), "muted")
        for row in found.sort("peak_mb", descending=True).head(8).iter_rows(named=True)
    ]
    return panel("BY STEP", bars(rows, size - 4, label=26), size)


def cpu(view: View, size: int) -> Block:
    found = capacity.cpu(view)
    if found.is_empty():
        return empty("CPU", size, "no samples in this range")
    rows = [
        join(
            span(f"{row['pipeline_id']:<18.18}", "text"),
            gauge(min(row["p95"] or 0.0, 1.0), size - 40, level_style(row["p95"] or 0.0)),
            span(f"  of {row['cores']} cores", "muted"),
        )
        for row in found.iter_rows(named=True)
    ]
    return panel("CPU", rows, size)


def headroom(view: View, size: int) -> Block:
    days = 14
    found = capacity.headroom(view, days)
    if not found:
        return empty("HEADROOM", size, "no memory shares recorded")
    body = heatmap(found, lambda value: level_style(value), label=18)
    key = legend([("█", "ok", "under 70%"), ("█", "warn", "70-90%"), ("█", "error", "over 90%")])
    return panel(f"HEADROOM · {days} DAYS", [*body, key], size)


def concurrency(view: View, size: int) -> Block:
    if view.runs.is_empty():
        return []
    half = share(size, 2)
    found = capacity.concurrency(view, size - 4 - 8)
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
        label=lambda v: mb(v),
        limit=None if host is None else (float(host), "error"),
    )
    return side_by_side(
        [
            panel("RUNNING AT ONCE", active, half),
            panel("THEIR MEMORY", memory, half, accent="accent"),
        ],
        size,
    )


# ---- profile -------------------------------------------------------------------------------


@page("profile", "Profile", picks="run")
def draw_profile(context: Context) -> Block:
    view, size = context.view, context.size
    profiles = view.frames.profiles
    if context.run_id:
        profiles = profiles.filter(pl.col("run_id") == context.run_id)
    if profiles.is_empty():
        return vstack(
            banner("profile", size),
            empty("PROFILE", size, "no profiled run: `pipeline run NAME --profile` records one"),
        )
    run_id = profiles.sort("at")["run_id"][-1]
    profile = profiles.filter(pl.col("run_id") == run_id)
    blocks = []
    for key, own in profile.group_by("step", maintain_order=True):
        own = own.sort("total_samples", descending=True).head(12)
        rows = [
            (
                row["function"].split(":", 1)[-1],
                row["total_share"],
                f"{row['total_share']:.0%} total {row['own_share']:.0%} own",
                "accent" if row["file"].startswith("dlt_framework") else "info",
            )
            for row in own.iter_rows(named=True)
        ]
        blocks.append(panel(str(key[0]).upper(), bars(rows, size - 4, label=48), size))
    colours = legend([("█", "accent", "this framework"), ("█", "info", "dlt and libraries")])
    return vstack(banner("profile", size, f"run {run_id[:8]}"), *blocks, [colours])

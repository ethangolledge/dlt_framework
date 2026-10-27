"""The pages for digging in: performance, capacity, completeness, failures, profiles, and the
advisor that turns them into changes."""

from statistics import median
from typing import Any

import polars as pl

from dashboard.model import advisor, capacity, completeness, failures, performance
from dashboard.model.facts import View
from dashboard.pages.base import Context, empty, level, nature_style, page
from dashboard.render.primitives import (
    Column,
    banner,
    bar,
    bars,
    gauge,
    heatmap,
    legend,
    panel,
    share,
    side_by_side,
    sparkline,
    table,
    wrap,
)
from dashboard.render.text import Block, Line, join, span, vstack
from dashboard.theme import level_style
from dashboard.units import edge, first_line, human, mb, seconds

# ---- performance ---------------------------------------------------------------------------


@page("performance", "Performance", "Where does the time go, and why: computing or waiting?")
def draw_performance(context: Context) -> Block:
    view, size = context.view, context.size
    half = share(size, 2)
    return vstack(
        banner("performance", size, "busy computes · waiting waits on upstream or the destination"),
        steps(view, size),
        side_by_side([durations(view, half), versions(view, half)], size),
        side_by_side([slow_tables(view, half), critical_path(view, half)], size),
        side_by_side([throughput(view, half), retries(view, half)], size),
    )


def steps(view: View, size: int) -> Block:
    found = performance.step_profile(view)
    if found.is_empty() or "p50_s" not in found.columns:
        return empty("STEPS", size, "no steps recorded in this range")
    columns = [
        Column("pipeline", 18),
        Column("step", 10),
        Column("p50", 7, "right"),
        Column("p95", 7, "right"),
        Column("share of run", 22),
        Column("computing", 18),
        Column("nature", 8),
        Column("retried", 8, "right"),
    ]
    rows = []
    for row in found.iter_rows(named=True):
        cpu = row["cpu_share"] or 0.0
        nature = "busy" if cpu >= 0.6 else "waiting" if cpu <= 0.2 else "mixed"
        rows.append(
            [
                row["pipeline_id"],
                row["step"],
                seconds(row["p50_s"]),
                seconds(row["p95_s"]),
                join(
                    bar(row["share"] or 0.0, 15, "info"),
                    span(f" {row['share'] or 0:>4.0%}", "muted"),
                ),
                join(bar(min(cpu, 1.0), 11, nature_style(nature)), span(f" {cpu:>4.0%}", "muted")),
                span(nature, nature_style(nature)),
                span(
                    f"{row['retried_runs']}/{row['runs']}", "warn" if row["retried_runs"] else "dim"
                ),
            ]
        )
    key = legend(
        [
            ("█", "accent", "busy: our code or dlt computing"),
            ("█", "info", "waiting: upstream or destination"),
            ("█", "warn", "mixed"),
        ]
    )
    return panel("STEPS ─ per pipeline, how long and how busy", [*table(columns, rows), key], size)


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
                sparkline(values, size - 4 - 18 - 30, "info"),
                span(
                    f"  p50 {seconds(p50):>7}  max {seconds(max(known) if known else None):>7}",
                    "muted",
                ),
            )
        )
    return panel("DURATION ─ each run, oldest to newest", rows, size)


def versions(view: View, size: int) -> Block:
    found = performance.by_version(view)
    if found.is_empty():
        return empty("BY RELEASE", size, "no successful runs yet")
    rows = []
    for key, own in found.group_by("pipeline_id", maintain_order=True):
        before = None
        for row in own.iter_rows(named=True):
            change = (
                "" if before is None or not before else f"{(row['p50_s'] - before) / before:+.0%}"
            )
            style = (
                "error"
                if change.startswith("+") and abs((row["p50_s"] - before) / before) > 0.2
                else "muted"
            )
            rows.append(
                [
                    str(key[0]),
                    row["code_version"] or "-",
                    f"{row['first']:%m-%d %H:%M}",
                    seconds(row["p50_s"]),
                    span(change, style),
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
    return panel("BY RELEASE ─ median duration per code version", table(columns, rows), size)


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
    return panel(
        "SLOW TABLES ─ load seconds (p95), rows a second", bars(rows, size - 4), size, accent="warn"
    )


def critical_path(view: View, size: int) -> Block:
    found = performance.critical_path(view)
    if found.is_empty() or "loads" not in found.columns:
        return empty("CRITICAL PATH", size, "no loads extracted several resources together")
    rows = [
        (
            f"{row['pipeline_id']}.{row['resource']}",
            row["last"] / row["loads"],
            f"last in {row['last']}/{row['loads']}",
            "accent",
        )
        for row in found.head(8).iter_rows(named=True)
    ]
    return panel(
        "CRITICAL PATH ─ which resource finishes extracting last",
        bars(rows, size - 4, label=26),
        size,
        accent="accent",
    )


def throughput(view: View, size: int) -> Block:
    found = performance.throughput(view)
    if found.is_empty() or "trend" not in found.columns:
        return empty("THROUGHPUT", size, "no increments in this range")
    rows = [
        join(
            span(f"{row['resource_id']:<26.26}", "text"),
            sparkline(row["trend"], size - 4 - 26 - 26, "ok"),
            span(f" {human(row['rows_per_s'] or 0):>6}/s  {human(row['rows']):>6} rows", "muted"),
        )
        for row in found.head(10).iter_rows(named=True)
    ]
    return panel("THROUGHPUT ─ rows a second of each resource's own extract", rows, size)


def retries(view: View, size: int) -> Block:
    found = performance.retries(view)
    if found.is_empty():
        return panel("RETRIES", [span("✔ no step needed a retry in this range", "ok")], size)
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
    return panel(
        "RETRIES ─ failed attempts that were tried again", table(columns, rows), size, accent="warn"
    )


# ---- capacity ------------------------------------------------------------------------------


@page(
    "capacity",
    "Capacity",
    "How much memory and CPU, and what runs at once? How big should containers be?",
)
def draw_capacity(context: Context) -> Block:
    view, size = context.view, context.size
    half = share(size, 2)
    return vstack(
        banner("capacity", size, "sizes from p99 peak + 25%"),
        sizing(view, size),
        side_by_side([by_step(view, half), cpu(view, half)], size),
        headroom(view, size),
    )


def sizing(view: View, size: int) -> Block:
    found = capacity.memory(view)
    if found.is_empty() or "p50_mb" not in found.columns:
        return empty("MEMORY", size, "no peaks recorded in this range")
    columns = [
        Column("pipeline", 18),
        Column("p50", 6, "right"),
        Column("p95", 6, "right"),
        Column("p99", 6, "right"),
        Column("max", 6, "right"),
        Column("of", 6, "right"),
        Column("p99 share", 26),
        Column("give it", 8, "right"),
        Column("runs", 5, "right"),
    ]
    rows = []
    for row in found.iter_rows(named=True):
        portion = row["share"]
        rows.append(
            [
                row["pipeline_id"],
                mb(row["p50_mb"]),
                mb(row["p95_mb"]),
                mb(row["p99_mb"]),
                mb(row["max_mb"]),
                mb(row["available_mb"]) + ("" if row["limit_mb"] else "*"),
                gauge(portion, 26, level_style(portion))
                if portion is not None
                else span("-", "muted"),
                span(mb(row["recommended_mb"]), "accent"),
                str(row["runs"]),
            ]
        )
    note = span("* no container limit: against the host's memory", "dim")
    return panel(
        "MEMORY ─ peak per run, against what it ran under", [*table(columns, rows), note], size
    )


def by_step(view: View, size: int) -> Block:
    found = capacity.memory_by_step(view)
    if found.is_empty():
        return empty("MEMORY BY STEP", size, "no samples carried a step in this range")
    rows = [
        (
            f"{row['pipeline_id']} {row['step']}",
            row["peak_mb"],
            mb(row["peak_mb"]),
            nature_style({"normalize": "busy", "extract": "waiting"}.get(row["step"])),
        )
        for row in found.iter_rows(named=True)
    ]
    return panel(
        "MEMORY BY STEP ─ the step that sets the peak", bars(rows, size - 4, label=26), size
    )


def cpu(view: View, size: int) -> Block:
    found = capacity.cpu(view)
    if found.is_empty():
        return empty("CPU", size, "no samples in this range")
    rows = [
        join(
            span(f"{row['pipeline_id']:<18.18}", "text"),
            gauge(min(row["p95"] or 0.0, 1.0), size - 40, level_style(row["p95"] or 0.0)),
            span(f"  p50 {row['p50'] or 0:.0%} of {row['cores']} cores", "muted"),
        )
        for row in found.iter_rows(named=True)
    ]
    return panel("CPU ─ p95 share of the host's cores in use", rows, size)


def headroom(view: View, size: int) -> Block:
    days = 14
    found = capacity.headroom(view, days)
    if not found:
        return empty("HEADROOM", size, "no memory shares recorded")
    body = heatmap(found, lambda value: level_style(value), label=18)
    key = legend(
        [
            ("░▒▓█", "text", "fuller as it darkens;"),
            ("█", "ok", "under 70%"),
            ("█", "warn", "70 to 90%"),
            ("█", "error", "over 90%"),
            ("·", "dim", "no run"),
        ]
    )
    return panel(f"HEADROOM ─ highest memory share each day, last {days} days", [*body, key], size)


# ---- completeness --------------------------------------------------------------------------


@page(
    "completeness",
    "Completeness",
    "Is all the data there, exactly once? Gaps, holes, duplicates, mismatches.",
)
def draw_completeness(context: Context) -> Block:
    view, size = context.view, context.size
    half = share(size, 2)
    return vstack(
        banner("completeness", size, "all of history, whatever the range"),
        coverage(view, size),
        breaks(view, size),
        side_by_side([reconciliation(view, half), doubles(view, half)], size),
    )


def coverage(view: View, size: int) -> Block:
    found = completeness.coverage(view, cells=max(20, size - 4 - 30 - 18))
    if not found:
        return empty("BACKFILL COVERAGE", size, "no resource declares a backfill")
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
        for command in item.commands[:3]:
            lines.append(join(span("    $ ", "accent"), span(command, "muted")))
    key = legend([("█", "ok", "loaded"), ("░", "error", "missing")])
    return panel(
        "BACKFILL COVERAGE ─ declared history, and how to fill each gap", [*lines, key], size
    )


def breaks(view: View, size: int) -> Block:
    found = completeness.cursor_breaks(view)
    if not found:
        return panel(
            "CURSOR ─ consecutive increments",
            [span("✔ every increment began where the one before ended", "ok")],
            size,
        )
    styles = {"hole": "error", "reset": "error", "reread": "warn"}
    meaning = {
        "hole": "skipped ahead: rows in between may be missing",
        "reset": "went back: state reset",
        "reread": "re-read: duplicates under Append",
    }
    columns = [
        Column("when", 11),
        Column("resource", 28),
        Column("kind", 7),
        Column("ended at", 20),
        Column("next began at", 20),
        Column("meaning", 44),
    ]
    rows: list[list[Line | str]] = [
        [
            f"{item.at:%m-%d %H:%M}",
            item.resource_id,
            span(item.kind, styles[item.kind]),
            edge(item.previous_last),
            edge(item.next_start),
            span(meaning[item.kind], "muted"),
        ]
        for item in found[-12:]
    ]
    return panel(
        "CURSOR ─ increments that did not meet", table(columns, rows), size, accent="error"
    )


def reconciliation(view: View, size: int) -> Block:
    found = completeness.reconciled(view)
    if found.is_empty() or "duplicates" not in found.columns:
        return empty("RECONCILIATION", size, "run `pipeline reconcile NAME --source` to count")
    columns = [
        Column("resource", 24),
        Column("rows", 8, "right"),
        Column("source", 8, "right"),
        Column("dupes", 6, "right"),
        Column("windows off", 11, "right"),
    ]
    rows = [
        [
            row["resource_id"],
            human(row["rows"]),
            "-" if row["source_rows"] is None else human(row["source_rows"]),
            span(str(row["duplicates"] or 0), "error" if row["duplicates"] else "dim"),
            span(str(row["differing"]), "error" if row["differing"] else "dim"),
        ]
        for row in found.iter_rows(named=True)
    ]
    return panel("RECONCILIATION ─ latest count, against the source", table(columns, rows), size)


def _cause(run: dict[str, Any]) -> str:
    if run["outcome"] == "lost":
        return "killed: no end was reported"
    return f"{run['error_cause_type']}: {first_line(run['error_message'])}"


def _window(row: dict[str, Any]) -> str:
    return f"[{edge(row['window_start'])}, {edge(row['window_end'])})"


def doubles(view: View, size: int) -> Block:
    twice, holes = completeness.double_loads(view), completeness.empty_windows(view)
    lines = [
        join(
            span("✖ ", "warn"),
            span(f"{row['resource_id']} {_window(row)} loaded {row['times']} times", "text"),
        )
        for row in twice.head(5).iter_rows(named=True)
    ]
    lines += [
        join(
            span("◌ ", "warn"),
            span(f"{row['resource_id']} {_window(row)} empty between full windows", "text"),
        )
        for row in holes.head(5).iter_rows(named=True)
    ]
    if not lines:
        lines = [span("✔ no window loaded twice, none suspiciously empty", "ok")]
    return panel("WINDOWS ─ loaded twice, or empty between full ones", lines, size, accent="warn")


# ---- failures ------------------------------------------------------------------------------


@page("failures", "Failures", "One recurring bug, or many problems? How fast do pipelines recover?")
def draw_failures(context: Context) -> Block:
    view, size = context.view, context.size
    return vstack(
        banner("failures", size, f"{failures.failed(view).height} in range"),
        causes(view, size),
        recovery(view, size),
        listing(view, size),
    )


def causes(view: View, size: int) -> Block:
    found = failures.by_cause(view)
    if found.is_empty():
        return panel("BY CAUSE", [span("✔ nothing failed in this range", "ok")], size)
    columns = [
        Column("count", 5, "right"),
        Column("cause", 30),
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
    return panel(
        "BY CAUSE ─ root cause, and the line it was raised at",
        table(columns, rows),
        size,
        accent="error",
    )


def recovery(view: View, size: int) -> Block:
    found = failures.recovery(view)
    columns = [
        Column("pipeline", 22),
        Column("streaks", 7, "right"),
        Column("recovers in", 12, "right"),
        Column("failing since", 20),
    ]
    rows = [
        [
            row["pipeline_id"],
            str(row["streaks"]),
            seconds(row["median_recovery_s"]),
            span(
                f"{row['failing_since']:%Y-%m-%d %H:%M}Z" if row["failing_since"] else "-",
                "error" if row["failing_since"] else "dim",
            ),
        ]
        for row in found.iter_rows(named=True)
    ]
    return panel(
        "RECOVERY ─ from the first failure to the next success", table(columns, rows), size
    )


def listing(view: View, size: int) -> Block:
    found = failures.failed(view)
    if found.is_empty():
        return []
    columns = [
        Column("started", 14),
        Column("pipeline", 18),
        Column("outcome", 7),
        Column("step", 9),
        Column("resource", 18),
        Column("cause", 60),
        Column("run", 9),
    ]
    rows = [
        [
            f"{row['started_at']:%m-%d %H:%M:%S}",
            row["pipeline_id"],
            span(row["outcome"], "error"),
            row["error_step"] or "-",
            row["error_resource"] or "-",
            span(_cause(row), "muted"),
            span(row["run_id"][:8], "muted"),
        ]
        for row in found.head(30).iter_rows(named=True)
    ]
    return panel("FAILED AND LOST RUNS ─ open one on the Run page", table(columns, rows), size)


# ---- profile -------------------------------------------------------------------------------


@page(
    "profile",
    "Profile",
    "Where did a profiled run spend its time? (`pipeline run NAME --profile`)",
    picks_run=True,
)
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
        blocks.append(
            panel(
                f"{str(key[0]).upper()} ─ functions by share of the step's samples",
                bars(rows, size - 4, label=48),
                size,
            )
        )
    colours = legend([("█", "accent", "this framework's code"), ("█", "info", "dlt and libraries")])
    return vstack(banner("profile", size, f"run {run_id}"), *blocks, [colours])


# ---- advisor -------------------------------------------------------------------------------


@page(
    "advisor",
    "Advisor",
    "What to change: concurrency, memory, the destination, completeness, reliability.",
)
def draw_advisor(context: Context) -> Block:
    view, size = context.view, context.size
    found = advisor.advise(view)
    if not found:
        return vstack(
            banner("advisor", size),
            panel(
                "ADVISOR",
                [span("✔ nothing to change: every rule is within its threshold", "ok")],
                size,
            ),
        )
    blocks = []
    for area in ("completeness", "memory", "concurrency", "destination", "reliability"):
        mine = [item for item in found if item.area == area]
        if not mine:
            continue
        lines = []
        for item in mine:
            lines.append(
                join(
                    level(item.level),
                    span(f"  {item.pipeline}  ", "bright"),
                    span(item.finding, "text"),
                )
            )
            for label, text, style in (
                ("evidence", item.evidence, "dim"),
                ("do", item.action, "ok"),
            ):
                body = wrap(text, size - 4 - 17, style)
                head = span(f"        {label:<9}", "accent" if label == "do" else "muted")
                lines.append(join(head, body[0]))
                lines.extend(join(span(" " * 17, None), line) for line in body[1:])
        blocks.append(
            panel(
                area.upper(),
                lines,
                size,
                accent="warn" if any(item.level == "warning" for item in mine) else "info",
            )
        )
    return vstack(banner("advisor", size, f"{len(found)} finding(s)"), *blocks)

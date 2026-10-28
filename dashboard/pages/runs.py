"""Runs: what is this pipeline doing, and what happened in this run?

One pipeline, chosen from tabs naming every pipeline (worst first). Its state; its runs,
newest first, the chosen one marked; and that run in full beneath: what broke and the command
to run, its steps, memory, what it loaded, and the error's chain and traceback. By default the
run chosen is the latest of an open incident, else the latest run.
"""

from datetime import timedelta
from typing import Any

import polars as pl

from dashboard.model import checks, gaps, incidents, overview, triage
from dashboard.model.facts import View
from dashboard.pages.base import (
    STATES,
    Context,
    ago,
    empty,
    header,
    nature_style,
    outcome,
    outcome_word,
    page,
    styled,
    tabs,
    to_run,
)
from dashboard.render.primitives import (
    Column,
    chart,
    gantt,
    gauge,
    legend,
    panel,
    resample,
    sparkline,
    table,
)
from dashboard.render.text import Block, Line, fit, hstack, join, span, vstack
from dashboard.theme import level_style
from dashboard.units import edge, human, mb, seconds

LISTED = 15  # runs listed before "older"
TREND = 40  # runs the trend sparklines cover


@page("runs", "Runs", picks="pipeline")
def draw(context: Context) -> Block:
    view, size = context.view, context.size
    production = view.mode("run") if not view.scope.modes else view
    found = incidents.incidents(production)
    report = checks.run(production)
    states = triage.statuses(production, found, report, gaps.holes(production), context.signals)
    names = [(status.pipeline, status.level) for status in states]
    run = view.run(context.run_id) if context.run_id else None
    name = context.focus or (run and run["pipeline_id"]) or (names[0][0] if names else None)
    if name is None:
        return vstack(header("runs", context, "runs"), empty("RUNS", size, "no pipelines"))
    status = next((item for item in states if item.pipeline == name), None)
    mine = production.only(name)
    run = run or _default(mine, status)
    return vstack(
        header("runs", context, "runs"),
        [tabs(names, name, size), []],
        state(mine, status, size),
        listing(mine, run, size),
        detail(view, run, size) if run else [],
    )


def _default(view: View, status: triage.Status | None) -> dict[str, Any] | None:
    """The latest run of the open incident, else the latest run."""
    if status and status.incident:
        return status.incident.latest
    return view.runs.row(0, named=True) if view.runs.height else None


def state(view: View, status: triage.Status | None, size: int) -> Block:
    """Its state in a line, and what each concern rests on."""
    level = status.level if status else "idle"
    glyph, style, word = STATES[level]
    ended = view.runs.filter(pl.col("outcome") != "running")
    good = ended.filter(pl.col("outcome").is_in(incidents.OK)).height
    fresh = overview.freshness(view).to_dicts()
    last_ok = ago(fresh[0]["age"]) if fresh else "never"
    lines = [
        join(
            span(f"{glyph} {word}  ", style),
            span(f"{good} of {ended.height} runs ok in range · last success {last_ok}", "muted"),
        )
    ]
    for concern in status.concerns if status else ():
        tone = "error" if concern.level == "error" else "warn"
        lines.append(join(span(f"  {concern.headline}", tone), span(f"   {concern.detail}", "dim")))
    return [*(fit(line, size) for line in lines), []]


def listing(view: View, chosen: dict[str, Any] | None, size: int) -> Block:
    runs = view.runs
    if runs.is_empty():
        return empty("RUNS", size, "no runs of this pipeline in range")
    columns = [
        Column("", 1),
        Column("started", 11),
        Column("outcome", 12),
        Column("took", 7, "right"),
        Column("rows", 7, "right"),
        Column("peak", 6, "right"),
        Column("version", 8),
        Column("cause", max(20, size - 4 - 74)),
        Column("run", 8),
    ]
    picked = chosen["run_id"] if chosen else None
    rows = [
        [
            span("▶" if run["run_id"] == picked else " ", "accent"),
            f"{run['started_at']:%m-%d %H:%M}",
            outcome_word(run["outcome"]),
            seconds(run["duration_s"]),
            "-" if run["rows"] is None else human(run["rows"]),
            span(mb(run["peak_rss_mb"]), level_style(run["memory_share"] or 0)),
            run["code_version"] or "-",
            span(incidents.detail(run) if run["outcome"] in incidents.BROKEN else "", "muted"),
            span(
                run["run_id"][:8],
                styled("info", "open this run", to_run(run["pipeline_id"], run["run_id"])),
            ),
        ]
        for run in runs.head(LISTED).iter_rows(named=True)
    ]
    older = runs.height - LISTED
    more = [span(f"+{older} older in range", "dim")] if older > 0 else []
    return panel("RUNS", [*_trend(runs, size - 4), *table(columns, rows), *more], size)


def _cause(run: dict[str, Any]) -> tuple[str, str]:
    """The cause of a broken run, in full on hover."""
    if run["outcome"] not in incidents.BROKEN:
        return "", "muted"
    text = incidents.detail(run)
    return text, styled("muted", text)


def _trend(runs: pl.DataFrame, size: int) -> Block:
    """Duration, rows, and peak memory over the latest runs, oldest first."""
    recent = runs.head(TREND).reverse()
    if recent.height < 6:
        return []  # too few runs for a trend
    cell = max(8, (size - 3 * 16) // 3)
    parts: Line = []
    for title, column in (("duration", "duration_s"), ("rows", "rows"), ("peak", "peak_rss_mb")):
        parts += [
            *span(f"{title} ", "muted"),
            *sparkline(recent[column].to_list(), cell, "info"),
            *span("   "),
        ]
    return [fit(join(parts, span(f"last {recent.height} runs", "dim")), size), []]


def detail(view: View, run: dict[str, Any], size: int) -> Block:
    """One run in full: the verdict, then whatever its records hold."""
    run_id = str(run["run_id"])
    steps = view.frames.steps.filter(pl.col("run_id") == run_id).sort("started_at")
    samples = view.frames.samples.filter(pl.col("run_id") == run_id).sort("at")
    loads = view.frames.loads.filter(pl.col("run_id") == run_id)
    tables = view.frames.tables.filter(pl.col("run_id") == run_id)
    missing = [
        name
        for name, frame in (("steps", steps), ("memory samples", samples), ("tables", tables))
        if frame.is_empty()
    ]
    note = [span(f"not recorded for this run: {', '.join(missing)}", "dim")] if missing else []
    return vstack(
        verdict(run, size),
        waterfall(steps, size),
        memory(run, samples, size),
        loaded(loads, size),
        destination(tables, size),
        error(run, size),
        note,
    )


def verdict(run: dict[str, Any], size: int) -> Block:
    """What happened and what to run next; then the run's vital signs."""
    glyph, style = outcome(str(run["outcome"]))
    word = "KILLED" if run["outcome"] == "lost" else str(run["outcome"]).upper()
    lines = [
        join(
            span(f"RUN {run['started_at']:%Y-%m-%d %H:%M:%S}Z  ", "info"),
            span(f"{glyph} {word}  ", style),
            span(incidents.detail(run) if run["outcome"] in incidents.BROKEN else "", style),
        )
    ]
    if run["outcome"] in incidents.BROKEN:
        lines.append(
            join(span("→ fix the cause, then ", "accent"), span(f"$ {incidents.rerun(run)}", "ok"))
        )
    share = run["memory_share"]
    available = run["memory_limit_mb"] or run["host_memory_mb"]
    lines.append(
        join(
            span(
                f"{seconds(run['duration_s'])} · {human(run['rows'] or 0)} rows · "
                f"cpu {seconds(run['cpu_seconds'])} · peak memory {mb(run['peak_rss_mb'])} ",
                "muted",
            ),
            gauge(float(share), 16, level_style(float(share))) if isinstance(share, float) else [],
            span(
                (
                    f" of {mb(available)} {'limit' if run['memory_limit_mb'] else 'host'}"
                    if available
                    else ""
                )
                + f" · {run['mode']} · code {run['code_version'] or '-'} · run {run['run_id']}",
                "muted",
            ),
        )
    )
    return [*(fit(line, size) for line in lines), []]


def error(run: dict[str, Any], size: int) -> Block:
    """Where in the code, the chain, and the end of the traceback."""
    if run["outcome"] != "failed":
        return []
    fields = [
        (name, value)
        for name, value in (
            ("type", f"{run['error_type']} (exit {run['exit_code']})"),
            ("window", run["error_window"]),
            ("input", run["error_notes"]),
            ("ours", run["error_where"]),
            ("origin", run["error_origin"]),
        )
        if value
    ]
    lines: Block = [
        join(span(f"{name:<8}", "muted"), span(str(value), "text")) for name, value in fields
    ]
    chain = str(run["error_chain"] or "").splitlines()
    if chain:
        lines += [span(""), span("CHAIN", "info"), *(span(f"  {line}", "muted") for line in chain)]
    trace = str(run["error_traceback"] or "").splitlines()[-14:]
    if trace:
        lines += [
            span(""),
            span("TRACEBACK", "info"),
            *(span(f"  {line}", "dim") for line in trace),
        ]
    return panel("ERROR", lines, size, accent="error")


def waterfall(steps: pl.DataFrame, size: int) -> Block:
    if steps.is_empty():
        return []
    start, last = steps.select(pl.col("started_at").min(), pl.col("at").max()).row(0)
    end = max(last, start + timedelta(seconds=1))
    lanes, rows = [], []
    for step in steps.iter_rows(named=True):
        busy = (step["cpu_s"] / step["duration_s"]) if step["duration_s"] else 0.0
        nature = "busy" if busy >= 0.6 else "waiting" if busy <= 0.2 else "mixed"
        failed = step["outcome"] == "failed"
        style = "error" if failed else nature_style(nature)
        took = seconds(step["duration_s"])
        tip = f"{step['step']} attempt {step['attempt']}: {took}, {busy:.0%} CPU"
        lanes.append(
            (
                f"{step['step']} #{step['attempt']}",
                [(step["started_at"], step["at"], "█", styled(style, tip))],
            )
        )
        rows.append(
            join(
                fit(span(seconds(step["duration_s"]), "text"), 7, "right"),
                fit(span(f"{busy:.0%} cpu", style), 10, "right"),
                span(f"  ✖ {step['error_cause_type']}" if failed else f"  {nature}", style),
            )
        )
    body = hstack([gantt(lanes, start, end, size - 4 - 37, label=14), [*rows, []]], gap=1)
    key = legend(
        [
            ("█", "accent", "busy"),
            ("█", "info", "waiting"),
            ("█", "warn", "mixed"),
            ("█", "error", "failed"),
        ]
    )
    return panel("STEPS", [*body, key], size)


_BANDS = {"extract": ("info", 1), "normalize": ("accent", 2), "load": ("warn", 3)}


def memory(run: dict[str, Any], samples: pl.DataFrame, size: int) -> Block:
    if samples.height < 2:
        return []
    limit = run["memory_limit_mb"]
    body = chart(
        [(samples["rss_mb"].to_list(), "ok")],
        size - 4,
        6,
        label=mb,
        limit=(float(limit), "error") if limit else None,
    )
    codes = resample([_BANDS.get(step or "", ("", 0))[1] for step in samples["step"]], size - 12)
    styles = {code: style for style, code in _BANDS.values()}
    bands = join(
        span(" " * 8), *(span("▀" if code else " ", styles.get(int(code or 0))) for code in codes)
    )
    key = legend(
        [("▀", style, step) for step, (style, _) in _BANDS.items()]
        + ([("·", "error", "limit")] if limit else [])
    )
    return panel("MEMORY", [*body, bands, key], size)


def loaded(loads: pl.DataFrame, size: int) -> Block:
    """Each resource the run loaded: rows, and where its cursor or window started and ended."""
    if loads.is_empty():
        return []
    columns = [
        Column("resource", 26),
        Column("rows", 9, "right"),
        Column("from", 25),
        Column("to", 25),
    ]
    rows = []
    for load in loads.sort("rows", descending=True).iter_rows(named=True):
        increment = load["window_kind"] == "increment"
        low = load["cursor_start_value"] if increment else load["window_start"]
        high = load["cursor_last_value"] if increment else load["window_end"]
        rows.append(
            [
                load["resource_id"].split(".", 1)[1],
                span(f"{load['rows']:,}", "text" if load["rows"] else "muted"),
                span(edge(low) if low else "start", "muted"),
                span(edge(high) if high else "-", "ok" if high else "muted"),
            ]
        )
    return panel("LOADED", table(columns, rows), size)


def destination(tables: pl.DataFrame, size: int) -> Block:
    if tables.is_empty():
        return []
    columns = [
        Column("table", 26),
        Column("rows", 9, "right"),
        Column("load", 7, "right"),
        Column("retries", 7, "right"),
    ]
    rows = [
        [table_["table"], f"{table_['rows']:,}", seconds(table_["load_s"]), str(table_["retries"])]
        for table_ in tables.sort("load_s", descending=True, nulls_last=True).iter_rows(named=True)
    ]
    return panel("TABLES", table(columns, rows), size)

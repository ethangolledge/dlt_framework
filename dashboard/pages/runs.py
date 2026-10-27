"""Runs: what ran when, alongside what; and one run in full."""

from datetime import timedelta
from typing import Any

import polars as pl

from dashboard.model import capacity, failures
from dashboard.model.facts import View
from dashboard.pages.base import (
    OUTCOME_LEGEND,
    Context,
    empty,
    nature_style,
    outcome,
    outcome_cell,
    page,
)
from dashboard.render.primitives import (
    Column,
    banner,
    bars,
    chart,
    gantt,
    gauge,
    legend,
    panel,
    resample,
    share,
    side_by_side,
    table,
)
from dashboard.render.text import Block, Line, fit, hstack, join, span, vstack
from dashboard.theme import level_style
from dashboard.units import first_line, human, mb, seconds

LATEST = 25  # runs listed under the timeline


@page("runs", "Runs", "What ran, when, and alongside what?")
def draw(context: Context) -> Block:
    view, size = context.view, context.size
    return vstack(
        banner("runs", size, f"{view.runs.height} runs in range"),
        timeline(view, size),
        concurrency(view, size),
        listing(view, size),
    )


def timeline(view: View, size: int) -> Block:
    if view.runs.is_empty():
        return empty("TIMELINE", size, "no runs in this range")
    lanes = []
    for key, own in sorted(view.runs.group_by("pipeline_id"), key=lambda group: str(group[0][0])):
        intervals = []
        for run in own.sort("started_at").iter_rows(named=True):
            glyph, style = outcome(run["outcome"])
            took, rows = seconds(run["duration_s"]), human(run["rows"] or 0)
            tip = f"{run['run_id'][:8]} {run['outcome']} {took} {rows} rows"
            intervals.append((run["started_at"], run["ended_at"], glyph, f"{style}|{tip}"))
        lanes.append((str(key[0]), intervals))
    body = gantt(lanes, view.scope.start, view.scope.end, size - 4, label=18)
    return panel("TIMELINE ─ a bar per run, start to end", [*body, OUTCOME_LEGEND], size)


def concurrency(view: View, size: int) -> Block:
    if view.runs.is_empty():
        return []
    cols = size - 4 - 8
    found = capacity.concurrency(view, cols)
    active = chart(
        [(found["runs"].cast(pl.Float64).to_list(), "info")],
        share(size, 2) - 4,
        5,
        label=lambda v: f"{v:.0f}",
    )
    host = found.select(pl.col("host_mb").max()).item()
    limit = None if host is None else (float(host), "error")
    memory = chart(
        [(found["memory_mb"].to_list(), "accent")],
        share(size, 2) - 4,
        5,
        label=lambda v: mb(v),
        limit=limit,
    )
    return side_by_side(
        [
            panel("ACTIVE RUNS ─ at once", active, share(size, 2)),
            panel(
                "THEIR PEAK MEMORY ─ summed, against the host",
                memory,
                share(size, 2),
                accent="accent",
            ),
        ],
        size,
    )


def listing(view: View, size: int) -> Block:
    columns = [
        Column("started", 14),
        Column("pipeline", 18),
        Column("mode", 8),
        Column("", 1),
        Column("outcome", 10),
        Column("took", 7, "right"),
        Column("rows", 7, "right"),
        Column("peak", 6, "right"),
        Column("exit", 4, "right"),
        Column("version", 9),
        Column("run", 10),
    ]
    rows = [
        [
            f"{run['started_at']:%m-%d %H:%M:%S}",
            run["pipeline_id"],
            run["mode"],
            outcome_cell(run["outcome"]),
            span(run["outcome"], outcome(run["outcome"])[1]),
            seconds(run["duration_s"]),
            human(run["rows"] or 0),
            span(mb(run["peak_rss_mb"]), level_style(run["memory_share"] or 0)),
            "-" if run["exit_code"] is None else str(run["exit_code"]),
            run["code_version"] or "-",
            span(run["run_id"][:8], "muted"),
        ]
        for run in view.runs.head(LATEST).iter_rows(named=True)
    ]
    return panel(f"LATEST {LATEST} RUNS ─ choose one on the Run page", table(columns, rows), size)


@page(
    "run",
    "Run",
    "One run in full: its steps, memory, resources, tables, and any error.",
    picks_run=True,
)
def draw_run(context: Context) -> Block:
    view, size = context.view, context.size
    run = view.run(context.run_id) if context.run_id else _pick(view)
    if run is None:
        return vstack(
            banner("run", size), empty("RUN", size, "no run chosen, and none in this range")
        )
    run_id = str(run["run_id"])
    steps = view.frames.steps.filter(pl.col("run_id") == run_id).sort("started_at")
    samples = view.frames.samples.filter(pl.col("run_id") == run_id).sort("at")
    loads = view.frames.loads.filter(pl.col("run_id") == run_id)
    tables = view.frames.tables.filter(pl.col("run_id") == run_id)
    half = share(size, 2)
    return vstack(
        banner("run", size, f"run {run_id}"),
        header(run, size),
        error(run, size),
        waterfall(run, steps, size),
        memory(run, samples, size),
        side_by_side([resources(loads, half), destination(tables, half)], size),
        cursors(loads, size),
    )


def _pick(view: View) -> dict[str, Any] | None:
    """With no run chosen: the latest failed or lost one in scope, else the latest."""
    broken = failures.failed(view)
    chosen = broken if broken.height else view.runs
    return chosen.row(0, named=True) if chosen.height else None


def header(run: dict[str, Any], size: int) -> Block:
    glyph, style = outcome(str(run["outcome"]))
    share_ = run["memory_share"]
    lines = [
        join(
            span(f"{glyph} {str(run['outcome']).upper()}", style),
            span(f"   {run['pipeline_id']} ", "bright"),
            span(f"({run['mode']})", "muted"),
            span(f"   {seconds(run['duration_s'])}   {human(run['rows'] or 0)} rows   ", "text"),
            span(
                f"code {run['code_version'] or '-'}   host {run['host'] or '-'}   "
                f"cpu {seconds(run['cpu_seconds'])}",
                "muted",
            ),
        ),
        join(
            span(
                f"started {run['started_at']:%Y-%m-%d %H:%M:%S}Z   "
                f"ended {run['ended_at']:%H:%M:%S}Z   ",
                "muted",
            ),
            span("memory ", "muted"),
            gauge(float(share_), 24, level_style(float(share_)))
            if isinstance(share_, float)
            else span("-", "muted"),
            span(
                f"  peak {mb(run['peak_rss_mb'])} "
                f"of {mb(run['memory_limit_mb'] or run['host_memory_mb'])}",
                "muted",
            ),
        ),
    ]
    return panel("RUN", lines, size, accent=style)


def error(run: dict[str, Any], size: int) -> Block:
    if run["outcome"] not in ("failed", "lost"):
        return []
    if run["outcome"] == "lost":
        step, resource = run["error_step"] or "an unknown step", run["error_resource"] or "-"
        seen = f"Last seen {seconds(run['duration_s'])} in, during {step} of {resource}"
        killed = [
            span(
                "Killed from outside (an OOM kill, a stopped container): no end was reported.",
                "error",
            ),
            span(f"{seen}, at {mb(run['peak_rss_mb'])}.", "text"),
            span("See Capacity for this pipeline's memory against its limit.", "muted"),
        ]
        return panel("LOST", killed, size, style="error", accent="error")
    lines: list[Line] = [
        join(span(f"{run['error_type']}", "error"), span(f"  exit {run['exit_code']}", "muted")),
        _field("step", run["error_step"]),
        _field("resource", run["error_resource"]),
        _field("window", run["error_window"]),
        _field("input", run["error_notes"]),
        _field("ours", run["error_where"]),
        _field("origin", run["error_origin"]),
        _field("cause", f"{run['error_cause_type']}: {first_line(run['error_message'])}"),
        span("", None),
        span("CHAIN (outermost first)", "info"),
        *(span(f"  {line}", "muted") for line in str(run["error_chain"] or "").splitlines()),
        span("", None),
        span("TRACEBACK (end)", "info"),
        *(
            span(f"  {line}", "dim")
            for line in str(run["error_traceback"] or "").splitlines()[-14:]
        ),
        span("", None),
        join(span("NEXT  ", "accent"), span(f"$ {failures.next_step(run)}", "ok")),
    ]
    return panel("ERROR", lines, size, style="error", accent="error")


def _field(name: str, value: object) -> Line:
    return join(span(f"{name:<10}", "muted"), span(str(value) if value else "-", "text"))


def waterfall(run: dict[str, Any], steps: pl.DataFrame, size: int) -> Block:
    if steps.is_empty():
        return empty("STEPS", size, "no steps recorded for this run (an older record shape)")
    start, last = steps.select(pl.col("started_at").min(), pl.col("at").max()).row(0)
    end = max(last, start + timedelta(seconds=1))
    detail = 36
    lanes, rows = [], []
    for step in steps.iter_rows(named=True):
        share_ = (step["cpu_s"] / step["duration_s"]) if step["duration_s"] else 0.0
        nature = "busy" if share_ >= 0.6 else "waiting" if share_ <= 0.2 else "mixed"
        failed = step["outcome"] == "failed"
        style = "error" if failed else nature_style(nature)
        took = seconds(step["duration_s"])
        tip = f"{step['step']} attempt {step['attempt']}: {took}, {share_:.0%} CPU"
        lanes.append(
            (
                f"{step['step']} #{step['attempt']}",
                [(step["started_at"], step["at"], "█", f"{style}|{tip}")],
            )
        )
        rows.append(
            join(
                fit(span(seconds(step["duration_s"]), "text"), 7, "right"),
                span("  ", None),
                fit(span(f"{share_:.0%} cpu", style), 9, "right"),
                span("  ", None),
                span(f"✖ {step['error_cause_type']}" if failed else nature, style),
            )
        )
    chart_ = gantt(lanes, start, end, size - 4 - detail - 1, label=14)
    body = hstack([chart_, [*rows, []]], gap=1)
    key = legend(
        [
            ("█", "accent", "busy: computing"),
            ("█", "info", "waiting: on upstream or the destination"),
            ("█", "warn", "mixed"),
            ("█", "error", "failed attempt"),
        ]
    )
    return panel("STEPS ─ attempts in order, wall time, CPU share", [*body, key], size)


def memory(run: dict[str, Any], samples: pl.DataFrame, size: int) -> Block:
    if samples.is_empty():
        return empty("MEMORY", size, "no samples for this run")
    limit = run["memory_limit_mb"] or None
    limit_line = (float(limit), "error") if limit else None
    rss = samples["rss_mb"].to_list()
    body = chart([(rss, "ok")], size - 4, 8, label=lambda v: mb(v), limit=limit_line)
    cols = size - 4 - 8
    bands = resample([_STEP_CODES.get(step or "", 0) for step in samples["step"].to_list()], cols)
    band_line = join(
        span(" " * 8, None),
        *(span(_BAND_GLYPH[int(code or 0)], _BAND_STYLE[int(code or 0)]) for code in bands),
    )
    key = legend(
        [
            ("▀", "info", "extract"),
            ("▀", "accent", "normalize"),
            ("▀", "warn", "load"),
            ("·", "error", "limit"),
        ]
    )
    return panel(
        f"MEMORY ─ RSS against {mb(limit) if limit else 'the host'} ─ bands are steps",
        [*body, band_line, key],
        size,
    )


_STEP_CODES = {"extract": 1, "normalize": 2, "load": 3}
_BAND_GLYPH = {0: " ", 1: "▀", 2: "▀", 3: "▀"}
_BAND_STYLE = {0: None, 1: "info", 2: "accent", 3: "warn"}


def resources(loads: pl.DataFrame, size: int) -> Block:
    if loads.is_empty():
        return empty("RESOURCES", size, "nothing loaded in this run")
    rows = []
    for load in loads.sort("rows", descending=True).iter_rows(named=True):
        span_s = (
            (load["extract_last_at"] - load["extract_first_at"]).total_seconds()
            if load["extract_first_at"]
            else 0.0
        )
        name = load["resource_id"].split(".", 1)[1]
        rows.append((name, span_s, f"{seconds(span_s)} {human(load['rows'])}", "ok"))
    return panel("RESOURCES ─ own extract time, rows", bars(rows, size - 4), size)


def destination(tables: pl.DataFrame, size: int) -> Block:
    if tables.is_empty():
        return empty("TABLES", size, "no tables loaded in this run")
    rows = [
        (
            table["table"],
            table["load_s"] or 0.0,
            f"{seconds(table['load_s'])} {human(table['rows'])}",
            "warn",
        )
        for table in tables.sort("load_s", descending=True, nulls_last=True).iter_rows(named=True)
    ]
    return panel("TABLES ─ load time, rows", bars(rows, size - 4), size, accent="warn")


def cursors(loads: pl.DataFrame, size: int) -> Block:
    moved = loads.filter(pl.col("cursor_last_value").is_not_null())
    if moved.is_empty():
        return []
    rows = [
        join(
            fit(span(load["resource_id"].split(".", 1)[1], "text"), 22),
            span(str(load["cursor_start_value"] or "start"), "muted"),
            span("  →  ", "accent"),
            span(str(load["cursor_last_value"]), "ok"),
            span(f"   part {load['part']}", "dim"),
        )
        for load in moved.iter_rows(named=True)
    ]
    return panel("CURSORS ─ where each load started and ended", rows, size)

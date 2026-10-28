"""Run: why did this run fail, and what do I run next?

The verdict and the command come first; then how the run spent its time and memory, what it
loaded, and last the error's detail for whoever fixes it.
"""

from datetime import timedelta
from typing import Any

import polars as pl

from dashboard.model import failures, triage
from dashboard.model.facts import View
from dashboard.pages.base import (
    Context,
    empty,
    nature_style,
    outcome,
    page,
    styled,
    to_pipeline,
)
from dashboard.render.primitives import (
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
)
from dashboard.render.text import Block, Line, fit, hstack, join, span, vstack
from dashboard.theme import level_style
from dashboard.units import first_line, human, mb, seconds


@page("run", "Run", picks="run")
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
        banner("run", size, f"{run['pipeline_id']} · {run_id[:8]}"),
        header(run, size),
        [[]],
        waterfall(run, steps, size),
        memory(run, samples, size),
        side_by_side([resources(loads, half), destination(tables, half)], size)
        if loads.height and tables.height
        else resources(loads, size) or destination(tables, size),
        cursors(loads, size),
        error(run, size),
    )


def _pick(view: View) -> dict[str, Any] | None:
    """With no run chosen: the latest failed or lost one in scope, else the latest."""
    broken = failures.failed(view)
    chosen = broken if broken.height else view.runs
    return chosen.row(0, named=True) if chosen.height else None


def header(run: dict[str, Any], size: int) -> Block:
    """The verdict: what happened, where, and what to run next; then the run's vital signs."""
    glyph, style = outcome(str(run["outcome"]))
    name = str(run["pipeline_id"])
    word = {"lost": "KILLED"}.get(str(run["outcome"]), str(run["outcome"]).upper())
    lines = [
        join(
            span(f"{glyph} {word}  ", style),
            span(name, styled("bright", "open this pipeline", to_pipeline(name))),
            span(f"  {_what(run)}", style),
        )
    ]
    if run["outcome"] in failures.BROKEN:
        lines.append(join(span("→ ", "accent"), span(triage.explain(run).action, "text")))
    share_ = run["memory_share"]
    lines.append(
        join(
            span(
                f"{seconds(run['duration_s'])} · {human(run['rows'] or 0)} rows · "
                f"cpu {seconds(run['cpu_seconds'])} · memory ",
                "muted",
            ),
            gauge(float(share_), 20, level_style(float(share_)))
            if isinstance(share_, float)
            else span("-", "muted"),
            span(
                f" of {mb(run['memory_limit_mb'] or run['host_memory_mb'])} · "
                f"{run['mode']} · code {run['code_version'] or '-'} · {run['host'] or '-'} · "
                f"{run['started_at']:%Y-%m-%d %H:%M:%S}Z",
                "muted",
            ),
        )
    )
    return [fit(line, size) for line in lines]


def _what(run: dict[str, Any]) -> str:
    """Where it broke and why, in one line."""
    where = " · ".join(str(part) for part in (run["error_step"], run["error_resource"]) if part)
    if run["outcome"] == "lost":
        seen = where or "an unknown step"
        return f"no end reported; last seen in {seen} at {mb(run['peak_rss_mb'])}"
    if run["outcome"] == "failed":
        cause = f"{run['error_cause_type']}: {first_line(run['error_message'])}"
        return f"in {where} · {cause}" if where else cause
    return ""


def error(run: dict[str, Any], size: int) -> Block:
    """The detail under the verdict: where in the code, the chain, the traceback's end."""
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
    lines: Block = [_field(name, value) for name, value in fields]
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


def _field(name: str, value: object) -> Line:
    return join(span(f"{name:<10}", "muted"), span(str(value) if value else "-", "text"))


def waterfall(run: dict[str, Any], steps: pl.DataFrame, size: int) -> Block:
    if steps.is_empty():
        return []
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
            ("█", "accent", "busy"),
            ("█", "info", "waiting"),
            ("█", "warn", "mixed"),
            ("█", "error", "failed"),
        ]
    )
    return panel("STEPS", [*body, key], size)


def memory(run: dict[str, Any], samples: pl.DataFrame, size: int) -> Block:
    if samples.is_empty():
        return []
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
    return panel("MEMORY", [*body, band_line, key], size)


_STEP_CODES = {"extract": 1, "normalize": 2, "load": 3}
_BAND_GLYPH = {0: " ", 1: "▀", 2: "▀", 3: "▀"}
_BAND_STYLE = {0: None, 1: "info", 2: "accent", 3: "warn"}


def resources(loads: pl.DataFrame, size: int) -> Block:
    if loads.is_empty():
        return []
    rows = []
    for load in loads.sort("rows", descending=True).iter_rows(named=True):
        span_s = (
            (load["extract_last_at"] - load["extract_first_at"]).total_seconds()
            if load["extract_first_at"]
            else 0.0
        )
        name = load["resource_id"].split(".", 1)[1]
        rows.append((name, span_s, f"{seconds(span_s)} {human(load['rows'])}", "ok"))
    return panel("RESOURCES", bars(rows, size - 4), size)


def destination(tables: pl.DataFrame, size: int) -> Block:
    if tables.is_empty():
        return []
    rows = [
        (
            table["table"],
            table["load_s"] or 0.0,
            f"{seconds(table['load_s'])} {human(table['rows'])}",
            "warn",
        )
        for table in tables.sort("load_s", descending=True, nulls_last=True).iter_rows(named=True)
    ]
    return panel("TABLES", bars(rows, size - 4), size, accent="warn")


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
    return panel("CURSORS", rows, size)

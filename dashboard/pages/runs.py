"""Runs: what is this pipeline, what is it doing, and what happened in this run?

One pipeline, chosen from a dropdown (or ``show runs NAME``) and named in full at the top:
what it is (source, destination, each resource's write mode and cursor); its state; its runs,
newest first, the chosen one marked; and that run in full beneath: what broke and the command
to run, the error, its steps, memory, and what it loaded. By default the run chosen is the
latest of an open incident, else the latest run.
"""

from datetime import timedelta
from typing import Any

import polars as pl

from dashboard.model import checks, gaps, incidents, overview, pipelines, triage
from dashboard.model.facts import View
from dashboard.model.pipelines import About
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
    time_axis,
    wrap,
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
    about = pipelines.about(view, name)
    return vstack(
        header("runs", context, "runs"),
        chosen(about, status, [item.pipeline for item in states], size),
        identity(about, size),
        state(mine, status, size),
        listing(mine, run, size),
        detail(view, run, about, size) if run else [],
    )


def chosen(about: About, status: triage.Status | None, names: list[str], size: int) -> Block:
    """The pipeline shown, unmistakably: its name large, its state, and how to pick another."""
    glyph, style, word = STATES[status.level if status else "idle"]
    where = f" {names.index(about.pipeline) + 1} of {len(names)}" if about.pipeline in names else ""
    return [
        fit(
            join(
                span("PIPELINE  ", "info"),
                span(f"{glyph} ", style),
                span(about.pipeline, "bright"),
                span(f"   {word}", style),
                span(f"   ·{where} · another: the pipeline dropdown, or `show runs NAME`", "dim"),
            ),
            size,
        ),
        [],
    ]


def identity(about: About, size: int) -> Block:
    """What the pipeline is: where it reads from and writes to, and how each resource reads."""
    source = (
        " ".join(part for part in (about.source_type, about.source) if part)
        or "source not recorded"
    )
    target = " · dataset ".join(part for part in (about.destination, about.dataset) if part)
    lines: Block = [
        join(span(source, "text"), span("  →  ", "accent"), span(target or "-", "text"))
    ]
    if about.resources:
        columns = [
            Column("resource", 26),
            Column("write", 8),
            Column("reads", 56),
            Column("history", 34),
            Column("if empty", 8),
        ]
        rows: list[list[Line | str]] = [
            [
                resource["resource"],
                resource["write_mode"],
                span(about.cursor(resource["resource"]), "muted"),
                span(_history(resource), "muted"),
                resource["empty"],
            ]
            for resource in about.resources
        ]
        lines += [[], *table(columns, rows)]
    return panel("ABOUT", lines, size)


def _default(view: View, status: triage.Status | None) -> dict[str, Any] | None:
    """The latest run of the open incident, else the latest run."""
    if status and status.incident:
        return status.incident.latest
    return view.runs.row(0, named=True) if view.runs.height else None


def _history(resource: dict[str, Any]) -> str:
    """The history a resource declares for backfills, or that it declares none."""
    if not resource["backfill_window"]:
        return "no backfill declared"
    end = edge(resource["backfill_end"]) if resource["backfill_end"] else "open"
    return f"{edge(resource['backfill_start'])} → {end}"


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
            span(*_cause(run)),
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


def detail(view: View, run: dict[str, Any], about: About, size: int) -> Block:
    """One run in full: the verdict and the error, then whatever else its records hold."""
    run_id = str(run["run_id"])
    steps = view.frames.steps.filter(pl.col("run_id") == run_id).sort("started_at")
    samples = view.frames.samples.filter(pl.col("run_id") == run_id).sort("at")
    loads = view.frames.loads.filter(pl.col("run_id") == run_id)
    tables = view.frames.tables.filter(pl.col("run_id") == run_id)
    absent = [
        name
        for name, frame in (
            ("steps", steps),
            ("memory samples", samples),
            ("loads", loads),
            ("tables", tables),
        )
        if frame.is_empty()
    ]
    why = "it stopped before them, or its monitoring predates them"
    note = (
        wrap(f"no {', '.join(absent)} recorded for this run: {why}", size, "dim") if absent else []
    )
    return vstack(
        verdict(run, size),
        error(run, size),
        waterfall(steps, size),
        memory(run, samples, size),
        loaded(loads, about, size),
        destination(tables, size),
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
    """The failure in full, under the verdict: the cause and its message, where it was raised,
    the chain of exceptions, and the end of the traceback."""
    if run["outcome"] != "failed":
        return []
    number = incidents.signal(run)
    meaning = incidents.SIGNALS.get(number, ("", ""))[1] if number is not None else None
    fields = [
        (name, value)
        for name, value in (
            ("cause", f"{run['error_cause_type']}: {run['error_message'] or ''}".strip()),
            ("meaning", meaning and f"signal {number}: {meaning}"),
            (
                "step",
                " · ".join(part for part in (run["error_step"], run["error_resource"]) if part),
            ),
            ("window", run["error_window"]),
            ("input", run["error_notes"]),
            ("ours", run["error_where"]),
            ("origin", run["error_origin"]),
            ("raised", f"{run['error_type']}, exit code {run['exit_code']}"),
        )
        if value
    ]
    lines: Block = []
    for name, value in fields:
        text = wrap(str(value), size - 4 - 9, "text")
        lines.append(join(span(f"{name:<9}", "muted"), text[0]))
        lines += [join(span(" " * 9), line) for line in text[1:4]]
    chain = str(run["error_chain"] or "").splitlines()
    if len(chain) > 1:
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
    axis = join(span(" " * 8), time_axis(samples["at"][0], samples["at"][-1], size - 12))
    if samples["step"].drop_nulls().is_empty():  # older samples carry no step to band by
        return panel("MEMORY", [*body, axis], size)
    codes = resample([_BANDS.get(step or "", ("", 0))[1] for step in samples["step"]], size - 12)
    styles = {code: style for style, code in _BANDS.values()}
    bands = join(
        span(" " * 8), *(span("▀" if code else " ", styles.get(int(code or 0))) for code in codes)
    )
    key = legend(
        [("▀", style, step) for step, (style, _) in _BANDS.items()]
        + ([("·", "error", "limit")] if limit else [])
    )
    return panel("MEMORY", [*body, axis, bands, key], size)


def loaded(loads: pl.DataFrame, about: About, size: int) -> Block:
    """Each resource the run loaded: its rows, how it reads, and where its cursor stood before
    and after (or the window a backfill loaded)."""
    if loads.is_empty():
        return []
    columns = [
        Column("resource", 24),
        Column("rows", 10, "right"),
        Column("cursor", 22),
        Column("cursor before", 20),
        Column("cursor after", 20),
    ]
    rows: list[list[Line | str]] = []
    for load in loads.sort("rows", descending=True).iter_rows(named=True):
        name = load["resource_id"].split(".", 1)[1]
        declared = next((row for row in about.resources if row["resource"] == name), {})
        reads = declared.get("cursor_path") or "no cursor: all of it"
        if load["window_kind"] != "increment":
            low, high = load["window_start"], load["window_end"]
            before = span(edge(low), "muted")
            after = span(edge(high), "ok")
            reads = f"{load['window_kind']} window"
        elif not declared.get("cursor_path"):
            before = after = span("-", "dim")
        else:
            low, high = load["cursor_start_value"], load["cursor_last_value"]
            before = (
                span(edge(low), "muted")
                if low
                else span("not recorded", styled("dim", "monitoring before this was recorded"))
            )
            after = span(edge(high), "ok") if high else span("-", "dim")
        rows.append(
            [
                name,
                span(f"{load['rows']:,}", "text" if load["rows"] else "muted"),
                span(reads, "muted"),
                before,
                after,
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

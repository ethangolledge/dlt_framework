"""Overview: is production OK?

Incremental runs only: backfills are ad hoc, so they get one line here and a page of their
own. The verdict; every pipeline worst first with its last run, last success, recent runs, and
what is wrong now; the open incidents with what to run; what is at risk; and the backfills.
"""

import polars as pl

from dashboard.model import checks, gaps, incidents, overview, triage
from dashboard.model.facts import View
from dashboard.model.incidents import Incident
from dashboard.model.triage import Status
from dashboard.pages.base import (
    OUTCOME_LEGEND,
    STATES,
    Context,
    ago,
    clear,
    empty,
    header,
    lasted,
    outcome,
    page,
    styled,
    to_pipeline,
    to_run,
    two_lines,
)
from dashboard.render.primitives import Column, panel, table
from dashboard.render.text import Block, Line, fit, join, span, vstack
from dashboard.units import seconds

RECENT = 20  # runs in each pipeline's strip
ROWS = 15  # pipelines listed: all failing or at risk, then healthy ones up to this


@page("overview", "Overview")
def draw(context: Context) -> Block:
    view, size = context.view, context.size
    production = view.mode("run")
    found = incidents.incidents(production)
    report = checks.run(production)
    missing = gaps.holes(view) + [item for h in gaps.histories(view) for item in h.missing]
    states = triage.statuses(production, found, report, missing, context.signals)
    return vstack(
        header("overview", context, "runs (backfills below)"),
        verdict(states, size),
        pipelines(production, states, size),
        open_incidents(view, incidents.incidents(view), size),
        at_risk(states, size),
        backfills(view, missing, size),
    )


def verdict(states: list[Status], size: int) -> Block:
    """``✖ 1 FAILING   ⚠ 1 AT RISK   █ 4 HEALTHY``, or all clear."""
    if not states:
        return [span("NO SIGNAL  no pipelines in this range", "muted"), []]
    counts = {key: sum(status.level == key for status in states) for key in STATES}
    if counts["error"] + counts["warning"] == 0:
        return [span(f"✔ ALL CLEAR  {len(states)} pipeline(s) healthy", "ok"), []]
    parts = [
        join(span(f"{STATES[key][0]} {counts[key]} {STATES[key][2]}", STATES[key][1]), span("    "))
        for key in STATES
        if counts[key]
    ]
    return [fit(join(*parts), size), []]


def pipelines(view: View, states: list[Status], size: int) -> Block:
    if not states:
        return empty("PIPELINES", size, "no runs in this range")
    inner = size - 4
    columns = [
        Column("", 1),
        Column("pipeline", 22),
        Column("last run", 13),
        Column("took", 7, "right"),
        Column("last ok", 9),
        Column("runs ok", 7, "right"),
        Column("recent runs", RECENT),
        Column("now", max(20, inner - 89)),
    ]
    fresh = dict(overview.freshness(view).select("pipeline_id", "age").iter_rows())
    rows: list[list[Line | str]] = []
    trouble = [status for status in states if status.level in ("error", "warning")]
    shown = (
        trouble
        + [status for status in states if status not in trouble][: max(0, ROWS - len(trouble))]
    )
    for status in shown:
        glyph, style, _ = STATES[status.level]
        own = view.runs.filter(pl.col("pipeline_id") == status.pipeline)
        latest = own.row(0, named=True) if own.height else None
        ended = own.filter(pl.col("outcome") != "running")
        good = ended.filter(pl.col("outcome").is_in(incidents.OK)).height
        tip = "; ".join(f"{concern.headline}: {concern.detail}" for concern in status.concerns)
        rows.append(
            [
                span(glyph, style),
                span(status.pipeline, styled("bright", tip or None, to_pipeline(status.pipeline))),
                join(
                    span(outcome(latest["outcome"])[0], outcome(latest["outcome"])[1]),
                    span(f" {ago(view.now - latest['started_at'])}", "muted"),
                )
                if latest
                else span("-", "muted"),
                seconds(latest["duration_s"]) if latest else "-",
                span(
                    ago(fresh.get(status.pipeline)), "muted" if status.pipeline in fresh else "dim"
                ),
                span(f"{good}/{ended.height}", "muted" if good == ended.height else "warn"),
                _strip(own),
                span(status.headline, styled("muted" if style == "ok" else style, tip or None)),
            ]
        )
    hidden = len(states) - len(shown)
    more = (
        [span(f"+{hidden} more healthy or idle: not shown (narrow with --pipeline)", "dim")]
        if hidden
        else []
    )
    return panel("PIPELINES", [*table(columns, rows), *more, OUTCOME_LEGEND], size)


def _strip(runs: pl.DataFrame) -> Line:
    """The latest runs, oldest to newest, one cell each, each linked to its run."""
    cells: Line = [(" ", None)] * max(0, RECENT - min(RECENT, runs.height))
    for run in runs.head(RECENT).reverse().iter_rows(named=True):
        glyph, style = outcome(run["outcome"])
        tip = f"{run['started_at']:%m-%d %H:%M} {run['outcome']} {seconds(run['duration_s'])}"
        cells.append((glyph, styled(style, tip, to_run(run["pipeline_id"], run["run_id"]))))
    return cells


def open_incidents(view: View, every: list[Incident], size: int) -> Block:
    found = [item for item in every if item.open]
    if not found:
        resolved = len(every)
        later = f"; {resolved} resolved in range (see Incidents)" if resolved else ""
        return clear("INCIDENTS", size, f"no open incidents{later}")
    lines: Block = []
    for item in found:
        latest = item.latest
        head = join(
            span(
                f"for {lasted(item.lasted(view.now))} (since {item.started_at:%m-%d %H:%M}), "
                f"{len(item.runs)} run(s): ",
                "error",
            ),
            span(item.summary, "text"),
        )
        under = join(
            span(f"latest {latest['started_at']:%m-%d %H:%M}: ", "muted"),
            span(
                incidents.detail(latest),
                styled("muted", to=to_run(item.pipeline, latest["run_id"])),
            ),
        )
        mode = "" if item.mode == "run" else f" ({item.mode})"
        lines += two_lines(
            span("✖", "error"), item.pipeline + mode, head, under, size - 4, pipeline=item.pipeline
        )
        command = span(f"→ fix it, then $ {incidents.rerun(latest)}", "ok")
        lines.append(fit(join(span(" " * 25), command), size - 4))
    return panel("OPEN INCIDENTS", lines, size, accent="error")


def at_risk(states: list[Status], size: int) -> Block:
    lines: Block = []
    for status in states:
        for concern in status.concerns:
            if concern.level == "warning":
                lines += two_lines(
                    span("⚠", "warn"),
                    status.pipeline,
                    span(concern.headline, "warn"),
                    span(concern.detail, "muted"),
                    size - 4,
                )
    return panel("AT RISK", lines, size, accent="warn") if lines else []


def backfills(view: View, missing: list[gaps.Missing], size: int) -> Block:
    """One line: backfill jobs in range, and the rows still missing."""
    jobs = gaps.jobs(view)
    declared = gaps.histories(view)
    fixable = [item for item in missing if item.kind in gaps.FIXABLE]
    if jobs.is_empty() and not declared and not fixable:
        text = span("none run in range, and no resource declares one", "muted")
    else:
        failed = jobs.filter(pl.col("outcome") == "failed").height
        text = join(
            span(f"{jobs.height} job(s) in range", "text"),
            span(f", {failed} failed" if failed else "", "error"),
            span(
                f" · {len(fixable)} range(s) missing" if fixable else " · nothing missing",
                "warn" if fixable else "ok",
            ),
            span(f" in {len(declared)} declared history(ies)", "muted"),
        )
    return [fit(join(span("BACKFILLS  ", "info"), text, span("  → Backfills", "dim")), size)]

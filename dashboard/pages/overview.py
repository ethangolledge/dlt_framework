"""Overview: does anything need me, and what do I do first?

Read top to bottom it tells one story: the verdict, four numbers against the period before,
every pipeline worst first with why in a few words, and the next things to do.
"""

import polars as pl

from dashboard.model import overview, triage
from dashboard.model.advisor import Advice
from dashboard.model.facts import buckets
from dashboard.model.triage import Status
from dashboard.pages.base import (
    OUTCOME_LEGEND,
    STATES,
    Context,
    clear,
    compared,
    empty,
    outcome,
    page,
    styled,
    to_pipeline,
    to_run,
    verdict,
)
from dashboard.render.primitives import (
    banner,
    card,
    panel,
    share,
    side_by_side,
    sparkline,
    time_axis,
)
from dashboard.render.text import Block, Line, fit, join, span, vstack
from dashboard.units import age

LABEL = 18  # pipeline names
NEXT = 5  # actions listed


@page("overview", "Overview")
def draw(context: Context) -> Block:
    view, size = context.view, context.size
    todo = triage.actions(view, context.signals)
    states = triage.statuses(view, todo)
    return vstack(
        banner("overview", size, overview.scope_label(view.scope)),
        verdict(states, size),
        [[]],
        cards(context),
        pipelines(context, states),
        upcoming([item for item in todo if item.level != "info"], size),
        [span("▸ show pipeline NAME  ·  show run ID  ·  show actions", "dim")],
    )


def cards(context: Context) -> Block:
    kpis = overview.kpis(context.view)
    size = share(context.size, len(kpis))
    inner = size - 4
    blocks = []
    for kpi in kpis:
        note = compared(kpi.note) if kpi.title != "STALEST" else kpi.note
        trend = any(value is not None for value in kpi.trend)
        below = (
            join(
                sparkline(kpi.trend, inner - len(note) - 2 * bool(note), "muted"),
                span(note and "  " + note, "muted"),
            )
            if trend
            else span(note, "muted")
        )
        blocks.append(card(kpi.title, kpi.value, size, style=kpi.style, below=below))
    return side_by_side(blocks, context.size)


def pipelines(context: Context, states: list[Status]) -> Block:
    view, size = context.view, context.size
    if not states:
        return empty("PIPELINES", size, "no runs in this range")
    inner = size - 4
    reason = max(24, inner * 2 // 5)
    count = max(12, inner - 2 - LABEL - 1 - 13 - reason)
    cells = dict(overview.matrix(view, count))
    starts = buckets(view.scope, count)
    latest = dict(view.runs.group_by("pipeline_id").agg(pl.col("started_at").max()).iter_rows())
    rows: Block = []
    for status in states:
        glyph, style, word = STATES[status.level]
        name = status.pipeline
        strip: Line = []
        for start, found in zip(starts, cells.get(name, [None] * count), strict=True):
            mark, tone = outcome(found)
            strip.append((mark, styled(tone, f"{start:%m-%d %H:%M} {found}") if found else tone))
        tip = "; ".join(f"{item.finding}: {item.action}" for item in status.found) or word.lower()
        when = latest.get(name)
        rows.append(
            join(
                span(f"{glyph} ", style),
                fit(span(name, styled("bright", tip, to_pipeline(name))), LABEL),
                span(" "),
                strip,
                fit(span(f"{age(view.now - when)} ago" if when else "-", "muted"), 11, "right"),
                span("  "),
                fit(
                    span(status.headline, styled("muted" if style == "ok" else style, tip)), reason
                ),
            )
        )
    axis = join(span(" " * (2 + LABEL + 1)), time_axis(view.scope.start, view.scope.end, count))
    return panel("PIPELINES", [*rows, axis, OUTCOME_LEGEND], size)


def upcoming(items: list[Advice], size: int) -> Block:
    """The first thing to do for each pipeline that needs something, worst first."""
    if not items:
        return clear("NEXT", size, "nothing needs doing")
    first: dict[str, Advice] = {}
    for item in items:
        first.setdefault(item.pipeline, item)
    lines: Block = []
    for number, item in enumerate(list(first.values())[:NEXT], 1):
        style = STATES[item.level][1]
        name = item.pipeline.split(", ")[0]
        tip = f"{item.finding} ({item.evidence})"
        lines.append(
            join(
                span(f"{number} ", style),
                fit(span(item.pipeline, styled("bright", to=to_pipeline(name))), LABEL),
                span(" "),
                span(
                    item.action, styled("text", tip, to_run(item.run_id) if item.run_id else None)
                ),
            )
        )
    if len(items) > len(lines):
        lines.append(span(f"+{len(items) - len(lines)} more on Actions", "dim"))
    return panel("NEXT", lines, size, accent=STATES[items[0].level][1])

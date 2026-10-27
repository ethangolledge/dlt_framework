"""Mission control: the 30-second morning glance."""

from dashboard.model import overview
from dashboard.pages.base import OUTCOME_LEGEND, Context, empty, level, outcome, outcome_cell, page
from dashboard.render.primitives import (
    Column,
    banner,
    card,
    matrix,
    panel,
    share,
    side_by_side,
    sparkline,
    table,
    time_axis,
)
from dashboard.render.text import Block, fit, join, span, vstack
from dashboard.theme import level_style
from dashboard.units import age, seconds


@page("overview", "Mission control", "Is everything running? What needs opening?")
def draw(context: Context) -> Block:
    view, size = context.view, context.size
    half = share(size, 2)
    return vstack(
        banner("mission control", size, overview.scope_label(view.scope)),
        cards(context),
        run_matrix(context),
        side_by_side([health(context, half), pipelines(context, half)], size),
        ticker(context),
    )


def cards(context: Context) -> Block:
    kpis = overview.kpis(context.view)
    size = share(context.size, len(kpis))
    blocks = [
        card(
            kpi.title,
            kpi.value,
            size,
            style=kpi.style,
            below=join(sparkline(kpi.trend, min(12, size - 6), kpi.style), span(" ", None))
            if any(v is not None for v in kpi.trend)
            else span(kpi.note, "muted"),
        )
        for kpi in kpis
    ]
    notes = join(
        *(join(fit(span(kpi.note, "muted"), size, "center"), span(" ", None)) for kpi in kpis)
    )
    return [*side_by_side(blocks, context.size), notes]


def run_matrix(context: Context) -> Block:
    view, size = context.view, context.size
    label = 18
    count = max(8, (size - 4 - label - 1) // 2)
    rows = overview.matrix(view, count)
    if not rows:
        return empty("RUN MATRIX", size, "no runs in this range")
    step = view.scope.length / count
    cells = [(pipeline, [outcome(cell) for cell in found]) for pipeline, found in rows]
    body = matrix(cells, label)
    axis = join(
        span(" " * (label + 1), None), time_axis(view.scope.start, view.scope.end, count * 2)
    )
    title = f"RUN MATRIX ─ one cell {seconds(step.total_seconds())} ─ worst outcome shown"
    return panel(title, [*body, axis, OUTCOME_LEGEND], size)


def health(context: Context, size: int) -> Block:
    if context.signals is None:
        return empty("HEALTH", size, "no [health] thresholds stated in this environment")
    if not context.signals:
        return panel(
            "HEALTH ─ now",
            [span("✔ ALL CLEAR  nothing failed, late, slow, or unusual", "ok")],
            size,
        )
    order = sorted(context.signals, key=lambda signal: (signal.level != "error", signal.pipeline))
    rows = [
        join(
            level(signal.level),
            span(f"  {signal.pipeline:<16.16} ", "text"),
            span(f"{signal.event:<19}", "accent"),
            span(signal.detail, f"muted|{signal.detail}"),
        )
        for signal in order
    ]
    return panel(
        "HEALTH ─ now",
        rows,
        size,
        accent="error" if any(s.level == "error" for s in order) else "warn",
    )


def pipelines(context: Context, size: int) -> Block:
    rows = overview.pipelines(context.view)
    if not rows:
        return empty("PIPELINES", size, "no runs in this range")
    columns = [
        Column("pipeline", 18),
        Column("last", 8),
        Column("duration", 12),
        Column("rows", 12),
        Column("mem", 5, "right"),
        Column("fail", 4, "right"),
    ]
    body = [
        [
            row["pipeline"],
            join(outcome_cell(row["outcome"]), span(" " + age(row["age"]), "muted")),
            sparkline(row["durations"], 12, "info"),
            sparkline(row["rows"], 12, "ok"),
            span(
                "-" if row["memory"] is None else f"{row['memory']:.0%}",
                level_style(row["memory"] or 0),
            ),
            span(str(row["failures"]), "error" if row["failures"] else "dim"),
        ]
        for row in rows
    ]
    return panel("PIPELINES", table(columns, body), size)


def ticker(context: Context) -> Block:
    events = overview.ticker(context.view)
    if not events:
        return []
    line = join(
        *(
            join(
                span(" >> ", "accent"),
                span(f"{when:%H:%M:%S} ", "muted"),
                span(text, outcome(name)[1]),
            )
            for when, name, text in events
        )
    )
    return [span("─" * context.size, "dim"), fit(line, context.size)]

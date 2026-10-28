"""Pipeline: what is wrong with this one, since when, and what to do?

Its state and why, four numbers against the period before, every run on a time axis (each
links to its run), how long runs take and how much memory they need, then its to-do list and
latest runs.
"""

from statistics import median
from typing import Any

import polars as pl

from dashboard.model import capacity, overview, triage
from dashboard.model.facts import View
from dashboard.pages.base import (
    STATES,
    Context,
    clear,
    compared,
    empty,
    outcome,
    outcome_cell,
    page,
    styled,
    to_run,
    todo,
)
from dashboard.render.primitives import (
    Column,
    banner,
    card,
    chart,
    gantt,
    panel,
    share,
    side_by_side,
    table,
)
from dashboard.render.text import Block, Line, fit, join, span, vstack
from dashboard.theme import level_style
from dashboard.units import age, change, human, mb, seconds

LATEST = 10  # runs listed


@page("pipeline", "Pipeline", picks="pipeline")
def draw(context: Context) -> Block:
    view, size = context.view, context.size
    found = triage.actions(view, context.signals)
    states = {status.pipeline: status for status in triage.statuses(view, found)}
    name = context.focus or next(iter(states), None)
    if name not in states:
        return vstack(
            banner("pipeline", size),
            empty("PIPELINE", size, "no pipeline chosen, and none in this range"),
        )
    status, mine = states[name], view.only(name)
    glyph, style, word = STATES[status.level]
    items = [item for item in found if item.concerns(name)]
    half = share(size, 2)
    return vstack(
        banner("pipeline", size, f"{name} · {overview.scope_label(view.scope)}"),
        [fit(join(span(f"{glyph} {word}  ", style), span(status.headline, style)), size)],
        [[]],
        cards(mine, size),
        runs(mine, size),
        side_by_side([durations(mine, half), peaks(mine, half)], size),
        panel("TO DO", todo(items, size - 4, named=False), size, accent=style)
        if items
        else clear("TO DO", size, "nothing to do"),
        latest(mine, size),
    )


def cards(view: View, size: int) -> Block:
    """Is it working, how long it usually takes, how full its memory gets, how fresh its data."""
    before = view.scope.previous().keep(view.history)
    rate = overview.kpis(view)[0]
    took, took_before = _typical(view.runs), _typical(before)
    memory = capacity.memory(view).to_dicts()
    fresh = overview.freshness(view).to_dicts()
    width = share(size, 4)
    share_ = memory[0]["share"] if memory else None
    blocks = [
        card(
            "SUCCESS", rate.value, width, style=rate.style, below=span(compared(rate.note), "muted")
        ),
        card(
            "TYPICAL RUN",
            seconds(took),
            width,
            style="bright",
            below=span(compared(change(took or 0, took_before or 0)), "muted"),
        ),
        card(
            "PEAK MEMORY",
            "-" if share_ is None else f"{share_:.0%}",
            width,
            style="muted" if share_ is None else level_style(share_),
            below=span(f"p99 of {mb(memory[0]['available_mb'])}" if memory else "-", "muted"),
        ),
        card(
            "LAST SUCCESS",
            age(fresh[0]["age"]) if fresh else "-",
            width,
            style="ok" if fresh and fresh[0]["age"] is not None else "error",
            below=span("ago" if fresh and fresh[0]["age"] is not None else "never", "muted"),
        ),
    ]
    return side_by_side(blocks, size)


def _typical(runs: pl.DataFrame) -> float | None:
    done = runs.filter(pl.col("outcome").is_in(overview.OK))["duration_s"].drop_nulls()
    return median(done.to_list()) if done.len() else None


def runs(view: View, size: int) -> Block:
    if view.runs.is_empty():
        return empty("RUNS", size, "no runs in this range")
    intervals = []
    for run in view.runs.sort("started_at").iter_rows(named=True):
        glyph, style = outcome(run["outcome"])
        tip = (
            f"{run['started_at']:%m-%d %H:%M} {run['outcome']} {seconds(run['duration_s'])} "
            f"{human(run['rows'] or 0)} rows"
        )
        intervals.append(
            (run["started_at"], run["ended_at"], glyph, styled(style, tip, to_run(run["run_id"])))
        )
    body = gantt([("", intervals)], view.scope.start, view.scope.end, size - 4, label=0)
    return panel("RUNS", body, size)


def durations(view: View, size: int) -> Block:
    values = view.runs.sort("started_at")["duration_s"].to_list()
    if not any(value is not None for value in values):
        return empty("DURATION", size, "no runs in this range")
    return panel("DURATION", chart([(values, "info")], size - 4, 6, label=seconds), size)


def peaks(view: View, size: int) -> Block:
    ordered = view.runs.sort("started_at")
    values = ordered["peak_rss_mb"].to_list()
    if not any(value is not None for value in values):
        return empty("PEAK MEMORY", size, "no peaks recorded in this range")
    latest = ordered.row(-1, named=True)
    limit = latest["memory_limit_mb"] or latest["host_memory_mb"]
    body = chart(
        [(values, "accent")],
        size - 4,
        6,
        label=mb,
        limit=(float(limit), "error") if limit else None,
    )
    return panel("PEAK MEMORY", body, size)


def latest(view: View, size: int) -> Block:
    if view.runs.is_empty():
        return []
    columns = [
        Column("started", 11),
        Column("outcome", 12),
        Column("took", 7, "right"),
        Column("rows", 7, "right"),
        Column("peak", 6, "right"),
        Column("version", 9),
        Column("run", 9),
    ]
    rows = [_row(run) for run in view.runs.head(LATEST).iter_rows(named=True)]
    return panel("LATEST RUNS", table(columns, rows), size)


def _row(run: dict[str, Any]) -> list[Line | str]:
    return [
        f"{run['started_at']:%m-%d %H:%M}",
        join(outcome_cell(run["outcome"]), span(f" {run['outcome']}", outcome(run["outcome"])[1])),
        seconds(run["duration_s"]),
        human(run["rows"] or 0),
        span(mb(run["peak_rss_mb"]), level_style(run["memory_share"] or 0)),
        run["code_version"] or "-",
        span(run["run_id"][:8], styled("info", "open this run", to_run(run["run_id"]))),
    ]

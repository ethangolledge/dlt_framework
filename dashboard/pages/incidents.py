"""Incidents: what broke, is it still broken, and how often?

Open incidents first, each with all its causes, its runs, and what to run once fixed; then
those resolved in the range, with how long they lasted: the history. Last, causes by day,
which tells one recurring bug from many different ones. The range, pipeline, and mode
filters narrow all three.
"""

from datetime import datetime

from dashboard.model import incidents
from dashboard.model.incidents import Incident
from dashboard.pages.base import Context, clear, header, lasted, page, styled, to_run, two_lines
from dashboard.render.primitives import panel
from dashboard.render.text import Block, Line, fit, join, span, vstack
from dashboard.theme import SPARK

DAYS = 14  # columns of causes by day
RUNS_SHOWN = 6  # run ids listed per incident


@page("incidents", "Incidents")
def draw(context: Context) -> Block:
    view, size = context.view, context.size
    found = incidents.incidents(view)
    still = [item for item in found if item.open]
    resolved = [item for item in found if not item.open]
    modes = " and ".join(view.scope.modes) if view.scope.modes else "runs and backfills"
    answer = span(f"✖ {len(still)} OPEN", "error") if still else span("✔ NOTHING OPEN", "ok")
    return vstack(
        header("incidents", context, modes),
        [fit(join(answer, span(f"   {len(resolved)} resolved in range", "muted")), size), []],
        panel("OPEN", _lines(still, view.now, size - 4), size, accent="error")
        if still
        else clear("OPEN", size, "no open incidents"),
        panel("RESOLVED", _lines(resolved, view.now, size - 4), size)
        if resolved
        else clear("RESOLVED", size, "none in range"),
        causes(found, view.now, size),
    )


def _lines(found: list[Incident], now: datetime, size: int) -> Block:
    lines: Block = []
    for item in found:
        latest = item.latest
        when = (
            f"since {item.started_at:%m-%d %H:%M}, for {lasted(item.lasted(now))}"
            if item.open
            else f"{item.started_at:%m-%d %H:%M} → {item.resolved_at:%m-%d %H:%M}, "
            f"lasted {lasted(item.lasted(now))}"
        )
        head = join(
            span(f"{when} · {len(item.runs)} run(s) · ", "error" if item.open else "muted"),
            span(item.summary, "text"),
        )
        runs: Line = [("runs ", "dim")]
        for run in item.runs[:RUNS_SHOWN]:
            tip = f"{run['started_at']:%m-%d %H:%M} {incidents.cause(run)}"
            runs.append(
                (f"{run['run_id'][:8]} ", styled("info", tip, to_run(item.pipeline, run["run_id"])))
            )
        if len(item.runs) > RUNS_SHOWN:
            runs.append((f"+{len(item.runs) - RUNS_SHOWN}", "dim"))
        under = join(
            span(f"latest: {incidents.detail(latest)}", "muted"),
            span(f"  → $ {incidents.rerun(latest)}", "ok") if item.open else [],
        )
        mode = "" if item.mode == "run" else f" ({item.mode})"
        glyph = span("✖", "error") if item.open else span("✔", "ok")
        lines += two_lines(glyph, item.pipeline + mode, head, under, size, pipeline=item.pipeline)
        lines.append(fit(join(span(" " * 25), runs), size))
    return lines


def causes(found: list[Incident], now: datetime, size: int) -> Block:
    """Broken runs per cause per day: a shade a day, darker for more."""
    rows = incidents.by_day(found, DAYS, now)
    if not rows:
        return []
    label = 34
    lines: Block = []
    for name, counts in rows:
        top = max(counts)
        cells: Line = [
            (
                SPARK[min(len(SPARK) - 1, round(count / top * (len(SPARK) - 1)))] if count else "·",
                "error" if count else "dim",
            )
            for count in counts
        ]
        lines.append(
            fit(
                join(
                    fit(span(name, "text"), label),
                    span(" "),
                    cells,
                    span(f"  {sum(counts)}", "muted"),
                ),
                size - 4,
            )
        )
    first = now.date().toordinal() - DAYS + 1
    axis = f"{datetime.fromordinal(first):%m-%d}".ljust(DAYS - 5) + f"{now:%m-%d}"
    lines.append(join(span(" " * (label + 1)), span(axis, "muted")))
    return panel(f"CAUSES BY DAY · {DAYS} DAYS", lines, size)

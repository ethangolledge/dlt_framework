"""Backfills: what history is missing, and how do I fill it?

Backfills are ad hoc jobs, separate from production runs. Per resource that declares a
history: how much of it is loaded, and a row for every range missing, with its dates, why it
is missing, and the command that fills it. Then the holes in incremental cursors, and the
backfill jobs themselves, with where a failed one stopped.
"""

import polars as pl

from dashboard.model import gaps
from dashboard.model.gaps import History, Missing
from dashboard.model.incidents import BROKEN, cause
from dashboard.pages.base import (
    Context,
    clear,
    header,
    outcome_word,
    page,
    styled,
    to_run,
)
from dashboard.render.primitives import Column, legend, panel, table, wrap
from dashboard.render.text import Block, fit, join, span, vstack
from dashboard.units import edge, human, seconds

_KINDS = {  # kind: (label, style)
    "failed": ("FAILED", "error"),
    "gap": ("GAP", "error"),
    "hole": ("HOLE", "error"),
    "empty": ("EMPTY", "warn"),
    "off": ("OFF", "warn"),
    "reset": ("RESET", "warn"),
    "twice": ("TWICE", "muted"),
}


KEY = legend(
    [
        ("GAP", "error", "never loaded"),
        ("FAILED", "error", "a job failed there"),
        ("HOLE", "error", "cursor skipped"),
        ("EMPTY", "warn", "no rows between full windows"),
        ("OFF", "warn", "differs from the source"),
    ]
)


@page("backfills", "Backfills")
def draw(context: Context) -> Block:
    view, size = context.view, context.size
    declared = gaps.histories(view, cells=max(20, size - 4 - 50))
    holes = gaps.holes(view)
    jobs = gaps.jobs(view)
    if not declared and not holes and jobs.is_empty():
        return vstack(header("backfills", context, "backfills"), _none(size))
    return vstack(
        header("backfills", context, "backfills"),
        _answer(declared, holes, size),
        *(history(item, size) for item in declared),
        cursor_holes(holes, size),
        listing(jobs, size),
        [fit(KEY, size)],
    )


def _answer(declared: list[History], holes: list[Missing], size: int) -> Block:
    missing = [item for h in declared for item in h.missing if item.kind in gaps.FIXABLE]
    missing += [item for item in holes if item.kind in gaps.FIXABLE]
    if not missing:
        return [span("✔ NOTHING MISSING  every declared history is loaded", "ok"), []]
    resources = len({item.resource_id for item in missing})
    return [
        fit(span(f"✖ {len(missing)} RANGE(S) MISSING in {resources} resource(s)", "error"), size),
        [],
    ]


def _none(size: int) -> Block:
    hint = (
        "A resource declares its history with backfill=TimeBackfill(start=..., every=...) or "
        "ValueBackfill(...); `pipeline backfill NAME` then loads it window by window, and this "
        "page shows what is loaded and what is missing."
    )
    return panel(
        "BACKFILLS",
        [
            span("No resource declares a history, and no backfill has run in range.", "text"),
            *wrap(hint, size - 4, "muted"),
        ],
        size,
    )


def history(item: History, size: int) -> Block:
    """A declared history: its extent, a ribbon of what is loaded, and each range missing."""
    ribbon = join(*(span("█" if cell else "░", "ok" if cell else "error") for cell in item.cells))
    portion = "open-ended" if item.fraction is None else f"{item.fraction:.0%} loaded"
    lines: Block = [
        join(
            span(f"{edge(item.start)} → {edge(item.end) if item.end else 'open'}  ", "muted"),
            span("▕", "dim"),
            ribbon,
            span("▏ ", "dim"),
            span(portion, "ok" if not item.missing else "warn"),
        )
    ]
    lines += _rows(item.missing, size - 4) or [span("✔ nothing missing", "ok")]
    title = f"{item.resource_id} · {item.window}"
    worst = "error" if any(m.kind in ("failed", "gap") for m in item.missing) else "info"
    return panel(title, lines, size, accent=worst)


def _rows(missing: list[Missing], size: int) -> Block:
    """A line per range: kind, edges, why; the command under it."""
    lines: Block = []
    for item in missing:
        label, style = _KINDS[item.kind]
        lines.append(
            fit(
                join(
                    span(f"{label:<7}", style),
                    span(f"{gaps.edges(item):<36} ", "text"),
                    span(item.why, "muted"),
                ),
                size,
            )
        )
        if item.command:
            lines.append(fit(join(span("        $ ", "accent"), span(item.command, "ok")), size))
    return lines


def cursor_holes(holes: list[Missing], size: int) -> Block:
    if not holes:
        return []
    by_resource: dict[str, list[Missing]] = {}
    for item in holes:
        by_resource.setdefault(item.resource_id, []).append(item)
    lines: Block = []
    for resource_id, items in by_resource.items():
        lines.append(span(resource_id, "bright"))
        lines += _rows(items, size - 4)
    return panel("INCREMENTAL CURSORS", lines, size, accent="error")


def listing(jobs: pl.DataFrame, size: int) -> Block:
    """Each backfill job; under a failed one, the window it stopped at and how to resume."""
    if jobs.is_empty():
        return clear("JOBS", size, "no backfill has run in range")
    columns = [
        Column("started", 11),
        Column("pipeline", 18),
        Column("outcome", 12),
        Column("windows", 7, "right"),
        Column("rows", 7, "right"),
        Column("took", 7, "right"),
        Column("cause", max(12, size - 4 - 86)),
        Column("run", 8),
    ]
    lines: Block = []
    for job in jobs.head(20).iter_rows(named=True):
        lines += table(
            columns,
            [
                [
                    f"{job['started_at']:%m-%d %H:%M}",
                    job["pipeline_id"],
                    outcome_word(job["outcome"]),
                    str(job["windows"]),
                    "-" if job["rows"] is None else human(job["rows"]),
                    seconds(job["duration_s"]),
                    span(cause(job) if job["outcome"] in BROKEN else "", "muted"),
                    span(
                        job["run_id"][:8],
                        styled("info", "open this run", to_run(job["pipeline_id"], job["run_id"])),
                    ),
                ]
            ],
        )[0 if not lines else 2 :]
        if job["resume"]:
            window = str(job["error_window"] or "").strip("[)").split(",")[0]
            stopped = f"stopped at {edge(window)}" if window else "stopped"
            lines.append(
                fit(
                    join(
                        span(f"            {stopped}  → ", "warn"),
                        span(f"$ {job['resume']}", "ok"),
                    ),
                    size - 4,
                )
            )
    return panel("JOBS", lines, size)

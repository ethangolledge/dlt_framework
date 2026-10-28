"""What every page shares: its context, how it is registered, and the pieces several draw.

Pages follow one story, so a reader never has to work out where to look:

- a page answers one question, and its first line is the answer (the verdict);
- what is wrong comes first and in colour; what is fine is quiet, or one line;
- panels carry titles, not descriptions; a number's detail is in its tooltip;
- every pipeline and run named links one level down: overview, pipeline, run.
"""

from collections.abc import Callable
from dataclasses import dataclass

from dashboard.model.advisor import Advice
from dashboard.model.facts import View
from dashboard.model.triage import Status
from dashboard.render.primitives import legend, panel
from dashboard.render.text import Block, Line, fit, join, span
from dashboard.theme import LEVELS, OUTCOMES
from dlt_framework.core.monitoring.health import Signal


@dataclass(frozen=True, slots=True)
class Context:
    """What a page draws from: the facts in scope, the width to fill, the health signals
    (``None`` where this environment states no thresholds), and the run or pipeline chosen."""

    view: View
    size: int
    signals: list[Signal] | None = None
    run_id: str | None = None
    focus: str | None = None  # the pipeline a pipeline page shows


@dataclass(frozen=True, slots=True)
class Page:
    key: str
    title: str
    draw: Callable[[Context], Block]
    picks: str | None = None  # "run" or "pipeline": what the frontend offers to choose


PAGES: dict[str, Page] = {}


def page(
    key: str, title: str, picks: str | None = None
) -> Callable[[Callable[[Context], Block]], Callable[[Context], Block]]:
    """Register a page's draw function under ``key``, in the order pages are defined."""

    def register(draw: Callable[[Context], Block]) -> Callable[[Context], Block]:
        PAGES[key] = Page(key, title, draw, picks)
        return draw

    return register


def styled(style: str, tip: str | None = None, to: str | None = None) -> str:
    """A segment's style with a hover tip and a link one level down (``to_pipeline``,
    ``to_run``): ``"ok|tip|pipeline/erp_sql"``. A terminal shows neither."""
    tip = (tip or "").replace("|", "¦")
    return f"{style}|{tip}|{to}" if to else f"{style}|{tip}" if tip else style


def to_pipeline(name: str) -> str:
    return f"pipeline/{name}"


def to_run(run_id: str) -> str:
    return f"run/{run_id}"


def outcome(name: str | None) -> tuple[str, str]:
    """A run outcome's ``(glyph, style)``."""
    return OUTCOMES[name or "none"]


def outcome_cell(name: str | None, tip: str | None = None) -> Line:
    glyph, style = outcome(name)
    return span(glyph, styled(style, tip))


OUTCOME_LEGEND = legend(
    [
        (OUTCOMES["succeeded"][0], "ok", "ok"),
        (OUTCOMES["retried"][0], "warn", "retried"),
        (OUTCOMES["failed"][0], "error", "failed"),
        (OUTCOMES["lost"][0], "error", "killed"),
        (OUTCOMES["running"][0], "info", "running"),
        (OUTCOMES["none"][0], "dim", "none"),
    ]
)

# A pipeline's state: glyph, style, and the word for it.
STATES = {
    "error": ("✖", "error", "FAILING"),
    "warning": ("⚠", "warn", "AT RISK"),
    "ok": ("█", "ok", "HEALTHY"),
    "idle": ("·", "muted", "IDLE"),
}


def verdict(statuses: list[Status], size: int) -> Block:
    """The answer to "does anything need me?": a count per state, and a stripe across the
    width in their proportions."""
    if not statuses:
        return [span("NO SIGNAL  no pipelines in this range", "muted")]
    counts = {key: sum(status.level == key for status in statuses) for key in STATES}
    if counts["error"] + counts["warning"] == 0:
        head = span(f"✔ ALL CLEAR  {len(statuses)} pipeline(s) healthy", "ok")
    else:
        head = join(
            *(
                join(
                    span(f"{STATES[key][0]} {counts[key]} {STATES[key][2]}", STATES[key][1]),
                    span("    ", None),
                )
                for key in STATES
                if counts[key]
            )
        )
    stripe: Line = []
    for key in STATES:
        stripe.extend([("▀", STATES[key][1])] * round(counts[key] / len(statuses) * size))
    return [fit(head, size), fit(stripe, size)]


def compared(note: str) -> str:
    """A change against the period before, or nothing when there was nothing then."""
    return "" if note in ("-", "new") else note


def level(name: str) -> Line:
    text, style = LEVELS[name]
    return span(text, style)


def todo(items: list[Advice], size: int, named: bool = True, label: int = 18) -> Block:
    """Findings, two lines each: level, pipeline (unless ``named`` is off), and the headline,
    with its evidence dimmed; then the action under it, linked to the run that shows it."""
    lines: Block = []
    indent = label + 3 if named else 2
    for item in items:
        glyph, style = LEVELS[item.level][0][0], LEVELS[item.level][1]
        first = item.pipeline.split(", ")[0]
        name = (
            join(
                fit(span(item.pipeline, styled("bright", to=to_pipeline(first))), label), span(" ")
            )
            if named
            else []
        )
        head = join(
            span(f"{glyph} ", style),
            name,
            span(item.finding, styled(style, item.evidence)),
            span(f"   {item.evidence}", "dim"),
        )
        target = to_run(item.run_id) if item.run_id else None
        action = span(f"→ {item.action}", styled("text", item.also or None, target))
        lines += [fit(head, size), fit(join(span(" " * indent), action), size)]
    return lines


def empty(title: str, size: int, why: str) -> Block:
    """A panel with nothing to show says so, and why."""
    return panel(title, [span("NO SIGNAL", "muted"), span(why, "dim")], size)


def clear(title: str, size: int, what: str) -> Block:
    """A panel whose check passed: one quiet line."""
    return panel(title, [span(f"✔ {what}", "ok")], size)


def nature_style(nature: str | None) -> str:
    """Busy steps compute (accent); waiting ones wait on something outside (info)."""
    return {"busy": "accent", "waiting": "info", "mixed": "warn"}.get(nature or "", "muted")

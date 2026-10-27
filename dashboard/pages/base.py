"""What every page shares: its context, how it is registered, and the pieces several draw."""

from collections.abc import Callable
from dataclasses import dataclass

from dashboard.model.facts import View
from dashboard.render.primitives import legend, panel
from dashboard.render.text import Block, Line, span
from dashboard.theme import LEVELS, OUTCOMES
from dlt_framework.core.monitoring.health import Signal


@dataclass(frozen=True, slots=True)
class Context:
    """What a page draws from: the facts in scope, the width to fill, the health signals
    (``None`` where this environment states no thresholds), and a chosen run."""

    view: View
    size: int
    signals: list[Signal] | None = None
    run_id: str | None = None


@dataclass(frozen=True, slots=True)
class Page:
    key: str
    title: str
    summary: str  # what the page answers, shown under its name
    draw: Callable[[Context], Block]
    picks_run: bool = False  # whether the frontend offers a run to choose


PAGES: dict[str, Page] = {}


def page(
    key: str, title: str, summary: str, picks_run: bool = False
) -> Callable[[Callable[[Context], Block]], Callable[[Context], Block]]:
    """Register a page's draw function under ``key``, in the order pages are defined."""

    def register(draw: Callable[[Context], Block]) -> Callable[[Context], Block]:
        PAGES[key] = Page(key, title, summary, draw, picks_run)
        return draw

    return register


def outcome(name: str | None) -> tuple[str, str]:
    """A run outcome's ``(glyph, style)``."""
    return OUTCOMES[name or "none"]


def outcome_cell(name: str | None, tip: str | None = None) -> Line:
    glyph, style = outcome(name)
    return span(glyph, f"{style}|{tip}" if tip else style)


OUTCOME_LEGEND = legend(
    [
        (OUTCOMES["succeeded"][0], "ok", "ok"),
        (OUTCOMES["retried"][0], "warn", "ok after retries"),
        (OUTCOMES["failed"][0], "error", "failed"),
        (OUTCOMES["lost"][0], "error", "lost (killed)"),
        (OUTCOMES["running"][0], "info", "running"),
        (OUTCOMES["none"][0], "dim", "nothing ran"),
    ]
)


def level(name: str) -> Line:
    text, style = LEVELS[name]
    return span(text, style)


def empty(title: str, size: int, why: str) -> Block:
    """A panel with nothing to show says so, and why."""
    return panel(title, [span("NO SIGNAL", "muted"), span(why, "dim")], size)


def nature_style(nature: str | None) -> str:
    """Busy steps compute (accent); waiting ones wait on something outside (info)."""
    return {"busy": "accent", "waiting": "info", "mixed": "warn"}.get(nature or "", "muted")

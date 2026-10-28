"""What every page shares: its context, how it is registered, and the pieces several draw.

Pages follow the same rules (``docs/dashboard-design.md``):

- a page answers one question, and its first line is the answer;
- the filters in force are always shown, and so is what is selected;
- what is wrong comes first and in colour; what is fine is quiet, or one line;
- panels carry titles, not descriptions; every claim shows its evidence;
- a pipeline or run named links to it on the Runs page.
"""

from collections.abc import Callable
from dataclasses import dataclass
from datetime import timedelta

from dashboard.model.facts import View
from dashboard.render.primitives import banner, legend, panel
from dashboard.render.text import Block, Line, fit, join, span
from dashboard.theme import LEVELS, OUTCOMES
from dlt_framework.core.monitoring.health import Signal


@dataclass(frozen=True, slots=True)
class Context:
    """What a page draws from: the facts in scope, the width to fill, the health signals
    (``None`` where this environment states no thresholds), and the pipeline and run chosen."""

    view: View
    size: int
    signals: list[Signal] | None = None
    run_id: str | None = None
    focus: str | None = None  # the pipeline chosen on the Runs page


@dataclass(frozen=True, slots=True)
class Page:
    key: str
    title: str
    draw: Callable[[Context], Block]
    picks: str | None = None  # "pipeline": the frontend offers a pipeline and a run to choose


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
    """A segment's style with a hover tip and a link (``to_pipeline``, ``to_run``):
    ``"ok|tip|pipeline/erp_sql"``. A terminal shows neither."""
    tip = (tip or "").replace("|", "¦")
    return f"{style}|{tip}|{to}" if to else f"{style}|{tip}" if tip else style


def to_pipeline(name: str) -> str:
    """The Runs page of a pipeline."""
    return f"pipeline/{name}"


def to_run(pipeline: str, run_id: str) -> str:
    """The Runs page of a pipeline, with this run open."""
    return f"run/{pipeline}/{run_id}"


def header(title: str, context: Context, what: str) -> Block:
    """The banner, and the filters in force under it: range, pipelines, and ``what`` the page
    shows (runs, backfills, or both)."""
    scope = context.view.scope
    pipelines = ", ".join(scope.pipelines) if scope.pipelines else "all pipelines"
    shown = f"{scope.start:%Y-%m-%d %H:%M} → {scope.end:%Y-%m-%d %H:%M}Z · {pipelines} · {what}"
    return [*banner(title, context.size), fit(span(shown, "muted"), context.size), []]


def outcome(name: str | None) -> tuple[str, str]:
    """A run outcome's ``(glyph, style)``."""
    return OUTCOMES[name or "none"]


def outcome_word(name: str) -> Line:
    glyph, style = outcome(name)
    return span(f"{glyph} {'killed' if name == 'lost' else name}", style)


OUTCOME_LEGEND = legend(
    [
        (OUTCOMES["succeeded"][0], "ok", "ok"),
        (OUTCOMES["retried"][0], "warn", "retried"),
        (OUTCOMES["failed"][0], "error", "failed"),
        (OUTCOMES["lost"][0], "error", "killed"),
        (OUTCOMES["running"][0], "info", "running"),
    ]
)

# A pipeline's state: glyph, style, and the word for it.
STATES = {
    "error": ("✖", "error", "FAILING"),
    "warning": ("⚠", "warn", "AT RISK"),
    "ok": ("█", "ok", "HEALTHY"),
    "idle": ("·", "muted", "NO RUNS"),
}


def level(name: str) -> Line:
    text, style = LEVELS[name]
    return span(text, style)


def ago(delta: timedelta | None) -> str:
    """``2d ago``, ``3h ago``, ``12m ago``, ``now``; ``never`` for none."""
    if delta is None:
        return "never"
    seconds = delta.total_seconds()
    for size, unit in ((86400, "d"), (3600, "h"), (60, "m")):
        if seconds >= size:
            return f"{int(seconds // size)}{unit} ago"
    return "now"


def lasted(delta: timedelta) -> str:
    """``2d 12h``, ``1h 5m``, ``41m``, ``24s``."""
    seconds = int(delta.total_seconds())
    if seconds >= 86400:
        return f"{seconds // 86400}d {seconds % 86400 // 3600}h"
    if seconds >= 3600:
        return f"{seconds // 3600}h {seconds % 3600 // 60}m"
    return f"{seconds // 60}m" if seconds >= 60 else f"{seconds}s"


def empty(title: str, size: int, why: str) -> Block:
    """A panel with nothing to show says so, and why."""
    return panel(title, [span("NO SIGNAL", "muted"), span(why, "dim")], size)


def clear(title: str, size: int, what: str) -> Block:
    """A panel whose check passed: one quiet line."""
    return panel(title, [span(f"✔ {what}", "ok")], size)


def nature_style(nature: str | None) -> str:
    """Busy steps compute (accent); waiting ones wait on something outside (info)."""
    return {"busy": "accent", "waiting": "info", "mixed": "warn"}.get(nature or "", "muted")


def two_lines(
    glyph: Line,
    label: str,
    head: Line,
    under: Line,
    size: int,
    width: int = 22,
    pipeline: str | None = None,
) -> Block:
    """A finding's shape: glyph, label (linked to ``pipeline``, else to the first pipeline it
    names), its headline; the next line, under the headline, what to do."""
    name = pipeline or label.split(", ")[0]
    first = join(
        glyph,
        span(" "),
        fit(span(label, styled("bright", to=to_pipeline(name))), width),
        span(" "),
        head,
    )
    return [fit(first, size), fit(join(span(" " * (width + 3)), under), size)]

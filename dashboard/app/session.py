"""What both frontends do before drawing: read the frames, apply the reader's scope, judge
health, and hand a page its context. A frontend only gathers the choices and shows the result."""

from dataclasses import dataclass
from datetime import UTC, datetime

from dashboard.model import source
from dashboard.model.facts import Scope, View
from dashboard.pages import PAGES
from dashboard.pages.base import Context
from dashboard.render.text import Block
from dlt_framework.core.monitoring import Frames

RANGES = ("1h", "6h", "24h", "7d", "30d", "all")


@dataclass(frozen=True, slots=True)
class Choices:
    """Everything a reader can choose; the same in a terminal and on the web."""

    page: str = "overview"
    since: str = "24h"
    pipelines: tuple[str, ...] = ()
    modes: tuple[str, ...] = ()
    run_id: str | None = None  # the run the Runs page opens
    focus: str | None = None  # the pipeline the Runs page shows
    size: int = 140


def load(uri: str | None) -> Frames:
    """The frames at the monitoring URI: ``uri``, or this environment's ``MONITORING__URI``."""
    return source.read(source.monitoring_uri(uri))


def draw(frames: Frames, choices: Choices, now: datetime | None = None) -> Block:
    """The chosen page, drawn for the chosen scope; health is judged by this environment's
    thresholds where it states them."""
    now = now or datetime.now(UTC)
    scope = Scope.last(
        choices.since,
        now,
        pipelines=choices.pipelines,
        modes=choices.modes,
    )
    view = View.build(frames, scope, now)
    signals = source.signals(frames, list(choices.pipelines) or view.pipelines)
    return PAGES[choices.page].draw(
        Context(view, choices.size, signals, choices.run_id, choices.focus)
    )

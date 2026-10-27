"""The look, stated once: colours by meaning, and the glyph every state is drawn with.

A state is never shown by colour alone: each has a glyph, so it reads in a plain terminal and
to colour-blind eyes.
"""

from typing import Final

# Style tokens: what a colour means, not which colour it is.
COLOURS: Final[dict[str, str]] = {
    "text": "#b8ffc8",
    "bright": "#eafff0",
    "muted": "#5f9f73",
    "dim": "#1f6f3f",
    "ok": "#39ff14",
    "info": "#00f0ff",
    "accent": "#ff2bd6",
    "warn": "#ffb000",
    "error": "#ff3355",
    "background": "#05070a",
    "panel": "#080c10",
}

# A run's outcome, as one character.
OUTCOMES: Final[dict[str, tuple[str, str]]] = {
    "succeeded": ("█", "ok"),
    "retried": ("▓", "warn"),
    "failed": ("✖", "error"),
    "lost": ("☠", "error"),
    "running": ("◌", "info"),
    "none": ("·", "dim"),
}
WORST_FIRST: Final[tuple[str, ...]] = ("lost", "failed", "retried", "running", "succeeded")

LEVELS: Final[dict[str, tuple[str, str]]] = {
    "error": ("✖ ERR", "error"),
    "warning": ("⚠ WRN", "warn"),
    "info": ("● INF", "info"),
}

SPARK: Final[str] = "▁▂▃▄▅▆▇█"
EIGHTHS: Final[str] = " ▏▎▍▌▋▊▉█"
SHADES: Final[str] = " ░▒▓█"


def level_style(share: float, warn: float = 0.7, error: float = 0.9) -> str:
    """The style of a share of a limit: ok, then warn, then error as it fills."""
    return "error" if share >= error else "warn" if share >= warn else "ok"

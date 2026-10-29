"""The look, stated once: colours, states, and outcomes by meaning, shared by the browser
dashboard and the terminal.

A state is never shown by colour alone: the dashboard gives each a glyph and a word too, so it
reads in a plain terminal and to colour-blind eyes.
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

# Each state a pipeline can be in, and each way a run can end: (glyph, colour token, word).
STATES: Final[dict[str, tuple[str, str, str]]] = {
    "error": ("✖", "error", "FAILING"),
    "warning": ("⚠", "warn", "AT RISK"),
    "ok": ("█", "ok", "LAST RUN OK"),
    "idle": ("·", "muted", "NO RECENT RUNS"),
}
OUTCOMES: Final[dict[str, tuple[str, str, str]]] = {
    "succeeded": ("█", "ok", "ok"),
    "empty": ("▁", "muted", "ok, loaded nothing"),
    "retried": ("▓", "warn", "ok after retries"),
    "failed": ("✖", "error", "failed"),
    "lost": ("☠", "accent", "killed"),
    "running": ("◌", "info", "running"),
}

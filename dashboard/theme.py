"""The look, stated once: colours by meaning, shared by the browser dashboard and the terminal.

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

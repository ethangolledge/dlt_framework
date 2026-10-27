"""Encoders: one block, drawn for the web (HTML) or a terminal (ANSI colour)."""

import html as _html

from dashboard.render.text import Block
from dashboard.theme import COLOURS

_RESET = "\x1b[0m"
FONTS = (
    '<link rel="stylesheet" href="https://fonts.googleapis.com/css2?'
    'family=JetBrains+Mono:wght@400;700&family=VT323&display=swap">'
)
"""The fonts the look is designed for, loaded by the browser; monospace stands in without them."""


def html(block: Block, title: str | None = None) -> str:
    """A ``<pre>`` of styled spans (classes ``t-<style>``, styled by ``stylesheet``); a style of
    ``"ok|a tooltip"`` also shows the tooltip on hover."""
    rows = []
    for line in block:
        parts = []
        for shown, styled in line:
            style, _, tip = (styled or "").partition("|")
            cls = f' class="t-{style}"' if style else ""
            hint = f' title="{_html.escape(tip)}"' if tip else ""
            parts.append(
                f"<span{cls}{hint}>{_html.escape(shown)}</span>"
                if cls or hint
                else _html.escape(shown)
            )
        rows.append("".join(parts))
    label = f' aria-label="{_html.escape(title)}"' if title else ""
    return f'<pre class="term"{label}>' + "\n".join(rows) + "</pre>"


def ansi(block: Block, colour: bool = True) -> str:
    """The block for a terminal, in 24-bit colour, or plain when ``colour`` is off."""
    rows = []
    for line in block:
        parts = []
        for shown, styled in line:
            style = (styled or "").partition("|")[0]
            if colour and style in COLOURS:
                red, green, blue = _rgb(COLOURS[style])
                parts.append(f"\x1b[38;2;{red};{green};{blue}m{shown}{_RESET}")
            else:
                parts.append(shown)
        rows.append("".join(parts))
    return "\n".join(rows)


def stylesheet(scanlines: bool = True) -> str:
    """CSS for ``html`` blocks: black, glowing phosphor, optional CRT scanlines."""
    styles = "\n".join(
        f".t-{name} {{ color: {value}; text-shadow: 0 0 6px {value}55; }}"
        for name, value in COLOURS.items()
    )
    lines = (
        "background-image: repeating-linear-gradient(0deg, rgba(0,255,120,.035) 0 1px, "
        "transparent 1px 3px);"
        if scanlines
        else ""
    )
    return f"""
.term {{
  font-family: 'JetBrains Mono', 'IBM Plex Mono', ui-monospace, monospace;
  font-size: 13px; line-height: 1.25; margin: 0 0 10px 0; padding: 10px 12px;
  color: {COLOURS["text"]}; background: {COLOURS["panel"]}; border-radius: 2px;
  white-space: pre; overflow-x: auto; {lines}
  box-shadow: inset 0 0 40px rgba(0,255,120,.06), 0 0 0 1px {COLOURS["dim"]}66;
}}
.term span[title] {{ cursor: help; }}
{styles}
"""


def _rgb(colour: str) -> tuple[int, int, int]:
    value = colour.lstrip("#")
    return int(value[0:2], 16), int(value[2:4], 16), int(value[4:6], 16)

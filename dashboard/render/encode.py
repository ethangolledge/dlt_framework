"""A block of styled text for a terminal: 24-bit colour, or plain."""

from dashboard.render.text import Block
from dashboard.theme import COLOURS

_RESET = "\x1b[0m"


def ansi(block: Block, colour: bool = True) -> str:
    """The block for a terminal, in 24-bit colour, or plain when ``colour`` is off. A style may
    carry a tooltip after ``|``; a terminal shows only the text."""
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


def _rgb(colour: str) -> tuple[int, int, int]:
    value = colour.lstrip("#")
    return int(value[0:2], 16), int(value[2:4], 16), int(value[4:6], 16)

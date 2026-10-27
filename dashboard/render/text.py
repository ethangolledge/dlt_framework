"""Styled text: what every primitive draws and every encoder writes.

A ``Line`` is a list of ``(text, style)`` segments; a ``Block`` is a list of lines. Styles are
theme tokens (``ok``, ``warn``, ...), optionally with a tooltip (``"ok|run 3f9f… 2m"``), or
``None`` for the default text colour. Every character
here is one cell wide, so a line's width is its length.
"""

from collections.abc import Iterable

Segment = tuple[str, str | None]
Line = list[Segment]
Block = list[Line]


def span(text: str, style: str | None = None) -> Line:
    return [(text, style)]


def width(line: Line) -> int:
    return sum(len(text) for text, _ in line)


def fit(line: Line, size: int, align: str = "left") -> Line:
    """``line`` cropped or padded to ``size`` cells; a cropped line ends with ``…``."""
    length = width(line)
    if length > size:
        return [*crop(line, size - 1), ("…", "muted")] if size > 0 else []
    pad = [(" " * (size - length), None)]
    if align == "right":
        return pad + line
    if align == "center":
        left = (size - length) // 2
        return [(" " * left, None), *line, (" " * (size - length - left), None)]
    return line + pad


def crop(line: Line, size: int) -> Line:
    out: Line = []
    left = size
    for text, style in line:
        if left <= 0:
            break
        out.append((text[:left], style))
        left -= len(text[:left])
    return out


def join(*lines: Line) -> Line:
    return [segment for line in lines for segment in line]


def plain(block: Block) -> str:
    """The block without styles: what a test compares."""
    return "\n".join("".join(text for text, _ in line) for line in block)


def block_width(block: Block) -> int:
    return max((width(line) for line in block), default=0)


def hstack(blocks: Iterable[Block], gap: int = 1) -> Block:
    """Blocks side by side, top-aligned, each padded to its own width."""
    blocks = [block for block in blocks if block]
    if not blocks:
        return []
    height = max(len(block) for block in blocks)
    widths = [block_width(block) for block in blocks]
    rows: Block = []
    for index in range(height):
        row: Line = []
        for column, (block, size) in enumerate(zip(blocks, widths, strict=True)):
            if column:
                row.append((" " * gap, None))
            row.extend(fit(block[index] if index < len(block) else [], size))
        rows.append(row)
    return rows


def vstack(*blocks: Block, gap: int = 0) -> Block:
    out: Block = []
    for index, block in enumerate(block for block in blocks if block):
        if index:
            out.extend([[]] * gap)
        out.extend(block)
    return out

"""ASCII primitives: everything a page draws, as styled text of a stated width.

Each takes plain values (numbers, datetimes, labels and theme styles) and knows nothing of the
frames, so any page, and any frontend, can use any of them.
"""

import math
import textwrap
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import datetime

from dashboard.render.text import Block, Line, block_width, fit, hstack, join, span, vstack, width
from dashboard.theme import EIGHTHS, SHADES, SPARK

Interval = tuple[
    datetime, datetime, str, str
]  # start, end, glyph, style (style may hold a tooltip)


def panel(title: str, body: Block, size: int, style: str = "dim", accent: str = "info") -> Block:
    """``body`` in a box ``size`` cells wide, its title set into the top edge."""
    inner = size - 4
    head = f"─ {title} " if title else ""
    top = join(
        span("┌", style), span(head, accent), span("─" * max(0, size - 2 - len(head)) + "┐", style)
    )
    rows = [join(span("│ ", style), fit(line, inner), span(" │", style)) for line in body or [[]]]
    return [top, *rows, span("└" + "─" * (size - 2) + "┘", style)]


def card(
    title: str, value: str, size: int, *, style: str = "ok", below: Line | None = None
) -> Block:
    """A KPI: its value large, and a line beneath (a sparkline, a change)."""
    body = [fit(span(value, style), size - 4, "center"), fit(below or [], size - 4, "center")]
    return panel(title, body, size, accent=style)


def sparkline(values: Sequence[float | None], size: int, style: str = "ok") -> Line:
    """``values`` as block heights, resampled to ``size`` cells; a missing value is a gap."""
    sampled = resample(values, size)
    known = [value for value in sampled if value is not None]
    if not known:
        return span(" " * size)
    low, high = min(known), max(known)
    cells = []
    for value in sampled:
        if value is None:
            cells.append(" ")
            continue
        level = 0 if high == low else (value - low) / (high - low)
        cells.append(SPARK[min(len(SPARK) - 1, int(level * (len(SPARK) - 1) + 0.5))])
    return span("".join(cells), style)


def resample(values: Sequence[float | None], size: int) -> list[float | None]:
    """``values`` as ``size`` points: the maximum of each bucket when shrinking (a peak is never
    lost), repeated when growing."""
    if not values or size <= 0:
        return [None] * max(size, 0)
    out: list[float | None] = []
    for index in range(size):
        low = index * len(values) // size
        high = max(low + 1, (index + 1) * len(values) // size)
        bucket = [value for value in values[low:high] if value is not None]
        out.append(max(bucket) if bucket else None)
    return out


def bar(fraction: float, size: int, style: str = "ok", track: str = "dim") -> Line:
    """A horizontal bar ``size`` cells long, filled to ``fraction`` in eighths of a cell."""
    fraction = min(max(fraction, 0.0), 1.0)
    eighths = round(fraction * size * 8)
    full, part = divmod(eighths, 8)
    filled = "█" * full + (EIGHTHS[part] if part and full < size else "")
    return join(span(filled, style), span("░" * (size - len(filled)), track))


def gauge(fraction: float, size: int, style: str) -> Line:
    """``[█████░░░] 62%``."""
    return join(
        span("[", "dim"),
        bar(fraction, size - 7, style),
        span("]", "dim"),
        span(f"{fraction:>4.0%}", style),
    )


def bars(rows: Sequence[tuple[str, float, str, str]], size: int, label: int = 18) -> Block:
    """Labelled bars against the largest: ``(label, value, shown value, style)``."""
    top = max((value for _, value, _, _ in rows), default=0) or 1
    shown = max((len(text) for _, _, text, _ in rows), default=0)
    length = max(4, size - label - shown - 3)
    return [
        join(
            fit(span(name, "text"), label),
            span("▕", "dim"),
            bar(value / top, length, style),
            span("▏", "dim"),
            fit(span(text, style), shown + 1, "right"),
        )
        for name, value, text, style in rows
    ]


def chart(
    series: Sequence[tuple[Sequence[float | None], str]],
    size: int,
    height: int,
    *,
    label: Callable[[float], str] = lambda value: f"{value:.0f}",
    floor: float | None = 0.0,
    limit: tuple[float, str] | None = None,
) -> Block:
    """Lines in braille (2 by 4 dots a cell), so curves stay smooth in text: each ``(values,
    style)`` spread across the width. ``limit`` draws a dotted line at a value (a memory
    limit), its label to the right."""
    axis = 7
    cols = max(4, size - axis - 1)
    known = [value for values, _ in series for value in values if value is not None]
    if limit:
        known.append(limit[0])
    if not known:
        return [fit(span("NO SIGNAL", "muted"), size, "center")]
    low = min(known) if floor is None else min(floor, min(known))
    high = max(known)
    high = high if high > low else low + 1
    dots: dict[tuple[int, int], str] = {}  # (x, y) dot -> style
    rows = height * 4
    for values, style in series:
        sampled = resample(values, cols * 2)
        previous: int | None = None
        for x, value in enumerate(sampled):
            if value is None:
                previous = None
                continue
            y = round((value - low) / (high - low) * (rows - 1))
            span_from, span_to = (y, y) if previous is None else sorted((previous, y))
            for dot in range(span_from, span_to + 1):  # join steep segments
                dots[x, dot] = style
            previous = y
    limit_row = None if not limit else round((limit[0] - low) / (high - low) * (rows - 1)) // 4
    out: Block = []
    for row in range(height - 1, -1, -1):
        cells: Line = []
        for col in range(cols):
            bits = 0
            painted: str | None = None
            for dx in (0, 1):
                for dy in range(4):
                    found = dots.get((col * 2 + dx, row * 4 + dy))
                    if found:
                        bits |= _BRAILLE[dx][dy]
                        painted = found
            if bits:
                cells.append((chr(0x2800 + bits), painted))
            elif row == limit_row and limit:
                cells.append(("·", limit[1]))
            else:
                cells.append((" ", None))
        value = low + (high - low) * (row * 4 + 3) / (rows - 1)
        tick = label(value) if row in (height - 1, 0) or row == height // 2 else ""
        out.append(
            join(
                fit(span(tick, "muted"), axis - 1, "right"),
                span("┤" if tick else "│", "dim"),
                cells,
            )
        )
    out.append(join(span(" " * (axis - 1) + "└" + "─" * cols, "dim")))
    return out


# Braille dot bits by (column, row from the bottom).
_BRAILLE = ((0x40, 0x04, 0x02, 0x01), (0x80, 0x20, 0x10, 0x08))


def gantt(
    lanes: Sequence[tuple[str, Sequence[Interval]]],
    start: datetime,
    end: datetime,
    size: int,
    label: int = 16,
) -> Block:
    """One lane per label, each interval a bar of its glyph between its edges on a shared time
    axis; an interval shorter than a cell still shows as one."""
    cols = max(8, size - label - 1)
    total = max((end - start).total_seconds(), 1.0)

    def column(moment: datetime) -> int:
        return min(cols - 1, max(0, int((moment - start).total_seconds() / total * cols)))

    out: Block = []
    for name, intervals in lanes:
        cells: Line = [("·", "dim")] * cols
        for low, high, glyph, style in sorted(intervals, key=lambda item: item[0]):
            for index in range(column(low), column(high) + 1):
                cells[index] = (glyph, style)
        out.append(join(fit(span(name, "text"), label), span(" ", None), _merge(cells)))
    out.append(join(span(" " * (label + 1), None), time_axis(start, end, cols)))
    return out


def time_axis(start: datetime, end: datetime, cols: int) -> Line:
    """Ticks at the start, middle, and end of ``[start, end)``: dates over a day, clock times
    over two minutes, and seconds from the start below that."""
    total = (end - start).total_seconds()
    if total < 120:
        ticks = ["+0s", f"+{total / 2:.1f}s", f"+{total:.1f}s"]
    else:
        fmt = "%m-%d %H:%M" if total > 86400 else "%H:%M" if total > 7200 else "%H:%M:%S"
        ticks = [moment.strftime(fmt) for moment in (start, start + (end - start) / 2, end)]
    gap = max(1, (cols - sum(len(tick) for tick in ticks)) // 2)
    return span((ticks[0] + " " * gap + ticks[1] + " " * gap + ticks[2])[:cols], "muted")


def matrix(
    rows: Sequence[tuple[str, Sequence[tuple[str, str]]]], label: int = 16, gap: bool = True
) -> Block:
    """A cell per ``(glyph, style)`` in each labelled row: the run matrix."""
    sep = " " if gap else ""
    return [
        join(
            fit(span(name, "text"), label),
            span(" ", None),
            _merge([seg for cell in cells for seg in ([cell, (sep, None)] if sep else [cell])]),
        )
        for name, cells in rows
    ]


def heatmap(
    rows: Sequence[tuple[str, Sequence[float | None]]],
    style: Callable[[float], str],
    label: int = 16,
) -> Block:
    """Shades ``░▒▓█`` of each value in ``[0, 1]``, coloured by ``style(value)``; blank for none."""
    out: Block = []
    for name, values in rows:
        cells: Line = []
        for value in values:
            if value is None:
                cells.append(("·", "dim"))
                continue
            level = min(len(SHADES) - 1, max(1, math.ceil(value * (len(SHADES) - 1))))
            cells.append((SHADES[level], style(value)))
        out.append(join(fit(span(name, "text"), label), span(" ", None), _merge(cells)))
    return out


@dataclass(frozen=True, slots=True)
class Column:
    name: str
    size: int
    align: str = "left"


def table(
    columns: Sequence[Column], rows: Sequence[Sequence[Line | str]], header: str = "info"
) -> Block:
    """Rows under a header; each cell a line or text, cropped to its column."""
    head = join(
        *(
            join(fit(span(column.name.upper(), header), column.size, column.align), span(" ", None))
            for column in columns
        )
    )
    rule = span("─" * width(head), "dim")
    body = [
        join(
            *(
                join(
                    fit(cell if isinstance(cell, list) else span(cell), column.size, column.align),
                    span(" ", None),
                )
                for column, cell in zip(columns, row, strict=True)
            )
        )
        for row in rows
    ]
    return [head, rule, *body] if body else [head, rule, span("NO SIGNAL", "muted")]


def legend(items: Sequence[tuple[str, str, str]]) -> Line:
    """``LEGEND █ ok  ✖ failed ...`` from ``(glyph, style, meaning)``."""
    parts: Line = [("LEGEND ", "muted")]
    for glyph, style, meaning in items:
        parts.extend([(glyph, style), (f" {meaning}  ", "muted")])
    return parts


def banner(title: str, size: int, subtitle: str = "") -> Block:
    """The page header: a framed, spaced-out title and the logo."""
    spaced = " ".join(title.upper())
    logo = span("▓▒░ DLT//OPS ░▒▓", "accent")
    head = join(logo, span("  ", None), span(spaced, "ok"))
    return [
        span("▄" * size, "dim"),
        join(fit(head, size - len(subtitle)), span(subtitle, "muted")),
        span("▀" * size, "dim"),
    ]


def wrap(text: str, size: int, style: str | None = None, indent: str = "") -> Block:
    """``text`` as lines of at most ``size`` cells, broken between words; follow-on lines start
    with ``indent``."""
    lines = textwrap.wrap(text, size, subsequent_indent=indent, break_on_hyphens=False) or [""]
    return [span(line, style) for line in lines]


def rule(text: str, size: int, style: str = "dim") -> Line:
    return fit(join(span("── ", style), span(text, "info"), span(" " + "─" * size, style)), size)


def side_by_side(blocks: Sequence[Block], size: int, gap: int = 1) -> Block:
    """Blocks across the width when they fit in ``size``, else one under another."""
    across = hstack(blocks, gap)
    return across if block_width(across) <= size else vstack(*blocks)


def share(size: int, parts: int, gap: int = 1) -> int:
    """The width of one of ``parts`` blocks laid across ``size`` with ``gap`` between them."""
    return (size - gap * (parts - 1)) // parts


def _merge(cells: Line) -> Line:
    """Adjacent cells of one style as one segment: shorter HTML, same text."""
    merged: Line = []
    for text, style in cells:
        if merged and merged[-1][1] == style:
            merged[-1] = (merged[-1][0] + text, style)
        else:
            merged.append((text, style))
    return merged

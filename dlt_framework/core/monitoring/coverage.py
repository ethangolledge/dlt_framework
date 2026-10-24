"""Backfill coverage: which parts of a resource's declared history have loaded, and what is missing.

Computed from the ``loads`` frame, which holds one row per successfully loaded window, so it
needs no dlt state: backfills stay stateless. A gap is a ``[start, end)`` range that
``backfill --missing`` loads; an open-ended row history has a final gap with ``end=None`` until a
page comes back short.
"""

from dataclasses import dataclass
from typing import Any

import polars as pl

from dlt_framework.core.backfill import Backfill
from dlt_framework.core.cursor import Cursor

Range = tuple[Any, Any | None]


@dataclass(frozen=True, slots=True)
class Coverage:
    start: Any
    end: Any | None
    covered: list[tuple[Any, Any]]

    @property
    def gaps(self) -> list[Range]:
        gaps: list[Range] = []
        position = self.start
        for low, high in self.covered:
            if self.end is not None and low >= self.end:
                break
            if low > position:
                gaps.append((position, low))
            position = max(position, high)
        if self.end is None or position < self.end:
            gaps.append((position, self.end))
        return gaps

    @property
    def fraction(self) -> float | None:
        """Share of the declared history loaded; ``None`` while a row history is open-ended."""
        if self.end is None:
            return None
        loaded = sum(
            (
                min(high, self.end) - max(low, self.start)
                for low, high in self.covered
                if high > self.start and low < self.end
            ),
            start=self.start - self.start,  # zero of the edge type: 0 or timedelta(0)
        )
        return loaded / (self.end - self.start)

    @classmethod
    def of(
        cls, loads: pl.DataFrame, resource_id: str, backfill: Backfill, cursor: Cursor | None
    ) -> "Coverage":
        window = backfill.window
        rows = loads.filter(
            (pl.col("resource_id") == resource_id) & (pl.col("window_kind") == window.kind)
        ).select("window_start", "window_end", "rows")
        loaded = [
            (window.parse_edge(start), window.parse_edge(end), count)
            for start, end, count in rows.iter_rows()
        ]
        start, end = backfill.bounds(None if cursor is None else cursor.start)
        if end is None:  # open-ended: history ends at the first short window
            ends = [high for low, high, count in loaded if window.between(low, high).is_last(count)]
            end = min(ends, default=None)
        return cls(start, end, _merge([(low, high) for low, high, _ in loaded]))


def _merge(ranges: list[tuple[Any, Any]]) -> list[tuple[Any, Any]]:
    merged: list[tuple[Any, Any]] = []
    for low, high in sorted(ranges):
        if merged and low <= merged[-1][1]:
            merged[-1] = (merged[-1][0], max(merged[-1][1], high))
        else:
            merged.append((low, high))
    return merged

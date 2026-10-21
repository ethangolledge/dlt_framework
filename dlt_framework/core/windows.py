"""Windows: one bounded chunk of a backfill.

A window says *which part* of the history one extract covers. How that reaches the upstream system
is decided elsewhere: cursors turn time and value windows into dlt incremental bounds, and each
source package turns row windows into its own paging (SQL ``OFFSET``, REST offset params, ...).
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from dlt_framework.core.errors import DefinitionError


class Window:
    def is_last(self, rows: int) -> bool:
        """Whether the backfill should stop after a chunk that extracted ``rows`` rows."""
        return False


@dataclass(frozen=True, slots=True)
class TimeWindow(Window):
    """Cursor values in ``[start, end)``, both UTC."""

    start: datetime
    end: datetime

    def __post_init__(self) -> None:
        if self.start >= self.end:
            raise DefinitionError(f"Time window start {self.start} must be before {self.end}")

    def __str__(self) -> str:
        return f"[{self.start.isoformat()}, {self.end.isoformat()})"


@dataclass(frozen=True, slots=True)
class ValueWindow(Window):
    """Cursor values in ``[start, end)`` for monotonic numbers: ids, sequences, rowversions."""

    start: int
    end: int

    def __post_init__(self) -> None:
        if self.start >= self.end:
            raise DefinitionError(f"Value window start {self.start} must be below {self.end}")

    def __str__(self) -> str:
        return f"[{self.start}, {self.end})"


@dataclass(frozen=True, slots=True)
class RowWindow(Window):
    """``size`` rows starting at row ``offset`` of a stable ordering; for sources with no cursor."""

    offset: int
    size: int

    def is_last(self, rows: int) -> bool:
        return rows < self.size

    def __str__(self) -> str:
        return f"rows [{self.offset}, {self.offset + self.size})"

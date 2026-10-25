"""Backfills: declared per resource, they produce the windows a backfill run loads.

Time and value backfills end where the resource's cursor starts, so history and incremental runs
meet exactly once at ``cursor.start``. Row backfills cover sources with no usable cursor; their
history is open-ended and stops at the first short page.
"""

from abc import ABC, abstractmethod
from calendar import monthrange
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import datetime, timedelta
from itertools import count
from typing import Any, ClassVar, Literal

from dlt_framework.core.cursor import Cursor, TimeCursor, ValueCursor
from dlt_framework.core.errors import ConfigurationError, DefinitionError
from dlt_framework.core.windows import RowWindow, TimeWindow, ValueWindow, Window


class Backfill(ABC):
    window: ClassVar[type[Window]]
    cursor: ClassVar[type[Cursor] | None]

    @abstractmethod
    def bounds(self, cursor_start: Any) -> tuple[Any, Any | None]:
        """The declared history as ``[start, end)``; ``end`` is ``None`` when open-ended."""

    @abstractmethod
    def advance(self, origin: Any, steps: int) -> Any:
        """The edge ``steps`` chunks after ``origin``."""

    def check(self, cursor: Cursor | None) -> None:
        if self.cursor is not None and not isinstance(cursor, self.cursor):
            raise DefinitionError(f"{type(self).__name__} needs a {self.cursor.__name__}")
        start, end = self.bounds(None if cursor is None else cursor.start)
        if end is not None and start >= end:
            raise DefinitionError(
                f"{type(self).__name__} start {start} must be before the cursor start {end}"
            )

    def windows(
        self, cursor: Cursor | None, start: Any | None = None, end: Any | None = None
    ) -> Iterator[Window]:
        """Chunks over the declared history, narrowed to ``[start, end)`` when given.

        Chunks step from ``start``, so a range resumed from a logged window edge lines up with
        the windows an earlier run loaded.
        """
        origin, limit = self.bounds(None if cursor is None else cursor.start)
        origin = origin if start is None else start
        if end is not None:
            limit = end if limit is None else min(limit, end)
        for step in count():
            low = self.advance(origin, step)
            if limit is not None and low >= limit:
                return
            high = self.advance(origin, step + 1)
            yield self.window.between(low, high if limit is None else min(high, limit))

    def edge(self, text: str) -> Any:
        """A ``--from``/``--to`` value as this backfill's window edge."""
        try:
            return self.window.parse_edge(text)
        except ValueError as error:
            raise ConfigurationError(
                f"{text!r} is not a {self.window.kind} window edge: {error}"
            ) from None


@dataclass(frozen=True, slots=True)
class Duration:
    """A calendar-aware step, e.g. ``Duration(1, "months")``."""

    amount: int
    unit: Literal["months", "weeks", "days", "hours", "minutes", "seconds"]

    def __post_init__(self) -> None:
        _positive(self.amount, "Duration amount")

    def after(self, start: datetime, steps: int) -> datetime:
        """``start`` plus ``steps`` durations; months stay anchored to ``start``'s day."""
        total = self.amount * steps
        if self.unit != "months":
            return start + timedelta(**{self.unit: total})
        year, month = divmod(start.year * 12 + start.month - 1 + total, 12)
        return start.replace(
            year=year, month=month + 1, day=min(start.day, monthrange(year, month + 1)[1])
        )


@dataclass(frozen=True)
class TimeBackfill(Backfill):
    window: ClassVar[type[Window]] = TimeWindow
    cursor: ClassVar[type[Cursor] | None] = TimeCursor

    start: datetime
    every: Duration

    def __post_init__(self) -> None:
        if self.start.tzinfo is None:
            raise DefinitionError(f"TimeBackfill start {self.start} must be timezone-aware")

    def bounds(self, cursor_start: Any) -> tuple[datetime, datetime]:
        return self.start, cursor_start

    def advance(self, origin: datetime, steps: int) -> datetime:
        return self.every.after(origin, steps)


@dataclass(frozen=True)
class ValueBackfill(Backfill):
    """Fixed-width value windows. For a ``RowVersion`` cursor, ``start`` and ``every`` are the
    integers the rowversion encodes."""

    window: ClassVar[type[Window]] = ValueWindow
    cursor: ClassVar[type[Cursor] | None] = ValueCursor

    start: int
    every: int

    def __post_init__(self) -> None:
        _positive(self.every, "ValueBackfill every")

    def bounds(self, cursor_start: Any) -> tuple[int, int]:
        return self.start, cursor_start

    def advance(self, origin: int, steps: int) -> int:
        return origin + self.every * steps


@dataclass(frozen=True)
class RowBackfill(Backfill):
    """Pages through every row in ``size`` batches until a batch comes back short.

    Offsets are positional: each page re-reads the rows before it (``OFFSET``), and rows inserted
    meanwhile shift later pages. Prefer a ``ValueBackfill`` on an id or rowversion for large tables.
    """

    window: ClassVar[type[Window]] = RowWindow
    cursor: ClassVar[type[Cursor] | None] = None

    size: int

    def __post_init__(self) -> None:
        _positive(self.size, "RowBackfill size")

    def bounds(self, cursor_start: Any) -> tuple[int, None]:
        return 0, None

    def advance(self, origin: int, steps: int) -> int:
        return origin + self.size * steps


def _positive(value: int, name: str) -> None:
    """A step of zero or less never advances: its backfill would loop over the same window."""
    if value <= 0:
        raise DefinitionError(f"{name} must be positive, not {value}")

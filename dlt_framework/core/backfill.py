"""Backfills: declared per resource, they produce the windows a backfill run loads.

Time and value backfills end where the resource's cursor starts, so history and incremental runs
meet exactly once at ``cursor.start``. Row backfills cover sources with no usable cursor.
"""

from abc import ABC, abstractmethod
from calendar import monthrange
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import datetime, timedelta
from itertools import count
from typing import ClassVar, Literal

from dlt_framework.core.cursor import Cursor, TimeCursor, ValueCursor
from dlt_framework.core.errors import ConfigurationError, DefinitionError
from dlt_framework.core.windows import RowWindow, TimeWindow, ValueWindow, Window


class Backfill(ABC):
    window: ClassVar[type[Window]]
    cursor: ClassVar[type[Cursor] | None]

    @abstractmethod
    def windows(self, cursor: Cursor | None, resume: str | None) -> Iterator[Window]:
        """Chunks to load, starting at ``resume`` (as logged by a previous run) if given."""

    def check(self, cursor: Cursor | None) -> None:
        if self.cursor is not None and not isinstance(cursor, self.cursor):
            raise DefinitionError(f"{type(self).__name__} needs a {self.cursor.__name__}")

    @staticmethod
    def _position(text: str) -> int:
        """A ``--from`` value for value and row backfills."""
        try:
            return int(text)
        except ValueError as error:
            raise ConfigurationError(f"--from {text!r} is not an integer") from error


@dataclass(frozen=True, slots=True)
class Duration:
    """A calendar-aware step, e.g. ``Duration(1, "months")``."""

    amount: int
    unit: Literal["months", "weeks", "days", "hours", "minutes", "seconds"]

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

    def check(self, cursor: Cursor | None) -> None:
        super().check(cursor)
        assert isinstance(cursor, TimeCursor)
        if self.start >= cursor.start:
            raise DefinitionError(f"TimeBackfill start must be before cursor start {cursor.start}")

    def windows(self, cursor: Cursor | None, resume: str | None) -> Iterator[Window]:
        assert isinstance(cursor, TimeCursor)
        start = self._instant(resume) if resume else self.start
        for step in count():
            chunk_start = self.every.after(start, step)
            if chunk_start >= cursor.start:
                return
            yield TimeWindow(chunk_start, min(self.every.after(start, step + 1), cursor.start))

    @staticmethod
    def _instant(text: str) -> datetime:
        try:
            instant = datetime.fromisoformat(text)
        except ValueError as error:
            raise ConfigurationError(f"--from {text!r} is not an ISO-8601 datetime") from error
        if instant.tzinfo is None:
            raise ConfigurationError(f"--from {text!r} needs a timezone, e.g. a trailing Z")
        return instant


@dataclass(frozen=True)
class ValueBackfill(Backfill):
    window: ClassVar[type[Window]] = ValueWindow
    cursor: ClassVar[type[Cursor] | None] = ValueCursor

    start: int
    every: int

    def check(self, cursor: Cursor | None) -> None:
        super().check(cursor)
        assert isinstance(cursor, ValueCursor)
        if self.start >= cursor.start:
            raise DefinitionError(f"ValueBackfill start must be below cursor start {cursor.start}")

    def windows(self, cursor: Cursor | None, resume: str | None) -> Iterator[Window]:
        assert isinstance(cursor, ValueCursor)
        for start in range(
            self._position(resume) if resume else self.start, cursor.start, self.every
        ):
            yield ValueWindow(start, min(start + self.every, cursor.start))


@dataclass(frozen=True)
class RowBackfill(Backfill):
    """Pages through every row in ``size`` batches until a batch comes back short."""

    window: ClassVar[type[Window]] = RowWindow
    cursor: ClassVar[type[Cursor] | None] = None

    size: int

    def windows(self, cursor: Cursor | None, resume: str | None) -> Iterator[Window]:
        for offset in count(self._position(resume) if resume else 0, self.size):
            yield RowWindow(offset, self.size)

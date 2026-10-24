"""Incremental cursors: where a resource's position comes from, for every source type."""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Literal

import dlt
from dlt.extract.incremental import Incremental
from dlt.extract.resource import DltResource

from dlt_framework.core.errors import DefinitionError
from dlt_framework.core.formats import TimeFormat
from dlt_framework.core.windows import TimeWindow, ValueWindow, Window

FALLBACK_COLUMN = "_cursor"
OnMissing = Literal["raise", "include", "exclude"]


@dataclass(frozen=True, kw_only=True)
class Cursor(ABC):
    """The field dlt tracks between runs and how rows without a value are handled.

    ``fallbacks`` are used when ``path`` is null: the first non-null of ``(path, *fallbacks)`` is
    written to the ``_cursor`` column and dlt tracks that column instead. Subclasses define the
    start of incremental loading and which windows they can bound.
    """

    path: str
    on_missing: OnMissing
    fallbacks: tuple[str, ...] = ()
    lag: float | None = None
    row_order: Literal["asc", "desc"] | None = None

    @property
    @abstractmethod
    def initial_value(self) -> Any:
        """Where an incremental run starts when no state exists."""

    @abstractmethod
    def bounds(self, window: Window) -> tuple[Any, Any] | None:
        """``(initial_value, end_value)`` for a window, or ``None`` if the window is positional
        (rows) and must not filter on this cursor."""

    @property
    def tracked_path(self) -> str:
        return FALLBACK_COLUMN if self.fallbacks else self.path

    def settings(self, window: Window | None) -> dict[str, Any] | None:
        """``dlt.sources.incremental`` arguments, or ``None`` when the cursor does not apply.

        No window: dlt resumes from stored state. A bounding window sets ``end_value``, which
        makes dlt use throwaway state, so a backfill never reads or moves the run cursor.
        """
        if window is None:
            start, end = self.initial_value, None
        elif (bounds := self.bounds(window)) is None:
            return None
        else:
            start, end = bounds
        return {
            "cursor_path": self.tracked_path,
            "initial_value": start,
            "end_value": end,
            "lag": self.lag,
            "row_order": self.row_order,
            "on_cursor_value_missing": self.on_missing,
            "range_start": "closed",
            "range_end": "open",
        }

    def incremental(self, window: Window | None) -> Incremental[Any] | None:
        settings = self.settings(window)
        return None if settings is None else dlt.sources.incremental(**settings)

    def prepare(self, resource: DltResource) -> None:
        """Runs before dlt's incremental filter sees a row."""
        if self.fallbacks:
            resource.add_map(self._coalesce, insert_at=1)

    def _coalesce(self, row: dict[str, Any]) -> dict[str, Any]:
        paths = (self.path, *self.fallbacks)
        row[FALLBACK_COLUMN] = next((row[p] for p in paths if row.get(p) is not None), None)
        return row


@dataclass(frozen=True, kw_only=True)
class TimeCursor(Cursor):
    """A timestamp field. ``start`` is a UTC instant; ``format`` is how the source writes it."""

    start: datetime
    format: TimeFormat

    def __post_init__(self) -> None:
        if self.start.tzinfo is None:
            raise DefinitionError(f"TimeCursor {self.path!r} start must be timezone-aware")

    @property
    def initial_value(self) -> Any:
        return self.format.render(self.start)

    def bounds(self, window: Window) -> tuple[Any, Any] | None:
        match window:
            case TimeWindow(start=start, end=end):
                return self.format.render(start), self.format.render(end)
            case ValueWindow():
                raise DefinitionError(f"TimeCursor {self.path!r} cannot be bounded by {window}")
            case _:
                return None


@dataclass(frozen=True, kw_only=True)
class ValueCursor(Cursor):
    """A monotonically increasing number: id, sequence, or ``rowversion`` cast to an integer."""

    start: int

    @property
    def initial_value(self) -> Any:
        return self.start

    def bounds(self, window: Window) -> tuple[Any, Any] | None:
        match window:
            case ValueWindow(start=start, end=end):
                return start, end
            case TimeWindow():
                raise DefinitionError(f"ValueCursor {self.path!r} cannot be bounded by {window}")
            case _:
                return None

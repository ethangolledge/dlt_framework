"""Incremental cursors: where a resource's position comes from, for every source type."""

from abc import ABC
from dataclasses import dataclass
from datetime import datetime
from typing import Any, ClassVar, Literal

import dlt
from dlt.extract.incremental import Incremental
from dlt.extract.resource import DltResource

from dlt_framework.core.errors import DefinitionError
from dlt_framework.core.formats import CursorFormat, TimeFormat, ValueFormat
from dlt_framework.core.windows import TimeWindow, ValueWindow, Window

FALLBACK_COLUMN = "_cursor"
OnMissing = Literal["raise", "include", "exclude"]


@dataclass(frozen=True, kw_only=True)
class Cursor(ABC):
    """The field dlt tracks between runs, where it starts, and how the source stores it.

    ``start`` is where an incremental run begins when no state exists, and where every backfill
    ends. ``format`` renders it, and every window edge, into the source's representation.
    ``fallbacks`` are used when ``path`` is null: the first non-null of ``(path, *fallbacks)`` is
    written to the ``_cursor`` column and dlt tracks that column instead. ``lag`` re-reads that
    much behind the last value on every run, for rows committed late with older cursor values.
    """

    window: ClassVar[type[Window]]

    path: str
    start: Any
    format: CursorFormat[Any]
    on_missing: OnMissing
    fallbacks: tuple[str, ...] = ()
    lag: float | None = None
    row_order: Literal["asc", "desc"] | None = None

    @property
    def tracked_path(self) -> str:
        return FALLBACK_COLUMN if self.fallbacks else self.path

    @property
    def initial_value(self) -> Any:
        return self.format.render(self.start)

    def bounds(self, window: Window) -> tuple[Any, Any] | None:
        """Rendered ``(initial_value, end_value)`` for a window, or ``None`` for a positional
        window, which must not filter on this cursor."""
        if window.positional:
            return None
        if not isinstance(window, self.window):
            raise DefinitionError(
                f"{type(self).__name__} {self.path!r} cannot be bounded by {window}"
            )
        start, end = window.edges
        return self.format.render(start), self.format.render(end)

    def beyond(self, stored: Any, limit: datetime) -> bool:
        """Whether a stored value is past ``limit``: a cursor there skips every row until then."""
        return False

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
    """A timestamp field. ``start`` is a timezone-aware instant."""

    window: ClassVar[type[Window]] = TimeWindow

    start: datetime
    format: TimeFormat

    def __post_init__(self) -> None:
        if self.start.tzinfo is None:
            raise DefinitionError(f"TimeCursor {self.path!r} start must be timezone-aware")

    def beyond(self, stored: Any, limit: datetime) -> bool:
        return self.format.parse(stored) > limit


@dataclass(frozen=True, kw_only=True)
class ValueCursor(Cursor):
    """A monotonically increasing number: an id, a sequence, or a ``RowVersion``."""

    window: ClassVar[type[Window]] = ValueWindow

    start: int
    format: ValueFormat

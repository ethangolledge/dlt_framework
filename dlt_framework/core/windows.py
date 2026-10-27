"""Windows: one bounded chunk of a backfill.

A window says *which part* of the history one extract covers, as ``[start, end)`` edges. How that
reaches the upstream system is decided elsewhere: cursors render time and value edges into dlt
incremental bounds, and each source package turns row windows into its own paging (SQL
``OFFSET``, REST offset params, ...).

Edges are written as text in logs, monitoring, and ``--from``/``--to``; ``format_edge`` and
``parse_edge`` are the only conversions between the two.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from typing import Any, ClassVar, Self

from dlt_framework.core.errors import DefinitionError


class Window(ABC):
    kind: ClassVar[str]
    positional: ClassVar[bool] = False
    """Whether the window counts rows rather than bounding a cursor: the source pages through it,
    and no cursor filters on it."""

    @property
    @abstractmethod
    def edges(self) -> tuple[Any, Any]:
        """``[start, end)`` as comparable values."""

    @classmethod
    @abstractmethod
    def between(cls, start: Any, end: Any) -> Self:
        """The window covering ``[start, end)``."""

    @classmethod
    def parse_edge(cls, text: str) -> Any:
        return int(text)

    @staticmethod
    def format_edge(edge: Any) -> str:
        return str(edge)

    def span(self) -> tuple[str, str]:
        start, end = self.edges
        return self.format_edge(start), self.format_edge(end)

    def is_last(self, rows: int) -> bool:
        """Whether the backfill should stop after a chunk that extracted ``rows`` rows."""
        return False

    def __str__(self) -> str:
        start, end = self.span()
        return f"{self.kind} [{start}, {end})"


@dataclass(frozen=True, slots=True)
class TimeWindow(Window):
    """Cursor values in ``[start, end)``, both timezone-aware."""

    kind: ClassVar[str] = "time"

    start: datetime
    end: datetime

    def __post_init__(self) -> None:
        if self.start >= self.end:
            raise DefinitionError(f"Time window start {self.start} must be before {self.end}")

    @property
    def edges(self) -> tuple[datetime, datetime]:
        return self.start, self.end

    @classmethod
    def between(cls, start: datetime, end: datetime) -> Self:
        return cls(start, end)

    @classmethod
    def parse_edge(cls, text: str) -> datetime:
        edge = datetime.fromisoformat(text)
        if edge.tzinfo is None:
            raise ValueError(f"{text!r} needs a timezone, e.g. a trailing Z")
        return edge

    @staticmethod
    def format_edge(edge: datetime) -> str:
        return edge.isoformat()


@dataclass(frozen=True, slots=True)
class ValueWindow(Window):
    """Cursor values in ``[start, end)`` for monotonic numbers: ids, sequences, rowversions."""

    kind: ClassVar[str] = "value"

    start: int
    end: int

    def __post_init__(self) -> None:
        if self.start >= self.end:
            raise DefinitionError(f"Value window start {self.start} must be below {self.end}")

    @property
    def edges(self) -> tuple[int, int]:
        return self.start, self.end

    @classmethod
    def between(cls, start: int, end: int) -> Self:
        return cls(start, end)


@dataclass(frozen=True, slots=True)
class RowWindow(Window):
    """``size`` rows starting at row ``offset`` of a stable ordering; for sources with no cursor."""

    kind: ClassVar[str] = "rows"
    positional: ClassVar[bool] = True

    offset: int
    size: int

    @property
    def edges(self) -> tuple[int, int]:
        return self.offset, self.offset + self.size

    @classmethod
    def between(cls, start: int, end: int) -> Self:
        return cls(start, end - start)

    def is_last(self, rows: int) -> bool:
        return rows < self.size


WINDOWS: dict[str, type[Window]] = {
    window.kind: window for window in (TimeWindow, ValueWindow, RowWindow)
}
"""Each window class by its ``kind``, as monitoring records it."""

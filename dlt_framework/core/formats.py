"""How a source stores cursor values. A source with an unusual format adds a subclass.

``render`` turns the framework's value (a UTC datetime, or an integer) into the source's own
representation, for ``WHERE`` clauses, request params, and dlt's comparisons. ``parse`` turns a
stored value back, so state can be checked and shown: ``2026-10-01T00:00:00+00:00``, not
``b'\\x00\\x00...'``.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import UTC, date, datetime
from typing import Any, Generic, TypeVar

T = TypeVar("T")


class CursorFormat(ABC, Generic[T]):
    @abstractmethod
    def render(self, value: T) -> Any:
        """``value`` as the source stores it."""

    @abstractmethod
    def parse(self, stored: Any) -> T:
        """A stored (rendered) value back as the framework's value."""

    def describe(self, stored: Any) -> str:
        """A stored value as text for logs and monitoring."""
        return str(self.parse(stored))


class TimeFormat(CursorFormat[datetime]):
    """Renders timezone-aware UTC instants; parses back to them."""

    def describe(self, stored: Any) -> str:
        return self.parse(stored).isoformat()


class ValueFormat(CursorFormat[int]):
    """Renders integers: ids, sequences, rowversions."""


@dataclass(frozen=True, slots=True)
class IsoTimestamp(TimeFormat):
    """``2026-10-01T00:00:00Z``"""

    def render(self, value: datetime) -> str:
        return value.strftime("%Y-%m-%dT%H:%M:%SZ")

    def parse(self, stored: Any) -> datetime:
        return _instant(stored)


@dataclass(frozen=True, slots=True)
class Strftime(TimeFormat):
    """Any string layout, e.g. ``Strftime("%Y-%m-%d %H:%M:%S")``; no offset means UTC."""

    pattern: str

    def render(self, value: datetime) -> str:
        return value.strftime(self.pattern)

    def parse(self, stored: Any) -> datetime:
        return _utc(datetime.strptime(stored, self.pattern))


@dataclass(frozen=True, slots=True)
class UnixSeconds(TimeFormat):
    def render(self, value: datetime) -> int:
        return int(value.timestamp())

    def parse(self, stored: Any) -> datetime:
        return datetime.fromtimestamp(stored, UTC)


@dataclass(frozen=True, slots=True)
class UnixMillis(TimeFormat):
    def render(self, value: datetime) -> int:
        return int(value.timestamp() * 1000)

    def parse(self, stored: Any) -> datetime:
        return datetime.fromtimestamp(stored / 1000, UTC)


@dataclass(frozen=True, slots=True)
class DateOnly(TimeFormat):
    def render(self, value: datetime) -> date:
        return value.date()

    def parse(self, stored: Any) -> datetime:
        return _instant(stored)


@dataclass(frozen=True, slots=True)
class AwareDatetime(TimeFormat):
    """Timezone-aware datetimes, e.g. Postgres ``timestamptz`` or file modification dates."""

    def render(self, value: datetime) -> datetime:
        return value

    def parse(self, stored: Any) -> datetime:
        return _instant(stored)


@dataclass(frozen=True, slots=True)
class NaiveDatetime(TimeFormat):
    """Naive datetimes holding UTC, e.g. SQL Server ``datetime2``."""

    def render(self, value: datetime) -> datetime:
        return value.replace(tzinfo=None)

    def parse(self, stored: Any) -> datetime:
        return _instant(stored)


@dataclass(frozen=True, slots=True)
class Integer(ValueFormat):
    """A number stored as a number: identity columns, sequences, offsets."""

    def render(self, value: int) -> int:
        return value

    def parse(self, stored: Any) -> int:
        return int(stored)


@dataclass(frozen=True, slots=True)
class RowVersion(ValueFormat):
    """SQL Server ``rowversion``: 8 big-endian bytes, which sort like the integer they encode.

    Windows and starts are stated as integers (``SELECT CAST(rv AS BIGINT)`` shows them) and
    reach the database as ``binary(8)``, so the column's index is used as is.
    """

    def render(self, value: int) -> bytes:
        return value.to_bytes(8, "big")

    def parse(self, stored: Any) -> int:
        return int.from_bytes(stored, "big")


def _instant(stored: Any) -> datetime:
    """A datetime, date, or ISO-8601 text as a UTC instant."""
    value = stored if isinstance(stored, datetime) else datetime.fromisoformat(str(stored))
    return _utc(value)


def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)

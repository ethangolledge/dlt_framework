"""How a source writes time cursor values. A source with an unusual format adds a subclass."""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import date, datetime


class TimeFormat(ABC):
    @abstractmethod
    def render(self, instant: datetime) -> str | int | date | datetime:
        """``instant`` (UTC, timezone-aware) as the source stores it."""


@dataclass(frozen=True, slots=True)
class IsoTimestamp(TimeFormat):
    """``2026-10-01T00:00:00Z``"""

    def render(self, instant: datetime) -> str:
        return instant.strftime("%Y-%m-%dT%H:%M:%SZ")


@dataclass(frozen=True, slots=True)
class Strftime(TimeFormat):
    """Any string layout, e.g. ``Strftime("%Y-%m-%d %H:%M:%S")``."""

    pattern: str

    def render(self, instant: datetime) -> str:
        return instant.strftime(self.pattern)


@dataclass(frozen=True, slots=True)
class UnixSeconds(TimeFormat):
    def render(self, instant: datetime) -> int:
        return int(instant.timestamp())


@dataclass(frozen=True, slots=True)
class UnixMillis(TimeFormat):
    def render(self, instant: datetime) -> int:
        return int(instant.timestamp() * 1000)


@dataclass(frozen=True, slots=True)
class DateOnly(TimeFormat):
    def render(self, instant: datetime) -> date:
        return instant.date()


@dataclass(frozen=True, slots=True)
class AwareDatetime(TimeFormat):
    """Timezone-aware datetimes, e.g. Postgres ``timestamptz`` or file modification dates."""

    def render(self, instant: datetime) -> datetime:
        return instant


@dataclass(frozen=True, slots=True)
class NaiveDatetime(TimeFormat):
    """Naive datetimes holding UTC, e.g. SQL Server ``datetime2``."""

    def render(self, instant: datetime) -> datetime:
        return instant.replace(tzinfo=None)

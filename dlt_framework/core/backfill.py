from __future__ import annotations

import hashlib
import json
import re
from calendar import monthrange
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any, Literal

import dlt
from dlt.extract.incremental import Incremental

from dlt_framework.core.errors import BackfillError

if TYPE_CHECKING:
    from dlt_framework.core.models import SourceContext


BoundFormat = Literal["datetime", "date", "iso8601", "unix"]
BoundValue = datetime | str | int
ChunkUnit = Literal["seconds", "minutes", "hours", "days", "weeks", "months", "years"]
_CHUNK_SIZE = re.compile(
    r"^P(?:"
    r"(?P<date_amount>[1-9][0-9]*)(?P<date_unit>[YMWD])"
    r"|T(?P<time_amount>[1-9][0-9]*)(?P<time_unit>[HMS])"
    r")$"
)
_DATE_UNITS: dict[str, ChunkUnit] = {
    "Y": "years",
    "M": "months",
    "W": "weeks",
    "D": "days",
}
_TIME_UNITS: dict[str, ChunkUnit] = {
    "H": "hours",
    "M": "minutes",
    "S": "seconds",
}


@dataclass(frozen=True, slots=True)
class BackfillBounds:
    start: datetime
    end: datetime

    def values(self, value_format: BoundFormat = "datetime") -> tuple[BoundValue, BoundValue]:
        return _format_bound(self.start, value_format), _format_bound(self.end, value_format)


@dataclass(frozen=True, slots=True)
class BackfillWindow:
    resource: str
    start: datetime
    end: datetime

    @property
    def bounds(self) -> BackfillBounds:
        return BackfillBounds(self.start, self.end)


@dataclass(slots=True)
class LegacyBackfillWindow:
    """Compatibility object supplied to deprecated ``source(backfill=...)`` factories."""

    resource: str
    start: datetime
    end: datetime
    _claimed_by: str | None = field(default=None, init=False, repr=False)

    def for_resource(self, resource_name: str) -> BackfillBounds | None:
        if resource_name != self.resource:
            return None
        self._claimed_by = resource_name
        return BackfillBounds(self.start, self.end)

    @property
    def claimed(self) -> bool:
        return self._claimed_by == self.resource


@dataclass(frozen=True, slots=True)
class ChunkSize:
    amount: int
    unit: ChunkUnit

    def boundary(self, start: datetime, chunk_number: int) -> datetime:
        if chunk_number < 1:
            raise BackfillError("Chunk number must be positive")
        total_amount = self.amount * chunk_number
        try:
            if self.unit == "years":
                return _add_months(start, total_amount * 12)
            if self.unit == "months":
                return _add_months(start, total_amount)
            return start + timedelta(**{self.unit: total_amount})
        except (OverflowError, ValueError) as error:
            raise BackfillError(
                f"Backfill chunk {chunk_number} with size {self} exceeds supported datetime bounds"
            ) from error

    def __str__(self) -> str:
        if self.unit in {"years", "months", "weeks", "days"}:
            unit = {"years": "Y", "months": "M", "weeks": "W", "days": "D"}[self.unit]
            return f"P{self.amount}{unit}"
        unit = {"hours": "H", "minutes": "M", "seconds": "S"}[self.unit]
        return f"PT{self.amount}{unit}"


@dataclass(frozen=True, slots=True)
class BackfillPlan:
    start: datetime
    end: datetime
    chunk_size: ChunkSize

    def windows(
        self,
        resource: str,
        *,
        start_at: datetime | None = None,
        max_chunks: int | None = None,
    ) -> Iterator[BackfillWindow]:
        chunk_start = self.start
        chunk_number = 1
        yielded = 0
        resume_at = self.start if start_at is None else start_at
        if resume_at < self.start or resume_at > self.end:
            raise BackfillError("Stored backfill checkpoint falls outside the requested plan")

        while chunk_start < self.end:
            chunk_end = min(self.chunk_size.boundary(self.start, chunk_number), self.end)
            if chunk_end <= chunk_start:
                raise BackfillError("Backfill chunk size did not advance the time window")
            if chunk_end > resume_at:
                if chunk_start < resume_at:
                    raise BackfillError(
                        "Stored backfill checkpoint is not aligned with the requested chunk plan"
                    )
                yielded += 1
                if max_chunks is not None and yielded > max_chunks:
                    raise BackfillError(
                        f"Backfill exceeds the safety limit of {max_chunks:,} chunks; "
                        "increase MAX_BACKFILL_CHUNKS only after reviewing the API and load impact"
                    )
                yield BackfillWindow(resource=resource, start=chunk_start, end=chunk_end)
            chunk_start = chunk_end
            chunk_number += 1

    def count(self, *, max_chunks: int | None = None) -> int:
        return sum(1 for _ in self.windows("_count", max_chunks=max_chunks))

    def fingerprint(self, resource: str) -> str:
        value = {
            "resource": resource,
            "from": _format_bound(self.start, "iso8601"),
            "to": _format_bound(self.end, "iso8601"),
            "chunksize": str(self.chunk_size),
        }
        return hashlib.sha256(json.dumps(value, sort_keys=True).encode()).hexdigest()[:20]


def parse_backfill_plan(
    *, resource: str | None, from_value: str | None, to_value: str | None, chunk_size: str | None
) -> BackfillPlan | None:
    supplied = (from_value, to_value, chunk_size)
    if not any(value is not None for value in supplied):
        return None
    if resource is None:
        raise BackfillError("Backfill arguments require --resource NAME")
    if not all(value is not None for value in supplied):
        values = {"--from": from_value, "--to": to_value, "--chunksize": chunk_size}
        missing = ", ".join(name for name, value in values.items() if value is None)
        raise BackfillError(
            f"--from, --to, and --chunksize must be supplied together; missing: {missing}"
        )
    assert from_value is not None and to_value is not None and chunk_size is not None
    start = parse_bound(from_value, "--from")
    end = parse_bound(to_value, "--to")
    if start >= end:
        raise BackfillError(
            f"--from ({from_value}) must be earlier than --to ({to_value}); the end is exclusive"
        )
    return BackfillPlan(start=start, end=end, chunk_size=parse_chunk_size(chunk_size))


def parse_bound(value: str, option: str) -> datetime:
    try:
        if "T" not in value and " " not in value:
            parsed_date = date.fromisoformat(value)
            return datetime.combine(parsed_date, datetime.min.time(), tzinfo=timezone.utc)
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as error:
        raise BackfillError(
            f"Invalid {option} value {value!r}; use an ISO-8601 date or timezone-aware datetime"
        ) from error
    if parsed.tzinfo is None:
        raise BackfillError(
            f"{option} datetime {value!r} has no timezone; append Z or an explicit offset"
        )
    return parsed.astimezone(timezone.utc)


def parse_chunk_size(value: str) -> ChunkSize:
    match = _CHUNK_SIZE.fullmatch(value)
    if match is None:
        raise BackfillError(
            f"Invalid --chunksize value {value!r}; use one positive ISO-8601 unit, "
            "for example PT30M, PT6H, P1D, P1W, P1M, or P1Y"
        )
    if (unit := match.group("date_unit")) is not None:
        return ChunkSize(int(match.group("date_amount")), _DATE_UNITS[unit])
    unit = match.group("time_unit")
    return ChunkSize(int(match.group("time_amount")), _TIME_UNITS[unit])


def incremental_for(
    context: SourceContext,
    *,
    resource_name: str,
    cursor_path: str,
    initial_value: Any = None,
    value_format: BoundFormat = "datetime",
    primary_key: str | tuple[str, ...] | None = None,
    row_order: Literal["asc", "desc"] | None = None,
) -> Incremental[Any]:
    bounds = context.bounds_for(resource_name)
    start, end = (initial_value, None) if bounds is None else bounds.values(value_format)
    return dlt.sources.incremental(
        cursor_path,
        initial_value=start,
        end_value=end,
        primary_key=primary_key,
        row_order=row_order,
        range_start="closed",
        range_end="open",
    )


def rest_incremental_config(
    context: SourceContext,
    *,
    resource_name: str,
    cursor_path: str,
    start_param: str,
    end_param: str,
    initial_value: Any = None,
    value_format: BoundFormat = "iso8601",
    primary_key: str | tuple[str, ...] | None = None,
    row_order: Literal["asc", "desc"] | None = None,
) -> dict[str, Any]:
    incremental = incremental_for(
        context,
        resource_name=resource_name,
        cursor_path=cursor_path,
        initial_value=initial_value,
        value_format=value_format,
        primary_key=primary_key,
        row_order=row_order,
    )
    return {**dict(incremental), "start_param": start_param, "end_param": end_param}


def range_kwargs(
    context: SourceContext,
    *,
    resource_name: str,
    start_argument: str,
    end_argument: str,
    value_format: BoundFormat = "datetime",
) -> dict[str, Any]:
    bounds = context.bounds_for(resource_name)
    if bounds is None:
        return {}
    start, end = bounds.values(value_format)
    return {start_argument: start, end_argument: end}


def _add_months(value: datetime, months: int) -> datetime:
    month_index = value.year * 12 + value.month - 1 + months
    year, zero_based_month = divmod(month_index, 12)
    month = zero_based_month + 1
    return value.replace(year=year, month=month, day=min(value.day, monthrange(year, month)[1]))


def _format_bound(value: datetime, value_format: BoundFormat) -> BoundValue:
    if value_format == "datetime":
        return value
    if value_format == "date":
        return value.date().isoformat()
    if value_format == "unix":
        return int(value.timestamp())
    if value_format == "iso8601":
        return value.isoformat().replace("+00:00", "Z")
    raise ValueError(f"Unsupported backfill bound format: {value_format}")

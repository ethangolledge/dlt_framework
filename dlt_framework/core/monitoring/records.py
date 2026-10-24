"""Monitoring records and their Polars frames.

Every record is an append-only event, so the same frames suit files and databases, and a run
that is killed still leaves the rows it already wrote. A record's Polars schema is derived from
its own type hints: the columns are declared once, on the dataclass.

The frames form a star schema. ``pipelines`` and ``resources`` describe definitions; ``runs``,
``loads``, and ``samples`` are facts about them. Keys are deterministic and readable:

- ``pipeline_id`` is the pipeline name, which is permanent because it keys dlt state;
- ``resource_id`` is ``<pipeline_id>.<resource name>``;
- ``run_id`` is generated per run;
- ``load_id`` is dlt's load id, which is also ``_dlt_loads.load_id`` and ``_dlt_load_id`` on
  every row loaded to the destination.

Definition rows are written at the start of every run, so ``(run_id, resource_id)`` gives a
resource as it was defined for that run, and the latest row per id gives it as it is now.
"""

import types
from collections.abc import Sequence
from dataclasses import asdict, dataclass, fields
from datetime import datetime
from typing import Any, ClassVar, Union, get_args, get_origin, get_type_hints

import polars as pl

from dlt_framework.core.resource import Resource

_POLARS_TYPES: dict[type, pl.DataType] = {
    str: pl.String(),
    int: pl.Int64(),
    float: pl.Float64(),
    datetime: pl.Datetime("us", "UTC"),
}


@dataclass(frozen=True, kw_only=True)
class Record:
    """Base for monitoring records; ``name`` is the frame (table/folder) a sink writes to."""

    name: ClassVar[str]

    @classmethod
    def schema(cls) -> dict[str, pl.DataType]:
        hints = get_type_hints(cls)
        return {field.name: _polars_type(hints[field.name]) for field in fields(cls)}

    @classmethod
    def frame(cls, records: Sequence["Record"]) -> pl.DataFrame:
        rows = [asdict(record) for record in records]
        return pl.DataFrame(rows, schema=cls.schema(), orient="row")


@dataclass(frozen=True, slots=True, kw_only=True)
class PipelineRecord(Record):
    """A pipeline's definition as of one run."""

    name: ClassVar[str] = "pipelines"

    pipeline_id: str
    run_id: str
    at: datetime
    source_type: str
    source: str
    destination: str
    dataset: str


@dataclass(frozen=True, slots=True, kw_only=True)
class ResourceRecord(Record):
    """A resource's definition as of one run; ``definition`` is the full declaration."""

    name: ClassVar[str] = "resources"

    resource_id: str
    pipeline_id: str
    run_id: str
    at: datetime
    resource: str
    write_mode: str
    cursor_kind: str | None
    cursor_path: str | None
    cursor_start: str | None
    backfill_kind: str | None
    empty: str
    definition: str

    @staticmethod
    def describe(resource: Resource) -> dict[str, Any]:
        """The definition columns, also shown by ``pipeline list``."""
        cursor, backfill = resource.cursor, resource.backfill
        return {
            "resource": resource.name,
            "write_mode": type(resource.write).__name__,
            "cursor_kind": type(cursor).__name__ if cursor else None,
            "cursor_path": cursor.tracked_path if cursor else None,
            "cursor_start": str(cursor.initial_value) if cursor else None,
            "backfill_kind": type(backfill).__name__ if backfill else None,
            "empty": resource.empty,
            "definition": repr(resource),
        }


@dataclass(frozen=True, slots=True, kw_only=True)
class RunRecord(Record):
    """A lifecycle event: ``started``, then ``succeeded`` or ``failed``.

    A ``started`` with no matching end event is a lost run, e.g. a container killed for OOM.
    """

    name: ClassVar[str] = "runs"

    run_id: str
    pipeline_id: str
    dlt_pipeline: str
    mode: str
    event: str
    at: datetime
    host: str
    dlt_version: str
    rows: int | None = None
    exit_code: int | None = None
    error_type: str | None = None
    error_step: str | None = None
    error_resource: str | None = None
    error_window: str | None = None
    error_cause_type: str | None = None
    error_message: str | None = None
    peak_rss_mb: float | None = None
    memory_limit_mb: float | None = None
    cpu_seconds: float | None = None


@dataclass(frozen=True, slots=True, kw_only=True)
class LoadRecord(Record):
    """One resource loaded over one window (``window_kind`` is ``increment`` on runs).

    ``(load_id, resource_id)`` is unique. Step timings cover the whole window, shared by the
    resources loaded in it.
    """

    name: ClassVar[str] = "loads"

    load_id: str
    resource_id: str
    run_id: str
    at: datetime
    window_kind: str
    window_start: str | None
    window_end: str | None
    rows: int
    extract_s: float
    normalize_s: float
    load_s: float
    cursor_last_value: str | None


@dataclass(frozen=True, slots=True, kw_only=True)
class SampleRecord(Record):
    """This process's memory and CPU at one moment of a run."""

    name: ClassVar[str] = "samples"

    run_id: str
    at: datetime
    rss_mb: float
    cpu_percent: float
    memory_limit_mb: float | None


def _polars_type(hint: Any) -> pl.DataType:
    """``X`` or ``X | None`` to its Polars type."""
    if get_origin(hint) in (Union, types.UnionType):
        (hint,) = (arg for arg in get_args(hint) if arg is not type(None))
    return _POLARS_TYPES[hint]

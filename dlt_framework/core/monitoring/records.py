"""Monitoring records and their Polars frames.

Every record is an append-only event, so the same frames suit files and databases, and a run
that is killed still leaves the rows it already wrote. A record's Polars schema is derived from
its own type hints: the columns are declared once, on the dataclass.

The frames form a star schema. ``pipelines`` and ``resources`` describe definitions; ``runs``,
``steps``, ``loads``, ``tables``, ``samples``, ``profiles``, and ``reconciliations`` are facts
about them. Keys are
deterministic and readable:

- ``pipeline_id`` is the pipeline name, which is permanent because it keys dlt state;
- ``resource_id`` is ``<pipeline_id>.<resource name>``;
- ``run_id`` is generated per run;
- ``load_id`` is dlt's load id, which is also ``_dlt_loads.load_id`` and ``_dlt_load_id`` on
  every row loaded to the destination.

Definition rows are written at the start of every run, so ``(run_id, resource_id)`` gives a
resource as it was defined for that run, and the latest row per id gives it as it is now.
"""

import types
from collections.abc import Iterable, Sequence
from dataclasses import asdict, dataclass, field, fields
from datetime import datetime
from typing import Any, ClassVar, Protocol, Union, get_args, get_origin, get_type_hints

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
    """A pipeline's definition as of one run, and how many workers dlt resolved for each step
    (null where dlt picks at run time, e.g. normalize workers from the CPU count)."""

    name: ClassVar[str] = "pipelines"

    pipeline_id: str
    run_id: str
    at: datetime
    source_type: str
    source: str
    destination: str
    dataset: str
    extract_workers: int | None
    normalize_workers: int | None
    load_workers: int | None


@dataclass(frozen=True, slots=True, kw_only=True)
class ResourceRecord(Record):
    """A resource's definition as of one run; ``definition`` is the full declaration, and the
    ``backfill_*`` columns its declared history, which coverage is measured against."""

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
    backfill_window: str | None
    backfill_start: str | None
    backfill_end: str | None
    empty: str
    definition: str

    @staticmethod
    def key(pipeline: str, resource: str) -> str:
        """``resource_id``: ``<pipeline>.<resource>``."""
        return f"{pipeline}.{resource}"

    @staticmethod
    def declared(frame: pl.DataFrame, pipeline: str) -> list[dict[str, Any]]:
        """The pipeline's resources as its latest run declared them."""
        own = frame.filter(pl.col("pipeline_id") == pipeline).sort("at")
        if own.is_empty():
            return []
        return own.filter(pl.col("run_id") == own["run_id"][-1]).to_dicts()

    @staticmethod
    def describe(resource: Resource) -> dict[str, Any]:
        """The definition columns, also shown by ``pipeline list``."""
        cursor, backfill = resource.cursor, resource.backfill
        return {
            "resource": resource.name,
            "write_mode": type(resource.write).__name__,
            "cursor_kind": type(cursor).__name__ if cursor else None,
            "cursor_path": cursor.tracked_path if cursor else None,
            "cursor_start": cursor.format.describe(cursor.initial_value) if cursor else None,
            "backfill_kind": type(backfill).__name__ if backfill else None,
            **_declared(backfill, cursor),
            "empty": resource.empty,
            "definition": repr(resource),
        }


@dataclass(frozen=True, slots=True, kw_only=True)
class RunRecord(Record):
    """A lifecycle event: ``started``, then ``succeeded`` or ``failed``; or ``lost``.

    A run killed from outside (an OOM kill, a stopped container) cannot write its end. The next
    run of the same pipeline writes ``lost`` for it, with what its last samples showed:
    ``duration_s`` up to the last sample, ``peak_rss_mb``, and the step running then.

    ``duration_s`` is wall time from start to the end event. The ``error_*`` columns say what
    failed (type, root cause type, message), where (the step, the resource, the window; the
    innermost frame in this package and the innermost overall), on what input (exception
    notes), and how it was raised (one line per cause, and the redacted traceback).
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
    code_version: str
    python: str
    cpu_count: int
    host_memory_mb: float
    rows: int | None = None
    duration_s: float | None = None
    exit_code: int | None = None
    error_type: str | None = None
    error_step: str | None = None
    error_resource: str | None = None
    error_window: str | None = None
    error_cause_type: str | None = None
    error_message: str | None = None
    error_where: str | None = None
    error_origin: str | None = None
    error_chain: str | None = None
    error_notes: str | None = None
    error_traceback: str | None = None
    peak_rss_mb: float | None = None
    memory_limit_mb: float | None = None
    cpu_seconds: float | None = None


@dataclass(frozen=True, slots=True, kw_only=True)
class StepRecord(Record):
    """One attempt at one step: ``sync``, ``pending``, ``extract``, ``normalize``, or ``load``.

    A retried step has a row per attempt, the failed ones with ``outcome="failed"``. ``load_id``
    is set on the steps of a load once it commits, and is null for ``sync``, ``pending``, and
    a load that failed. ``cpu_s`` is this process's CPU time over the attempt: near
    ``duration_s`` or above it the step was busy computing, near 0 it was waiting on upstream or
    the destination. Pipelines run in one process share it.
    """

    name: ClassVar[str] = "steps"

    run_id: str
    load_id: str | None
    step: str
    attempt: int
    resources: str | None
    at: datetime
    started_at: datetime
    duration_s: float
    cpu_s: float
    outcome: str
    error_cause_type: str | None = None


@dataclass(frozen=True, slots=True, kw_only=True)
class LoadRecord(Record):
    """One resource loaded over one window (``window_kind`` is ``increment`` on runs).

    ``(load_id, resource_id)`` is unique. ``part`` numbers the loads of one unit: the chunks of
    an increment, or the windows of a backfill. ``rows`` are the resource's own rows;
    ``child_rows`` those of its nested tables. ``bytes`` is what extract wrote, and
    ``extract_first_at``/``extract_last_at`` when this resource wrote its first and last item,
    so its own extract time shows even when resources extract together. The cursor moved from
    ``cursor_start_value`` to ``cursor_last_value``; both are null without a cursor.
    """

    name: ClassVar[str] = "loads"

    load_id: str
    resource_id: str
    run_id: str
    at: datetime
    window_kind: str
    window_start: str | None
    window_end: str | None
    part: int
    rows: int
    child_rows: int
    bytes: int
    extract_first_at: datetime | None
    extract_last_at: datetime | None
    cursor_start_value: str | None
    cursor_last_value: str | None


@dataclass(frozen=True, slots=True, kw_only=True)
class TableRecord(Record):
    """One destination table written by one load: a resource's own table or a nested one.

    ``load_s`` spans its load jobs (for a merge, the merge itself), so a slow table, such as a
    merge without an index, stands out from the rest of its load.
    """

    name: ClassVar[str] = "tables"

    load_id: str
    resource_id: str
    run_id: str
    at: datetime
    table: str
    rows: int
    bytes: int
    jobs: int
    retries: int
    load_started_at: datetime | None
    load_finished_at: datetime | None
    load_s: float | None


@dataclass(frozen=True, slots=True, kw_only=True)
class SampleRecord(Record):
    """This process's memory, CPU, and IO at one moment of a run, and what it was doing.

    ``memory_limit_mb`` is the container's limit, null outside one; ``system_available_mb`` is
    what the host could still give, so headroom is known either way. ``step`` and
    ``resources`` are what the run was doing when sampled.
    """

    name: ClassVar[str] = "samples"

    run_id: str
    at: datetime
    step: str | None
    resources: str | None
    rss_mb: float
    cpu_percent: float
    threads: int
    read_mb_s: float | None
    write_mb_s: float | None
    memory_limit_mb: float | None
    system_available_mb: float
    system_total_mb: float


@dataclass(frozen=True, slots=True, kw_only=True)
class ProfileRecord(Record):
    """Where a profiled run spent its time: one row per step and function.

    Stacks of every thread are sampled at a fixed interval. ``own_samples`` counts samples whose
    innermost frame was this function (time in its own code), ``total_samples`` those with it
    anywhere on the stack (time in it and what it called); the shares divide by the step's
    samples. A thread blocked on a socket counts: waiting on upstream is time spent.
    """

    name: ClassVar[str] = "profiles"

    run_id: str
    at: datetime
    step: str | None
    function: str
    file: str
    line: int
    own_samples: int
    total_samples: int
    own_share: float
    total_share: float


@dataclass(frozen=True, slots=True, kw_only=True)
class ReconcileRecord(Record):
    """What a destination table holds, from ``pipeline reconcile``: one ``all`` row per
    resource, and one per backfill window where the cursor column is in the table.

    ``distinct_keys`` (on ``all`` rows of resources with a primary key) below
    ``destination_rows`` means duplicate keys. ``source_rows`` is upstream's count where the
    source can give one (``--source``): a window where they differ is missing or extra rows.
    """

    name: ClassVar[str] = "reconciliations"

    reconcile_id: str
    resource_id: str
    at: datetime
    window_kind: str
    window_start: str | None
    window_end: str | None
    destination_rows: int
    distinct_keys: int | None
    source_rows: int | None

    @staticmethod
    def window(kind: str, start: str | None, end: str | None) -> str:
        """``all``, or the window as ``[start, end)``."""
        return "all" if kind == "all" else f"[{start}, {end})"


RECORDS: tuple[type[Record], ...] = (
    PipelineRecord,
    ResourceRecord,
    RunRecord,
    StepRecord,
    LoadRecord,
    TableRecord,
    SampleRecord,
    ProfileRecord,
    ReconcileRecord,
)
"""Every frame monitoring writes."""


@dataclass(frozen=True, slots=True)
class Frames:
    """Every monitoring frame, read once and shared by whatever reads them: ``pipeline health``,
    ``status``, a dashboard. A frame not given is empty, with its schema."""

    pipelines: pl.DataFrame = field(default_factory=lambda: PipelineRecord.frame([]))
    resources: pl.DataFrame = field(default_factory=lambda: ResourceRecord.frame([]))
    runs: pl.DataFrame = field(default_factory=lambda: RunRecord.frame([]))
    steps: pl.DataFrame = field(default_factory=lambda: StepRecord.frame([]))
    loads: pl.DataFrame = field(default_factory=lambda: LoadRecord.frame([]))
    tables: pl.DataFrame = field(default_factory=lambda: TableRecord.frame([]))
    samples: pl.DataFrame = field(default_factory=lambda: SampleRecord.frame([]))
    profiles: pl.DataFrame = field(default_factory=lambda: ProfileRecord.frame([]))
    reconciliations: pl.DataFrame = field(default_factory=lambda: ReconcileRecord.frame([]))

    @classmethod
    def read(cls, sink: "SupportsRead", records: Iterable[type[Record]] = ()) -> "Frames":
        """``records`` (every frame when empty) from ``sink``; the others empty."""
        return cls(**{record.name: sink.read(record) for record in records or RECORDS})


class SupportsRead(Protocol):
    def read(self, record: type[Record]) -> pl.DataFrame: ...


def _declared(backfill: Any, cursor: Any) -> dict[str, str | None]:
    """The declared history's window kind and ``[start, end)`` edges as text (``end`` is null
    when open-ended), so coverage needs no pipeline code."""
    if backfill is None:
        return dict.fromkeys(("backfill_window", "backfill_start", "backfill_end"))
    start, end = backfill.bounds(None if cursor is None else cursor.start)
    edge = backfill.window.format_edge
    return {
        "backfill_window": backfill.window.kind,
        "backfill_start": edge(start),
        "backfill_end": None if end is None else edge(end),
    }


def _polars_type(hint: Any) -> pl.DataType:
    """``X`` or ``X | None`` to its Polars type."""
    if get_origin(hint) in (Union, types.UnionType):
        (hint,) = (arg for arg in get_args(hint) if arg is not type(None))
    return _POLARS_TYPES[hint]

"""The monitor: turns one run's lifecycle into records and writes them through a sink.

It sits around the run, never inside the loaders: the run hands it each step to time and dlt's
own step results to record, and everything else (samples, profiles, lost runs) it gathers
itself.
"""

import logging
import os
import platform
import socket
import threading
import time
import uuid
from collections.abc import Callable, Iterable, Mapping
from dataclasses import replace
from datetime import UTC, datetime
from itertools import count
from types import TracebackType
from typing import Any, ClassVar, Self, TypeVar

import dlt
import polars as pl
from dlt.common.configuration import configspec, resolve_configuration
from dlt.common.configuration.specs import BaseConfiguration
from dlt.common.pipeline import ExtractInfo, LoadInfo, NormalizeInfo
from dlt.common.schema import Schema
from dlt.common.schema.utils import get_root_table

from dlt_framework.core.errors import error_fields, root_cause
from dlt_framework.core.monitoring.profiler import StackSampler
from dlt_framework.core.monitoring.records import (
    LoadRecord,
    PipelineRecord,
    ProfileRecord,
    Record,
    ResourceRecord,
    RunRecord,
    SampleRecord,
    StepRecord,
    TableRecord,
)
from dlt_framework.core.monitoring.sampler import ResourceSampler
from dlt_framework.core.monitoring.sinks import Sink
from dlt_framework.core.pipeline import Pipeline
from dlt_framework.core.resource import Resource
from dlt_framework.core.settings import MonitoringSettings
from dlt_framework.core.windows import Window

LOGGER = logging.getLogger(__name__)
T = TypeVar("T")

LOAD_STEPS = ("extract", "normalize", "load")
URGENT_PRESSURE = 0.7  # above this share of memory used, every sample is written at once


class Monitor:
    """Writes a ``started`` record and the definition rows on entry; a ``steps`` row per step
    attempt; ``loads`` and ``tables`` rows per committed load; samples (taken every
    ``sample_every_s``, written every ``flush_every_s``, or at once under memory pressure);
    profiles when asked; and a ``succeeded``/``failed`` record at the end.

    Records are written as they happen, so a killed run still leaves its start and its memory
    trend, which the next run turns into a ``lost`` record. A failing sink is logged and never
    changes the run's outcome.
    """

    def __init__(
        self,
        sink: Sink,
        definition: Pipeline,
        settings: MonitoringSettings,
        *,
        dlt_pipeline: str,
        mode: str,
    ) -> None:
        self.sink = sink
        self.run_id = uuid.uuid4().hex
        self.definition, self.dlt_pipeline, self.mode = definition, dlt_pipeline, mode
        self.code_version = settings.code_version
        self.flush_s = settings.flush_every_s
        self.memory_limit_mb = ResourceSampler.memory_limit_mb()
        self._sampler = ResourceSampler(settings.sample_every_s, self._sampled)
        self._profiler = (
            StackSampler(settings.profile_every_ms / 1000, lambda: self._phase[0])
            if settings.profile_every_ms > 0
            else None
        )
        self._phase: tuple[str | None, str | None] = (None, None)  # step, resources
        self._steps: list[StepRecord] = []  # attempts of the load in progress
        self._samples: list[Record] = []
        self._lock = threading.Lock()
        self._flushed_at = time.monotonic()
        self._started_at = time.monotonic()

    def __enter__(self) -> Self:
        self._write(self._run_record("started"))
        self._write(self._pipeline_record())
        self._write(
            *(self._resource_record(resource) for resource in self.definition.source.resources)
        )
        self._sampler.__enter__()
        if self._profiler:
            self._profiler.start()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self._stop()
        self._flush()

    def attempts(
        self, step: str, call: Callable[[], T], resources: Iterable[Resource] = ()
    ) -> Callable[[], T]:
        """``call``, recording a ``steps`` row each time it is called: the retry layer calls it
        once per attempt. Samples and profiles taken meanwhile are marked with the step."""
        names = ", ".join(resource.name for resource in resources) or None
        numbers = count(1)

        def attempt() -> T:
            started, wall, cpu = _now(), time.monotonic(), self._sampler.cpu_used()
            self._phase = (step, names)
            outcome, cause = "ok", None
            try:
                return call()
            except BaseException as error:
                outcome, cause = "failed", type(root_cause(error)).__name__
                raise
            finally:
                self._phase = (None, None)
                record = StepRecord(
                    run_id=self.run_id,
                    load_id=None,
                    step=step,
                    attempt=next(numbers),
                    resources=names,
                    at=_now(),
                    started_at=started,
                    duration_s=time.monotonic() - wall,
                    cpu_s=self._sampler.cpu_used() - cpu,
                    outcome=outcome,
                    error_cause_type=cause,
                )
                if step in LOAD_STEPS:
                    self._steps.append(record)  # written with its load_id once committed
                else:
                    self._write(record)

        return attempt

    def loaded(
        self,
        schema: Schema,
        resources: Iterable[Resource],
        window: Window | None,
        part: int,
        info: tuple[ExtractInfo, NormalizeInfo, LoadInfo],
        cursors: Mapping[str, tuple[Mapping[str, Any] | None, Mapping[str, Any] | None]],
    ) -> None:
        """Record one committed load from dlt's own step results: each resource's rows, bytes,
        and extract span; each destination table's rows and load time; the steps that made it.
        ``cursors`` maps a resource to its stored cursor state before and after."""
        extracted, normalized, loaded = info
        (load_id,) = extracted.loads_ids
        by_resource = _metrics(extracted, load_id, "resource_metrics")
        by_table = _metrics(normalized, load_id, "table_metrics")
        jobs = _metrics(loaded, load_id, "job_metrics").values()
        owner = {table: _resource_of(schema, table) for table in by_table}
        start, end = window.span() if window else (None, None)
        now = _now()
        loads, tables = [], []
        for resource in resources:
            resource_id = self._resource_id(resource)
            own = by_resource.get(resource.name)
            mine = [table for table in by_table if owner[table] == resource.name]
            before, after = cursors.get(resource.name, (None, None))
            loads.append(
                LoadRecord(
                    load_id=load_id,
                    resource_id=resource_id,
                    run_id=self.run_id,
                    at=now,
                    window_kind=window.kind if window else "increment",
                    window_start=start,
                    window_end=end,
                    part=part,
                    rows=own.items_count if own else 0,
                    child_rows=sum(
                        by_table[table].items_count for table in mine if _parent(schema, table)
                    ),
                    bytes=own.file_size if own else 0,
                    extract_first_at=_instant(own.created) if own and own.items_count else None,
                    extract_last_at=(
                        _instant(own.last_modified) if own and own.items_count else None
                    ),
                    cursor_start_value=(
                        _described(resource, before, initial=True) if window is None else None
                    ),
                    cursor_last_value=_described(resource, after),
                )
            )
            for table in mine:
                tables.append(self._table(load_id, resource_id, table, by_table[table], jobs, now))
        self._write(*(replace(record, load_id=load_id) for record in self._steps))
        self._steps.clear()
        self._write(*loads)
        self._write(*tables)

    def lost(self) -> None:
        """Write ``lost`` for earlier runs of this dlt pipeline that never reported an end.

        Called once this run holds the pipeline lock, so a run still going elsewhere on this
        host is never taken for lost. What its samples showed last goes on the record and the
        log line, so an OOM kill says how close it came, and in which step.
        """
        try:
            runs = self.sink.read(RunRecord).filter(pl.col("dlt_pipeline") == self.dlt_pipeline)
            ended = runs.filter(pl.col("event") != "started")["run_id"]
            gone = runs.filter(
                (pl.col("event") == "started")
                & ~pl.col("run_id").is_in(ended.implode())
                & (pl.col("run_id") != self.run_id)
            )
            if gone.is_empty():
                return
            samples = self.sink.read(SampleRecord).filter(
                pl.col("run_id").is_in(gone["run_id"].implode())
            )
            for started in gone.iter_rows(named=True):
                self._report_lost(started, samples.filter(pl.col("run_id") == started["run_id"]))
        except Exception:
            LOGGER.exception("Could not check for lost runs; the run continues")

    def finished(self, rows: int, error: BaseException | None) -> None:
        self._stop()  # the final sample belongs in the outcome's peak
        self._write(*self._steps)  # steps of a load that never committed
        self._steps.clear()
        if self._profiler:
            self._write(*self._profile())
        stats: dict[str, Any] = {
            "rows": rows,
            "duration_s": time.monotonic() - self._started_at,
            "peak_rss_mb": self._sampler.peak_rss_mb,
            "memory_limit_mb": self.memory_limit_mb,
            "cpu_seconds": self._sampler.cpu_seconds,
        }
        if error is None:
            self._write(self._run_record("succeeded", **stats))
            return
        self._write(self._run_record("failed", **stats, **error_fields(error)))

    def _report_lost(self, started: dict[str, Any], samples: pl.DataFrame) -> None:
        last = samples.sort("at").tail(1).to_dicts()
        seen = last[0] if last else None
        details: dict[str, Any] = {
            "duration_s": (seen["at"] - started["at"]).total_seconds() if seen else None,
            "peak_rss_mb": samples["rss_mb"].max() if samples.height else None,
            "memory_limit_mb": seen["memory_limit_mb"] if seen else None,
            "error_step": seen["step"] if seen else None,
            "error_resource": seen["resources"] if seen else None,
        }
        identity = {key: started[key] for key in _IDENTITY}
        self._write(RunRecord(**identity, event="lost", at=_now(), **details))
        LOGGER.warning(
            "Run %s of %s started %s and never ended (killed, e.g. for memory); last seen %s",
            started["run_id"],
            self.dlt_pipeline,
            f"{started['at']:%Y-%m-%d %H:%M:%S}Z",
            _last_seen(seen),
            extra={"event": "run_lost", "lost_run_id": started["run_id"], **details},
        )

    def _table(
        self,
        load_id: str,
        resource_id: str,
        table: str,
        written: Any,
        jobs: Iterable[Any],
        now: datetime,
    ) -> TableRecord:
        mine = [job for job in jobs if job.table_name == table]
        starts = [_aware(job.started_at) for job in mine if job.started_at]
        ends = [_aware(job.finished_at) for job in mine if job.finished_at]
        first, last = min(starts, default=None), max(ends, default=None)
        return TableRecord(
            load_id=load_id,
            resource_id=resource_id,
            run_id=self.run_id,
            at=now,
            table=table,
            rows=written.items_count,
            bytes=written.file_size,
            jobs=len(mine),
            retries=sum(job.retry_count for job in mine),
            load_started_at=first,
            load_finished_at=last,
            load_s=(last - first).total_seconds() if first and last else None,
        )

    def _profile(self) -> list[ProfileRecord]:
        assert self._profiler is not None
        now = _now()
        return [ProfileRecord(run_id=self.run_id, at=now, **row) for row in self._profiler.top()]

    def _stop(self) -> None:
        self._sampler.stop()
        if self._profiler:
            self._profiler.stop()

    def _pipeline_record(self) -> PipelineRecord:
        definition = self.definition
        return PipelineRecord(
            pipeline_id=definition.name,
            run_id=self.run_id,
            at=_now(),
            source_type=type(definition.source).__name__,
            source=definition.source.name,
            destination=definition.target.destination,
            dataset=definition.target.dataset,
            **_workers(definition.source.name),
        )

    def _resource_record(self, resource: Resource) -> ResourceRecord:
        return ResourceRecord(
            resource_id=self._resource_id(resource),
            pipeline_id=self.definition.name,
            run_id=self.run_id,
            at=_now(),
            **ResourceRecord.describe(resource),
        )

    def _resource_id(self, resource: Resource) -> str:
        return ResourceRecord.key(self.definition.name, resource.name)

    def _run_record(self, event: str, **details: Any) -> RunRecord:
        return RunRecord(
            run_id=self.run_id,
            pipeline_id=self.definition.name,
            dlt_pipeline=self.dlt_pipeline,
            mode=self.mode,
            event=event,
            at=_now(),
            host=socket.gethostname(),
            dlt_version=dlt.__version__,
            code_version=self.code_version,
            python=platform.python_version(),
            cpu_count=os.cpu_count() or 1,
            host_memory_mb=ResourceSampler.host_memory_mb(),
            **details,
        )

    def _sampled(self, measures: dict[str, Any]) -> None:
        step, resources = self._phase
        sample = SampleRecord(
            run_id=self.run_id,
            at=_now(),
            step=step,
            resources=resources,
            memory_limit_mb=self.memory_limit_mb,
            **measures,
        )
        with self._lock:
            self._samples.append(sample)
        urgent = _pressure(sample) >= URGENT_PRESSURE
        if urgent or time.monotonic() - self._flushed_at >= self.flush_s:
            self._flush()

    def _flush(self) -> None:
        with self._lock:
            batch, self._samples = self._samples, []
            self._flushed_at = time.monotonic()
        if batch:
            self._write(*batch)

    def _write(self, *records: Record) -> None:
        if not records:
            return
        try:
            self.sink.write(records[0].name, type(records[0]).frame(records))
        except Exception:
            LOGGER.exception(
                "Monitoring sink %s failed; the run continues", type(self.sink).__name__
            )


# What a lost record copies from its run's start.
_IDENTITY = (
    "run_id",
    "pipeline_id",
    "dlt_pipeline",
    "mode",
    "host",
    "dlt_version",
    "code_version",
    "python",
    "cpu_count",
    "host_memory_mb",
)


@configspec
class _ExtractWorkers(BaseConfiguration):
    """The ``workers`` key of a dlt step's config section."""

    __section__: ClassVar[str] = "extract"
    workers: int | None = None


@configspec
class _NormalizeWorkers(_ExtractWorkers):
    __section__: ClassVar[str] = "normalize"


@configspec
class _LoadWorkers(_ExtractWorkers):
    __section__: ClassVar[str] = "load"


def _workers(source: str) -> dict[str, int | None]:
    """Workers stated for each step, from the keys dlt reads them from:
    ``[sources.<source>.extract]`` above ``[extract]``, and ``[normalize]``, ``[load]``. Null
    where none is stated and dlt picks at run time."""
    specs = {
        "extract_workers": (_ExtractWorkers(), ("sources", source)),
        "normalize_workers": (_NormalizeWorkers(), ()),
        "load_workers": (_LoadWorkers(), ()),
    }
    workers: dict[str, int | None] = {}
    for field, (spec, sections) in specs.items():
        try:
            workers[field] = resolve_configuration(spec, sections=sections).workers
        except Exception:  # never fail a run for a monitoring detail
            workers[field] = None
    return workers


def _metrics(info: Any, load_id: str, key: str) -> dict[str, Any]:
    """``key`` of every metrics entry a dlt step recorded for ``load_id``, merged: extract
    records one for the source and another for dlt's state."""
    merged: dict[str, Any] = {}
    for entry in info.metrics.get(load_id, []):
        merged |= entry.get(key, {})
    return merged


def _resource_of(schema: Schema, table: str) -> str | None:
    """The resource whose rows fill ``table``: its own, or its root's for a nested table."""
    if table not in schema.tables or table.startswith("_dlt"):
        return None
    return get_root_table(schema.tables, table).get("resource")


def _parent(schema: Schema, table: str) -> bool:
    return bool(schema.tables.get(table, {}).get("parent"))


def _described(
    resource: Resource, state: Mapping[str, Any] | None, *, initial: bool = False
) -> str | None:
    """A stored cursor value as text; with no state yet, the cursor's start when ``initial``."""
    cursor = resource.cursor
    if cursor is None:
        return None
    value = state.get("last_value") if state else None
    if value is None:
        return cursor.format.describe(cursor.initial_value) if initial else None
    return cursor.format.describe(value)


def _pressure(sample: SampleRecord) -> float:
    """Share of memory in use: of the container's limit, or of the host without one."""
    if sample.memory_limit_mb:
        return sample.rss_mb / sample.memory_limit_mb
    return 1 - sample.system_available_mb / sample.system_total_mb


def _last_seen(sample: dict[str, Any] | None) -> str:
    if sample is None:
        return "never sampled"
    limit = sample["memory_limit_mb"]
    of = f" of {limit:.0f}" if limit else f", host had {sample['system_available_mb']:.0f} free"
    doing = f" in {sample['step']} of {sample['resources']}" if sample["step"] else ""
    return f"{sample['at']:%H:%M:%S}Z{doing} at {sample['rss_mb']:.0f} MB{of}"


def _instant(epoch: float) -> datetime:
    return datetime.fromtimestamp(epoch, UTC)


def _aware(moment: datetime) -> datetime:
    return moment if moment.tzinfo else moment.replace(tzinfo=UTC)


def _now() -> datetime:
    return datetime.now(UTC)

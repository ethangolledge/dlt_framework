"""The monitor: turns one run's lifecycle into records and writes them through a sink."""

import logging
import socket
import threading
import time
import uuid
from collections.abc import Mapping
from datetime import UTC, datetime
from types import TracebackType
from typing import Any, Self

import dlt

from dlt_framework.core.errors import error_fields
from dlt_framework.core.monitoring.records import (
    LoadRecord,
    PipelineRecord,
    Record,
    ResourceRecord,
    RunRecord,
    SampleRecord,
)
from dlt_framework.core.monitoring.sampler import ResourceSampler
from dlt_framework.core.monitoring.sinks import Sink
from dlt_framework.core.pipeline import Pipeline
from dlt_framework.core.resource import Resource
from dlt_framework.core.windows import Window

LOGGER = logging.getLogger(__name__)


class Monitor:
    """Writes a ``started`` record and the definition rows on entry, a ``loads`` record per
    resource and window, resource samples every ``flush_s`` seconds, and a ``succeeded``/``failed``
    record at the end.

    Records are written as they happen, so a killed run still leaves its start and its memory
    trend. A failing sink is logged and never changes the run's outcome.
    """

    def __init__(
        self,
        sink: Sink,
        definition: Pipeline,
        *,
        dlt_pipeline: str,
        mode: str,
        interval_s: float = 15.0,
        flush_s: float = 60.0,
    ) -> None:
        self.sink = sink
        self.run_id = uuid.uuid4().hex
        self.definition, self.dlt_pipeline, self.mode = definition, dlt_pipeline, mode
        self.flush_s = flush_s
        self.memory_limit_mb = ResourceSampler.memory_limit_mb()
        self._sampler = ResourceSampler(interval_s, self._sampled)
        self._samples: list[Record] = []
        self._lock = threading.Lock()
        self._flushed_at = time.monotonic()

    def __enter__(self) -> Self:
        self._write(self._run_record("started"))
        self._write(self._pipeline_record())
        self._write(
            *(self._resource_record(resource) for resource in self.definition.source.resources)
        )
        self._sampler.__enter__()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self._sampler.stop()
        self._flush()

    def loaded(
        self,
        resource: Resource,
        window: Window | None,
        load_id: str,
        rows: int,
        timings: Mapping[str, float],
        cursor_state: Mapping[str, Any] | None,
    ) -> None:
        start, end = window.span() if window else (None, None)
        last_value = cursor_state.get("last_value") if cursor_state else None
        self._write(
            LoadRecord(
                load_id=load_id,
                resource_id=self._resource_id(resource),
                run_id=self.run_id,
                at=_now(),
                window_kind=window.kind if window else "increment",
                window_start=start,
                window_end=end,
                rows=rows,
                extract_s=timings.get("extract", 0.0),
                normalize_s=timings.get("normalize", 0.0),
                load_s=timings.get("load", 0.0),
                cursor_last_value=None if last_value is None else str(last_value),
            )
        )

    def finished(self, rows: int, error: BaseException | None) -> None:
        self._sampler.stop()  # the final sample belongs in the outcome's peak
        stats: dict[str, Any] = {
            "rows": rows,
            "peak_rss_mb": self._sampler.peak_rss_mb,
            "memory_limit_mb": self.memory_limit_mb,
            "cpu_seconds": self._sampler.cpu_seconds,
        }
        if error is None:
            self._write(self._run_record("succeeded", **stats))
            return
        self._write(self._run_record("failed", **stats, **error_fields(error)))

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
        return f"{self.definition.name}.{resource.name}"

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
            **details,
        )

    def _sampled(self, rss_mb: float, cpu_percent: float) -> None:
        sample = SampleRecord(
            run_id=self.run_id,
            at=_now(),
            rss_mb=rss_mb,
            cpu_percent=cpu_percent,
            memory_limit_mb=self.memory_limit_mb,
        )
        with self._lock:
            self._samples.append(sample)
        if time.monotonic() - self._flushed_at >= self.flush_s:
            self._flush()

    def _flush(self) -> None:
        with self._lock:
            batch, self._samples = self._samples, []
            self._flushed_at = time.monotonic()
        if batch:
            self._write(*batch)

    def _write(self, *records: Record) -> None:
        try:
            self.sink.write(records[0].name, type(records[0]).frame(records))
        except Exception:
            LOGGER.exception(
                "Monitoring sink %s failed; the run continues", type(self.sink).__name__
            )


def _now() -> datetime:
    return datetime.now(UTC)

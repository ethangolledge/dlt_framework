"""Running a pipeline: one spine, with incremental and backfill flavours.

Both flavours load the same source into the same dataset through ``Run.__call__``. They differ
only in the class attributes and hooks below: the dlt pipeline name (so state never mixes), the
staging dataset (so concurrent merges never share staging tables), the units of work they
extract, and the safety checks they apply.
"""

import fcntl
import logging
import time
from abc import ABC, abstractmethod
from collections import Counter
from collections.abc import Callable, Iterable
from pathlib import Path
from types import TracebackType
from typing import IO, ClassVar, Self, TypeVar

import dlt
from dlt.common.pipeline import ExtractInfo
from dlt.destinations.exceptions import DatabaseUndefinedRelation

from dlt_framework.core.errors import ConfigurationError, DefinitionError, GuardError, RunError
from dlt_framework.core.monitoring import Monitor, Sink
from dlt_framework.core.notify import RunEvent
from dlt_framework.core.pipeline import Pipeline
from dlt_framework.core.resource import Resource
from dlt_framework.core.state import stored_cursor
from dlt_framework.core.windows import Window

LOGGER = logging.getLogger(__name__)
T = TypeVar("T")

Unit = tuple[tuple[Resource, ...], Iterable[Window | None]]


class Run(ABC):
    mode: ClassVar[str]
    suffix: ClassVar[str] = ""
    staging_layout: ClassVar[str] = "%s_staging"
    enforce_empty: ClassVar[bool] = True

    def __init__(self, definition: Pipeline, sink: Sink) -> None:
        self.definition = definition
        self.sink = sink

    @property
    def name(self) -> str:
        return self.definition.name + self.suffix

    @abstractmethod
    def units(self) -> Iterable[Unit]:
        """Resources to extract together, and the windows to extract them over.

        A window of ``None`` means "from stored state". A unit stops early when a window reports
        ``is_last`` for the rows it extracted.
        """

    def check(self, pipeline: dlt.Pipeline) -> None:  # noqa: B027 - optional hook
        """Refuse to start when loading would be unsafe. Runs after state is synced."""

    def __call__(self) -> Counter[str]:
        rows: Counter[str] = Counter()
        monitor = Monitor(self.sink, self.definition, dlt_pipeline=self.name, mode=self.mode)
        with monitor:
            try:
                pipeline = dlt.pipeline(
                    pipeline_name=self.name,
                    destination=self.definition.target.factory(self.staging_layout),
                    dataset_name=self.definition.target.dataset,
                )
                with PipelineLock(pipeline.pipelines_dir, self.name):
                    self._prepare(pipeline)
                    for resources, windows in self.units():
                        for window in windows:
                            loaded = self._load(pipeline, resources, window, monitor)
                            rows.update(loaded)
                            if window is not None and window.is_last(sum(loaded.values())):
                                break
            except BaseException as error:
                monitor.finished(rows.total(), error)
                self._notify(RunEvent(self.name, self.mode, rows, error))
                raise
            monitor.finished(rows.total(), None)
        self._notify(RunEvent(self.name, self.mode, rows))
        return rows

    def _prepare(self, pipeline: dlt.Pipeline) -> None:
        retry = self.definition.retry
        retry("sync", pipeline.sync_destination)
        if pipeline.has_pending_data:
            LOGGER.warning("Loading pending packages from an interrupted run", extra=self._log())
            retry("pending", lambda: pipeline.run([]))
        self.check(pipeline)

    def _load(
        self,
        pipeline: dlt.Pipeline,
        resources: tuple[Resource, ...],
        window: Window | None,
        monitor: Monitor,
    ) -> Counter[str]:
        timings: dict[str, float] = {}
        try:
            info = self._timed(
                timings,
                "extract",
                lambda: pipeline.extract(
                    self.definition.source.build(resources, window),
                    schema_contract=self.definition.schema_contract,
                ),
            )
            (load_id,) = info.loads_ids  # one source extracted, so one package
            rows = self._row_counts(info, resources)
            if self.enforce_empty:
                self._check_empty(pipeline, resources, rows)
            self._timed(timings, "normalize", pipeline.normalize)
            self._timed(timings, "load", pipeline.load)
        except RunError as error:
            error.locate(
                ", ".join(resource.name for resource in resources), str(window or "increment")
            )
            raise
        for resource in resources:
            cursor = stored_cursor(pipeline, self.definition.source, resource)
            monitor.loaded(resource, window, load_id, rows[resource.name], timings, cursor)
        LOGGER.info("Loaded %s: %s", window or "increment", dict(rows), extra=self._log())
        return rows

    def _timed(self, timings: dict[str, float], step: str, call: Callable[[], T]) -> T:
        started = time.monotonic()
        result = self.definition.retry(step, call)
        timings[step] = time.monotonic() - started
        return result

    def _check_empty(
        self, pipeline: dlt.Pipeline, resources: Iterable[Resource], rows: Counter[str]
    ) -> None:
        for resource in resources:
            if rows[resource.name] or resource.empty == "allow":
                continue
            if resource.empty == "warn":
                LOGGER.warning("%s extracted no rows", resource.name, extra=self._log())
                continue
            pipeline.drop_pending_packages()
            raise GuardError(
                f"{resource.name} extracted no rows and its empty policy is 'fail'; nothing was "
                "loaded. Check upstream filters and credentials, or set empty='allow'",
                step="extract",
            )

    def _notify(self, event: RunEvent) -> None:
        for notifier in self.definition.notifiers:
            try:
                notifier.send(event)
            except Exception:
                LOGGER.exception("Notifier %s failed", type(notifier).__name__)

    def _log(self) -> dict[str, str]:
        return {"pipeline": self.name, "mode": self.mode}

    @staticmethod
    def _row_counts(info: ExtractInfo, resources: Iterable[Resource]) -> Counter[str]:
        """Root rows extracted per resource, ignoring dlt's internal resources."""
        names = {resource.name for resource in resources}
        rows: Counter[str] = Counter()
        for metric in info.asdict().get("resource_metrics", []):
            if metric["resource_name"] in names:
                rows[metric["resource_name"]] += metric["items_count"]
        return rows


class IncrementalRun(Run):
    """Loads every resource from its stored cursor under the production pipeline name."""

    mode = "run"

    def __init__(self, definition: Pipeline, sink: Sink, allow_initial_load: bool = False) -> None:
        super().__init__(definition, sink)
        self.allow_initial_load = allow_initial_load

    def units(self) -> Iterable[Unit]:
        return [(tuple(self.definition.source.resources), (None,))]

    def check(self, pipeline: dlt.Pipeline) -> None:
        """Refuse to restart a cursor from its start over a table that already has data.

        That combination means state was not found: the pipeline, source, or resource was
        renamed, or the dataset or destination changed. dlt would silently reload from the start.
        """
        if self.allow_initial_load:
            return
        source = self.definition.source
        lost = [
            resource.name
            for resource in source.resources
            if resource.cursor
            and stored_cursor(pipeline, source, resource) is None
            and self._has_rows(pipeline, resource.name)
        ]
        if lost:
            raise GuardError(
                f"No stored cursor for {lost} in pipeline {self.name}, but their tables already "
                "hold data. Check the pipeline, source, and resource names and the destination. "
                "Re-run with --allow-initial-load only to deliberately reload from cursor start",
                step="check",
            )

    @staticmethod
    def _has_rows(pipeline: dlt.Pipeline, table: str) -> bool:
        with pipeline.sql_client() as client:
            try:
                name = client.make_qualified_table_name(table)
                return bool(client.execute_sql(f"SELECT 1 FROM {name} LIMIT 1"))
            except DatabaseUndefinedRelation:
                return False


class BackfillRun(Run):
    """Loads history under ``<name>__backfill`` from each resource's declared backfill.

    Resources are backfilled one at a time. Time and value windows bound the cursor with
    ``end_value``, so dlt never reads or moves the run state. Nothing is checkpointed: resume an
    interrupted backfill with ``--resource`` and ``--from`` set to the end of the last loaded
    window, as logged.
    """

    mode = "backfill"
    suffix = "__backfill"
    staging_layout = "%s_backfill_staging"
    enforce_empty = False

    def __init__(
        self,
        definition: Pipeline,
        sink: Sink,
        resource: str | None = None,
        resume: str | None = None,
    ) -> None:
        super().__init__(definition, sink)
        if resume is not None and resource is None:
            raise ConfigurationError("--from needs --resource: resume points are per resource")
        self.resume = resume
        self.resources = tuple(
            item
            for item in definition.source.resources
            if item.backfill is not None and resource in (None, item.name)
        )
        if not self.resources:
            target = f"resource {resource!r}" if resource else "any resource"
            raise DefinitionError(f"Pipeline {definition.name} declares no backfill for {target}")

    def units(self) -> Iterable[Unit]:
        for resource in self.resources:
            assert resource.backfill is not None
            yield (resource,), resource.backfill.windows(resource.cursor, self.resume)


class PipelineLock:
    """Stops two processes on one host running the same pipeline name.

    Separate hosts or containers sharing a destination need single-flight from the scheduler.
    """

    def __init__(self, directory: str, name: str) -> None:
        self.name = name
        self.path = Path(directory) / f"{name}.lock"
        self._file: IO[str] | None = None

    def __enter__(self) -> Self:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._file = self.path.open("w")
        try:
            fcntl.flock(self._file, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            self._file.close()
            raise GuardError(
                f"Pipeline {self.name} is already running on this host", step="lock"
            ) from None
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        assert self._file is not None
        fcntl.flock(self._file, fcntl.LOCK_UN)
        self._file.close()

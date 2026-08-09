from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import PurePosixPath

import dlt
import pytest
from dlt.common.exceptions import TerminalException

from dlt_framework.core import runtime
from dlt_framework.core.backfill import BackfillPlan, ChunkSize
from dlt_framework.core.errors import (
    BackfillError,
    DataContractError,
    SourceDefinitionError,
    TerminalRunError,
    TransientRunError,
)
from dlt_framework.core.models import (
    PipelineConfig,
    ResourcePolicy,
    RetryConfig,
    SourceDefinition,
)


class FakePipeline:
    def __init__(self) -> None:
        self.run_args = None
        self.run_kwargs = None
        self.has_pending_data = False
        self.state = {}
        self.last_trace = None
        self.normalizes = 0
        self.loads = 0
        self.pending_runs = 0
        self.dropped = 0

    def sync_destination(self):
        return None

    def run(self, *args, **kwargs):
        if args == ([],):
            self.pending_runs += 1
            self.has_pending_data = False
        self.run_args = args
        self.run_kwargs = kwargs
        return "load info"

    def extract(self, *args, **kwargs):
        self.run_args = args
        self.run_kwargs = kwargs
        return object()

    @contextmanager
    def managed_state(self, *, extract_state=False):
        yield self.state

    def normalize(self):
        self.normalizes += 1

    def load(self):
        self.loads += 1
        return "load info"

    def drop_pending_packages(self):
        self.dropped += 1
        self.has_pending_data = False


def config(definition, **kwargs):
    return PipelineConfig(
        pipeline_name="products",
        dataset_name="raw_products",
        destination="postgres",
        source=definition,
        retry=RetryConfig(attempts=1, minimum_wait=0, maximum_wait=0),
        **kwargs,
    )


def backfill_definition() -> SourceDefinition:
    @dlt.source
    def factory(*, backfill=None):
        bounds = None if backfill is None else backfill.for_resource("events")
        return dlt.resource(
            [{"id": "current" if bounds is None else bounds.start.isoformat()}],
            name="events",
            write_disposition="merge",
            primary_key="id",
        )

    return SourceDefinition(PurePosixPath("rest/events.py"), "events", factory, legacy=True)


def three_day_plan() -> BackfillPlan:
    return BackfillPlan(
        start=datetime(2026, 1, 1, tzinfo=timezone.utc),
        end=datetime(2026, 1, 4, tzinfo=timezone.utc),
        chunk_size=ChunkSize(1, "days"),
    )


def test_run_uses_explicit_pipeline_identity(monkeypatch) -> None:
    source = object()
    fake_pipeline = FakePipeline()
    calls = []
    definition = SourceDefinition(
        PurePosixPath("rest/products.py"), "products", lambda context: None
    )
    monkeypatch.setattr(runtime, "instantiate_source", lambda value, **kwargs: source)
    monkeypatch.setattr(
        runtime.dlt,
        "pipeline",
        lambda **kwargs: calls.append(kwargs) or fake_pipeline,
    )

    result = runtime.run(config(definition))

    assert result.last_load_info == "load info"
    assert calls == [
        {
            "pipeline_name": "products",
            "dataset_name": "raw_products",
            "destination": "postgres",
        }
    ]
    assert fake_pipeline.run_args == (source,)


def test_validate_builds_normal_and_bounded_sources() -> None:
    definition = backfill_definition()
    normal = config(definition, resource="events")
    bounded = config(
        definition,
        resource="events",
        backfill=three_day_plan(),
    )

    assert list(runtime.validate(normal).selected_resources) == ["events"]
    assert list(runtime.validate(bounded).selected_resources) == ["events"]


def test_run_selects_one_resource_before_loading(monkeypatch) -> None:
    @dlt.source
    def factory():
        return [
            dlt.resource([{"id": 1}], name="products"),
            dlt.resource([{"id": 2}], name="orders"),
        ]

    fake_pipeline = FakePipeline()
    monkeypatch.setattr(runtime.dlt, "pipeline", lambda **kwargs: fake_pipeline)
    definition = SourceDefinition(PurePosixPath("rest/store.py"), "store", factory, legacy=True)

    runtime.run(config(definition, resource="orders"))

    assert list(fake_pipeline.run_args[0].selected_resources) == ["orders"]


def test_run_rejects_unknown_resource(monkeypatch) -> None:
    @dlt.source
    def factory():
        return dlt.resource([{"id": 1}], name="products")

    monkeypatch.setattr(runtime.dlt, "pipeline", lambda **kwargs: FakePipeline())
    definition = SourceDefinition(PurePosixPath("rest/store.py"), "store", factory, legacy=True)

    with pytest.raises(SourceDefinitionError, match="available: products"):
        runtime.run(config(definition, resource="orders"))


def test_run_backfills_and_persists_bounded_checkpoint(monkeypatch) -> None:
    bound_windows = []

    @dlt.source
    def factory(*, backfill=None):
        bounds = backfill.for_resource("events")
        bound_windows.append((bounds.start, bounds.end))
        return dlt.resource(
            [{"id": bounds.start.isoformat()}],
            name="events",
            write_disposition="merge",
            primary_key="id",
        )

    fake_pipeline = FakePipeline()
    monkeypatch.setattr(runtime.dlt, "pipeline", lambda **kwargs: fake_pipeline)
    definition = SourceDefinition(PurePosixPath("rest/events.py"), "events", factory, legacy=True)
    plan = BackfillPlan(
        start=datetime(2026, 1, 1, tzinfo=timezone.utc),
        end=datetime(2026, 1, 3, 6, tzinfo=timezone.utc),
        chunk_size=ChunkSize(1, "days"),
    )

    result = runtime.run(config(definition, resource="events", backfill=plan))

    assert result.chunks_loaded == 3
    assert len(bound_windows) == 3
    checkpoint = fake_pipeline.state["dlt_framework"]["backfills"]["events"]
    assert checkpoint["status"] == "completed"
    assert checkpoint["next_from"] == "2026-01-03T06:00:00Z"
    assert fake_pipeline.normalizes == fake_pipeline.loads == 6


@pytest.mark.parametrize(
    ("write_disposition", "primary_key", "message"),
    [("append", "id", "write_disposition='merge'"), ("merge", None, "primary key")],
)
def test_run_validates_backfill_load_policy(
    monkeypatch, write_disposition, primary_key, message
) -> None:
    @dlt.source
    def factory(*, backfill=None):
        backfill.for_resource("events")
        return dlt.resource(
            [{"id": 1}],
            name="events",
            write_disposition=write_disposition,
            primary_key=primary_key,
        )

    monkeypatch.setattr(runtime.dlt, "pipeline", lambda **kwargs: FakePipeline())
    definition = SourceDefinition(PurePosixPath("rest/events.py"), "events", factory, legacy=True)
    plan = BackfillPlan(
        start=datetime(2026, 1, 1, tzinfo=timezone.utc),
        end=datetime(2026, 1, 2, tzinfo=timezone.utc),
        chunk_size=ChunkSize(1, "days"),
    )

    with pytest.raises(BackfillError, match=message):
        runtime.run(config(definition, resource="events", backfill=plan))


def test_transient_operation_is_retried_and_classified() -> None:
    attempts = 0
    definition = SourceDefinition(PurePosixPath("rest/source.py"), "source", lambda context: None)

    def fail():
        nonlocal attempts
        attempts += 1
        raise ConnectionError("offline")

    with pytest.raises(TransientRunError, match="2 attempt"):
        runtime._call_with_retry(
            PipelineConfig(
                pipeline_name="source",
                dataset_name="raw_source",
                destination="postgres",
                source=definition,
                retry=RetryConfig(attempts=2, minimum_wait=0, maximum_wait=0),
            ),
            "connect",
            fail,
        )
    assert attempts == 2


def test_pending_packages_are_drained_before_extraction(monkeypatch) -> None:
    fake_pipeline = FakePipeline()
    fake_pipeline.has_pending_data = True
    monkeypatch.setattr(runtime.dlt, "pipeline", lambda **kwargs: fake_pipeline)

    @dlt.source
    def factory():
        return dlt.resource([{"id": 1}], name="rows")

    definition = SourceDefinition(PurePosixPath("rest/source.py"), "source", factory, legacy=True)
    runtime.run(config(definition))

    assert fake_pipeline.pending_runs == 1


class ExtractMetrics:
    def __init__(self, count: int) -> None:
        self.count = count

    def asdict(self):
        metrics = []
        if self.count:
            metrics.append({"resource_name": "rows", "items_count": self.count})
        return {"resource_metrics": metrics}


@pytest.mark.parametrize("empty_policy", ["allow", "warn"])
def test_nonfatal_empty_policies_do_not_drop_package(empty_policy, caplog) -> None:
    @dlt.source
    def factory():
        return dlt.resource([], name="rows")

    source = factory()
    pipeline = FakePipeline()
    runtime._check_empty_policy(
        pipeline,
        ExtractMetrics(0),
        source,
        "rows",
        {"rows": ResourcePolicy(empty=empty_policy)},
    )

    assert pipeline.dropped == 0
    if empty_policy == "warn":
        assert "produced no root rows" in caplog.text


def test_empty_fail_drops_extract_before_destination_load() -> None:
    @dlt.source
    def factory():
        return dlt.resource([], name="rows", write_disposition="replace")

    pipeline = FakePipeline()
    with pytest.raises(DataContractError, match="produced no root rows"):
        runtime._check_empty_policy(
            pipeline,
            ExtractMetrics(0),
            factory(),
            "rows",
            {"rows": ResourcePolicy(empty="fail")},
        )

    assert pipeline.dropped == 1
    assert pipeline.loads == 0


def test_terminal_exception_is_not_retried() -> None:
    definition = SourceDefinition(PurePosixPath("rest/source.py"), "source", lambda context: None)
    attempts = 0

    def fail():
        nonlocal attempts
        attempts += 1
        raise TerminalException("bad credentials")

    with pytest.raises(TerminalRunError, match="1 attempt"):
        runtime._call_with_retry(
            PipelineConfig(
                pipeline_name="source",
                dataset_name="raw_source",
                destination="postgres",
                source=definition,
                retry=RetryConfig(attempts=3, minimum_wait=0, maximum_wait=0),
            ),
            "connect",
            fail,
        )
    assert attempts == 1


def test_completed_backfill_checkpoint_skips_all_chunks() -> None:
    pipeline = FakePipeline()
    plan = three_day_plan()
    fingerprint = plan.fingerprint("events")
    pipeline.state = {
        "dlt_framework": {
            "backfills": {
                "events": {
                    "fingerprint": fingerprint,
                    "next_from": "2026-01-04T00:00:00Z",
                    "status": "completed",
                }
            }
        }
    }

    result = runtime._run_backfill(
        pipeline,
        config(backfill_definition(), resource="events", backfill=plan),
    )

    assert result.chunks_loaded == 0
    assert result.resumed_from == "2026-01-04T00:00:00Z"


def test_running_backfill_checkpoint_resumes_next_bound() -> None:
    pipeline = FakePipeline()
    plan = three_day_plan()
    pipeline.state = {
        "dlt_framework": {
            "backfills": {
                "events": {
                    "fingerprint": plan.fingerprint("events"),
                    "next_from": "2026-01-02T00:00:00Z",
                    "status": "running",
                }
            }
        }
    }

    result = runtime._run_backfill(
        pipeline,
        config(backfill_definition(), resource="events", backfill=plan),
    )

    assert result.chunks_loaded == 2
    assert result.resumed_from == "2026-01-02T00:00:00Z"


def test_conflicting_unfinished_backfill_requires_restart() -> None:
    pipeline = FakePipeline()
    pipeline.state = {
        "dlt_framework": {
            "backfills": {
                "events": {
                    "fingerprint": "another-plan",
                    "next_from": "2026-01-02T00:00:00Z",
                    "status": "running",
                }
            }
        }
    }

    with pytest.raises(BackfillError, match="different unfinished"):
        runtime._run_backfill(
            pipeline,
            config(backfill_definition(), resource="events", backfill=three_day_plan()),
        )


def test_restart_replays_conflicting_backfill() -> None:
    pipeline = FakePipeline()
    pipeline.state = {
        "dlt_framework": {
            "backfills": {
                "events": {
                    "fingerprint": "another-plan",
                    "next_from": "2026-01-02T00:00:00Z",
                    "status": "running",
                }
            }
        }
    }

    result = runtime._run_backfill(
        pipeline,
        config(
            backfill_definition(),
            resource="events",
            backfill=three_day_plan(),
            restart_backfill=True,
        ),
    )

    assert result.chunks_loaded == 3

import logging
import sqlite3
import sys
import textwrap
import time
from contextlib import closing

import polars as pl
import pytest
from conftest import ORDERS, Upstream

from dlt_framework.cli import main
from dlt_framework.core import Retry
from dlt_framework.core.errors import (
    ConfigurationError,
    ResourceExhaustedError,
    TransientRunError,
    redact,
)
from dlt_framework.core.logs import _Handler, configure
from dlt_framework.core.monitoring import (
    DatabaseSink,
    LoadRecord,
    Monitor,
    ParquetSink,
    PipelineRecord,
    ResourceRecord,
    RunRecord,
    SampleRecord,
    Sink,
)
from dlt_framework.core.monitoring.sampler import ResourceSampler
from dlt_framework.core.run import BackfillRun, IncrementalRun


def lost_runs(runs: pl.DataFrame) -> pl.DataFrame:
    """Runs that started but never reported an outcome: killed containers."""
    ended = runs.filter(pl.col("event") != "started").select("run_id")
    return runs.filter(pl.col("event") == "started").join(ended, on="run_id", how="anti")


@pytest.mark.parametrize(
    "record", [PipelineRecord, ResourceRecord, RunRecord, LoadRecord, SampleRecord]
)
def test_frames_have_fixed_schemas_even_when_empty(record) -> None:
    frame = record.frame([])
    assert frame.is_empty() and frame.schema == pl.Schema(record.schema())
    assert record.schema()["at"] == pl.Datetime("us", "UTC")


def test_sinks_resolve_from_uri_and_round_trip(tmp_path) -> None:
    assert isinstance(Sink.resolve(f"file://{tmp_path}"), ParquetSink)
    assert isinstance(Sink.resolve("s3://bucket/monitoring"), ParquetSink)
    assert isinstance(Sink.resolve("postgresql+psycopg2://u:p@h/db"), DatabaseSink)
    with pytest.raises(ConfigurationError, match="No monitoring sink") as raised:
        Sink.resolve("postgress://loader:hunter2@host/db")  # a typo'd scheme
    assert "hunter2" not in str(raised.value)

    frame = pl.DataFrame({"run_id": ["a"], "rows": [3]})
    ParquetSink(str(tmp_path / "parquet")).write("runs", frame)
    assert pl.read_parquet(tmp_path / "parquet" / "runs" / "*.parquet").equals(frame)

    database = tmp_path / "monitoring.sqlite"
    DatabaseSink(f"sqlite:///{database}").write("runs", frame)
    DatabaseSink(f"sqlite:///{database}").write("runs", frame)
    with closing(sqlite3.connect(database)) as connection:
        assert connection.execute("SELECT count(*) FROM runs").fetchone() == (2,)


@pytest.mark.parametrize(
    ("files", "limit"),
    [
        ({"memory.max": "536870912\n"}, 512.0),
        ({"memory.max": "max\n"}, None),
        ({"memory/memory.limit_in_bytes": "9223372036854771712"}, None),
        ({"memory/memory.limit_in_bytes": "1073741824"}, 1024.0),
        ({}, None),
    ],
)
def test_memory_limit_comes_from_the_container_not_the_host(tmp_path, files, limit) -> None:
    for name, content in files.items():
        (tmp_path / name).parent.mkdir(parents=True, exist_ok=True)
        (tmp_path / name).write_text(content)
    assert ResourceSampler.memory_limit_mb(tmp_path) == limit


def test_run_records_lifecycle_rows_and_decoded_cursor(make_pipeline, sink, frames) -> None:
    upstream = Upstream([{"id": 1, "updated_at": 100}, {"id": 2, "updated_at": 110}])
    IncrementalRun(make_pipeline({ORDERS: upstream}), sink)()

    runs, loads = frames("runs"), frames("loads")
    assert runs["event"].to_list() == ["started", "succeeded"]
    succeeded = runs.filter(pl.col("event") == "succeeded").row(0, named=True)
    expected = {"pipeline_id": "shop", "dlt_pipeline": "shop", "mode": "run", "rows": 2}
    assert expected.items() <= succeeded.items()
    assert runs["peak_rss_mb"].drop_nulls().item() > 0
    load = loads.row(0, named=True)
    assert (load["resource_id"], load["window_kind"], load["rows"]) == (
        "shop.orders",
        "increment",
        2,
    )
    assert load["cursor_last_value"] == "110"
    assert lost_runs(runs).is_empty()


def test_frames_join_on_their_keys_and_to_the_destination(
    make_pipeline, sink, frames, query
) -> None:
    upstream = Upstream([{"id": 1, "updated_at": 100}])
    IncrementalRun(make_pipeline({ORDERS: upstream}), sink)()
    second = make_pipeline({ORDERS: upstream}, name="shop_eu")
    IncrementalRun(second, sink, allow_initial_load=True)()  # same table: the guard would refuse

    pipelines, resources = frames("pipelines"), frames("resources")
    assert pipelines.select("pipeline_id", "source_type", "destination", "dataset").rows() == [
        ("shop", "CustomSource", "duckdb", "raw_shop"),
        ("shop_eu", "CustomSource", "duckdb", "raw_shop"),
    ]
    assert resources.select("resource_id", "write_mode", "cursor_kind", "cursor_start").rows() == [
        ("shop.orders", "Merge", "ValueCursor", "100"),
        ("shop_eu.orders", "Merge", "ValueCursor", "100"),
    ]
    runs = frames("runs").filter(pl.col("event") == "succeeded")
    joined = (
        frames("loads")
        .select("load_id", "run_id", "resource_id", "rows")
        .join(
            resources.select("run_id", "resource_id", "pipeline_id", "resource"),
            on=["run_id", "resource_id"],
        )
        .join(pipelines.select("run_id", "pipeline_id"), on=["run_id", "pipeline_id"])
        .join(runs.select("run_id", "pipeline_id"), on=["run_id", "pipeline_id"])
        .sort("pipeline_id")
    )
    assert joined.select("pipeline_id", "resource", "rows").rows() == [
        ("shop", "orders", 1),
        ("shop_eu", "orders", 1),
    ]
    loaded = {row[0] for row in query("SELECT load_id FROM raw_shop._dlt_loads")}
    assert set(joined["load_id"]) <= loaded


def test_backfill_records_one_load_per_window(make_pipeline, sink, frames) -> None:
    upstream = Upstream([{"id": 1, "updated_at": 10}, {"id": 2, "updated_at": 60}])
    BackfillRun(make_pipeline({ORDERS: upstream}), sink)()
    loads = frames("loads")
    assert loads.select("window_kind", "window_start", "window_end", "rows").rows() == [
        ("value", "0", "50", 1),
        ("value", "50", "100", 1),
    ]
    assert frames("runs")["dlt_pipeline"].unique().to_list() == ["shop__backfill"]


def test_failure_records_where_and_why(make_pipeline, sink, frames) -> None:
    """dlt treats unknown errors as transient, so this exhausts retries (exit 4)."""

    def broken(incremental, window):
        raise ValueError("upstream returned garbage")
        yield

    with pytest.raises(TransientRunError) as raised:
        IncrementalRun(make_pipeline({ORDERS: broken}), sink)()
    assert (raised.value.step, raised.value.resource, raised.value.window) == (
        "extract",
        "orders",
        "increment",
    )
    assert raised.value.location == "extract of orders, window increment"
    failed = frames("runs").filter(pl.col("event") == "failed").row(0, named=True)
    assert {
        "exit_code": 4,
        "error_type": "TransientRunError",
        "error_step": "extract",
        "error_resource": "orders",
        "error_window": "increment",
        "error_cause_type": "ValueError",
        "error_message": "upstream returned garbage",
    }.items() <= failed.items()


def test_memory_error_is_not_retried_and_exits_5(make_pipeline, sink, frames) -> None:
    calls = []

    def hungry(incremental, window):
        calls.append(1)
        raise MemoryError
        yield

    definition = make_pipeline({ORDERS: hungry})
    object.__setattr__(definition, "retry", Retry(attempts=3, min_wait=0, max_wait=0))
    with pytest.raises(ResourceExhaustedError):
        IncrementalRun(definition, sink)()
    assert len(calls) == 1
    failed = frames("runs").filter(pl.col("event") == "failed").row(0, named=True)
    assert (failed["exit_code"], failed["error_cause_type"]) == (5, "MemoryError")


def test_a_killed_run_leaves_its_start_and_memory_trend(make_pipeline, sink, frames) -> None:
    definition = make_pipeline({ORDERS: Upstream([])})
    monitor = Monitor(sink, definition, dlt_pipeline="shop", mode="run", interval_s=0.01, flush_s=0)
    monitor.__enter__()
    time.sleep(0.1)
    monitor._sampler.stop()  # the process dies: no finished(), no __exit__

    lost = lost_runs(frames("runs"))
    assert lost["run_id"].to_list() == [monitor.run_id]
    assert frames("samples").filter(pl.col("run_id") == monitor.run_id).height >= 2


def test_a_failing_sink_never_fails_the_run(make_pipeline, caplog) -> None:
    class Broken(Sink):
        @classmethod
        def from_uri(cls, uri: str) -> Sink:
            return cls()

        def write(self, name: str, frame: pl.DataFrame) -> None:
            raise OSError("disk full")

    upstream = Upstream([{"id": 1, "updated_at": 100}])
    assert IncrementalRun(make_pipeline({ORDERS: upstream}), Broken())()["orders"] == 1
    assert "Monitoring sink Broken failed" in caplog.text


BROKEN_PIPELINE = """
from dlt_framework.core import FREEZE_DATA_TYPES, Pipeline, Replace, Resource, Retry, Target
from dlt_framework.sources.custom import CustomSource

def orders(incremental, window):
    raise ValueError("upstream exploded")
    yield

Pipeline(
    name="broken",
    target=Target("duckdb", dataset="raw_broken"),
    schema_contract=FREEZE_DATA_TYPES,
    retry=Retry(attempts=1),
    source=CustomSource(
        name="broken",
        resources={Resource(name="orders", write=Replace(), cursor=None, empty="fail"): orders},
    ),
)
"""


@pytest.fixture
def broken_package(tmp_path, monkeypatch) -> str:
    root = tmp_path / "failing" / "pipelines"
    root.mkdir(parents=True)
    (tmp_path / "failing" / "__init__.py").write_text("")
    (root / "__init__.py").write_text("")
    (root / "broken.py").write_text(textwrap.dedent(BROKEN_PIPELINE))
    monkeypatch.syspath_prepend(str(tmp_path))
    for module in [name for name in sys.modules if name.split(".")[0] == "failing"]:
        monkeypatch.delitem(sys.modules, module)  # re-import, so the pipeline registers again
    return "failing"


def test_cli_failure_line_says_what_where_and_why(broken_package, caplog) -> None:
    assert main(["--package", broken_package, "run", "broken"]) == 4
    record = next(r for r in caplog.records if getattr(r, "event", None) == "run_failed")
    assert record.getMessage() == (
        "run broken failed in extract of orders, window increment: "
        "ValueError: upstream exploded (after 1 attempt) (TransientRunError, exit 4)"
    )
    assert record.exc_info is not None and record.exc_info[0] is ValueError


def test_cli_requires_a_stated_monitoring_sink(broken_package, monkeypatch, caplog) -> None:
    # dlt reads .dlt/ once per process; an empty variable overrides the repo's secrets.toml.
    monkeypatch.setenv("MONITORING__URI", "")
    assert main(["--package", broken_package, "run", "broken"]) == 2
    assert "monitoring.uri is not set" in caplog.text


def test_dlt_logs_share_the_framework_handler() -> None:
    configure("json", "INFO")
    handler = logging.getLogger().handlers[0]
    assert logging.getLogger("dlt").handlers == [handler]
    formatter = handler.formatter
    handler.setFormatter(logging.Formatter("%(message)s"))  # what dlt does on pipeline start
    assert handler.formatter is formatter


@pytest.mark.parametrize(
    ("text", "masked"),
    [
        (
            "401 Client Error: Unauthorized for url: https://api.x.com/o?api_key=hunter2&limit=5",
            "401 Client Error: Unauthorized for url: https://api.x.com/o?api_key=***&limit=***",
        ),
        (
            "could not connect to postgresql://loader:hunter2@db/x",
            "could not connect to postgresql://loader:***@db/x",
        ),
        ("no secrets in https://api.x.com/orders", "no secrets in https://api.x.com/orders"),
    ],
)
def test_redact_masks_url_secrets(text, masked) -> None:
    assert redact(text) == masked


def test_logs_and_monitoring_never_hold_url_secrets(make_pipeline, sink, frames) -> None:
    def unauthorised(incremental, window):
        raise ValueError("401 for url: https://api.x.com/orders?api_key=hunter2")
        yield

    with pytest.raises(TransientRunError) as raised:
        IncrementalRun(make_pipeline({ORDERS: unauthorised}), sink)()
    error = raised.value
    record = logging.makeLogRecord(
        {"msg": "failed: %s", "args": (error,), "exc_info": (type(error), error, None)}
    )
    handler = _Handler()
    handler.setFormatter(logging.Formatter())
    logged = handler.format(record)
    assert "api_key=***" in logged and "hunter2" not in logged
    failed = frames("runs").filter(pl.col("event") == "failed").row(0, named=True)
    assert failed["error_message"].endswith("?api_key=***")


def test_cli_logs_an_unexpected_bug_with_its_traceback(broken_package, monkeypatch, caplog) -> None:
    def bug(self):
        raise KeyError("framework bug")

    monkeypatch.setattr(IncrementalRun, "__call__", bug)
    assert main(["--package", broken_package, "run", "broken"]) == 1
    record = next(r for r in caplog.records if getattr(r, "event", None) == "run_failed")
    assert record.getMessage() == "run broken failed: 'framework bug' (KeyError, exit 1)"
    assert record.exc_info is not None and record.exc_info[0] is KeyError

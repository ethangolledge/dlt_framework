import io
import json
import logging
import os
import re
import sqlite3
import time
from contextlib import closing
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import ClassVar

import polars as pl
import pytest
import sqlalchemy as sa
from conftest import ORDERS, Upstream, define

import dlt_framework
from dlt_framework.cli import main
from dlt_framework.core import (
    Append,
    Duration,
    Replace,
    Resource,
    RowBackfill,
    TimeBackfill,
    TimeCursor,
    UnixSeconds,
)
from dlt_framework.core.errors import (
    ConfigurationError,
    ResourceExhaustedError,
    TerminalRunError,
    code_path,
    redact,
)
from dlt_framework.core.logs import JsonFormatter, TextFormatter, _Handler
from dlt_framework.core.monitoring import (
    Coverage,
    DatabaseSink,
    LoadRecord,
    Monitor,
    ParquetSink,
    PipelineRecord,
    Record,
    ResourceRecord,
    RunRecord,
    SampleRecord,
    Sink,
    sinks,
    status,
)
from dlt_framework.core.monitoring.sampler import ResourceSampler
from dlt_framework.core.run import BackfillRun, IncrementalRun
from dlt_framework.core.settings import MonitoringSettings
from dlt_framework.core.windows import RowWindow, TimeWindow, ValueWindow, Window
from dlt_framework.sources.filesystem import Csv, Files, FilesystemSource


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
    pytest.importorskip("psycopg2")  # the postgres extra: resolving a Postgres URL loads it
    assert isinstance(Sink.resolve(f"file://{tmp_path}"), ParquetSink)
    assert isinstance(Sink.resolve("s3://bucket/monitoring"), ParquetSink)
    assert isinstance(Sink.resolve("postgresql+psycopg2://u:p@h/db"), DatabaseSink)
    with pytest.raises(ConfigurationError) as raised:
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


@dataclass(frozen=True, kw_only=True)
class Event(Record):
    name: ClassVar[str] = "events"

    run_id: str
    rows: int | None = None


def postgres_sink(tmp_path) -> DatabaseSink:
    url = os.environ.get("TEST_POSTGRES_URL")
    if not url:
        pytest.skip("TEST_POSTGRES_URL is not configured")
    sink = DatabaseSink(url)
    with sink.engine.begin() as connection:
        connection.execute(sa.text("DROP TABLE IF EXISTS events"))
    return sink


@pytest.mark.parametrize(
    "sink",
    [
        lambda tmp_path: ParquetSink((tmp_path / "monitoring").as_uri()),
        lambda tmp_path: DatabaseSink(f"sqlite:///{tmp_path / 'monitoring.sqlite'}"),
        pytest.param(postgres_sink, marks=pytest.mark.postgres),
    ],
)
def test_sinks_read_back_nothing_then_rows_written_before_a_column_existed(tmp_path, sink) -> None:
    sink = sink(tmp_path)
    assert sink.read(Event).is_empty()
    sink.write("events", Event.frame([Event(run_id="a")]).drop("rows"))  # an older record shape
    sink.write("events", Event.frame([Event(run_id="b", rows=3)]))
    assert sink.read(Event).sort("run_id").rows() == [("a", None), ("b", 3)]


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


def test_run_records_lifecycle_rows_and_decoded_cursor(make_pipeline, frames) -> None:
    upstream = Upstream([{"id": 1, "updated_at": 100}, {"id": 2, "updated_at": 110}])
    IncrementalRun(make_pipeline({ORDERS: upstream}))()

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
    IncrementalRun(make_pipeline({ORDERS: upstream}))()
    IncrementalRun(make_pipeline({ORDERS: upstream}, name="shop_eu", dataset="raw_shop_eu"))()

    pipelines, resources = frames("pipelines"), frames("resources")
    assert pipelines.select("pipeline_id", "source_type", "destination", "dataset").rows() == [
        ("shop", "CustomSource", "duckdb", "raw_shop"),
        ("shop_eu", "CustomSource", "duckdb", "raw_shop_eu"),
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
    loaded = {
        load_id
        for dataset in ("raw_shop", "raw_shop_eu")
        for (load_id,) in query(f"SELECT load_id FROM {dataset}._dlt_loads")
    }
    assert set(joined["load_id"]) <= loaded


def test_backfill_records_one_load_per_window(make_pipeline, frames) -> None:
    upstream = Upstream([{"id": 1, "updated_at": 10}, {"id": 2, "updated_at": 60}])
    BackfillRun(make_pipeline({ORDERS: upstream}))()
    loads = frames("loads")
    assert loads.select("window_kind", "window_start", "window_end", "rows").rows() == [
        ("value", "0", "50", 1),
        ("value", "50", "100", 1),
    ]
    assert frames("runs")["dlt_pipeline"].unique().to_list() == ["shop__backfill"]


def test_failure_records_where_and_why(make_pipeline, frames) -> None:
    """An unknown error is not retried: it fails the run at once (exit 3)."""

    def broken(incremental, window):
        raise ValueError("upstream returned garbage")
        yield

    with pytest.raises(TerminalRunError) as raised:
        IncrementalRun(make_pipeline({ORDERS: broken}))()
    assert (raised.value.step, raised.value.resource, raised.value.window) == (
        "extract",
        "orders",
        "increment",
    )
    failed = frames("runs").filter(pl.col("event") == "failed").row(0, named=True)
    assert {
        "exit_code": 3,
        "error_type": "TerminalRunError",
        "error_step": "extract",
        "error_resource": "orders",
        "error_window": "increment",
        "error_cause_type": "ValueError",
        "error_message": "upstream returned garbage",
    }.items() <= failed.items()


def test_memory_error_is_not_retried_and_exits_5(make_pipeline, frames, monkeypatch) -> None:
    calls = []

    def hungry(incremental, window):
        calls.append(1)
        raise MemoryError
        yield

    definition = make_pipeline({ORDERS: hungry})
    monkeypatch.setenv("RETRY__ATTEMPTS", "3")
    with pytest.raises(ResourceExhaustedError):
        IncrementalRun(definition)()
    assert len(calls) == 1
    failed = frames("runs").filter(pl.col("event") == "failed").row(0, named=True)
    assert (failed["exit_code"], failed["error_cause_type"]) == (5, "MemoryError")


def test_a_killed_run_leaves_its_start_and_memory_trend(make_pipeline, sink, frames) -> None:
    definition = make_pipeline({ORDERS: Upstream([])})
    every = MonitoringSettings(
        uri="unused", sample_every_s=0.01, flush_every_s=0, code_version="test", profile_every_ms=0
    )
    monitor = Monitor(sink, definition, every, dlt_pipeline="shop", mode="run")
    monitor.__enter__()
    monitor._sampler.stop()  # the process dies: no finished(), no __exit__

    lost = lost_runs(frames("runs"))
    assert lost["run_id"].to_list() == [monitor.run_id]
    assert frames("samples").filter(pl.col("run_id") == monitor.run_id).height >= 1
    (row,) = status(definition, frames("runs"), LoadRecord.frame([]))
    assert (row["mode"], row["outcome"], row["last_success"]) == ("run", "lost or running", "-")


@sinks.register("broken")
class BrokenSink(Sink):
    """A sink a developer registered, which fails on every write."""

    @classmethod
    def from_uri(cls, uri: str) -> "BrokenSink":
        return cls()

    def write(self, name: str, frame: pl.DataFrame) -> None:
        raise OSError("disk full")

    def read(self, record: type[Record]) -> pl.DataFrame:
        return record.frame([])


def test_a_registered_sink_is_chosen_by_scheme_and_never_fails_the_run(
    make_pipeline, monkeypatch
) -> None:
    monkeypatch.setenv("MONITORING__URI", "broken://anywhere")
    upstream = Upstream([{"id": 1, "updated_at": 100}])
    assert IncrementalRun(make_pipeline({ORDERS: upstream}))()["orders"] == 1


BROKEN_PIPELINE = """
from dlt_framework.core import FREEZE_DATA_TYPES, DuckDb, Pipeline, Replace, Resource
from dlt_framework.sources.custom import CustomSource

def orders(incremental, window):
    raise ValueError("upstream exploded")
    yield

Pipeline(
    name="broken",
    target=DuckDb(dataset="raw_broken"),
    schema_contract=FREEZE_DATA_TYPES,
    source=CustomSource(
        name="broken",
        resources={Resource(name="orders", write=Replace(), cursor=None, empty="fail"): orders},
    ),
)
"""


@pytest.fixture
def broken_package(write_package) -> str:
    return write_package("failing", "broken", BROKEN_PIPELINE)


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


def test_logs_and_monitoring_never_hold_url_secrets(make_pipeline, frames) -> None:
    def unauthorised(incremental, window):
        raise ValueError("401 for url: https://api.x.com/orders?api_key=hunter2")
        yield

    with pytest.raises(TerminalRunError) as raised:
        IncrementalRun(make_pipeline({ORDERS: unauthorised}))()
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
    assert record.exc_info is not None and record.exc_info[0] is KeyError  # traceback logged


def loads(*windows: tuple[Window, int]) -> pl.DataFrame:
    """A ``loads`` frame holding successful backfill windows of ``shop.orders``."""
    records = [
        LoadRecord(
            load_id=str(index),
            resource_id="shop.orders",
            run_id="r",
            at=datetime.now(UTC),
            window_kind=window.kind,
            window_start=window.span()[0],
            window_end=window.span()[1],
            part=index,
            rows=rows,
            child_rows=0,
            bytes=0,
            extract_first_at=None,
            extract_last_at=None,
            cursor_start_value=None,
            cursor_last_value=None,
        )
        for index, (window, rows) in enumerate(windows)
    ]
    return LoadRecord.frame(records)


def test_coverage_of_value_windows() -> None:
    backfill, cursor = ORDERS.backfill, ORDERS.cursor  # [0, 100) in steps of 50
    half = Coverage.of(loads((ValueWindow(50, 100), 3)), "shop.orders", backfill, cursor)
    assert (half.gaps, half.fraction) == ([(0, 50)], 0.5)
    overlapping = loads(
        (ValueWindow(0, 30), 1), (ValueWindow(20, 50), 1), (ValueWindow(50, 100), 0)
    )
    full = Coverage.of(overlapping, "shop.orders", backfill, cursor)
    assert (full.gaps, full.fraction) == ([], 1.0)
    assert Coverage.of(loads(), "other.orders", backfill, cursor).gaps == [(0, 100)]


def test_coverage_of_time_windows() -> None:
    day = timedelta(days=1)
    start = datetime(2026, 1, 1, tzinfo=UTC)
    cursor = TimeCursor(path="t", start=start + 3 * day, format=UnixSeconds(), on_missing="raise")
    backfill = TimeBackfill(start=start, every=Duration(1, "days"))
    frame = loads(
        (TimeWindow(start, start + day), 5), (TimeWindow(start + 2 * day, start + 3 * day), 5)
    )
    coverage = Coverage.of(frame, "shop.orders", backfill, cursor)
    assert coverage.gaps == [(start + day, start + 2 * day)]
    assert coverage.fraction == 2 / 3


def test_coverage_of_rows_is_open_until_a_short_page() -> None:
    backfill = RowBackfill(size=2)
    open_ended = Coverage.of(loads((RowWindow(0, 2), 2)), "shop.orders", backfill, None)
    assert (open_ended.gaps, open_ended.fraction) == ([(2, None)], None)
    finished = loads((RowWindow(0, 2), 2), (RowWindow(2, 2), 1))
    complete = Coverage.of(finished, "shop.orders", backfill, None)
    assert (complete.gaps, complete.fraction) == ([], 1.0)


def test_sinks_read_back_what_they_wrote(tmp_path) -> None:
    frame = loads((ValueWindow(0, 50), 2))
    for sink in (
        ParquetSink(str(tmp_path / "parquet")),
        DatabaseSink(f"sqlite:///{tmp_path}/m.db"),
    ):
        assert sink.read(LoadRecord).is_empty()
        sink.write(LoadRecord.name, frame)
        back = sink.read(LoadRecord)
        assert back.schema == pl.Schema(LoadRecord.schema())
        assert back.drop("at").equals(frame.drop("at"))


# ---- what each frame says about a run: every column against a run whose answers are known --


CUSTOMERS = Resource(name="customers", write=Replace(), cursor=None, empty="fail")


def test_loads_and_tables_say_what_each_resource_and_table_received(make_pipeline, frames) -> None:
    orders = Upstream(
        [
            {"id": 1, "updated_at": 100, "lines": [{"sku": "a"}, {"sku": "b"}]},
            {"id": 2, "updated_at": 110, "lines": [{"sku": "c"}]},
        ]
    )
    customers = Upstream([{"id": 7, "updated_at": None}])
    definition = make_pipeline({ORDERS: orders, CUSTOMERS: customers})
    IncrementalRun(definition)()

    loads = frames("loads").sort("resource_id")
    assert loads.select(
        "resource_id", "part", "rows", "child_rows", "cursor_start_value", "cursor_last_value"
    ).rows() == [
        ("shop.customers", 0, 1, 0, None, None),
        ("shop.orders", 0, 2, 3, "100", "110"),  # from the cursor's start; three nested lines
    ]
    assert (loads["bytes"] > 0).all()
    assert (loads["extract_first_at"] <= loads["extract_last_at"]).all()
    assert loads["load_id"].n_unique() == 1  # extracted together, one package

    tables = frames("tables").sort("table")
    assert tables.select("resource_id", "table", "rows").rows() == [
        ("shop.customers", "customers", 1),
        ("shop.orders", "orders", 2),
        ("shop.orders", "orders__lines", 3),
    ]
    assert (tables["jobs"] >= 1).all() and (tables["load_s"] >= 0).all()
    assert (tables["retries"] == 0).all()

    orders.rows.append({"id": 3, "updated_at": 120, "lines": []})
    IncrementalRun(definition)()
    second = (
        frames("loads")
        .filter(pl.col("resource_id") == "shop.orders")
        .sort("at")
        .row(-1, named=True)
    )
    assert (second["cursor_start_value"], second["cursor_last_value"]) == ("110", "120")


def test_steps_record_each_attempt_with_its_load(make_pipeline, frames, monkeypatch) -> None:
    """A step that heals on its second attempt shows both; steps of a load carry its id."""
    calls = []

    def flaky(incremental, window):
        calls.append(1)
        if len(calls) == 1:
            raise ConnectionError("upstream reset the connection")
        yield [{"id": 1, "updated_at": 100}]

    monkeypatch.setenv("RETRY__ATTEMPTS", "2")
    IncrementalRun(make_pipeline({ORDERS: flaky}))()

    steps = frames("steps").sort("started_at")
    assert steps.select("step", "attempt", "outcome", "error_cause_type").rows() == [
        ("sync", 1, "ok", None),
        ("extract", 1, "failed", "ConnectionError"),
        ("extract", 2, "ok", None),
        ("normalize", 1, "ok", None),
        ("load", 1, "ok", None),
    ]
    (load_id,) = frames("loads")["load_id"]
    assert steps.filter(pl.col("step") != "sync")["load_id"].to_list() == [load_id] * 4
    assert steps.filter(pl.col("step") == "extract")["resources"].to_list() == ["orders"] * 2
    assert (steps["duration_s"] >= 0).all() and (steps["cpu_s"] >= 0).all()
    run = frames("runs").filter(pl.col("event") == "succeeded").row(0, named=True)
    assert run["duration_s"] >= steps["duration_s"].sum()


def test_samples_say_what_was_running(make_pipeline, frames, monkeypatch) -> None:
    def slow(incremental, window):
        time.sleep(0.3)
        yield [{"id": 1, "updated_at": 100}]

    monkeypatch.setenv("MONITORING__SAMPLE_EVERY_S", "0.02")
    IncrementalRun(make_pipeline({ORDERS: slow}))()

    samples = frames("samples")
    extracting = samples.filter(pl.col("step") == "extract")
    assert extracting.height >= 3 and set(extracting["resources"]) == {"orders"}
    assert (samples["system_total_mb"] >= samples["system_available_mb"]).all()
    assert (samples["threads"] >= 1).all() and (samples["rss_mb"] > 0).all()


def test_samples_are_written_at_once_under_memory_pressure(make_pipeline, sink, frames) -> None:
    settings = MonitoringSettings(
        uri="unused", sample_every_s=3600, flush_every_s=3600, code_version="t", profile_every_ms=0
    )
    monitor = Monitor(sink, make_pipeline({}), settings, dlt_pipeline="shop", mode="run")
    monitor.memory_limit_mb = 1000.0
    calm = {"rss_mb": 100.0, "cpu_percent": 1.0, "threads": 1, "read_mb_s": None}
    calm |= {"write_mb_s": None, "system_available_mb": 1.0, "system_total_mb": 2.0}
    monitor._sampled(calm)
    assert sink.read(SampleRecord).is_empty()  # held until the next flush
    monitor._sampled(calm | {"rss_mb": 800.0})  # 80% of the limit
    assert sink.read(SampleRecord)["rss_mb"].to_list() == [100.0, 800.0]


def test_the_next_run_records_a_killed_run_as_lost(make_pipeline, sink, frames, caplog) -> None:
    definition = make_pipeline({ORDERS: Upstream([{"id": 1, "updated_at": 100}])})
    every = MonitoringSettings(
        uri="unused", sample_every_s=0.01, flush_every_s=0, code_version="t", profile_every_ms=0
    )
    killed = Monitor(sink, definition, every, dlt_pipeline="shop", mode="run")
    killed.__enter__()
    killed._phase = ("load", "orders")
    time.sleep(0.05)
    killed._sampler.stop()  # the process dies: no finished(), no __exit__

    IncrementalRun(definition)()
    IncrementalRun(definition)()  # reported once, not again

    lost = frames("runs").filter(pl.col("event") == "lost")
    assert lost["run_id"].to_list() == [killed.run_id]
    row = lost.row(0, named=True)
    assert (row["error_step"], row["error_resource"], row["code_version"]) == (
        "load",
        "orders",
        "t",
    )
    assert row["duration_s"] > 0 and row["peak_rss_mb"] > 0
    (logged,) = [r for r in caplog.records if getattr(r, "event", None) == "run_lost"]
    assert killed.run_id in logged.getMessage() and "in load of orders" in logged.getMessage()


def test_a_failure_names_the_resource_that_broke_not_all_extracted_with_it(
    make_pipeline, frames
) -> None:
    def broken(incremental, window):
        raise ValueError("upstream returned garbage")
        yield

    definition = make_pipeline(
        {CUSTOMERS: Upstream([{"id": 1, "updated_at": None}]), ORDERS: broken}
    )
    with pytest.raises(TerminalRunError) as raised:
        IncrementalRun(definition)()
    assert raised.value.resource == "orders"
    failed = frames("runs").filter(pl.col("event") == "failed").row(0, named=True)
    assert failed["error_resource"] == "orders"
    # Innermost in this package: the wrapper around a custom extract; a project's own extract
    # code lives in the package too, so there it is named itself.
    assert re.fullmatch(r"dlt_framework/core/source\.py:\d+ in run", failed["error_where"])
    assert failed["error_origin"].endswith("in broken")
    assert failed["error_chain"].splitlines()[-1] == "ValueError: upstream returned garbage"
    assert "Traceback" in failed["error_traceback"]


def test_a_bad_file_is_named_in_the_failure(tmp_path, frames) -> None:
    folder = tmp_path / "in"
    folder.mkdir()
    (folder / "good.csv").write_text("id\n1\n")
    (folder / "bad.csv").write_bytes(b"id\n1\n2\xff\xfe\n")  # not UTF-8: DuckDB refuses it
    resource = Resource(name="orders", write=Append(), cursor=None, empty="fail")
    files = Files(bucket_url=folder.as_uri(), file_glob="*.csv", reader=Csv())
    with pytest.raises(TerminalRunError):
        IncrementalRun(
            define("files", FilesystemSource(name="files", resources={resource: files}))
        )()
    failed = frames("runs").filter(pl.col("event") == "failed").row(0, named=True)
    assert failed["error_notes"] == f"while reading {(folder / 'bad.csv').as_uri()}"
    assert failed["error_where"].startswith("dlt_framework/sources/filesystem/source.py:")


@pytest.mark.parametrize("formatter", [JsonFormatter, TextFormatter])
def test_every_log_line_of_a_run_carries_its_run_id(
    make_pipeline, frames, caplog, formatter
) -> None:
    """Through the handler the CLI installs, framework and dlt lines alike."""
    caplog.set_level(logging.INFO)
    stream = io.StringIO()
    handler = _Handler()
    handler.setStream(stream)
    handler.setFormatter(formatter())
    root = logging.getLogger()
    root.addHandler(handler)
    try:
        IncrementalRun(make_pipeline({ORDERS: Upstream([])}))()  # warns "extracted no rows"
    finally:
        root.removeHandler(handler)
    (run_id,) = frames("runs")["run_id"].unique()
    (load_id,) = frames("loads")["load_id"]
    lines = [line for line in stream.getvalue().splitlines() if "dlt_framework" in line]
    assert any("extracted no rows" in line for line in lines)
    if formatter is TextFormatter:
        assert all(f"[{run_id}]: " in line for line in lines)
        return
    logged = [json.loads(line) for line in lines]
    assert all(line["run_id"] == run_id and line["pipeline"] == "shop" for line in logged)
    (loaded,) = [line for line in logged if line["message"].startswith("Loaded")]
    assert loaded["load_id"] == load_id


def crunch(seconds: float) -> int:
    """Busy work a profile should find."""
    end, total = time.monotonic() + seconds, 0
    while time.monotonic() < end:
        total += sum(range(200))
    return total


def test_a_profiled_run_finds_where_extract_spends_its_time(
    make_pipeline, frames, monkeypatch
) -> None:
    def busy(incremental, window):
        crunch(0.5)
        yield [{"id": 1, "updated_at": 100}]

    monkeypatch.setenv("MONITORING__PROFILE_EVERY_MS", "5")
    IncrementalRun(make_pipeline({ORDERS: busy}))()

    profile = frames("profiles").filter(pl.col("step") == "extract")
    top = profile.sort("own_samples", descending=True).row(0, named=True)
    assert (top["function"], top["file"]) == ("test_monitoring:crunch", "test_monitoring.py")
    assert top["own_samples"] == top["total_samples"] > 0  # it calls no Python of its own
    assert 0 < top["own_share"] <= 1


def test_code_paths_are_anchored_on_the_package_not_a_folder_name() -> None:
    package = Path(dlt_framework.__file__).parent
    assert code_path(str(package / "core" / "run.py")) == "dlt_framework/core/run.py"
    installed = "/venv/lib/python3.14/site-packages/dlt/extract/pipe.py"
    assert code_path(installed) == "dlt/extract/pipe.py"
    assert code_path("/home/me/dlt_framework/tests/test_x.py") == "test_x.py"  # a repo, not ours

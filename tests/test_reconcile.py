"""``pipeline reconcile``: counts of what the destination holds, against upstream where it can
count, so missing and duplicated records are found, not inferred."""

from datetime import UTC, datetime

import polars as pl
import pytest
from conftest import HEALTH, define

from dlt_framework.cli import main
from dlt_framework.core import Integer, Merge, Resource, ValueBackfill, ValueCursor
from dlt_framework.core.monitoring import (
    LoadRecord,
    ParquetSink,
    ReconcileRecord,
    RunRecord,
    StepRecord,
)
from dlt_framework.core.monitoring.health import health
from dlt_framework.core.reconcile import reconcile
from dlt_framework.core.run import BackfillRun, IncrementalRun
from dlt_framework.sources.sql import SqlAlchemy, SqlSource

ORDERS = Resource(
    name="orders",
    write=Merge(primary_key=("id",), strategy="delete-insert"),
    cursor=ValueCursor(path="version", start=40, format=Integer(), on_missing="raise"),
    empty="warn",
    backfill=ValueBackfill(start=10, every=20),  # [10, 30) and [30, 40)
)


@pytest.fixture
def erp(sqlite):
    """Five orders at versions 10 to 50, loaded: 40 and 50 by a run, the rest by a backfill."""
    sqlite(
        "erp",
        "Orders",
        "id INTEGER PRIMARY KEY, version INTEGER",
        [(i, i * 10) for i in range(1, 6)],
    )
    source = SqlSource(name="erp", backend=SqlAlchemy(), resources={ORDERS: {"table": "Orders"}})
    definition = define("erp", source)
    IncrementalRun(definition)()
    BackfillRun(definition)()
    return definition


def counts(definition, source: bool = True) -> list[tuple]:
    records = reconcile(IncrementalRun(definition), source)
    return [
        (
            r.window_kind,
            r.window_start,
            r.window_end,
            r.destination_rows,
            r.distinct_keys,
            r.source_rows,
        )
        for r in records
    ]


def test_a_complete_table_matches_upstream_in_total_and_per_window(erp) -> None:
    assert counts(erp) == [
        ("all", None, None, 5, 5, 5),
        ("value", "10", "30", 2, None, 2),
        ("value", "30", "40", 1, None, 1),
    ]
    assert [row[-1] for row in counts(erp, source=False)] == [None, None, None]


def test_a_lost_row_shows_in_its_window_and_a_duplicate_in_the_keys(erp, query) -> None:
    query("DELETE FROM raw_shop.orders WHERE id = 2")
    assert counts(erp)[:2] == [("all", None, None, 4, 4, 5), ("value", "10", "30", 1, None, 2)]

    query("INSERT INTO raw_shop.orders SELECT * FROM raw_shop.orders WHERE id = 4")
    assert counts(erp)[0] == ("all", None, None, 5, 4, 5)


def test_health_reports_the_latest_reconciliation(erp, query, sink) -> None:
    query("DELETE FROM raw_shop.orders WHERE id = 2")
    query("INSERT INTO raw_shop.orders SELECT * FROM raw_shop.orders WHERE id = 4")
    reconciled = ReconcileRecord.frame(reconcile(IncrementalRun(erp), True))
    frames = (sink.read(RunRecord), LoadRecord.frame([]), StepRecord.frame([]), reconciled)
    found = [
        (signal.resource, signal.detail)
        for signal in health(erp, HEALTH, *frames)
        if signal.event == "reconcile_mismatch"
    ]
    assert found == [
        (
            "orders",
            "1 duplicate keys; "
            "1 count(s) differ from the source, first [10, 30): 1 loaded, 2 upstream",
        )
    ]


def test_the_cli_records_each_reconciliation(erp, sink, monkeypatch, capsys) -> None:
    monkeypatch.setattr("dlt_framework.core.pipeline.Pipeline.discover", lambda package: None)
    assert main(["reconcile", "erp", "--source"]) == 0
    assert main(["reconcile", "erp"]) == 0
    frame = sink.read(ReconcileRecord)
    assert frame["reconcile_id"].n_unique() == 2 and frame.height == 6
    assert "orders" in capsys.readouterr().out


def test_compaction_keeps_every_row_in_one_file(tmp_path) -> None:
    sink = ParquetSink(str(tmp_path / "m"))
    frames = [ReconcileRecord.frame(reconcile_rows(index)) for index in range(3)]
    for frame in frames:
        sink.write(ReconcileRecord.name, frame)
    assert sink.compact(ReconcileRecord) == 3
    assert len(list((tmp_path / "m" / "reconciliations").iterdir())) == 1
    assert sink.read(ReconcileRecord).sort("reconcile_id").equals(pl.concat(frames))
    assert sink.compact(ReconcileRecord) == 0  # one file: nothing to merge


def reconcile_rows(index: int) -> list[ReconcileRecord]:
    return [
        ReconcileRecord(
            reconcile_id=str(index),
            resource_id="erp.orders",
            at=datetime(2026, 1, 1, tzinfo=UTC),
            window_kind="all",
            window_start=None,
            window_end=None,
            destination_rows=index,
            distinct_keys=None,
            source_rows=None,
        )
    ]

from datetime import UTC, datetime, timedelta

import dlt
import pytest
from conftest import ORDERS, Upstream

from dlt_framework.core import (
    Append,
    Integer,
    Merge,
    Pipeline,
    Replace,
    Resource,
    RowBackfill,
    TimeCursor,
    UnixSeconds,
    ValueCursor,
)
from dlt_framework.core.errors import (
    ConfigurationError,
    DefinitionError,
    GuardError,
    TerminalRunError,
    TransientRunError,
)
from dlt_framework.core.run import BackfillRun, IncrementalRun, PipelineLock


def rows(*pairs: tuple[int, int | None]) -> list[dict]:
    return [{"id": i, "updated_at": u} for i, u in pairs]


def state_rows(query, pipeline_name: str) -> list[tuple]:
    return query(
        "SELECT version, version_hash FROM raw_shop._dlt_pipeline_state "
        f"WHERE pipeline_name = '{pipeline_name}' ORDER BY version"
    )


def snapshot(empty: str = "fail") -> Resource:
    return Resource(name="snapshot", write=Replace(), cursor=None, empty=empty)  # type: ignore[arg-type]


def test_incremental_run_resumes_from_stored_cursor(make_pipeline, query) -> None:
    upstream = Upstream(rows((1, 90), (2, 100), (3, 110)))
    definition = make_pipeline({ORDERS: upstream})

    assert IncrementalRun(definition)()["orders"] == 2
    upstream.rows.append({"id": 4, "updated_at": 120})
    IncrementalRun(definition)()

    assert [start for start, _ in upstream.requests] == [100, 110]
    assert query("SELECT id FROM raw_shop.orders ORDER BY id") == [(2,), (3,), (4,)]


def test_backfill_never_touches_the_production_cursor(make_pipeline, query) -> None:
    upstream = Upstream(rows((1, 10), (2, 60), (3, 100), (4, 110)))
    definition = make_pipeline({ORDERS: upstream})
    IncrementalRun(definition)()
    production_state = state_rows(query, "shop")

    for _ in range(2):  # replaying a merge backfill must not duplicate rows
        BackfillRun(definition)()

    assert state_rows(query, "shop") == production_state
    assert upstream.requests[1:3] == [(0, 50), (50, 100)]  # ends at the cursor start, 100
    assert query("SELECT id FROM raw_shop.orders ORDER BY id") == [(1,), (2,), (3,), (4,)]
    upstream.rows.append({"id": 5, "updated_at": 120})
    IncrementalRun(definition)()
    assert upstream.requests[-1] == (110, None)


def test_every_run_name_has_its_own_staging_dataset(make_pipeline, query) -> None:
    """Pipelines sharing a dataset, and a pipeline's backfill, never share staging tables."""
    customers = Resource(
        name="customers",
        write=Merge(primary_key=("id",), strategy="delete-insert"),
        cursor=None,
        empty="warn",
    )
    shop = make_pipeline({ORDERS: Upstream(rows((1, 10), (2, 100)))})  # 10: backfill history
    crm = make_pipeline({customers: Upstream(rows((1, None)))}, name="crm", source="crm")
    IncrementalRun(shop)()
    IncrementalRun(crm)()
    BackfillRun(shop)()
    schemas = [name for (name,) in query("SELECT schema_name FROM information_schema.schemata")]
    assert len([name for name in schemas if name.startswith("raw_shop_staging")]) == 3


def test_a_table_has_one_owning_pipeline(make_pipeline) -> None:
    make_pipeline({ORDERS: Upstream([])})
    with pytest.raises(DefinitionError):
        make_pipeline({ORDERS: Upstream([])}, name="shop_copy")
    make_pipeline({ORDERS: Upstream([])}, name="shop_eu", dataset="raw_shop_eu")


def test_row_backfill_pages_until_a_short_page(make_pipeline, query) -> None:
    events = Resource(
        name="events", write=Append(), cursor=None, empty="warn", backfill=RowBackfill(size=2)
    )
    upstream = Upstream(rows((1, None), (2, None), (3, None), (4, None), (5, None)))
    BackfillRun(make_pipeline({events: upstream}))()
    assert upstream.requests == [("rows", 0), ("rows", 2), ("rows", 4)]
    assert query("SELECT count(*) FROM raw_shop.events") == [(5,)]


def test_runs_select_resources_and_backfills_take_stated_ranges(make_pipeline) -> None:
    orders, snapshots = Upstream(rows((1, 100))), Upstream([])
    definition = make_pipeline({ORDERS: orders, snapshot(): snapshots})
    assert IncrementalRun(definition, ["orders"])() == {"orders": 1}
    assert snapshots.requests == []
    with pytest.raises(ConfigurationError):
        IncrementalRun(definition, ["nope"])
    with pytest.raises(DefinitionError):
        BackfillRun(definition, ["snapshot"])

    orders.requests.clear()
    BackfillRun(definition, ["orders"], {"orders": [(50, None), (0, 20)]})()
    assert orders.requests == [(50, 100), (0, 20)]


def test_lost_state_guard_blocks_silent_reload(make_pipeline) -> None:
    upstream = Upstream(rows((1, 100)))
    IncrementalRun(make_pipeline({ORDERS: upstream}))()
    Pipeline.registry.clear()  # the code now names the pipeline differently
    renamed = make_pipeline({ORDERS: upstream}, name="shop_renamed")

    with pytest.raises(GuardError):
        IncrementalRun(renamed)()
    IncrementalRun(renamed, allow_initial_load=True)()
    assert upstream.requests[-1] == (100, None)


def test_empty_snapshot_fails_before_loading(make_pipeline, query) -> None:
    upstream = Upstream([{"id": 1, "updated_at": None}])
    definition = make_pipeline({snapshot(): upstream})
    IncrementalRun(definition)()
    upstream.rows.clear()

    with pytest.raises(GuardError):
        IncrementalRun(definition)()
    assert query("SELECT count(*) FROM raw_shop.snapshot") == [(1,)]


def test_fallback_cursor_tracks_rows_with_null_primary(make_pipeline, query) -> None:
    cursor = ValueCursor(
        path="updated_at",
        start=100,
        format=Integer(),
        on_missing="raise",
        fallbacks=("created_at",),
    )
    upstream = Upstream(
        [
            {"id": 1, "updated_at": None, "created_at": 90},
            {"id": 2, "updated_at": None, "created_at": 105},
            {"id": 3, "updated_at": 110, "created_at": 80},
        ]
    )
    merge = Merge(primary_key=("id",), strategy="delete-insert")
    resource = Resource(name="orders", write=merge, cursor=cursor, empty="warn")
    IncrementalRun(make_pipeline({resource: upstream}))()
    assert query("SELECT id, _cursor FROM raw_shop.orders ORDER BY id") == [(2, 105), (3, 110)]


def test_append_and_replace_write_modes(make_pipeline, query) -> None:
    events = Upstream([{"id": 1, "updated_at": None}])
    snapshots = Upstream([{"id": 1, "updated_at": None}])
    appended = Resource(name="events", write=Append(), cursor=None, empty="warn")
    definition = make_pipeline({appended: events, snapshot(): snapshots})
    IncrementalRun(definition)()
    snapshots.rows[:] = [{"id": 2, "updated_at": None}]
    IncrementalRun(definition)()
    assert query("SELECT count(*) FROM raw_shop.events") == [(2,)]
    assert query("SELECT id FROM raw_shop.snapshot") == [(2,)]


def test_same_pipeline_cannot_run_twice_on_one_host(tmp_path) -> None:
    with PipelineLock(str(tmp_path), "shop"), pytest.raises(GuardError):
        PipelineLock(str(tmp_path), "shop").__enter__()
    with PipelineLock(str(tmp_path), "shop"):  # released after the first holder exits
        pass


def stored(name: str) -> object:
    """The committed cursor, as a fresh process would see it after syncing."""
    pipeline = dlt.pipeline(name, destination="duckdb", dataset_name="raw_shop")
    pipeline.sync_destination()
    return pipeline.state["sources"]["shop"]["resources"]["orders"]["incremental"]["updated_at"][
        "last_value"
    ]


def test_a_terminal_failure_after_extract_never_leaves_the_cursor_ahead(
    make_pipeline, query
) -> None:
    """Row 4 (115) arrives with row 3 (120), whose type breaks the frozen contract. Kept, the
    local cursor at 120 would make the next run skip row 4 for good."""
    upstream = Upstream(
        [{"id": 1, "updated_at": 100, "amount": 1}, {"id": 2, "updated_at": 110, "amount": 2}]
    )
    definition = make_pipeline({ORDERS: upstream})
    IncrementalRun(definition)()
    upstream.rows += [
        {"id": 3, "updated_at": 120, "amount": "not a number"},
        {"id": 4, "updated_at": 115, "amount": 4},
    ]
    with pytest.raises(TerminalRunError) as raised:
        IncrementalRun(definition)()
    assert raised.value.step == "normalize"
    assert stored("shop") == 110

    upstream.rows[2]["amount"] = 3  # upstream fixed
    IncrementalRun(definition)()
    assert upstream.requests[-1] == (110, None)
    assert query("SELECT id FROM raw_shop.orders ORDER BY id") == [(1,), (2,), (3,), (4,)]


def test_a_transient_load_failure_keeps_the_package_for_the_next_run(
    make_pipeline, query, monkeypatch
) -> None:
    upstream = Upstream(rows((1, 100)))
    definition = make_pipeline({ORDERS: upstream})
    load = dlt.Pipeline.load
    monkeypatch.setattr(
        dlt.Pipeline,
        "load",
        lambda self, *a, **k: (_ for _ in ()).throw(ConnectionError("destination down")),
    )
    with pytest.raises(TransientRunError):
        IncrementalRun(definition)()
    monkeypatch.setattr(dlt.Pipeline, "load", load)

    IncrementalRun(definition)()  # loads the pending package first, then extracts from 100
    assert query("SELECT id FROM raw_shop.orders") == [(1,)]
    assert stored("shop") == 100


EVENTS_CURSOR = TimeCursor(
    path="updated_at",
    start=datetime(2026, 1, 1, tzinfo=UTC),
    format=UnixSeconds(),
    on_missing="raise",
)


def test_a_future_cursor_value_is_refused_before_it_is_committed(
    make_pipeline, monkeypatch
) -> None:
    now = int(datetime.now(UTC).timestamp())
    events = Resource(name="orders", write=Append(), cursor=EVENTS_CURSOR, empty="warn")
    upstream = Upstream(rows((1, now - 60)))
    definition = make_pipeline({events: upstream})
    IncrementalRun(definition)()

    upstream.rows.append({"id": 2, "updated_at": now + int(timedelta(days=30).total_seconds())})
    with pytest.raises(GuardError) as raised:
        IncrementalRun(definition)()
    assert raised.value.step == "extract"
    assert stored("shop") == now - 60

    monkeypatch.setenv(
        "GUARDS__FUTURE_CURSOR_TOLERANCE_S", str(timedelta(days=365).total_seconds())
    )
    IncrementalRun(definition)()  # a cursor stored before the guard existed
    monkeypatch.delenv("GUARDS__FUTURE_CURSOR_TOLERANCE_S")
    with pytest.raises(GuardError):
        IncrementalRun(definition)()

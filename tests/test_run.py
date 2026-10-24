import pytest
from conftest import ORDERS, Upstream

from dlt_framework.core import Append, Merge, Replace, Resource, RowBackfill, ValueCursor
from dlt_framework.core.errors import ConfigurationError, DefinitionError, GuardError
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


def test_incremental_run_resumes_from_stored_cursor(make_pipeline, query, sink) -> None:
    upstream = Upstream(rows((1, 90), (2, 100), (3, 110)))
    definition = make_pipeline({ORDERS: upstream})

    assert IncrementalRun(definition, sink)()["orders"] == 2
    upstream.rows.append({"id": 4, "updated_at": 120})
    IncrementalRun(definition, sink)()

    assert [start for start, _ in upstream.requests] == [100, 110]
    assert query("SELECT id FROM raw_shop.orders ORDER BY id") == [(2,), (3,), (4,)]


def test_backfill_never_touches_the_production_cursor(make_pipeline, query, sink) -> None:
    upstream = Upstream(rows((1, 10), (2, 60), (3, 100), (4, 110)))
    definition = make_pipeline({ORDERS: upstream})
    IncrementalRun(definition, sink)()
    production_state = state_rows(query, "shop")

    for _ in range(2):  # replaying a merge backfill must not duplicate rows
        BackfillRun(definition, sink)()

    assert state_rows(query, "shop") == production_state
    assert upstream.requests[1:3] == [(0, 50), (50, 100)]  # ends at the cursor start, 100
    assert query("SELECT id FROM raw_shop.orders ORDER BY id") == [(1,), (2,), (3,), (4,)]
    upstream.rows.append({"id": 5, "updated_at": 120})
    IncrementalRun(definition, sink)()
    assert upstream.requests[-1] == (110, None)


def test_backfill_uses_its_own_staging_dataset(make_pipeline, query, sink) -> None:
    BackfillRun(make_pipeline({ORDERS: Upstream(rows((1, 10)))}), sink)()
    schemas = {name for (name,) in query("SELECT schema_name FROM information_schema.schemata")}
    assert "raw_shop_backfill_staging" in schemas
    assert "raw_shop_staging" not in schemas


def test_row_backfill_pages_until_a_short_page(make_pipeline, query, sink) -> None:
    events = Resource(
        name="events", write=Append(), cursor=None, empty="warn", backfill=RowBackfill(size=2)
    )
    upstream = Upstream(rows((1, None), (2, None), (3, None), (4, None), (5, None)))
    BackfillRun(make_pipeline({events: upstream}), sink)()
    assert upstream.requests == [("rows", 0), ("rows", 2), ("rows", 4)]
    assert query("SELECT count(*) FROM raw_shop.events") == [(5,)]


def test_backfill_selection_and_resume(make_pipeline, sink) -> None:
    definition = make_pipeline({ORDERS: Upstream([]), snapshot(): Upstream([])})
    with pytest.raises(DefinitionError, match="no backfill for resource 'snapshot'"):
        BackfillRun(definition, sink, resource="snapshot")
    with pytest.raises(ConfigurationError, match="--resource"):
        BackfillRun(definition, sink, resume="50")
    upstream = Upstream([])
    BackfillRun(make_pipeline({ORDERS: upstream}, name="b"), sink, resource="orders", resume="50")()
    assert upstream.requests == [(50, 100)]


def test_lost_state_guard_blocks_silent_reload(make_pipeline, sink) -> None:
    upstream = Upstream(rows((1, 100)))
    IncrementalRun(make_pipeline({ORDERS: upstream}), sink)()
    renamed = make_pipeline({ORDERS: upstream}, name="shop_renamed")

    with pytest.raises(GuardError, match="No stored cursor"):
        IncrementalRun(renamed, sink)()
    IncrementalRun(renamed, sink, allow_initial_load=True)()
    assert upstream.requests[-1] == (100, None)


def test_empty_snapshot_fails_before_loading(make_pipeline, query, sink) -> None:
    upstream = Upstream([{"id": 1, "updated_at": None}])
    definition = make_pipeline({snapshot(): upstream})
    IncrementalRun(definition, sink)()
    upstream.rows.clear()

    with pytest.raises(GuardError, match="no rows"):
        IncrementalRun(definition, sink)()
    assert query("SELECT count(*) FROM raw_shop.snapshot") == [(1,)]


def test_fallback_cursor_tracks_rows_with_null_primary(make_pipeline, query, sink) -> None:
    cursor = ValueCursor(
        path="updated_at", start=100, on_missing="raise", fallbacks=("created_at",)
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
    IncrementalRun(make_pipeline({resource: upstream}), sink)()
    assert query("SELECT id, _cursor FROM raw_shop.orders ORDER BY id") == [(2, 105), (3, 110)]


def test_append_and_replace_write_modes(make_pipeline, query, sink) -> None:
    events = Upstream([{"id": 1, "updated_at": None}])
    snapshots = Upstream([{"id": 1, "updated_at": None}])
    appended = Resource(name="events", write=Append(), cursor=None, empty="warn")
    definition = make_pipeline({appended: events, snapshot(): snapshots})
    IncrementalRun(definition, sink)()
    snapshots.rows[:] = [{"id": 2, "updated_at": None}]
    IncrementalRun(definition, sink)()
    assert query("SELECT count(*) FROM raw_shop.events") == [(2,)]
    assert query("SELECT id FROM raw_shop.snapshot") == [(2,)]


def test_same_pipeline_cannot_run_twice_on_one_host(tmp_path) -> None:
    with PipelineLock(str(tmp_path), "shop"), pytest.raises(GuardError, match="already running"):
        PipelineLock(str(tmp_path), "shop").__enter__()
    with PipelineLock(str(tmp_path), "shop"):  # released after the first holder exits
        pass


def test_failure_is_notified_and_reraised(make_pipeline, sink) -> None:
    events = []

    class Recorder:
        def send(self, event) -> None:
            events.append(event)

    definition = make_pipeline({snapshot(): Upstream([])})
    object.__setattr__(definition, "notifiers", (Recorder(),))
    with pytest.raises(GuardError):
        IncrementalRun(definition, sink)()
    assert len(events) == 1 and not events[0].succeeded

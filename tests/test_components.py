from datetime import UTC, date, datetime

import pytest

from dlt_framework.core import (
    FREEZE_DATA_TYPES,
    Append,
    AwareDatetime,
    DateOnly,
    Duration,
    IsoTimestamp,
    Merge,
    NaiveDatetime,
    Pipeline,
    Replace,
    Resource,
    RowBackfill,
    Strftime,
    Target,
    TimeBackfill,
    TimeCursor,
    UnixMillis,
    UnixSeconds,
    ValueBackfill,
    ValueCursor,
)
from dlt_framework.core.errors import ConfigurationError, DefinitionError
from dlt_framework.core.windows import RowWindow, TimeWindow, ValueWindow
from dlt_framework.sources.custom import CustomSource
from dlt_framework.sources.filesystem import Files, FilesystemSource
from dlt_framework.sources.rest import RestApiSource
from dlt_framework.sources.sql import SqlSource

UTC = UTC
INSTANT = datetime(2026, 3, 1, 12, 30, tzinfo=UTC)


def time_cursor(start: datetime = datetime(2026, 1, 1, tzinfo=UTC), **extra) -> TimeCursor:
    return TimeCursor(
        path="updated_at", start=start, format=IsoTimestamp(), on_missing="raise", **extra
    )


@pytest.mark.parametrize(
    ("time_format", "rendered"),
    [
        (IsoTimestamp(), "2026-03-01T12:30:00Z"),
        (Strftime("%d/%m/%Y"), "01/03/2026"),
        (UnixSeconds(), 1772368200),
        (UnixMillis(), 1772368200000),
        (DateOnly(), date(2026, 3, 1)),
        (AwareDatetime(), INSTANT),
        (NaiveDatetime(), datetime(2026, 3, 1, 12, 30)),
    ],
)
def test_time_formats(time_format, rendered) -> None:
    assert time_format.render(INSTANT) == rendered


def test_month_durations_stay_anchored_to_start_day() -> None:
    start = datetime(2026, 1, 31, tzinfo=UTC)
    month = Duration(1, "months")
    assert [month.after(start, n).date() for n in (1, 2)] == [date(2026, 2, 28), date(2026, 3, 31)]


def test_time_backfill_ends_at_cursor_start_and_can_resume() -> None:
    cursor = time_cursor(start=datetime(2026, 1, 3, 12, tzinfo=UTC))
    backfill = TimeBackfill(start=datetime(2026, 1, 1, tzinfo=UTC), every=Duration(1, "days"))
    windows = list(backfill.windows(cursor, None))
    assert [(w.start.day, w.end.day, w.end.hour) for w in windows] == [
        (1, 2, 0),
        (2, 3, 0),
        (3, 3, 12),
    ]
    resumed = list(backfill.windows(cursor, "2026-01-02T00:00:00Z"))
    assert resumed == windows[1:]
    with pytest.raises(ConfigurationError):
        list(backfill.windows(cursor, "2026-01-02"))


def test_value_and_row_backfills() -> None:
    cursor = ValueCursor(path="id", start=25, on_missing="raise")
    assert list(ValueBackfill(start=0, every=10).windows(cursor, "10")) == [
        ValueWindow(10, 20),
        ValueWindow(20, 25),
    ]
    rows = RowBackfill(size=100).windows(None, "200")
    assert [next(rows), next(rows)] == [RowWindow(200, 100), RowWindow(300, 100)]
    with pytest.raises(ConfigurationError, match="not an integer"):
        next(ValueBackfill(start=0, every=10).windows(cursor, "2026-01-01"))
    with pytest.raises(ConfigurationError, match="not an integer"):
        next(RowBackfill(size=100).windows(None, "ten"))
    assert RowWindow(0, 100).is_last(99) and not RowWindow(0, 100).is_last(100)


def test_backfill_must_match_cursor_and_start_before_it() -> None:
    every_day = Duration(1, "days")
    with pytest.raises(DefinitionError, match="TimeCursor"):
        TimeBackfill(start=datetime(2025, 1, 1, tzinfo=UTC), every=every_day).check(None)
    with pytest.raises(DefinitionError, match="before cursor start"):
        TimeBackfill(start=datetime(2027, 1, 1, tzinfo=UTC), every=every_day).check(time_cursor())
    with pytest.raises(DefinitionError, match="ValueCursor"):
        ValueBackfill(start=0, every=1).check(time_cursor())


def test_cursors_bound_only_their_own_window_kind() -> None:
    cursor = time_cursor()
    window = TimeWindow(datetime(2025, 1, 1, tzinfo=UTC), datetime(2025, 2, 1, tzinfo=UTC))
    assert cursor.bounds(window) == ("2025-01-01T00:00:00Z", "2025-02-01T00:00:00Z")
    assert cursor.settings(RowWindow(0, 10)) is None
    assert cursor.settings(None)["initial_value"] == "2026-01-01T00:00:00Z"
    with pytest.raises(DefinitionError):
        cursor.bounds(ValueWindow(0, 1))
    with pytest.raises(DefinitionError):
        ValueCursor(path="id", start=0, on_missing="raise").bounds(window)
    with pytest.raises(DefinitionError, match="timezone-aware"):
        time_cursor(start=datetime(2026, 1, 1))


def test_write_modes() -> None:
    assert Append().hints() == {"write_disposition": "append"}
    assert Replace().hints() == {"write_disposition": "replace"}
    assert Merge(primary_key=("id",), strategy="upsert").hints() == {
        "write_disposition": {"disposition": "merge", "strategy": "upsert"},
        "primary_key": ["id"],
    }
    with pytest.raises(DefinitionError):
        Merge(primary_key=(), strategy="upsert")


@pytest.mark.parametrize("name", ["Orders", "1orders", "order-items", ""])
def test_names_must_be_snake_case(name) -> None:
    with pytest.raises(DefinitionError):
        Resource(name=name, write=Append(), cursor=None, empty="warn")


def test_pipeline_names_are_unique_and_cannot_mimic_backfills() -> None:
    def define(name: str) -> Pipeline:
        return Pipeline(
            name=name,
            source=CustomSource(name="s", resources={}),
            target=Target("duckdb", dataset="raw"),
            schema_contract=FREEZE_DATA_TYPES,
        )

    define("shop")
    with pytest.raises(DefinitionError, match="defined twice"):
        define("shop")
    with pytest.raises(DefinitionError, match="__"):
        define("shop__backfill")
    with pytest.raises(DefinitionError, match="No pipeline named"):
        Pipeline.get("missing")


def test_duckdb_file_cannot_share_dataset_name(monkeypatch) -> None:
    monkeypatch.setenv("DESTINATION__DUCKDB__CREDENTIALS", "/data/raw.duckdb")
    with pytest.raises(ConfigurationError):
        Target("duckdb", dataset="raw").factory("%s_staging")


ROWS = Resource(name="t", write=Append(), cursor=None, empty="warn", backfill=RowBackfill(size=10))


@pytest.mark.parametrize(
    ("build", "message"),
    [
        (lambda: SqlSource(name="erp", resources={ROWS: {}}), "order_by"),
        (
            lambda: RestApiSource(name="api", client={"base_url": "x"}, resources={ROWS: {}}),
            "offset",
        ),
        (
            lambda: FilesystemSource(
                name="files", resources={ROWS: Files(bucket_url="x", file_glob="*", reader="csv")}
            ),
            "cannot push RowWindow",
        ),
        (
            lambda: SqlSource(
                name="erp",
                resources={
                    Resource(
                        name="t",
                        write=Append(),
                        cursor=time_cursor(fallbacks=("created_at",)),
                        empty="warn",
                    ): {}
                },
            ),
            "fallback",
        ),
        (
            lambda: FilesystemSource(
                name="files",
                resources={
                    Resource(name="t", write=Append(), cursor=time_cursor(), empty="warn"): Files(
                        bucket_url="x", file_glob="*", reader="csv"
                    )
                },
            ),
            "modification_date",
        ),
    ],
)
def test_each_source_type_validates_its_own_rules(build, message) -> None:
    with pytest.raises(DefinitionError, match=message):
        build()

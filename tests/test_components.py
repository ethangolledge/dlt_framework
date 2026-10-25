from datetime import UTC, date, datetime

import pytest

from dlt_framework.core import (
    FREEZE_DATA_TYPES,
    Append,
    AwareDatetime,
    DateOnly,
    DuckDb,
    Duration,
    Integer,
    IsoTimestamp,
    Merge,
    NaiveDatetime,
    Pipeline,
    Replace,
    RowBackfill,
    RowVersion,
    Strftime,
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

UTC = UTC
INSTANT = datetime(2026, 3, 1, 12, 30, tzinfo=UTC)


def time_cursor(start: datetime = datetime(2026, 1, 1, tzinfo=UTC), **extra) -> TimeCursor:
    return TimeCursor(
        path="updated_at", start=start, format=IsoTimestamp(), on_missing="raise", **extra
    )


@pytest.mark.parametrize(
    ("cursor_format", "value", "stored"),
    [
        (IsoTimestamp(), INSTANT, "2026-03-01T12:30:00Z"),
        (Strftime("%d/%m/%Y %H:%M"), INSTANT, "01/03/2026 12:30"),
        (UnixSeconds(), INSTANT, 1772368200),
        (UnixMillis(), INSTANT, 1772368200000),
        (DateOnly(), datetime(2026, 3, 1, tzinfo=UTC), date(2026, 3, 1)),
        (AwareDatetime(), INSTANT, INSTANT),
        (NaiveDatetime(), INSTANT, datetime(2026, 3, 1, 12, 30)),
        (Integer(), 1005, 1005),
        (RowVersion(), 1005, b"\x00\x00\x00\x00\x00\x00\x03\xed"),
    ],
)
def test_cursor_formats_render_and_parse_back(cursor_format, value, stored) -> None:
    """parse(render(x)) == x is what lets guards, monitoring, and coverage read stored state."""
    assert cursor_format.render(value) == stored
    assert cursor_format.parse(stored) == value


def test_month_durations_stay_anchored_to_start_day() -> None:
    start = datetime(2026, 1, 31, tzinfo=UTC)
    month = Duration(1, "months")
    assert [month.after(start, n).date() for n in (1, 2)] == [date(2026, 2, 28), date(2026, 3, 31)]


def test_time_backfill_ends_at_cursor_start_and_narrows_to_stated_edges() -> None:
    cursor = time_cursor(start=datetime(2026, 1, 3, 12, tzinfo=UTC))
    backfill = TimeBackfill(start=datetime(2026, 1, 1, tzinfo=UTC), every=Duration(1, "days"))
    windows = list(backfill.windows(cursor))
    assert [(w.start.day, w.end.day, w.end.hour) for w in windows] == [
        (1, 2, 0),
        (2, 3, 0),
        (3, 3, 12),
    ]
    resumed = backfill.edge("2026-01-02T00:00:00Z")
    assert list(backfill.windows(cursor, resumed)) == windows[1:]
    assert list(backfill.windows(cursor, end=backfill.edge("2026-01-02T06:00:00Z"))) == [
        windows[0],
        TimeWindow(datetime(2026, 1, 2, tzinfo=UTC), datetime(2026, 1, 2, 6, tzinfo=UTC)),
    ]
    with pytest.raises(ConfigurationError):
        backfill.edge("2026-01-02")


def test_value_and_row_backfills() -> None:
    cursor = ValueCursor(path="id", start=25, format=Integer(), on_missing="raise")
    values = ValueBackfill(start=0, every=10)
    assert list(values.windows(cursor, 10)) == [ValueWindow(10, 20), ValueWindow(20, 25)]
    assert list(values.windows(cursor, 0, 15)) == [ValueWindow(0, 10), ValueWindow(10, 15)]
    rows = RowBackfill(size=100).windows(None, 200)
    assert [next(rows), next(rows)] == [RowWindow(200, 100), RowWindow(300, 100)]
    assert list(RowBackfill(size=100).windows(None, 0, 150)) == [
        RowWindow(0, 100),
        RowWindow(100, 50),
    ]
    with pytest.raises(ConfigurationError):
        values.edge("2026-01-01")
    with pytest.raises(ConfigurationError):
        RowBackfill(size=100).edge("ten")
    assert RowWindow(0, 100).is_last(99) and not RowWindow(0, 100).is_last(100)


def test_backfill_must_match_cursor_and_start_before_it() -> None:
    every_day = Duration(1, "days")
    with pytest.raises(DefinitionError):
        TimeBackfill(start=datetime(2025, 1, 1, tzinfo=UTC), every=every_day).check(None)
    with pytest.raises(DefinitionError):
        TimeBackfill(start=datetime(2027, 1, 1, tzinfo=UTC), every=every_day).check(time_cursor())
    with pytest.raises(DefinitionError):
        ValueBackfill(start=0, every=1).check(time_cursor())


@pytest.mark.parametrize(
    "define",
    [
        lambda: RowBackfill(size=0),
        lambda: ValueBackfill(start=0, every=-5),
        lambda: Duration(0, "days"),
        lambda: TimeBackfill(start=datetime(2025, 1, 1), every=Duration(1, "days")),
    ],
)
def test_a_backfill_that_could_never_advance_is_refused(define) -> None:
    with pytest.raises(DefinitionError):
        define()


@pytest.mark.parametrize(
    ("window", "text"),
    [
        (
            TimeWindow(datetime(2025, 1, 1, tzinfo=UTC), datetime(2025, 2, 1, tzinfo=UTC)),
            ("2025-01-01T00:00:00+00:00", "2025-02-01T00:00:00+00:00"),
        ),
        (ValueWindow(5, 10), ("5", "10")),
        (RowWindow(100, 50), ("100", "150")),
    ],
)
def test_window_edges_round_trip_through_text(window, text) -> None:
    """Spans written to logs and monitoring parse back to the same window."""
    assert window.span() == text
    assert type(window).between(*map(type(window).parse_edge, text)) == window


def test_cursors_bound_only_their_own_window_kind() -> None:
    cursor = time_cursor()
    window = TimeWindow(datetime(2025, 1, 1, tzinfo=UTC), datetime(2025, 2, 1, tzinfo=UTC))
    assert cursor.bounds(window) == ("2025-01-01T00:00:00Z", "2025-02-01T00:00:00Z")
    assert cursor.settings(RowWindow(0, 10)) is None
    assert cursor.settings(None)["initial_value"] == "2026-01-01T00:00:00Z"
    with pytest.raises(DefinitionError):
        cursor.bounds(ValueWindow(0, 1))
    with pytest.raises(DefinitionError):
        ValueCursor(path="id", start=0, format=Integer(), on_missing="raise").bounds(window)
    with pytest.raises(DefinitionError):
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


def test_pipeline_names_are_unique_and_cannot_mimic_backfills() -> None:
    def define(name: str) -> Pipeline:
        return Pipeline(
            name=name,
            source=CustomSource(name="s", resources={}),
            target=DuckDb(dataset="raw"),
            schema_contract=FREEZE_DATA_TYPES,
        )

    define("shop")
    with pytest.raises(DefinitionError):
        define("shop")
    with pytest.raises(DefinitionError):
        define("shop__backfill")
    with pytest.raises(DefinitionError):
        Pipeline.get("missing")


def test_rowversions_sort_like_their_integers() -> None:
    rendered = [RowVersion().render(value) for value in (1, 255, 256, 2**40)]
    assert rendered == sorted(rendered)

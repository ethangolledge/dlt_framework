from datetime import datetime, timezone

import pytest

from dlt_framework.core.backfill import (
    BackfillError,
    BackfillWindow,
    ChunkSize,
    incremental_for,
    parse_backfill_plan,
    parse_bound,
    parse_chunk_size,
    range_kwargs,
    rest_incremental_config,
)
from dlt_framework.core.models import SourceContext


def test_parses_dates_offsets_and_chunk_sizes() -> None:
    assert parse_bound("2026-01-01", "--from") == datetime(2026, 1, 1, tzinfo=timezone.utc)
    assert parse_bound("2026-01-01T02:00:00+02:00", "--from") == datetime(
        2026, 1, 1, tzinfo=timezone.utc
    )
    assert parse_chunk_size("PT30S") == ChunkSize(30, "seconds")
    assert parse_chunk_size("PT30M") == ChunkSize(30, "minutes")
    assert parse_chunk_size("PT6H") == ChunkSize(6, "hours")
    assert parse_chunk_size("P2D") == ChunkSize(2, "days")
    assert parse_chunk_size("P3W") == ChunkSize(3, "weeks")
    assert parse_chunk_size("P4M") == ChunkSize(4, "months")
    assert parse_chunk_size("P1Y") == ChunkSize(1, "years")


@pytest.mark.parametrize(
    "value",
    ["", "P0D", "1d", "P1H", "PT1D", "P1.5D", "P1DT2H", "p1d"],
)
def test_rejects_invalid_chunk_sizes(value: str) -> None:
    with pytest.raises(BackfillError, match="chunksize"):
        parse_chunk_size(value)


def test_rejects_naive_datetime() -> None:
    with pytest.raises(BackfillError, match="timezone"):
        parse_bound("2026-01-01T12:00:00", "--from")


def test_generates_half_open_windows_with_short_final_chunk() -> None:
    plan = parse_backfill_plan(
        resource="events",
        from_value="2026-01-01",
        to_value="2026-01-03T06:00:00Z",
        chunk_size="P1D",
    )

    assert [(window.start, window.end) for window in plan.windows("events")] == [
        (
            datetime(2026, 1, 1, tzinfo=timezone.utc),
            datetime(2026, 1, 2, tzinfo=timezone.utc),
        ),
        (
            datetime(2026, 1, 2, tzinfo=timezone.utc),
            datetime(2026, 1, 3, tzinfo=timezone.utc),
        ),
        (
            datetime(2026, 1, 3, tzinfo=timezone.utc),
            datetime(2026, 1, 3, 6, tzinfo=timezone.utc),
        ),
    ]


def test_calendar_chunks_are_anchored_to_the_original_start() -> None:
    plan = parse_backfill_plan(
        resource="events",
        from_value="2025-01-31",
        to_value="2025-04-01",
        chunk_size="P1M",
    )

    assert [(window.start.day, window.end.day) for window in plan.windows("events")] == [
        (31, 28),
        (28, 31),
        (31, 1),
    ]
    assert ChunkSize(1, "years").boundary(
        datetime(2024, 2, 29, tzinfo=timezone.utc),
        4,
    ) == datetime(2028, 2, 29, tzinfo=timezone.utc)


@pytest.mark.parametrize(
    ("resource", "from_value", "to_value", "chunk_size", "message"),
    [
        (None, "2026-01-01", "2026-01-02", "P1D", "resource"),
        ("events", "2026-01-01", None, "P1D", "supplied together"),
        ("events", "2026-01-02", "2026-01-01", "P1D", "earlier"),
    ],
)
def test_rejects_invalid_backfill_plans(
    resource, from_value, to_value, chunk_size, message
) -> None:
    with pytest.raises(BackfillError, match=message):
        parse_backfill_plan(
            resource=resource,
            from_value=from_value,
            to_value=to_value,
            chunk_size=chunk_size,
        )


def test_connector_helpers_bind_only_the_selected_resource() -> None:
    window = BackfillWindow(
        resource="events",
        start=datetime(2026, 1, 1, tzinfo=timezone.utc),
        end=datetime(2026, 1, 2, tzinfo=timezone.utc),
    )

    context = SourceContext(selected_resource="events", window=window)
    assert (
        range_kwargs(
            context,
            resource_name="snapshot",
            start_argument="start",
            end_argument="end",
        )
        == {}
    )
    assert range_kwargs(
        context,
        resource_name="events",
        start_argument="start_date",
        end_argument="end_date",
        value_format="date",
    ) == {"start_date": "2026-01-01", "end_date": "2026-01-02"}
    assert context.backfill_bound


def test_builds_native_and_rest_incrementals() -> None:
    native_window = BackfillWindow(
        resource="events",
        start=datetime(2026, 1, 1, tzinfo=timezone.utc),
        end=datetime(2026, 1, 2, tzinfo=timezone.utc),
    )
    native_context = SourceContext(selected_resource="events", window=native_window)
    incremental = incremental_for(
        native_context,
        resource_name="events",
        cursor_path="updated_at",
    )

    assert incremental.cursor_path == "updated_at"
    assert incremental.initial_value == native_window.start
    assert incremental.end_value == native_window.end
    assert incremental.range_start == "closed"
    assert incremental.range_end == "open"

    rest_window = BackfillWindow(
        resource="events",
        start=native_window.start,
        end=native_window.end,
    )
    rest_context = SourceContext(selected_resource="events", window=rest_window)
    config = rest_incremental_config(
        rest_context,
        resource_name="events",
        cursor_path="updated_at",
        start_param="updated_from",
        end_param="updated_to",
    )

    assert config["cursor_path"] == "updated_at"
    assert config["initial_value"] == "2026-01-01T00:00:00Z"
    assert config["end_value"] == "2026-01-02T00:00:00Z"
    assert config["start_param"] == "updated_from"
    assert config["end_param"] == "updated_to"
    assert rest_context.backfill_bound


def test_rejects_excessive_chunks_and_misaligned_resume() -> None:
    plan = parse_backfill_plan(
        resource="events",
        from_value="2026-01-01",
        to_value="2026-01-05",
        chunk_size="P1D",
    )

    with pytest.raises(BackfillError, match="safety limit"):
        plan.count(max_chunks=3)
    with pytest.raises(BackfillError, match="not aligned"):
        list(
            plan.windows(
                "events",
                start_at=datetime(2026, 1, 2, 12, tzinfo=timezone.utc),
            )
        )

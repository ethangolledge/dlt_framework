"""``pipeline health`` and the alert fallback: each signal from a history whose answer is known."""

import json
import logging
from dataclasses import replace
from datetime import UTC, datetime, timedelta

import polars as pl
import pytest
from conftest import HEALTH, ORDERS, Upstream, declared

from dlt_framework.cli import main
from dlt_framework.core.alerts import AlertSettings, attach
from dlt_framework.core.logs import _Bound
from dlt_framework.core.monitoring import Frames, LoadRecord, RunRecord, StepRecord
from dlt_framework.core.monitoring.health import health
from dlt_framework.core.run import IncrementalRun
from dlt_framework.core.settings import HealthSettings

NOW = datetime(2026, 10, 27, 12, tzinfo=UTC)
HOUR = timedelta(hours=1)


def history(*events: tuple[str, dict], every: timedelta = HOUR) -> pl.DataFrame:
    """Ended runs of ``shop``, one ``every`` apart, the last ending now."""
    start = NOW - every * (len(events) - 1)
    records = [
        RunRecord(
            run_id=f"r{index}",
            pipeline_id="shop",
            dlt_pipeline="shop",
            mode="run",
            event=event,
            at=start + every * index,
            host="h",
            dlt_version="1",
            code_version="t",
            python="3",
            cpu_count=1,
            host_memory_mb=16_000.0,
            **{"duration_s": 10.0, "peak_rss_mb": 100.0, **details},
        )
        for index, (event, details) in enumerate(events)
    ]
    return RunRecord.frame(records)


def ok(**details) -> tuple[str, dict]:
    return "succeeded", details


def increments(rows: list[int], resource: str = "orders") -> pl.DataFrame:
    """One increment of ``rows[i]`` rows in run ``r{i}``."""
    return LoadRecord.frame(
        [
            LoadRecord(
                load_id=str(index),
                resource_id=f"shop.{resource}",
                run_id=f"r{index}",
                at=NOW,
                window_kind="increment",
                window_start=None,
                window_end=None,
                part=0,
                rows=count,
                child_rows=0,
                bytes=0,
                extract_first_at=None,
                extract_last_at=None,
                cursor_start_value=None,
                cursor_last_value=None,
            )
            for index, count in enumerate(rows)
        ]
    )


def retried(*runs: int) -> pl.DataFrame:
    """A second extract attempt in each of ``runs``."""
    return StepRecord.frame(
        [
            StepRecord(
                run_id=f"r{run}",
                load_id=None,
                step="extract",
                attempt=2,
                resources="orders",
                at=NOW,
                started_at=NOW,
                duration_s=1.0,
                cpu_s=0.0,
                outcome="ok",
            )
            for run in runs
        ]
    )


INCREMENTAL = replace(ORDERS, backfill=None)  # declares no history, so has no gaps


@pytest.fixture
def shop(make_pipeline):
    return make_pipeline({INCREMENTAL: Upstream([])})


def signals(definition, runs, loads=None, steps=None, reconciled=None, settings=HEALTH, now=NOW):
    frames = Frames(runs=runs, resources=declared(definition))
    frames = replace(
        frames,
        **{
            name: frame
            for name, frame in (("loads", loads), ("steps", steps), ("reconciliations", reconciled))
            if frame is not None
        },
    )
    found = health(definition.name, settings, frames, now=now)
    return [(signal.event, signal.level, signal.resource, signal.detail) for signal in found]


def test_a_steady_pipeline_is_healthy(shop) -> None:
    runs = history(*[ok()] * 8)
    loads = pl.concat([increments([100] * 8)])
    assert signals(shop, runs, loads) == []


def test_a_failing_pipeline_says_what_failed_and_for_how_long(shop) -> None:
    failure = {"error_type": "TerminalRunError", "error_resource": "orders"}
    failure["error_message"] = "HTTPError: 401\nmore detail"
    runs = history(ok(), ("failed", failure), ("failed", failure))
    assert signals(shop, runs) == [
        (
            "run_failed",
            "error",
            None,
            "TerminalRunError in orders (2 runs in a row): HTTPError: 401",
        )
    ]


def test_a_killed_run_is_reported(shop) -> None:
    runs = history(ok(), ok(), ("lost", {"error_step": "load", "peak_rss_mb": 3900.0}))
    assert signals(shop, runs) == [
        ("run_lost", "error", None, "killed during load, peak 3900 MB; no end was recorded")
    ]


def test_a_pipeline_is_late_against_its_own_schedule(shop) -> None:
    assert signals(shop, history(*[ok()] * 5), now=NOW + 3 * HOUR) == []  # schedule unknown
    runs = history(*[ok()] * 6)  # hourly, the last now
    assert signals(shop, runs, now=NOW + 2 * HOUR) == []  # within two intervals
    assert signals(shop, runs, now=NOW + 3 * HOUR) == [
        ("run_stale", "error", None, "last success 2026-10-27 12:00Z; expected every 1:00:00")
    ]
    daily = HealthSettings(**{**HEALTH, "expect_every_s": 86400})
    assert signals(shop, runs, settings=daily, now=NOW + 3 * HOUR) == []  # a stated schedule


def test_runs_by_hand_set_no_schedule_to_be_late_against(shop) -> None:
    # a session of hand runs: seconds to minutes apart, then nothing for days
    offsets = [0, 848, 877, 912, 927, 943, 982, 1016]
    runs = history(*[ok()] * len(offsets)).with_columns(
        at=pl.Series([NOW + timedelta(seconds=offset) for offset in offsets])
    )
    assert signals(shop, runs, now=NOW + timedelta(days=2)) == []
    stated = HealthSettings(**{**HEALTH, "expect_every_s": 86400})
    assert signals(shop, runs, settings=stated, now=NOW + timedelta(days=3))[0][0] == "run_stale"


def test_a_slow_run_is_slow_for_this_pipeline(shop) -> None:
    runs = history(*[ok()] * 6, ok(duration_s=16.0))
    assert signals(shop, runs) == [
        ("run_slow", "warning", None, "took 16s; usually under 10s (p95)")
    ]
    assert signals(shop, history(*[ok()] * 6, ok(duration_s=14.0))) == []
    unmeasured = history(*[ok(duration_s=None)] * 6, ok(duration_s=16.0))  # older records
    assert signals(shop, unmeasured) == []


def test_memory_close_to_the_limit_warns(shop) -> None:
    runs = history(ok(), ok(peak_rss_mb=850.0, memory_limit_mb=1000.0))
    assert signals(shop, runs) == [
        ("memory_high", "warning", None, "peak memory 85% of what was available")
    ]
    host = history(ok(), ok(peak_rss_mb=14_000.0))  # no container: the host's memory
    assert [signal[0] for signal in signals(shop, host)] == ["memory_high"]


def test_retries_in_most_recent_runs_are_a_trend(shop) -> None:
    runs = history(*[ok()] * 5)
    assert signals(shop, runs, steps=retried(2, 4)) == []
    assert signals(shop, runs, steps=retried(1, 2, 4)) == [
        (
            "retries_rising",
            "warning",
            None,
            "3 of the last 5 runs retried a step, most often extract",
        )
    ]


@pytest.mark.parametrize(
    ("rows", "empty", "found"),
    [
        ([100] * 6 + [0], "warn", "no rows; usually 100"),
        ([100] * 6 + [0], "allow", None),
        ([100] * 6 + [5000], "warn", "5000 rows; usually 100"),
        ([100] * 6 + [900], "warn", None),
    ],
)
def test_unusual_rows_against_the_resource_history(make_pipeline, rows, empty, found) -> None:
    resource = replace(INCREMENTAL, empty=empty)
    definition = make_pipeline({resource: Upstream([])})
    runs = history(*[ok()] * len(rows))
    expected = [("rows_anomaly", "warning", "orders", found)] if found else []
    assert signals(definition, runs, increments(rows)) == expected


def test_backfill_gaps_are_reported(make_pipeline) -> None:
    definition = make_pipeline({ORDERS: Upstream([])})  # history [0, 100) in steps of 50
    loads = LoadRecord.frame(
        [
            LoadRecord(
                **{
                    **increments([1]).row(0, named=True),
                    "window_kind": "value",
                    "window_start": "50",
                    "window_end": "100",
                }
            )
        ]
    )
    assert signals(definition, history(ok()), loads) == [
        ("coverage_gap", "warning", "orders", "1 gap(s), first [0, 50)")
    ]
    assert signals(definition, history(ok())) == [  # never backfilled: all of it is missing
        ("coverage_gap", "warning", "orders", "1 gap(s), first [0, 100)")
    ]


def test_cli_health_logs_each_signal_and_exits_6(make_pipeline, monkeypatch, caplog) -> None:
    def broken(incremental, window):
        raise ValueError("upstream exploded")
        yield

    monkeypatch.setattr("dlt_framework.core.pipeline.Pipeline.discover", lambda package: None)
    definition = make_pipeline({INCREMENTAL: broken})
    with pytest.raises(Exception):  # noqa: B017 - the failure itself is tested elsewhere
        IncrementalRun(definition)()
    assert main(["health", "shop"]) == 6
    (logged,) = [r for r in caplog.records if getattr(r, "event", None) == "run_failed"]
    assert logged.levelno == logging.ERROR and logged.pipeline == "shop"
    assert "TerminalRunError in orders: upstream exploded" in logged.getMessage()


# ---- the webhook fallback ----------------------------------------------------------------


def alerting(site, **stated) -> AlertSettings:
    settings = {
        "webhook_url": f"{site.url}/hook",
        "body": '{"text": "$level $pipeline $event: $message"}',
        "events": ["run_failed", "run_lost"],
        "level": "WARNING",
        "cooldown_s": 3600,
        "timeout_s": 5,
    }
    return AlertSettings(**{**settings, **stated})


def alerted(site, settings, *lines: tuple[int, str, dict]) -> list[dict]:
    """Log ``lines`` with alerts attached, drain the queue, and return the bodies posted."""
    site.pages["/hook"] = (200, "{}")
    root = logging.getLogger()
    before = list(root.handlers)
    listener = attach(settings, _Bound())
    try:
        for level, message, extra in lines:
            logging.getLogger("dlt_framework.test").log(level, message, extra=extra)
    finally:
        assert listener is not None
        listener.stop()  # posts everything queued before returning
        root.handlers = before
    return [json.loads(body) for method, path, body in site.requests if path == "/hook"]


def test_alert_events_post_the_template_once_per_cooldown(site) -> None:
    failed = {"event": "run_failed", "pipeline": "shop"}
    posted = alerted(
        site,
        alerting(site),
        (logging.ERROR, 'shop failed: 401 for https://x.io/o?api_key=hunter2 "quoted"', failed),
        (logging.ERROR, "shop failed again", failed),  # within the cooldown
        (logging.ERROR, "eu failed", {"event": "run_failed", "pipeline": "eu"}),
        (logging.WARNING, "a retry", {"event": "retry", "pipeline": "shop"}),  # not an alert
        (logging.WARNING, "no event at all", {}),
    )
    assert posted == [
        {"text": 'ERROR shop run_failed: shop failed: 401 for https://x.io/o?api_key=*** "quoted"'},
        {"text": "ERROR eu run_failed: eu failed"},
    ]


def test_alerts_are_off_without_a_url(site) -> None:
    assert attach(alerting(site, webhook_url=None), _Bound()) is None


def test_a_failing_webhook_never_fails_the_run_or_prints_its_url(site, capsys) -> None:
    settings = alerting(site, webhook_url="http://127.0.0.1:9/bot-secret-token/send")
    alerted(site, settings, (logging.ERROR, "shop failed", {"event": "run_failed"}))
    err = capsys.readouterr().err
    assert "alert webhook failed" in err and "secret-token" not in err

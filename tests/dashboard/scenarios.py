"""Frames shaped like real monitoring data, each scenario built for the behaviour it tests.

Not a demo week with every pattern planted: real frames are sparse (older records hold no
steps, tables, or memory limits), bursty (runs by hand, minutes apart, then nothing for days),
and their trouble is mixed (a streak of different causes). Each builder here makes the least
that shows one thing, and the tests say what the pages must make of it.
"""

from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from itertools import count
from typing import Any

from dlt_framework.core.monitoring import (
    Frames,
    LoadRecord,
    PipelineRecord,
    ReconcileRecord,
    ResourceRecord,
    RunRecord,
    SampleRecord,
    StepRecord,
    TableRecord,
)
from dlt_framework.core.monitoring.records import Record

NOW = datetime(2026, 10, 28, 12, tzinfo=UTC)
_IDS = count()


@dataclass
class Frames_:
    """Records collected, then turned into frames."""

    records: list[Record] = field(default_factory=list)

    def add(self, *records: Record) -> "Frames_":
        self.records.extend(records)
        return self

    def frames(self) -> Frames:
        by: dict[type[Record], list[Record]] = {}
        for record in self.records:
            by.setdefault(type(record), []).append(record)
        return Frames(**{kind.name: kind.frame(rows) for kind, rows in by.items()})


def run(
    pipeline: str,
    start: datetime,
    outcome: str | None = "succeeded",
    seconds: float = 10.0,
    *,
    mode: str = "run",
    run_id: str | None = None,
    **details: Any,
) -> list[RunRecord]:
    """A run's records: started, and its end unless ``outcome`` is ``None`` (killed, or still
    running). Identity as older records hold it: no host memory, no code version."""
    run_id = run_id or f"{pipeline[:4]}{next(_IDS):04d}"
    identity = {
        "run_id": run_id,
        "pipeline_id": pipeline,
        "dlt_pipeline": pipeline if mode == "run" else f"{pipeline}__backfill",
        "mode": mode,
        "host": "h",
        "dlt_version": "1.0",
        "code_version": details.pop("code_version", None),
        "python": "3.14",
        "cpu_count": 4,
        "host_memory_mb": details.pop("host_memory_mb", None),
    }
    records = [RunRecord(event="started", at=start, **identity)]
    if outcome:
        end = start + timedelta(seconds=seconds)
        records.append(RunRecord(event=outcome, at=end, duration_s=seconds, **identity, **details))
    return records


def failure(cause: str, message: str, step: str = "extract", **more: Any) -> dict[str, Any]:
    return {
        "error_type": "TerminalRunError",
        "error_cause_type": cause,
        "error_message": message,
        "error_step": step,
        "rows": 0,
        **more,
    }


def sample(
    run_id: str, at: datetime, rss: float, step: str | None = None, **more: Any
) -> SampleRecord:
    return SampleRecord(
        run_id=run_id,
        at=at,
        step=step,
        resources=None,
        rss_mb=rss,
        cpu_percent=50.0,
        threads=4,
        read_mb_s=None,
        write_mb_s=None,
        memory_limit_mb=more.get("limit"),
        system_available_mb=more.get("available"),
        system_total_mb=more.get("total"),
    )


def load(
    resource_id: str,
    run_id: str,
    at: datetime,
    rows: int,
    *,
    kind: str = "increment",
    start: str | None = None,
    end: str | None = None,
    load_id: str | None = None,
    **more: Any,
) -> LoadRecord:
    return LoadRecord(
        load_id=load_id or f"l{next(_IDS)}",
        resource_id=resource_id,
        run_id=run_id,
        at=at,
        window_kind=kind,
        window_start=start,
        window_end=end,
        part=0,
        rows=rows,
        child_rows=0,
        bytes=0,
        extract_first_at=more.get("first"),
        extract_last_at=more.get("last"),
        cursor_start_value=more.get("cursor_from"),
        cursor_last_value=more.get("cursor_to"),
    )


def resource(
    resource_id: str,
    run_id: str,
    at: datetime,
    *,
    history: tuple[datetime, datetime] | None = None,
    cursor: str | None = "TimeCursor",
    write: str = "Merge",
) -> ResourceRecord:
    pipeline, name = resource_id.split(".", 1)
    return ResourceRecord(
        resource_id=resource_id,
        pipeline_id=pipeline,
        run_id=run_id,
        at=at,
        resource=name,
        write_mode=write,
        cursor_kind=cursor,
        cursor_path="updated_at" if cursor else None,
        cursor_start=None,
        backfill_kind="TimeBackfill" if history else None,
        backfill_window="time" if history else None,
        backfill_start=history[0].isoformat() if history else None,
        backfill_end=history[1].isoformat() if history else None,
        empty="warn",
        definition=f"Resource({name!r})",
    )


def hand_runs() -> Frames:
    """copy_trades as it is: older records, runs by hand in one evening, two days ago.

    ``form4`` broke four times running, each differently: killed, config missing twice,
    then stopped by a signal, and has not succeeded since. ``insider`` broke five times (three
    killed at gigabytes, with no limit recorded) and then recovered. ``members`` failed once
    and recovered within a minute; ``filings`` never broke.
    """
    evening = NOW - timedelta(days=2, hours=17)
    built = Frames_()
    killed = run("form4", evening, None, run_id="form4-killed")
    built.add(*killed, sample("form4-killed", evening + timedelta(minutes=5), 278.0))
    built.add(
        *run(
            "form4",
            evening + timedelta(minutes=7),
            "failed",
            0.2,
            **failure("ConfigFieldMissingException", "Missing 1 field(s) in configuration"),
        )
    )
    built.add(
        *run(
            "form4",
            evening + timedelta(minutes=8),
            "failed",
            0.2,
            **failure("ConfigFieldMissingException", "Missing 1 field(s) in configuration"),
        )
    )
    built.add(
        *run(
            "form4",
            evening + timedelta(minutes=30),
            "failed",
            634.0,
            run_id="form4-signal",
            **failure(
                "SignalReceivedException",
                "Received `signal_code=15`",
                step=None,
                error_type="SignalReceivedException",
            ),
        )
    )
    at = evening + timedelta(hours=1)
    built.add(
        *run("insider", at, "failed", 402.0, **failure("InvalidInputException", "read_csv failed"))
    )
    for minutes, peak in ((12, 3142.0), (16, 4533.0), (38, 5195.0)):
        run_id = f"insider-killed-{minutes}"
        built.add(*run("insider", at + timedelta(minutes=minutes), None, run_id=run_id))
        built.add(sample(run_id, at + timedelta(minutes=minutes, seconds=60), peak))
    built.add(
        *run(
            "insider",
            at + timedelta(minutes=14),
            "failed",
            15.0,
            **failure("OSError", "CSV Error on Line: 53542"),
        )
    )
    for minutes in (41, 55, 56, 57, 58):
        run_id = f"insider-ok-{minutes}"
        built.add(
            *run(
                "insider",
                at + timedelta(minutes=minutes),
                "succeeded",
                20.0,
                run_id=run_id,
                rows=1_000_000,
                peak_rss_mb=1100.0,
            )
        )
        cursor = (at + timedelta(minutes=minutes)).isoformat()
        built.add(
            load(
                "insider.submission",
                run_id,
                at + timedelta(minutes=minutes),
                1_000_000,
                cursor_to=cursor,
            )
        )
    built.add(resource("insider.submission", "insider-ok-58", at + timedelta(minutes=58)))
    built.add(
        *run(
            "members",
            evening + timedelta(minutes=10),
            "failed",
            2.4,
            **failure("UnboundColumnException", "columns did not bind", step="normalize"),
        )
    )
    for minutes in (11, 12, 30):
        built.add(
            *run(
                "members",
                evening + timedelta(minutes=minutes),
                "succeeded",
                3.5,
                rows=468,
                peak_rss_mb=320.0,
            )
        )
    for minutes in (0, 5, 35):
        built.add(
            *run(
                "filings",
                evening + timedelta(minutes=minutes),
                "succeeded",
                7.0,
                rows=4764,
                peak_rss_mb=350.0,
            )
        )
    return built.frames()


def scheduled(days: int = 3) -> Frames:
    """``orders`` every hour, recorded in full: steps, samples by step, tables, a memory limit,
    and code versions. Release 1.1 makes it twice as slow; it runs at 90% of its 2 GB limit;
    extract mostly waits; one run was killed at the limit and the next succeeded."""
    built = Frames_()
    first = NOW - timedelta(days=days)
    committed = first - timedelta(hours=1)  # where the cursor stands: a killed run moves nothing
    for hour in range(days * 24):
        start = first + timedelta(hours=hour)
        release = "1.1" if hour >= days * 12 else "1.0"
        took = 120.0 if release == "1.1" else 60.0
        run_id = f"orders-{hour:03d}"
        killed = hour == days * 24 - 5
        built.add(
            *run(
                "orders",
                start,
                None if killed else "succeeded",
                took,
                run_id=run_id,
                code_version=release,
                host_memory_mb=16_000.0,
                rows=5_000,
                peak_rss_mb=1840.0,
                memory_limit_mb=2048.0,
            )
        )
        built.add(
            PipelineRecord(
                pipeline_id="orders",
                run_id=run_id,
                at=start,
                source_type="rest",
                source="shop",
                destination="duckdb",
                dataset="shop",
                extract_workers=5,
                normalize_workers=1,
                load_workers=1,
            )
        )
        built.add(resource("orders.orders", run_id, start))
        extract = took * 0.8
        built.add(
            StepRecord(
                run_id=run_id,
                load_id=None,
                step="extract",
                attempt=1,
                resources="orders",
                at=start + timedelta(seconds=extract),
                started_at=start,
                duration_s=extract,
                cpu_s=extract * 0.05,
                outcome="ok",
            ),
            StepRecord(
                run_id=run_id,
                load_id=None,
                step="load",
                attempt=1,
                resources="orders",
                at=start + timedelta(seconds=took),
                started_at=start + timedelta(seconds=extract),
                duration_s=took - extract,
                cpu_s=1.0,
                outcome="ok",
            ),
        )
        peak = 2000.0 if killed else 1840.0
        for second in range(0, int(took) + 1, 20):  # up to its peak
            built.add(
                sample(
                    run_id,
                    start + timedelta(seconds=second),
                    peak * (0.5 + second / took / 2),
                    "extract",
                    limit=2048.0,
                    total=16_000.0,
                    available=8000.0,
                )
            )
        if not killed:
            built.add(
                load(
                    "orders.orders",
                    run_id,
                    start,
                    5_000,
                    cursor_from=committed.isoformat(),
                    cursor_to=start.isoformat(),
                ),
                TableRecord(
                    load_id=f"t{hour}",
                    resource_id="orders.orders",
                    run_id=run_id,
                    at=start,
                    table="orders",
                    rows=5_000,
                    bytes=0,
                    jobs=1,
                    retries=0,
                    load_started_at=None,
                    load_finished_at=None,
                    load_s=took * 0.2,
                ),
            )
            committed = start
    return built.frames()


def backfilled() -> Frames:
    """``shop.orders`` declares ten days of daily history. A backfill loaded days 0 to 5 (day 2
    twice, day 4 empty between full days), failed at day 6, and was not rerun, so days 6 to 9
    are missing. Reconciliation finds day 3 short of the source. Its incremental cursor
    skipped two hours."""
    built = Frames_()
    start = datetime(2026, 10, 1, tzinfo=UTC)
    history = (start, start + timedelta(days=10))
    job = NOW - timedelta(days=1)
    built.add(*run("shop", job, "succeeded", 600.0, mode="backfill", run_id="job-ok", rows=50_000))
    for day in range(6):
        low, high = start + timedelta(days=day), start + timedelta(days=day + 1)
        rows = 0 if day == 4 else 10_000
        built.add(
            load(
                "shop.orders",
                "job-ok",
                job,
                rows,
                kind="time",
                start=low.isoformat(),
                end=high.isoformat(),
            )
        )
        if day == 2:
            built.add(
                load(
                    "shop.orders",
                    "job-ok",
                    job,
                    rows,
                    kind="time",
                    start=low.isoformat(),
                    end=high.isoformat(),
                )
            )
    day6 = start + timedelta(days=6)
    built.add(
        *run(
            "shop",
            job + timedelta(hours=1),
            "failed",
            30.0,
            mode="backfill",
            run_id="job-failed",
            **failure(
                "HTTPError",
                "503 Service Unavailable",
                error_resource="orders",
                error_window=f"[{day6.isoformat()}, {(day6 + timedelta(days=1)).isoformat()})",
            ),
        )
    )
    built.add(
        ReconcileRecord(
            reconcile_id="rec",
            resource_id="shop.orders",
            at=job + timedelta(hours=2),
            window_kind="all",
            window_start=None,
            window_end=None,
            destination_rows=50_300,
            distinct_keys=50_300,
            source_rows=None,
        ),
        ReconcileRecord(
            reconcile_id="rec",
            resource_id="shop.orders",
            at=job + timedelta(hours=2),
            window_kind="time",
            window_start=(start + timedelta(days=3)).isoformat(),
            window_end=(start + timedelta(days=4)).isoformat(),
            destination_rows=10_000,
            distinct_keys=None,
            source_rows=10_400,
        ),
    )
    for hour, (low, high) in enumerate(((0, 1), (1, 2), (4, 5))):  # 2h to 4h never read
        at = NOW - timedelta(hours=6 - hour)
        run_id = f"shop-inc-{hour}"
        built.add(*run("shop", at, "succeeded", 5.0, run_id=run_id, rows=100))
        base = NOW - timedelta(hours=10)
        built.add(
            load(
                "shop.orders",
                run_id,
                at,
                100,
                cursor_from=(base + timedelta(hours=low)).isoformat(),
                cursor_to=(base + timedelta(hours=high)).isoformat(),
            )
        )
        built.add(resource("shop.orders", run_id, at, history=history))
    return built.frames()

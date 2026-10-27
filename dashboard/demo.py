"""Demo frames: a week of four made-up pipelines, built from the framework's own records, with
every pattern the dashboard looks for put in on purpose.

- ``orders_api`` (REST, hourly): waits on upstream, retries now and then, a backfill with two
  gaps, and one cursor hole.
- ``erp_sql`` (SQL, every three hours): big; memory grows with volume towards its limit, one
  run OOM-killed; one resource holds up the others; a slow merge table; release ``1.5.0``
  made it slower; reconciliation finds duplicate keys and a window short of the source.
- ``files_landing`` (files, every six hours): often nothing new, which its policy allows.
- ``scrape_filings`` (scrape, daily): failing for the last three days, so also late.

The records are the framework's, so they have the frames' exact shapes; nothing here is
special-cased by the dashboard.
"""

import random
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime, timedelta
from typing import Any

from dlt_framework.core.monitoring import (
    Frames,
    LoadRecord,
    PipelineRecord,
    ProfileRecord,
    ReconcileRecord,
    Record,
    ResourceRecord,
    RunRecord,
    SampleRecord,
    StepRecord,
    TableRecord,
)
from dlt_framework.core.monitoring.records import RECORDS

HOST_MB = 16_384.0


@dataclass
class _Pipeline:
    name: str
    source_type: str
    every: timedelta
    resources: list[str]
    limit_mb: float | None
    rows: int  # typical rows a run, over all resources
    extract_cpu: float  # CPU share of extract: low waits on upstream
    cursor: str  # TimeCursor or ValueCursor
    workers: tuple[int, int, int] = (5, 3, 1)
    children: dict[str, str] = field(default_factory=dict)  # resource -> its nested table


PIPELINES = [
    _Pipeline(
        "orders_api",
        "RestApiSource",
        timedelta(hours=1),
        ["orders", "customers"],
        2048.0,
        6_000,
        0.08,
        "TimeCursor",
        children={"orders": "orders__lines"},
    ),
    _Pipeline(
        "erp_sql",
        "SqlSource",
        timedelta(hours=3),
        ["ledger_lines", "accounts", "vendors"],
        4096.0,
        400_000,
        0.35,
        "ValueCursor",
        workers=(3, 3, 1),
    ),
    _Pipeline(
        "files_landing",
        "FilesystemSource",
        timedelta(hours=6),
        ["invoices"],
        1024.0,
        900,
        0.5,
        "TimeCursor",
    ),
    _Pipeline(
        "scrape_filings",
        "ScrapeSource",
        timedelta(days=1),
        ["filings"],
        1024.0,
        1_500,
        0.05,
        "TimeCursor",
    ),
]


def frames(now: datetime | None = None, days: int = 7, seed: int = 7) -> Frames:
    """A week of runs ending at ``now``, the same every time for one seed."""
    now = (now or datetime.now(UTC)).replace(microsecond=0)
    build = _Builder(random.Random(seed), now)
    for pipeline in PIPELINES:
        build.pipeline(pipeline, now - timedelta(days=days))
    build.backfills()
    build.reconcile()
    return build.frames()


class _Builder:
    def __init__(self, rng: random.Random, now: datetime) -> None:
        self.rng, self.now = rng, now
        self.records: dict[str, list[Record]] = {record.name: [] for record in RECORDS}
        self.cursor: dict[str, int] = {}
        midnight = now.replace(hour=0, minute=0, second=0)
        self.history = (
            midnight - timedelta(days=30),
            midnight - timedelta(days=7),
        )  # orders' backfill

    def add(self, *records: Record) -> None:
        for record in records:
            self.records[record.name].append(record)

    def frames(self) -> Frames:
        by_name = {record.name: record for record in RECORDS}
        return Frames(**{name: by_name[name].frame(rows) for name, rows in self.records.items()})

    def pipeline(self, pipeline: _Pipeline, start: datetime) -> None:
        at, index = start + timedelta(minutes=self.rng.randint(0, 20)), 0
        while at < self.now - timedelta(minutes=5):
            self.run(pipeline, at, index)
            at += pipeline.every + timedelta(seconds=self.rng.randint(-90, 90))
            index += 1

    def run(self, pipeline: _Pipeline, started: datetime, index: int) -> None:
        rng, name = self.rng, pipeline.name
        run_id = f"{rng.getrandbits(128):032x}"
        release = "1.5.0" if started > self.now - timedelta(days=3) else "1.4.1"
        identity: dict[str, Any] = {
            "run_id": run_id,
            "pipeline_id": name,
            "dlt_pipeline": name,
            "mode": "run",
            "host": "worker-1",
            "dlt_version": "1.29.1",
            "code_version": release,
            "python": "3.12.8",
            "cpu_count": 8,
            "host_memory_mb": HOST_MB,
        }
        self.add(RunRecord(**identity, event="started", at=started))
        self.add(
            PipelineRecord(
                pipeline_id=name,
                run_id=run_id,
                at=started,
                source_type=pipeline.source_type,
                source=name,
                destination="duckdb",
                dataset=f"raw_{name}",
                extract_workers=pipeline.workers[0],
                normalize_workers=pipeline.workers[1],
                load_workers=pipeline.workers[2],
            )
        )
        for resource in pipeline.resources:
            self.add(self._resource(pipeline, resource, run_id, started))

        volume = self._volume(pipeline, started)
        failing = name == "scrape_filings" and started > self.now - timedelta(days=3)
        killed = name == "erp_sql" and index == 31
        clock = self._step(run_id, "sync", started, 0.6 + rng.random(), 0.05, None)
        steps: list[StepRecord] = []  # written with the load's id once it commits, as runs do
        attempt = 1
        if name == "orders_api" and rng.random() < 0.12:
            clock = self._step(
                run_id,
                "extract",
                clock,
                6 + rng.random() * 8,
                0.02,
                "ConnectionError",
                buffered=steps,
                resources=pipeline.resources,
            )
            attempt = 2
        slow = 1.6 if name == "erp_sql" and release == "1.5.0" else 1.0
        extract_s = (4 + volume / 900) * slow * (0.8 + rng.random() * 0.4)
        extract_start = clock
        if failing:
            self._step(run_id, "extract", clock, 3.2, 0.04, "HTTPError")
            self._samples(
                run_id,
                started,
                clock + timedelta(seconds=3.2),
                pipeline,
                volume,
                "extract",
                pipeline.resources,
            )
            self.add(
                RunRecord(
                    **identity,
                    event="failed",
                    at=clock + timedelta(seconds=3.4),
                    rows=0,
                    duration_s=(clock - started).total_seconds() + 3.4,
                    exit_code=3,
                    error_type="TerminalRunError",
                    error_step="extract",
                    error_resource="filings",
                    error_window="increment",
                    error_cause_type="HTTPError",
                    error_message="403 Client Error: Forbidden for url: https://filings.example.gov/index?page=***",
                    error_where="dlt_framework/sources/scrape/source.py:212 in _crawl",
                    error_origin="adapters.py:667 in send",
                    error_chain=_CHAIN,
                    error_notes=None,
                    error_traceback=_TRACEBACK,
                    peak_rss_mb=180.0,
                    memory_limit_mb=pipeline.limit_mb,
                    cpu_seconds=1.1,
                )
            )
            return
        clock = self._step(
            run_id,
            "extract",
            clock,
            extract_s,
            pipeline.extract_cpu,
            None,
            attempt=attempt,
            buffered=steps,
            resources=pipeline.resources,
        )
        normalize_s = 1 + volume / 25_000
        normalize_start = clock
        clock = self._step(
            run_id,
            "normalize",
            clock,
            normalize_s,
            2.4,
            None,
            buffered=steps,
            resources=pipeline.resources,
        )
        load_s = 1.5 + volume / (9_000 if name == "erp_sql" else 60_000)
        load_start = clock
        if killed:
            self._samples(
                run_id,
                started,
                clock + timedelta(seconds=load_s * 0.6),
                pipeline,
                volume * 2.2,
                "load",
                pipeline.resources,
                climb=True,
            )
            return  # killed for memory: no end, no load records
        clock = self._step(
            run_id, "load", clock, load_s, 0.12, None, buffered=steps, resources=pipeline.resources
        )
        load_id = f"{started.timestamp():.6f}"
        self.add(*(replace(step, load_id=load_id) for step in steps))
        self._samples(
            run_id,
            started,
            clock,
            pipeline,
            volume,
            None,
            pipeline.resources,
            bands=[
                ("extract", extract_start, normalize_start),
                ("normalize", normalize_start, load_start),
                ("load", load_start, clock),
            ],
        )
        total = self._loads(
            pipeline,
            run_id,
            load_id,
            clock,
            extract_start,
            normalize_start,
            volume,
            hole=name == "orders_api" and index == 100,
        )
        self._tables(pipeline, run_id, load_id, load_start, clock, total)
        if name == "erp_sql" and index == 12:
            self._profile(run_id, clock)
        peak = min(self._peak(pipeline, volume), (pipeline.limit_mb or HOST_MB) * 0.92)  # it lived
        self.add(
            RunRecord(
                **identity,
                event="succeeded",
                at=clock,
                rows=sum(total.values()),
                duration_s=(clock - started).total_seconds(),
                peak_rss_mb=peak,
                memory_limit_mb=pipeline.limit_mb,
                cpu_seconds=extract_s * pipeline.extract_cpu + normalize_s * 2.4 + load_s * 0.12,
            )
        )

    def _volume(self, pipeline: _Pipeline, at: datetime) -> float:
        if pipeline.name == "files_landing":
            return 0 if self.rng.random() < 0.55 else pipeline.rows * (0.5 + self.rng.random())
        daily = 1 + 0.6 * abs((at.hour - 12) / 12 - 0.5)  # busier in the day
        growth = (
            1 + 0.12 * (at - (self.now - timedelta(days=7))).days
            if pipeline.name == "erp_sql"
            else 1
        )
        return pipeline.rows * daily * growth * (0.75 + self.rng.random() * 0.5)

    def _peak(self, pipeline: _Pipeline, volume: float) -> float:
        base = {
            "orders_api": 380.0,
            "erp_sql": 900.0,
            "files_landing": 210.0,
            "scrape_filings": 260.0,
        }[pipeline.name]
        return base + volume / {"erp_sql": 420.0}.get(pipeline.name, 40.0) + self.rng.random() * 40

    def _step(
        self,
        run_id: str,
        step: str,
        start: datetime,
        seconds: float,
        cpu: float,
        cause: str | None,
        *,
        attempt: int = 1,
        buffered: list[StepRecord] | None = None,
        resources: list[str] | None = None,
    ) -> datetime:
        end = start + timedelta(seconds=seconds)
        record = StepRecord(
            run_id=run_id,
            load_id=None,
            step=step,
            attempt=attempt,
            resources=", ".join(resources) if resources else None,
            at=end,
            started_at=start,
            duration_s=seconds,
            cpu_s=seconds * cpu,
            outcome="failed" if cause else "ok",
            error_cause_type=cause,
        )
        if buffered is None:
            self.add(record)
        else:
            buffered.append(record)
        return end

    def _samples(
        self,
        run_id: str,
        start: datetime,
        end: datetime,
        pipeline: _Pipeline,
        volume: float,
        step: str | None,
        resources: list[str],
        *,
        bands: list[tuple[str, datetime, datetime]] | None = None,
        climb: bool = False,
    ) -> None:
        peak = self._peak(pipeline, volume)
        at, span = start + timedelta(seconds=2), max((end - start).total_seconds(), 1)
        while at <= end:
            share = (at - start).total_seconds() / span
            current = step
            for name, low, high in bands or []:
                if low <= at < high:
                    current = name
            rss = 150 + (peak - 150) * (
                share if climb else min(1.0, 0.35 + share) * (0.85 + 0.15 * self.rng.random())
            )
            if climb:
                rss = min(rss * 1.15, (pipeline.limit_mb or HOST_MB) * 0.99)
            cpu = {"extract": pipeline.extract_cpu * 100, "normalize": 240.0, "load": 12.0}.get(
                current or "", 5.0
            )
            self.add(
                SampleRecord(
                    run_id=run_id,
                    at=at,
                    step=current,
                    resources=", ".join(resources) if current else None,
                    rss_mb=rss,
                    cpu_percent=cpu * (0.8 + self.rng.random() * 0.4),
                    threads=14 if current == "extract" else 6,
                    read_mb_s=self.rng.random() * 30,
                    write_mb_s=self.rng.random() * (60 if current == "load" else 8),
                    memory_limit_mb=pipeline.limit_mb,
                    system_available_mb=HOST_MB - 4000 - rss,
                    system_total_mb=HOST_MB,
                )
            )
            at += timedelta(seconds=max(2.0, span / 40) if not climb else 3.0)

    def _resource(
        self, pipeline: _Pipeline, resource: str, run_id: str, at: datetime
    ) -> ResourceRecord:
        declared = pipeline.name == "orders_api" and resource == "orders"
        start, end = self.history
        return ResourceRecord(
            resource_id=f"{pipeline.name}.{resource}",
            pipeline_id=pipeline.name,
            run_id=run_id,
            at=at,
            resource=resource,
            write_mode="Merge" if resource != "invoices" else "Append",
            cursor_kind=pipeline.cursor,
            cursor_path="updated_at" if pipeline.cursor == "TimeCursor" else "rv",
            cursor_start=end.isoformat() if declared else "0",
            backfill_kind="TimeBackfill" if declared else None,
            backfill_window="time" if declared else None,
            backfill_start=start.isoformat() if declared else None,
            backfill_end=end.isoformat() if declared else None,
            empty="allow" if resource == "invoices" else "warn",
            definition=f"Resource(name={resource!r}, ...)",
        )

    def _loads(
        self,
        pipeline: _Pipeline,
        run_id: str,
        load_id: str,
        at: datetime,
        extract_start: datetime,
        extract_end: datetime,
        volume: float,
        *,
        hole: bool,
    ) -> dict[str, int]:
        weights = {
            "ledger_lines": 0.82,
            "accounts": 0.12,
            "vendors": 0.06,
            "orders": 0.7,
            "customers": 0.3,
        }
        totals = {}
        for resource in pipeline.resources:
            rows = int(volume * weights.get(resource, 1.0))
            span = (extract_end - extract_start).total_seconds() * (
                1.0 if weights.get(resource, 1.0) >= 0.7 else 0.25 + self.rng.random() * 0.3
            )
            key = f"{pipeline.name}.{resource}"
            before = self.cursor.get(key, self._origin(pipeline))
            step = int(pipeline.every.total_seconds()) if pipeline.cursor == "TimeCursor" else rows
            after = before + max(step, 0)
            start_value = before
            if hole and resource == "orders":
                start_value = before + 7_200  # a hole: two hours never loaded
            self.cursor[key] = after if rows else before
            text = _cursor_text(pipeline.cursor)
            self.add(
                LoadRecord(
                    load_id=load_id,
                    resource_id=key,
                    run_id=run_id,
                    at=at,
                    window_kind="increment",
                    window_start=None,
                    window_end=None,
                    part=0,
                    rows=rows,
                    child_rows=rows * 3 if resource in pipeline.children else 0,
                    bytes=rows * 210,
                    extract_first_at=extract_start if rows else None,
                    extract_last_at=extract_start + timedelta(seconds=span) if rows else None,
                    cursor_start_value=text(start_value),
                    cursor_last_value=text(self.cursor[key]),
                )
            )
            totals[resource] = rows
        return totals

    def _origin(self, pipeline: _Pipeline) -> int:
        """Where a cursor starts: a week ago for time cursors (epoch seconds), else a number."""
        return int(self.history[1].timestamp()) if pipeline.cursor == "TimeCursor" else 1_000_000

    def _tables(
        self,
        pipeline: _Pipeline,
        run_id: str,
        load_id: str,
        start: datetime,
        end: datetime,
        totals: dict[str, int],
    ) -> None:
        span = (end - start).total_seconds()
        for resource, rows in totals.items():
            tables = [(resource, rows)] + (
                [(pipeline.children[resource], rows * 3)] if resource in pipeline.children else []
            )
            for table, count in tables:
                share = 0.78 if table == "ledger_lines" else 0.1
                self.add(
                    TableRecord(
                        load_id=load_id,
                        resource_id=f"{pipeline.name}.{resource}",
                        run_id=run_id,
                        at=end,
                        table=table,
                        rows=count,
                        bytes=count * 180,
                        jobs=2 if table == "ledger_lines" else 1,
                        retries=0,
                        load_started_at=start,
                        load_finished_at=start + timedelta(seconds=span * share),
                        load_s=span * share,
                    )
                )

    def _profile(self, run_id: str, at: datetime) -> None:
        functions = [
            (
                "extract",
                "sqlalchemy.engine.cursor:CursorResult.fetchmany",
                "sqlalchemy/engine/cursor.py",
                2101,
                0.41,
                0.52,
            ),
            (
                "extract",
                "dlt.extract.incremental:Incremental.__call__",
                "dlt/extract/incremental/__init__.py",
                560,
                0.18,
                0.31,
            ),
            (
                "extract",
                "dlt_framework.core.source:Chunk.__call__",
                "dlt_framework/core/source.py",
                34,
                0.03,
                0.05,
            ),
            (
                "normalize",
                "dlt.normalize.items_normalizers:JsonLItemsNormalizer._normalize_chunk",
                "dlt/normalize/items_normalizers.py",
                240,
                0.36,
                0.71,
            ),
            (
                "normalize",
                "dlt.common.json:_orjson_dumps",
                "dlt/common/json/_orjson.py",
                22,
                0.22,
                0.22,
            ),
            ("load", "duckdb:DuckDBPyConnection.execute", "duckdb/__init__.py", 1, 0.83, 0.88),
        ]
        for step, function, file, line, own, total in functions:
            self.add(
                ProfileRecord(
                    run_id=run_id,
                    at=at,
                    step=step,
                    function=function,
                    file=file,
                    line=line,
                    own_samples=int(own * 900),
                    total_samples=int(total * 900),
                    own_share=own,
                    total_share=total,
                )
            )

    def backfills(self) -> None:
        """``orders_api.orders``: daily windows over its 23 declared days, two never loaded."""
        start, end = self.history
        day, number = start, 0
        while day < end:
            if number not in (6, 15):
                at = self.now - timedelta(days=8) + timedelta(minutes=number * 3)
                self.add(
                    LoadRecord(
                        load_id=f"b{number}",
                        resource_id="orders_api.orders",
                        run_id="backfill-1",
                        at=at,
                        window_kind="time",
                        window_start=day.isoformat(),
                        window_end=(day + timedelta(days=1)).isoformat(),
                        part=number,
                        rows=0 if number == 11 else 4_000 + self.rng.randint(0, 900),
                        child_rows=0,
                        bytes=0,
                        extract_first_at=None,
                        extract_last_at=None,
                        cursor_start_value=None,
                        cursor_last_value=None,
                    )
                )
                if number == 3:  # loaded twice: the backfill was rerun over it
                    self.add(
                        LoadRecord(
                            load_id="b3-again",
                            resource_id="orders_api.orders",
                            run_id="backfill-1",
                            at=at + timedelta(hours=1),
                            window_kind="time",
                            window_start=day.isoformat(),
                            window_end=(day + timedelta(days=1)).isoformat(),
                            part=number,
                            rows=4_200,
                            child_rows=0,
                            bytes=0,
                            extract_first_at=None,
                            extract_last_at=None,
                            cursor_start_value=None,
                            cursor_last_value=None,
                        )
                    )
            day += timedelta(days=1)
            number += 1

    def reconcile(self) -> None:
        at = self.now - timedelta(hours=2)
        rows = [
            ("erp_sql.ledger_lines", "all", None, None, 9_402_118, 9_402_106, 9_402_106),
            ("erp_sql.accounts", "all", None, None, 1_311, 1_311, 1_311),
            ("orders_api.orders", "all", None, None, 418_220, 418_220, 421_020),
            (
                "orders_api.orders",
                "time",
                "2026-10-09T00:00:00+00:00",
                "2026-10-10T00:00:00+00:00",
                3_102,
                None,
                5_902,
            ),
        ]
        for resource_id, kind, start, end, destination, keys, source in rows:
            self.add(
                ReconcileRecord(
                    reconcile_id="rec-1",
                    resource_id=resource_id,
                    at=at,
                    window_kind=kind,
                    window_start=start,
                    window_end=end,
                    destination_rows=destination,
                    distinct_keys=keys,
                    source_rows=source,
                )
            )


def _cursor_text(kind: str) -> Any:
    if kind == "TimeCursor":
        return lambda value: datetime.fromtimestamp(value, UTC).isoformat()
    return str


_CHAIN = (
    "TerminalRunError: HTTPError: 403 Client Error: Forbidden\n"
    "ScrapeError: 1 request failed\n"
    "HTTPError: 403 Client Error: Forbidden"
)
_TRACEBACK = """Traceback (most recent call last):
  File "dlt_framework/sources/scrape/source.py", line 212, in _crawl
    raise ScrapeError(f"{len(failed)} request failed")
dlt_framework.sources.scrape.source.ScrapeError: 1 request failed

The above exception was the direct cause of the following exception:

Traceback (most recent call last):
  File "dlt_framework/core/retry.py", line 91, in __call__
    raise self._classify(step, error, retrying.statistics["attempt_number"]) from error
dlt_framework.core.errors.TerminalRunError: HTTPError: 403 Client Error: Forbidden
"""

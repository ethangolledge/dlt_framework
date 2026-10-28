"""Frames to facts: each fact against frames whose answer is known, dirty data included."""

from dataclasses import replace
from datetime import UTC, datetime, timedelta

import polars as pl
import pytest

from dashboard import demo
from dashboard.model import (
    advisor,
    capacity,
    completeness,
    failures,
    overview,
    performance,
    triage,
)
from dashboard.model.facts import Scope, View, runs
from dlt_framework.core.monitoring import Frames, LoadRecord, RunRecord, SampleRecord, StepRecord
from dlt_framework.core.monitoring.health import Signal

NOW = datetime(2026, 10, 27, 12, tzinfo=UTC)
IDENTITY = {
    "pipeline_id": "p",
    "dlt_pipeline": "p",
    "mode": "run",
    "host": "h",
    "dlt_version": "1",
    "code_version": "v",
    "python": "3",
    "cpu_count": 4,
    "host_memory_mb": 1000.0,
}


def run(run_id: str, event: str, at: datetime, **details) -> RunRecord:
    return RunRecord(run_id=run_id, event=event, at=at, **IDENTITY, **details)


def started_and(
    run_id: str, start: datetime, event: str | None, seconds: float = 10, **details
) -> list[RunRecord]:
    records = [run(run_id, "started", start)]
    if event:
        records.append(
            run(run_id, event, start + timedelta(seconds=seconds), duration_s=seconds, **details)
        )
    return records


def frames_of(*records, **frames) -> Frames:
    by = {}
    for record in records:
        by.setdefault(type(record), []).append(record)
    built = {kind.name: kind.frame(rows) for kind, rows in by.items()}
    return Frames(**{**built, **frames})


def sample(run_id: str, at: datetime, rss: float, step: str | None = None) -> SampleRecord:
    return SampleRecord(
        run_id=run_id,
        at=at,
        step=step,
        resources=None,
        rss_mb=rss,
        cpu_percent=50.0,
        threads=1,
        read_mb_s=None,
        write_mb_s=None,
        memory_limit_mb=None,
        system_available_mb=500.0,
        system_total_mb=1000.0,
    )


def step(
    run_id: str,
    name: str,
    attempt: int,
    outcome: str = "ok",
    cause: str | None = None,
    seconds: float = 10,
    cpu: float = 1,
) -> StepRecord:
    return StepRecord(
        run_id=run_id,
        load_id=None,
        step=name,
        attempt=attempt,
        resources="r",
        at=NOW,
        started_at=NOW,
        duration_s=seconds,
        cpu_s=cpu,
        outcome=outcome,
        error_cause_type=cause,
    )


def outcomes(frames: Frames) -> dict[str, str]:
    return dict(runs(frames, NOW).select("run_id", "outcome").iter_rows())


def test_runs_have_one_row_each_however_often_records_were_written() -> None:
    records = started_and("a", NOW - timedelta(hours=1), "succeeded", rows=5)
    twice = frames_of(*records, *records)  # an interrupted compaction wrote them again
    table = runs(twice, NOW)
    assert table.height == 1 and table.row(0, named=True)["rows"] == 5


def test_a_run_with_no_end_is_running_until_it_outlives_its_pipeline_then_lost() -> None:
    history = [
        r
        for index in range(5)
        for r in started_and(
            f"ok{index}", NOW - timedelta(days=1, hours=index), "succeeded", seconds=3600
        )
    ]
    young = run("young", "started", NOW - timedelta(minutes=30))
    old = run("old", "started", NOW - timedelta(hours=3))  # 1.5 x p99 (1h) is 1.5h
    found = outcomes(frames_of(*history, young, old))
    assert (found["young"], found["old"]) == ("running", "lost")


def test_a_lost_run_takes_its_duration_and_peak_from_its_last_samples() -> None:
    start = NOW - timedelta(hours=2)
    frames = frames_of(
        run("x", "started", start),
        sample("x", start + timedelta(seconds=30), 300),
        sample("x", start + timedelta(seconds=90), 900, "load"),
    )
    row = runs(frames, NOW).row(0, named=True)
    assert (row["outcome"], row["duration_s"], row["peak_rss_mb"]) == ("lost", 90.0, 900.0)
    assert row["memory_share"] == 0.9  # against the host, with no container limit


def test_a_success_after_a_retried_step_is_retried() -> None:
    frames = frames_of(
        *started_and("r", NOW - timedelta(hours=1), "succeeded"),
        step("r", "extract", 1, "failed", "ConnectionError"),
        step("r", "extract", 2),
    )
    assert outcomes(frames) == {"r": "retried"}


def test_scope_keeps_runs_active_in_its_range_and_its_filters() -> None:
    inside = started_and("in", NOW - timedelta(hours=2), "failed")
    before = started_and("old", NOW - timedelta(days=3), "succeeded")
    view = View.build(frames_of(*inside, *before), Scope.last("24h", NOW), NOW)
    assert view.runs["run_id"].to_list() == ["in"] and view.history.height == 2
    kept = View.build(frames_of(*inside), Scope.last("24h", NOW, outcomes=("succeeded",)), NOW)
    assert kept.runs.is_empty()
    with pytest.raises(KeyError):
        Scope.last("3x", NOW)


def test_the_matrix_shows_the_worst_outcome_of_each_bucket() -> None:
    hour = NOW - timedelta(minutes=50)
    frames = frames_of(
        *started_and("a", hour, "succeeded"),
        *started_and("b", hour + timedelta(minutes=2), "failed"),
    )
    ((_, cells),) = overview.matrix(View.build(frames, Scope.last("1h", NOW), NOW), 4)
    assert cells == ["failed", None, None, None]


def test_kpis_compare_with_the_period_before() -> None:
    now_runs = [
        r
        for index in range(4)
        for r in started_and(f"n{index}", NOW - timedelta(hours=index + 1), "succeeded", rows=10)
    ]
    before = [r for r in started_and("b", NOW - timedelta(hours=30), "succeeded", rows=10)]
    kpis = {
        kpi.title: kpi
        for kpi in overview.kpis(
            View.build(frames_of(*now_runs, *before), Scope.last("24h", NOW), NOW)
        )
    }
    assert (kpis["RUNS"].value, kpis["RUNS"].note) == ("4", "▲300%")
    assert (kpis["SUCCESS"].value, kpis["ROWS"].value) == ("100.0%", "40")


@pytest.fixture(scope="module")
def week() -> View:
    """The demo week: every pattern the dashboard looks for, planted."""
    return View.build(demo.frames(NOW), Scope.last("7d", NOW), NOW)


def test_the_demo_patterns_are_all_found(week) -> None:
    kinds = {found.kind for found in completeness.cursor_breaks(week)}
    assert "hole" in kinds
    covered = {item.resource_id: item for item in completeness.coverage(week)}["orders_api.orders"]
    assert len(covered.gaps) == 2 and covered.fraction == pytest.approx(21 / 23)
    assert covered.commands[0].startswith("pipeline backfill orders_api --resource orders --from ")
    assert completeness.double_loads(week)["times"].to_list() == [2]
    assert completeness.empty_windows(week).height == 1
    reconciled = {
        row["resource_id"]: row for row in completeness.reconciled(week).iter_rows(named=True)
    }
    assert (
        reconciled["erp_sql.ledger_lines"]["duplicates"],
        reconciled["orders_api.orders"]["differing"],
    ) == (12, 2)
    assert (
        failures.recovery(week)
        .filter(pl.col("pipeline_id") == "scrape_filings")["failing_since"]
        .item()
        is not None
    )
    assert week.runs.filter(pl.col("outcome") == "lost")["pipeline_id"].to_list() == ["erp_sql"]


def test_performance_finds_waiting_steps_the_critical_path_and_the_slow_release(week) -> None:
    steps = {
        (row["pipeline_id"], row["step"]): row
        for row in performance.step_profile(week).iter_rows(named=True)
    }
    assert steps["orders_api", "extract"]["cpu_share"] < 0.2  # waits on upstream
    assert steps["erp_sql", "normalize"]["cpu_share"] > 1  # computes, over several processes
    path = performance.critical_path(week).row(0, named=True)
    assert path["last"] == path["loads"] and path["resource"] in ("orders", "ledger_lines")
    assert performance.slow_tables(week).row(0, named=True)["table"] == "ledger_lines"
    versions = (
        performance.by_version(week).filter(pl.col("pipeline_id") == "erp_sql")["p50_s"].to_list()
    )
    assert versions[1] > versions[0] * 1.5
    retried = performance.retries(week)
    assert set(retried["error_cause_type"]) == {
        "ConnectionError"
    }  # terminal failures are not retries


def test_capacity_sizes_from_the_p99_peak_and_rounds_up() -> None:
    start = NOW - timedelta(hours=5)
    records = [
        r
        for index in range(5)
        for r in started_and(
            f"m{index}",
            start + timedelta(minutes=index),
            "succeeded",
            peak_rss_mb=1000.0 + index,
            memory_limit_mb=2048.0,
        )
    ]
    row = capacity.memory(View.build(frames_of(*records), Scope.last("24h", NOW), NOW)).row(
        0, named=True
    )
    assert row["recommended_mb"] == 1280.0  # 1004 x 1.25 = 1255, up to the next 256
    assert row["share"] == pytest.approx(1004 / 2048, abs=0.001)


def test_the_advisor_turns_each_planted_problem_into_an_action(week) -> None:
    found = {(item.area, item.pipeline, item.finding) for item in advisor.advise(week)}
    expect = [
        ("concurrency", "orders_api", "extract waits on upstream"),
        ("memory", "erp_sql", "memory at 92%"),
        ("memory", "files_landing", "memory 27% used"),
        ("completeness", "orders_api", "orders: 2 history gap(s)"),
        ("completeness", "orders_api", "orders: cursor skipped ahead"),
        ("completeness", "erp_sql", "ledger_lines: 12 duplicate keys, 1 window(s) off the source"),
        ("reliability", "scrape_filings", "HTTPError recurs"),
    ]
    assert [item for item in expect if item not in found] == []
    slower = [finding for area, _, finding in found if area == "performance"]
    assert len(slower) == 1 and slower[0].startswith("1.5.0 is 9")  # 1.6x the work, and noise
    actions = {(item.pipeline, item.finding): item.action for item in advisor.advise(week)}
    waits = actions["orders_api", "extract waits on upstream"]
    assert waits == "raise [sources.orders_api.extract] workers (now 5)"


def test_triage_puts_failing_pipelines_first_with_the_fix_and_the_command(week) -> None:
    found = triage.actions(week, None)
    first = found[0]
    assert (first.level, first.pipeline, first.finding) == (
        "error",
        "scrape_filings",
        "failed 3 runs in a row: HTTPError",
    )
    assert first.action.endswith("then $ pipeline run scrape_filings") and first.run_id
    # the recurring HTTPError is the same story as the failure, so it is told once
    assert not [item for item in found if item.finding == "HTTPError recurs"]
    levels = [triage.RANK[item.level] for item in found]
    assert levels == sorted(levels)


def test_statuses_rank_pipelines_by_their_worst_finding(week) -> None:
    states = triage.statuses(week, triage.actions(week, None))
    assert [(status.pipeline, status.level) for status in states] == [
        ("scrape_filings", "error"),
        ("erp_sql", "warning"),
        ("orders_api", "warning"),
        ("files_landing", "ok"),
    ]
    assert states[1].headline == "memory at 92%  +2"  # memory first: it is what kills runs
    assert states[-1].headline == "ok"


def test_health_adds_what_only_its_thresholds_can_judge(week) -> None:
    late = Signal("files_landing", None, "run_stale", "error", "no success for 30h")
    again = Signal("scrape_filings", None, "run_failed", "error", "HTTPError")
    found = triage.actions(week, [late, again])
    states = {status.pipeline: status for status in triage.statuses(week, found)}
    assert (states["files_landing"].level, states["files_landing"].headline) == ("error", "late")
    assert sum(item.pipeline == "scrape_filings" and item.level == "error" for item in found) == 1


def test_a_lost_run_is_a_failure_to_give_more_memory() -> None:
    frames = frames_of(
        *started_and("ok", NOW - timedelta(hours=30), "succeeded"),
        run("gone", "started", NOW - timedelta(hours=26)),
    )
    view = View.build(frames, Scope.last("7d", NOW), NOW)
    (found,) = triage.failing(view)
    assert found.finding == "last run killed" and found.action.startswith("give it more memory")


def test_a_view_narrowed_to_one_pipeline_keeps_only_its_runs(week) -> None:
    mine = week.only("erp_sql")
    assert set(mine.runs["pipeline_id"]) == {"erp_sql"} == set(mine.history["pipeline_id"])
    assert mine.steps["run_id"].is_in(mine.runs["run_id"].implode()).all()
    assert mine.scope.pipelines == ("erp_sql",)


def test_cursor_breaks_tell_holes_rereads_and_resets_apart() -> None:
    def load(index: int, start: int, last: int) -> LoadRecord:
        return LoadRecord(
            load_id=str(index),
            resource_id="p.r",
            run_id="x",
            at=NOW + timedelta(minutes=index),
            window_kind="increment",
            window_start=None,
            window_end=None,
            part=0,
            rows=1,
            child_rows=0,
            bytes=0,
            extract_first_at=None,
            extract_last_at=None,
            cursor_start_value=str(start),
            cursor_last_value=str(last),
        )

    loads = LoadRecord.frame(
        [load(0, 0, 10), load(1, 10, 20), load(2, 25, 30), load(3, 28, 35), load(4, 5, 8)]
    )
    resources = (
        demo.frames(NOW)
        .resources.head(1)
        .with_columns(resource_id=pl.lit("p.r"), cursor_kind=pl.lit("ValueCursor"))
    )
    view = replace(
        View.build(Frames(), Scope.last("1h", NOW), NOW),
        frames=Frames(loads=loads, resources=resources),
    )
    assert [
        (found.kind, found.previous_last, found.next_start)
        for found in completeness.cursor_breaks(view)
    ] == [
        ("hole", "20", "25"),
        ("reread", "30", "28"),
        ("reset", "35", "5"),
    ]


def test_empty_frames_give_empty_facts_not_errors() -> None:
    view = View.build(Frames(), Scope.last("24h", NOW), NOW)
    assert view.runs.is_empty() and overview.matrix(view, 10) == []
    assert performance.step_profile(view).is_empty() and capacity.memory(view).is_empty()
    assert completeness.coverage(view) == [] and advisor.advise(view) == []

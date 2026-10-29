"""Frames to facts: runs, incidents, missing data, checks, and triage, each against frames
whose answer is known, shaped like real data (sparse, bursty, mixed causes)."""

from datetime import UTC, datetime, timedelta

import pytest
import scenarios
from scenarios import NOW

from dashboard.model import checks, gaps, incidents, performance, triage
from dashboard.model.checks import Check, Finding, Limits
from dashboard.model.facts import Scope, View, runs
from dlt_framework.core.monitoring import Frames, RunRecord, SampleRecord, StepRecord
from dlt_framework.core.monitoring.health import Signal

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
    return RunRecord(run_id=run_id, event=event, at=at, **{**IDENTITY, **details})


def started_and(
    run_id: str, start: datetime, event: str | None, seconds: float = 10, **details
) -> list[RunRecord]:
    records = [run(run_id, "started", start)]
    if event:
        records.append(
            run(run_id, event, start + timedelta(seconds=seconds), duration_s=seconds, **details)
        )
    return records


def frames_of(*records) -> Frames:
    built = scenarios.Frames_()
    return built.add(*records).frames()


def sample(run_id: str, at: datetime, rss: float) -> SampleRecord:
    return scenarios.sample(run_id, at, rss, available=500.0, total=1000.0)


def outcomes(frames: Frames) -> dict[str, str]:
    return dict(runs(frames, NOW).select("run_id", "outcome").iter_rows())


def view(frames: Frames, since: str = "7d") -> View:
    return View.build(frames, Scope.last(since, NOW), NOW)


# ---- runs ----------------------------------------------------------------------------------


def test_runs_have_one_row_each_however_often_records_were_written() -> None:
    records = started_and("a", NOW - timedelta(hours=1), "succeeded", rows=5)
    table = runs(frames_of(*records, *records), NOW)  # an interrupted compaction wrote them twice
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


def test_a_lost_run_takes_its_duration_peak_and_host_memory_from_its_samples() -> None:
    start = NOW - timedelta(hours=2)
    frames = frames_of(
        run("x", "started", start, host_memory_mb=None),
        sample("x", start + timedelta(seconds=30), 300),
        sample("x", start + timedelta(seconds=90), 900),
    )
    row = runs(frames, NOW).row(0, named=True)
    assert (row["outcome"], row["duration_s"], row["peak_rss_mb"]) == ("lost", 90.0, 900.0)
    assert (row["available_mb"], row["memory_share"]) == (1000.0, 0.9)  # the host, from samples


def test_a_success_that_loaded_nothing_is_empty_and_a_failure_is_not() -> None:
    frames = frames_of(
        *started_and("none", NOW - timedelta(hours=2), "succeeded", rows=0),
        *started_and("some", NOW - timedelta(hours=1), "succeeded", rows=5),
        *started_and("broke", NOW - timedelta(minutes=30), "failed", rows=0),
    )
    empty = dict(runs(frames, NOW).select("run_id", "empty").iter_rows())
    assert empty == {"none": True, "some": False, "broke": False}


def test_a_success_after_a_retried_step_is_retried() -> None:
    attempt = {
        "run_id": "r",
        "load_id": None,
        "step": "extract",
        "resources": "r",
        "at": NOW,
        "started_at": NOW,
        "duration_s": 1.0,
        "cpu_s": 0.1,
    }
    frames = frames_of(
        *started_and("r", NOW - timedelta(hours=1), "succeeded"),
        StepRecord(attempt=1, outcome="failed", **attempt),
        StepRecord(attempt=2, outcome="ok", **attempt),
    )
    assert outcomes(frames) == {"r": "retried"}


def test_scope_keeps_runs_active_in_its_range() -> None:
    inside = started_and("in", NOW - timedelta(hours=2), "failed")
    before = started_and("old", NOW - timedelta(days=3), "succeeded")
    found = view(frames_of(*inside, *before), "24h")
    assert found.runs["run_id"].to_list() == ["in"] and found.history.height == 2
    with pytest.raises(KeyError):
        Scope.last("3x", NOW)


def test_all_of_history_starts_at_the_first_run_not_1970() -> None:
    found = view(frames_of(*started_and("a", NOW - timedelta(days=40), "succeeded")), "all")
    assert found.scope.start == NOW - timedelta(days=40)


def test_runs_and_backfills_are_kept_apart() -> None:
    found = view(scenarios.backfilled())
    assert set(found.mode("run").runs["mode"]) == {"run"}
    assert set(found.mode("backfill").runs["mode"]) == {"backfill"}
    assert found.mode("run").runs.height + found.mode("backfill").runs.height == found.runs.height


# ---- incidents -----------------------------------------------------------------------------


@pytest.fixture(scope="module")
def hand() -> View:
    return view(scenarios.hand_runs())


def test_an_incident_counts_every_cause_in_its_streak(hand) -> None:
    (form4,) = [item for item in incidents.incidents(hand) if item.pipeline == "form4"]
    assert form4.open and len(form4.runs) == 4
    assert form4.causes == [
        ("ConfigFieldMissingException", 2),
        ("stopped (SIGTERM)", 1),
        ("killed", 1),
    ]
    assert form4.latest["run_id"] == "form4-signal"
    assert incidents.rerun(form4.latest) == "pipeline run form4"


def test_an_incident_is_resolved_by_the_next_success_and_kept_as_history(hand) -> None:
    found = {item.pipeline: item for item in incidents.incidents(hand)}
    insider, members = found["insider"], found["members"]
    assert not insider.open and insider.causes[0] == ("killed", 3) and len(insider.runs) == 5
    assert not members.open and members.resolved_at - members.started_at < timedelta(minutes=2)
    assert "filings" not in found
    assert [item.open for item in incidents.incidents(hand)] == [True, False, False]  # open first


def test_a_resolved_incident_before_the_range_is_not_shown() -> None:
    found = incidents.incidents(view(scenarios.hand_runs(), "1h"))
    assert [item.pipeline for item in found] == ["form4"]  # still open, so still shown


def test_killed_is_only_called_memory_when_the_peak_says_so(hand) -> None:
    insider = next(item for item in incidents.incidents(hand) if item.pipeline == "insider")
    assert insider.causes[0][0] == "killed"  # gigabytes, but no limit recorded to compare with
    scheduled = incidents.incidents(view(scenarios.scheduled()))
    assert [item.summary for item in scheduled] == ["killed near memory limit"]


def test_backfill_incidents_resume_from_the_window_they_stopped_at() -> None:
    (item,) = incidents.incidents(view(scenarios.backfilled()))
    assert (item.mode, item.open) == ("backfill", True)
    assert incidents.rerun(item.latest) == (
        "pipeline backfill shop --resource orders --from 2026-10-07T00:00:00+00:00"
    )


# ---- missing data --------------------------------------------------------------------------


@pytest.fixture(scope="module")
def filled() -> View:
    return view(scenarios.backfilled())


def test_every_missing_range_has_its_dates_its_reason_and_its_command(filled) -> None:
    (history,) = gaps.histories(filled)
    assert history.fraction == pytest.approx(0.6)
    found = {item.kind: item for item in history.missing}
    assert set(found) == {"failed", "empty", "off", "twice"}
    failed = found["failed"]
    assert (failed.start, failed.end) == ("2026-10-07T00:00:00+00:00", "2026-10-11T00:00:00+00:00")
    assert "job-fail" in failed.why and "HTTPError" in failed.why
    assert failed.command == (
        "pipeline backfill shop --resource orders "
        "--from 2026-10-07T00:00:00+00:00 --to 2026-10-11T00:00:00+00:00"
    )
    assert found["empty"].start.startswith("2026-10-05") and found["empty"].command
    assert "10,400 at the source" in found["off"].why
    assert found["twice"].command is None  # nothing missing: no command to run


def test_a_cursor_hole_is_found_and_reloaded_where_a_backfill_is_declared(filled) -> None:
    (hole,) = gaps.holes(filled)
    assert (hole.kind, hole.start[11:16], hole.end[11:16]) == ("hole", "04:00", "06:00")
    assert hole.command and hole.command.startswith("pipeline backfill shop --resource orders")


def test_a_hole_without_a_declared_backfill_says_so() -> None:
    built = scenarios.Frames_()
    for index, (low, high) in enumerate(((0, 1), (3, 4))):
        at = NOW - timedelta(hours=5 - index)
        built.add(*scenarios.run("p", at, run_id=f"r{index}"))
        built.add(scenarios.resource("p.t", f"r{index}", at))
        built.add(
            scenarios.load(
                "p.t",
                f"r{index}",
                at,
                1,
                cursor_from=(NOW - timedelta(hours=10 - low)).isoformat(),
                cursor_to=(NOW - timedelta(hours=10 - high)).isoformat(),
            )
        )
    (hole,) = gaps.holes(view(built.frames()))
    assert hole.command is None and "no backfill declared" in hole.why


def test_backfill_jobs_say_what_they_loaded_and_how_to_resume(filled) -> None:
    jobs = {row["run_id"]: row for row in gaps.jobs(filled).iter_rows(named=True)}
    assert (jobs["job-ok"]["windows"], jobs["job-ok"]["resume"]) == (6, None)
    assert jobs["job-failed"]["resume"].endswith("--from 2026-10-07T00:00:00+00:00")


# ---- checks --------------------------------------------------------------------------------


def test_checks_without_their_data_say_what_they_need_and_claim_nothing(hand) -> None:
    report = checks.run(hand)
    assert report.findings == []
    skipped = {check.key: lacks for check, lacks in report.skipped}
    assert skipped["memory.tight"] == ["a memory limit or the host's memory"]
    assert skipped["steps.waiting"] == ["step records"]


def test_checks_find_what_full_records_show_with_evidence() -> None:
    report = checks.run(view(scenarios.scheduled()))
    found = {item.check: item for item in report.findings}
    assert set(found) == {"memory.tight", "memory.killed", "release.slower", "steps.waiting"}
    assert found["memory.tight"].title == "memory at 90% of its container limit"
    assert found["memory.tight"].evidence == "p99 peak 1.8G of 2G, 72 runs"
    assert found["memory.tight"].action == "give it 2.25G"  # p99 1840M + 25%, up to 256M
    assert found["release.slower"].title == "1.1 is 100% slower"
    assert found["steps.waiting"].action == "raise [sources.shop.extract] workers (now 5)"
    assert [item.level for item in report.findings] == ["warning"] * 3 + ["info"]


def test_thresholds_come_from_limits_not_code() -> None:
    relaxed = Limits(memory_tight=0.95, slower=2.0)
    keys = {item.check for item in checks.run(view(scenarios.scheduled()), relaxed).findings}
    assert "memory.tight" not in keys and "release.slower" not in keys


def test_a_new_check_is_one_class(hand) -> None:
    class Always(Check):
        key, title, area, needs = "test.always", "anything", "speed", ("runs.rows",)

        def find(self, view, limits):
            yield Finding(self.key, "info", self.area, "p", "found", "evidence", "act")

    try:
        assert [item.check for item in checks.run(hand).findings] == ["test.always"]
    finally:
        checks.CHECKS.pop()


def test_memory_sizes_from_the_highest_peak_and_rounds_up() -> None:
    records = [
        r
        for index in range(5)
        for r in started_and(
            f"m{index}",
            NOW - timedelta(hours=5) + timedelta(minutes=index),
            "succeeded",
            peak_rss_mb=1000.0 + index,
            memory_limit_mb=2048.0,
        )
    ]
    row = performance.memory(view(frames_of(*records), "24h")).row(0, named=True)
    assert row["top"] == "max"  # five runs are too few for a p99
    assert row["recommended_mb"] == 1280.0  # 1004 x 1.25 = 1255, up to the next 256
    assert row["share"] == pytest.approx(1004 / 2048, abs=0.001)


# ---- triage --------------------------------------------------------------------------------


def statuses(found: View, signals=None) -> dict[str, triage.Status]:
    production = found.mode("run")
    report = checks.run(production)
    missing = gaps.holes(found) + [item for h in gaps.histories(found) for item in h.missing]
    states = triage.statuses(production, incidents.incidents(found), report, missing, signals)
    return {status.pipeline: status for status in states}


def test_triage_fails_only_what_is_failing_now(hand) -> None:
    found = statuses(hand)
    assert found["form4"].level == "error"
    assert {name for name, status in found.items() if status.level == "ok"} == {
        "filings",
        "insider",
        "members",
    }


def test_triage_puts_risks_and_missing_rows_under_at_risk() -> None:
    orders = statuses(view(scenarios.scheduled()))["orders"]
    assert [concern.headline for concern in orders.concerns] == [
        "memory at 90% of its container limit",
        "1 run(s) killed at their memory limit",
        "1.1 is 100% slower",
    ]
    shop = statuses(view(scenarios.backfilled()))["shop"]
    assert shop.level == "warning" and shop.headline.startswith("orders: 4 range(s) missing")


def test_health_adds_only_what_its_thresholds_alone_can_judge(hand) -> None:
    late = Signal("filings", None, "run_stale", "error", "no success for 30h")
    again = Signal("form4", None, "run_failed", "error", "said by the incident already")
    found = statuses(hand, [late, again])
    assert (found["filings"].level, found["filings"].headline) == ("error", "late")
    assert len(found["form4"].concerns) == 1


def test_empty_frames_give_empty_facts_not_errors() -> None:
    empty = view(Frames())
    assert empty.runs.is_empty() and incidents.incidents(empty) == []
    assert gaps.histories(empty) == [] and gaps.holes(empty) == [] and gaps.jobs(empty).is_empty()
    assert checks.run(empty).findings == [] and performance.memory(empty).is_empty()
    assert statuses(empty) == {}


def test_the_frames_real_runs_write_are_read_whole(make_pipeline, tmp_path) -> None:
    from conftest import ORDERS, Upstream

    from dlt_framework.core.monitoring import ParquetSink
    from dlt_framework.core.run import IncrementalRun

    IncrementalRun(make_pipeline({ORDERS: Upstream([{"id": 1, "updated_at": 100}])}))()
    frames = Frames.read(ParquetSink(str(tmp_path / "monitoring")))
    now = datetime.now(UTC)
    found = View.build(frames, Scope.last("1h", now), now)
    assert found.runs.row(0, named=True)["outcome"] == "succeeded"
    assert incidents.incidents(found) == []
    assert "steps.waiting" not in {check.key for check, _ in checks.run(found).skipped}
    assert gaps.jobs(found).is_empty()

"""Every page draws within its width from every shape of frames, and says what it must: the
answer first, the selection visible, and every missing range or failure with what to do."""

from datetime import UTC, datetime, timedelta

import pytest
import scenarios
from conftest import ORDERS, Upstream
from scenarios import NOW

from dashboard.model.facts import Scope, View
from dashboard.pages import PAGES
from dashboard.pages.base import Context
from dashboard.render.text import Block, plain, width
from dlt_framework.core.monitoring import Frames, ParquetSink
from dlt_framework.core.monitoring.health import Signal
from dlt_framework.core.run import IncrementalRun

SHAPES = {
    "hand_runs": scenarios.hand_runs,
    "scheduled": scenarios.scheduled,
    "backfilled": scenarios.backfilled,
    "empty": Frames,
}


def block(
    frames: Frames,
    key: str,
    size: int = 160,
    *,
    since: str = "7d",
    now: datetime = NOW,
    **chosen,
) -> Block:
    view = View.build(frames, Scope.last(since, now), now)
    drawn = PAGES[key].draw(Context(view, size, chosen.pop("signals", None), **chosen))
    assert drawn and max(width(line) for line in drawn) <= size, f"{key} overflows {size}"
    return drawn


def text(frames: Frames, key: str, **chosen) -> list[str]:
    return [line.rstrip() for line in plain(block(frames, key, **chosen)).splitlines()]


@pytest.mark.parametrize("key", list(PAGES))
@pytest.mark.parametrize("shape", list(SHAPES))
@pytest.mark.parametrize("size", [100, 160, 220])
def test_every_page_draws_every_shape_within_its_width(key, shape, size) -> None:
    block(SHAPES[shape](), key, size)


@pytest.mark.parametrize("key", list(PAGES))
def test_every_page_shows_the_filters_in_force(key) -> None:
    filters = text(scenarios.hand_runs(), key)[3]
    assert filters.startswith("2026-10-21 12:00 → 2026-10-28 12:00Z · all pipelines")


def test_the_overview_answers_first_then_says_who_and_what_to_run() -> None:
    lines = text(scenarios.hand_runs(), "overview")
    assert lines[5].startswith("✖ 1 FAILING    █ 3 HEALTHY")
    rows = [line for line in lines if " ago " in line]
    assert [row.split()[2] for row in rows] == ["form4", "filings", "insider", "members"]
    joined = "\n".join(lines)
    assert "ConfigFieldMissingException ×2, stopped (SIGTERM), killed" in joined
    assert "→ fix it, then $ pipeline run form4" in joined
    assert "BACKFILLS  none run in range, and no resource declares one" in joined


def test_the_overview_counts_backfills_apart_from_production() -> None:
    lines = text(scenarios.backfilled(), "overview")
    assert lines[5].startswith("⚠ 1 AT RISK")  # its backfill failed; production runs are fine
    assert any("orders: 4 range(s) missing" in line for line in lines)
    assert any(line.startswith("BACKFILLS  2 job(s) in range, 1 failed") for line in lines)


def test_the_overview_lists_every_problem_but_only_some_healthy_pipelines() -> None:
    built = scenarios.Frames_()
    for index in range(30):
        built.add(*scenarios.run(f"p{index:02d}", NOW - timedelta(hours=1)))
    built.add(
        *scenarios.run("zz", NOW - timedelta(hours=1), "failed", **scenarios.failure("E", "x"))
    )
    lines = text(built.frames(), "overview")
    rows = [line for line in lines if " ago " in line]
    assert len(rows) == 15 and "zz" in rows[0]
    assert any("+16 more healthy or idle: not shown" in line for line in lines)


def test_health_shows_lateness_only_it_can_judge() -> None:
    late = Signal("filings", None, "run_stale", "error", "no success for 30h")
    lines = text(scenarios.hand_runs(), "overview", signals=[late])
    assert lines[5].startswith("✖ 2 FAILING")


def test_the_runs_page_names_the_pipeline_shown_and_what_it_is() -> None:
    lines = text(scenarios.hand_runs(), "runs")
    assert lines[5].startswith("PIPELINE  ✖ form4   FAILING   · 1 of 4")
    joined = "\n".join(lines)
    assert "─ ABOUT " in joined
    chosen = [line for line in lines if "▶" in line]
    assert len(chosen) == 1 and "form4-si" in chosen[0]  # the open incident's latest run
    assert "→ fix the cause, then $ pipeline run form4" in joined
    assert "meaning  signal 15: told to stop from outside" in joined
    assert "no steps, memory samples, loads, tables recorded for this run" in joined


def test_the_runs_page_says_how_each_resource_reads_and_where_its_cursor_moved() -> None:
    lines = text(scenarios.hand_runs(), "runs", focus="insider")
    joined = "\n".join(lines)
    assert "submission" in joined and "cursor updated_at (time)" in joined
    (loaded,) = [
        line for line in lines if line.lstrip("│ ").startswith("submission") and "1,000,000" in line
    ]
    assert "not recorded" in loaded  # these loads stored no start: older monitoring


def test_the_runs_page_opens_the_run_asked_for() -> None:
    lines = text(scenarios.hand_runs(), "runs", focus="insider", run_id="insider-killed-12")
    assert lines[5].startswith("PIPELINE  █ insider")
    (chosen,) = [line for line in lines if "▶" in line]
    assert "killed" in chosen
    assert any("KILLED" in line and "no end reported" in line for line in lines)


def test_the_runs_page_shows_steps_memory_and_loads_where_recorded() -> None:
    joined = "\n".join(text(scenarios.scheduled(), "runs"))
    for title in ("STEPS", "MEMORY", "LOADED", "TABLES"):
        assert f"─ {title} " in joined
    assert "not recorded" not in joined


def test_links_lead_to_a_pipeline_and_to_its_runs() -> None:
    drawn = block(scenarios.hand_runs(), "overview")
    targets = {
        style.split("|")[2]
        for line in drawn
        for _, style in line
        if style and style.count("|") == 2
    }
    assert "pipeline/form4" in targets
    assert "run/form4/form4-signal" in targets


def test_the_backfills_page_lists_each_missing_range_with_its_command() -> None:
    lines = text(scenarios.backfilled(), "backfills")
    joined = "\n".join(lines)
    assert lines[5].startswith("✖ 4 RANGE(S) MISSING in 1 resource(s)")
    assert "60% loaded" in joined
    for kind in ("FAILED", "EMPTY", "OFF", "TWICE", "HOLE"):
        assert any(line.lstrip("│ ").startswith(kind) for line in lines), kind
    assert "$ pipeline backfill shop --resource orders --from 2026-10-07T00:00:00+00:00" in joined
    assert "stopped at 2026-10-07 00:00  → $ pipeline backfill shop --resource orders" in joined


def test_the_backfills_page_says_how_to_start_when_there_are_none() -> None:
    joined = "\n".join(text(scenarios.hand_runs(), "backfills"))
    assert "No resource declares a history" in joined and "TimeBackfill" in joined


def test_the_incidents_page_keeps_open_and_resolved_apart() -> None:
    lines = text(scenarios.hand_runs(), "incidents")
    assert lines[5].startswith("✖ 1 OPEN   2 resolved in range")
    joined = "\n".join(lines)
    assert joined.index("─ OPEN ") < joined.index("form4") < joined.index("─ RESOLVED ")
    assert "lasted" in joined and "killed ×3" in joined
    assert "CAUSES BY DAY" in joined


def test_the_tuning_page_finds_with_evidence_or_says_what_it_could_not_check() -> None:
    full = "\n".join(text(scenarios.scheduled(), "tuning"))
    assert "memory at 90% of its container limit   p99 peak 1.8G of 2G, 72 runs" in full
    assert "→ give it 2.25G" in full
    sparse = "\n".join(text(scenarios.hand_runs(), "tuning"))
    assert "every check that could run found nothing to change" in sparse
    assert "runs close to their memory" in sparse and "needs a memory limit" in sparse


def test_pages_draw_the_frames_real_runs_write(make_pipeline, tmp_path) -> None:
    IncrementalRun(make_pipeline({ORDERS: Upstream([{"id": 1, "updated_at": 100}])}))()
    frames = Frames.read(ParquetSink(str(tmp_path / "monitoring")))
    now = datetime.now(UTC)
    for key in PAGES:
        block(frames, key, 140, since="1h", now=now)
    joined = "\n".join(text(frames, "runs", since="1h", now=now))
    assert "SUCCEEDED" in joined and "extract #1" in joined and "orders" in joined

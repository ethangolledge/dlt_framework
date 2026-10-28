"""Every page draws, at every width, from empty frames, the demo, and real runs' frames."""

from datetime import UTC, datetime

import pytest
from conftest import ORDERS, Upstream

from dashboard import demo
from dashboard.model.facts import Scope, View
from dashboard.pages import PAGES
from dashboard.pages.base import Context
from dashboard.render.text import plain, width
from dlt_framework.core.monitoring import Frames, ParquetSink
from dlt_framework.core.run import IncrementalRun

NOW = datetime(2026, 10, 27, 12, tzinfo=UTC)


def draw(
    frames: Frames, key: str, size: int, now: datetime = NOW, run_id: str | None = None
) -> str:
    view = View.build(frames, Scope.last("7d", now), now)
    block = PAGES[key].draw(Context(view, size, None, run_id))
    assert block and max(width(line) for line in block) <= size, f"{key} overflows {size}"
    return plain(block)


@pytest.mark.parametrize("key", list(PAGES))
@pytest.mark.parametrize("size", [100, 160, 220])
def test_every_page_draws_the_demo_within_its_width(key, size) -> None:
    assert draw(demo.frames(NOW), key, size)


@pytest.mark.parametrize("key", list(PAGES))
def test_every_page_says_no_signal_on_empty_frames(key) -> None:
    text = draw(Frames(), key, 120)
    assert "NO SIGNAL" in text or "✔" in text


def test_pages_draw_the_frames_real_runs_write(make_pipeline, tmp_path) -> None:
    IncrementalRun(make_pipeline({ORDERS: Upstream([{"id": 1, "updated_at": 100}])}))()
    frames = Frames.read(ParquetSink(str(tmp_path / "monitoring")))
    now = datetime.now(UTC)
    for key in PAGES:
        draw(frames, key, 140, now)
    run_id = frames.runs["run_id"][0]
    text = draw(frames, "run", 140, now, run_id)
    assert "SUCCEEDED" in text and "extract #1" in text and "orders" in text


def test_the_run_page_leads_with_what_broke_and_what_to_run(week_frames) -> None:
    text = draw(week_frames, "run", 160)  # with none chosen: the latest failure
    lines = [line.rstrip() for line in text.splitlines()]
    assert lines[3].startswith("✖ FAILED  scrape_filings  in extract · filings · HTTPError")
    assert lines[4].endswith("then $ pipeline run scrape_filings")
    assert any("TRACEBACK" in line for line in lines)


def test_the_overview_answers_first_then_says_who_and_what_next(week_frames) -> None:
    lines = draw(week_frames, "overview", 160).splitlines()
    assert lines[3].startswith("✖ 1 FAILING    ⚠ 2 AT RISK    █ 1 HEALTHY")
    rows = [line for line in lines if "ago" in line]
    assert [row.split()[2] for row in rows] == [
        "scrape_filings",
        "erp_sql",
        "orders_api",
        "files_landing",
    ]
    assert "1 scrape_filings" in draw(week_frames, "overview", 160)


def test_the_overview_shows_health_when_thresholds_apply(week_frames) -> None:
    from dlt_framework.core.monitoring.health import Signal

    view = View.build(week_frames, Scope.last("7d", NOW), NOW)
    signal = Signal("files_landing", None, "run_stale", "error", "no success for 30h")
    text = plain(PAGES["overview"].draw(Context(view, 160, [signal])))
    assert "✖ 2 FAILING" in text and "late" in text


def test_the_pipeline_page_tells_one_pipelines_story(week_frames) -> None:
    view = View.build(week_frames, Scope.last("7d", NOW), NOW)
    block = PAGES["pipeline"].draw(Context(view, 160, None, focus="erp_sql"))
    text = plain(block)
    assert text.splitlines()[3].startswith("⚠ AT RISK  memory at 92%")
    assert "→ give it 4.8G" in text and "LATEST RUNS" in text
    runs = {
        style.split("|")[2] for line in block for _, style in line if style and "|run/" in style
    }
    assert runs <= {f"run/{run_id}" for run_id in view.runs["run_id"]} and runs


@pytest.fixture(scope="module")
def week_frames() -> Frames:
    return demo.frames(NOW)

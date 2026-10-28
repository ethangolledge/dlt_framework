"""The frontends, reading frames through a monitoring sink as they would in use: the terminal
command, the static export, and the web app's smoke test."""

from pathlib import Path

import pytest
import scenarios

from dashboard.app import terminal
from dashboard.pages import PAGES
from dlt_framework.core.monitoring import ParquetSink
from dlt_framework.core.monitoring.records import RECORDS


@pytest.fixture
def uri(tmp_path: Path) -> str:
    """A Parquet monitoring folder holding the hand-run and backfill scenarios."""
    folder = tmp_path / "monitoring"
    sink = ParquetSink(str(folder))
    for frames in (scenarios.hand_runs(), scenarios.backfilled()):
        for record in RECORDS:
            frame = getattr(frames, record.name)
            if frame.height:
                sink.write(record.name, frame)
    return f"file://{folder}"


def show(uri: str, *args: str) -> list[str]:
    argv = ["show", *args, "--uri", uri, "--since", "all", "--width", "140", "--no-colour"]
    assert terminal.main(argv) == 0
    return []


@pytest.mark.parametrize("key", list(PAGES))
def test_the_terminal_shows_every_page(key, uri, capsys) -> None:
    show(uri, key)
    out = capsys.readouterr().out
    assert "DLT//OPS" in out and "\x1b[" not in out


def test_the_terminal_shows_the_pipeline_and_run_it_is_given(uri, capsys) -> None:
    show(uri, "runs", "insider", "--run", "insider-killed-12")
    out = capsys.readouterr().out
    assert "[insider]" in out and "KILLED" in out


def test_colour_is_on_when_asked(uri, capsys) -> None:
    terminal.main(["show", "--uri", uri, "--width", "100", "--colour"])
    assert "\x1b[38;2;" in capsys.readouterr().out


def test_export_writes_every_page_and_every_page_a_link_leads_to(uri, tmp_path, capsys) -> None:
    site = tmp_path / "site"
    assert terminal.main(["export", str(site), "--uri", uri, "--since", "all"]) == 0
    written = {path.name for path in site.iterdir()}
    assert {"index.html", *(f"{key}.html" for key in PAGES if key != "overview")} <= written
    assert {"runs-form4.html", "runs-form4-form4-signal.html"} <= written
    for page in written:
        html = (site / page).read_text()
        linked = {part.split('"', 1)[0] for part in html.split('href="')[1:]}
        assert {link for link in linked if not link.startswith("https://")} <= written, page


def test_the_terminal_reads_the_environment_monitoring_uri(make_pipeline, capsys) -> None:
    from conftest import ORDERS, Upstream

    from dlt_framework.core.run import IncrementalRun

    IncrementalRun(make_pipeline({ORDERS: Upstream([{"id": 1, "updated_at": 100}])}))()
    assert terminal.main(["show", "--width", "120", "--no-colour", "--since", "1h"]) == 0
    assert "shop" in capsys.readouterr().out


def test_the_web_app_draws_each_page(uri) -> None:
    testing = pytest.importorskip("streamlit.testing.v1")

    import dashboard.app.web as web_module

    app = testing.AppTest.from_file(str(Path(web_module.__file__)), default_timeout=60)
    app.run()
    assert not app.exception

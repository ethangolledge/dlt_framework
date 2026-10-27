"""The frontends: the terminal command, the demo folder, and the web app's smoke test."""

import pytest

from dashboard.app import terminal
from dashboard.pages import PAGES
from dlt_framework.core.monitoring import Frames, ParquetSink


@pytest.mark.parametrize("key", list(PAGES))
def test_the_terminal_shows_every_page(key, capsys) -> None:
    assert (
        terminal.main(["show", key, "--demo", "--width", "120", "--since", "7d", "--no-colour"])
        == 0
    )
    out = capsys.readouterr().out
    assert "DLT//OPS" in out and "\x1b[" not in out


def test_colour_is_on_when_asked(capsys) -> None:
    terminal.main(["show", "--demo", "--width", "100", "--colour"])
    assert "\x1b[38;2;" in capsys.readouterr().out


def test_export_writes_a_linked_page_for_each_page(tmp_path, capsys) -> None:
    assert terminal.main(["export", str(tmp_path / "site"), "--demo"]) == 0
    written = sorted(path.name for path in (tmp_path / "site").iterdir())
    assert written == sorted(["index.html", *(f"{key}.html" for key in PAGES if key != "overview")])
    page = (tmp_path / "site" / "runs.html").read_text()
    assert '<a class="on" href="runs.html">' in page and '<pre class="term"' in page


def test_the_demo_folder_reads_back_through_a_sink(tmp_path, capsys) -> None:
    assert terminal.main(["demo", str(tmp_path / "demo")]) == 0
    frames = Frames.read(ParquetSink(str(tmp_path / "demo")))
    assert frames.runs.height > 100 and frames.profiles.height > 0
    assert "MONITORING__URI=file://" in capsys.readouterr().out


def test_the_terminal_reads_the_environment_monitoring_uri(make_pipeline, capsys) -> None:
    from conftest import ORDERS, Upstream

    from dlt_framework.core.run import IncrementalRun

    IncrementalRun(make_pipeline({ORDERS: Upstream([{"id": 1, "updated_at": 100}])}))()
    assert terminal.main(["show", "runs", "--width", "120", "--no-colour", "--since", "1h"]) == 0
    assert "shop" in capsys.readouterr().out


def test_the_web_app_draws_each_page() -> None:
    testing = pytest.importorskip("streamlit.testing.v1")
    from pathlib import Path

    import dashboard.app.web as web_module

    app = testing.AppTest.from_file(str(Path(web_module.__file__)), default_timeout=60)
    app.run()
    assert not app.exception

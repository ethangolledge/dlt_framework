"""The command line, reading frames through a monitoring sink as it would in use."""

from pathlib import Path

import polars as pl
import pytest
import scenarios

from dashboard.app import terminal
from dlt_framework.core.monitoring import Frames, ParquetSink
from dlt_framework.core.monitoring.records import RECORDS


def sink(folder: Path, *parts: Frames) -> str:
    """A Parquet monitoring folder holding ``parts``; its URI."""
    writer = ParquetSink(str(folder))
    for record in RECORDS:
        frame = pl.concat([getattr(part, record.name) for part in parts], how="diagonal_relaxed")
        if frame.height:
            writer.write(record.name, frame)
    return f"file://{folder}"


@pytest.fixture
def uri(tmp_path: Path) -> str:
    return sink(tmp_path / "monitoring", scenarios.hand_runs(), scenarios.backfilled())


def test_status_lists_every_pipeline_worst_first_and_fails_while_one_fails(uri, capsys) -> None:
    assert terminal.main(["status", "--uri", uri, "--no-colour"]) == 1
    lines = capsys.readouterr().out.splitlines()
    assert lines[1].startswith("✖ 1 FAILING")
    assert lines[2].startswith("✖ form4") and "→ $ pipeline run form4" in lines[2]
    names = {line.split()[1] for line in lines[2:]}
    assert names == {"form4", "shop", "filings", "insider", "members"}


def test_status_succeeds_when_nothing_fails(tmp_path, capsys) -> None:
    healthy = scenarios.Frames_().add(*scenarios.run("p", scenarios.NOW)).frames()
    assert terminal.main(["status", "--uri", sink(tmp_path / "m", healthy), "--no-colour"]) == 0
    assert "█ 1 HEALTHY" in capsys.readouterr().out


def test_run_shows_how_one_run_ended_and_what_to_run(uri, capsys) -> None:
    assert terminal.main(["run", "form4-si", "--uri", uri, "--no-colour"]) == 0
    out = capsys.readouterr().out
    assert "FAILED  stopped by SIGTERM" in out and "$ pipeline run form4" in out


def test_run_asks_for_more_of_an_id_that_is_not_unique(uri, capsys) -> None:
    assert terminal.main(["run", "form4", "--uri", uri]) == 2
    assert "give more of the id" in capsys.readouterr().err


def test_colour_is_on_when_asked(uri, capsys) -> None:
    terminal.main(["status", "--uri", uri, "--colour"])
    assert "\x1b[38;2;" in capsys.readouterr().out


def test_build_writes_the_dashboard(uri, tmp_path, capsys) -> None:
    assert terminal.main(["build", str(tmp_path / "site"), "--uri", uri]) == 0
    assert (tmp_path / "site" / "index.html").exists()
    assert "open it in a browser" in capsys.readouterr().out


def test_commands_read_the_environment_monitoring_uri(make_pipeline, capsys) -> None:
    from conftest import ORDERS, Upstream

    from dlt_framework.core.run import IncrementalRun

    IncrementalRun(make_pipeline({ORDERS: Upstream([{"id": 1, "updated_at": 100}])}))()
    assert terminal.main(["status", "--no-colour"]) == 0
    assert "shop" in capsys.readouterr().out

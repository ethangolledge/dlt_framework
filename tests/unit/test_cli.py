import logging

import pytest

from pipeline.core.errors import ConfigurationError
from pipeline.execution import run_pipeline


def test_main_runs_one_discovered_source(monkeypatch) -> None:
    definition = object()
    config = object()

    monkeypatch.setattr(run_pipeline, "discover_source", lambda selector: definition)
    monkeypatch.setattr(run_pipeline, "load_pipeline_config", lambda source: config)
    monkeypatch.setattr(run_pipeline, "run", lambda value: "loaded")

    assert run_pipeline.main(["accounts"]) == 0


def test_main_rejects_multiple_source_arguments() -> None:
    with pytest.raises(SystemExit):
        run_pipeline.main(["accounts", "orders"])


def test_main_reports_framework_error(monkeypatch, caplog) -> None:
    def fail(selector):
        raise ConfigurationError("bad configuration")

    monkeypatch.setattr(run_pipeline, "discover_source", fail)

    with caplog.at_level(logging.ERROR):
        result = run_pipeline.main(["accounts"])

    assert result == 2
    assert "bad configuration" in caplog.text


def test_main_reports_pipeline_error(monkeypatch, caplog) -> None:
    monkeypatch.setattr(run_pipeline, "discover_source", lambda selector: object())
    monkeypatch.setattr(run_pipeline, "load_pipeline_config", lambda source: object())

    def fail(config):
        raise RuntimeError("destination unavailable")

    monkeypatch.setattr(run_pipeline, "run", fail)

    with caplog.at_level(logging.ERROR):
        result = run_pipeline.main(["accounts"])

    assert result == 1
    assert "destination unavailable" in caplog.text

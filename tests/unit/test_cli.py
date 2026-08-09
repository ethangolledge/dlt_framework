import json
import logging

import dlt
import pytest

from dlt_framework.core.errors import (
    ConfigurationError,
    TerminalRunError,
    TransientRunError,
)
from dlt_framework.core.models import ResourcePolicy, RunSummary
from dlt_framework.execution import run_pipeline


def summary() -> RunSummary:
    return RunSummary("accounts", None, 1, 1)


def test_main_runs_one_discovered_source(monkeypatch) -> None:
    definition = object()
    config = object()
    monkeypatch.setattr(run_pipeline, "discover_source", lambda selector: definition)
    monkeypatch.setattr(run_pipeline, "load_pipeline_config", lambda source, **kwargs: config)
    monkeypatch.setattr(run_pipeline, "run", lambda value: summary())

    assert run_pipeline.main(["run", "accounts"]) == 0


def test_legacy_command_is_supported_with_warning(monkeypatch) -> None:
    monkeypatch.setattr(run_pipeline, "discover_source", lambda selector: object())
    monkeypatch.setattr(run_pipeline, "load_pipeline_config", lambda source, **kwargs: object())
    monkeypatch.setattr(run_pipeline, "run", lambda value: summary())

    with pytest.warns(DeprecationWarning, match="explicit command"):
        assert run_pipeline.main(["accounts"]) == 0


def test_main_rejects_multiple_source_arguments() -> None:
    with pytest.raises(SystemExit):
        run_pipeline.main(["run", "accounts", "orders"])


def test_main_passes_resource_and_backfill_to_configuration(monkeypatch) -> None:
    captured = {}
    monkeypatch.setattr(run_pipeline, "discover_source", lambda selector: object())

    def load(source, **kwargs):
        captured.update(kwargs)
        return object()

    monkeypatch.setattr(run_pipeline, "load_pipeline_config", load)
    monkeypatch.setattr(run_pipeline, "run", lambda config: summary())

    result = run_pipeline.main(
        [
            "backfill",
            "accounts",
            "--resource",
            "events",
            "--from",
            "2026-01-01",
            "--to",
            "2026-01-03",
            "--chunksize",
            "P1D",
        ]
    )

    assert result == 0
    assert captured["resource"] == "events"
    assert str(captured["backfill"].chunk_size) == "P1D"


def test_backfill_requires_all_bounds() -> None:
    with pytest.raises(SystemExit):
        run_pipeline.main(["backfill", "accounts", "--resource", "events", "--from", "2026-01-01"])


def test_main_reports_framework_error(monkeypatch, caplog) -> None:
    def fail(selector):
        raise ConfigurationError("bad configuration")

    monkeypatch.setattr(run_pipeline, "discover_source", fail)
    with caplog.at_level(logging.ERROR):
        result = run_pipeline.main(["run", "accounts"])

    assert result == 2
    assert "bad configuration" in caplog.text


@pytest.mark.parametrize(
    ("error", "exit_code"),
    [(TerminalRunError("bad data"), 3), (TransientRunError("offline"), 4)],
)
def test_main_returns_classified_pipeline_exit_codes(monkeypatch, error, exit_code) -> None:
    monkeypatch.setattr(run_pipeline, "discover_source", lambda selector: object())
    monkeypatch.setattr(run_pipeline, "load_pipeline_config", lambda source, **kwargs: object())

    def fail(config):
        raise error

    monkeypatch.setattr(run_pipeline, "run", fail)
    assert run_pipeline.main(["run", "accounts"]) == exit_code


def test_list_command_prints_resource_contract(monkeypatch, capsys) -> None:
    @dlt.source
    def factory():
        return dlt.resource(
            [{"id": 1}],
            name="orders",
            write_disposition="merge",
            primary_key="id",
        )

    definition = type(
        "Definition",
        (),
        {"resources": {"orders": ResourcePolicy(empty="fail", backfill=True)}},
    )()
    monkeypatch.setattr(run_pipeline, "discover_source", lambda selector: definition)
    monkeypatch.setattr(run_pipeline, "instantiate_source", lambda definition: factory())

    assert run_pipeline.main(["list", "accounts"]) == 0
    assert capsys.readouterr().out.strip() == (
        "orders\twrite=merge\tprimary_key=id\tempty=fail\tbackfill=yes"
    )


def test_validate_command_does_not_run_extraction(monkeypatch) -> None:
    source = type("Source", (), {"selected_resources": {"orders": object()}})()
    monkeypatch.setattr(run_pipeline, "discover_source", lambda selector: object())
    monkeypatch.setattr(run_pipeline, "load_pipeline_config", lambda source, **kwargs: object())
    monkeypatch.setattr(run_pipeline, "validate", lambda config: source)
    monkeypatch.setattr(
        run_pipeline,
        "run",
        lambda config: pytest.fail("validate must not run extraction"),
    )

    assert run_pipeline.main(["validate", "accounts", "--resource", "orders"]) == 0


def test_unexpected_error_returns_one_with_traceback(monkeypatch, caplog) -> None:
    monkeypatch.setattr(run_pipeline, "discover_source", lambda selector: object())
    monkeypatch.setattr(run_pipeline, "load_pipeline_config", lambda source, **kwargs: object())

    def fail(config):
        raise RuntimeError("programming error")

    monkeypatch.setattr(run_pipeline, "run", fail)
    with caplog.at_level(logging.ERROR):
        assert run_pipeline.main(["run", "accounts"]) == 1
    assert "programming error" in caplog.text


def test_json_formatter_emits_structured_context() -> None:
    record = logging.LogRecord("framework", logging.INFO, __file__, 1, "loaded", (), None)
    record.event = "pipeline_succeeded"
    record.resource = "orders"

    payload = json.loads(run_pipeline.JsonFormatter().format(record))

    assert payload["message"] == "loaded"
    assert payload["event"] == "pipeline_succeeded"
    assert payload["resource"] == "orders"

import json
import logging
import textwrap

import pytest
from dlt.common.exceptions import TerminalValueError

from dlt_framework.cli import JsonFormatter, main
from dlt_framework.core.errors import TerminalRunError, TransientRunError
from dlt_framework.core.retry import Retry

DEFINITION = """
from dlt_framework.core import (
    FREEZE_DATA_TYPES, Merge, Pipeline, Resource, Retry, Target, ValueBackfill, ValueCursor,
)
from dlt_framework.sources.custom import CustomSource

def orders(incremental, window):
    yield [{"id": 1, "updated_at": 100}]

ORDERS = Resource(
    name="orders",
    write=Merge(primary_key=("id",), strategy="delete-insert"),
    cursor=ValueCursor(path="updated_at", start=50, on_missing="raise"),
    empty="warn",
    backfill=ValueBackfill(start=0, every=25),
)

Pipeline(
    name="shop",
    target=Target("duckdb", dataset="raw_shop"),
    schema_contract=FREEZE_DATA_TYPES,
    retry=Retry(attempts=1),
    source=CustomSource(name="shop", resources={ORDERS: orders}),
)
"""


@pytest.fixture
def package(tmp_path, monkeypatch) -> str:
    root = tmp_path / "client" / "pipelines"
    root.mkdir(parents=True)
    (tmp_path / "client" / "__init__.py").write_text("")
    (root / "__init__.py").write_text("")
    (root / "shop.py").write_text(textwrap.dedent(DEFINITION))
    monkeypatch.syspath_prepend(str(tmp_path))
    return "client"


def test_list_check_run_and_backfill(package, capsys) -> None:
    assert main(["--package", package, "list"]) == 0
    listed = capsys.readouterr().out
    assert "shop\tCustomSource\torders\tMerge\tcursor=updated_at\tbackfill=ValueBackfill" in listed
    assert main(["--package", package, "check", "shop"]) == 0
    assert main(["--package", package, "run", "shop"]) == 0
    assert main(["--package", package, "backfill", "shop"]) == 0
    assert (
        main(["--package", package, "backfill", "shop", "--resource", "orders", "--from", "25"])
        == 0
    )


@pytest.mark.parametrize(
    "arguments",
    [
        ["run", "missing"],
        ["backfill", "shop", "--from", "25"],
        ["backfill", "shop", "--resource", "x"],
    ],
)
def test_definition_and_input_errors_exit_2(package, arguments) -> None:
    assert main(["--package", package, *arguments]) == 2


def test_default_package_discovers_shipped_pipelines(capsys) -> None:
    assert main(["list"]) == 0
    assert "dummyjson\tRestApiSource\tproducts\tReplace" in capsys.readouterr().out


def test_json_logs_include_extra_fields() -> None:
    record = logging.makeLogRecord({"msg": "done", "pipeline": "shop", "levelname": "INFO"})
    assert json.loads(JsonFormatter().format(record))["pipeline"] == "shop"


def test_retry_classifies_terminal_and_transient_errors() -> None:
    retry = Retry(attempts=2, min_wait=0, max_wait=0)

    def fail(error: Exception):
        def step() -> None:
            raise error

        return step

    with pytest.raises(TerminalRunError):
        retry("extract", fail(TerminalValueError("bad config")))
    with pytest.raises(TransientRunError):
        retry("load", fail(ConnectionError("reset")))
    with pytest.raises(TerminalRunError):
        retry("load", fail(ConnectionError("permission denied")))
    assert retry("load", lambda: 42) == 42

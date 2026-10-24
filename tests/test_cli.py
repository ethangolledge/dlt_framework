import pytest
from dlt.common.exceptions import TerminalValueError

from dlt_framework.cli import main
from dlt_framework.core.errors import TerminalRunError, TransientRunError
from dlt_framework.core.monitoring.coverage import Coverage
from dlt_framework.core.pipeline import Pipeline
from dlt_framework.core.retry import Retry

DEFINITION = """
from dlt_framework.core import (
    FREEZE_DATA_TYPES, Integer, Merge, Pipeline, Resource, Target, ValueBackfill, ValueCursor,
)
from dlt_framework.sources.custom import CustomSource

def orders(incremental, window):
    yield [{"id": 1, "updated_at": 100}]

ORDERS = Resource(
    name="orders",
    write=Merge(primary_key=("id",), strategy="delete-insert"),
    cursor=ValueCursor(path="updated_at", start=50, format=Integer(), on_missing="raise"),
    empty="warn",
    backfill=ValueBackfill(start=0, every=25),
)

Pipeline(
    name="shop",
    target=Target("duckdb", dataset="raw_shop"),
    schema_contract=FREEZE_DATA_TYPES,
    source=CustomSource(name="shop", resources={ORDERS: orders}),
)
"""


@pytest.fixture
def package(write_package) -> str:
    return write_package("client", "shop", DEFINITION)


def test_list_check_run_status_and_backfill(package) -> None:
    for command in (
        ["list"],
        ["check", "shop"],
        ["run", "shop", "--resource", "orders"],
        ["backfill", "shop"],
        ["backfill", "shop", "--resource", "orders", "--from", "25", "--to", "50"],
        ["status", "shop"],
    ):
        assert main(["--package", package, *command]) == 0


@pytest.mark.parametrize(
    "arguments",
    [
        ["run", "missing"],
        ["run", "shop", "--resource", "x"],
        ["backfill", "shop", "--from", "25"],
        ["backfill", "shop", "--resource", "x"],
        ["backfill", "shop", "--resource", "orders", "--from", "soon"],
        ["backfill", "shop", "--missing", "--to", "50"],
    ],
)
def test_definition_and_input_errors_exit_2(package, arguments) -> None:
    assert main(["--package", package, *arguments]) == 2


def test_retry_classifies_terminal_and_transient_errors() -> None:
    retry = Retry(attempts=2, initial_wait_s=0, max_wait_s=0, give_up_after_s=60)

    def fail(error: Exception):
        def step() -> None:
            raise error

        return step

    with pytest.raises(TerminalRunError):
        retry("extract", fail(TerminalValueError("bad config")))
    with pytest.raises(TransientRunError):
        retry("load", fail(ConnectionError("reset")))
    with pytest.raises(TerminalRunError):
        retry("load", fail(PermissionError("denied")))
    with pytest.raises(TerminalRunError):
        retry("sync", fail(ModuleNotFoundError("No module named 'duckdb'", name="duckdb")))
    assert retry("load", lambda: 42) == 42


def test_retry_gives_up_after_its_time_limit() -> None:
    """A long outage ends on give_up_after_s, not only on the attempt count."""
    calls = []

    def down() -> None:
        calls.append(1)
        raise ConnectionError("database is not accepting connections")

    retry = Retry(attempts=1000, initial_wait_s=0.05, max_wait_s=0.05, give_up_after_s=0.2)
    with pytest.raises(TransientRunError):
        retry("extract", down)
    assert 1 < len(calls) < 1000


def test_backfill_missing_loads_only_the_gaps(package, frames) -> None:
    """Declared history is [0, 50) in steps of 25; loading [25, 50) leaves [0, 25) missing."""
    partial = ["backfill", "shop", "--resource", "orders", "--from", "25"]
    assert main(["--package", package, *partial]) == 0
    orders = next(r for r in Pipeline.get("shop").source.resources if r.name == "orders")

    def coverage() -> Coverage:
        return Coverage.of(frames("loads"), "shop.orders", orders.backfill, orders.cursor)

    assert coverage().gaps == [(0, 25)]
    assert main(["--package", package, "backfill", "shop", "--missing"]) == 0
    assert coverage().gaps == []
    windows = frames("loads").select("window_start", "window_end").rows()
    assert sorted(windows) == [("0", "25"), ("25", "50")]  # each window loaded once


NEEDS_A_MISSING_LIBRARY = """
from dlt_framework.core import FREEZE_DATA_TYPES, Pipeline, Replace, Resource, Target
from dlt_framework.sources.custom import CustomSource

class GeoSource(CustomSource):
    def requires(self):
        return {"definitely_not_installed": "geo"}

def places(incremental, window):
    yield [{"id": 1}]

Pipeline(
    name="places",
    target=Target("duckdb", dataset="raw_places"),
    schema_contract=FREEZE_DATA_TYPES,
    source=GeoSource(
        name="places",
        resources={Resource(name="places", write=Replace(), cursor=None, empty="fail"): places},
    ),
)
"""


@pytest.mark.parametrize("command", ["check", "run", "backfill"])
def test_a_pipeline_missing_a_library_is_refused_before_it_starts(write_package, command) -> None:
    package = write_package("partial", "places", NEEDS_A_MISSING_LIBRARY)
    assert main(["--package", package, command, "places"]) == 2


def test_a_pipeline_whose_module_cannot_load_is_refused(write_package) -> None:
    package = write_package("partial", "scraper", "import not_installed_library\n")
    assert main(["--package", package, "run", "scraper"]) == 2


def test_a_broken_import_inside_the_framework_is_a_bug(write_package) -> None:
    package = write_package("buggy", "shop", "import dlt_framework.core.no_such_module\n")
    assert main(["--package", package, "list"]) == 1

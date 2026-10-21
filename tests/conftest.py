from collections.abc import Callable, Iterator
from typing import Any

import duckdb
import pytest

from dlt_framework.core import (
    FREEZE_DATA_TYPES,
    Merge,
    Pipeline,
    Resource,
    Retry,
    Target,
    ValueBackfill,
    ValueCursor,
)
from dlt_framework.core.windows import RowWindow
from dlt_framework.sources.custom import CustomSource


@pytest.fixture(autouse=True)
def isolated(tmp_path, monkeypatch) -> Iterator[None]:
    """Every test gets its own dlt working dir, DuckDB file, and empty pipeline registry."""
    monkeypatch.setenv("DLT_DATA_DIR", str(tmp_path / "dlt"))
    monkeypatch.setenv("DESTINATION__DUCKDB__CREDENTIALS", str(tmp_path / "warehouse.duckdb"))
    monkeypatch.setenv("RUNTIME__LOG_LEVEL", "WARNING")
    Pipeline.registry.clear()
    yield
    Pipeline.registry.clear()


class Upstream:
    """A fake API: holds rows, filters and pages them like a real API, and records every request."""

    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self.rows = rows
        self.requests: list[tuple[Any, Any]] = []

    def __call__(self, incremental: Any, window: Any) -> Iterator[list[dict[str, Any]]]:
        if isinstance(window, RowWindow):
            self.requests.append(("rows", window.offset))
            yield self.rows[window.offset : window.offset + window.size]
        elif incremental is None:
            self.requests.append((None, None))
            yield list(self.rows)
        else:
            start, end = incremental.start_value, incremental.end_value
            self.requests.append((start, end))
            yield [
                row
                for row in self.rows
                if row["updated_at"] is None
                or (row["updated_at"] >= start and (end is None or row["updated_at"] < end))
            ]


ORDERS = Resource(
    name="orders",
    write=Merge(primary_key=("id",), strategy="delete-insert"),
    cursor=ValueCursor(path="updated_at", start=100, on_missing="raise"),
    empty="warn",
    backfill=ValueBackfill(start=0, every=50),
)


@pytest.fixture
def make_pipeline() -> Callable[..., Pipeline]:
    def make(
        resources: dict[Resource, Upstream], name: str = "shop", source: str = "shop"
    ) -> Pipeline:
        return Pipeline(
            name=name,
            target=Target("duckdb", dataset="raw_shop"),
            schema_contract=FREEZE_DATA_TYPES,
            source=CustomSource(name=source, resources=resources),
            retry=Retry(attempts=1),
        )

    return make


@pytest.fixture
def query(tmp_path) -> Callable[[str], list[tuple[Any, ...]]]:
    def run(sql: str) -> list[tuple[Any, ...]]:
        with duckdb.connect(str(tmp_path / "warehouse.duckdb")) as connection:
            return connection.execute(sql).fetchall()

    return run

import sys
import textwrap
import threading
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

import duckdb
import polars as pl
import pytest

from dlt_framework.core import (
    FREEZE_DATA_TYPES,
    DuckDb,
    Integer,
    Merge,
    Pipeline,
    Resource,
    ValueBackfill,
    ValueCursor,
)
from dlt_framework.core.monitoring import ParquetSink
from dlt_framework.core.windows import RowWindow
from dlt_framework.sources.custom import CustomSource


@pytest.fixture(autouse=True)
def isolated(tmp_path, monkeypatch) -> Iterator[None]:
    """Every test gets its own dlt working dir, DuckDB file, monitoring folder, and registry.

    Every other setting comes from the repo's .dlt/config.toml, so the suite also proves that file
    states each required key. Retries are one immediate attempt, as an environment would state.
    """
    monkeypatch.setenv("DLT_DATA_DIR", str(tmp_path / "dlt"))
    monkeypatch.setenv("DESTINATION__DUCKDB__CREDENTIALS", str(tmp_path / "warehouse.duckdb"))
    monkeypatch.setenv("MONITORING__URI", f"file://{tmp_path / 'monitoring'}")
    monkeypatch.setenv("RUNTIME__LOG_LEVEL", "WARNING")
    for key, value in RETRY.items():
        monkeypatch.setenv(f"RETRY__{key.upper()}", str(value))
    Pipeline.registry.clear()
    Pipeline.unavailable.clear()
    yield
    Pipeline.registry.clear()
    Pipeline.unavailable.clear()


RETRY = {"attempts": 1, "initial_wait_s": 0, "max_wait_s": 0, "give_up_after_s": 60}


@pytest.fixture
def sink(tmp_path) -> ParquetSink:
    return ParquetSink(str(tmp_path / "monitoring"))


@pytest.fixture
def frames(tmp_path) -> Callable[[str], pl.DataFrame]:
    """Read back a monitoring frame (``runs``, ``loads``, ``samples``) written by a test."""

    def read(name: str) -> pl.DataFrame:
        return pl.read_parquet(tmp_path / "monitoring" / name / "*.parquet").sort("at")

    return read


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
    cursor=ValueCursor(path="updated_at", start=100, format=Integer(), on_missing="raise"),
    empty="warn",
    backfill=ValueBackfill(start=0, every=50),
)


@pytest.fixture
def make_pipeline() -> Callable[..., Pipeline]:
    def make(
        resources: dict[Resource, Upstream],
        name: str = "shop",
        source: str = "shop",
        dataset: str = "raw_shop",
    ) -> Pipeline:
        return Pipeline(
            name=name,
            target=DuckDb(dataset=dataset),
            schema_contract=FREEZE_DATA_TYPES,
            source=CustomSource(name=source, resources=resources),
        )

    return make


@pytest.fixture
def query(tmp_path) -> Callable[[str], list[tuple[Any, ...]]]:
    def run(sql: str) -> list[tuple[Any, ...]]:
        with duckdb.connect(str(tmp_path / "warehouse.duckdb")) as connection:
            return connection.execute(sql).fetchall()

    return run


@pytest.fixture
def write_package(tmp_path, monkeypatch) -> Callable[[str, str, str], str]:
    """Writes ``<package>/pipelines/<module>.py`` holding ``source`` for ``--package``.

    Imported afresh on discovery, so its pipelines register again after the registry is cleared.
    """

    def write(package: str, module: str, source: str) -> str:
        root = tmp_path / package / "pipelines"
        root.mkdir(parents=True)
        (tmp_path / package / "__init__.py").write_text("")
        (root / "__init__.py").write_text("")
        (root / f"{module}.py").write_text(textwrap.dedent(source))
        monkeypatch.syspath_prepend(str(tmp_path))
        for name in [name for name in sys.modules if name.split(".")[0] == package]:
            monkeypatch.delitem(sys.modules, name)
        return package

    return write


@dataclass
class Site:
    """A local web server: ``pages`` maps a path to ``(status, body)``; ``requests`` records
    every ``(method, path, body)`` it received, and ``headers`` each request's headers."""

    url: str = ""
    pages: dict[str, tuple[int, str]] = field(default_factory=dict)
    requests: list[tuple[str, str, bytes]] = field(default_factory=list)
    headers: list[dict[str, str]] = field(default_factory=list)

    def paths(self, method: str = "GET") -> list[str]:
        return [path for verb, path, _ in self.requests if verb == method and path != "/robots.txt"]


@pytest.fixture
def site() -> Iterator[Site]:
    served = Site()

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            self._reply()

        def do_POST(self) -> None:
            self._reply()

        def _reply(self) -> None:
            body = self.rfile.read(int(self.headers.get("Content-Length") or 0))
            served.requests.append((self.command, self.path, body))
            served.headers.append(dict(self.headers))
            status, text = served.pages.get(self.path, (404, "not found"))
            data = text.encode()
            self.send_response(status)
            kind = "application/json" if text.startswith("{") else "text/html"
            self.send_header("Content-Type", kind)
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def log_message(self, *args: Any) -> None:
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    served.url = f"http://127.0.0.1:{server.server_port}"
    threading.Thread(target=server.serve_forever, daemon=True).start()
    yield served
    server.shutdown()

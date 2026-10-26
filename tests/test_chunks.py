"""Chunked increments: one large increment loads as consecutive bounded loads, each committing
its rows and cursor, for every source type and cursor format, and lands every row once."""

import json
import os
import sqlite3
from collections.abc import Callable, Iterator
from contextlib import closing
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from typing import Any
from urllib.parse import parse_qs, urlparse

import pyarrow as pa
import pytest
import requests
import sqlalchemy
from conftest import Site
from requests.adapters import BaseAdapter
from test_scrape import listing, spider
from test_sources import BACKENDS, define

from dlt_framework.core import (
    Append,
    AwareDatetime,
    DateOnly,
    Integer,
    IsoTimestamp,
    Merge,
    NaiveDatetime,
    Replace,
    Resource,
    RowVersion,
    Strftime,
    TimeCursor,
    UnixMillis,
    UnixSeconds,
    ValueBackfill,
    ValueCursor,
)
from dlt_framework.core.cursor import Cursor
from dlt_framework.core.errors import DefinitionError, GuardError, TerminalRunError
from dlt_framework.core.run import BackfillRun, IncrementalRun
from dlt_framework.sources.filesystem import Csv, Files, FilesystemSource
from dlt_framework.sources.rest import RestApiSource
from dlt_framework.sources.scrape import ScrapeSource
from dlt_framework.sources.sql import ConnectorX, SqlSource

START = datetime(2026, 1, 1, tzinfo=UTC)
# Cursor steps of twelve rows, with values shared across chunk edges.
STEPS = [0, 0, 1, 2, 2, 2, 3, 4, 5, 5, 6, 7]


def time_cursor(format: Any, path: str = "updated_at") -> TimeCursor:
    return TimeCursor(path=path, start=START, format=format, on_missing="raise", row_order="asc")


def value_cursor(format: Any, path: str = "updated_at", start: int = 100) -> ValueCursor:
    return ValueCursor(path=path, start=start, format=format, on_missing="raise", row_order="asc")


# Every cursor format, with the source's value at cursor step ``k``.
FORMATS: dict[str, tuple[Cursor, Callable[[int], Any]]] = {
    "iso": (time_cursor(IsoTimestamp()), lambda k: IsoTimestamp().render(START + timedelta(k))),
    "strftime": (
        time_cursor(Strftime("%Y-%m-%d %H:%M:%S")),
        lambda k: Strftime("%Y-%m-%d %H:%M:%S").render(START + timedelta(k)),
    ),
    "unix_seconds": (
        time_cursor(UnixSeconds()),
        lambda k: UnixSeconds().render(START + timedelta(k)),
    ),
    "unix_millis": (time_cursor(UnixMillis()), lambda k: UnixMillis().render(START + timedelta(k))),
    "date": (time_cursor(DateOnly()), lambda k: DateOnly().render(START + timedelta(k))),
    "aware": (time_cursor(AwareDatetime()), lambda k: START + timedelta(k)),
    "naive": (time_cursor(NaiveDatetime()), lambda k: NaiveDatetime().render(START + timedelta(k))),
    "integer": (value_cursor(Integer()), lambda k: 100 + k),
    "rowversion": (value_cursor(RowVersion()), lambda k: RowVersion().render(100 + k)),
}


def chunked(cursor: Cursor | None, chunk: int = 3, **stated: Any) -> Resource:
    return Resource(
        name="orders",
        write=stated.pop("write", Append()),  # appends would duplicate any row loaded twice
        cursor=cursor,
        empty=stated.pop("empty", "warn"),
        chunk=chunk,
        **stated,
    )


class Ascending:
    """An upstream returning rows from the requested start, ascending by cursor, ``page`` rows a
    yield as single rows, pages, or Arrow tables; records the start of every request."""

    def __init__(self, rows: list[dict[str, Any]], page: int = 2, shape: str = "page") -> None:
        self.rows, self.page, self.shape = rows, page, shape
        self.starts: list[Any] = []
        self.fail_at: int | None = None  # the request that fails, counting from 0

    def __call__(self, incremental: Any, window: Any) -> Iterator[Any]:
        start = incremental.start_value if incremental else None
        if len(self.starts) == self.fail_at:
            raise RuntimeError("upstream broke")
        self.starts.append(start)
        found = [row for row in self.rows if start is None or row["updated_at"] >= start]
        for at in range(0, len(found), self.page):
            batch = found[at : at + self.page]
            if self.shape == "row":
                yield from batch
            elif self.shape == "arrow":
                yield pa.Table.from_pylist(batch)
            else:
                yield batch


def ids(query, table: str = "raw_shop.orders") -> list[int]:
    return [row[0] for row in query(f"SELECT id FROM {table} ORDER BY id")]


def increments(frames) -> list[int]:
    loads = frames("loads")
    return loads.filter(loads["window_kind"] == "increment")["rows"].to_list()


@pytest.mark.parametrize("shape", ["row", "page", "arrow"])
@pytest.mark.parametrize("name", FORMATS)
def test_every_cursor_format_and_item_shape_loads_each_row_once(
    make_pipeline, query, frames, name, shape
) -> None:
    cursor, value = FORMATS[name]
    upstream = Ascending(
        [{"id": i, "updated_at": value(k)} for i, k in enumerate(STEPS)], shape=shape
    )
    definition = make_pipeline({chunked(cursor): upstream})

    assert IncrementalRun(definition)()["orders"] == len(STEPS)
    assert ids(query) == list(range(len(STEPS)))
    assert len(increments(frames)) > 2 and sum(increments(frames)) == len(STEPS)
    assert upstream.starts == sorted(upstream.starts)  # each chunk starts where one ended

    assert IncrementalRun(definition)()["orders"] == 0  # nothing new, and nothing repeated
    assert ids(query) == list(range(len(STEPS)))


def test_a_failed_chunk_keeps_the_chunks_before_it(make_pipeline, query) -> None:
    upstream = Ascending([{"id": i, "updated_at": 100 + k} for i, k in enumerate(STEPS)])
    upstream.fail_at = 1
    definition = make_pipeline({chunked(value_cursor(Integer())): upstream})
    with pytest.raises(TerminalRunError):
        IncrementalRun(definition)()
    assert ids(query) == [0, 1, 2, 3]  # the first chunk, committed

    upstream.fail_at = None
    IncrementalRun(definition)()
    assert upstream.starts[1] == 102  # resumed from the first chunk's last value
    assert ids(query) == list(range(len(STEPS)))


def test_whole_and_chunked_resources_load_in_one_run(make_pipeline, query) -> None:
    customers = Resource(name="customers", write=Replace(), cursor=None, empty="fail")
    whole = Ascending([{"id": 1, "updated_at": 0}])
    orders = Ascending([{"id": i, "updated_at": 100 + k} for i, k in enumerate(STEPS)])
    definition = make_pipeline({customers: whole, chunked(value_cursor(Integer())): orders})

    assert IncrementalRun(definition)() == {"customers": 1, "orders": len(STEPS)}
    assert len(whole.starts) == 1 and len(orders.starts) > 2


def test_only_the_first_chunk_is_held_to_the_empty_policy(make_pipeline) -> None:
    upstream = Ascending([{"id": i, "updated_at": 100 + i} for i in range(4)])
    definition = make_pipeline({chunked(value_cursor(Integer()), chunk=2, empty="fail"): upstream})
    assert IncrementalRun(definition)()["orders"] == 4  # its last chunk is empty

    upstream.rows = []
    with pytest.raises(GuardError):
        IncrementalRun(definition)()


def test_backfills_load_their_windows_whole(make_pipeline) -> None:
    upstream = Ascending([{"id": i, "updated_at": i * 10} for i in range(10)])
    resource = chunked(value_cursor(Integer()), chunk=1, backfill=ValueBackfill(start=0, every=50))
    assert BackfillRun(make_pipeline({resource: upstream}))()["orders"] == 10
    assert upstream.starts == [0, 50]


@pytest.mark.parametrize(
    ("cursor", "stated"),
    [
        (None, {}),
        (ValueCursor(path="v", start=0, format=Integer(), on_missing="raise"), {}),
        (
            ValueCursor(path="v", start=0, format=Integer(), on_missing="raise", row_order="desc"),
            {},
        ),
        (replace(value_cursor(Integer()), lag=5), {}),
        (replace(value_cursor(Integer()), on_missing="include"), {}),
        (value_cursor(Integer()), {"write": Replace()}),
        (value_cursor(Integer()), {"write": Merge(primary_key=("id",), strategy="scd2")}),
        (value_cursor(Integer()), {"chunk": 0}),
    ],
    ids=["no cursor", "unordered", "descending", "lag", "null cursors", "replace", "scd2", "zero"],
)
def test_chunks_that_would_skip_or_repeat_rows_are_refused(cursor, stated) -> None:
    with pytest.raises(DefinitionError):
        chunked(cursor, **stated)


def test_merges_load_in_chunks(make_pipeline, query) -> None:
    upstream = Ascending([{"id": i, "updated_at": 100 + k} for i, k in enumerate(STEPS)])
    write = Merge(primary_key=("id",), strategy="delete-insert")
    IncrementalRun(make_pipeline({chunked(value_cursor(Integer()), write=write): upstream}))()
    assert ids(query) == list(range(len(STEPS)))


# ---- each source type ---------------------------------------------------------------------


@pytest.fixture
def statements() -> Iterator[list[str]]:
    """Every SQL statement sent through SQLAlchemy while the test runs."""
    sent: list[str] = []

    def record(connection, cursor, statement, *args) -> None:
        sent.append(statement)

    sqlalchemy.event.listen(sqlalchemy.engine.Engine, "before_cursor_execute", record)
    yield sent
    sqlalchemy.event.remove(sqlalchemy.engine.Engine, "before_cursor_execute", record)


@pytest.fixture
def warehouse(tmp_path, monkeypatch) -> None:
    """A SQLite table whose rows share cursor values, with an integer and a rowversion cursor."""
    database = tmp_path / "erp.sqlite"
    with closing(sqlite3.connect(database)) as connection, connection:
        connection.execute("CREATE TABLE Orders (id INTEGER PRIMARY KEY, version INTEGER, rv BLOB)")
        connection.executemany(
            "INSERT INTO Orders VALUES (?, ?, ?)",
            [(i, 100 + k, (100 + k).to_bytes(8, "big")) for i, k in enumerate(STEPS)],
        )
    monkeypatch.setenv("SOURCES__ERP__CREDENTIALS", f"sqlite:///{database}")


SQL_CURSORS = [
    pytest.param(value_cursor(Integer(), path="version"), id="integer"),
    pytest.param(value_cursor(RowVersion(), path="rv"), id="rowversion"),
]
STREAM = {"backend_kwargs": {"return_type": "arrow_stream"}}  # connectorx reads in batches


@pytest.mark.parametrize("backend", BACKENDS)
@pytest.mark.parametrize("cursor", SQL_CURSORS)
def test_sql_chunks_order_by_the_cursor_and_never_limit_the_query(
    warehouse, query, frames, statements, backend, cursor
) -> None:
    if isinstance(backend, ConnectorX) and isinstance(cursor.format, RowVersion):
        pytest.skip("connectorx sends a binary cursor value as a text literal, chunked or not")
    config = {
        "table": "Orders",
        "chunk_size": 2,
        **(STREAM if isinstance(backend, ConnectorX) else {}),
    }
    source = SqlSource(name="erp", backend=backend, resources={chunked(cursor): config})
    assert IncrementalRun(define("erp", source))()["orders"] == len(STEPS)
    assert ids(query) == list(range(len(STEPS)))
    assert len(increments(frames)) > 2
    if not isinstance(backend, ConnectorX):  # connectorx sends SQL past SQLAlchemy
        reads = [sql for sql in statements if 'FROM "Orders"' in sql]
        assert reads and all("ORDER BY" in sql and "LIMIT" not in sql for sql in reads)


def test_connectorx_refuses_a_chunk_it_would_read_whole() -> None:
    with pytest.raises(DefinitionError):
        SqlSource(
            name="erp", backend=ConnectorX(), resources={chunked(SQL_CURSORS[0].values[0]): {}}
        )
    SqlSource(
        name="erp", backend=ConnectorX(), resources={chunked(SQL_CURSORS[0].values[0]): STREAM}
    )


class AscendingApi(BaseAdapter):
    """Orders updated since ``updated_since``, ascending, two a page by ``page``."""

    def __init__(self, rows: list[dict[str, Any]]) -> None:
        super().__init__()
        self.rows = rows
        self.queries: list[dict[str, list[str]]] = []

    def send(self, request, **kwargs) -> requests.Response:
        query = parse_qs(urlparse(request.url).query)
        self.queries.append(query)
        page = int(query["page"][0])
        found = [row for row in self.rows if row["updated_at"] >= query["updated_since"][0]]
        response = requests.Response()
        response.status_code = 200
        response.headers["Content-Type"] = "application/json"
        response._content = json.dumps({"orders": found[(page - 1) * 2 : page * 2]}).encode()
        response.url, response.request = request.url, request
        return response

    def close(self) -> None:
        pass


def test_rest_chunks_resume_each_request_from_the_last_chunk(query, frames) -> None:
    _, value = FORMATS["iso"]
    adapter = AscendingApi([{"id": i, "updated_at": value(k)} for i, k in enumerate(STEPS)])
    session = requests.Session()
    session.mount("https://", adapter)
    endpoint = {
        "path": "orders",
        "data_selector": "orders",
        "paginator": {
            "type": "page_number",
            "base_page": 1,
            "page_param": "page",
            "total_path": None,
        },
        "start_param": "updated_since",
    }
    source = RestApiSource(
        name="api",
        client={"base_url": "https://api.test/", "session": session},
        resources={chunked(time_cursor(IsoTimestamp())): endpoint},
    )
    assert IncrementalRun(define("api", source))()["orders"] == len(STEPS)
    assert ids(query) == list(range(len(STEPS)))
    starts = [q["updated_since"][0] for q in adapter.queries if q["page"] == ["1"]]
    assert len(increments(frames)) > 2 and starts == sorted(set(starts))


def test_filesystem_chunks_count_files_and_read_each_whole(tmp_path, query, frames) -> None:
    folder = tmp_path / "in"
    folder.mkdir()
    for number in range(5):
        path = folder / f"{number}.csv"
        path.write_text(f"id\n{number * 2}\n{number * 2 + 1}\n")
        modified = (datetime.now(UTC) - timedelta(hours=10 - number)).timestamp()
        os.utime(path, (modified, modified))
    cursor = time_cursor(AwareDatetime(), path="modification_date")
    cursor = replace(cursor, start=datetime.now(UTC) - timedelta(days=1))
    files = Files(bucket_url=folder.as_uri(), file_glob="*.csv", reader=Csv(), files_per_page=1)
    source = FilesystemSource(name="files", resources={chunked(cursor, chunk=2): files})

    assert IncrementalRun(define("files", source))()["orders"] == 10
    assert ids(query) == list(range(10))
    assert increments(frames)[:3] == [4, 4, 2]  # two files a chunk, each read whole


def test_scrape_chunks_stop_the_crawl_and_resume_past_the_cursor(site: Site, query) -> None:
    listing(site, list(range(1, 8)))  # oldest first: ascending by id
    resource = chunked(value_cursor(Integer(), path="id", start=1), chunk=2)
    source = ScrapeSource(name="site", resources={resource: spider(site)})
    assert IncrementalRun(define("site", source))()["orders"] == 7
    assert ids(query) == list(range(1, 8))

"""Each source type proves its cursors and windows reach the upstream request (guard G2)."""

import json
import os
import sqlite3
from contextlib import closing
from datetime import UTC, datetime, timedelta
from urllib.parse import parse_qs, urlparse

import pytest
import requests
from requests.adapters import BaseAdapter

from dlt_framework.core import (
    FREEZE_DATA_TYPES,
    Append,
    AwareDatetime,
    Duration,
    Integer,
    IsoTimestamp,
    Merge,
    Pipeline,
    Resource,
    RowBackfill,
    RowVersion,
    Target,
    TimeBackfill,
    TimeCursor,
    ValueBackfill,
    ValueCursor,
)
from dlt_framework.core.errors import GuardError
from dlt_framework.core.run import BackfillRun, IncrementalRun
from dlt_framework.sources.filesystem import Files, FilesystemSource
from dlt_framework.sources.rest import RestApiSource
from dlt_framework.sources.sql import SqlSource

UTC = UTC
MERGE = Merge(primary_key=("id",), strategy="delete-insert")


def define(name: str, source) -> Pipeline:
    return Pipeline(
        name=name,
        target=Target("duckdb", dataset="raw_shop"),
        schema_contract=FREEZE_DATA_TYPES,
        source=source,
    )


class RecordingAdapter(BaseAdapter):
    """Serves pages of orders honouring skip/take, and records every query string."""

    def __init__(self, total: int) -> None:
        super().__init__()
        self.total = total
        self.queries: list[dict[str, list[str]]] = []

    def send(self, request, **kwargs) -> requests.Response:
        query = parse_qs(urlparse(request.url).query)
        self.queries.append(query)
        skip, take = int(query.get("skip", ["0"])[0]), int(query.get("take", [self.total])[0])
        orders = [{"id": i, "updated_at": "2026-01-05T00:00:00Z"} for i in range(self.total)]
        response = requests.Response()
        response.status_code = 200
        response.headers["Content-Type"] = "application/json"
        response._content = json.dumps({"orders": orders[skip : skip + take]}).encode()
        response.url, response.request = request.url, request
        return response

    def close(self) -> None:
        pass


def rest_pipeline(resource: Resource, total: int = 1) -> tuple[Pipeline, RecordingAdapter]:
    adapter = RecordingAdapter(total)
    session = requests.Session()
    session.mount("https://", adapter)
    endpoint = {
        "path": "orders",
        "data_selector": "orders",
        "paginator": "single_page",
        "start_param": "updated_since",
        "end_param": "updated_before",
        "offset_param": "skip",
        "limit_param": "take",
    }
    source = RestApiSource(
        name="api",
        client={"base_url": "https://api.test/", "session": session},
        resources={resource: endpoint},
    )
    return define("api", source), adapter


TIMED = Resource(
    name="orders",
    write=MERGE,
    cursor=TimeCursor(
        path="updated_at",
        start=datetime(2026, 2, 1, tzinfo=UTC),
        format=IsoTimestamp(),
        on_missing="raise",
    ),
    empty="warn",
    backfill=TimeBackfill(start=datetime(2026, 1, 1, tzinfo=UTC), every=Duration(1, "months")),
)


def test_rest_run_sends_start_only() -> None:
    definition, adapter = rest_pipeline(TIMED)
    IncrementalRun(definition)()
    assert adapter.queries == [{"updated_since": ["2026-02-01T00:00:00Z"]}]


def test_rest_time_backfill_sends_both_bounds() -> None:
    definition, adapter = rest_pipeline(TIMED)
    BackfillRun(definition)()
    assert adapter.queries == [
        {"updated_since": ["2026-01-01T00:00:00Z"], "updated_before": ["2026-02-01T00:00:00Z"]}
    ]


def test_rest_row_backfill_sends_offset_and_limit() -> None:
    paged = Resource(
        name="orders", write=MERGE, cursor=None, empty="warn", backfill=RowBackfill(size=2)
    )
    definition, adapter = rest_pipeline(paged, total=3)
    assert BackfillRun(definition)()["orders"] == 3
    assert adapter.queries == [{"skip": ["0"], "take": ["2"]}, {"skip": ["2"], "take": ["2"]}]


@pytest.fixture
def erp(tmp_path, monkeypatch) -> None:
    """A SQLite source table of five orders."""
    database = tmp_path / "erp.sqlite"
    with closing(sqlite3.connect(database)) as connection, connection:
        connection.execute("CREATE TABLE Orders (id INTEGER PRIMARY KEY, version INTEGER)")
        connection.executemany(
            "INSERT INTO Orders VALUES (?, ?)", [(i, i * 10) for i in range(1, 6)]
        )
    monkeypatch.setenv("SOURCES__ERP__CREDENTIALS", f"sqlite:///{database}")


def sql_pipeline(resource: Resource, **config) -> Pipeline:
    options = {"table": "Orders", **config}
    return define("erp", SqlSource(name="erp", backend="sqlalchemy", resources={resource: options}))


def test_sql_value_backfill_is_a_bounded_where_clause(erp, query) -> None:
    resource = Resource(
        name="orders",
        write=MERGE,
        cursor=ValueCursor(path="version", start=40, format=Integer(), on_missing="raise"),
        empty="warn",
        backfill=ValueBackfill(start=10, every=20),
    )
    assert BackfillRun(sql_pipeline(resource))()["orders"] == 3  # versions 10..30, below 40
    assert query("SELECT id FROM raw_shop.orders ORDER BY id") == [(1,), (2,), (3,)]


def test_sql_row_backfill_orders_and_pages(erp, query) -> None:
    resource = Resource(
        name="orders", write=MERGE, cursor=None, empty="warn", backfill=RowBackfill(size=2)
    )
    assert BackfillRun(sql_pipeline(resource, order_by=["id"]))()["orders"] == 5
    assert query("SELECT id FROM raw_shop.orders ORDER BY id") == [(i,) for i in range(1, 6)]


def test_filesystem_loads_new_files_and_backfills_by_modification_date(tmp_path, query) -> None:
    folder = tmp_path / "in"
    folder.mkdir()
    (folder / "old.csv").write_text("id\n1\n")
    (folder / "new.csv").write_text("id\n2\n")
    cursor_start = datetime.now(UTC) - timedelta(hours=1)
    os.utime(folder / "old.csv", (0, (cursor_start - timedelta(days=1)).timestamp()))
    resource = Resource(
        name="orders",
        write=MERGE,
        cursor=TimeCursor(
            path="modification_date", start=cursor_start, format=AwareDatetime(), on_missing="raise"
        ),
        empty="warn",
        backfill=TimeBackfill(start=cursor_start - timedelta(days=7), every=Duration(7, "days")),
    )
    files = Files(bucket_url=folder.as_uri(), file_glob="*.csv", reader="csv")
    definition = define("files", FilesystemSource(name="files", resources={resource: files}))

    assert IncrementalRun(definition)()["orders"] == 1
    assert BackfillRun(definition)()["orders"] == 1
    assert query("SELECT id FROM raw_shop.orders ORDER BY id") == [(1,), (2,)]

    # The cursor lives on the listing resource; the lost-state guard must still find it.
    IncrementalRun(definition)()
    Pipeline.registry.clear()  # the code now names the pipeline differently
    renamed = define("files_renamed", FilesystemSource(name="files", resources={resource: files}))
    with pytest.raises(GuardError):
        IncrementalRun(renamed)()


def test_append_resource_without_cursor_loads_everything(erp, query) -> None:
    resource = Resource(name="orders", write=Append(), cursor=None, empty="fail")
    assert IncrementalRun(sql_pipeline(resource))()["orders"] == 5


@pytest.fixture
def ledger(tmp_path, monkeypatch) -> None:
    """A SQL Server-style table: ``rv`` holds a rowversion, 8 big-endian bytes."""
    database = tmp_path / "ledger.sqlite"
    with closing(sqlite3.connect(database)) as connection, connection:
        connection.execute("CREATE TABLE Ledger (id INTEGER PRIMARY KEY, rv BLOB NOT NULL)")
        connection.executemany(
            "INSERT INTO Ledger VALUES (?, ?)",
            [(i, (1000 + i).to_bytes(8, "big")) for i in range(1, 7)],
        )
    monkeypatch.setenv("SOURCES__LEDGER__CREDENTIALS", f"sqlite:///{database}")


def test_sql_rowversion_runs_and_backfills_by_the_integer_it_encodes(ledger, query, frames) -> None:
    resource = Resource(
        name="ledger",
        write=MERGE,
        cursor=ValueCursor(path="rv", start=1004, format=RowVersion(), on_missing="raise"),
        empty="warn",
        backfill=ValueBackfill(start=1000, every=2),
    )
    source = SqlSource(
        name="ledger", backend="sqlalchemy", resources={resource: {"table": "Ledger"}}
    )
    definition = define("ledger", source)

    assert IncrementalRun(definition)()["ledger"] == 3  # rowversions 1004..1006
    assert BackfillRun(definition)()["ledger"] == 3  # [1000, 1002) and [1002, 1004)
    assert query("SELECT id FROM raw_shop.ledger ORDER BY id") == [(i,) for i in range(1, 7)]
    increments = frames("loads").filter(frames("loads")["window_kind"] == "increment")
    assert increments["cursor_last_value"].to_list() == ["1006"]  # readable, not b'\\x00...'


BACKENDS = ["sqlalchemy", "pyarrow", "connectorx"]


@pytest.mark.parametrize("backend", BACKENDS)
def test_sql_backends_load_the_same_rows_and_cursor(erp, query, frames, backend) -> None:
    resource = Resource(
        name="orders",
        write=MERGE,
        cursor=ValueCursor(path="version", start=20, format=Integer(), on_missing="raise"),
        empty="warn",
    )
    source = SqlSource(name="erp", backend=backend, resources={resource: {"table": "Orders"}})
    assert IncrementalRun(define("erp", source))()["orders"] == 4
    assert query("SELECT id FROM raw_shop.orders ORDER BY id") == [(2,), (3,), (4,), (5,)]
    assert frames("loads")["cursor_last_value"].to_list() == ["50"]

"""Each source type proves its cursors and windows reach the upstream request (guard G2)."""

import json
import os
import sqlite3
from contextlib import closing
from datetime import datetime, timedelta, timezone
from urllib.parse import parse_qs, urlparse

import pytest
import requests
import sqlalchemy
from requests.adapters import BaseAdapter

from dlt_framework.core import (
    FREEZE_DATA_TYPES,
    Append,
    AwareDatetime,
    Duration,
    IsoTimestamp,
    Merge,
    Pipeline,
    Resource,
    Retry,
    RowBackfill,
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

UTC = timezone.utc
MERGE = Merge(primary_key=("id",), strategy="delete-insert")


def define(name: str, source) -> Pipeline:
    return Pipeline(
        name=name,
        target=Target("duckdb", dataset="raw_shop"),
        schema_contract=FREEZE_DATA_TYPES,
        retry=Retry(attempts=1),
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
def erp(tmp_path, monkeypatch) -> list[str]:
    """A SQLite source table plus a list collecting every SQL statement sent to it."""
    database = tmp_path / "erp.sqlite"
    with closing(sqlite3.connect(database)) as connection, connection:
        connection.execute("CREATE TABLE Orders (id INTEGER PRIMARY KEY, version INTEGER)")
        connection.executemany(
            "INSERT INTO Orders VALUES (?, ?)", [(i, i * 10) for i in range(1, 6)]
        )
    monkeypatch.setenv("SOURCES__ERP__CREDENTIALS", f"sqlite:///{database}")
    return []


def sql_pipeline(resource: Resource, statements: list[str], **config) -> Pipeline:
    def capture(engine):
        sqlalchemy.event.listen(engine, "before_cursor_execute", lambda *a: statements.append(a[2]))
        return engine

    options = {"table": "Orders", "engine_adapter_callback": capture, **config}
    return define("erp", SqlSource(name="erp", resources={resource: options}))


def test_sql_value_backfill_is_a_bounded_where_clause(erp, query) -> None:
    resource = Resource(
        name="orders",
        write=MERGE,
        cursor=ValueCursor(path="version", start=40, on_missing="raise"),
        empty="warn",
        backfill=ValueBackfill(start=10, every=20),
    )
    assert BackfillRun(sql_pipeline(resource, erp))()["orders"] == 3
    assert query("SELECT id FROM raw_shop.orders ORDER BY id") == [(1,), (2,), (3,)]
    select = next(s for s in erp if 'FROM "Orders"' in s or "FROM Orders" in s)
    assert '"Orders".version >= ?' in select and '"Orders".version < ?' in select


def test_sql_row_backfill_orders_and_pages(erp, query) -> None:
    resource = Resource(
        name="orders", write=MERGE, cursor=None, empty="warn", backfill=RowBackfill(size=2)
    )
    assert BackfillRun(sql_pipeline(resource, erp, order_by=["id"]))()["orders"] == 5
    selects = [s for s in erp if "LIMIT" in s]
    assert len(selects) == 3 and all('ORDER BY "Orders".id' in s and "OFFSET" in s for s in selects)
    assert query("SELECT count(*) FROM raw_shop.orders") == [(5,)]


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
    renamed = define("files_renamed", FilesystemSource(name="files", resources={resource: files}))
    with pytest.raises(GuardError):
        IncrementalRun(renamed)()


def test_append_resource_without_cursor_loads_everything(erp, query) -> None:
    resource = Resource(name="orders", write=Append(), cursor=None, empty="fail")
    assert IncrementalRun(sql_pipeline(resource, erp))()["orders"] == 5

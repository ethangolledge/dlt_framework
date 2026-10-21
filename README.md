# dlt framework

A small library for building named, independently runnable [dlt](https://dlthub.com/docs) pipelines
into DuckDB or Postgres. You declare *what* to load; the framework owns how it runs, how it
backfills, and the guards that stop a pipeline silently reloading its whole source.

## Layout

```
dlt_framework/
  core/                 shared by every source type; never imports a source package
  sources/
    rest/               RestApiSource       + pipelines/ defined on it
    sql/                SqlSource           (SQL Server, Postgres, ...)
    filesystem/         FilesystemSource    (S3, GCS, Azure, local)
    custom/             CustomSource        (any Python generator)
  cli.py                one entry point for every pipeline
```

Each source type owns its package: its adapter, anything specific to it, and the pipelines built on
it (`sources/<type>/pipelines/<system>.py`). `lint-imports` enforces that core never imports a
source type and that source types never import each other.

## Components

Everything that decides what data lands is a required argument.

| Component | Variants |
| --- | --- |
| `Pipeline(name, source, target, schema_contract)` | Unique, permanent name = dlt state key |
| `Target(destination, dataset)` | Credentials from dlt env vars / `.dlt/secrets.toml` |
| `Resource(name, write, cursor, empty, backfill=None)` | One stream of rows into one table |
| Write mode | `Append()`, `Replace()`, `Merge(primary_key=..., strategy=...)` |
| Cursor | `TimeCursor(path, start, format, on_missing)`, `ValueCursor(path, start, on_missing)`, or `None` |
| Time format | `IsoTimestamp`, `Strftime`, `UnixSeconds`, `UnixMillis`, `DateOnly`, `AwareDatetime`, `NaiveDatetime` |
| Backfill | `TimeBackfill(start, every)`, `ValueBackfill(start, every)`, `RowBackfill(size)` |

```python
from datetime import datetime, timezone

from dlt_framework.core import (
    FREEZE_DATA_TYPES, Duration, IsoTimestamp, Merge, Pipeline, Resource, Target, TimeBackfill,
    TimeCursor,
)
from dlt_framework.sources.rest import RestApiSource

orders = Resource(
    name="orders",
    write=Merge(primary_key=("id",), strategy="delete-insert"),
    cursor=TimeCursor(
        path="updated_at",
        start=datetime(2026, 10, 1, tzinfo=timezone.utc),
        format=IsoTimestamp(),
        on_missing="raise",
        fallbacks=("created_at",),
    ),
    empty="warn",
    backfill=TimeBackfill(start=datetime(2022, 1, 1, tzinfo=timezone.utc), every=Duration(1, "months")),
)

Pipeline(
    name="acme_shop",
    target=Target("postgres", dataset="raw_shop"),
    schema_contract=FREEZE_DATA_TYPES,
    source=RestApiSource(
        name="shop",
        client={"base_url": "https://api.example.com/", "paginator": {"type": "header_link"}},
        resources={
            orders: {"path": "orders", "start_param": "updated_since", "end_param": "updated_before"},
        },
    ),
)
```

## Commands

```text
dlt-framework list                                     pipelines, source types, resources
dlt-framework check NAME                               build run + backfill sources offline
dlt-framework run NAME [--allow-initial-load]
dlt-framework backfill NAME [--resource R --from RESUME]
```

- **run** loads every resource from its stored cursor under pipeline name `NAME`. If a cursor
  resource has no stored state but its table already holds rows (renamed pipeline/source/resource,
  wrong dataset or destination), it refuses to start instead of reloading from the cursor start.
- **backfill** loads each resource's declared history under `NAME__backfill`, into the same
  tables, with its own staging dataset. Time and value backfills run from their `start` up to the
  cursor's `start`, bounded with `end_value`, so they never read or move the run cursor. Row
  backfills page through sources with no usable cursor until a page comes back short. Nothing is
  checkpointed: resume with `--resource R --from <end of last logged window>`.

Exit codes: `0` success, `2` definition/config/guard refusal, `3` terminal failure, `4` transient
retries exhausted, `130` interrupted.

## Running

```bash
cp .env.example .env
uv sync --group dev
uv run dlt-framework run dummyjson
```

```bash
docker build -t dlt-pipelines .
docker run --rm --user "$(id -u):$(id -g)" --env-file .env \
  --mount type=bind,source="$(pwd)/test_data",target=/data \
  --mount type=bind,source="$(pwd)/dlt_state",target=/var/lib/dlt \
  dlt-pipelines run dummyjson
```

The scheduler must guarantee one active job per pipeline name across hosts (the built-in lock
covers one host), deliver `SIGTERM` with a grace period, and alert on non-zero exits. Keep
`DLT_DATA_DIR` persistent, or keep `PIPELINES__RESTORE_FROM_DESTINATION=true` on ephemeral runners.

## Development

```bash
uv run ruff check dlt_framework tests
uv run ruff format --check dlt_framework tests
uv run lint-imports
uv run mypy dlt_framework
uv run pytest --cov=dlt_framework --cov-branch --cov-fail-under=90
```

Guidance for building and changing pipelines, including how dlt state works underneath, is in
`.claude/skills/dlt-pipelines/`.

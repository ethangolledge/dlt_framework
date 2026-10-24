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
pipeline list                                         pipelines, source types, resources
pipeline check NAME                                   build run + backfill sources offline
pipeline run NAME [--allow-initial-load]              incremental load
pipeline backfill NAME [--resource R --from RESUME]   declared history
```

- **run** loads every resource from its stored cursor under pipeline name `NAME`. If a cursor
  resource has no stored state but its table already holds rows (renamed pipeline/source/resource,
  wrong dataset or destination), it refuses to start instead of reloading from the cursor start.
- **backfill** loads each resource's declared history under `NAME__backfill`, into the same
  tables, with its own staging dataset. Time and value backfills run from their `start` up to the
  cursor's `start`, bounded with `end_value`, so they never read or move the run cursor. Row
  backfills page through sources with no usable cursor until a page comes back short. Nothing is
  checkpointed: resume with `--resource R --from <end of last logged window>`.

Exit codes: `0` success, `1` unexpected error (a bug; logged with its traceback), `2`
definition/config/guard refusal, `3` terminal failure, `4` transient retries exhausted, `5` out of
memory (Python `MemoryError`, never retried), `130` interrupted.
`137` comes from Docker/the kernel, not the framework: the container was OOM-killed (see
Monitoring).

A failure is logged as one line saying what failed, where, and the root cause, followed by the
root cause's traceback:

```
ERROR dlt_framework.cli: run acme_shop failed in extract of orders, window time [2025-01-01T00:00:00+00:00, 2025-02-01T00:00:00+00:00): HTTPError: 401 Client Error: Unauthorized ... (TerminalRunError, exit 3)
```

dlt's own logger is set to `WARNING` (`.dlt/config.toml`) and shares the framework's handler, so
`LOG_FORMAT=json|text` controls every line. That handler masks URL passwords and query-string
values (`?api_key=***`) in every line, tracebacks included; monitoring `error_message` and
notifier messages are masked the same way.

## Monitoring

Every run writes append-only Polars frames to a **monitoring sink**. You state the sink per
environment, so dev, UAT, and prod never mix: `monitoring.uri` in `.dlt/secrets.toml`, or
`MONITORING__URI` in containers. Run commands refuse to start without one.

| URI | Sink |
| --- | --- |
| `file:///data/monitoring`, `s3://bucket/monitoring/prod`, `gs://...`, `az://...` | `ParquetSink`: immutable files at `<root>/<frame>/<uuid>.parquet` |
| `postgresql://...`, `mssql+pyodbc://...`, `sqlite:///...` | `DatabaseSink`: tables `runs`, `loads`, `samples` created on first write |

Anything else is one `Sink` subclass declaring its `schemes` (`core/monitoring/sinks.py`).

The frames are a star schema with readable, deterministic keys:

| Frame | One row per | Keys | Answers |
| --- | --- | --- | --- |
| `pipelines` | pipeline, per run | `pipeline_id`, `run_id` | Source type and name, destination, dataset |
| `resources` | resource, per run | `resource_id`, `pipeline_id`, `run_id` | Write mode, cursor kind/path/start, backfill kind, empty policy, full definition |
| `runs` | `started` / `succeeded` / `failed` event | `run_id`, `pipeline_id` | When, mode, outcome, exit code, error type/step/resource/window/root cause, rows, peak RSS vs container memory limit, CPU seconds |
| `loads` | resource × window | `load_id`, `resource_id`, `run_id` | Rows, extract/normalize/load seconds, window bounds (backfills), decoded cursor `last_value` after the load |
| `samples` | resource sample (every 15 s, flushed every 60 s) | `run_id` | RSS, CPU % (of one core), container memory limit over time |

- `pipeline_id` is the pipeline name (permanent, since it keys dlt state); `resource_id` is
  `<pipeline_id>.<resource>`; `run_id` is generated per run.
- `load_id` is dlt's load id: the same value as `_dlt_loads.load_id` and `_dlt_load_id` on every
  row in the destination, so a monitoring row leads straight to the rows it loaded.
- Definitions are written every run. Join on `(run_id, resource_id)` for a resource as it was
  for that run, or keep the latest row per `resource_id` for a one-row-per-key dimension.

Records are written as they happen, so a container killed mid-run still leaves its `started` row
and its memory trend. A lost run is a `started` with no outcome:

```python
import polars as pl

runs = pl.read_parquet("monitoring/runs/*.parquet")
ended = runs.filter(pl.col("event") != "started").select("run_id")
lost = runs.filter(pl.col("event") == "started").join(ended, on="run_id", how="anti")
samples = pl.read_parquet("monitoring/samples/*.parquet").join(lost.select("run_id"), on="run_id")
```

## Running locally

One-time setup:

```bash
uv sync --group dev                              # builds .venv from uv.lock
cp .dlt/secrets.toml.example .dlt/secrets.toml   # local DuckDB path; git-ignored
direnv allow                                     # auto-activates .venv on cd (see .envrc)
```

Without [direnv](https://direnv.net), run `source .venv/bin/activate` once per terminal instead.
Then:

```bash
pipeline list
pipeline run dummyjson
```

## Running in Docker

The image's entrypoint is the `pipeline` command, so the same words follow the image name.
`.envrc` and `.dlt/secrets.toml` never enter the image; pass configuration as environment
variables (`.env.example`).

```bash
docker build -t dlt-pipelines .
docker run --rm --user "$(id -u):$(id -g)" --env-file .env \
  --mount type=bind,source="$(pwd)/test_data",target=/data \
  --mount type=bind,source="$(pwd)/dlt_state",target=/var/lib/dlt \
  dlt-pipelines run dummyjson
```

The scheduler must guarantee one active job per pipeline name across hosts (the built-in lock
covers one host), deliver `SIGTERM` with a grace period, and alert on non-zero exits. Keep
`DLT_DATA_DIR` persistent, or keep `restore_from_destination` (on in `.dlt/config.toml`) on ephemeral
runners.

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

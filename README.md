# dlt framework

A small library for named, independently runnable [dlt](https://dlthub.com/docs) pipelines that
land raw data in DuckDB or Postgres: the bronze layer of a medallion stack. A pipeline definition
states **what** lands and **where**. `.dlt/config.toml` states **how runs behave** in each
environment. The framework owns the run itself: incremental state, backfills, guards, and
monitoring.

## Layout

```
dlt_framework/
  core/                 shared by every source type; never imports a source package
    config.py           settings resolution and registries (sinks, notifiers, log formats)
    settings.py         what config.toml must state: retry, monitoring, guards, notifications, logging
    run.py              the run spine: incremental runs, backfills, guards, rollback
    monitoring/         run, load and resource frames, sinks, coverage, status
  sources/
    rest/               RestApiSource       + pipelines/ defined on it
    sql/                SqlSource           (SQL Server, Postgres, ...)
    filesystem/         FilesystemSource    (S3, GCS, Azure, local)
    custom/             CustomSource        (any Python generator)
    scrape/             ScrapeSource        (web pages through Scrapling spiders)
  cli.py                one entry point for every pipeline
```

Each source type owns its package: its adapter, its quirks, and the pipelines built on it
(`sources/<type>/pipelines/<system>.py`). `lint-imports` enforces that core never imports a source
type and that source types never import each other.

## Installing

A clone installs nothing until you say what it needs. The core covers runs, monitoring, and the
REST and custom source types; each destination and every other source type is an extra:

| Extra | Adds | Size |
| --- | --- | --- |
| *(core)* | dlt, Polars, psutil, SQLAlchemy, tenacity: runs, monitoring, `RestApiSource`, `CustomSource` | small |
| `duckdb` | DuckDB destination | small |
| `postgres` | Postgres destination (and Postgres monitoring sink) | small |
| `sql` | `SqlSource`, with the `pyarrow` backend | medium |
| `connectorx` | `SqlSource(backend="connectorx")`, includes `sql` | medium |
| `filesystem` | `FilesystemSource`: local folders and S3, CSV/JSONL/Parquet readers | medium |
| `gcs` / `azure` | `gs://` / `az://` buckets for `FilesystemSource`, includes `filesystem` | medium |
| `scrape` | `ScrapeSource`: Scrapling with its fetchers | large (~700 MB in the image: Scrapling's spiders import its browser automation packages even for plain HTTP) |

```bash
uv sync --no-dev --extra postgres --extra sql     # run SQL pipelines into Postgres
uv sync --all-extras                              # develop the framework: every extra, plus test tools
```

Each destination and source type declares the modules it needs at run time (a SQL backend, a
bucket protocol, a destination driver), so the framework knows which pipelines can run here:

- `pipeline list` shows only those.
- `pipeline check` reports every pipeline, ready or not, with the `uv sync --extra ...` that
  would make it ready. That includes pipelines whose module could not even be imported.
- `pipeline check NAME`, `run` and `backfill` refuse a pipeline that is not ready, before
  anything starts (exit 2).

## Defining a pipeline

Everything that decides what lands is a required argument; nothing defaults.

| Component | Variants |
| --- | --- |
| `Pipeline(name, source, target, schema_contract)` | Unique, permanent name = dlt state key. Each table has one owning pipeline. |
| `Target(destination, dataset)` | Credentials from dlt env vars / `.dlt/secrets.toml` |
| `Resource(name, write, cursor, empty, backfill=None)` | One stream of rows into one table |
| Write mode | `Append()`, `Replace()`, `Merge(primary_key=..., strategy=...)` |
| Cursor | `TimeCursor(path, start, format, on_missing)`, `ValueCursor(path, start, format, on_missing)`, or `None` |
| Time format | `IsoTimestamp`, `Strftime`, `UnixSeconds`, `UnixMillis`, `DateOnly`, `AwareDatetime`, `NaiveDatetime` |
| Value format | `Integer`, `RowVersion` (SQL Server `rowversion`: 8 bytes, stated as the integer it encodes) |
| Backfill | `TimeBackfill(start, every)`, `ValueBackfill(start, every)`, `RowBackfill(size)` |
| Source | `RestApiSource`, `SqlSource(name, backend, resources)`, `FilesystemSource`, `CustomSource`, `ScrapeSource` |

```python
from datetime import datetime, timezone

from dlt_framework.core import (
    FREEZE_DATA_TYPES, Duration, IsoTimestamp, Merge, Pipeline, Resource, RowVersion, Target,
    TimeBackfill, TimeCursor, ValueBackfill, ValueCursor,
)
from dlt_framework.sources.rest import RestApiSource
from dlt_framework.sources.sql import SqlSource

orders = Resource(
    name="orders",
    write=Merge(primary_key=("id",), strategy="delete-insert"),
    cursor=TimeCursor(
        path="updated_at",
        start=datetime(2026, 10, 1, tzinfo=timezone.utc),
        format=IsoTimestamp(),
        on_missing="raise",
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

ledger = Resource(
    name="ledger",
    write=Merge(primary_key=("id",), strategy="delete-insert"),
    cursor=ValueCursor(path="rv", start=900_000_000, format=RowVersion(), on_missing="raise"),
    empty="warn",
    backfill=ValueBackfill(start=0, every=50_000_000),  # rowversion integers
)

Pipeline(
    name="erp_finance",
    target=Target("postgres", dataset="raw_erp"),
    schema_contract=FREEZE_DATA_TYPES,
    source=SqlSource(name="erp", backend="pyarrow", resources={ledger: {"table": "Ledger"}}),
)
```

## Configuration

`.dlt/config.toml` states every operational setting, and every key is required: a missing one
stops the command with exit 2 and names the keys dlt looked for. Values resolve through dlt's
configuration system, highest precedence first:

1. environment variables, e.g. `RETRY__ATTEMPTS=3`;
2. a per-pipeline section, `[pipelines.<name>.<section>]`, which only needs the keys it changes;
3. the shared section.

Secrets (`monitoring.uri`, webhook URLs, credentials) live in `.dlt/secrets.toml` locally and in
environment variables in containers.

| Section | Keys | Decides |
| --- | --- | --- |
| `[retry]` | `attempts`, `initial_wait_s`, `max_wait_s`, `give_up_after_s` | Jittered exponential backoff for transient failures; a step gives up after `attempts` or `give_up_after_s`, so a database down for minutes is ridden out |
| `[monitoring]` | `uri` (secret), `sample_every_s`, `flush_every_s` | Where monitoring goes, and how often memory and CPU are sampled and written |
| `[guards]` | `future_cursor_tolerance_s` | How far ahead of now a time cursor may move before the run refuses it |
| `[notifications]` | `on`, `channels`; `[notifications.<channel>] kind`, plus that kind's fields | Which outcomes alert, and through which channels |
| `[logging]` | `format`, `level` | One log format for framework and dlt lines |

```toml
[pipelines.acme_shop.retry]
give_up_after_s = 3600          # this API has hour-long maintenance windows

[notifications]
on = ["failed"]
channels = ["ops"]

[notifications.ops]
kind = "webhook"                # notifications.ops.url goes in secrets.toml
```

dlt's own performance settings live in the same file: `[extract] workers`,
`[data_writer] file_max_items`, `[normalize]`, `[load]`, and `[runtime] request_*` for HTTP
retries.

### Swapping implementations

Monitoring sinks, alert channels, and log formats are chosen from config by key. A new one is a
registered class; nothing in core changes.

| Kind | Chosen by | Shipped | Add one |
| --- | --- | --- | --- |
| Monitoring sink | `monitoring.uri` scheme | `ParquetSink` (`file`, `s3`, `gs`, `az`, ...), `DatabaseSink` (`postgresql`, `mssql`, `mysql`, `sqlite`) | `@sinks.register("scheme")` on a `Sink` with `from_uri`, `write`, `read` |
| Alert channel | `notifications.<channel>.kind` | `WebhookNotifier` (`webhook`: Slack, Mattermost, any JSON endpoint) | `@notifiers.register("kind")` on a `@configspec` `Notifier` with `send` |
| Log format | `logging.format` | `text`, `json` | `@formatters.register("name")` on a `logging.Formatter` |

## Commands

```text
pipeline list                                             pipelines ready to run here
pipeline check [NAME]                                     readiness of every pipeline, or one built offline
pipeline run NAME [--resource R ...] [--allow-initial-load]
pipeline backfill NAME [--resource R ...] [--from X] [--to Y] [--missing]
pipeline status [NAME]                                    last runs, cursors, backfill coverage
```

- **run** loads resources (all, or each `--resource`) from their stored cursors under pipeline
  name `NAME`. Runs are stateful.
- **backfill** loads declared history under `NAME__backfill`, into the same tables. Backfills are
  stateless: windows are bounded with dlt's `end_value`, so they never read or move the run
  cursor. Time and value histories run from the backfill `start` to the cursor `start`; row
  histories page until a page comes back short.
  - `--from X --to Y` (with one `--resource`) loads exactly `[X, Y)` in that resource's window
    edges: ISO-8601 datetimes with a timezone, integers (ids, rowversions), or row offsets.
  - `--missing` loads only the gaps that monitoring has no successful window for.
- **status** shows, per resource:
  - the last run, its mode and outcome (`lost or running` when it never reported one);
  - the last success, the rows loaded, and the cursor's position;
  - backfill coverage: % of the declared history, the number of gaps, and the first gap.

  It reads monitoring only, never the destination.

## Guarantees

- **State is never ahead of the data.** dlt moves the cursor in local state during extract. A
  failure the next run cannot finish (a guard, a schema contract, a terminal load error) discards
  the local working state, so the destination's committed cursor stays the truth and the same rows
  are extracted again. A transient failure or an interrupt keeps the extracted package, and the
  next run loads it first. No state ever needs editing by hand.
- **No stuck cursors.** A time cursor reaching more than `future_cursor_tolerance_s` past now is
  refused before it is committed, naming the resource and the value. One bad row cannot stall a
  pipeline until a date in the future.
- **No silent full reloads.** A cursor resource with no stored state over a table that already
  holds rows refuses to start (the pipeline, source or resource was renamed, or the dataset or
  destination changed). `--allow-initial-load` is the deliberate override.
- **No silent empties.** `empty="fail"` refuses to load an empty extract, so a `Replace` resource
  cannot wipe its table on a bad upstream day.
- **No shared staging.** Every run name has its own staging dataset,
  `<dataset>_staging_<run|backfill>_<pipeline>`, so concurrent pipelines on one dataset never
  share staging tables.
- **One owner per table**, checked when pipelines are defined.
- **One active run per pipeline per host** (a file lock). The scheduler guarantees one per
  pipeline across hosts.

## Performance and scaling

- **SQL backends.** `SqlSource(backend=...)` is required:
  - `pyarrow` keeps exact types and runs 20–30× faster than `sqlalchemy`.
  - `connectorx` is the fastest, with costs: it reads whole results into memory (it ignores
    `chunk_size`) and drops timezones from tz-aware timestamps. The framework rejects it with a
    tz-aware time cursor, and derives its connection string from the credentials.
  - Install it with the `connectorx` extra.
- **Extract.** A source's resources always extract in parallel threads; `[extract] workers` sets
  how many, and `[sources.<name>.extract] workers = 1` serialises a rate-limited upstream. One
  large table is a single stream.
- **Normalize and load.** `[data_writer] file_max_items` rotates files so a large table spreads
  across normalize processes and load jobs. Normalize workers start with `spawn`: forking a
  process that runs threads can deadlock.
- **Large backfills.** Use a `ValueBackfill` on an id or rowversion. `RowBackfill` pages with
  `OFFSET`, which re-reads earlier rows on every page.
- **Scaling out.** Every command acts on one pipeline, so each pipeline is its own process or
  container. SQL, S3 and REST pipelines run side by side from one image.

## Failures

Exit codes:

| Code | Meaning |
| --- | --- |
| `0` | success |
| `1` | unexpected error (a bug; logged with its traceback) |
| `2` | definition, configuration, or guard refusal |
| `3` | terminal failure |
| `4` | transient retries exhausted |
| `5` | out of memory (Python `MemoryError`, never retried) |
| `130` | interrupted |
| `137` | not raised by the framework: Docker or the kernel OOM-killed the container (see Monitoring) |

A failure is one line saying what failed, where, and the root cause, followed by the root cause's
traceback:

```
ERROR dlt_framework.cli: run acme_shop failed in extract of orders, window time [2025-01-01T00:00:00+00:00, 2025-02-01T00:00:00+00:00): HTTPError: 401 Client Error: Unauthorized ... (TerminalRunError, exit 3)
```

- **One handler for every line.** dlt's own logger is set to `WARNING` and shares the framework's
  handler, so `[logging] format` controls every line.
- **Secrets are masked.** The handler masks URL passwords and query-string values
  (`?api_key=***`) in every line, tracebacks included. Monitoring `error_message` and alerts are
  masked the same way.
- **Unknown errors exit 4, not 3.** dlt treats any error it doesn't recognise as transient, so it
  is retried before it fails.

## Monitoring

Every run writes append-only Polars frames to the monitoring sink stated for the environment.
They form a star schema with readable, deterministic keys:

| Frame | One row per | Keys | Answers |
| --- | --- | --- | --- |
| `pipelines` | pipeline, per run | `pipeline_id`, `run_id` | Source type and name, destination, dataset |
| `resources` | resource, per run | `resource_id`, `pipeline_id`, `run_id` | Write mode, cursor kind/path/start, backfill kind, empty policy, full definition |
| `runs` | `started` / `succeeded` / `failed` event | `run_id`, `pipeline_id` | When, mode, outcome, exit code, error type/step/resource/window/root cause, rows, peak RSS vs container memory limit, CPU seconds |
| `loads` | resource × window | `load_id`, `resource_id`, `run_id` | Rows, step seconds, window bounds, the cursor's value after the load (readable: ISO or integer) |
| `samples` | resource sample | `run_id` | RSS, CPU % (of one core), container memory limit over time |

- **The keys.** `pipeline_id` is the pipeline name; `resource_id` is `<pipeline_id>.<resource>`;
  `run_id` is generated per run.
- **`load_id` links to the destination.** It is dlt's load id, the same value as
  `_dlt_loads.load_id` and `_dlt_load_id` on every row in the destination.
- **Definitions are written every run.** Join on `(run_id, resource_id)` for a resource as it was
  in that run, or keep the latest row per `resource_id` for a one-row-per-key dimension.
- **Killed runs leave a trace.** Records are written as they happen, so a container killed
  mid-run still leaves its `started` row and its memory trend, and `pipeline status` shows the run
  as `lost or running`.

## Scraping

`ScrapeSource` (the `scrape` extra) loads web pages through [Scrapling](https://scrapling.readthedocs.io)
spiders. Every item a spider yields becomes a row, as scraped, for downstream layers to shape.
Scrapling owns fetching (sessions, concurrency, throttling, robots.txt, retrying blocked pages);
dlt owns loading and the cursor. `sources/scrape/pipelines/quotes.py` is a working example.

```python
class PostsSpider(ScrapeSpider):
    name = "posts"
    start_urls = ["https://example.com/news"]
    robots_txt_obey = True        # stated, never defaulted
    failed_requests = "raise"     # or "warn": load what was scraped, log the failures

    async def parse(self, response):
        for post in response.css("article"):
            yield {"url": response.url, "title": post.css("h2::text").get(), "posted": ...}
        if next_page := response.css("a.next::attr(href)").get():
            yield response.follow(next_page)
```

- **Incremental.** A newest-first listing with `row_order="desc"` on the cursor stops the crawl at
  the first item below the stored cursor, so only new pages are fetched. A spider can also read
  `self.incremental.start_value` to stop paginating itself.
- **Windows.** A backfill hands each window to the spider as `self.window`, and the spider turns
  it into page numbers or date filters. A `RowBackfill` walks pages until one comes back short.
- **No silent failures.** Scrapling only logs some failures:
  - failed requests;
  - `parse` errors;
  - pages still blocked after retries;
  - requests skipped by robots.txt, including when robots.txt itself is unreachable.

  `ScrapeSpider` collects them, and `failed_requests = "raise"` fails the extract so nothing is
  committed. Spider logs go through the framework's handler and its URL-secret masking.
- **Stateless outside dlt.** Scrapling's crawl checkpoint (`crawldir`) is not used; dlt state and
  monitoring are the record.
- **Browsers.** Plain HTTP (the default session) runs in the standard image. Browser sessions
  (`StealthySession`) also need `scrapling install` and an image with a browser and its system
  libraries.
- **Your responsibility.** Check each site's terms and robots.txt, and whether pages hold
  personal data, before scraping it.

## Adding to the framework

| To add | Write | Touch core? |
| --- | --- | --- |
| A pipeline | `sources/<type>/pipelines/<system>.py` with a `Pipeline(...)`; then `pipeline check <name>` and `pipeline run <name>` | No |
| A source type (GraphQL, MongoDB, Shopify) | `sources/<type>/`: subclass `Source[ConfigT]`, implement `resource`, list `windows`, add its quirks in `validate`. A library the core lacks becomes an extra named after the source type in `pyproject.toml`, plus a row in *Installing* | No |
| A cursor format | A `TimeFormat` or `ValueFormat` with `render` and `parse` | No, if it lives in the source package |
| A sink, alert channel, or log format | A registered class (see *Swapping implementations*) | No |

## Running locally

```bash
uv sync --no-dev --extra duckdb                  # what the example pipelines need (see Installing)
cp .dlt/secrets.toml.example .dlt/secrets.toml   # local DuckDB path and monitoring uri; git-ignored
direnv allow                                     # activates .venv on cd (see .envrc)
pipeline list
pipeline run dummyjson
pipeline status
```

Without [direnv](https://direnv.net), run `source .venv/bin/activate` once per terminal instead.

## Running in Docker

The image's entrypoint is the `pipeline` command, and the image includes `.dlt/config.toml`.
`.envrc` and `.dlt/secrets.toml` never enter the image: pass secrets and overrides as environment
variables (`.env.example`).

```bash
docker build --build-arg EXTRAS="--extra duckdb" -t dlt-pipelines .   # EXTRAS is required
docker run --rm --user "$(id -u):$(id -g)" --env-file .env \
  --mount type=bind,source="$(pwd)/test_data",target=/data \
  --mount type=bind,source="$(pwd)/dlt_state",target=/var/lib/dlt \
  dlt-pipelines run dummyjson
```

The scheduler must:
- guarantee one active job per pipeline name across hosts (the built-in lock covers one host);
- deliver `SIGTERM` with a grace period;
- alert on non-zero exits.

Keep `DLT_DATA_DIR` persistent, or keep `restore_from_destination` (on in `.dlt/config.toml`) on
ephemeral runners.

## Development

```bash
uv sync --all-extras                              # every extra plus the dev tools
uv run ruff check dlt_framework tests
uv run ruff format --check dlt_framework tests
uv run lint-imports
uv run mypy dlt_framework
uv run pytest --cov=dlt_framework --cov-branch --cov-fail-under=90
```

`TEST_POSTGRES_URL=postgresql://...` also runs the Postgres tests. Guidance for building and
changing pipelines, including how dlt state works underneath, is in `.claude/skills/dlt-pipelines/`.

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
    config.py           settings resolution and registries (sinks, log formats)
    settings.py         what config.toml must state: retry, monitoring, guards, logging
    run.py              the run spine: incremental runs, backfills, guards, rollback
    buckets.py          folders and buckets by URL, with one credentials setting
    monitoring/         frames, the monitor, sampler, profiler, sinks, coverage, status, health
  sources/
    rest/               RestApiSource       + pipelines/ defined on it
    sql/                SqlSource           (SQL Server, Postgres, ...)
    filesystem/         FilesystemSource    (S3, GCS, Azure, local)
      documents/        Documents reader    (PDFs to rows: templates, model, OCR)
    custom/             CustomSource        (any Python generator)
    scrape/             ScrapeSource        (web pages through Scrapling spiders)
  cli.py                one entry point for every pipeline
dashboard/              the monitoring dashboard: model, render, pages, app (terminal, web)
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
| `sql` | `SqlSource`, with the `PyArrow()` backend | medium |
| `mssql` | SQL Server for `SqlSource` and the monitoring sink (pymssql; no system driver), includes `sql` | medium |
| `connectorx` | `SqlSource(backend=ConnectorX())`, includes `sql` | medium |
| `filesystem` | `FilesystemSource`: local folders and S3, CSV/JSONL/Parquet readers | medium |
| `gcs` / `azure` | `gs://` / `az://` buckets for `FilesystemSource`, includes `filesystem` | medium |
| `documents` | `Documents` reader for `FilesystemSource`: PDFs to rows, OCR for scans (LiteParse, Anthropic SDK) | small |
| `dashboard` | `pipeline-dashboard serve`: the web app (Streamlit); `show` and `export` need nothing extra | medium |
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
| Target | `DuckDb(dataset=...)`, `Postgres(dataset=...)`; credentials from dlt env vars / `.dlt/secrets.toml` |
| `Resource(name, write, cursor, empty, backfill=None, chunk=None)` | One stream of rows into one table; `chunk` loads a large increment in bounded parts (Performance and scaling) |
| Write mode | `Append()`, `Replace()`, `Merge(primary_key=..., strategy=...)` |
| Cursor | `TimeCursor(path, start, format, on_missing)`, `ValueCursor(path, start, format, on_missing)`, or `None` |
| Time format | `IsoTimestamp`, `Strftime`, `UnixSeconds`, `UnixMillis`, `DateOnly`, `AwareDatetime`, `NaiveDatetime` |
| Value format | `Integer`, `RowVersion` (SQL Server `rowversion`: 8 bytes, stated as the integer it encodes) |
| Backfill | `TimeBackfill(start, every)`, `ValueBackfill(start, every)`, `RowBackfill(size)` |
| Source | `RestApiSource`, `SqlSource(name, backend, resources)`, `FilesystemSource`, `CustomSource`, `ScrapeSource` |

```python
from datetime import datetime, timezone

from dlt_framework.core import (
    FREEZE_DATA_TYPES, Duration, IsoTimestamp, Merge, Pipeline, Postgres, Resource, RowVersion,
    TimeBackfill, TimeCursor, ValueBackfill, ValueCursor,
)
from dlt_framework.sources.rest import RestApiSource
from dlt_framework.sources.sql import PyArrow, SqlSource

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
    target=Postgres(dataset="raw_shop"),
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
    target=Postgres(dataset="raw_erp"),
    schema_contract=FREEZE_DATA_TYPES,
    source=SqlSource(name="erp", backend=PyArrow(), resources={ledger: {"table": "Ledger"}}),
)
```

## Configuration

`.dlt/config.toml` states every operational setting, and every key is required: a missing one
stops the command with exit 2 and names the keys dlt looked for. Values resolve through dlt's
configuration system, highest precedence first:

1. environment variables, e.g. `RETRY__ATTEMPTS=3`;
2. a per-pipeline section, `[pipelines.<name>.<section>]`, which only needs the keys it changes;
3. the shared section.

Secrets (`monitoring.uri`, credentials) live in `.dlt/secrets.toml` locally and in
environment variables in containers.

| Section | Keys | Decides |
| --- | --- | --- |
| `[retry]` | `attempts`, `initial_wait_s`, `max_wait_s`, `give_up_after_s` | Jittered exponential backoff for transient failures; a step gives up after `attempts` or `give_up_after_s`, so a database down for minutes is ridden out |
| `[monitoring]` | `uri` (secret), `sample_every_s`, `flush_every_s` | Where monitoring goes, and how often memory and CPU are sampled and written |
| `[guards]` | `future_cursor_tolerance_s` | How far ahead of now a time cursor may move before the run refuses it |
| `[logging]` | `format`, `level` | One log format for framework and dlt lines |

```toml
[pipelines.acme_shop.retry]
give_up_after_s = 3600          # this API has hour-long maintenance windows
```

dlt's own performance settings live in the same file: `[extract] workers`,
`[data_writer] file_max_items`, `[normalize]`, `[load]`, and `[runtime] request_*` for HTTP
retries.

- **DuckDB's own settings** (`memory_limit`, `threads`, `preserve_insertion_order`) go in
  `[destination.duckdb.credentials.global_config]`, as dlt names them. They are set on the file
  as it is opened, and every connection to it in the process shares them. Without a
  `memory_limit`, DuckDB takes up to 80% of the machine's memory. Every load job into one file runs
  on its one connection, so `[load] workers` above 1 adds memory there, not speed.
- **Folders pipelines meet at** (a spider downloads into one, a `FilesystemSource` reads it) go
  in `[folders]`, read with `buckets.folder("<name>")`: a bucket URL, or a local path relative to
  the working directory.

### Swapping implementations

Monitoring sinks and log formats are chosen from config by key. A new one is a
registered class; nothing in core changes.

| Kind | Chosen by | Shipped | Add one |
| --- | --- | --- | --- |
| Monitoring sink | `monitoring.uri` scheme | `ParquetSink` (`file`, `s3`, `gs`, `az`, ...), `DatabaseSink` (`postgresql`, `mssql`, `sqlite`) | `@sinks.register("scheme")` on a `Sink` with `from_uri`, `write`, `read` |
| Log format | `logging.format` | `text`, `json` | `@formatters.register("name")` on a `logging.Formatter` |

## Commands

```text
pipeline list                                             pipelines ready to run here
pipeline check [NAME]                                     readiness of every pipeline, or one built offline
pipeline run NAME [NAME ...] [--resource R ...] [--allow-initial-load] [--profile]
pipeline backfill NAME [NAME ...] [--resource R ...] [--from X] [--to Y] [--missing] [--profile]
pipeline status [NAME]                                    last runs, cursors, backfill coverage
pipeline health [NAME]                                    what is wrong now; exit 6 if anything
pipeline reconcile NAME [--resource R ...] [--source]     count what the destination holds
pipeline compact                                          merge monitoring files
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
- **Several names** (`pipeline run shop erp crm`) run those pipelines at the same time, each on
  its own thread of one process, because DuckDB lets only one process write a file. Each logs
  its own outcome, one failing does not stop the others, and the exit code is that of the first
  named to fail. They share one connection per DuckDB file and load to it one at a time, while
  extract and normalize run at once. `--resource` and `--allow-initial-load` need one name.
  Monitoring samples memory and CPU for the whole process.
- **status** shows, per resource:
  - the last run, its mode and outcome (`lost or running` when it never reported one);
  - the last success, the rows loaded, and the cursor's position;
  - backfill coverage: % of the declared history, the number of gaps, and the first gap.

  It reads monitoring only, never the destination.
- **health** judges each pipeline's latest run against its own history and lists what is wrong
  now (Monitoring: Health). Each finding is also an `ERROR`/`WARNING` log line with an `event`
  field, and the command exits 6 when there are any, so a scheduled `pipeline health` is an
  alert by itself.
- **reconcile** counts the destination's rows, distinct primary keys, and rows per declared
  backfill window, and with `--source` the source's own counts where it can give them (SQL).
  Counts go to the `reconciliations` frame; the totals and any window that disagrees are printed.
  It extracts and loads nothing.
- **compact** rewrites each Parquet monitoring frame as one file, deleting the files it read
  (files written meanwhile are kept). Run it on a schedule, or before loading the frames into a
  database.
- **--profile** samples every thread's stack every 10 ms into the `profiles` frame, for one run.
  It slows the run a little.

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
  - `PyArrow()` keeps exact types and runs 20–30× faster than `SqlAlchemy()`.
  - `ConnectorX()` is the fastest, with costs: it reads whole results into memory (it ignores
    `chunk_size`) unless `backend_kwargs={"return_type": "arrow_stream"}`, and drops timezones
    from tz-aware timestamps. The framework rejects it with a tz-aware time cursor, or with a
    `chunk` while not streaming, and derives its connection string from the credentials.
  - Install it with the `connectorx` extra.
- **Extract.** A source's resources always extract in parallel threads; `[extract] workers` sets
  how many, and `[sources.<name>.extract] workers = 1` serialises a rate-limited upstream. One
  large table is a single stream.
- **Normalize and load.** `[data_writer] file_max_items` rotates files so a large table spreads
  across normalize processes and load jobs. Normalize workers start with `spawn`: forking a
  process that runs threads can deadlock.
- **Large increments.** `Resource(chunk=N)` loads a big change as consecutive loads of about N
  new rows each (files, for a filesystem source). Each chunk commits its rows and cursor before
  the next starts, so memory, the size of each merge, and what a failure costs are bounded by N,
  not by how much changed upstream. A failed chunk keeps the chunks before it, and the next run
  resumes after them. This works for every source type and cursor format.
  - **Cursor order.** The cursor needs `row_order="asc"`: SQL sources add `ORDER BY` on the
    cursor and filesystem sources sort the listing; any other upstream must already return rows
    in that order.
  - **What it refuses.** `pipeline check` rejects a chunked resource that would skip or repeat
    rows: one with `lag`, null cursors included, `Replace`, or an `scd2` merge.
  - **How runs change.** A chunked resource loads on its own, after the pipeline's other
    resources. Only its first chunk is held to its empty policy. Backfills ignore `chunk`, since
    their windows already bound each load.
  - **Sizing.** A chunk ends with the page or batch that reaches N, so size N in multiples of the
    source's page (`chunk_size`, a REST page, `Files(files_per_page=...)`). Index the cursor
    column upstream: every chunk is an ordered read from the last value.
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
| `5` | out of memory (Python `MemoryError`, or the database's own limit; never retried) |
| `6` | `pipeline health` found something wrong |
| `130` | interrupted |
| `137` | not raised by the framework: Docker or the kernel OOM-killed the container (see Monitoring) |

A failure is one line saying what failed, where, on what input, and in which run, followed by
the root cause's traceback with its notes:

```
ERROR dlt_framework.cli: run sec_insider failed in extract of insider_owner_signature, window increment: OSError: Invalid Input Error: CSV Error on Line: 53542 ... (TerminalRunError, exit 3; raised at dlt_framework/sources/filesystem/source.py:63 in read_csv; while reading file:///data/landing/sec_insider/owner_signature/2016q1.tsv; run 3f9fe3f6...)
```

- **The resource that broke.** Resources extract together; the failure names the one dlt
  reports, not all of them.
- **The input.** A source adds what it was reading where the error itself does not say (the
  CSV reader adds the file; dlt's HTTP and SQL errors already name the URL or table).
- **The same in monitoring.** The `failed` row of `runs` holds every field, the chain of causes,
  and the redacted traceback, so a dashboard shows a failure without container logs.
- **Every line names its run.** Lines logged during a run carry `run_id` (and `load_id` during a
  load), in brackets in the text format and as fields in JSON, to join logs to monitoring.

- **One handler for every line.** dlt's own logger is set to `WARNING` and shares the framework's
  handler, so `[logging] format` controls every line.
- **Secrets are masked.** The handler masks URL passwords and query-string values
  (`?api_key=***`) in every line, tracebacks included. Monitoring `error_message` is masked the
  same way.
- **Only errors known to heal are retried.** The network, a busy or restarting database, and
  what dlt's destinations mark transient are retried briefly (`[retry]`), then exit 4. Anything
  else fails at once: bad credentials and schema contracts exit 3, a missing secret exits 2, and
  out of memory exits 5. An unknown error is treated as a bug and exits 3.
- **Retries belong to the scheduler.** The in-process retry only rides out a blip. Retry exit 4
  from the scheduler, never 2, 3 or 5. A run that failed after extracting keeps its package, and
  the next run loads it before extracting anything new. Under an orchestrator that retries for you,
  set `RETRY__ATTEMPTS=1` so no container waits out a backoff, and keep `DLT_DATA_DIR` persistent,
  or the retry extracts again.

## Monitoring

Every run writes append-only Polars frames to the monitoring sink stated for the environment.
Measures are taken around the code, never inside it: the run hands each step and dlt's own
step results to the monitor, which samples the process from a thread of its own. They form a
star schema with readable, deterministic keys:

| Frame | One row per | Keys | Answers |
| --- | --- | --- | --- |
| `pipelines` | pipeline, per run | `pipeline_id`, `run_id` | Source type and name, destination, dataset |
| `resources` | resource, per run | `resource_id`, `pipeline_id`, `run_id` | Write mode, cursor kind/path/start, backfill kind, empty policy, full definition |
| `runs` | `started` / `succeeded` / `failed` / `lost` event | `run_id`, `pipeline_id` | When, mode, outcome, duration, exit code, rows, peak RSS against the container limit or host memory, CPU seconds, code version; on failure the type, step, resource, window, root cause, where it was raised, input notes, cause chain, and traceback |
| `steps` | step attempt (`sync`, `pending`, `extract`, `normalize`, `load`) | `run_id`, `load_id` | Wall and CPU seconds, attempt number, outcome and cause: retries, and whether a slow step was computing or waiting |
| `loads` | resource × load | `load_id`, `resource_id`, `run_id` | Rows, nested rows, bytes, the resource's own extract span, window bounds, part (chunk or window number), the cursor before and after |
| `tables` | destination table × load | `load_id`, `resource_id`, `table` | Rows, bytes, load jobs, retries, and load seconds per table, nested tables included |
| `samples` | moment of a run | `run_id` | RSS, CPU %, threads, IO MB/s, container limit, host memory free, and the step and resources running |
| `profiles` | function × step of a profiled run | `run_id` | Own and total stack samples, and their share of the step |
| `reconciliations` | resource (and backfill window) × `pipeline reconcile` | `reconcile_id`, `resource_id` | Destination rows and distinct keys, against the source's count |

- **The keys.** `pipeline_id` is the pipeline name; `resource_id` is `<pipeline_id>.<resource>`;
  `run_id` is generated per run and is on every log line of it.
- **`load_id` links to the destination.** It is dlt's load id, the same value as
  `_dlt_loads.load_id` and `_dlt_load_id` on every row in the destination.
- **Definitions are written every run.** Join on `(run_id, resource_id)` for a resource as it was
  in that run, or keep the latest row per `resource_id` for a one-row-per-key dimension.
- **Busy or waiting.** A step's `cpu_s / duration_s` near 1 (or above, on several cores) is our
  code or dlt computing; near 0 it waited, on upstream in extract (a slow query, a missing index,
  a rate limit) or on the destination in load. `tables.load_s` then says which table.
- **Killed runs leave a trace.** Records are written as they happen, and while memory is above
  70% of the limit every sample is written at once, so a container killed for memory leaves its
  `started` row and its last minute. The next run of that pipeline writes a `lost` row for it,
  with its duration, peak memory, and the step it was in, and logs a `run_lost` warning.
- **Process-wide measures.** CPU, memory, and IO are the process's: pipelines run together in
  one process share them.
- **`code_version`** comes from `[monitoring] code_version`; set `MONITORING__CODE_VERSION` to the
  image tag or git SHA per build, so a change in behaviour can be traced to a release.

### Health

`pipeline health` reports, from the frames alone, each pipeline's state now. Thresholds are in
`[health]`, overridable per pipeline (`[pipelines.<name>.health]`):

| Event | Level | When |
| --- | --- | --- |
| `run_failed` | error | The latest run failed (with how many in a row) |
| `run_lost` | error | The latest run was killed |
| `run_stale` | error | No success for `stale_after_intervals` × the expected gap: `expect_every_s`, or, once there are five, the median gap between successes when the gaps are regular (runs by hand set no schedule) |
| `run_slow` | warning | The latest success took over `slow_after` × the p95 of recent ones |
| `rows_anomaly` | warning | A resource loaded none where it usually loads some (unless its empty policy is `allow`), or over `rows_spike` × its median |
| `memory_high` | warning | The latest run's peak left under `memory_headroom` of its memory |
| `retries_rising` | warning | A step was retried in 3 of the last 5 runs |
| `coverage_gap` | warning | Declared backfill history has a gap |
| `reconcile_mismatch` | warning | The latest reconciliation found duplicate keys, or counts that differ from the source |

### Alerts

Alerting belongs to the orchestrator: it sees what a process cannot report (an OOM kill, a job
that never started) and can alert on exit codes and on the `event` field of log lines. Where
there is none, `[alerts]` posts those events to one webhook, chosen in config, so the code knows
no service. It is off until `ALERTS__WEBHOOK_URL` (a secret) is set. `body` is a template of the
log line's fields, JSON-escaped and redacted; for Telegram:

```toml
[alerts]   # ALERTS__WEBHOOK_URL=https://api.telegram.org/bot<token>/sendMessage
body = '{"chat_id": "<chat>", "text": "$level $pipeline $event: $message"}'
```

Posting happens on a background thread, at most once per pipeline and event per `cooldown_s`, and
a failed post is one line on stderr (never the URL) that never fails a run.

## Dashboard

`dashboard/` draws the monitoring frames as a terminal-styled, black-and-neon dashboard: box
panels, sparklines, braille charts, gantt timelines, and heatmaps, every state a glyph as well
as a colour. It reads the frames and nothing else (no pipeline code, destination, or dlt state),
so it works for any pipeline and source type, wherever the frames can be read.

```bash
pipeline-dashboard show [PAGE] [PIPELINE] [--run RUN] [--from DAY --to DAY | --since 14d]
                        [--pipeline P] [--mode run]
pipeline-dashboard serve [--port 8501]                 # web app (the dashboard extra)
pipeline-dashboard export FOLDER [--since 14d]         # static HTML, linked, for any web host
```

Each command reads `MONITORING__URI` (as runs do), or `--uri`, and shows the last 14 days
unless given a range of days (`--from 2026-10-01 --to 2026-10-14`; a date range on the web). The design, and why, is in
`docs/dashboard-design.md`: each page answers one question with its first line, shows the
filters in force and what is selected, and every claim shows its evidence.

| Page | Answers |
| --- | --- |
| Overview | Is production OK? Incremental runs only: every pipeline worst first (last run, last success, recent runs, what is wrong now), open incidents with the command to run, what is at risk, and one line on backfills |
| Runs | What is this pipeline, what is it doing, and what happened in this run? One pipeline, picked from a dropdown and named at the top; what it is (source, destination, each resource's write mode, cursor, and history); its runs, newest first; the chosen run in full (what broke and the command, the error, steps, memory, each resource's rows and cursor before and after) |
| Backfills | What history is missing, and how do I fill it? Per declared history: a ribbon of what is loaded, and a row per missing range with its dates, why, and the command; holes in incremental cursors; the backfill jobs and where a failed one stopped |
| Incidents | What broke, is it still broken, and how often? Open and resolved incidents, each with all its causes counted, and causes by day |
| Tuning | What would make it faster, cheaper, steadier? The checks' findings with evidence, the evidence panels the records support, and the checks that could not run with what they need |

- **Incidents** (`model/incidents.py`): a streak of failed or killed runs of one pipeline in
  one mode, from the first to the next success. Open until then, resolved after, so the open
  list clears itself and the resolved list is the history.
- **Missing data** (`model/gaps.py`): gaps in a declared history (never loaded, or where a
  backfill failed), empty windows between full ones, windows off the source in reconciliation,
  holes in an incremental cursor, each with the backfill that fills it.
- **Checks** (`model/checks.py`): one class each, registered by being defined. A check states
  the data it needs and runs only when it is there; thresholds are `Limits`, overridable in
  `[dashboard.checks]`. A killed run is called a memory kill only when its peak was near a
  known limit.

- **Layers**: `app` (frontends) → `pages` → `model` (frames to facts, Polars) and `render`
  (primitives to styled text, encoded as HTML or ANSI) → `theme`. `lint-imports` enforces it,
  and that the framework never imports the dashboard. A new frontend is one module in `app/`; a
  new page is one function registered with `@page`.
- **Dirty data is handled in `model/facts.py`**: duplicate records, older record shapes, runs with
  no end (running until they outlive their pipeline's p99 duration, then lost, timed from their
  last sample), empty ranges ("NO SIGNAL"), and UTC throughout.
- **Health** adds what only its `[health]` thresholds can judge (late, slower than usual,
  unusual rows); incidents, checks, and gaps say the rest, from the frames alone.

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
- **Documents.** A spider that downloads PDFs states `documents` and calls
  `await self.save(response)`; see *Documents*.

## Documents

PDFs become rows in two pipelines that meet at a folder or bucket, so the extraction never
depends on where a document came from: a spider, SharePoint, or files dropped in a bucket.

1. **Download.** A spider states `documents`, a folder or bucket URL, and calls
   `await self.save(response)` on each PDF it finds. Files are named by content hash, so a document
   seen twice is stored once.
2. **Extract.** A `FilesystemSource` with a `Documents` reader (the `documents` extra) reads that
   folder into rows of a Pydantic model you state, then disposes of each file once its rows are
   committed.

```bash
pipeline run filings && pipeline run invoices
```

```python
class Invoice(BaseModel):
    number: str
    total: Decimal
    lines: list[Line]                # lands as the child table invoices__lines

reader = Documents(
    schema=Invoice,
    extract=(                        # tried in order; the first result that validates lands
        Template(                    # a known layout: local, free, exact
            name="acme",
            match="ACME Ltd",        # found only in this layout
            fields={
                "number": Text(r"Invoice (\S+)"),
                "total": Text(r"Total (\S+)"),
                "lines": Table(
                    columns={"item": 50, "amount": 200},  # left edge of each column, in points
                    key="amount",
                    starts=r"[\d.]+",   # a row starts where its amount is a number
                ),
            },
        ),
        Model(name="claude-haiku-5-5", instructions="A supplier invoice."),  # anything else
    ),
    on_invalid="raise",              # or "warn": log it, load the rest, leave the file
    once_loaded=Delete(),            # or Keep(), or Archive("s3://bucket/documents")
)
files = Files(bucket_url="file:///data/landing", file_glob="*.pdf", reader=reader)
```

- **Cost.** Templates and OCR run locally. `Model` runs at low effort, only on documents no
  template read, and is sent the text of a born-digital PDF or the pages of a scan. Each row
  records `extracted_by`.
- **Validation.** Template values arrive as text and the schema's types and validators parse
  them, so a layout that has drifted fails instead of landing wrong.
- **Layouts.** `Table` places words in columns by position rather than trusting detected cell
  borders, which break on shaded and borderless tables. `Document.read(data, None).pages` shows
  where each header word sits.
- **Scans.** `ocr=Ocr(languages=("eng",), tessdata="/usr/share/tessdata")` reads a scan with the
  Tesseract bundled in LiteParse, each word where it is printed, so printed scans are read by
  templates. `tessdata` holds the language data (the `tesseract-ocr-eng` package, or `None` to
  download it on first use). Pages are not turned upright, and tick boxes and handwriting are
  not text: `Model` is sent a scan's page images, so it still sees them.
- **Disposal.** `Delete()` and `Archive(bucket_url)` act only after the rows are committed, so a
  failed run loses nothing. Buckets use `sources.filesystem.credentials`, as listing does.
- **Keys.** `Model` reads `ANTHROPIC_API_KEY`. Merge on `document` (the file path) so a re-read
  file replaces its rows.

## Adding to the framework

| To add | Write | Touch core? |
| --- | --- | --- |
| A pipeline | `sources/<type>/pipelines/<system>.py` with a `Pipeline(...)`; then `pipeline check <name>` and `pipeline run <name>` | No |
| A source type (GraphQL, MongoDB, Shopify) | `sources/<type>/`: subclass `Source[ConfigT]`, implement `resource`, list `windows`, add its quirks in `validate`. Work due once rows are committed goes to `extraction.after_commit`. A library the core lacks becomes an extra named after the source type in `pyproject.toml`, plus a row in *Installing* | No |
| A destination | A `Target` subclass naming its dlt `destination` and the driver it `requires()` | Yes, `core/target.py` |
| A cursor format | A `TimeFormat` or `ValueFormat` with `render` and `parse` | No, if it lives in the source package |
| A sink or log format | A registered class (see *Swapping implementations*) | No |

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

A pipeline that needs a system package states it at build time too, e.g. OCR language data:
`--build-arg EXTRAS="--extra duckdb --extra documents" --build-arg SYSTEM_PACKAGES="tesseract-ocr-eng"`
(`tessdata="/usr/share/tesseract-ocr/5/tessdata"`).

The scheduler must:
- guarantee one active job per pipeline name across hosts (the built-in lock covers one host);
- deliver `SIGTERM` with a grace period;
- alert on non-zero exits.

Keep `DLT_DATA_DIR` persistent, or keep `restore_from_destination` (on in `.dlt/config.toml`) on
ephemeral runners.

## Development

```bash
uv sync --all-extras                              # every extra plus the dev tools
uv run ruff check dlt_framework dashboard tests
uv run ruff format --check dlt_framework dashboard tests
uv run lint-imports
uv run mypy dlt_framework dashboard
uv run pytest --cov=dlt_framework --cov=dashboard --cov-branch --cov-fail-under=90
```

`TEST_POSTGRES_URL=postgresql://...` also runs the Postgres tests. Guidance for building and
changing pipelines, including how dlt state works underneath, is in `.claude/skills/dlt-pipelines/`.

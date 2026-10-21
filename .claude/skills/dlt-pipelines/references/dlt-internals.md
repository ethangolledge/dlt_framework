# How dlt behaves underneath

Facts about dlt 1.29.1 that decide whether a run is safe. Each section names the vendored doc page
(`dlt-docs/...`) or installed source module (`.venv/lib/python*/site-packages/dlt/...`) that proves
it. If the pinned dlt version changes, re-verify this file against the new source before trusting
it.

## Contents

1. Pipeline identity and the working directory
2. Where state lives and how it is restored
3. How incremental state is keyed
4. How a cursor actually filters
5. Bounded (backfill) incrementals are stateless
6. Run lifecycle and pending packages
7. Write dispositions and the staging dataset
8. Schemas and contracts
9. Destructive switches
10. Inspecting a pipeline

## 1. Pipeline identity and the working directory

A pipeline is identified by `pipeline_name`. Its working directory is
`<pipelines_dir>/<pipeline_name>` (`DLT_DATA_DIR` sets the root). It holds `state.json`, schemas,
extracted/normalized load packages, and traces. Every process that creates a pipeline with the same
name and pipelines dir shares that directory and its state.

- Never run two processes with the same pipeline name and working dir at once; dlt cannot protect
  its state or temp files (`dlt-docs/reference/performance.md`, "Pitfalls").
- `dataset_name` is the destination schema. It is independent of the pipeline name and may be
  shared by several pipelines.

Source: `dlt-docs/general-usage/pipeline.md` ("Pipeline working directory").

## 2. Where state lives and how it is restored

State is a JSON dict committed **atomically with the data** into `<dataset>._dlt_pipeline_state`,
one row per state version, keyed by `pipeline_name`. A run only advances the stored cursor when its
load package commits.

`Pipeline.sync_destination()` (`dlt/pipeline/pipeline.py`) runs before every `run`:

| Situation | What dlt does |
| --- | --- |
| Destination has state for this `pipeline_name` with version >= local | Replaces local state and schemas with the destination copy. |
| Destination has **no** state row for this `pipeline_name` | Logs at **INFO** "The state was not found in the destination" and continues as a first run. |
| Dataset does not exist at the destination but local state does | Wipes the local working state; the run becomes a first run. |
| Remote state has a different `dataset_name` | Logs a warning and continues with the current dataset name. |

The second and third rows are the silent full-load paths: nothing fails, and the incremental falls
back to its `initial_value`. `restore_from_destination=false` skips restoration entirely, so an
ephemeral runner with that setting starts every run from `initial_value`.

Source: `dlt-docs/general-usage/state.md` ("Syncing state with destination").

## 3. How incremental state is keyed

`Incremental.get_state()` (`dlt/extract/incremental/__init__.py`) reads:

```text
state["sources"][<source name>]["resources"][<resource name>]["incremental"][<cursor_path>]
  = {"initial_value", "last_value", "unique_hashes", "start_value"}
```

- `<source name>` is `DltSource.name`, which is also the schema name. In this framework it is
  `Source(name=...)`, and `<resource name>` is `Resource(name=...)`, except where the cursor
  lives on a different dlt resource. For example, the filesystem file listing is stored as
  `<name>_files_<name>`; `Source.state_key` returns the real key.
- The lookup also implicitly includes `pipeline_name`, `dataset_name` and the destination location
  from section 2.

Changing **any** of these keys makes the lookup empty, and the resource restarts from
`initial_value`: destination location, `dataset_name`, `pipeline_name`, source name, resource
name, `cursor_path` (for a `Cursor` with `fallbacks`, the tracked path is its `column`). An `initial_value` of `None` means no lower bound at all: the whole
upstream history is extracted.

## 4. How a cursor actually filters

There are two separate mechanisms, and only one of them protects the upstream system:

1. **Upstream filtering**: your code (or a `rest_api` placeholder such as
   `{incremental.start_value}`) puts the cursor into the request, SQL predicate, or connector
   argument. This is what stops a 30-million-row extraction.
2. **Client-side filtering**: `dlt.sources.incremental` is a filter step that runs after extraction
   (and after `add_map`/`add_filter`). It drops rows outside `[start, end)` and updates
   `last_value`.

If (1) is mis-wired, (2) still makes the destination look correct. The run downloads everything,
discards most of it, and loads a plausible delta. Only request counts, extract metrics, or a test
that captures the outbound request reveal it.

Other cursor behaviour that matters:

- `range_start="closed"` (default) re-reads the boundary value and deduplicates boundary rows with
  `unique_hashes`, using the resource primary key or a row content hash.
  `range_start="open"` disables that deduplication.
- `on_cursor_value_missing="raise"` (default) fails on rows without a cursor value. Change it only
  deliberately; `"include"` lets such rows through on every run.
- `row_order="asc"|"desc"` stops the generator at the first out-of-range row. Use it **only** if
  upstream guarantees that order; otherwise data is silently skipped.
- `lag` re-reads a trailing window on every run and disables boundary deduplication. Pair it with
  `merge`.
- `IncrementalCursorInvalidCoercion` means the `initial_value` type does not match the cursor
  field's type.

Source: `dlt-docs/general-usage/incremental/cursor.md`, `.../lag.md`, `.../troubleshooting.md`.

## 5. Bounded (backfill) incrementals are stateless

When `end_value` is set, `get_state()` returns a throwaway mock state. Nothing is read from or
written to pipeline state, and boundary deduplication is off. `end_value` without `initial_value`
raises. The range is half-open: `[initial_value, end_value)` by default (`range_start="closed"`,
`range_end="open"`).

Consequences:

- A bounded run never advances or corrupts the incremental cursor.
- A bounded run gives no resume point. This framework deliberately keeps backfills stateless:
  progress is logged per chunk, and an interrupted backfill is resumed by re-running from the
  last logged chunk end.
- Re-running a chunk re-loads it. Only `merge` with a stable primary key makes that idempotent.

dlt also supports injecting `[start, end)` through `TimeIntervalContext`, or through
`DLT_INTERVAL_START`/`DLT_INTERVAL_END`, for incrementals declared with
`allow_external_schedulers=True`. It raises `ExternalSchedulerNotAvailable` rather than silently
falling back to state when no interval resolves. This framework passes windows explicitly
instead, because interval injection rejects string cursors.

dlt binds state and filtering only to a resource parameter **annotated** as `Incremental`.
Passing an incremental into an unannotated parameter silently does nothing: no filter, no state.
`CustomSource` wraps user functions in an annotated generator for this reason.

Source: `dlt-docs/general-usage/incremental/cursor.md` ("Using `end_value` for backfill",
"Injecting and reading the current interval").

## 6. Run lifecycle and pending packages

`pipeline.run()` is `sync_destination` → if pending packages exist, normalize/load them **and
return without extracting** → `extract` → `normalize` → `load`. This framework calls the steps
separately (`Run.__call__`). It drains pending packages first with `pipeline.run([])`, then
extracts.

- An extracted package contains the state update. Deleting pending packages
  (`drop_pending_packages`) discards both the data and its cursor advance.
- With `load.raise_on_failed_jobs=true`, a failed job aborts the package. Its load id never reaches
  `_dlt_loads` and its state is not visible.
- `replace` with the default `truncate-and-insert` truncates before inserting, so it is not atomic.
  `merge` only swaps a table in once all of its staging data is loaded.

Source: `dlt-docs/running-in-production/running.md` ("What `run` does inside", "Failed jobs",
"Partially loaded packages").

## 7. Write dispositions and the staging dataset

| Disposition | Use for | Staging dataset |
| --- | --- | --- |
| `append` | Immutable events with a reliable cursor. Replays duplicate rows. | No |
| `replace` | Complete snapshots. An empty extract clears the table. | With `insert-from-staging` / `staging-optimized` (this repo uses `insert-from-staging`) |
| `merge` (`delete-insert`, `upsert`, `scd2`, `insert-only`) | Mutable entities and any replayable range. Needs `primary_key` and/or `merge_key`. | Yes |

The staging dataset name is `staging_dataset_name_layout % dataset_name`, which defaults to
`<dataset>_staging`. **Two pipelines that merge into the same dataset concurrently share the same
staging tables and can clobber each other.** Give each pipeline its own layout
(`dlt-docs/reference/performance.md` "Pitfalls" item 3; `dlt-docs/dlt-ecosystem/staging.md`).
`BackfillRun` uses `%s_backfill_staging` for this reason.

The `replace` strategy is destination-wide (`[destination] replace_strategy`), not per table. This
repo sets `insert-from-staging` in `.dlt/config.toml`, so a replace never leaves an empty table
mid-load.

Switching an existing table from `append`/`replace` to `merge` has root-key caveats; read
`dlt-docs/general-usage/merge-loading.md` ("Switch from append/replace to merge") first.

## 8. Schemas and contracts

- The schema name equals the source name. Schema versions are stored in `<dataset>._dlt_version`,
  and `sync_destination` restores the newest version for that name.
- The framework default contract (`Pipeline.schema_contract`) is
  `tables: evolve, columns: evolve, data_type: freeze`. New columns load; a type change fails
  instead of creating a variant column.
- Identifiers are normalised to snake_case (`dlt-docs/general-usage/naming-convention.md`). A
  resource name or JSON key that normalises differently than expected changes the table or column
  name.

Source: `dlt-docs/general-usage/schema.md`, `.../schema-contracts.md`, `.../schema-evolution.md`.

## 9. Destructive switches

None of these may appear in a scheduled code path. They are operator actions only.

- `dev_mode=True`: creates a new dataset suffix and resets state on every run.
- `refresh="drop_sources" | "drop_resources" | "drop_data"`: wipes state and drops or truncates
  tables, forcing a reload from `initial_value`.
- `dlt pipeline <name> drop ...` / `--drop-all` / `--state-paths`: drops tables and/or state.
- Dropping the destination dataset resets the pipeline to its first run.

Source: `dlt-docs/general-usage/pipeline.md` ("Refresh pipeline data and state"),
`dlt-docs/reference/command-line-interface.md` ("dlt pipeline drop").

## 10. Inspecting a pipeline

Point the CLI at the same working directory the job uses (`--pipelines-dir` or `DLT_DATA_DIR`):

```bash
dlt pipeline --list-pipelines
dlt pipeline -v <pipeline_name> info      # includes per-resource incremental state
dlt pipeline <pipeline_name> trace        # last run steps and timings
dlt pipeline <pipeline_name> failed-jobs
dlt pipeline <pipeline_name> sync         # re-download state/schemas from destination (destructive to local dir)
```

Check `last_value` before and after a run to prove the cursor advanced. The `Bind incremental on
<resource>` log line shows the `start_value` each run actually used.

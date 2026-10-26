# Running pipelines: identity, incremental runs, backfills

This is the operating model for every pipeline built with this framework. The mechanics behind each
rule are in `dlt-internals.md`; read that first if a rule seems arbitrary.

## Contents

1. Names are state keys
2. One pipeline definition, two run types
3. Choosing a cursor and a backfill
4. Bootstrapping a resource without a full load
5. Full-load guards

## 1. Names are state keys

Every name below is part of the key dlt uses to find an incremental cursor
(`dlt-internals.md` §2–3). Renaming one after the first production load makes dlt restart that
resource from its cursor `start`. The lost-state guard (G3) catches this and refuses to run, but
the fix is still a migration.

| Name | Convention | Example | Set in |
| --- | --- | --- | --- |
| Pipeline | `<client>_<system>`, snake_case, no `__`, permanent | `acme_shop` | `Pipeline(name=...)` |
| Backfill pipeline | `<pipeline>__backfill`, **derived by `BackfillRun`, never configured** | `acme_shop__backfill` | `core/run.py` |
| Dataset | `raw_<system>`, shared by both run types | `raw_shop` | `DuckDb(dataset=...)` / `Postgres(dataset=...)` |
| Source | Upstream system, snake_case, permanent | `shop` | `Source(name=...)` |
| Resource | snake_case plural entity; also the destination table name | `orders`, `order_line_items` | `Resource(name=...)` |
| Cursor path | The upstream field path, verbatim | `updated_at` | `TimeCursor/ValueCursor(path=...)` |
| DuckDB file | never equal to a dataset name (`DuckDb` enforces) | `/data/warehouse.duckdb` | `DESTINATION__DUCKDB__CREDENTIALS` |

The upstream name can differ from the resource name. For example, SQL Server `dbo.Orders` maps to
resource `orders` through the SQL config key `table`.

Rules:

- A pipeline is a group of resources on one schedule. `--resource` runs a subset (state is per
  resource). Never run the same pipeline name in two processes at once: `PipelineLock` stops it on
  one host; across hosts the scheduler must. Resources already extract in parallel threads. To run
  pipelines at the same time, name them in one command (`pipeline run shop erp`): they run on
  threads of one process, which DuckDB needs, sharing one connection per file and loading to it one
  at a time. Pipelines loading elsewhere can also run as separate processes.
- Treat renaming any name in the table as a **migration**, not a refactor. It needs a written plan
  for the stored cursor (keep the old name, or re-seed through a backfill and
  `--allow-initial-load`) and user sign-off.
- Never derive a name from a date, environment, run id, or anything else that varies between runs.

## 2. One pipeline definition, two run types

Both run types call the same spine, `Run.__call__` in `dlt_framework/core/run.py`:
sync → drain pending packages → `check` → for each unit, for each window: build → extract →
empty policy → normalize → load, stopping a unit early when `window.is_last(rows)`, or, for a
chunked resource's windows of `None`, once a chunk ends before its limit. They differ
only in class attributes and the `units` / `check` hooks:

| | `IncrementalRun` | `BackfillRun` |
| --- | --- | --- |
| Command | `pipeline run NAME [NAME ...] [--resource R ...] [--allow-initial-load]` | `pipeline backfill NAME [NAME ...] [--resource R ...] [--from X] [--to Y] [--missing]` |
| dlt pipeline name | `NAME` | `NAME__backfill` |
| Dataset / tables | `Target.dataset` | same dataset, same tables |
| Units | one: the selected resources, window `None` (resume from stored cursor); plus one per `chunk` resource, repeating window `None` until a chunk ends short | one per selected resource with a declared `backfill`, over its windows (narrowed by `--from/--to`, or the gaps for `--missing`) |
| Incremental state | read and advanced | untouched: time/value windows set `end_value` (mock state); row windows apply no cursor |
| Resume after interruption | automatic (dlt state; a kept package loads first) | stateless: `pipeline backfill NAME --missing` (coverage from the `loads` frame) |
| Staging dataset | `<dataset>_staging_run_<pipeline>` | `<dataset>_staging_backfill_<pipeline>` |
| Empty policy | enforced (on a chunked resource's first chunk) | not enforced (an empty chunk is normal; a short row page ends the unit) |
| `check` hook | future-cursor guard (G8), lost-state guard (G3) | none |

Never load history through `run`: no lowering a cursor `start`, no `refresh=`, no `dev_mode`, no
`dlt pipeline drop`. History always goes through `backfill`.

## 3. Choosing a cursor and a backfill

| Upstream has | Cursor | Backfill | Notes |
| --- | --- | --- | --- |
| Reliable timestamp | `TimeCursor(path, start, format, on_missing)` | `TimeBackfill(start, every)` | Pick the `format` the source uses; each source package handles the bounds. |
| Timestamp that can be null | `TimeCursor(..., fallbacks=(...))` | `TimeBackfill` | REST/custom only. SQL needs the coalesce done upstream (view or computed column). |
| Monotonic number: id, sequence | `ValueCursor(path, start, format=Integer(), on_missing)` | `ValueBackfill(start, every)` | |
| SQL Server `rowversion` | `ValueCursor(path, start, format=RowVersion(), on_missing)` | `ValueBackfill(start, every)` | `start`, `every`, `--from/--to` are the integers the rowversion encodes (`CAST(rv AS BIGINT)`); they reach SQL as `binary(8)`. |
| Nothing usable | `None` (every run loads everything; use `Replace`) | `RowBackfill(size)` | SQL needs `order_by`; REST needs `offset_param`/`limit_param`. |
| CDC / a stream the source tracks itself | `None` | none | The source type owns its position. |
| A scraped, newest-first listing | `TimeCursor` / `ValueCursor` with `row_order="desc"` | the spider maps `self.window` to pages or date filters | dlt stops the crawl at the first item below the cursor (closed range: the item *at* the cursor is re-read). |

A source type lists the window kinds it can push upstream (`Source.windows`). Declaring a backfill
it cannot push fails at definition time.

A large increment (a batch job rewriting millions of rows) needs no backfill: give the resource
`chunk=N` and `row_order="asc"` on its cursor. Each chunk commits its rows and cursor before the
next, so one run loads the change as consecutive bounded loads, and a failure keeps the chunks
before it. `Resource` refuses a chunk that would skip or repeat rows (`lag`, null cursors
included, `Replace`, `scd2`).

Write mode is always your stated choice. Replaying a backfill chunk is idempotent with `Merge`; an
`Append` resource duplicates replayed rows.

## 4. Bootstrapping a resource without a full load

1. Choose the boundary `T`: a UTC instant, normally the start of the current day.
2. Declare the cursor with `start=T`, and a backfill whose `start` is the earliest history you
   need. The backfill ends at `T` by construction, half-open, so `T` is loaded exactly once.
3. Run `pipeline run NAME`. The first run loads `[T, now)` and stores the cursor.
4. Run `pipeline backfill NAME`. Steps 3 and 4 are independent and may overlap.
5. Verify with `pipeline status NAME`: the cursor advanced, and backfill coverage reaches 100%
   with 0 gaps. `pipeline backfill NAME --missing` loads whatever is missing.

## 5. Full-load guards

| Guard | Prevents | Enforced by |
| --- | --- | --- |
| G1 Explicit cursor start | A missing lower bound extracting all history | `start` is a required cursor field |
| G2 Upstream binding | Cursor filtering only client-side while the request pulls everything (`dlt-internals.md` §4) | Per-source tests capturing the outbound request or SQL (`tests/test_sources.py`); every new source type or framework key needs one |
| G3 Lost-state guard | Renamed identity, wrong dataset, or new destination restarting from cursor start over existing data | `IncrementalRun.check` with `Source.state_key`; override deliberately with `--allow-initial-load` |
| G4 Immutable identity | Accidental renames | §1 rules; `Pipeline` rejects duplicate names |
| G5 Pre-flight | Unknown `start_value` after a change | `pipeline check NAME`, then compare `cursor_start` (`resources` frame) with `cursor_last_value` (`loads` frame, joined on `run_id`, `resource_id`) (or `dlt pipeline -v NAME info`) |
| G6 History only via backfill | History loaded through `run` | §2 |
| G7 Ordered stop only on ordered data | `row_order` skipping rows | Set `row_order` only when upstream ordering is documented and tested |
| G8 Future-cursor guard | One future-dated row stopping a time cursor until that date | `Run._check_cursors` after extract and in `IncrementalRun.check`; tolerance in `[guards]` |
| G9 State never ahead of data | A terminal failure after extract leaving the local cursor past rows that never landed | `Run._kept_or_discarded`: discards local state (`pipeline.drop()`) unless the failure is transient or an interrupt (`dlt-internals.md` §6) |

When a requested change touches any name in §1, a cursor field, `start`, `format`, `row_order`,
`lag`, `on_missing`, the destination, or `restore_from_destination`, state the effect on the next
run's `start_value` before making the change.

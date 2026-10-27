# Engineering standards

This framework is reused across companies for years. It must stay small enough for a person to
read without AI. Every change must leave it smaller or clearer, never wider. The usual failure is
an agent that does not read what exists, adds a parallel implementation, and breaks a guarantee
the original upheld. These rules exist to stop that.

## 1. Read before you write

Before adding any function, class, module, flag, or dependency:

1. Find where the concept already lives:

   | Concept | Lives in |
   | --- | --- |
   | Exceptions, exit codes, error location (`step`/`resource`/`window`), cause chain (`causes`, `root_cause`), failure fields (`error_fields`), masking secrets in URLs (`redact`), name validation | `core/errors.py` |
   | Window kinds (time, value, rows) and `is_last` | `core/windows.py` |
   | How a source stores cursor values (`render`/`parse`) | `core/formats.py` (`TimeFormat`, `ValueFormat` subclasses) |
   | Cursor kinds, fallbacks, dlt incremental settings | `core/cursor.py` (`Cursor`, `TimeCursor`, `ValueCursor`) |
   | Backfill declarations and their windows, `Duration` | `core/backfill.py` |
   | Write modes | `core/write.py` (`Append`, `Replace`, `Merge`) |
   | Resource definition | `core/resource.py` |
   | Destinations (`DuckDb`, `Postgres`): dataset, credentials, DuckDB's shared connection and load lock | `core/target.py` |
   | Source contract: `resource`, `validate`, `state_key`, `windows`, shared preparation; `Extraction` (one build's window and its after-commit work) | `core/source.py` |
   | Pipeline identity, registry, discovery | `core/pipeline.py` |
   | Retry policy (a configspec) and classification; `MemoryError`, permission and terminal errors never retried | `core/retry.py` |
   | Settings every environment states (retry, monitoring, health, guards, logging) | `core/settings.py` |
   | Resolving settings (`resolve`, `REQUIRED`) and registries chosen by config | `core/config.py` |
   | Decoded cursor state (one place for the state path) | `core/state.py` (`stored_cursor`) |
   | Monitoring records → Polars frames, sinks (write, read, compact), sampler, profiler, monitor, backfill coverage, status, health | `core/monitoring/` |
   | Counting what the destination (and source) holds | `core/reconcile.py`; a source's own count is `Source.count` |
   | One log format for framework and dlt lines; fields bound to every line of a run (`logs.bound`) | `core/logs.py` |
   | The webhook fallback for alert events | `core/alerts.py` |
   | Run spine, incremental vs backfill, guards, rollback after extract, staging layout, lock | `core/run.py` |
   | PDFs to rows: templates and locators, model fallback, OCR, disposal after load | `sources/filesystem/documents/` |
   | Opening folders and buckets with one credentials setting | `core/buckets.py` |
   | Source types and their pipelines | `sources/<type>/source.py`, `sources/<type>/pipelines/` |
   | CLI and logging | `cli.py` |

2. Read the whole module you will touch and its tests, so you know which invariants it enforces.
3. If something close exists, **extend or parameterise it**. Never add a sibling that "sort of" does
   the same thing.
4. Check whether dlt already does it (`references/dlt-docs-index.md`). Pagination, auth, HTTP
   retry/backoff, incremental cursors, dedup, schema inference/evolution, state, and write
   dispositions belong to dlt. Re-implementing any of them is a defect.

## 2. Where new code goes

Dependency direction is `cli` → `sources` → `core`. Source types never import each other.
`lint-imports` enforces both rules in CI.

| New need | Goes in | Do not |
| --- | --- | --- |
| Feature for one source type (SQL Server rowversion casting, Shopify throttling, S3 manifest) | That source type's package only | Touch `core/` or another source type |
| Work due once an extract's rows are committed (releasing files, acknowledging a queue) | `extraction.after_commit(fn)` in the source's `resource()`: per build, never on the definition | State on a definition object, which every run of it shares |
| A new destination | A `Target` subclass in `core/target.py` naming its dlt `destination` and `requires()` | `if destination == ...` checks |
| A new SQL backend | A `Backend` subclass in `sources/sql/source.py` (`requires`, `validate`, `backend_kwargs`) | `if backend == ...` checks |
| A new source type (GraphQL, MongoDB, CDC) | `sources/<type>/`: subclass `Source[ConfigT]`, implement `resource`, list `windows`, extend `validate` (and `state_key` if the cursor lives on another dlt resource). Libraries the core lacks go in an extra named after the source type; declare what dlt imports only at run time in `requires()` (module -> extra); add the package to the import-linter independence contract | Edit `Source.build` or `Run`, or add the library to core dependencies |
| One-off extraction logic | `CustomSource` generator `fn(incremental, window)` | A new source type for a single API |
| A new cursor format | A `TimeFormat` / `ValueFormat` subclass with `render` and `parse`: in `core/formats.py` if generic, else in the source package | Branching on formats in adapters |
| A new cursor or window kind | A `Cursor` / `Window` / `Backfill` subclass in `core/` | `if kind == ...` chains |
| A new write behaviour (hard deletes, scd2 options) | A field on, or class beside, `Merge`/`Append`/`Replace` in `core/write.py` | Hints applied in adapters |
| Alerting on a run's outcome | The orchestrator, per task, from the exit code and the `event` field of log lines; where there is none, the one generic webhook in `core/alerts.py`, configured by URL and body template | A channel for a named service (Telegram, Slack) in code |
| A new health signal | A rule in `core/monitoring/health.py` reading the frames, its threshold a `HealthSettings` field | A threshold checked inside a run |
| A source that can count its rows | `Source.count(resource, window)` on its `Source` subclass | Counting in `core/reconcile.py` by source type |
| A new document layout | A `Template` (with `Text`/`Table` locators) in the pipeline that reads it | A new extractor class per layout |
| A new way to find a field, extract a document, or dispose of a file | A `Locator`, `Extractor`, or `Disposal` subclass in `sources/filesystem/documents/` | Branching in `Documents` |
| A new monitoring destination (HTTP collector, queue, warehouse) | `@sinks.register("scheme")` on a `Sink` implementing `from_uri`/`write`/`read` | Branching on destinations in `Monitor` |
| A new operational setting | A field on the relevant spec in `core/settings.py` (`= REQUIRED`), stated in `.dlt/config.toml` (the one exception: `alerts.webhook_url`, a secret that is off unless set) | A default in code, or `os.environ` reads |
| A new monitored metric | A field on the relevant record in `core/monitoring/records.py` (schema derives from it), filled by `Monitor` from what the run hands it or dlt's step results; a new frame also goes in `RECORDS` | A second schema definition, log lines parsed later, or measuring inside a loader |
| A new run type | Subclass `Run`; set class attributes, implement `units` (and `check`) | Copy `__call__` |
| Code two source types both need | `core/`, deliberately, with a test, once a second consumer exists | Copy it into both packages |

## 3. One concept, one implementation

- Logic that exists twice is a bug waiting to diverge. If two functions differ only by a value,
  merge them and pass the value. If they differ by one step, use a hook (§4).
- Constants and names are defined once and imported.
- Derived values are computed from their source of truth, never configured twice. Examples: the
  backfill pipeline name (`Pipeline.name + BackfillRun.suffix`), and a backfill's end (the cursor
  `start`).
- Delete code made redundant by your change, in the same change. Do not leave compatibility shims
  or deprecated paths "for later".

## 4. Explicit, modular definitions

- **Anything that decides what data lands is a required argument**, with no defaults. That
  covers write mode and merge strategy, cursor (or explicit `None`), cursor start/format,
  `on_missing`, empty policy, and schema contract. Optional arguments are only for features that
  are absent when unused.
- **Variants are classes, not flags.** No booleans like `replayable` or `supports_x`, and no
  string-mode switches in shared code. Behaviour lives on the variant (`hints()`, `bounds()`,
  `is_last()`, `windows()`), or capability is declared as a type list (`Source.windows`).

| Shape | Construct | Example here |
| --- | --- | --- |
| Immutable value or definition | `@dataclass(frozen=True, kw_only=True)` (add `slots=True` only if the class never calls `super()`) | `Resource`, `TimeCursor`, `Merge`, `Pipeline` |
| Variants of one concept | ABC + one small class per variant | `WriteMode`, `TimeFormat`, `Cursor`, `Window`, `Backfill`, `Source` |
| Shared sequence, varying steps | Template method; subclasses override hooks and class attributes | `Run.__call__` → `units`/`check`; `Source.build` → `resource`/`validate` |
| Implementations chosen by configuration | `Registry.register` decorator, looked up by key | sinks, log formats |
| Settings stated in config | dlt `@configspec`, fields `= REQUIRED` | `Retry`, `RunSettings` |
| Callable policy objects | `__call__` | `Retry`, `Run` |
| Scoped resources | `__enter__` / `__exit__` | `PipelineLock` |
| Pure helpers tied to one class | `@staticmethod` | `SqlSource._page`, `Retry.is_transient` |
| Dispatch on a variant | `match` on class patterns | `errors.exit_code` |

Inheritance rules:

- Shared behaviour lives in the base class **once**. A subclass that copies a base method and
  changes a line is wrong. Extract the line into an overridable hook instead.
- The base class owns the sequence. Subclasses override hooks and class attributes, never the
  sequence method.
- Keep hierarchies one level deep below the base. Use composition for orthogonal concerns, as with
  `Retry` and the monitoring sink.
- No mixins, registries, or plugin systems without two real consumers in the codebase.
- No class for a single stateless function. No getters or setters around plain attributes.

## 5. Code quality bar

- Full type hints. `mypy` passes under the repo's strict settings. Ruff lint and format pass at
  line length 100.
- Names say what a thing is in domain terms.
- Docstrings explain why and what a caller must know. Comments only for non-obvious reasons.
- Text that leaves the process (logs, monitoring) goes through `redact`; never put a
  credential or connection string into an exception message yourself.
- Raise the narrowest `RunError` subclass (with `step=` where known) so the CLI returns the right
  exit code and the failure line says where it happened. Never swallow an exception or downgrade
  it to a log line. The exception is a monitoring-sink failure, which must not mask a run's
  outcome.
- `__init__.py` files only re-export the public API (`pipelines/__init__.py` stays empty so
  discovery can walk it).
- Change only what the task needs. No drive-by refactors, renames, or formatting churn outside
  the change.

## 6. Tests prove behaviour, especially failure behaviour

- Every behaviour change ships with a test that fails without it.
- Use real dlt against DuckDB (fixtures in `tests/conftest.py`: `make_pipeline`, `Upstream`,
  `query`). Do not mock dlt.
- Test the dangerous paths explicitly: a window loading exactly its rows, lost state, empty
  extracts, transient vs terminal errors, replayed ranges not duplicating keys, production state
  unchanged by a backfill.
- Test core functionality only, by its outcome: rows landed, state committed or rolled back,
  the error type or exit code. Do not assert exact messages, log lines, table layout, rendered
  SQL, request order or timing, a shipped config value, or one feature's internals: such a test
  breaks a production release without a real fault. A test like that may help while building a
  feature; remove it once the feature is done.
- Run all gates before claiming done (commands in `SKILL.md`). Report exactly what ran and what
  did not.

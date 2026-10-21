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
   | Exceptions, exit codes, name validation | `core/errors.py` |
   | Window kinds (time, value, rows) and `is_last` | `core/windows.py` |
   | How a source writes time values | `core/formats.py` (`TimeFormat` subclasses) |
   | Cursor kinds, fallbacks, dlt incremental settings | `core/cursor.py` (`Cursor`, `TimeCursor`, `ValueCursor`) |
   | Backfill declarations and their windows, `Duration` | `core/backfill.py` |
   | Write modes | `core/write.py` (`Append`, `Replace`, `Merge`) |
   | Resource definition | `core/resource.py` |
   | Destination, dataset, staging layout | `core/target.py` |
   | Source contract: `resource`, `validate`, `state_key`, `windows`, shared preparation | `core/source.py` |
   | Pipeline identity, registry, discovery | `core/pipeline.py` |
   | Retry classification | `core/retry.py` |
   | Run spine, incremental vs backfill, lost-state guard, lock | `core/run.py` |
   | Notifications | `core/notify.py` |
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
| A new source type (GraphQL, MongoDB, CDC) | `sources/<type>/`: subclass `Source[ConfigT]`, implement `resource`, list `windows`, extend `validate` (and `state_key` if the cursor lives on another dlt resource) | Edit `Source.build` or `Run` |
| One-off extraction logic | `CustomSource` generator `fn(incremental, window)` | A new source type for a single API |
| A new time format | A `TimeFormat` subclass: in `core/formats.py` if generic, else in the source package | Branching on formats in adapters |
| A new cursor or window kind | A `Cursor` / `Window` / `Backfill` subclass in `core/` | `if kind == ...` chains |
| A new write behaviour (hard deletes, scd2 options) | A field on, or class beside, `Merge`/`Append`/`Replace` in `core/write.py` | Hints applied in adapters |
| A new alert channel (Slack, Teams) | A class with `send(event: RunEvent)`, passed via `Pipeline(notifiers=...)` | Channel checks in `Run` |
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
| Contract implemented outside the class tree | `typing.Protocol` | `Notifier` |
| Callable policy objects | `__call__` | `Retry`, `Run` |
| Scoped resources | `__enter__` / `__exit__` | `PipelineLock` |
| Pure helpers tied to one class | `@staticmethod` | `SqlSource._page`, `Retry.is_transient` |
| Dispatch on a variant | `match` on class patterns | `TimeCursor.bounds` |

Inheritance rules:

- Shared behaviour lives in the base class **once**. A subclass that copies a base method and
  changes a line is wrong. Extract the line into an overridable hook instead.
- The base class owns the sequence. Subclasses override hooks and class attributes, never the
  sequence method.
- Keep hierarchies one level deep below the base. Use composition for orthogonal concerns, as with
  `Retry` and notifiers.
- No mixins, registries, or plugin systems without two real consumers in the codebase.
- No class for a single stateless function. No getters or setters around plain attributes.

## 5. Code quality bar

- Full type hints. `mypy` passes under the repo's strict settings. Ruff lint and format pass at
  line length 100.
- Names say what a thing is in domain terms.
- Docstrings explain why and what a caller must know. Comments only for non-obvious reasons.
- Raise the narrowest `RunError` subclass so the CLI returns the right exit code. Never swallow an
  exception or downgrade it to a log line. The one exception is notifier failures, which must not
  mask a run's outcome.
- `__init__.py` files only re-export the public API (`pipelines/__init__.py` stays empty so
  discovery can walk it).
- Change only what the task needs. No drive-by refactors, renames, or formatting churn outside
  the change.

## 6. Tests prove behaviour, especially failure behaviour

- Every behaviour change ships with a test that fails without it.
- Use real dlt against DuckDB (fixtures in `tests/conftest.py`: `make_pipeline`, `Upstream`,
  `query`). Do not mock dlt.
- Test the dangerous paths explicitly: the cursor or window reaching the request or SQL (capture
  it), lost state, empty extracts, transient vs terminal errors, replayed ranges not duplicating
  keys, production state unchanged by a backfill.
- Run all gates before claiming done (commands in `SKILL.md`). Report exactly what ran and what
  did not.

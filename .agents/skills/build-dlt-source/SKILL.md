---
name: build-dlt-source
description: Add, update, or validate a production client extraction source in this dlt framework without changing framework core. Use when onboarding a REST API, GraphQL API, SQL database, MongoDB collection, or another credential-driven source; selecting endpoints, tables, or resources; defining append/replace/merge behavior, primary keys, empty-response policy, incrementals, pagination, or backfill bounds; declaring source and DuckDB/Postgres destination configuration; or creating files below dlt_framework/sources/.
---

# Build a dlt source

Build one production source for one isolated client deployment. Optimize for unattended recovery
and fast future maintenance, not for speculative connector reuse.

## Read the canonical guidance

Read the relevant repository documents before editing:

1. Always read `docs/source-authoring.md` for the source contract, write behavior, bounded-query
   rules, and required tests.
2. Read `docs/configuration.md` when adding environment variables, destination credentials,
   pipeline identity, worker settings, or retry controls.
3. Read `docs/operations.md` for backfills, restart behavior, retry/recovery, persistent state, or
   deployment changes.
4. Read `docs/architecture.md` before proposing framework-core changes, new destinations, state
   stores, schedulers, locks, or notification behavior.
5. Inspect the closest source module and its tests. Use `dlt_framework/sources/rest/dummyjson.py`
   for the basic contract and `tests/fixtures/sources/rest/backfillable.py` for bounded extraction.

Treat those files as the source of truth. Do not copy their connector examples or configuration
tables into this skill.

## Establish the extraction contract

Resolve these facts from the request, upstream documentation, and repository before coding:

- source selector and source protocol;
- credential and non-secret configuration variable names, never live secret values;
- resources/endpoints/tables/collections to load;
- pagination or database batching behavior and its termination condition;
- incremental cursor, ordering, initial value, overlap, and late-arriving-record behavior;
- `append`, `replace`, or `merge`, including stable primary keys for merge;
- `allow`, `warn`, or `fail` when a resource extracts zero records;
- whether upstream accepts explicit inclusive-lower/exclusive-upper bounds;
- certified destination (`duckdb` or `postgres`) and destination credential variable name.

Discover repository facts before asking. Ask the user only when missing semantics would change data
correctness. Never guess a primary key, write disposition, empty-snapshot behavior, or cursor.

## Implement the smallest source

1. Add or edit one `dlt_framework/sources/{kind}/{source_name}.py` module.
2. Prefer a maintained native dlt connector. Add a custom request loop only when the connector
   cannot express the upstream protocol.
3. Accept `SourceContext`; when `context.selected_resource` is set, construct only that resource so
   unselected endpoints or reflected tables do no work.
4. Keep authentication, pagination, incrementals, type conversion, write disposition, and primary
   keys in native dlt resource configuration.
5. Export `SOURCE = define_source(...)` and declare one `ResourcePolicy` per resource. Keep the
   contract and actual resource names identical.
6. Use `dlt.secrets.value` for credentials and `dlt.config.value` for non-secret source settings.
7. Keep records raw. Do not add business transformations, orchestration, alerts, or destination
   adapters to a source module.
8. Do not add `__init__.py`; this repository uses namespace packages.

For a supported source pattern, do not edit `dlt_framework/core/` or
`dlt_framework/execution/`. If a real source exposes a missing framework capability, stop and
describe the concrete gap and failure mode before expanding core.

## Apply reliability rules

- Use `merge` with a stable source key for mutable entities and every backfillable resource.
- Use `replace` only for complete snapshots; explicitly decide whether an empty result may clear
  the target.
- Use `append` only for immutable events with a tested cursor/offset and overlap policy.
- For backfills, bind bounds through `rest_incremental_config`, `incremental_for`, or
  `range_kwargs`; declare `backfill=True`; and keep the range half-open.
- Prove both bounds reach the actual outbound request, SQL predicate, or connector arguments.
  Merely calling a bound helper is insufficient.
- Protect custom pagination against repeated/non-advancing cursors, malformed payloads, and absent
  termination fields.
- Distinguish retryable 429/5xx/timeouts from authentication, validation, schema, and other terminal
  failures. Do not build an independent unbounded retry loop inside a source.
- Add a connector helper or dependency only for this real source and only with failure-focused
  tests. Do not create empty connector folders or generic abstractions in anticipation of clients.

## Validate before handoff

Add tests proportional to the source:

- contract/discovery and selected-resource construction;
- exact pagination/cursor behavior and termination;
- empty `allow`, `warn`, or `fail` behavior as declared;
- transient and terminal upstream failures;
- exact half-open bounds and idempotent overlap when bounded;
- additive schema change acceptance and incompatible type rejection when the source has a stable
  schema fixture;
- DuckDB integration, plus Postgres integration when destination-specific behavior is involved.

Run the repository gates:

```bash
uv run dlt-framework validate KIND/NAME [--resource RESOURCE]
uv run ruff check dlt_framework tests
uv run ruff format --check dlt_framework tests
uv run mypy dlt_framework
uv run pytest --cov=dlt_framework --cov-branch --cov-fail-under=90
```

Run a live source read or destination write only when credentials exist and the user authorizes it.
Do not claim end-to-end success when only mocks or local fixtures ran.

## Update operator-facing configuration

- Add redacted variable names and safe placeholders to `.env.example` when the source introduces
  configuration.
- Require stable `PIPELINE_NAME` and `DATASET_NAME` in production examples.
- Keep a DuckDB filename different from `DATASET_NAME`.
- Keep credentials out of `.dlt/config.toml`, logs, tests, commits, and the final response.
- Update canonical docs only when framework behavior changes; source-specific facts belong beside
  the source or in its tests, not in generic framework documentation.

## Handoff

Report:

- source path and CLI selector;
- resource-to-table mapping, write disposition, primary key, empty policy, and backfill support;
- required source and destination variable names with redacted placeholders;
- quality commands and contract/integration tests run;
- whether a live extraction/load occurred;
- any unresolved upstream permission, rate-limit, historical-access, or recovery constraint.

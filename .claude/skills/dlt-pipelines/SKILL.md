---
name: dlt-pipelines
description: Build, change, run, backfill, debug, or review dlt (data load tool) pipelines in this framework. Use for any work touching pipeline definitions, source types (REST, SQL, filesystem/S3, custom), resources, write modes, incremental cursors, backfill windows, pipeline/dataset/source/resource naming, dlt state, destinations (DuckDB/Postgres), the framework under dlt_framework/, or questions about how dlt behaves. Includes offline dlt docs pinned to the installed version.
---

# dlt pipelines

This repository is a small library for named, independently runnable dlt pipelines. The two ways
to break it are:

1. A run that silently becomes a full extraction, because incremental state was not found or the
   cursor never reached upstream.
2. A change that duplicates or bypasses existing logic.

Every rule below exists to prevent one of those.

## Non-negotiables

1. **Do not work from memory of dlt.** Look it up in the offline docs
   (`references/dlt-docs-index.md`) or the installed source. Do not web-search unless the user
   agrees.
2. **Names are state keys.** Pipeline, dataset, destination, source, resource, and cursor path
   together locate the incremental cursor. Never rename one casually
   (`references/running-pipelines.md` §1).
3. **Incremental and backfill are different dlt pipelines.** `run` uses `NAME`. History loads only
   through declared, stateless backfills under `NAME__backfill` (§2). Never load history by
   lowering a cursor `start`, `refresh=`, `dev_mode`, or dropping state.
4. **Every choice that decides what lands is stated, and the cursor or window provably reaches the
   upstream request.** No defaults for write mode, cursor, format, or empty policy. dlt's
   client-side filter hides a broken upstream filter (`references/dlt-internals.md` §4).
5. **Source-specific code stays in its source package; shared code lives once in core.** Extend
   through subclasses, never by editing the spine or branching on types
   (`references/engineering-standards.md` §2, §4).
6. **State the consequence.** Before changing anything that affects a cursor or identity, say what
   the next run's `start_value` will be.

## Reference map

Read only what the task needs, but always read the file for the area you are changing.

| File | Read when |
| --- | --- |
| `references/dlt-internals.md` | Anything touching state, incrementals, write dispositions, staging, schemas, or failure recovery. Verified facts about dlt underneath. |
| `references/running-pipelines.md` | Naming, running, backfilling, bootstrapping a table, full-load guards. |
| `references/engineering-standards.md` | Any code change: where each concept lives, extension points, OOP rules, quality and test bar. |
| `references/dlt-docs-index.md` | Any dlt API or behaviour question; maps topics to the vendored docs. |
| `README.md` | Public usage, commands, exit codes, deployment contract. |
| `dlt_framework/sources/rest/pipelines/dummyjson.py`, `tests/conftest.py`, `tests/test_sources.py` | A working definition, test fixtures, and per-source upstream-binding tests. |

## Workflow: adding a pipeline or table

1. **Establish the extraction contract** from the request, upstream API docs, and the repo. Ask
   the user only for facts that change data correctness. Never guess a primary key, write mode,
   cursor, or empty-result behaviour.
   - resources and their names (§1 naming);
   - pagination and how it terminates;
   - cursor kind (time, value, none), its field, format, ordering guarantee, `start` boundary,
     null cursors (`fallbacks`), late-arriving data (`lag`), `on_missing`;
   - how history can be windowed: by time, by value, or only by rows
     (`references/running-pipelines.md` §3), and which bounds upstream accepts;
   - `Append` / `Replace` / `Merge` (strategy and keys), and the empty policy;
   - credential variable names (never values).
2. **Declare it** in `dlt_framework/sources/<type>/pipelines/<system>.py`: one
   `Pipeline(name, target, schema_contract, source)` with `Resource` definitions on the existing
   source type (`RestApiSource`, `SqlSource`, `FilesystemSource`, or `CustomSource`). Add a new
   source type only for a reusable protocol, as its own package.
3. **Group resources deliberately.** Resources in one pipeline always run together. Use
   `parallel=True` for concurrency inside one process. Use a separate pipeline for resources that
   must run on a different schedule or in another process.
4. **Test.** Add or extend a binding test that captures the outbound request or SQL and asserts
   the cursor start, plus each backfill window's bounds (G2). Behaviour changes need DuckDB
   integration tests.
5. **Verify offline**: `dlt-framework check NAME`, then the quality gates.

## Workflow: running or backfilling

Follow `references/running-pipelines.md` §2–4. Before any production run after a change, confirm
each table's `start_value` (G5). Run a live extraction or destination write only when credentials
exist and the user has authorised it.

## Workflow: changing the framework

Read the module and its tests first. Use the placement table in
`references/engineering-standards.md` §2. Run types differ only in class attributes and hooks on
`Run`; source types differ only in their `Source` subclass. If a change seems to need an
`if mode == ...` or `isinstance(source, ...)` in shared code, it belongs in a hook or variant.

## Quality gates

```bash
uv run dlt-framework check NAME
uv run ruff check dlt_framework tests
uv run ruff format --check dlt_framework tests
uv run lint-imports
uv run mypy dlt_framework
uv run pytest --cov=dlt_framework --cov-branch --cov-fail-under=90
```

## Handoff

Report, briefly:

- files changed and the pipeline name(s);
- per resource: write mode, primary key, cursor kind + `start`, empty policy, backfill kind;
- the effect on the next run's `start_value` for every touched resource;
- variable names introduced (redacted);
- gates and tests run, with results; whether any live extraction happened;
- unresolved upstream constraints (permissions, rate limits, historical access).

# dlt documentation index (offline)

`dlt-docs/` holds the official dlt documentation for the exact version pinned in `pyproject.toml`
(see `dlt-docs/VERSION`). It is Apache-2.0 (`dlt-docs/LICENSE.txt`). Use it instead of web search
and instead of memory. Model training data about dlt is sparse and often describes old APIs.

## Lookup order

1. This index → the page for the topic.
2. Search across pages: `grep -rn "<term>" .claude/skills/dlt-pipelines/dlt-docs`.
3. Docs and code disagree, or the exact signature matters → read the installed source, which is
   authoritative for the pinned version:
   `.venv/lib/python*/site-packages/dlt/` (e.g. `extract/incremental/__init__.py`,
   `pipeline/pipeline.py`, `sources/rest_api/`, `sources/sql_database/`).
4. Not covered → say so and ask, or (only with the user's agreement) check dlthub.com/docs.

Links inside the pages that point to non-vendored pages are dead locally. Their dlthub.com/docs
equivalent has the same path.

## Topic → page

All paths relative to `.claude/skills/dlt-pipelines/dlt-docs/`.

### Pipelines, state, lifecycle

| Topic | Page |
| --- | --- |
| Pipeline args, working dir, `dev_mode`, `refresh` modes | `general-usage/pipeline.md` |
| State storage, restore from destination, resetting state | `general-usage/state.md` |
| Extract → normalize → load explained | `reference/explainers/how-dlt-works.md` |
| Running, inspecting load info, progress | `walkthroughs/run-a-pipeline.md` |
| Production: retries, failed jobs, partial loads, signals, logging | `running-in-production/running.md` |
| Workers, buffers, file rotation, parallel pipelines and their pitfalls, `DLT_DATA_DIR` | `reference/performance.md` |
| `dlt pipeline info/trace/drop/sync/failed-jobs` CLI | `reference/command-line-interface.md` |
| Glossary | `general-usage/glossary.md` |
| FAQ | `reference/frequently-asked-questions.md` |

### Incremental loading and backfills

| Topic | Page |
| --- | --- |
| Overview of incremental strategies | `general-usage/incremental-loading.md` |
| `dlt.sources.incremental`: `initial_value`, `end_value`, `row_order`, `range_start/end`, dedup, missing cursor values, external schedulers / `TimeIntervalContext` | `general-usage/incremental/cursor.md` |
| Lag / attribution windows | `general-usage/incremental/lag.md` |
| Custom resource state | `general-usage/incremental/advanced-state.md` |
| Cursor not advancing, coercion errors | `general-usage/incremental/troubleshooting.md` |
| Walkthrough adding incremental config | `walkthroughs/add-incremental-configuration.md` |

### Sources and resources

| Topic | Page |
| --- | --- |
| `@dlt.source`: dynamic resources, renaming, `with_resources`, `add_limit` | `general-usage/source.md` |
| `@dlt.resource`: hints, transformers, parallel/async, `add_map`/`add_filter`, `apply_hints` | `general-usage/resource.md` |
| REST API source: client, auth, paginators, endpoints, incremental placeholders, response actions | `dlt-ecosystem/verified-sources/rest_api/basic.md` |
| REST API: custom paginators/auth, RESTClient, headers, troubleshooting | `dlt-ecosystem/verified-sources/rest_api/advanced.md` |
| SQL database source: backends, reflection, incremental, partitioned backfills | `dlt-ecosystem/verified-sources/sql_database/*.md` |
| Filesystem source | `dlt-ecosystem/verified-sources/filesystem/index.md` |
| MongoDB source | `dlt-ecosystem/verified-sources/mongodb.md` |
| Arrow / pandas items | `dlt-ecosystem/verified-sources/arrow-pandas.md` |
| Building a custom API pipeline from scratch | `tutorial/load-data-from-an-api.md` |
| Tutorials: REST API / SQL database | `tutorial/rest-api.md`, `tutorial/sql-database.md` |
| Dispatching rows to multiple tables | `walkthroughs/dispatch-to-multiple-tables.md` |
| Renaming / removing / pseudonymising columns | `general-usage/customising-pipelines/*.md` |

### Configuration and credentials

| Topic | Page |
| --- | --- |
| `config.toml` / `secrets.toml` / env var layout and lookup order | `general-usage/credentials/setup.md` |
| Credential types (connection strings, OAuth, GCP...) | `general-usage/credentials/complex_types.md` |
| Custom config providers, `@with_config` | `general-usage/credentials/advanced.md` |
| Vaults | `general-usage/credentials/vaults.md` |
| Adding credentials walkthrough | `walkthroughs/add_credentials.md` |

### Schemas, write dispositions, destinations

| Topic | Page |
| --- | --- |
| Schema, column hints, data types, key precedence | `general-usage/schema.md` |
| Schema evolution | `general-usage/schema-evolution.md` |
| Schema contracts (`evolve`/`freeze`/`discard_*`), Pydantic validation | `general-usage/schema-contracts.md` |
| Adjusting a schema | `walkthroughs/adjust-a-schema.md` |
| Naming convention and identifier normalisation | `general-usage/naming-convention.md` |
| `replace` strategies | `general-usage/full-loading.md` |
| `merge`: delete-insert, upsert, scd2, insert-only; hard deletes; dedup_sort | `general-usage/merge-loading.md` |
| Destination tables, nested tables, `_dlt_*` system tables | `general-usage/destination-tables.md` |
| Declaring destinations | `general-usage/destination.md` |
| Staging dataset and `staging_dataset_name_layout` | `dlt-ecosystem/staging.md` |
| DuckDB / Postgres specifics | `dlt-ecosystem/destinations/duckdb.md`, `.../postgres.md` |
| Reading loaded data (`pipeline.dataset()`) | `general-usage/dataset-access/dataset.md` |
| Data quality lifecycle | `general-usage/data-quality-lifecycle.md` |

## Updating the vendored docs

When the dlt pin changes, re-vendor in the same change:

```bash
uv run python .claude/skills/dlt-pipelines/scripts/sync_dlt_docs.py --version <new dlt tag>
```

Then re-verify `references/dlt-internals.md` against the new installed source and update this index
for added or removed pages. The included page list is the `INCLUDED` tuple in the script.

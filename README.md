# dlt framework

An opinionated extraction foundation that is cloned and adapted for each client. It moves raw
source data into DuckDB or Postgres and deliberately leaves scheduling, transformations, and
alert delivery outside the repository.

The reliability contract is idempotent at-least-once delivery: transient work may be replayed,
and backfillable resources must make that replay safe with `merge` and a stable primary key.

## Quick start

```bash
cp .env.example .env
uv sync --group dev
uv run dlt-framework validate rest/dummyjson
uv run dlt-framework run rest/dummyjson --resource products
```

Build and run DuckDB in Docker:

```bash
docker build -t dlt-pipelines .
mkdir -p test_data dlt_state
docker run --rm --stop-timeout 300 \
  --user "$(id -u):$(id -g)" \
  --env-file .env \
  --mount type=bind,source="$(pwd)/test_data",target=/data \
  --mount type=bind,source="$(pwd)/dlt_state",target=/var/lib/dlt \
  dlt-pipelines run rest/dummyjson --resource products
```

Passing the host UID/GID keeps bind-mounted DuckDB and pipeline-state files writable without
running the container as root. If an older container created those files under another UID, repair
them once with `sudo chown -R "$(id -u):$(id -g)" test_data dlt_state`.

dlt stores committed incremental state in the destination's `_dlt_pipeline_state` table. The
local `DLT_DATA_DIR` is its working directory and also contains schemas, traces, and load packages
that may not have reached the destination yet. Choose either deployment mode deliberately:

```bash
# Recommended: preserve the working directory and any interrupted load package.
docker run --rm \
  --user "$(id -u):$(id -g)" \
  --env-file .env \
  --env DLT_DATA_DIR=/var/lib/dlt \
  --mount type=bind,source="$(pwd)/test_data",target=/data \
  --mount type=bind,source="$(pwd)/dlt_state",target=/var/lib/dlt \
  dlt-pipelines run dummyjson

# Stateless runner: restore committed state from DuckDB and discard local artifacts on exit.
docker run --rm \
  --user "$(id -u):$(id -g)" \
  --env-file .env \
  --env DLT_DATA_DIR=/tmp/dlt \
  --env PIPELINES__RESTORE_FROM_DESTINATION=true \
  --mount type=bind,source="$(pwd)/test_data",target=/data \
  dlt-pipelines run dummyjson
```

`DLT_DATA_DIR` selects the in-container working path; the Docker mount decides whether it is
persistent. Keep `PIPELINES__RESTORE_FROM_DESTINATION=true` for stateless runners. A stateless
runner can recover committed incremental state, but cannot recover an extracted or normalized
package that was interrupted before reaching the destination.

`DESTINATION__DUCKDB__CREDENTIALS=/data/client.duckdb` writes one DuckDB database file. The
configured `DATASET_NAME` is a schema inside that database. Keep the file stem and dataset name
different because DuckDB otherwise sees an ambiguous catalog/schema reference.

## Commands

```text
dlt-framework run SOURCE [--resource NAME]
dlt-framework backfill SOURCE --resource NAME --from ISO --to ISO --chunksize ISO [--restart]
dlt-framework list SOURCE
dlt-framework validate SOURCE [--resource NAME]
```

- `run` loads the full source or one resource.
- `backfill` processes sequential half-open `[from, to)` windows and checkpoints after each
  successful chunk.
- `list` shows resource write modes, keys, empty policies, and backfill support.
- `validate` checks configuration and source contracts without extracting records.

The old command form (`dlt-framework SOURCE ...`) remains available for one compatibility
release and emits a deprecation warning.

## Source contract

Each file under `dlt_framework/sources/` exports one explicit `SOURCE` contract. Resource
selection reaches the factory before endpoint construction or database reflection.

```python
import dlt
from dlt.sources.rest_api import RESTAPIConfig, rest_api_resources

from dlt_framework.core.contracts import define_source
from dlt_framework.core.models import ResourcePolicy, SourceContext


@dlt.source
def source(context: SourceContext):
    names = ("orders", "customers")
    selected = names if context.selected_resource is None else (context.selected_resource,)
    config: RESTAPIConfig = {
        "client": {"base_url": "https://api.example.com/"},
        "resources": [
            {
                "name": name,
                "write_disposition": "replace",
                "endpoint": {"path": name},
            }
            for name in selected
        ],
    }
    return rest_api_resources(config)


SOURCE = define_source(
    factory=source,
    resources={
        "orders": ResourcePolicy(empty="fail"),
        "customers": ResourcePolicy(empty="allow"),
    },
)
```

Write disposition, primary key, pagination, authentication, and cursor behavior stay in native
dlt resources. `ResourcePolicy` contains only framework behavior that dlt cannot infer:

- `empty="fail"` aborts before normalization/loading and protects snapshots from accidental
  truncation.
- `empty="warn"` loads but emits a structured warning.
- `empty="allow"` treats an empty extraction as expected.
- `backfill=True` declares bounded extraction support.

See [Source authoring](docs/source-authoring.md) for the complete implementation checklist.

## Backfills

Chunk sizes use one positive ISO-8601 unit:

```text
PT30S  PT30M  PT6H  P1D  P1W  P1M  P1Y
```

Months and years are calendar-aware and anchored to the original start. Dates mean midnight UTC;
datetimes require `Z` or an explicit offset. The upper bound is exclusive.

```bash
uv run dlt-framework backfill rest/orders \
  --resource events \
  --from 2026-01-01 \
  --to 2026-02-01 \
  --chunksize P1D
```

The checkpoint is stored in dlt pipeline state at the destination. An interrupted run resumes the
same plan. A conflicting unfinished plan is rejected; `--restart` clears its checkpoint and safely
replays from the start. Plans above `MAX_BACKFILL_CHUNKS` (10,000 by default) are rejected before
extraction.

## Production contract

The job runner must provide the controls that cannot be made process-local:

- one active job per `PIPELINE_NAME`;
- persistent `/var/lib/dlt` storage;
- `SIGTERM` delivery and at least 300 seconds of shutdown grace;
- a whole-job timeout;
- alerting on nonzero exits and missing scheduled successes.

Exit codes are `0` success, `2` configuration/source contract, `3` terminal pipeline/data failure,
`4` transient retry exhaustion, and `130` interruption. Production logs use JSON and include the
operation, resource, chunk, and causal exception without credential values.

Read [Operations](docs/operations.md), [Configuration](docs/configuration.md), and the
[architecture decision](docs/architecture.md) before deploying a client pipeline.

## Development

This repository uses namespace packages and intentionally has no `__init__.py` files.

```bash
uv sync --group dev
uv run ruff check dlt_framework tests
uv run ruff format --check dlt_framework tests
uv run mypy dlt_framework
uv run pytest --cov=dlt_framework --cov-branch
```

Only DuckDB and Postgres destinations are certified. Add a connector or destination dependency
only alongside a real client source and its integration/recovery tests.

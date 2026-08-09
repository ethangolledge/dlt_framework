# Configuration

## Framework environment

| Variable | Required | Default | Purpose |
| --- | --- | --- | --- |
| `DESTINATION` | Yes | — | Certified values: `duckdb`, `postgres`. |
| `DLT_FRAMEWORK_ENV` | Production | `development` | Production requires explicit stable identities. |
| `PIPELINE_NAME` | Production | Source filename | Identity used to restore dlt state. |
| `DATASET_NAME` | Production | Source filename | Destination schema/dataset. |
| `PIPELINE_RETRY_ATTEMPTS` | No | `5` | Total transient attempts. |
| `PIPELINE_RETRY_MIN_WAIT` | No | `4` | Initial retry delay in seconds. |
| `PIPELINE_RETRY_MAX_WAIT` | No | `60` | Maximum retry delay in seconds. |
| `MAX_BACKFILL_CHUNKS` | No | `10000` | Pre-extraction safety limit. |
| `LOG_FORMAT` | No | `text` | Use `json` in production. |
| `LOG_LEVEL` | No | `INFO` | Framework log level. |
| `DLT_DATA_DIR` | No | Image: `/var/lib/dlt` | Local dlt working directory. |
| `PIPELINES__RESTORE_FROM_DESTINATION` | No | `true` | Restore committed state and schemas from the target. |

Destination credentials use native dlt variables:

```dotenv
DESTINATION__DUCKDB__CREDENTIALS=/data/client.duckdb
# or
DESTINATION__POSTGRES__CREDENTIALS=postgresql://loader:password@host:5432/database
```

Keep the DuckDB file stem different from `DATASET_NAME`.

## `.dlt/config.toml`

The committed `.dlt/config.toml` is non-secret and establishes worker and failure defaults:

- `[extract].workers`: extraction thread workers. This helps only sources that expose parallel
  callables/awaitables or parallelized resources; it does not make ordinary generators parallel.
- `[normalize].workers`: normalization workers.
- `[load].workers`: destination load workers.
- `raise_on_failed_jobs=true`: a failed destination job fails the process.
- `delete_completed_jobs=true`: removes successful local job artifacts.
- `truncate_staging_dataset=true`: cleans staging data after merge/replace.
- `start_new_jobs_on_signal=true`: lets an in-flight load drain after shutdown begins.
- `[pipelines].restore_from_destination=true`: restores committed state on a clean runner.

The destination and local directory serve different purposes. dlt writes committed pipeline state
to `_dlt_pipeline_state` in the destination. `DLT_DATA_DIR` contains the local working copy plus
schemas, traces, extracted files, and pending load packages. Mount that directory for full local
recovery, or point it at ephemeral storage and rely on destination restoration for successfully
committed state.

Tune workers against source rate limits, destination connection limits, memory, and record size;
more workers are not automatically faster or safer.

Environment variables override TOML using dlt's double-underscore convention, for example
`EXTRACT__WORKERS=10`. Credentials belong in environment variables or ignored
`.dlt/secrets.toml`, never in `config.toml`.

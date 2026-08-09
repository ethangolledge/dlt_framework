# Operations and recovery

## Deployment checklist

- Use stable `PIPELINE_NAME`, `DATASET_NAME`, destination, and credentials across restarts.
- Choose a local-state mode: mount `DLT_DATA_DIR` for pending-package recovery, or use ephemeral
  storage with destination restoration enabled.
- For local bind mounts, run with the host UID/GID and ensure both destination and state paths are
  writable; filesystem permission failures are terminal and must not be retried.
- Prevent overlapping jobs for the same pipeline identity in the scheduler.
- Deliver `SIGTERM`, configure at least 300 seconds grace, and set a whole-job timeout.
- Alert on exit codes 2–4 and on a missing expected success event.
- Retain centralized JSON logs with access controls.
- Back up the destination according to the client's recovery objectives.

## Retry behavior

The framework retries dlt-classified transient sync, extract, and load failures five times by
default with exponential jitter. Configuration, authentication, schema-contract, validation,
normalization, and terminal destination failures stop immediately.

The HTTP client's own Retry-After/rate-limit behavior still applies. Framework retries cover a
failed pipeline step; they do not justify unbounded API pressure.

## Restart behavior

At startup the runner synchronizes destination state and processes local pending packages before
extracting new data. Never delete the persistent dlt directory as a first response to failure.

Committed source/incremental state is stored in the destination's `_dlt_pipeline_state` table.
Local `DLT_DATA_DIR` storage additionally preserves work that has not been committed yet:

- **Persistent local state (recommended):** mount the configured directory. Restarts can continue
  pending extract/normalize/load work and retain local traces.
- **Destination-restored state:** use an ephemeral writable directory and keep
  `PIPELINES__RESTORE_FROM_DESTINATION=true`. A clean runner restores committed state and schemas,
  but interrupted uncommitted packages are discarded and must be extracted again.

Do not set `PIPELINES__RESTORE_FROM_DESTINATION=false` on an ephemeral runner. For incremental
sources, that can make a clean container behave like a first run.

For backfills, the destination checkpoint contains only the active/latest plan and next bound. If
the process dies:

- before a chunk load completes, dlt retries or resumes its pending package;
- after the chunk load but before checkpoint persistence, that chunk is replayed safely by merge;
- after checkpoint persistence, execution resumes at the next bound.

Use `--restart` only when intentionally replaying the requested plan. It clears framework
checkpoint state; it does not delete destination rows.

## Failure triage

1. Read the classified exit code and first causal exception.
2. Confirm whether the failure is configuration/terminal or transient exhaustion.
3. Inspect persistent dlt pipeline state and pending packages.
4. Fix credentials, schema, source code, or destination availability without changing pipeline
   identity.
5. Rerun the same command and bounds.

Do not drop pending packages unless they are terminally invalid and the consequence has been
reviewed. Do not change a primary key or write disposition during recovery without reconciling
already loaded rows.

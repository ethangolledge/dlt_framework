# Operations and recovery

## Deployment checklist

- Use stable `PIPELINE_NAME`, `DATASET_NAME`, destination, and credentials across restarts.
- Mount `/var/lib/dlt` on persistent storage.
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

# Architecture decision: client-isolated extraction foundation

## Context

This repository is maintained by us and cloned per engagement. Each deployment must run
without supervision, while new client sources must remain quick to implement.

## Decisions

- One isolated repository/image and destination identity per client pipeline.
- One-shot, orchestrator-neutral CLI; the deployment runtime owns scheduling, locks, timeouts, and
  notification delivery.
- Idempotent at-least-once delivery. Universal exactly-once delivery is not promised because many
  APIs cannot provide the necessary transactional boundary.
- dlt owns extraction state, schemas, normalisation, and destinations.
- Backfill progress uses bounded dlt state rather than framework-specific control tables.
- DuckDB and Postgres are the only certified destinations until another target has integration and
  recovery tests.
- Additive tables/columns are accepted; incompatible data-type changes fail.
- Connector abstractions are introduced only with a real source and failure-focused tests.

## Consequences

The framework remains small and source files remain dlt-native. Reliability depends on stable
pipeline identity, idempotent keys, persistent dlt storage, and an external single-flight job
runner. A new destination or source protocol is not considered supported merely because dlt can
import it; support requires the tests and runbook needed to recover it unattended.

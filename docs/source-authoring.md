# Source authoring

## Definition of done

A client source is complete when it changes one source module plus its tests/configuration, does
not require framework-core edits, and proves the source's pagination or cursor semantics.

Before coding, record:

- source selector and credential variable names;
- requested resources and endpoint/table names;
- write disposition and stable keys;
- incremental cursor and overlap behavior;
- whether an empty response is valid;
- whether upstream accepts explicit lower and upper bounds.

Never guess these properties. They determine whether retries lose or duplicate data.

## Implementation rules

1. Prefer a maintained dlt connector over a custom request loop.
2. Accept `SourceContext` and construct only `context.selected_resource` when one is selected.
3. Keep credentials in `dlt.secrets.value`; use `dlt.config.value` for non-secret source settings.
4. Declare write disposition and primary keys on the dlt resource, exactly once.
5. Declare every resource in `SOURCE = define_source(...)` with an explicit empty policy.
6. Do not transform business data in the extraction framework.
7. Do not add generic connector helpers before a real source needs them.

## Write behavior

- `merge`: mutable entities and every backfillable resource. Declare a stable source key.
- `replace`: complete snapshots only. Decide explicitly whether an empty snapshot may clear the
  table.
- `append`: immutable events only. Contract-test the source cursor/offset and overlap behavior.

## Bounded resources

Use the framework helpers so every source receives the same half-open bounds. For a REST endpoint:

```python
from dlt_framework.core.backfill import rest_incremental_config

incremental = rest_incremental_config(
    context,
    resource_name="events",
    cursor_path="updated_at",
    start_param="updated_from",
    end_param="updated_to",
)
```

For a SQL query or connector accepting keyword arguments, use `incremental_for` or
`range_kwargs`. Set `ResourcePolicy(empty=..., backfill=True)` and `merge` plus a primary key.

A unit/contract test must capture the outbound request, SQL predicate, or connector arguments and
assert both exact bounds. Calling a helper alone is not evidence that the upstream query is
bounded.

## Required tests

- discovery and `dlt-framework validate`;
- selected-resource construction does not initialize other resources;
- pagination terminates and cannot repeat a non-advancing cursor;
- 429/5xx/timeouts are classified as transient and terminal 4xx/auth errors are not retried;
- empty-response policy;
- rerunning overlapping incremental/backfill input does not duplicate stable keys;
- additive schema changes and incompatible type changes;
- one destination integration test.

Do not claim live end-to-end success when source credentials or network access were unavailable.

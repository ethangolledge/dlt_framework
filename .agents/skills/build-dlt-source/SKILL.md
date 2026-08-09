---
name: build-dlt-source
description: Add or update a credential-driven extraction source in this dlt pipeline framework. Use when onboarding a REST API, Chess.com, Shopify GraphQL, PostgreSQL, MySQL, Microsoft SQL Server, or MongoDB source; declaring its source and destination environment variables; choosing resources/tables and append-or-replace behavior; or creating files below pipeline/sources/. Also use when asked to implement a new client pipeline without transformations or changes to framework core.
---

# Build a DLT Source

Implement client pipelines by declaring source credentials, declaring destination credentials, and adding one focused file below `pipeline/sources/`. Treat `README.md` as the detailed source of truth and this skill as the execution checklist.

## Required input

Establish these values from the request or existing project context:

1. Source filename, using a valid Python identifier such as `orders_api`.
2. Source kind: `rest`, `shopify`, `sql`, or `mongodb`.
3. Source connection values and credential names.
4. Destination kind and credentials. The built-in targets are `postgres` and `duckdb`.
5. Resources, endpoints, SQL tables, or MongoDB collections to load.
6. `append` or `replace` for every resource, including a justified default where appropriate.

Ask only for values that cannot be inferred safely. Do not require live secret values to write or test source code; placeholders and environment-variable names are sufficient. If resource selection or write disposition is missing, call that out because it changes pipeline behavior.

## Guardrails

- Keep extraction code in exactly one new or edited `pipeline/sources/{kind}/{source_name}.py` file unless tests also need updating.
- Do not change `pipeline/core/` or `pipeline/execution/` for a supported connector.
- Do not transform source records. Load the raw records exposed by the source.
- Do not hardcode, print, log, or commit credentials. Use `dlt.config.value` for non-secret configuration and `dlt.secrets.value` for secrets.
- Do not expose values from `.env` in output. Edit `.env` only when the user asks to configure the local environment; otherwise return a placeholder credential block.
- Make `source()` callable with no arguments and return exactly one `DltSource`.
- Make every path component and filename a valid Python identifier. Do not add `__init__.py`; this repository uses namespace packages.
- Declare `append` or `replace` in the source file. Never move write disposition into deployment configuration.
- Keep source resources sequential and let native `dlt` connectors handle authentication, pagination, incrementals, and type conversion.
- Add a dependency to `pyproject.toml` only for a genuinely new connector or destination. MongoDB additionally requires the one-time verified-source installation described in `README.md`.

## Credential contract

The filename becomes the pipeline name, destination schema, and custom source configuration namespace. For `pipeline/sources/rest/orders_api.py`, use:

```dotenv
# Source configuration: declare only arguments used by source()
SOURCES__ORDERS_API__BASE_URL=https://api.example.com/
SOURCES__ORDERS_API__API_KEY=replace-me

# Destination configuration: choose one block
DESTINATION=postgres
DESTINATION__POSTGRES__CREDENTIALS=postgresql://loader:password@host:5432/database
```

or:

```dotenv
DESTINATION=duckdb
DESTINATION__DUCKDB__CREDENTIALS=/data/pipelines.duckdb
```

Apply these naming rules:

- Convert the source filename to uppercase for `SOURCES__<SOURCE>__...`.
- Use the decorated custom source's filename namespace for REST, Chess.com, and Shopify.
- Use `SOURCES__SQL_DATABASE__...` for `sql_database()` credentials, regardless of wrapper filename.
- Use `SOURCES__MONGODB__...` for the verified `mongodb()` connector.
- URL-encode reserved characters inside connection URIs.
- Encode lists passed through the environment with Python-literal syntax, for example `["orders", "customers"]`.

Before coding, present or internally establish a compact contract like:

```text
Source: rest/orders_api
Source credentials: SOURCES__ORDERS_API__API_KEY
Source config: SOURCES__ORDERS_API__BASE_URL
Destination: postgres
Destination credentials: DESTINATION__POSTGRES__CREDENTIALS
Resources: orders=append, customers=replace
```

Never repeat actual secret values in the final handoff.

## Implementation workflow

1. Read the relevant connector section and environment-variable table in `README.md`.
2. Inspect nearby source files and `pyproject.toml`; preserve unrelated user changes.
3. Normalize the requested source name and choose its directory.
4. Define optional `source()` arguments with `dlt.config.value` or `dlt.secrets.value` so callers pass no arguments.
5. Create native dlt resources using the closest template below.
6. Assign an explicit write disposition to every resource.
7. Verify discovery with the filename selector; qualify it as `{kind}/{source_name}` only if another folder contains the same filename.
8. Run the full test suite. Run a live extraction only when credentials and network access are available and the user authorizes the external read/write.
9. Hand off the source path, selector, credential variable names, resource-to-table mapping, dispositions, and validation result.

## Connector patterns

Adapt the smallest matching pattern. Prefer the native connector over custom request loops.

### REST API

```python
import dlt
from dlt.sources.rest_api import RESTAPIConfig, rest_api_resources


@dlt.source
def source(
    base_url: str = dlt.config.value,
    api_key: str = dlt.secrets.value,
):
    config: RESTAPIConfig = {
        "client": {
            "base_url": base_url,
            "auth": {"type": "bearer", "token": api_key},
        },
        "resource_defaults": {"write_disposition": "append"},
        "resources": [
            "orders",
            {
                "name": "customers",
                "write_disposition": "replace",
                "endpoint": {"path": "customers"},
            },
        ],
    }
    return rest_api_resources(config)
```

Express pagination, selectors, endpoint parameters, and incrementals through `RESTAPIConfig`. For snapshot endpoints, prefer `replace`; for immutable event/history feeds, prefer `append`. Confirm semantics rather than guessing when duplicates would matter.

### Chess.com PubAPI

Use the REST connector with `https://api.chess.com/pub/`, `username: str = dlt.config.value`, and `user_agent: str = dlt.config.value`. Supply the identifying user agent as a header. The API is public, so do not invent an API-key variable. Use the complete example in `README.md` and generally replace current player, stats, games, and club snapshots.

### Shopify GraphQL

```python
import dlt
from pipeline.core.graphql import graphql_connection_resource


@dlt.source
def source(
    shop_url: str = dlt.config.value,
    api_version: str = dlt.config.value,
    access_token: str = dlt.secrets.value,
):
    endpoint = f"{shop_url.rstrip('/')}/admin/api/{api_version}/graphql.json"
    headers = {"X-Shopify-Access-Token": access_token}
    return [
        graphql_connection_resource(
            name="products",
            endpoint=endpoint,
            headers=headers,
            write_disposition="replace",
            query="""
            query Products($cursor: String) {
              products(first: 250, after: $cursor) {
                nodes { id title status createdAt updatedAt }
                pageInfo { hasNextPage endCursor }
              }
            }
            """,
        ),
    ]
```

Use one `graphql_connection_resource` per top-level Shopify connection. Keep `nodes` and `pageInfo { hasNextPage endCursor }` in every query. Confirm the app has the read scopes and any older-orders permission required by the selected resources.

### SQL database

```python
from dlt.sources.sql_database import sql_database

from pipeline.core.resources import set_write_dispositions


def source():
    return set_write_dispositions(
        sql_database(),
        default="replace",
        overrides={"events": "append"},
    )
```

Use the same wrapper for PostgreSQL, MySQL, and Microsoft SQL Server. Configure `SOURCES__SQL_DATABASE__CREDENTIALS`, with optional `SCHEMA` and `TABLE_NAMES`; the URI driver identifies the engine. Ensure every override names a reflected resource or `set_write_dispositions` will reject it.

### MongoDB

```python
from mongodb import mongodb

from pipeline.core.resources import set_write_dispositions


def source():
    return set_write_dispositions(mongodb(), default="append")
```

Configure `SOURCES__MONGODB__CONNECTION_URL`, with optional `DATABASE` and `COLLECTION_NAMES`. Before using this wrapper, confirm the verified source generated by `dlt init mongodb duckdb` exists. Do not reimplement its type conversion.

## Validation

Run:

```bash
pytest
```

Also verify the new module imports and discovery accepts its selector without making an external API or database call where possible. A successful implementation satisfies all of these conditions:

- `source()` binds with zero required arguments.
- Instantiation returns a `DltSource` when placeholder/test configuration permits it.
- The source filename matches the expected pipeline and schema name.
- Each resource name matches its destination table and has an explicit disposition.
- Only the intended source, tests, and explicitly necessary dependency/configuration files changed.

Do not claim end-to-end success when a live source or destination was unavailable. Distinguish unit/discovery validation from an actual load.

## Handoff

Report only what the operator needs:

- source file and run selector;
- required source and destination environment-variable names, with redacted placeholders;
- resource/table names and their dispositions;
- tests run and whether a live load occurred;
- any new dependency, Shopify permission, or MongoDB initialization prerequisite.

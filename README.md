# dlt pipeline framework

A framework for defining dlt sources and loading them into a destination. It performs no transformations, instead loads raw source data into target.

## Source → schema → tables

Each Python file below `pipeline/sources/{source_type}` represents one source. The filename becomes the source name, pipeline name, and destination schema. Each resource becomes a table.

```text
pipeline/sources/rest/products.py

products schema
├── products resource  → products table
├── variants resource  → variants table
├── inventory resource → inventory table
├── prices resource    → prices table
└── categories resource → categories table
```

Run the source by its filename:

```bash
docker run ... pipeline-image products
```

The runner discovers `products.py` recursively. Exactly one source is accepted per invocation. If a filename exists in more than one folder, qualify it as `rest/products` or `shopify/products`; the schema remains `products`.

## What changes

For the source and destination types already installed in this template, work is limited to:

1. Add or edit a file below `pipeline/sources/{source_type}`.
2. Set that source's environment variables.
3. Set `DESTINATION` and the destination credentials.

Core, discovery, and execution code should not change between clients. Append or replace is declared in the source file because it is resource behavior, not deployment configuration.

REST, Chess.com, Shopify/GraphQL, PostgreSQL sources, MySQL sources, MSSQL sources, and the Postgres and DuckDB targets have their dependencies included. MongoDB's Python driver is included, but DLT's verified MongoDB source must currently be added once with `dlt init mongodb duckdb`. A completely new connector or destination type may also require a one-time dependency addition to `pyproject.toml`.

## Writing a REST source

Every file exposes a zero-required-argument `source()` returning one `DltSource`. That source may contain any number of resources.

```python
import dlt
from dlt.sources.rest_api import RESTAPIConfig, rest_api_resources


@dlt.source
def source(api_key: str = dlt.secrets.value):
    config: RESTAPIConfig = {
        "client": {
            "base_url": "https://api.example.com/",
            "auth": {"type": "bearer", "token": api_key},
        },
        "resource_defaults": {
            "write_disposition": "append",
        },
        "resources": [
            "products",
            "variants",
            "inventory",
            {
                "name": "prices",
                "write_disposition": "replace",
                "endpoint": {"path": "prices"},
            },
        ],
    }
    return rest_api_resources(config)
```

Use native `dlt` source configuration for authentication, pagination, incremental extraction, and resource hints. Core does not transform records, parallelize resources, or override resource write dispositions.

## Writing a Chess.com source

Chess.com's PubAPI is public and read-only. A `chess_com.py` source can use DLT's REST connector directly, with one resource for each endpoint:

```python
import dlt
from dlt.sources.rest_api import RESTAPIConfig, rest_api_resources


@dlt.source
def source(
    username: str = dlt.config.value,
    user_agent: str = dlt.config.value,
):
    player_path = f"player/{username}"
    config: RESTAPIConfig = {
        "client": {
            "base_url": "https://api.chess.com/pub/",
            "headers": {"User-Agent": user_agent},
        },
        "resource_defaults": {
            "write_disposition": "replace",
        },
        "resources": [
            {"name": "player", "endpoint": {"path": player_path}},
            {"name": "stats", "endpoint": {"path": f"{player_path}/stats"}},
            {
                "name": "current_games",
                "endpoint": {
                    "path": f"{player_path}/games",
                    "data_selector": "games",
                },
            },
            {
                "name": "clubs",
                "endpoint": {
                    "path": f"{player_path}/clubs",
                    "data_selector": "clubs",
                },
            },
        ],
    }
    return rest_api_resources(config)
```

Running `chess_com` creates the `chess_com` schema and the `player`, `stats`, `current_games`, and `clubs` tables. These are current snapshots, so `replace` avoids duplicating data on repeated runs. Chess.com recommends an identifying user agent containing contact information.

## Writing a Shopify source

Shopify's Admin API is GraphQL. A single `shopify.py` source can expose each Shopify query as a resource and therefore as a table in the `shopify` schema.

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
        graphql_connection_resource(
            name="orders",
            endpoint=endpoint,
            headers=headers,
            write_disposition="append",
            query="""
            query Orders($cursor: String) {
              orders(first: 250, after: $cursor) {
                nodes { id name createdAt updatedAt displayFinancialStatus }
                pageInfo { hasNextPage endCursor }
              }
            }
            """,
        ),
        graphql_connection_resource(
            name="customers",
            endpoint=endpoint,
            headers=headers,
            write_disposition="replace",
            query="""
            query Customers($cursor: String) {
              customers(first: 250, after: $cursor) {
                nodes { id displayName createdAt updatedAt }
                pageInfo { hasNextPage endCursor }
              }
            }
            """,
        ),
    ]
```

The corresponding source environment variables are:

```dotenv
SOURCES__SHOPIFY__SHOP_URL=https://your-store.myshopify.com
SOURCES__SHOPIFY__API_VERSION=2026-07
SOURCES__SHOPIFY__ACCESS_TOKEN=replace-me
```

The Shopify app must have the matching read scopes for the selected resources. Access to older orders may require Shopify's additional all-orders permission.

## Writing a SQL database source

PostgreSQL, MySQL, and Microsoft SQL Server use the same source file. `dlt` creates one resource per selected source table; only the connection string changes between database engines.

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

`set_write_dispositions` applies one explicit policy to every reflected table and allows small per-table overrides. It works with any `DltSource`, so source files do not need to repeat resource loops.

## Writing a MongoDB source

After adding `dlt`'s verified MongoDB source, the runnable wrapper remains small:

```python
from mongodb import mongodb

from pipeline.core.resources import set_write_dispositions


def source():
    return set_write_dispositions(
        mongodb(),
        default="append",
    )
```

The verified source creates one resource per selected collection and handles MongoDB-specific type conversion. Add it once with `dlt init mongodb duckdb`; the runtime destination is still selected through `DESTINATION`.

## Destination configuration

Core requires one environment variable:

```text
DESTINATION=postgres
```

Destination credentials use native `dlt` environment variables:

```bash
docker run --rm \
  -e DESTINATION=postgres \
  -e 'DESTINATION__POSTGRES__CREDENTIALS=postgresql://loader:password@host/database' \
  -e SOURCES__PRODUCTS__API_KEY=secret \
  pipeline-image \
  products
```

For DuckDB:

```bash
docker run --rm \
  -e DESTINATION=duckdb \
  -e DESTINATION__DUCKDB__CREDENTIALS=/data/pipelines.duckdb \
  pipeline-image \
  products
```

Postgres and DuckDB dependencies are included initially. Additional native `dlt` destinations only require their dependency extra.

## Environment variable examples

Use one target block and the block for the source being run. Docker Compose can provide a different `.env` or `environment` block to each service.

The pipeline/schema name always comes from the runnable filename. Source settings use the name of the decorated DLT source being configured:

| Source used by the file | Environment prefix |
| --- | --- |
| Custom source in `products.py` | `SOURCES__PRODUCTS__` |
| Custom source in `chess_com.py` | `SOURCES__CHESS_COM__` |
| Custom source in `shopify.py` | `SOURCES__SHOPIFY__` |
| DLT `mongodb()` connector | `SOURCES__MONGODB__` |
| DLT `sql_database()` connector | `SOURCES__SQL_DATABASE__` |

### Targets

| Variable | Type | Required | Purpose |
| --- | --- | --- | --- |
| `DESTINATION` | String | Yes | Destination type. Currently `postgres` or `duckdb`. |
| `DESTINATION__POSTGRES__CREDENTIALS` | PostgreSQL URI | For Postgres | Target database connection. |
| `DESTINATION__DUCKDB__CREDENTIALS` | File path or DuckDB URI | For DuckDB | Target database file. Set it explicitly because the schema already uses the pipeline name. |

Postgres target:

```dotenv
DESTINATION=postgres
DESTINATION__POSTGRES__CREDENTIALS=postgresql://loader:password@postgres:5432/pipelines
```

DuckDB target:

```dotenv
DESTINATION=duckdb
DESTINATION__DUCKDB__CREDENTIALS=/data/pipelines.duckdb
```

### REST source

Generic REST variables are defined by the arguments on the decorated `source()` function. The prefix is `SOURCES__<SOURCE_FILENAME>__`.

| Common variable | Type | Required | Purpose |
| --- | --- | --- | --- |
| `SOURCES__<SOURCE>__BASE_URL` | URL string | If not fixed in code | API root URL. |
| `SOURCES__<SOURCE>__API_KEY` | Secret string | API-dependent | API-key authentication. |
| `SOURCES__<SOURCE>__ACCESS_TOKEN` | Secret string | API-dependent | Bearer or OAuth access token. |
| `SOURCES__<SOURCE>__USERNAME` | String | API-dependent | Source username. |
| `SOURCES__<SOURCE>__PASSWORD` | Secret string | API-dependent | Source password. |
| `SOURCES__<SOURCE>__START_DATE` | ISO-8601 datetime string | No | Incremental extraction starting point when implemented by the source. |
| `SOURCES__<SOURCE>__PAGE_SIZE` | Integer | No | Requested API page size when exposed by the source. |

Only arguments actually declared by that source file need environment variables.

For `pipeline/sources/rest/products.py`:

```dotenv
SOURCES__PRODUCTS__API_KEY=replace-me
```

Additional decorated source arguments follow the same pattern:

```dotenv
SOURCES__PRODUCTS__BASE_URL=https://api.example.com
SOURCES__PRODUCTS__START_DATE=2026-01-01T00:00:00Z
```

### Chess.com source

| Variable | Type | Required | Purpose |
| --- | --- | --- | --- |
| `SOURCES__CHESS_COM__USERNAME` | String | Yes | Public Chess.com username to load. |
| `SOURCES__CHESS_COM__USER_AGENT` | String | Yes | Identifies the client and supplies contact information. |

For `pipeline/sources/rest/chess_com.py`:

```dotenv
SOURCES__CHESS_COM__USERNAME=hikaru
SOURCES__CHESS_COM__USER_AGENT=client-data-pipeline/1.0 (contact: data@example.com)
```

The PubAPI does not require an API key. Run it with `chess_com` just like any other source file.

### Shopify source

| Variable | Type | Required | Purpose |
| --- | --- | --- | --- |
| `SOURCES__SHOPIFY__SHOP_URL` | HTTPS URL | Yes | Store URL, such as `https://store.myshopify.com`. |
| `SOURCES__SHOPIFY__API_VERSION` | `YYYY-MM` string | Yes | Shopify Admin API version used by the queries. |
| `SOURCES__SHOPIFY__ACCESS_TOKEN` | Secret string | Yes | Shopify Admin API access token. |

For `pipeline/sources/shopify/shopify.py`:

```dotenv
SOURCES__SHOPIFY__SHOP_URL=https://your-store.myshopify.com
SOURCES__SHOPIFY__API_VERSION=2026-07
SOURCES__SHOPIFY__ACCESS_TOKEN=replace-me
```

### MongoDB source

| Variable | Type | Required | Purpose |
| --- | --- | --- | --- |
| `SOURCES__MONGODB__CONNECTION_URL` | MongoDB URI | Yes | Source server credentials and connection settings. |
| `SOURCES__MONGODB__DATABASE` | String | No | Database to load; it may instead come from the connection URI. |
| `SOURCES__MONGODB__COLLECTION_NAMES` | Python list of strings | No | Collections to load; omission loads all collections. |

The verified `mongodb()` source reads:

```dotenv
SOURCES__MONGODB__CONNECTION_URL=mongodb://user:password@mongodb:27017
SOURCES__MONGODB__DATABASE=commerce
SOURCES__MONGODB__COLLECTION_NAMES=["products", "orders", "customers"]
```

### SQL database source

| Variable | Type | Required | Purpose |
| --- | --- | --- | --- |
| `SOURCES__SQL_DATABASE__CREDENTIALS` | SQLAlchemy database URI | Yes | Source database connection. |
| `SOURCES__SQL_DATABASE__SCHEMA` | String | No | Source schema; omission uses the database default. |
| `SOURCES__SQL_DATABASE__TABLE_NAMES` | Python list of strings | No | Tables to load; omission reflects all tables in the schema. |

The same source code works for each SQL engine. Select source tables through a Python-list environment value:

```dotenv
SOURCES__SQL_DATABASE__TABLE_NAMES=["orders", "customers", "events"]
```

PostgreSQL source:

```dotenv
SOURCES__SQL_DATABASE__CREDENTIALS=postgresql://reader:password@postgres-source:5432/application
SOURCES__SQL_DATABASE__SCHEMA=public
```

MySQL source:

```dotenv
SOURCES__SQL_DATABASE__CREDENTIALS=mysql+pymysql://reader:password@mysql-source:3306/application
```

Microsoft SQL Server source:

```dotenv
SOURCES__SQL_DATABASE__CREDENTIALS=mssql+pymssql://reader:password@mssql-source:1433/application
SOURCES__SQL_DATABASE__SCHEMA=dbo
```

Credentials containing reserved URL characters must be URL-encoded. Lists and dictionaries passed through `dlt` environment variables use Python-literal syntax.

Write disposition is intentionally absent from these tables. Set `append` or `replace` on each DLT resource, through REST `resource_defaults`, or with `set_write_dispositions()` in the source file.

## Development

The repository uses namespace packages and contains no `__init__.py` files. Install dependencies and run every test with:

```bash
python -m pip install -e . --group dev
pytest
```

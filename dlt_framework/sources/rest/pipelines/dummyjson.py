"""Example: a public demo API loaded as full snapshots."""

from dlt_framework.core import FREEZE_DATA_TYPES, Pipeline, Replace, Resource, Target
from dlt_framework.sources.rest import RestApiSource

Pipeline(
    name="dummyjson",
    target=Target("duckdb", dataset="raw_dummyjson"),
    schema_contract=FREEZE_DATA_TYPES,
    source=RestApiSource(
        name="dummyjson",
        client={
            "base_url": "https://dummyjson.com/",
            "paginator": {
                "type": "offset",
                "limit": 100,
                "offset_param": "skip",
                "limit_param": "limit",
                "total_path": "total",
            },
        },
        resources={
            Resource(name=name, write=Replace(), cursor=None, empty="fail"): {
                "path": name,
                "data_selector": name,
            }
            for name in ("products", "users", "carts")
        },
    ),
)

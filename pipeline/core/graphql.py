from collections.abc import Mapping
from typing import Any

import dlt
from dlt.extract.resource import DltResource
from dlt.sources.helpers import requests

from pipeline.core.resources import WriteDisposition


def graphql_connection_resource(
    *,
    name: str,
    endpoint: str,
    query: str,
    headers: Mapping[str, str],
    write_disposition: WriteDisposition = "append",
    connection_name: str | None = None,
    variables: Mapping[str, Any] | None = None,
) -> DltResource:
    """Create a resource from a cursor-paginated GraphQL connection."""
    resource_headers = dict(headers)
    base_variables = {} if variables is None else dict(variables)
    connection_name = name if connection_name is None else connection_name

    @dlt.resource(name=name, write_disposition=write_disposition)
    def resource():
        cursor = None

        while True:
            page_variables = {**base_variables, "cursor": cursor}
            response = requests.post(
                endpoint,
                headers=resource_headers,
                json={"query": query, "variables": page_variables},
                timeout=60,
            )
            response.raise_for_status()
            payload = response.json()
            if payload.get("errors"):
                raise RuntimeError(payload["errors"])

            connection = payload["data"][connection_name]
            yield connection["nodes"]

            page_info = connection["pageInfo"]
            if not page_info["hasNextPage"]:
                return
            cursor = page_info["endCursor"]

    return resource

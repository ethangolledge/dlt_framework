"""REST APIs through dlt's declarative ``rest_api`` source."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, ClassVar, cast

from dlt.extract.resource import DltResource
from dlt.sources.rest_api import rest_api_resources
from dlt.sources.rest_api.typing import ClientConfig, EndpointResource, EndpointResourceBase

from dlt_framework.core.backfill import RowBackfill
from dlt_framework.core.errors import DefinitionError
from dlt_framework.core.resource import Resource
from dlt_framework.core.source import Source
from dlt_framework.core.windows import RowWindow, TimeWindow, ValueWindow, Window

_FRAMEWORK_KEYS = ("start_param", "end_param", "offset_param", "limit_param")


@dataclass(frozen=True, kw_only=True)
class RestApiSource(Source[dict[str, Any]]):
    """Each resource maps to a ``rest_api`` endpoint config, plus these framework keys:

    - ``start_param``: query param receiving the cursor's start value on every run. Without it the
      cursor still filters rows, but the API returns its whole history every run.
    - ``end_param``: query param receiving the window end, on time and value backfills only.
    - ``offset_param`` / ``limit_param``: required for ``RowBackfill``; each window becomes one
      request for ``limit`` rows at ``offset``.
    """

    windows: ClassVar[tuple[type[Window], ...]] = (TimeWindow, ValueWindow, RowWindow)

    client: ClientConfig
    resource_defaults: EndpointResourceBase = field(default_factory=EndpointResourceBase)

    def validate(self, resource: Resource, config: dict[str, Any]) -> None:
        super().validate(resource, config)
        if isinstance(resource.backfill, RowBackfill) and not (
            config.get("offset_param") and config.get("limit_param")
        ):
            raise DefinitionError(
                f"{resource.name!r}: RowBackfill needs offset_param and limit_param"
            )

    def resource(
        self, resource: Resource, config: dict[str, Any], window: Window | None
    ) -> DltResource:
        endpoint = {key: value for key, value in config.items() if key not in _FRAMEWORK_KEYS}
        params = endpoint["params"] = dict(endpoint.get("params", {}))
        settings = resource.cursor.settings(window) if resource.cursor else None
        if settings is not None:
            endpoint["incremental"] = settings
            if config.get("start_param"):
                params[config["start_param"]] = "{incremental.start_value}"
            if config.get("end_param") and settings["end_value"] is not None:
                params[config["end_param"]] = "{incremental.end_value}"
        if isinstance(window, RowWindow):
            params[config["offset_param"]] = window.offset
            params[config["limit_param"]] = window.size
            endpoint["paginator"] = "single_page"
        (dlt_resource,) = rest_api_resources(
            {
                "client": self.client,
                "resource_defaults": self.resource_defaults,
                "resources": [
                    cast(EndpointResource, {"name": resource.name, "endpoint": endpoint})
                ],
            }
        )
        return dlt_resource

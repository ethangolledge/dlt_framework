import dlt
from dlt.extract.resource import DltResource
from dlt.sources.rest_api import RESTAPIConfig, rest_api_resources

from dlt_framework.core.contracts import define_source
from dlt_framework.core.models import ResourcePolicy, SourceContext

RESOURCE_NAMES = ("products", "users", "carts")


@dlt.source
def source(context: SourceContext) -> list[DltResource]:
    selected = RESOURCE_NAMES if context.selected_resource is None else (context.selected_resource,)
    config: RESTAPIConfig = {
        "client": {
            "base_url": "https://dummyjson.com/",
            "paginator": {
                "type": "offset",
                "limit": 100,
                "offset_param": "skip",
                "limit_param": "limit",
                "total_path": "total",
            },
        },
        "resource_defaults": {"write_disposition": "replace"},
        "resources": [
            {
                "name": name,
                "endpoint": {"path": name, "data_selector": name},
            }
            for name in selected
        ],
    }
    return rest_api_resources(config)


SOURCE = define_source(
    factory=source,
    resources={name: ResourcePolicy(empty="fail") for name in RESOURCE_NAMES},
)

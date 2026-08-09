import dlt

from dlt_framework.core.contracts import define_source
from dlt_framework.core.models import ResourcePolicy, SourceContext

execution_order = []


@dlt.resource(name="products", write_disposition="append")
def products():
    execution_order.append("products")
    yield {"id": 1}


@dlt.resource(name="inventory", write_disposition="replace")
def inventory():
    if execution_order != ["products"]:
        raise RuntimeError("resources did not execute sequentially")
    execution_order.append("inventory")
    yield {"product_id": 1, "quantity": 10}


@dlt.source(name="original_name_is_ignored")
def source(context: SourceContext):
    execution_order.clear()
    resources = {"products": products, "inventory": inventory}
    if context.selected_resource is not None:
        return resources[context.selected_resource]
    return list(resources.values())


SOURCE = define_source(
    factory=source,
    resources={
        "products": ResourcePolicy(empty="fail"),
        "inventory": ResourcePolicy(empty="fail"),
    },
)

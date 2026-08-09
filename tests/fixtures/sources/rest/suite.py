import dlt


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
def source():
    execution_order.clear()
    return [products, inventory]

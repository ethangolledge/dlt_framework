import dlt


@dlt.source(name="beta")
def source():
    return dlt.resource(
        [{"id": 2, "value": "beta"}],
        name="beta_rows",
        write_disposition="replace",
    )

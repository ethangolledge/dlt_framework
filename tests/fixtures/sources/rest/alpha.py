import dlt


@dlt.source(name="alpha")
def source():
    return dlt.resource(
        [{"id": 1, "value": "alpha"}],
        name="alpha_rows",
        write_disposition="append",
    )

import dlt


@dlt.source
def source(api_key: str = dlt.secrets.value):
    @dlt.resource(name="credentials")
    def credentials():
        yield {"api_key": api_key}

    return credentials

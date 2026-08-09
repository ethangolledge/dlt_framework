from pipeline.core.graphql import graphql_connection_resource


class Response:
    def __init__(self, payload):
        self.payload = payload

    def raise_for_status(self) -> None:
        pass

    def json(self):
        return self.payload


def test_graphql_connection_resource_pages_with_cursor(monkeypatch) -> None:
    responses = iter(
        [
            Response(
                {
                    "data": {
                        "products": {
                            "nodes": [{"id": "one"}],
                            "pageInfo": {"hasNextPage": True, "endCursor": "next"},
                        }
                    }
                }
            ),
            Response(
                {
                    "data": {
                        "products": {
                            "nodes": [{"id": "two"}],
                            "pageInfo": {"hasNextPage": False, "endCursor": None},
                        }
                    }
                }
            ),
        ]
    )
    request_variables = []

    def post(*args, **kwargs):
        request_variables.append(kwargs["json"]["variables"])
        return next(responses)

    monkeypatch.setattr("pipeline.core.graphql.requests.post", post)

    resource = graphql_connection_resource(
        name="products",
        endpoint="https://example.com/graphql",
        headers={"Authorization": "secret"},
        query="query Products($cursor: String) { products { nodes { id } } }",
        write_disposition="replace",
    )

    assert list(resource) == [{"id": "one"}, {"id": "two"}]
    assert request_variables == [{"cursor": None}, {"cursor": "next"}]
    assert resource.write_disposition == "replace"

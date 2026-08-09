import dlt
import pytest

from dlt_framework.core.resources import set_write_dispositions


def example_source():
    @dlt.source(name="example")
    def source():
        return [
            dlt.resource([{"id": 1}], name="products"),
            dlt.resource([{"id": 2}], name="orders"),
        ]

    return source()


def test_sets_default_and_overridden_write_dispositions() -> None:
    source = set_write_dispositions(
        example_source(),
        default="append",
        overrides={"products": "replace"},
    )

    assert source.resources["products"].write_disposition == "replace"
    assert source.resources["orders"].write_disposition == "append"


def test_rejects_unknown_resource_override() -> None:
    with pytest.raises(ValueError, match="missing"):
        set_write_dispositions(
            example_source(),
            default="append",
            overrides={"missing": "replace"},
        )

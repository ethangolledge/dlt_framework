import pytest

from dlt_framework.core.contracts import define_source
from dlt_framework.core.errors import SourceDefinitionError
from dlt_framework.core.models import ResourcePolicy


def test_define_source_copies_valid_resource_policies() -> None:
    def factory(context):
        return None

    policies = {"orders": ResourcePolicy(empty="allow")}

    contract = define_source(factory=factory, resources=policies)

    assert contract.factory is factory
    assert contract.resources == policies
    assert contract.resources is not policies


@pytest.mark.parametrize(
    ("factory", "resources", "message"),
    [
        (None, {"orders": ResourcePolicy(empty="allow")}, "callable"),
        (lambda context: None, {}, "at least one"),
        (lambda context: None, {" orders ": ResourcePolicy(empty="allow")}, "whitespace"),
    ],
)
def test_define_source_rejects_invalid_contract(factory, resources, message) -> None:
    with pytest.raises(SourceDefinitionError, match=message):
        define_source(factory=factory, resources=resources)

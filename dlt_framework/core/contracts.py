from collections.abc import Mapping

from dlt_framework.core.errors import SourceDefinitionError
from dlt_framework.core.models import ResourcePolicy, SourceContract, SourceFactory


def define_source(
    *,
    factory: SourceFactory,
    resources: Mapping[str, ResourcePolicy],
) -> SourceContract:
    """Declare the small operational contract for a client source module."""
    if not callable(factory):
        raise SourceDefinitionError("Source factory must be callable")
    if not resources:
        raise SourceDefinitionError("A source contract must declare at least one resource")
    invalid = [name for name in resources if not name or name != name.strip()]
    if invalid:
        raise SourceDefinitionError(
            "Resource policy names cannot be blank or contain surrounding whitespace"
        )
    return SourceContract(factory=factory, resources=dict(resources))

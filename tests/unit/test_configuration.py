from pathlib import PurePosixPath

import pytest

from pipeline.core.configuration import load_pipeline_config
from pipeline.core.errors import ConfigurationError
from pipeline.core.models import SourceDefinition


def source_definition() -> SourceDefinition:
    return SourceDefinition(PurePosixPath("rest/products.py"), "example", lambda: None)


def test_configuration_comes_from_source_and_destination() -> None:
    source = source_definition()

    config = load_pipeline_config(source, {"DESTINATION": " postgres "})

    assert config.name == "products"
    assert config.destination == "postgres"
    assert config.source is source


def test_destination_is_required() -> None:
    with pytest.raises(ConfigurationError, match="DESTINATION"):
        load_pipeline_config(source_definition(), {"DESTINATION": "  "})

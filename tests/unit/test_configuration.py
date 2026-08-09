from pathlib import PurePosixPath

import pytest

from dlt_framework.core.configuration import load_pipeline_config
from dlt_framework.core.errors import ConfigurationError
from dlt_framework.core.models import SourceDefinition


def source_definition() -> SourceDefinition:
    return SourceDefinition(PurePosixPath("rest/products.py"), "example", lambda context: None)


def test_configuration_comes_from_source_and_destination() -> None:
    source = source_definition()

    config = load_pipeline_config(source, {"DESTINATION": " postgres "})

    assert config.name == "products"
    assert config.pipeline_name == "products"
    assert config.dataset_name == "products"
    assert config.destination == "postgres"
    assert config.source is source
    assert config.resource is None
    assert config.backfill is None


def test_configuration_accepts_one_resource() -> None:
    config = load_pipeline_config(
        source_definition(),
        {"DESTINATION": "duckdb"},
        resource="events",
    )

    assert config.resource == "events"


def test_destination_is_required() -> None:
    with pytest.raises(ConfigurationError, match="DESTINATION"):
        load_pipeline_config(source_definition(), {"DESTINATION": "  "})


def test_production_requires_stable_pipeline_identity() -> None:
    with pytest.raises(ConfigurationError, match="PIPELINE_NAME"):
        load_pipeline_config(
            source_definition(),
            {"DESTINATION": "postgres", "DLT_FRAMEWORK_ENV": "production"},
        )


def test_rejects_duckdb_catalog_dataset_collision() -> None:
    with pytest.raises(ConfigurationError, match="cannot equal"):
        load_pipeline_config(
            source_definition(),
            {
                "DESTINATION": "duckdb",
                "DATASET_NAME": "warehouse",
                "DESTINATION__DUCKDB__CREDENTIALS": "/data/warehouse.duckdb",
            },
        )


def test_validates_retry_configuration() -> None:
    with pytest.raises(ConfigurationError, match="cannot exceed"):
        load_pipeline_config(
            source_definition(),
            {
                "DESTINATION": "postgres",
                "PIPELINE_RETRY_MIN_WAIT": "10",
                "PIPELINE_RETRY_MAX_WAIT": "1",
            },
        )


def test_production_accepts_explicit_identity_and_retry_values() -> None:
    config = load_pipeline_config(
        source_definition(),
        {
            "DESTINATION": "postgres",
            "DLT_FRAMEWORK_ENV": "production",
            "PIPELINE_NAME": "client_orders",
            "DATASET_NAME": "raw_orders",
            "PIPELINE_RETRY_ATTEMPTS": "2",
            "PIPELINE_RETRY_MIN_WAIT": "0.5",
            "PIPELINE_RETRY_MAX_WAIT": "2",
            "MAX_BACKFILL_CHUNKS": "25",
        },
    )

    assert config.pipeline_name == "client_orders"
    assert config.dataset_name == "raw_orders"
    assert config.retry.attempts == 2
    assert config.max_backfill_chunks == 25


@pytest.mark.parametrize(
    ("name", "value", "message"),
    [
        ("PIPELINE_NAME", "9bad", "invalid"),
        ("PIPELINE_RETRY_ATTEMPTS", "many", "positive integer"),
        ("PIPELINE_RETRY_ATTEMPTS", "0", "at least 1"),
        ("PIPELINE_RETRY_MIN_WAIT", "soon", "must be a number"),
        ("PIPELINE_RETRY_MIN_WAIT", "-1", "cannot be negative"),
    ],
)
def test_rejects_invalid_framework_values(name, value, message) -> None:
    values = {"DESTINATION": "postgres", name: value}
    with pytest.raises(ConfigurationError, match=message):
        load_pipeline_config(source_definition(), values)

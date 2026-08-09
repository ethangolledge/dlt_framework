import importlib
from pathlib import Path

import duckdb

from pipeline.core.configuration import load_pipeline_config
from pipeline.core.discovery import discover_source, instantiate_source
from pipeline.core.runtime import run


FIXTURE_ROOT = Path(__file__).parents[1] / "fixtures" / "sources"
FIXTURE_PACKAGE = "fixtures.sources"


def discover(name: str):
    return discover_source(
        name,
        source_root=FIXTURE_ROOT,
        source_package=FIXTURE_PACKAGE,
    )


def test_source_is_schema_and_resources_are_tables(tmp_path, monkeypatch) -> None:
    database = tmp_path / "pipelines.duckdb"
    monkeypatch.setenv("DLT_DATA_DIR", str(tmp_path / "dlt-data"))
    monkeypatch.setenv("DESTINATION__DUCKDB__CREDENTIALS", str(database))
    definition = discover("suite")
    source = instantiate_source(definition)

    assert source.name == "suite"
    assert source.schema.name == "suite"
    assert list(source.resources) == ["products", "inventory"]
    assert source.resources["products"].write_disposition == "append"
    assert source.resources["inventory"].write_disposition == "replace"

    run(load_pipeline_config(definition, {"DESTINATION": "duckdb"}))

    source_module = importlib.import_module(definition.module_name)
    assert source_module.execution_order == ["products", "inventory"]
    with duckdb.connect(str(database), read_only=True) as connection:
        assert connection.sql("SELECT count(*) FROM suite.products").fetchone()[0] == 1
        assert connection.sql("SELECT count(*) FROM suite.inventory").fetchone()[0] == 1


def test_native_resource_append_and_replace_are_preserved(tmp_path, monkeypatch) -> None:
    database = tmp_path / "disposition.duckdb"
    monkeypatch.setenv("DLT_DATA_DIR", str(tmp_path / "dlt-data"))
    monkeypatch.setenv("DESTINATION__DUCKDB__CREDENTIALS", str(database))
    alpha = load_pipeline_config(discover("alpha"), {"DESTINATION": "duckdb"})
    beta = load_pipeline_config(discover("beta"), {"DESTINATION": "duckdb"})

    run(alpha)
    run(alpha)
    run(beta)
    run(beta)

    with duckdb.connect(str(database), read_only=True) as connection:
        assert connection.sql("SELECT count(*) FROM alpha.alpha_rows").fetchone()[0] == 2
        assert connection.sql("SELECT count(*) FROM beta.beta_rows").fetchone()[0] == 1

import os
from pathlib import Path

import psycopg2
import pytest

from dlt_framework.core.configuration import load_pipeline_config
from dlt_framework.core.discovery import discover_source
from dlt_framework.core.runtime import run

FIXTURE_ROOT = Path(__file__).parents[1] / "fixtures" / "sources"
FIXTURE_PACKAGE = "fixtures.sources"


@pytest.mark.postgres
def test_postgres_destination_loads_certified_source(tmp_path, monkeypatch) -> None:
    credentials = os.environ.get("TEST_POSTGRES_URL")
    if not credentials:
        pytest.skip("TEST_POSTGRES_URL is not configured")
    monkeypatch.setenv("DLT_DATA_DIR", str(tmp_path / "dlt-data"))
    monkeypatch.setenv("DESTINATION__POSTGRES__CREDENTIALS", credentials)
    definition = discover_source(
        "alpha",
        source_root=FIXTURE_ROOT,
        source_package=FIXTURE_PACKAGE,
    )
    config = load_pipeline_config(
        definition,
        {
            "DESTINATION": "postgres",
            "PIPELINE_NAME": "alpha_postgres_ci",
            "DATASET_NAME": "raw_alpha_ci",
        },
    )

    summary = run(config)

    assert summary.chunks_loaded == 1
    with psycopg2.connect(credentials) as connection, connection.cursor() as cursor:
        cursor.execute("SELECT count(*) FROM raw_alpha_ci.alpha_rows")
        assert cursor.fetchone() == (1,)

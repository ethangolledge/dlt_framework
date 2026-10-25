import os

import psycopg2
import pytest
from conftest import ORDERS, Upstream

from dlt_framework.core import FREEZE_DATA_TYPES, Pipeline, Postgres
from dlt_framework.core.run import BackfillRun, IncrementalRun
from dlt_framework.sources.custom import CustomSource


@pytest.mark.postgres
def test_postgres_run_and_backfill(monkeypatch) -> None:
    credentials = os.environ.get("TEST_POSTGRES_URL")
    if not credentials:
        pytest.skip("TEST_POSTGRES_URL is not configured")
    monkeypatch.setenv("DESTINATION__POSTGRES__CREDENTIALS", credentials)
    with psycopg2.connect(credentials) as connection, connection.cursor() as cursor:
        cursor.execute("DROP SCHEMA IF EXISTS raw_ci CASCADE")
    upstream = Upstream([{"id": 1, "updated_at": 50}, {"id": 2, "updated_at": 100}])
    definition = Pipeline(
        name="ci",
        target=Postgres(dataset="raw_ci"),
        schema_contract=FREEZE_DATA_TYPES,
        source=CustomSource(name="ci", resources={ORDERS: upstream}),
    )

    IncrementalRun(definition)()
    BackfillRun(definition)()

    with psycopg2.connect(credentials) as connection, connection.cursor() as cursor:
        cursor.execute("SELECT id FROM raw_ci.orders ORDER BY id")
        assert cursor.fetchall() == [(1,), (2,)]

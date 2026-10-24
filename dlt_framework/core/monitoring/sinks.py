"""Sinks: where monitoring frames go. The framework never assumes which one.

A sink appends frames (``runs``, ``loads``, ...) and reads them back for ``pipeline status`` and
``backfill --missing``. ``monitoring.uri`` picks one by scheme. A new destination (HTTP collector,
queue, another warehouse) is one ``Sink`` subclass registered with ``@sinks.register(scheme)``.
"""

import uuid
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, ClassVar, Self
from urllib.parse import urlparse

import polars as pl
import sqlalchemy as sa

from dlt_framework.core.config import Registry
from dlt_framework.core.monitoring.records import Record


class Sink(ABC):
    @classmethod
    @abstractmethod
    def from_uri(cls, uri: str) -> Self:
        """Build the sink from its configured URI."""

    @abstractmethod
    def write(self, name: str, frame: pl.DataFrame) -> None:
        """Append ``frame`` to the frame called ``name``."""

    @abstractmethod
    def read(self, record: type[Record]) -> pl.DataFrame:
        """Everything written for ``record``, with its schema; empty before the first write."""

    @staticmethod
    def resolve(uri: str) -> "Sink":
        """The sink registered for ``uri``'s scheme, e.g. ``s3://...`` or ``postgresql://...``.

        Errors name the scheme only: the URI can hold a password.
        """
        return sinks.get(urlparse(uri).scheme.split("+")[0]).from_uri(uri)


sinks: Registry[Sink] = Registry("monitoring sink scheme")


@sinks.register("file", "s3", "gs", "gcs", "az", "abfs")
class ParquetSink(Sink):
    """Immutable Parquet files under ``{root}/{name}/``: local folders and object storage.

    One file per write means no read-modify-write and no races between pipelines.
    """

    def __init__(self, root: str) -> None:
        self.root = root.rstrip("/")

    @classmethod
    def from_uri(cls, uri: str) -> Self:
        return cls(uri.removeprefix("file://"))

    def write(self, name: str, frame: pl.DataFrame) -> None:
        path = f"{self.root}/{name}/{uuid.uuid4().hex}.parquet"
        if "://" not in path:  # object stores have no folders to create
            Path(path).parent.mkdir(parents=True, exist_ok=True)
        frame.write_parquet(path)

    def read(self, record: type[Record]) -> pl.DataFrame:
        folder = f"{self.root}/{record.name}"
        if "://" not in folder and not any(Path(folder).glob("*.parquet")):
            return record.frame([])
        # Files written before a column existed read it as null.
        return pl.scan_parquet(
            f"{folder}/*.parquet",
            schema=record.schema(),
            missing_columns="insert",
            extra_columns="ignore",
        ).collect()


@sinks.register("postgresql", "postgres", "mssql", "mysql", "sqlite")
class DatabaseSink(Sink):
    """Appends to tables in any SQLAlchemy database; tables are created from the frame schema.

    ``postgres://`` and ``postgresql://`` without a driver use psycopg2, the driver dlt installs
    for its Postgres destination (SQLAlchemy 2.1 would otherwise pick psycopg 3).
    """

    _TYPES: ClassVar[dict[type[pl.DataType], sa.types.TypeEngine[Any]]] = {
        pl.String: sa.Text(),
        pl.Int64: sa.BigInteger(),
        pl.Float64: sa.Float(),
        pl.Datetime: sa.DateTime(timezone=True),
    }

    _DEFAULT_DRIVERS: ClassVar[dict[str, str]] = {
        "postgres": "postgresql+psycopg2",
        "postgresql": "postgresql+psycopg2",
    }

    def __init__(self, uri: str) -> None:
        url = sa.make_url(uri)
        url = url.set(drivername=self._DEFAULT_DRIVERS.get(url.drivername, url.drivername))
        # Writes are infrequent: connect per write instead of holding a connection all run.
        self.engine = sa.create_engine(url, poolclass=sa.pool.NullPool)

    @classmethod
    def from_uri(cls, uri: str) -> Self:
        return cls(uri)

    def write(self, name: str, frame: pl.DataFrame) -> None:
        columns = [sa.Column(c, self._TYPES[type(t)]) for c, t in frame.schema.items()]
        table = sa.Table(name, sa.MetaData(), *columns)
        with self.engine.begin() as connection:
            table.create(connection, checkfirst=True)
            if frame.height:
                connection.execute(table.insert(), frame.to_dicts())

    def read(self, record: type[Record]) -> pl.DataFrame:
        with self.engine.connect() as connection:
            if not sa.inspect(connection).has_table(record.name):
                return record.frame([])
            table = sa.Table(record.name, sa.MetaData(), autoload_with=connection)
            rows = connection.execute(sa.select(table)).mappings().all()
        return pl.DataFrame([dict(row) for row in rows], schema=record.schema(), strict=False)

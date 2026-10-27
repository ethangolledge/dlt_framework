"""Sinks: where monitoring frames go. The framework never assumes which one.

A sink appends frames (``runs``, ``loads``, ...) and reads them back for ``pipeline status`` and
``backfill --missing``. ``monitoring.uri`` picks one by scheme. A new destination (HTTP collector,
queue, another warehouse) is one ``Sink`` subclass registered with ``@sinks.register(scheme)``.
"""

import uuid
from abc import ABC, abstractmethod
from typing import Any, ClassVar, Self
from urllib.parse import urlparse

import polars as pl
import sqlalchemy as sa
from fsspec.core import url_to_fs

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

    def compact(self, record: type[Record]) -> int:
        """Merge what was written for ``record`` into fewer, larger pieces; returns how many
        pieces were merged. A sink that appends in place has nothing to do."""
        return 0

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

    One file per write means no read-modify-write and no races between pipelines. Both go
    through fsspec, so a local folder and a bucket take the same path; bucket credentials come
    from the environment (e.g. ``AWS_ACCESS_KEY_ID``), as fsspec and Polars read them.
    """

    def __init__(self, root: str) -> None:
        self.fs, self.root = url_to_fs(root.rstrip("/"))

    @classmethod
    def from_uri(cls, uri: str) -> Self:
        return cls(uri)

    def write(self, name: str, frame: pl.DataFrame) -> None:
        folder = f"{self.root}/{name}"
        self.fs.makedirs(folder, exist_ok=True)  # a no-op on object storage
        with self.fs.open(f"{folder}/{uuid.uuid4().hex}.parquet", "wb") as file:
            frame.write_parquet(file)

    def compact(self, record: type[Record]) -> int:
        """Rewrite the frame's files as one, then delete them. Files written meanwhile are not
        among those read, so they are kept; a crash between the two steps leaves duplicates, never
        a loss, and the next compaction keeps one of each (records are immutable)."""
        files = self._files(record)
        if len(files) < 2:
            return 0
        self.write(record.name, self._scan(record, files).unique(maintain_order=True))
        self.fs.rm(files)
        return len(files)

    def read(self, record: type[Record]) -> pl.DataFrame:
        files = self._files(record)
        if not files:
            return record.frame([])
        return self._scan(record, files)

    def _files(self, record: type[Record]) -> list[str]:
        return list(self.fs.glob(f"{self.root}/{record.name}/*.parquet"))

    def _scan(self, record: type[Record], files: list[str]) -> pl.DataFrame:
        # Files written before a column existed read it as null.
        return pl.scan_parquet(
            [self.fs.unstrip_protocol(file) for file in files],
            schema=record.schema(),
            missing_columns="insert",
            extra_columns="ignore",
        ).collect()


@sinks.register("postgresql", "postgres", "mssql", "sqlite")
class DatabaseSink(Sink):
    """Appends to tables in Postgres (the ``postgres`` extra), SQL Server (``mssql``), or SQLite.

    Tables are created from the frame schema, and a column a newer record adds is added to the
    table before rows are written. A URL without a driver uses the one its extra installs:
    psycopg2 for Postgres (SQLAlchemy 2.1 would otherwise pick psycopg 3), pymssql for SQL Server.
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
        "mssql": "mssql+pymssql",
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
            existing = {column["name"] for column in sa.inspect(connection).get_columns(name)}
            quote = connection.dialect.identifier_preparer.quote
            for column in columns:
                if column.name not in existing:  # ADD without COLUMN: every dialect here takes it
                    kind = column.type.compile(dialect=connection.dialect)
                    connection.execute(
                        sa.text(f"ALTER TABLE {quote(name)} ADD {quote(column.name)} {kind}")
                    )
            if frame.height:
                connection.execute(table.insert(), frame.to_dicts())

    def read(self, record: type[Record]) -> pl.DataFrame:
        with self.engine.connect() as connection:
            if not sa.inspect(connection).has_table(record.name):
                return record.frame([])
            table = sa.Table(record.name, sa.MetaData(), autoload_with=connection)
            rows = connection.execute(sa.select(table)).mappings().all()
        return pl.DataFrame([dict(row) for row in rows], schema=record.schema(), strict=False)

"""Sinks: where monitoring frames go. The framework never assumes which one.

A sink receives a frame name (``runs``, ``loads``, ``samples``) and an append-only frame. A new
destination (HTTP collector, queue, another warehouse) is one ``Sink`` subclass registered for
its URI schemes; nothing else changes.
"""

import uuid
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, ClassVar, Self
from urllib.parse import urlparse

import polars as pl
import sqlalchemy as sa

from dlt_framework.core.errors import ConfigurationError


class Sink(ABC):
    schemes: ClassVar[tuple[str, ...]] = ()
    registry: ClassVar[dict[str, type["Sink"]]] = {}

    def __init_subclass__(cls, **kwargs: object) -> None:
        super().__init_subclass__(**kwargs)
        for scheme in cls.schemes:
            Sink.registry[scheme] = cls

    @classmethod
    @abstractmethod
    def from_uri(cls, uri: str) -> Self:
        """Build the sink from its configured URI."""

    @abstractmethod
    def write(self, name: str, frame: pl.DataFrame) -> None:
        """Append ``frame`` to the frame called ``name``."""

    @staticmethod
    def resolve(uri: str) -> "Sink":
        """The sink registered for ``uri``'s scheme, e.g. ``s3://...`` or ``postgresql://...``."""
        scheme = urlparse(uri).scheme.split("+")[0]
        try:
            return Sink.registry[scheme].from_uri(uri)
        except KeyError:
            known = ", ".join(sorted(Sink.registry))
            # Never echo the URI: it can hold a password.
            raise ConfigurationError(
                f"No monitoring sink for {scheme!r} URIs; schemes: {known}"
            ) from None


class ParquetSink(Sink):
    """Immutable Parquet files under ``{root}/{name}/``: local folders and object storage.

    One file per write means no read-modify-write and no races between pipelines.
    """

    schemes = ("file", "s3", "gs", "gcs", "az", "abfs")

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


class DatabaseSink(Sink):
    """Appends to tables in any SQLAlchemy database; tables are created from the frame schema."""

    schemes = ("postgresql", "postgres", "mssql", "mysql", "sqlite")
    _TYPES: ClassVar[dict[type[pl.DataType], sa.types.TypeEngine[Any]]] = {
        pl.String: sa.Text(),
        pl.Int64: sa.BigInteger(),
        pl.Float64: sa.Float(),
        pl.Datetime: sa.DateTime(timezone=True),
    }

    def __init__(self, uri: str) -> None:
        # Writes are infrequent: connect per write instead of holding a connection all run.
        self.engine = sa.create_engine(uri, poolclass=sa.pool.NullPool)

    @classmethod
    def from_uri(cls, uri: str) -> Self:
        return cls(uri)

    def write(self, name: str, frame: pl.DataFrame) -> None:
        columns = [sa.Column(c, self._TYPES[type(t)]) for c, t in frame.schema.items()]
        table = sa.Table(name, sa.MetaData(), *columns)
        with self.engine.begin() as connection:
            table.create(connection, checkfirst=True)
            connection.execute(table.insert(), frame.to_dicts())

"""Files in S3, GCS, Azure, or a local folder through dlt's ``filesystem`` source."""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import ClassVar

from dlt.extract.resource import DltResource
from dlt.sources.filesystem import filesystem, read_csv_duckdb, read_jsonl, read_parquet

from dlt_framework.core import buckets
from dlt_framework.core.cursor import TimeCursor
from dlt_framework.core.errors import DefinitionError
from dlt_framework.core.resource import Resource
from dlt_framework.core.source import Extraction, Source
from dlt_framework.core.windows import TimeWindow, Window

MODIFICATION_DATE = "modification_date"


class Reader(ABC):
    """How a resource's listed files become rows."""

    @abstractmethod
    def transformer(self, extraction: Extraction) -> DltResource:
        """A dlt transformer from file items to rows, built for each extract; work due once its
        rows are committed goes to ``extraction.after_commit``."""

    def requires(self) -> dict[str, str]:
        """Modules dlt imports at read time, mapped to the extra that installs them."""
        return {}


@dataclass(frozen=True, slots=True)
class Csv(Reader):
    def transformer(self, extraction: Extraction) -> DltResource:
        return read_csv_duckdb()

    def requires(self) -> dict[str, str]:
        return {"duckdb": "filesystem"}


@dataclass(frozen=True, slots=True)
class Jsonl(Reader):
    def transformer(self, extraction: Extraction) -> DltResource:
        return read_jsonl()


@dataclass(frozen=True, slots=True)
class Parquet(Reader):
    def transformer(self, extraction: Extraction) -> DltResource:
        return read_parquet()

    def requires(self) -> dict[str, str]:
        return {"pyarrow": "filesystem"}


@dataclass(frozen=True, slots=True, kw_only=True)
class Files:
    """Where a resource's files are and how to read them.

    ``bucket_url`` is e.g. ``s3://bucket/prefix`` or ``file:///data/in``; credentials resolve from
    ``sources.filesystem.credentials``.
    """

    bucket_url: str
    file_glob: str
    reader: Reader


@dataclass(frozen=True, kw_only=True)
class FilesystemSource(Source[Files]):
    """Each resource maps to ``Files``. The cursor, if any, is the file modification date: new or
    changed files are read; rows inside a file are not filtered."""

    windows: ClassVar[tuple[type[Window], ...]] = (TimeWindow,)

    def validate(self, resource: Resource, config: Files) -> None:
        super().validate(resource, config)
        cursor = resource.cursor
        if cursor is not None and (
            not isinstance(cursor, TimeCursor)
            or cursor.path != MODIFICATION_DATE
            or cursor.fallbacks
        ):
            raise DefinitionError(
                f"{resource.name!r}: a filesystem cursor is TimeCursor(path='{MODIFICATION_DATE}') "
                "without fallbacks"
            )

    def requires(self) -> dict[str, str]:
        needed: dict[str, str] = {}
        for files in self.resources.values():
            needed |= files.reader.requires()
            needed |= buckets.requires(files.bucket_url)
        return needed

    def state_key(self, resource: Resource) -> str:
        # The cursor lives on the file listing, which dlt names "<listing>_<reader resource>".
        return f"{self._listing_name(resource)}_{resource.name}"

    def resource(self, resource: Resource, config: Files, extraction: Extraction) -> DltResource:
        listing = filesystem(bucket_url=config.bucket_url, file_glob=config.file_glob)
        listing = listing.with_name(self._listing_name(resource))
        if resource.cursor is not None:
            listing.apply_hints(incremental=resource.cursor.incremental(extraction.window))
        return (listing | config.reader.transformer(extraction)).with_name(resource.name)

    @staticmethod
    def _listing_name(resource: Resource) -> str:
        return f"{resource.name}_files"

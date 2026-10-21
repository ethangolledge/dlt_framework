"""Files in S3, GCS, Azure, or a local folder through dlt's ``filesystem`` source."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, ClassVar, Literal

from dlt.extract.resource import DltResource
from dlt.sources.filesystem import filesystem, read_csv_duckdb, read_jsonl, read_parquet

from dlt_framework.core.cursor import TimeCursor
from dlt_framework.core.errors import DefinitionError
from dlt_framework.core.resource import Resource
from dlt_framework.core.source import Source
from dlt_framework.core.windows import TimeWindow, Window

_READERS: dict[str, Any] = {"csv": read_csv_duckdb, "jsonl": read_jsonl, "parquet": read_parquet}
MODIFICATION_DATE = "modification_date"


@dataclass(frozen=True, slots=True, kw_only=True)
class Files:
    """Where a resource's files are and how to read them.

    ``bucket_url`` is e.g. ``s3://bucket/prefix`` or ``file:///data/in``; credentials resolve from
    ``sources.filesystem.credentials``.
    """

    bucket_url: str
    file_glob: str
    reader: Literal["csv", "jsonl", "parquet"]


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

    def state_key(self, resource: Resource) -> str:
        # The cursor lives on the file listing, which dlt names "<listing>_<reader resource>".
        return f"{self._listing_name(resource)}_{resource.name}"

    def resource(self, resource: Resource, config: Files, window: Window | None) -> DltResource:
        listing = filesystem(bucket_url=config.bucket_url, file_glob=config.file_glob)
        listing = listing.with_name(self._listing_name(resource))
        if resource.cursor is not None:
            listing.apply_hints(incremental=resource.cursor.incremental(window))
        return (listing | _READERS[config.reader]()).with_name(resource.name)

    @staticmethod
    def _listing_name(resource: Resource) -> str:
        return f"{resource.name}_files"

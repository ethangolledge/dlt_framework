"""The ``Documents`` reader: listed files to rows, then their disposal once committed."""

import logging
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from typing import Any, Literal

import dlt
from dlt.common.exceptions import TerminalValueError
from dlt.common.storages.fsspec_filesystem import FileItemDict
from dlt.extract.resource import DltResource
from pydantic import BaseModel, ValidationError

from dlt_framework.core.source import Extraction
from dlt_framework.sources.filesystem.documents.disposal import Disposal
from dlt_framework.sources.filesystem.documents.document import (
    UNREADABLE,
    Document,
    ExtractionError,
    Ocr,
)
from dlt_framework.sources.filesystem.documents.extractors import Extractor
from dlt_framework.sources.filesystem.source import Reader

LOGGER = logging.getLogger(__name__)


class DocumentError(TerminalValueError):
    """No extractor read a document whose reader states ``on_invalid="raise"``."""


@dataclass(frozen=True, kw_only=True)
class Documents(Reader):
    """PDFs to rows of ``schema``, each with its ``document`` path and ``extracted_by``.

    Each document is tried against ``extract`` in order, and the first result that validates
    lands. ``on_invalid`` decides what a document none of them reads does: ``"raise"`` fails the
    extract, so nothing is committed; ``"warn"`` logs it, loads the rest, and leaves the file in
    place. ``once_loaded`` disposes of each file read (``Keep``, ``Delete``, ``Archive``) after
    its rows are committed, never before, so a failed run loses nothing; files of a run
    interrupted between extract and load stay in place. ``ocr`` gives scans a text layer.
    """

    schema: type[BaseModel]
    extract: tuple[Extractor, ...]
    on_invalid: Literal["raise", "warn"]
    once_loaded: Disposal
    ocr: Ocr | None = None

    def transformer(self, extraction: Extraction) -> DltResource:
        read: list[FileItemDict] = []  # this extract's files, disposed of once committed

        def dispose() -> None:
            for item in read:
                self.once_loaded(item)

        extraction.after_commit(dispose)

        @dlt.transformer(name="documents")
        def documents(items: Iterable[FileItemDict]) -> Iterator[dict[str, Any]]:
            for item in items:
                row = self._row(item)
                if row is not None:
                    read.append(item)
                    yield row

        return documents

    def requires(self) -> dict[str, str]:
        return self.once_loaded.requires()

    def _row(self, item: FileItemDict) -> dict[str, Any] | None:
        failures = []
        try:
            document = Document.read(item.read_bytes(), self.ocr)
            for extractor in self.extract:
                try:
                    found = extractor.extract(document, self.schema)
                except (ExtractionError, ValidationError) as error:
                    failures.append(f"{extractor.name}: {error}")
                    continue
                extra = {"document": item["relative_path"], "extracted_by": extractor.name}
                return found.model_dump() | extra
        except UNREADABLE as error:
            failures.append(f"not a readable PDF: {error}")
        summary = f"{item['relative_path']}: no extractor read it; " + "; ".join(failures)
        if self.on_invalid == "raise":
            raise DocumentError(summary)
        LOGGER.warning("%s. The file is left in place", summary)
        return None

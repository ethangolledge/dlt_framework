"""PDFs read into rows of a stated Pydantic model. Needs the ``documents`` extra.

State each known layout as a ``Template``: it runs locally, costs nothing, and is exact. End
with a ``Model`` to read any other layout, billed per document. A nested list in the schema
lands as a child table (``<resource>__<field>``). With ``ocr`` stated, a scan gets a text layer
before the templates run, so printed scans are read locally too.

Not re-exported from the filesystem package, so CSV and Parquet pipelines never need the extra.
"""

from dlt_framework.sources.filesystem.documents.disposal import Archive, Delete, Disposal, Keep
from dlt_framework.sources.filesystem.documents.document import (
    Document,
    ExtractionError,
    Ocr,
    Word,
)
from dlt_framework.sources.filesystem.documents.extractors import (
    Extractor,
    Model,
    ModelRequestError,
    Template,
)
from dlt_framework.sources.filesystem.documents.locators import Locator, Table, Text
from dlt_framework.sources.filesystem.documents.reader import DocumentError, Documents

__all__ = [
    "Archive",
    "Delete",
    "Disposal",
    "Document",
    "DocumentError",
    "Documents",
    "ExtractionError",
    "Extractor",
    "Keep",
    "Locator",
    "Model",
    "ModelRequestError",
    "Ocr",
    "Table",
    "Template",
    "Text",
    "Word",
]

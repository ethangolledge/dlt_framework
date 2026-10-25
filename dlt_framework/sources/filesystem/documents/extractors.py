"""Ways to read a document into the schema: a stated layout, or a model for any layout."""

import base64
import re
from abc import ABC, abstractmethod
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, TypeVar

import anthropic
from dlt.common.exceptions import TerminalValueError
from pydantic import BaseModel

from dlt_framework.sources.filesystem.documents.document import Document, ExtractionError
from dlt_framework.sources.filesystem.documents.locators import Locator

SchemaT = TypeVar("SchemaT", bound=BaseModel)
_MAX_TOKENS = 16000
_RETRIED = {408, 409, 429}  # client errors the SDK retries; every other 4xx is final


class ModelRequestError(TerminalValueError):
    """The model API refused the request (a bad key, model name, or request); retrying cannot
    fix it, so the run fails at once instead of retrying the extract."""


class Extractor(ABC):
    name: str
    """Recorded on each row as ``extracted_by``."""

    @abstractmethod
    def extract(self, document: Document, schema: type[SchemaT]) -> SchemaT:
        """The document as ``schema``; raises ``ExtractionError`` or pydantic's
        ``ValidationError`` when this extractor cannot read it."""


@dataclass(frozen=True, kw_only=True)
class Template(Extractor):
    """A known layout, read locally. ``match`` is a pattern found only in documents of that
    layout; ``fields`` locates each schema field. Values arrive as text: the schema's types and
    validators parse them, so a layout that has drifted fails validation instead of landing."""

    name: str
    match: str
    fields: Mapping[str, Locator]

    def extract(self, document: Document, schema: type[SchemaT]) -> SchemaT:
        if re.search(self.match, document.text) is None:
            raise ExtractionError(f"not the {self.name} layout")
        return schema.model_validate(
            {name: locator.find(document) for name, locator in self.fields.items()}
        )


@dataclass(frozen=True, kw_only=True)
class Model(Extractor):
    """Any layout, read by the Claude model ``name`` with structured outputs at low effort,
    billed per document. It is sent the document's text, or for a scan the PDF itself, read as
    page images: they cost more, but tick boxes and handwriting are not text. The key comes from
    ``ANTHROPIC_API_KEY``."""

    name: str
    instructions: str

    def extract(self, document: Document, schema: type[SchemaT]) -> SchemaT:
        content: str | list[Any] = document.text
        if document.scanned:
            pdf = base64.standard_b64encode(document.data).decode()
            source = {"type": "base64", "media_type": "application/pdf", "data": pdf}
            content = [{"type": "document", "source": source}]
        try:
            response = anthropic.Anthropic().messages.parse(
                model=self.name,
                max_tokens=_MAX_TOKENS,
                output_config={"effort": "low"},
                output_format=schema,
                system=self.instructions,
                messages=[{"role": "user", "content": content}],
            )
        except anthropic.APIStatusError as error:
            if error.status_code < 500 and error.status_code not in _RETRIED:
                raise ModelRequestError(f"{self.name}: {error.message}") from error
            raise
        if response.parsed_output is None:
            raise ExtractionError(f"{self.name} stopped with {response.stop_reason}")
        return response.parsed_output

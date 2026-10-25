"""One document as the extractors see it, and reading it: OCR first when it is a scan."""

from dataclasses import dataclass
from typing import Any

import liteparse


class ExtractionError(ValueError):
    """This step cannot read this document; the reader moves on to the next extractor."""


UNREADABLE = (liteparse.ParseError, ExtractionError)
"""Errors that mean the file cannot be read at all, whatever the extractor."""


@dataclass(frozen=True, slots=True, kw_only=True)
class Ocr:
    """Tesseract, bundled with LiteParse; each recognised word is placed where it is printed, so
    ``Table`` columns work on scans. ``languages`` are Tesseract codes, such as ``("eng",)``.
    ``tessdata`` is the folder holding their ``.traineddata`` files (the ``tesseract`` language
    packages install them, e.g. ``/usr/share/tessdata``); ``None`` lets LiteParse download them
    from GitHub on first use. Pages are not turned upright: a scan fed in sideways reads as noise.
    """

    languages: tuple[str, ...]
    tessdata: str | None

    def settings(self) -> dict[str, Any]:
        return {
            "ocr_enabled": True,
            "ocr_language": "+".join(self.languages),
            "tessdata_path": self.tessdata,
            "ocr_failure_fatal": True,
        }


@dataclass(frozen=True, slots=True)
class Word:
    text: str
    x: float
    """Left edge, in points from the page's left edge."""
    y: float
    """Top edge, in points from the page's top edge."""


@dataclass(frozen=True)
class Document:
    """``data`` is the file as stored. ``pages`` and ``text`` are what templates read, from an
    OCR pass when the file is a scan (``scanned``) and the reader states ``ocr``. ``text`` has one
    line per printed line, with the layout's runs of spaces collapsed to one."""

    data: bytes
    pages: tuple[tuple[Word, ...], ...]
    text: str
    scanned: bool

    @classmethod
    def read(cls, data: bytes, ocr: Ocr | None) -> "Document":
        """The document in ``data``; raises one of ``UNREADABLE`` when it cannot be read."""
        parsed = _parse(data, {"ocr_enabled": False})
        scanned = not any(page.text.strip() for page in parsed.pages)
        if scanned and ocr:
            parsed = _parse(data, ocr.settings())
        pages = tuple(
            tuple(Word(w.text, w.x, w.y) for item in page.text_items for w in item.words)
            for page in parsed.pages
        )
        text = "\n".join(" ".join(line.split()) for line in parsed.text.splitlines())
        return cls(data, pages, text, scanned)


def _parse(data: bytes, settings: dict[str, Any]) -> liteparse.ParseResult:
    with liteparse.LiteParse(emit_word_boxes=True, quiet=True, **settings) as parser:
        return parser.parse(data)

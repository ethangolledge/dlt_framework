"""Where a field's value is printed in a document of a known layout."""

import re
from abc import ABC, abstractmethod
from bisect import bisect_right
from collections.abc import Iterable, Iterator, Mapping
from dataclasses import dataclass
from operator import attrgetter
from typing import Any

from dlt_framework.sources.filesystem.documents.document import Document, ExtractionError, Word

_LINE_TOLERANCE = 3  # points: words this close vertically share a line


class Locator(ABC):
    @abstractmethod
    def find(self, document: Document) -> Any:
        """The field's value as printed; raises ``ExtractionError`` when it is not there."""


@dataclass(frozen=True, slots=True)
class Text(Locator):
    """The first capture group of ``pattern`` in the document's text."""

    pattern: str

    def find(self, document: Document) -> str:
        found = re.search(self.pattern, document.text, re.MULTILINE)
        if found is None:
            raise ExtractionError(f"no text matches {self.pattern!r}")
        return found.group(1)


@dataclass(frozen=True, slots=True, kw_only=True)
class Table(Locator):
    """Rows of a table laid out in columns, read on every page.

    ``columns`` maps each column to the x position of its left edge, in points from the page's
    left edge (``Document.pages`` holds each header word's ``x``). A column ends where the next
    begins, the last at the page edge; words left of the first column are not read. Positions are
    used rather than detected cell borders, which break on shaded or borderless tables.

    A row starts at a line whose ``key`` cell matches ``starts``; the lines below it on the same
    page continue its cells, so wrapped text is joined. Lines before a page's first row (headers)
    are skipped. ``ignore`` drops lines and ``ends`` ends the table; both match the whole
    printed line, including words outside the columns.

    ``header`` matches the lines of the table header repeated on each page: they are dropped, and
    a row (or its note) cut by a page break continues on the next page. Without it, lines before
    a page's first row are skipped, since they cannot be told from a header.

    ``notes`` matches the first line of a note printed across the table under its row, such as a
    comment or a description: from there to the next row, lines land whole, one per line, in the
    row's ``notes`` cell instead of being split into columns.
    """

    columns: Mapping[str, float]
    key: str
    starts: str
    ends: str | None = None
    ignore: str | None = None
    header: str | None = None
    notes: str | None = None

    def find(self, document: Document) -> list[dict[str, str]]:
        names = sorted(self.columns, key=self.columns.__getitem__)
        edges = [self.columns[name] for name in names]
        rows: list[dict[str, str]] = []
        row: dict[str, str] | None = None
        noting = False  # inside the current row's note
        for page in document.pages:
            started = False  # a row has started on this page
            for words in _lines(page):
                text = " ".join(word.text for word in words)
                if self.ends and re.search(self.ends, text):
                    return self._found(rows)
                if any(p and re.search(p, text) for p in (self.ignore, self.header)):
                    continue
                cells = dict.fromkeys(names, "")
                for word in words:
                    if column := bisect_right(edges, word.x):  # 0: left of the table
                        name = names[column - 1]
                        cells[name] = f"{cells[name]} {word.text}".lstrip()
                if re.fullmatch(self.starts, cells[self.key]):
                    row = cells | ({"notes": ""} if self.notes else {})
                    rows.append(row)
                    started, noting = True, False
                elif row is None or not (started or self.header):
                    continue
                elif self.notes and (noting or re.search(self.notes, text)):
                    row["notes"] = f"{row['notes']}\n{text}".lstrip("\n")
                    noting = True
                else:
                    for name, value in cells.items():
                        row[name] = f"{row[name]} {value}".strip()
        return self._found(rows)

    def _found(self, rows: list[dict[str, str]]) -> list[dict[str, str]]:
        if not rows:
            raise ExtractionError(f"no row whose {self.key} matches {self.starts!r}")
        return rows


def _lines(words: Iterable[Word]) -> Iterator[list[Word]]:
    """Words grouped into printed lines, top to bottom, each left to right."""
    line: list[Word] = []
    for word in sorted(words, key=attrgetter("y")):
        if line and word.y - line[-1].y > _LINE_TOLERANCE:
            yield sorted(line, key=attrgetter("x"))
            line = []
        line.append(word)
    if line:
        yield sorted(line, key=attrgetter("x"))

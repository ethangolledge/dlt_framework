"""A resource: one stream of rows from a source into one destination table."""

from dataclasses import dataclass
from typing import Literal

from dlt_framework.core.backfill import Backfill
from dlt_framework.core.cursor import Cursor
from dlt_framework.core.errors import DefinitionError, require_identifier
from dlt_framework.core.write import WriteMode

EmptyPolicy = Literal["allow", "warn", "fail"]


@dataclass(frozen=True, kw_only=True)
class Resource:
    """Every choice that decides what lands is stated here.

    ``name`` is the destination table and the dlt resource name (part of its state key).
    ``cursor=None`` means every run extracts everything (a snapshot). ``empty`` decides what an
    incremental run does when the resource extracts nothing; ``fail`` stops a ``Replace`` resource
    from emptying its table. ``backfill`` declares how history is loaded, if it can be.

    ``chunk`` splits a large increment into consecutive loads of about that many new items of
    what the cursor tracks (rows; files for a filesystem source), each committing its rows and
    cursor before the next starts. A chunk ends with the page or batch that reaches it, so size
    it in multiples of the source's page. Memory, the size of each merge, and what a failure
    costs are then bounded by the chunk, not by how much changed upstream. It needs a cursor with
    ``row_order="asc"``: SQL and filesystem sources sort by it, other upstreams must return rows
    in that order. Backfills ignore it; their windows already bound each load.
    """

    name: str
    write: WriteMode
    cursor: Cursor | None
    empty: EmptyPolicy
    backfill: Backfill | None = None
    chunk: int | None = None

    def __post_init__(self) -> None:
        require_identifier(self.name, "Resource name")
        if self.backfill is not None:
            self.backfill.check(self.cursor)
        if self.chunk is not None and (problem := self._chunk_problem()):
            raise DefinitionError(f"{self.name!r} cannot load in chunks: {problem}")

    def _chunk_problem(self) -> str | None:
        """Why committing the cursor at the end of every chunk would skip or repeat rows."""
        cursor = self.cursor
        if self.chunk is not None and self.chunk <= 0:
            return f"chunk must be positive, not {self.chunk}"
        if cursor is None or cursor.row_order != "asc":
            return (
                "it needs a cursor with row_order='asc', since each chunk commits the cursor at "
                "its last row and a row arriving later with a lower value would be skipped"
            )
        if cursor.lag:
            return "lag re-reads the rows behind the cursor in every chunk"
        if cursor.on_missing == "include":
            return "rows without a cursor value would be read again by every chunk"
        return self.write.split_problem()

"""A resource: one stream of rows from a source into one destination table."""

from dataclasses import dataclass
from typing import Literal

from dlt_framework.core.backfill import Backfill
from dlt_framework.core.cursor import Cursor
from dlt_framework.core.errors import require_identifier
from dlt_framework.core.write import WriteMode

EmptyPolicy = Literal["allow", "warn", "fail"]


@dataclass(frozen=True, kw_only=True)
class Resource:
    """Every choice that decides what lands is stated here.

    ``name`` is the destination table and the dlt resource name (part of its state key).
    ``cursor=None`` means every run extracts everything (a snapshot). ``empty`` decides what an
    incremental run does when the resource extracts nothing; ``fail`` stops a ``Replace`` resource
    from emptying its table. ``backfill`` declares how history is loaded, if it can be.
    """

    name: str
    write: WriteMode
    cursor: Cursor | None
    empty: EmptyPolicy
    backfill: Backfill | None = None

    def __post_init__(self) -> None:
        require_identifier(self.name, "Resource name")
        if self.backfill is not None:
            self.backfill.check(self.cursor)

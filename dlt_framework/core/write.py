"""Write modes: how extracted rows are written to the destination table."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Literal

from dlt_framework.core.errors import DefinitionError


class WriteMode(ABC):
    @abstractmethod
    def hints(self) -> dict[str, Any]:
        """Keyword arguments for ``DltResource.apply_hints``."""


@dataclass(frozen=True, slots=True)
class Append(WriteMode):
    """Adds every extracted row. Replayed rows are duplicated."""

    def hints(self) -> dict[str, Any]:
        return {"write_disposition": "append"}


@dataclass(frozen=True, slots=True)
class Replace(WriteMode):
    """Swaps the table for the extract. The strategy is destination-wide (``.dlt/config.toml``)."""

    def hints(self) -> dict[str, Any]:
        return {"write_disposition": "replace"}


@dataclass(frozen=True, slots=True, kw_only=True)
class Merge(WriteMode):
    """Upserts by ``primary_key``; replays are idempotent."""

    primary_key: tuple[str, ...]
    strategy: Literal["delete-insert", "upsert", "scd2", "insert-only"]

    def __post_init__(self) -> None:
        if not self.primary_key:
            raise DefinitionError("Merge needs at least one primary key column")

    def hints(self) -> dict[str, Any]:
        return {
            "write_disposition": {"disposition": "merge", "strategy": self.strategy},
            "primary_key": list(self.primary_key),
        }

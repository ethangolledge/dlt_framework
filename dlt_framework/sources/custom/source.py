"""Any extraction logic dlt has no declarative source for."""

from collections.abc import Callable, Iterator
from dataclasses import dataclass
from typing import Any, ClassVar

from dlt.extract.incremental import Incremental
from dlt.extract.resource import DltResource

from dlt_framework.core.resource import Resource
from dlt_framework.core.source import Extraction, Source
from dlt_framework.core.windows import RowWindow, TimeWindow, ValueWindow, Window

ExtractFunction = Callable[[Incremental[Any] | None, Window | None], Iterator[Any]]


@dataclass(frozen=True, kw_only=True)
class CustomSource(Source[ExtractFunction]):
    """Each resource maps to a generator ``fn(incremental, window) -> Iterator[rows or pages]``.

    ``incremental`` is ``None`` without a cursor or for row windows; otherwise use its
    ``start_value`` and ``end_value`` in the upstream request, and dlt filters and tracks the rows
    you yield. ``window`` is ``None`` on runs; on backfills, page through ``RowWindow`` yourself.
    """

    windows: ClassVar[tuple[type[Window], ...]] = (TimeWindow, ValueWindow, RowWindow)

    def resource(
        self, resource: Resource, config: ExtractFunction, extraction: Extraction
    ) -> DltResource:
        return self._generator(
            resource, extraction, lambda incremental: config(incremental, extraction.window)
        )

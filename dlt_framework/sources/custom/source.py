"""Any extraction logic dlt has no declarative source for."""

from collections.abc import Callable, Iterator
from dataclasses import dataclass
from typing import Any, ClassVar

import dlt
from dlt.extract.incremental import Incremental
from dlt.extract.resource import DltResource

from dlt_framework.core.resource import Resource
from dlt_framework.core.source import Source
from dlt_framework.core.windows import RowWindow, TimeWindow, ValueWindow, Window

Extractor = Callable[[Incremental[Any] | None, Window | None], Iterator[Any]]


@dataclass(frozen=True, kw_only=True)
class CustomSource(Source[Extractor]):
    """Each resource maps to a generator ``fn(incremental, window) -> Iterator[rows or pages]``.

    ``incremental`` is ``None`` without a cursor or for row windows; otherwise use its
    ``start_value`` and ``end_value`` in the upstream request, and dlt filters and tracks the rows
    you yield. ``window`` is ``None`` on runs; on backfills, page through ``RowWindow`` yourself.
    """

    windows: ClassVar[tuple[type[Window], ...]] = (TimeWindow, ValueWindow, RowWindow)

    def resource(self, resource: Resource, config: Extractor, window: Window | None) -> DltResource:
        # dlt binds state and filtering only to a parameter annotated as Incremental, so the
        # user's function is wrapped instead of trusting it to carry the annotation.
        def extract(incremental: Incremental[Any] | None = None) -> Iterator[Any]:
            yield from config(incremental, window)

        cursor = resource.cursor.incremental(window) if resource.cursor else None
        return dlt.resource(extract, name=resource.name)(incremental=cursor)

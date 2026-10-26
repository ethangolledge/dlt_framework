"""The contract every source type implements."""

from abc import ABC, abstractmethod
from collections.abc import Callable, Iterable, Iterator, Mapping
from dataclasses import dataclass, field
from typing import Any, ClassVar, Generic, TypeVar

import dlt
from dlt.extract.incremental import Incremental
from dlt.extract.items_transform import LimitItem
from dlt.extract.resource import DltResource
from dlt.extract.source import DltSource

from dlt_framework.core.errors import DefinitionError, require_identifier
from dlt_framework.core.resource import Resource
from dlt_framework.core.windows import Window

ConfigT = TypeVar("ConfigT")


class Chunk(LimitItem):
    """Stops a resource once ``rows`` new rows have passed dlt's incremental filter.

    It is dlt's ``add_limit(count_rows=True)``, except that it is never pushed into the upstream
    request (``sql_table`` would add ``LIMIT``). The closed start of each chunk re-reads the rows
    sharing the cursor's last value, and dlt drops them as already loaded: counted upstream, they
    could fill a chunk and leave the cursor where it was.
    """

    def __init__(self, rows: int) -> None:
        super().__init__(max_items=rows, max_time=None, count_rows=True)
        self.exhausted = False

    def limit(self, chunk_size: int) -> None:
        return None


@dataclass(frozen=True, slots=True)
class Extraction:
    """One build of a source: the window it covers, and what to do once its rows are committed.

    Made fresh for every extract, so what one extract holds (such as the files to dispose of)
    never reaches another run, even of a pipeline sharing the same definition objects.
    """

    window: Window | None
    _committed: list[Callable[[], None]] = field(default_factory=list)
    _chunks: list[Chunk] = field(default_factory=list)

    def after_commit(self, callback: Callable[[], None]) -> None:
        """Call ``callback`` once this extract's rows are committed; never after a failure."""
        self._committed.append(callback)

    def committed(self) -> None:
        for callback in self._committed:
            callback()

    def chunk(self, resource: DltResource, rows: int) -> None:
        """Stop ``resource`` after ``rows`` new rows, on the pipe that holds its cursor: the
        head of the pipe, such as a file listing rather than its reader, so files are read
        whole."""
        pipe = resource._pipe
        while pipe.has_parent:
            pipe = pipe.parent
        self._chunks.append(chunk := Chunk(rows))
        pipe.append_step(chunk)

    @property
    def cut(self) -> bool:
        """Whether a chunk stopped its resource before upstream ran out, so more may follow."""
        return any(chunk.exhausted for chunk in self._chunks)


@dataclass(frozen=True, kw_only=True)
class Source(ABC, Generic[ConfigT]):
    """A named group of resources, each with source-type specific extraction config.

    ``name`` is the dlt source and schema name and part of every resource's state key: it must
    never change once a pipeline has loaded. Resources are always extracted in parallel threads;
    ``[sources.<name>.extract] workers`` in config sets how many (``1`` serialises a rate-limited
    upstream).

    A source type implements ``resource`` and lists the ``windows`` it can push upstream. It can
    extend ``validate`` for its own definition rules. Write hints, cursor preparation, and
    parallelism are applied here, once, for every source type.
    """

    windows: ClassVar[tuple[type[Window], ...]]

    name: str
    resources: Mapping[Resource, ConfigT]

    def __post_init__(self) -> None:
        require_identifier(self.name, "Source name")
        for resource, config in self.resources.items():
            self.validate(resource, config)

    @abstractmethod
    def resource(self, resource: Resource, config: ConfigT, extraction: Extraction) -> DltResource:
        """The dlt resource named ``resource.name``, with its cursor or ``extraction.window``
        pushed into the upstream request so only the needed rows are fetched."""

    def requires(self) -> dict[str, str]:
        """Modules extraction needs beyond the core, mapped to the extra that installs them.

        Libraries the source package imports itself need no entry: without them it never loads.
        Declare what dlt imports only at run time, such as a SQL backend or a bucket protocol.
        """
        return {}

    def state_key(self, resource: Resource) -> str:
        """The dlt resource whose state holds this resource's cursor."""
        return resource.name

    def validate(self, resource: Resource, config: ConfigT) -> None:
        if resource.backfill is not None and resource.backfill.window not in self.windows:
            raise DefinitionError(
                f"{type(self).__name__} cannot push {resource.backfill.window.__name__} upstream "
                f"for {resource.name!r}"
            )

    def build(
        self, resources: Iterable[Resource], window: Window | None
    ) -> tuple[DltSource, Extraction]:
        """The dlt source extracting ``resources`` over ``window``, and its ``Extraction``, whose
        ``committed`` the run calls once the rows are committed."""
        extraction = Extraction(window)
        prepared = [self._prepared(resource, extraction) for resource in resources]
        source = dlt.source(name=self.name)(lambda: prepared)()
        return source.parallelize(), extraction

    @staticmethod
    def _generator(
        resource: Resource,
        extraction: Extraction,
        extract: Callable[[Incremental[Any] | None], Iterator[Any]],
    ) -> DltResource:
        """``extract(incremental)`` as the dlt resource ``resource.name``, given its cursor.

        dlt binds state and filtering only to a parameter annotated as ``Incremental``, so
        ``extract`` is wrapped instead of trusting it to carry the annotation.
        """

        def run(incremental: Incremental[Any] | None = None) -> Iterator[Any]:
            yield from extract(incremental)

        cursor = resource.cursor.incremental(extraction.window) if resource.cursor else None
        return dlt.resource(run, name=resource.name)(incremental=cursor)

    def _prepared(self, resource: Resource, extraction: Extraction) -> DltResource:
        dlt_resource = self.resource(resource, self.resources[resource], extraction)
        dlt_resource.apply_hints(**resource.write.hints())
        if resource.cursor is not None:
            resource.cursor.prepare(dlt_resource)
        if resource.chunk is not None and extraction.window is None:
            extraction.chunk(dlt_resource, resource.chunk)
        return dlt_resource

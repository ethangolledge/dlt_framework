"""The contract every source type implements."""

from abc import ABC, abstractmethod
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import ClassVar, Generic, TypeVar

import dlt
from dlt.extract.resource import DltResource
from dlt.extract.source import DltSource

from dlt_framework.core.errors import DefinitionError, require_identifier
from dlt_framework.core.resource import Resource
from dlt_framework.core.windows import Window

ConfigT = TypeVar("ConfigT")


@dataclass(frozen=True, kw_only=True)
class Source(ABC, Generic[ConfigT]):
    """A named group of resources, each with source-type specific extraction config.

    ``name`` is the dlt source and schema name and part of every resource's state key: it must
    never change once a pipeline has loaded. ``parallel`` extracts resources in dlt worker threads
    inside one process, which is safe for one pipeline name.

    A source type implements ``resource`` and lists the ``windows`` it can push upstream. It can
    extend ``validate`` for its own definition rules. Write hints, cursor preparation, and
    parallelism are applied here, once, for every source type.
    """

    windows: ClassVar[tuple[type[Window], ...]]

    name: str
    resources: Mapping[Resource, ConfigT]
    parallel: bool = False

    def __post_init__(self) -> None:
        require_identifier(self.name, "Source name")
        for resource, config in self.resources.items():
            self.validate(resource, config)

    @abstractmethod
    def resource(self, resource: Resource, config: ConfigT, window: Window | None) -> DltResource:
        """The dlt resource named ``resource.name``, with its cursor or window pushed into the
        upstream request so only the needed rows are fetched."""

    def state_key(self, resource: Resource) -> str:
        """The dlt resource whose state holds this resource's cursor."""
        return resource.name

    def validate(self, resource: Resource, config: ConfigT) -> None:
        if resource.backfill is not None and resource.backfill.window not in self.windows:
            raise DefinitionError(
                f"{type(self).__name__} cannot push {resource.backfill.window.__name__} upstream "
                f"for {resource.name!r}"
            )

    def build(self, resources: Iterable[Resource], window: Window | None) -> DltSource:
        prepared = [self._prepared(resource, window) for resource in resources]
        source = dlt.source(name=self.name)(lambda: prepared)()
        return source.parallelize() if self.parallel else source

    def _prepared(self, resource: Resource, window: Window | None) -> DltResource:
        dlt_resource = self.resource(resource, self.resources[resource], window)
        dlt_resource.apply_hints(**resource.write.hints())
        if resource.cursor is not None:
            resource.cursor.prepare(dlt_resource)
        return dlt_resource

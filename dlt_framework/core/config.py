"""How configuration fills settings and chooses implementations.

Settings are dlt configspecs: fields declared ``= REQUIRED`` must be stated in config (or the
environment), and ``resolve`` fills them, looking under the given sections first. Monitoring
sinks and log formats are chosen from a ``Registry`` by a configured key;
adding one is a decorated class in any imported module, and core does not change.
"""

from collections.abc import Callable
from typing import Any, Generic, TypeVar

from dlt.common.configuration import resolve_configuration
from dlt.common.configuration.exceptions import ConfigurationException
from dlt.common.configuration.specs import BaseConfiguration

from dlt_framework.core.errors import ConfigurationError, DefinitionError

T = TypeVar("T")
SpecT = TypeVar("SpecT", bound=BaseConfiguration)

REQUIRED: Any = None
"""A configspec field default: dlt requires a non-optional field that defaults to ``None``."""


def resolve(spec: SpecT, *sections: str) -> SpecT:
    """``spec`` filled from config, looking under ``sections`` first, then above them."""
    try:
        return resolve_configuration(spec, sections=sections)
    except ConfigurationException as error:
        raise ConfigurationError(str(error)) from None


class Registry(Generic[T]):
    def __init__(self, kind: str) -> None:
        self.kind = kind
        self._entries: dict[str, type[T]] = {}

    def register(self, *keys: str) -> Callable[[type[T]], type[T]]:
        """``@registry.register("s3", "gs")`` makes the class available under those keys."""

        def add(entry: type[T]) -> type[T]:
            for key in keys:
                if key in self._entries:
                    raise DefinitionError(f"{self.kind} {key!r} is registered twice")
                self._entries[key] = entry
            return entry

        return add

    def get(self, key: str) -> type[T]:
        try:
            return self._entries[key]
        except KeyError:
            known = ", ".join(sorted(self._entries)) or "none"
            raise ConfigurationError(f"No {self.kind} {key!r}; known: {known}") from None

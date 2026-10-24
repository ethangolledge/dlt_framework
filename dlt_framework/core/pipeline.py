"""Pipeline definitions and the registry that makes their names unique."""

import importlib
import pkgutil
from dataclasses import dataclass, field
from typing import Any, ClassVar

from dlt.common.schema.typing import TSchemaContractDict

from dlt_framework.core.errors import DefinitionError, require_identifier
from dlt_framework.core.notify import Notifier
from dlt_framework.core.retry import Retry
from dlt_framework.core.source import Source
from dlt_framework.core.target import Target

FREEZE_DATA_TYPES: TSchemaContractDict = {
    "tables": "evolve",
    "columns": "evolve",
    "data_type": "freeze",
}
"""New tables and columns load; a column changing type fails the run."""


@dataclass(frozen=True, kw_only=True)
class Pipeline:
    """A named, independently runnable pipeline. Creating one registers it.

    ``name`` is the dlt pipeline name: the key for this pipeline's state at the destination and
    its local working directory. Two processes must never run the same name at once, and the name
    must never change after the first production load. Backfills run as ``<name>__backfill``, so
    names cannot contain ``__``.
    """

    name: str
    source: Source[Any]
    target: Target
    schema_contract: TSchemaContractDict
    retry: Retry = field(default_factory=Retry)
    notifiers: tuple[Notifier, ...] = ()

    registry: ClassVar[dict[str, "Pipeline"]] = {}

    def __post_init__(self) -> None:
        require_identifier(self.name, "Pipeline name")
        if "__" in self.name:
            raise DefinitionError(f"Pipeline name {self.name!r} cannot contain '__'")
        if self.name in self.registry:
            raise DefinitionError(f"Pipeline {self.name!r} is defined twice")
        self.registry[self.name] = self

    @classmethod
    def get(cls, name: str) -> "Pipeline":
        try:
            return cls.registry[name]
        except KeyError:
            known = ", ".join(sorted(cls.registry)) or "none"
            raise DefinitionError(f"No pipeline named {name!r}; defined: {known}") from None

    @classmethod
    def discover(cls, package: str) -> None:
        """Import every module below ``package`` so the pipelines they define register.

        Imported by name, not statically, so core never depends on any source package.
        """
        root = importlib.import_module(package)
        for module in pkgutil.walk_packages(root.__path__, f"{package}."):
            importlib.import_module(module.name)

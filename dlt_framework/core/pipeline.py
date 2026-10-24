"""Pipeline definitions and the registry that makes their names unique."""

import importlib
import importlib.metadata
import importlib.util
import pkgutil
import re
from dataclasses import dataclass
from typing import Any, ClassVar

from dlt.common.schema.typing import TSchemaContractDict

from dlt_framework.core.errors import ConfigurationError, DefinitionError, require_identifier
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
    """A named, independently runnable pipeline: what lands (``source``) and where (``target``).
    How it runs (retries, monitoring, alerts) is stated per environment in config. Creating one
    registers it.

    ``name`` is the dlt pipeline name: the key for this pipeline's state at the destination and
    its local working directory. Two processes must never run the same name at once, and the name
    must never change after the first production load. Backfills run as ``<name>__backfill``, so
    names cannot contain ``__``. Each destination table belongs to one pipeline.
    """

    name: str
    source: Source[Any]
    target: Target
    schema_contract: TSchemaContractDict

    registry: ClassVar[dict[str, "Pipeline"]] = {}
    unavailable: ClassVar[dict[str, str]] = {}
    """Modules not loaded because an optional dependency is not installed: module -> import."""

    def __post_init__(self) -> None:
        require_identifier(self.name, "Pipeline name")
        if "__" in self.name:
            raise DefinitionError(f"Pipeline name {self.name!r} cannot contain '__'")
        if self.name in self.registry:
            raise DefinitionError(f"Pipeline {self.name!r} is defined twice")
        for other in self.registry.values():
            if shared := self.tables & other.tables:
                tables = sorted(f"{dataset}.{table}" for _, dataset, table in shared)
                raise DefinitionError(
                    f"Pipelines {other.name!r} and {self.name!r} both write {tables}: each table "
                    "has one owner, or their loads and cursors overwrite each other"
                )
        self.registry[self.name] = self

    @property
    def missing(self) -> dict[str, str]:
        """Modules this pipeline needs that are not installed here, mapped to their extra."""
        needed = {**self.target.requires(), **self.source.requires()}
        return {module: extra for module, extra in needed.items() if not _installed(module)}

    @property
    def install(self) -> str:
        """The command installing what is missing, e.g. ``uv sync --extra sql``; empty if none."""
        extras = sorted(set(self.missing.values()))
        return "uv sync " + " ".join(f"--extra {extra}" for extra in extras) if extras else ""

    def require_installed(self) -> None:
        if self.missing:
            raise ConfigurationError(
                f"{self.name} needs {sorted(self.missing)}, which this environment does not have: "
                f"{self.install}"
            )

    @property
    def tables(self) -> set[tuple[str, str, str]]:
        """``(destination, dataset, table)`` for every resource."""
        target = self.target
        return {(target.destination, target.dataset, r.name) for r in self.source.resources}

    @classmethod
    def get(cls, name: str) -> "Pipeline":
        try:
            return cls.registry[name]
        except KeyError:
            known = ", ".join(sorted(cls.registry)) or "none"
            message = f"No pipeline named {name!r}; defined: {known}"
            if cls.unavailable:
                message += f". Not loaded, missing optional dependencies: {cls.not_loaded()}"
            raise DefinitionError(message) from None

    @classmethod
    def not_loaded(cls) -> str:
        """``module (needs 'x': uv sync --extra e)`` for every module a dependency kept out."""
        return "; ".join(
            f"{module} (needs {name!r}: {install_for(name)})"
            for module, name in cls.unavailable.items()
        )

    @classmethod
    def discover(cls, package: str) -> None:
        """Import every module below ``package`` so the pipelines they define register.

        Imported by name, not statically, so core never depends on any source package. A module
        whose optional dependency is not installed (a source type's extra) is recorded in
        ``unavailable`` and reported by ``pipeline check`` and by ``get``. A missing module of the
        framework or of ``package`` itself is a bug and raises.
        """
        root = importlib.import_module(package)
        # A package that fails to import is yielded first and imported below, which reports it.
        for module in pkgutil.walk_packages(root.__path__, f"{package}.", onerror=lambda _: None):
            try:
                importlib.import_module(module.name)
            except ModuleNotFoundError as error:
                own = {package.split(".")[0], __name__.split(".")[0]}
                if error.name is None or error.name.split(".")[0] in own:
                    raise
                cls.unavailable[module.name] = error.name


def _installed(module: str) -> bool:
    return importlib.util.find_spec(module) is not None


_REQUIREMENT = re.compile(r"^([A-Za-z0-9_.-]+).*extra == ['\"]([^'\"]+)['\"]")


def install_for(module: str) -> str:
    """The extra whose requirements include ``module``'s distribution, from this package's
    metadata, as a command; a plain hint when no extra provides it."""
    wanted = re.sub(r"[-_.]+", "-", module).lower()
    for requirement in importlib.metadata.metadata("dlt-framework").get_all("Requires-Dist") or []:
        if (found := _REQUIREMENT.match(requirement)) and found[1].lower() == wanted:
            return f"uv sync --extra {found[2]}"
    return f"install {module!r}"

import importlib
import inspect
from pathlib import Path, PurePosixPath

from dlt.extract.source import DltSource

from pipeline.core.errors import SourceDefinitionError
from pipeline.core.models import SourceDefinition


DEFAULT_SOURCE_ROOT = Path(__file__).parents[1] / "sources"
DEFAULT_SOURCE_PACKAGE = "pipeline.sources"


def discover_source(
    selector: str,
    *,
    source_root: Path = DEFAULT_SOURCE_ROOT,
    source_package: str = DEFAULT_SOURCE_PACKAGE,
) -> SourceDefinition:
    root = source_root.resolve()
    relative_path = _resolve_selector(selector, root)
    module_parts = (*relative_path.parts[:-1], relative_path.stem)
    module_name = ".".join((source_package, *module_parts))
    try:
        module = importlib.import_module(module_name)
    except Exception as error:
        raise SourceDefinitionError(f"Could not import source file {selector}: {error}") from error
    factory = getattr(module, "source", None)

    if not callable(factory):
        raise SourceDefinitionError(f"{selector} must expose a callable named source")
    try:
        inspect.signature(factory).bind()
    except (TypeError, ValueError) as error:
        raise SourceDefinitionError(
            f"{selector} source() must be callable without arguments"
        ) from error

    return SourceDefinition(
        relative_path=relative_path,
        module_name=module_name,
        factory=factory,
    )


def instantiate_source(definition: SourceDefinition) -> DltSource:
    try:
        source = definition.factory()
    except Exception as error:
        raise SourceDefinitionError(
            f"Could not create source from {definition.relative_path}: {error}"
        ) from error

    if not isinstance(source, DltSource):
        raise SourceDefinitionError(
            f"{definition.relative_path} source() must return a dlt DltSource"
        )
    return source.clone(with_name=definition.relative_path.stem)


def _resolve_selector(selector: str, root: Path) -> PurePosixPath:
    if not selector or selector != selector.strip():
        raise SourceDefinitionError("Source names cannot be empty or contain surrounding whitespace")
    if "\\" in selector:
        raise SourceDefinitionError(f"Qualified source names must use forward slashes: {selector}")

    relative_path = PurePosixPath(selector)
    if relative_path.is_absolute() or any(part in {".", ".."} for part in relative_path.parts):
        raise SourceDefinitionError(
            f"Source names must be relative and cannot traverse directories: {selector}"
        )
    if relative_path.suffix and relative_path.suffix != ".py":
        raise SourceDefinitionError(f"Source names may only use the .py suffix: {selector}")
    if not relative_path.suffix:
        relative_path = relative_path.with_suffix(".py")

    module_parts = (*relative_path.parts[:-1], relative_path.stem)
    if not all(part.isidentifier() for part in module_parts):
        raise SourceDefinitionError(
            f"Every source path component must be a valid Python identifier: {selector}"
        )

    if len(relative_path.parts) > 1:
        source_path = (root / Path(*relative_path.parts)).resolve()
        if not source_path.is_relative_to(root):
            raise SourceDefinitionError(f"Source file escapes the source directory: {selector}")
        if not source_path.is_file():
            raise SourceDefinitionError(f"Source file does not exist: {relative_path}")
        return relative_path

    matches = sorted(
        path.resolve()
        for path in root.rglob(relative_path.name)
        if path.is_file() and path.resolve().is_relative_to(root)
    )
    if not matches:
        raise SourceDefinitionError(
            f"No source named {relative_path.stem} was found below {root}"
        )
    if len(matches) > 1:
        candidates = ", ".join(str(path.relative_to(root)) for path in matches)
        raise SourceDefinitionError(
            f"Source name {relative_path.stem} is ambiguous; use one of: {candidates}"
        )
    return PurePosixPath(matches[0].relative_to(root).as_posix())

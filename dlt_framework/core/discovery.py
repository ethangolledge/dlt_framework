import importlib
import inspect
import warnings
from pathlib import Path, PurePosixPath

from dlt.extract.source import DltSource

from dlt_framework.core.backfill import BackfillWindow, LegacyBackfillWindow
from dlt_framework.core.errors import BackfillError, SourceDefinitionError
from dlt_framework.core.models import SourceContext, SourceContract, SourceDefinition

DEFAULT_SOURCE_ROOT = Path(__file__).parents[1] / "sources"
DEFAULT_SOURCE_PACKAGE = "dlt_framework.sources"


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
        raise SourceDefinitionError(
            f"Could not import source {selector!r}: {error}. Fix its imports or qualify "
            "the selector as TYPE/NAME"
        ) from error

    contract = getattr(module, "SOURCE", None)
    if contract is not None:
        if not isinstance(contract, SourceContract):
            raise SourceDefinitionError(
                f"{relative_path} SOURCE must be created with define_source(...)"
            )
        return SourceDefinition(
            relative_path=relative_path,
            module_name=module_name,
            factory=contract.factory,
            resources=dict(contract.resources),
        )

    factory = getattr(module, "source", None)
    if not callable(factory):
        raise SourceDefinitionError(f"{relative_path} must expose SOURCE = define_source(...)")
    warnings.warn(
        f"{relative_path} uses the deprecated source() convention; declare SOURCE with "
        "define_source(...) before the next major release",
        DeprecationWarning,
        stacklevel=2,
    )
    return SourceDefinition(
        relative_path=relative_path,
        module_name=module_name,
        factory=factory,
        legacy=True,
    )


def instantiate_source(
    definition: SourceDefinition,
    *,
    resource: str | None = None,
    window: BackfillWindow | None = None,
) -> DltSource:
    context = SourceContext(selected_resource=resource, window=window)
    try:
        if definition.legacy:
            source, bound = _instantiate_legacy(definition, window)
        else:
            source = definition.factory(context)
            bound = context.backfill_bound
    except BackfillError:
        raise
    except Exception as error:
        raise SourceDefinitionError(
            f"Could not create source from {definition.relative_path}: {error}"
        ) from error

    if not isinstance(source, DltSource):
        raise SourceDefinitionError(
            f"{definition.relative_path} source factory must return a dlt DltSource"
        )
    if window is not None and not bound:
        raise BackfillError(
            f"Resource {window.resource} did not bind the requested backfill window; "
            "use SourceContext.bounds_for(...) or a shared bounded-query helper"
        )
    return source.clone(with_name=definition.relative_path.stem)


def _instantiate_legacy(
    definition: SourceDefinition, window: BackfillWindow | None
) -> tuple[DltSource, bool]:
    if window is None:
        try:
            inspect.signature(definition.factory).bind()
        except (TypeError, ValueError) as error:
            raise SourceDefinitionError(
                f"Legacy {definition.relative_path} source() must be callable without arguments"
            ) from error
        return definition.factory(), True

    legacy_window = LegacyBackfillWindow(window.resource, window.start, window.end)
    try:
        inspect.signature(definition.factory).bind(backfill=legacy_window)
    except (TypeError, ValueError) as error:
        raise BackfillError(
            f"Legacy source {definition.relative_path.stem} does not accept backfill windows"
        ) from error
    return definition.factory(backfill=legacy_window), legacy_window.claimed


def _resolve_selector(selector: str, root: Path) -> PurePosixPath:
    if not selector or selector != selector.strip():
        raise SourceDefinitionError(
            "Source names cannot be empty or contain surrounding whitespace"
        )
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
    if not all(part.isidentifier() for part in (*relative_path.parts[:-1], relative_path.stem)):
        raise SourceDefinitionError(
            f"Every source path component must be a valid Python identifier: {selector}"
        )
    if len(relative_path.parts) > 1:
        source_path = (root / Path(*relative_path.parts)).resolve()
        if not source_path.is_relative_to(root) or not source_path.is_file():
            raise SourceDefinitionError(f"Source file does not exist: {relative_path}")
        return relative_path
    matches = sorted(
        path.resolve()
        for path in root.rglob(relative_path.name)
        if path.is_file() and path.resolve().is_relative_to(root)
    )
    if not matches:
        raise SourceDefinitionError(f"No source named {relative_path.stem} was found below {root}")
    if len(matches) > 1:
        candidates = ", ".join(str(path.relative_to(root)) for path in matches)
        raise SourceDefinitionError(
            f"Source name {relative_path.stem} is ambiguous; use one of: {candidates}"
        )
    return PurePosixPath(matches[0].relative_to(root).as_posix())

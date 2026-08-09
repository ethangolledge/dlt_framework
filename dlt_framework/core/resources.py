import warnings
from collections.abc import Mapping
from typing import Literal

from dlt.extract.source import DltSource

WriteDisposition = Literal["append", "replace", "merge"]


def set_write_dispositions(
    source: DltSource,
    default: WriteDisposition,
    overrides: Mapping[str, WriteDisposition] | None = None,
) -> DltSource:
    """Deprecated compatibility helper; declare hints on each dlt resource instead."""
    warnings.warn(
        "set_write_dispositions is deprecated; declare dlt resource hints at the source",
        DeprecationWarning,
        stacklevel=2,
    )
    resource_overrides = {} if overrides is None else dict(overrides)
    unknown = resource_overrides.keys() - source.resources.keys()
    if unknown:
        names = ", ".join(sorted(unknown))
        raise ValueError(f"Unknown resource write-disposition overrides: {names}")

    for name, resource in source.resources.items():
        resource.apply_hints(
            write_disposition=resource_overrides.get(name, default),
        )
    return source

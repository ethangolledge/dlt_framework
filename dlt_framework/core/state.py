"""Reading dlt pipeline state in the framework's terms."""

from typing import Any

import dlt

from dlt_framework.core.resource import Resource
from dlt_framework.core.source import Source


def stored_cursor(
    pipeline: dlt.Pipeline, source: Source[Any], resource: Resource
) -> dict[str, Any] | None:
    """The decoded incremental state of ``resource`` (``last_value``, ``initial_value``, ...),
    or ``None`` when it has no cursor or nothing is stored yet."""
    if resource.cursor is None:
        return None
    resources = pipeline.state.get("sources", {}).get(source.name, {}).get("resources", {})
    incremental = resources.get(source.state_key(resource), {}).get("incremental", {})
    stored: dict[str, Any] | None = incremental.get(resource.cursor.tracked_path)
    return stored

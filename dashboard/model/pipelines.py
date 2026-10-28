"""What a pipeline is, as its latest run declared it: its source and destination, and each
resource's write mode, cursor, and backfill. Runs say what happened; this says what it is."""

from dataclasses import dataclass
from typing import Any

import polars as pl

from dashboard.model.facts import View
from dashboard.units import edge
from dlt_framework.core.monitoring import ResourceRecord


@dataclass(frozen=True, slots=True)
class About:
    pipeline: str
    source_type: str | None  # RestApiSource, SqlSource, FilesystemSource, ScrapeSource...
    source: str | None
    destination: str | None
    dataset: str | None
    resources: list[dict[str, Any]]  # each as its latest run declared it

    def cursor(self, resource: str) -> str:
        """How a resource reads: ``cursor modification_date (time) from 2025-01-01``, or
        ``no cursor: all of it, each run``."""
        found = next((row for row in self.resources if row["resource"] == resource), None)
        if found is None or not found["cursor_path"]:
            return "no cursor: all of it, each run"
        kind = {"TimeCursor": "time", "ValueCursor": "value"}.get(found["cursor_kind"], "")
        start = f" from {edge(found['cursor_start'])}" if found["cursor_start"] else ""
        return f"cursor {found['cursor_path']} ({kind}){start}"


def about(view: View, pipeline: str) -> About:
    """``pipeline`` as its latest run recorded it; unknown parts are ``None``."""
    latest = (
        view.frames.pipelines.filter(pl.col("pipeline_id") == pipeline)
        .sort("at")
        .tail(1)
        .to_dicts()
    )
    row = latest[0] if latest else {}
    return About(
        pipeline=pipeline,
        source_type=row.get("source_type"),
        source=row.get("source"),
        destination=row.get("destination"),
        dataset=row.get("dataset"),
        resources=sorted(
            ResourceRecord.declared(view.frames.resources, pipeline),
            key=lambda resource: str(resource["resource"]),
        ),
    )

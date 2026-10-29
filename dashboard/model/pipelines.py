"""What a pipeline is, as its latest run declared it: its source and destination, and each
resource's write mode, cursor, and backfill. Runs say what happened; this says what it is."""

from dataclasses import dataclass
from datetime import datetime
from typing import Any

import polars as pl

from dashboard.model.facts import OK, View
from dlt_framework.core.monitoring import ResourceRecord


@dataclass(frozen=True, slots=True)
class About:
    pipeline: str
    source_type: str | None  # RestApiSource, SqlSource, FilesystemSource, ScrapeSource...
    source: str | None
    destination: str | None
    dataset: str | None
    resources: list[dict[str, Any]]  # each as its latest run declared it


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


def last_success(view: View) -> dict[str, datetime | None]:
    """Each pipeline's last successful incremental run (``None``: it never succeeded)."""
    return dict(
        view.history.filter(pl.col("mode") == "run")
        .group_by("pipeline_id")
        .agg(pl.col("ended_at").filter(pl.col("outcome").is_in(OK)).max())
        .iter_rows()
    )

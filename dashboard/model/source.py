"""Where the frames come from: the monitoring sink the framework writes to, by URI."""

from dlt_framework.core.config import resolve
from dlt_framework.core.errors import ConfigurationError
from dlt_framework.core.monitoring import Frames, Sink
from dlt_framework.core.monitoring.health import Signal, health
from dlt_framework.core.settings import HealthSettings, MonitoringSettings, RunSettings


def monitoring_uri(given: str | None = None) -> str:
    """``given``, or the environment's ``monitoring.uri`` (``MONITORING__URI``), as runs use."""
    if given:
        return given
    return str(resolve(MonitoringSettings(), "monitoring").uri)


def read(uri: str) -> Frames:
    """Every frame, through the sink the URI names: a Parquet folder, a bucket, a database."""
    return Frames.read(Sink.resolve(uri))


def signals(frames: Frames, pipelines: list[str]) -> list[Signal] | None:
    """``pipeline health`` for each pipeline, with its own thresholds; ``None`` when this
    environment states no ``[health]`` thresholds to judge by."""
    found = []
    for pipeline in pipelines:
        try:
            settings: HealthSettings = RunSettings.of(pipeline).health
        except ConfigurationError:
            return None
        found.extend(health(pipeline, settings, frames))
    return found

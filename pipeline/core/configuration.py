import os
from collections.abc import Mapping

from pipeline.core.errors import ConfigurationError
from pipeline.core.models import PipelineConfig, SourceDefinition


def load_pipeline_config(
    source: SourceDefinition,
    environ: Mapping[str, str] | None = None,
) -> PipelineConfig:
    values = os.environ if environ is None else environ

    return PipelineConfig(
        name=source.relative_path.stem,
        destination=_required_value(values, "DESTINATION"),
        source=source,
    )


def _required_value(environ: Mapping[str, str], name: str) -> str:
    value = environ.get(name, "").strip()
    if not value:
        raise ConfigurationError(f"Required environment variable {name} is missing or blank")
    return value

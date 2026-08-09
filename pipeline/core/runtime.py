from typing import Any

import dlt

from pipeline.core.discovery import instantiate_source
from pipeline.core.models import PipelineConfig


def run(config: PipelineConfig) -> Any:
    source = instantiate_source(config.source)

    pipeline = dlt.pipeline(
        pipeline_name=config.name,
        dataset_name=config.name,
        destination=config.destination,
    )
    return pipeline.run(source)

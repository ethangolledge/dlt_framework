from pathlib import PurePosixPath

from pipeline.core import runtime
from pipeline.core.models import PipelineConfig, SourceDefinition


class FakeSource:
    pass


class FakePipeline:
    def __init__(self) -> None:
        self.run_args = None
        self.run_kwargs = None

    def run(self, *args, **kwargs):
        self.run_args = args
        self.run_kwargs = kwargs
        return "load info"


def test_run_uses_source_name_for_pipeline_and_schema(monkeypatch) -> None:
    source = FakeSource()
    fake_pipeline = FakePipeline()
    pipeline_calls = []
    definition = SourceDefinition(PurePosixPath("rest/products.py"), "products", lambda: None)
    monkeypatch.setattr(runtime, "instantiate_source", lambda value: source)
    monkeypatch.setattr(
        runtime.dlt,
        "pipeline",
        lambda **kwargs: pipeline_calls.append(kwargs) or fake_pipeline,
    )
    config = PipelineConfig(name="products", destination="postgres", source=definition)

    result = runtime.run(config)

    assert result == "load info"
    assert pipeline_calls == [
        {"pipeline_name": "products", "dataset_name": "products", "destination": "postgres"}
    ]
    assert fake_pipeline.run_args == (source,)
    assert fake_pipeline.run_kwargs == {}

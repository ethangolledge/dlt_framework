from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import PurePosixPath
from typing import Any


SourceFactory = Callable[[], Any]


@dataclass(frozen=True, slots=True)
class SourceDefinition:
    relative_path: PurePosixPath
    module_name: str
    factory: SourceFactory = field(repr=False, compare=False)


@dataclass(frozen=True, slots=True)
class PipelineConfig:
    name: str
    destination: str
    source: SourceDefinition

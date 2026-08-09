from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from pathlib import PurePosixPath
from typing import TYPE_CHECKING, Any, Literal

if TYPE_CHECKING:
    from dlt.extract.source import DltSource

    from dlt_framework.core.backfill import BackfillBounds, BackfillPlan, BackfillWindow


EmptyPolicy = Literal["allow", "warn", "fail"]
SourceFactory = Callable[..., "DltSource"]


@dataclass(frozen=True, slots=True)
class ResourcePolicy:
    """Operational behavior that cannot be inferred from dlt resource hints."""

    empty: EmptyPolicy
    backfill: bool = False


@dataclass(frozen=True, slots=True)
class SourceContract:
    factory: SourceFactory = field(repr=False, compare=False)
    resources: Mapping[str, ResourcePolicy]


@dataclass(slots=True)
class SourceContext:
    """Runtime values available while a client source is being constructed."""

    selected_resource: str | None = None
    window: BackfillWindow | None = None
    _bound_resource: str | None = field(default=None, init=False, repr=False)

    def bounds_for(self, resource_name: str) -> BackfillBounds | None:
        """Return this run's bounds for ``resource_name`` and record the binding."""
        if self.window is None or self.window.resource != resource_name:
            return None
        if self._bound_resource not in {None, resource_name}:
            from dlt_framework.core.errors import BackfillError

            raise BackfillError(
                f"Backfill window for {self.window.resource} was already bound to "
                f"{self._bound_resource}"
            )
        self._bound_resource = resource_name
        return self.window.bounds

    @property
    def backfill_bound(self) -> bool:
        return self.window is None or self._bound_resource == self.window.resource


@dataclass(frozen=True, slots=True)
class SourceDefinition:
    relative_path: PurePosixPath
    module_name: str
    factory: SourceFactory = field(repr=False, compare=False)
    resources: Mapping[str, ResourcePolicy] = field(default_factory=dict)
    legacy: bool = False


@dataclass(frozen=True, slots=True)
class RetryConfig:
    attempts: int = 5
    minimum_wait: float = 4.0
    maximum_wait: float = 60.0


@dataclass(frozen=True, slots=True)
class PipelineConfig:
    pipeline_name: str
    dataset_name: str
    destination: str
    source: SourceDefinition
    resource: str | None = None
    backfill: BackfillPlan | None = None
    restart_backfill: bool = False
    max_backfill_chunks: int = 10_000
    retry: RetryConfig = RetryConfig()
    schema_contract: Mapping[str, Any] = field(
        default_factory=lambda: {
            "tables": "evolve",
            "columns": "evolve",
            "data_type": "freeze",
        }
    )

    @property
    def name(self) -> str:
        """Compatibility alias for the pre-0.2 configuration model."""
        return self.pipeline_name


@dataclass(frozen=True, slots=True)
class RunSummary:
    pipeline_name: str
    resource: str | None
    chunks_total: int
    chunks_loaded: int
    resumed_from: str | None = None
    completed: bool = True
    last_load_info: Any = field(default=None, repr=False, compare=False)

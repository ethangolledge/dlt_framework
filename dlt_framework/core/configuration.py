import os
import re
from collections.abc import Mapping
from pathlib import Path
from urllib.parse import urlparse

from dlt_framework.core.backfill import BackfillPlan
from dlt_framework.core.errors import ConfigurationError
from dlt_framework.core.models import PipelineConfig, RetryConfig, SourceDefinition

_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def load_pipeline_config(
    source: SourceDefinition,
    environ: Mapping[str, str] | None = None,
    *,
    resource: str | None = None,
    backfill: BackfillPlan | None = None,
    restart_backfill: bool = False,
) -> PipelineConfig:
    values = os.environ if environ is None else environ
    if resource is not None and (not resource or resource != resource.strip()):
        raise ConfigurationError("--resource cannot be empty or contain surrounding whitespace")
    destination = _required_value(values, "DESTINATION")
    production = values.get("DLT_FRAMEWORK_ENV", "development").strip().lower() == "production"
    default_name = source.relative_path.stem
    pipeline_name = _identity(values, "PIPELINE_NAME", default_name, production)
    dataset_name = _identity(values, "DATASET_NAME", default_name, production)
    retry = RetryConfig(
        attempts=_positive_int(values, "PIPELINE_RETRY_ATTEMPTS", 5),
        minimum_wait=_nonnegative_float(values, "PIPELINE_RETRY_MIN_WAIT", 4.0),
        maximum_wait=_nonnegative_float(values, "PIPELINE_RETRY_MAX_WAIT", 60.0),
    )
    if retry.minimum_wait > retry.maximum_wait:
        raise ConfigurationError("PIPELINE_RETRY_MIN_WAIT cannot exceed PIPELINE_RETRY_MAX_WAIT")
    max_chunks = _positive_int(values, "MAX_BACKFILL_CHUNKS", 10_000)
    if restart_backfill and backfill is None:
        raise ConfigurationError("--restart is valid only for the backfill command")
    _validate_duckdb_identity(destination, dataset_name, values)
    return PipelineConfig(
        pipeline_name=pipeline_name,
        dataset_name=dataset_name,
        destination=destination,
        source=source,
        resource=resource,
        backfill=backfill,
        restart_backfill=restart_backfill,
        max_backfill_chunks=max_chunks,
        retry=retry,
    )


def _identity(values: Mapping[str, str], name: str, default: str, production: bool) -> str:
    raw = values.get(name, "").strip()
    if not raw:
        if production:
            raise ConfigurationError(
                f"{name} is required when DLT_FRAMEWORK_ENV=production; use a stable "
                "identifier so destination state can be restored"
            )
        raw = default
    if not _IDENTIFIER.fullmatch(raw):
        raise ConfigurationError(
            f"{name}={raw!r} is invalid; use letters, numbers, and underscores and do "
            "not start with a number"
        )
    return raw


def _validate_duckdb_identity(
    destination: str, dataset_name: str, values: Mapping[str, str]
) -> None:
    if destination.lower() != "duckdb":
        return
    credentials = values.get("DESTINATION__DUCKDB__CREDENTIALS", "").strip()
    if not credentials or credentials == ":memory:":
        return
    parsed = urlparse(credentials)
    path = parsed.path if parsed.scheme == "duckdb" else credentials
    catalog = Path(path).stem
    if catalog.casefold() == dataset_name.casefold():
        raise ConfigurationError(
            f"DuckDB file/catalog {catalog!r} cannot equal DATASET_NAME {dataset_name!r}; "
            "use distinct names to avoid ambiguous catalog/schema references"
        )
    _validate_duckdb_access(Path(path))


def _validate_duckdb_access(database: Path) -> None:
    """Fail before dlt retries a local filesystem permission problem."""
    target = database if database.exists() else database.parent
    if not target.exists():
        raise ConfigurationError(
            f"DuckDB parent directory {database.parent} does not exist; create and mount it "
            "before running the pipeline"
        )
    if os.access(target, os.W_OK):
        return
    raise ConfigurationError(
        f"DuckDB {'file' if database.exists() else 'directory'} {target} is not writable by "
        f"the current process (uid={os.geteuid()}); fix its ownership or permissions. For a "
        'Docker bind mount, run with --user "$(id -u):$(id -g)" and mount a host-owned '
        "pipeline-state directory at /var/lib/dlt"
    )


def _positive_int(values: Mapping[str, str], name: str, default: int) -> int:
    raw = values.get(name, str(default)).strip()
    try:
        value = int(raw)
    except ValueError as error:
        raise ConfigurationError(f"{name} must be a positive integer, got {raw!r}") from error
    if value < 1:
        raise ConfigurationError(f"{name} must be at least 1, got {value}")
    return value


def _nonnegative_float(values: Mapping[str, str], name: str, default: float) -> float:
    raw = values.get(name, str(default)).strip()
    try:
        value = float(raw)
    except ValueError as error:
        raise ConfigurationError(f"{name} must be a number, got {raw!r}") from error
    if value < 0:
        raise ConfigurationError(f"{name} cannot be negative, got {value}")
    return value


def _required_value(values: Mapping[str, str], name: str) -> str:
    value = values.get(name, "").strip()
    if not value:
        raise ConfigurationError(
            f"Required environment variable {name} is missing or blank; set it in the "
            "container environment or --env-file"
        )
    return value

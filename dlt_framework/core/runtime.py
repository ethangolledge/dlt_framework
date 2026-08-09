import logging
from collections.abc import Callable, Mapping
from datetime import datetime
from typing import Any, TypeVar, cast

import dlt
from dlt.common.pipeline import ExtractInfo
from dlt.extract.source import DltSource
from dlt.pipeline.helpers import retry_load
from dlt.pipeline.pipeline import Pipeline
from tenacity import Retrying, retry_if_exception, stop_after_attempt, wait_exponential_jitter

from dlt_framework.core.backfill import BackfillWindow, parse_bound
from dlt_framework.core.discovery import instantiate_source
from dlt_framework.core.errors import (
    BackfillError,
    DataContractError,
    FrameworkError,
    SourceDefinitionError,
    TerminalRunError,
    TransientRunError,
)
from dlt_framework.core.models import PipelineConfig, ResourcePolicy, RunSummary

LOGGER = logging.getLogger(__name__)
_T = TypeVar("_T")
_STATE_KEY = "dlt_framework"
_BACKFILLS_KEY = "backfills"


def run(config: PipelineConfig) -> RunSummary:
    pipeline = dlt.pipeline(
        pipeline_name=config.pipeline_name,
        dataset_name=config.dataset_name,
        destination=config.destination,
    )
    _call_with_retry(config, "sync", pipeline.sync_destination)
    if pipeline.has_pending_data:
        LOGGER.warning(
            "Pending dlt packages found; processing them before new extraction",
            extra={"event": "pending_packages", "pipeline": config.pipeline_name},
        )
        _call_with_retry(config, "pending", lambda: pipeline.run([]))

    if config.backfill is None:
        load_info = _execute_source(pipeline, config)
        return RunSummary(
            pipeline_name=config.pipeline_name,
            resource=config.resource,
            chunks_total=1,
            chunks_loaded=1,
            last_load_info=load_info,
        )

    return _run_backfill(pipeline, config)


def validate(config: PipelineConfig) -> DltSource:
    """Validate a source and its selected resource without extracting data."""
    if config.backfill is None:
        return _build_selected_source(config)
    assert config.resource is not None
    first_window = next(
        config.backfill.windows(
            config.resource,
            max_chunks=config.max_backfill_chunks,
        )
    )
    source = _build_selected_source(config, first_window)
    _validate_backfill_resource(source, config.resource)
    config.backfill.count(max_chunks=config.max_backfill_chunks)
    return source


def _run_backfill(pipeline: Pipeline, config: PipelineConfig) -> RunSummary:
    if config.resource is None or config.backfill is None:
        raise BackfillError("A backfill requires one selected resource and a bounded plan")
    resource_name = config.resource
    total = config.backfill.count(max_chunks=config.max_backfill_chunks)
    fingerprint = config.backfill.fingerprint(resource_name)
    checkpoint = _checkpoint_for(pipeline, resource_name)

    if config.restart_backfill and checkpoint is not None:
        _persist_checkpoint(pipeline, config, resource_name, None)
        checkpoint = None

    start_at = config.backfill.start
    resumed_from = None
    if checkpoint is not None:
        if checkpoint.get("fingerprint") != fingerprint:
            if checkpoint.get("status") != "completed":
                raise BackfillError(
                    f"Resource {resource_name} has a different unfinished backfill; "
                    "resume its original bounds or pass --restart to replay this plan"
                )
        elif checkpoint.get("status") == "completed":
            LOGGER.info(
                "Backfill plan is already complete",
                extra={"event": "backfill_already_complete", "resource": resource_name},
            )
            return RunSummary(
                pipeline_name=config.pipeline_name,
                resource=resource_name,
                chunks_total=total,
                chunks_loaded=0,
                resumed_from=checkpoint.get("next_from"),
            )
        else:
            start_at = parse_bound(checkpoint["next_from"], "stored checkpoint")
            resumed_from = checkpoint["next_from"]

    loaded = 0
    last_load_info = None
    for index, window in enumerate(
        config.backfill.windows(
            resource_name,
            start_at=start_at,
            max_chunks=config.max_backfill_chunks,
        ),
        start=1,
    ):
        LOGGER.info(
            "Running backfill chunk %s/%s for %s [%s, %s)",
            index,
            total,
            resource_name,
            _display_bound(window.start),
            _display_bound(window.end),
            extra={
                "event": "backfill_chunk_started",
                "resource": resource_name,
                "chunk": index,
                "chunk_total": total,
            },
        )
        last_load_info = _execute_source(pipeline, config, window)
        loaded += 1
        status = "completed" if window.end == config.backfill.end else "running"
        _persist_checkpoint(
            pipeline,
            config,
            resource_name,
            {
                "fingerprint": fingerprint,
                "from": _display_bound(config.backfill.start),
                "to": _display_bound(config.backfill.end),
                "chunksize": str(config.backfill.chunk_size),
                "next_from": _display_bound(window.end),
                "status": status,
            },
        )
    return RunSummary(
        pipeline_name=config.pipeline_name,
        resource=resource_name,
        chunks_total=total,
        chunks_loaded=loaded,
        resumed_from=resumed_from,
        last_load_info=last_load_info,
    )


def _execute_source(
    pipeline: Pipeline,
    config: PipelineConfig,
    window: BackfillWindow | None = None,
) -> Any:
    used_source: DltSource | None = None

    def extract_source() -> ExtractInfo:
        nonlocal used_source
        used_source = _build_selected_source(config, window)
        if window is not None:
            _validate_backfill_resource(used_source, window.resource)
        return pipeline.extract(
            used_source,
            schema_contract=cast(Any, config.schema_contract),
        )

    extract_info = _call_with_retry(config, "extract", extract_source)
    assert used_source is not None
    _check_empty_policy(
        pipeline,
        extract_info,
        used_source,
        config.resource,
        config.source.resources,
    )
    _call_with_retry(config, "normalize", pipeline.normalize)
    return _call_with_retry(config, "load", pipeline.load)


def _build_selected_source(
    config: PipelineConfig, window: BackfillWindow | None = None
) -> DltSource:
    if config.source.resources:
        if config.resource is not None and config.resource not in config.source.resources:
            available = ", ".join(sorted(config.source.resources))
            raise SourceDefinitionError(
                f"Source contract has no resource {config.resource!r}; available: {available}"
            )
        if window is not None:
            policy = config.source.resources[window.resource]
            if not policy.backfill:
                raise BackfillError(f"Resource {window.resource} does not declare backfill support")
    source = instantiate_source(
        config.source,
        resource=config.resource,
        window=window,
    )
    if config.source.resources:
        declared = set(config.source.resources)
        actual = set(source.resources)
        expected = {config.resource} if config.resource is not None else declared
        if expected != actual:
            missing = sorted(expected - actual)
            undeclared = sorted(actual - expected)
            raise SourceDefinitionError(
                f"Source contract/resources disagree; missing={missing or 'none'}, "
                f"undeclared={undeclared or 'none'}"
            )
    return _select_resource(source, config.resource)


def _select_resource(source: DltSource, resource_name: str | None) -> DltSource:
    if resource_name is None:
        return source
    if resource_name not in source.resources:
        available = ", ".join(sorted(source.resources)) or "none"
        raise SourceDefinitionError(
            f"Source {source.name} has no resource {resource_name!r}; available: {available}"
        )
    return source.with_resources(resource_name)


def _validate_backfill_resource(source: DltSource, resource_name: str) -> None:
    resource = source.resources[resource_name]
    schema = resource.compute_table_schema()
    if schema.get("write_disposition") != "merge":
        raise BackfillError(
            f"Backfill resource {resource_name} uses {schema.get('write_disposition')!r}; "
            "set write_disposition='merge' so retries are idempotent"
        )
    if not any(column.get("primary_key") for column in schema.get("columns", {}).values()):
        raise BackfillError(
            f"Backfill resource {resource_name} has no primary key; declare one so merge "
            "can identify repeated records"
        )


def _check_empty_policy(
    pipeline: Pipeline,
    extract_info: ExtractInfo,
    source: DltSource,
    selected_resource: str | None,
    policies: Mapping[str, ResourcePolicy],
) -> None:
    if not policies:
        return
    resource_counts: dict[str, int] = {}
    metrics = extract_info.asdict().get("resource_metrics", [])
    for metric in metrics:
        name = metric["resource_name"]
        resource_counts[name] = resource_counts.get(name, 0) + metric["items_count"]
    names = [selected_resource] if selected_resource else list(source.selected_resources)
    for name in names:
        policy = policies[name]
        if resource_counts.get(name, 0) > 0 or policy.empty == "allow":
            continue
        message = f"Resource {name} produced no root rows"
        if policy.empty == "warn":
            LOGGER.warning(message, extra={"event": "empty_resource", "resource": name})
        else:
            pipeline.drop_pending_packages()
            raise DataContractError(
                f"{message}; investigate upstream filters/credentials or set empty='allow' "
                "when an empty successful load is expected"
            )


def _checkpoint_for(pipeline: Pipeline, resource: str) -> dict[str, str] | None:
    state = cast(dict[str, Any], pipeline.state)
    return state.get(_STATE_KEY, {}).get(_BACKFILLS_KEY, {}).get(resource)


def _persist_checkpoint(
    pipeline: Pipeline,
    config: PipelineConfig,
    resource: str,
    checkpoint: dict[str, str] | None,
) -> None:
    with pipeline.managed_state(extract_state=True) as state:
        mutable_state = cast(dict[str, Any], state)
        framework_state = mutable_state.setdefault(_STATE_KEY, {})
        backfills = framework_state.setdefault(_BACKFILLS_KEY, {})
        if checkpoint is None:
            backfills.pop(resource, None)
        else:
            backfills[resource] = checkpoint
    _call_with_retry(config, "checkpoint_normalize", pipeline.normalize)
    _call_with_retry(config, "checkpoint_load", pipeline.load)


def _call_with_retry(
    config: PipelineConfig,
    operation: str,
    call: Callable[[], _T],
) -> _T:
    retry_predicate = retry_load(("sync", "extract", "load", "run"))
    attempt_number = 0
    try:
        for attempt in Retrying(
            stop=stop_after_attempt(config.retry.attempts),
            wait=wait_exponential_jitter(
                initial=max(config.retry.minimum_wait, 0.001),
                max=max(config.retry.maximum_wait, 0.001),
                jitter=min(1.0, config.retry.maximum_wait),
            ),
            retry=retry_if_exception(retry_predicate),
            reraise=True,
        ):
            attempt_number = attempt.retry_state.attempt_number
            with attempt:
                if attempt_number > 1:
                    LOGGER.warning(
                        "Retrying %s (attempt %s/%s)",
                        operation,
                        attempt_number,
                        config.retry.attempts,
                        extra={"event": "retry", "operation": operation},
                    )
                return call()
    except FrameworkError:
        raise
    except (KeyboardInterrupt, SystemExit):
        raise
    except BaseException as error:
        message = f"{operation} failed after {attempt_number or 1} attempt(s): {error}"
        if retry_predicate(error) and attempt_number >= config.retry.attempts:
            raise TransientRunError(message) from error
        raise TerminalRunError(message) from error
    raise RuntimeError("Retry loop finished without returning or raising")


def _display_bound(value: datetime) -> str:
    return value.isoformat().replace("+00:00", "Z")

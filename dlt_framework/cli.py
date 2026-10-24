"""Command line: ``pipeline run|backfill|list|check``."""

import argparse
import logging
import os
from collections import Counter
from collections.abc import Sequence

import dlt
import polars as pl

from dlt_framework.core import logs
from dlt_framework.core.errors import (
    ConfigurationError,
    RunError,
    error_fields,
    exit_code,
    root_cause,
)
from dlt_framework.core.monitoring import ResourceRecord, Sink
from dlt_framework.core.notify import summarise
from dlt_framework.core.pipeline import Pipeline
from dlt_framework.core.run import BackfillRun, IncrementalRun

LOGGER = logging.getLogger(__name__)
SOURCES_PACKAGE = "dlt_framework.sources"
_LIST_COLUMNS = ("resource", "write_mode", "cursor_path", "backfill_kind")


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        Pipeline.discover(args.package)
        if args.command == "list":
            _list()
        elif args.command == "check":
            _check(Pipeline.get(args.pipeline))
        elif args.command == "run":
            rows = IncrementalRun(Pipeline.get(args.pipeline), _sink(), args.allow_initial_load)()
            _log_success(args, rows)
        else:
            rows = BackfillRun(Pipeline.get(args.pipeline), _sink(), args.resource, args.resume)()
            _log_success(args, rows)
    except (Exception, KeyboardInterrupt) as error:
        _log_failure(args, error)
        return exit_code(error)
    return 0


def _sink() -> Sink:
    """The monitoring sink for this environment: ``monitoring.uri`` / ``MONITORING__URI``."""
    uri: str | None = dlt.secrets.get("monitoring.uri")
    if not uri:
        raise ConfigurationError(
            "monitoring.uri is not set; state where run monitoring goes, e.g. "
            "MONITORING__URI=file:///data/monitoring or a database URL"
        )
    return Sink.resolve(str(uri))


def _log_success(args: argparse.Namespace, rows: Counter[str]) -> None:
    LOGGER.info(
        "%s %s succeeded: %s",
        args.command,
        args.pipeline,
        summarise(rows),
        extra={"event": "run_succeeded", "rows": rows.total()},
    )


def _log_failure(args: argparse.Namespace, error: BaseException) -> None:
    """One line saying what failed and where, then the root cause's traceback.

    A framework error raised by the framework itself is its own explanation and has no traceback;
    anything else, including an unexpected bug (exit 1), keeps one.
    """
    target = f"{args.command} {getattr(args, 'pipeline', '')}".strip()
    if isinstance(error, KeyboardInterrupt):
        LOGGER.warning("%s interrupted", target, extra={"event": "run_interrupted"})
        return
    cause = root_cause(error)
    fields = error_fields(error)
    location = error.location if isinstance(error, RunError) else ""
    LOGGER.error(
        "%s failed%s: %s (%s, exit %s)",
        target,
        f" in {location}" if location else "",
        error,
        fields["error_type"],
        fields["exit_code"],
        exc_info=None if isinstance(cause, RunError) else (type(cause), cause, cause.__traceback__),
        extra={"event": "run_failed", **fields},
    )


def _list() -> None:
    rows = [
        {
            "pipeline": name,
            "source": type(definition.source).__name__,
            **{column: ResourceRecord.describe(resource)[column] for column in _LIST_COLUMNS},
        }
        for name, definition in sorted(Pipeline.registry.items())
        for resource in definition.source.resources
    ]
    schema = dict.fromkeys(("pipeline", "source", *_LIST_COLUMNS), pl.String)
    with pl.Config(tbl_rows=-1, tbl_hide_dataframe_shape=True, tbl_hide_column_data_types=True):
        print(pl.DataFrame(rows, schema=schema).fill_null("-"))


def _check(definition: Pipeline) -> None:
    """Build the run source and the first window of every backfill, without extracting."""
    source = definition.source
    source.build(tuple(source.resources), None)
    for resource in source.resources:
        if resource.backfill is not None:
            first = next(iter(resource.backfill.windows(resource.cursor, None)), None)
            source.build((resource,), first)
    LOGGER.info("%s builds: %s", definition.name, ", ".join(r.name for r in source.resources))


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="pipeline")
    parser.add_argument("--package", default=SOURCES_PACKAGE, help="where pipelines are defined")
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("list", help="list pipelines and their resources")
    check = commands.add_parser("check", help="build a pipeline's sources without extracting")
    check.add_argument("pipeline")
    run = commands.add_parser("run", help="load from the stored incremental cursors")
    run.add_argument("pipeline")
    run.add_argument(
        "--allow-initial-load",
        action="store_true",
        help="load from cursor start even though the tables already hold data",
    )
    backfill = commands.add_parser("backfill", help="load history as declared by each resource")
    backfill.add_argument("pipeline")
    backfill.add_argument("--resource", help="backfill only this resource")
    backfill.add_argument("--from", dest="resume", help="resume point logged by an earlier run")
    return parser


def entrypoint() -> None:
    logs.configure(
        os.environ.get("LOG_FORMAT", "text").lower(), os.environ.get("LOG_LEVEL", "INFO")
    )
    raise SystemExit(main())

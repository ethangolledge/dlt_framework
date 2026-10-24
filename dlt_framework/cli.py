"""Command line: ``pipeline list|check|run|backfill|status``.

Each command acts on one named pipeline (``status`` on one or all), so separate processes or
containers run different pipelines, such as a SQL database and an S3 bucket, at the same time.
"""

import argparse
import logging
import sys
from collections import Counter
from collections.abc import Sequence
from typing import Any

import polars as pl

from dlt_framework.core import logs
from dlt_framework.core.config import resolve
from dlt_framework.core.errors import (
    ConfigurationError,
    RunError,
    error_fields,
    exit_code,
    root_cause,
)
from dlt_framework.core.monitoring import (
    Coverage,
    LoadRecord,
    ResourceRecord,
    RunRecord,
    Sink,
    status,
)
from dlt_framework.core.monitoring.status import COLUMNS as STATUS_COLUMNS
from dlt_framework.core.notify import summarise
from dlt_framework.core.pipeline import Pipeline, install_for
from dlt_framework.core.run import BackfillRun, IncrementalRun
from dlt_framework.core.settings import LoggingSettings, RunSettings

LOGGER = logging.getLogger(__name__)
SOURCES_PACKAGE = "dlt_framework.sources"
_LIST_COLUMNS = ("resource", "write_mode", "cursor_path", "backfill_kind")


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        Pipeline.discover(args.package)
        args.command(args)
    except (Exception, KeyboardInterrupt) as error:
        _log_failure(args, error)
        return exit_code(error)
    return 0


def _list(args: argparse.Namespace) -> None:
    """Pipelines that can run here; ``check`` reports the rest and what they need."""
    rows = [
        {
            "pipeline": name,
            "source": type(definition.source).__name__,
            **{column: ResourceRecord.describe(resource)[column] for column in _LIST_COLUMNS},
        }
        for name, definition in sorted(Pipeline.registry.items())
        if not definition.missing
        for resource in definition.source.resources
    ]
    _print(rows, ("pipeline", "source", *_LIST_COLUMNS))


def _check(args: argparse.Namespace) -> None:
    """One pipeline: it is installed here, and its run and the first window of every backfill
    build without extracting. No pipeline: whether each one can run here, and what it needs."""
    if args.pipeline is None:
        _readiness()
        return
    definition = Pipeline.get(args.pipeline)
    definition.require_installed()
    source = definition.source
    source.build(tuple(source.resources), None)
    for resource in source.resources:
        if resource.backfill is not None:
            first = next(iter(resource.backfill.windows(resource.cursor)), None)
            source.build((resource,), first)
    LOGGER.info("%s builds: %s", definition.name, ", ".join(r.name for r in source.resources))


def _readiness() -> None:
    rows = [
        {
            "pipeline": name,
            "ready": "no" if definition.missing else "yes",
            "install": definition.install,
        }
        for name, definition in sorted(Pipeline.registry.items())
    ] + [
        {"pipeline": module, "ready": "not loaded", "install": install_for(name)}
        for module, name in sorted(Pipeline.unavailable.items())
    ]
    _print(rows, ("pipeline", "ready", "install"))


def _run(args: argparse.Namespace) -> None:
    definition = Pipeline.get(args.pipeline)
    _log_success(args, IncrementalRun(definition, args.resource, args.allow_initial_load)())


def _backfill(args: argparse.Namespace) -> None:
    definition = Pipeline.get(args.pipeline)
    run = BackfillRun(definition, args.resource, _ranges(definition, args))
    _log_success(args, run())


def _ranges(
    definition: Pipeline, args: argparse.Namespace
) -> dict[str, list[tuple[Any, Any]]] | None:
    """The edges each resource is narrowed to: ``--from``/``--to``, or the gaps in monitoring."""
    if args.missing:
        if args.start or args.end:
            raise ConfigurationError("--missing finds its own windows; drop --from and --to")
        loads = _sink(definition).read(LoadRecord)
        gaps = {
            resource.name: Coverage.of(
                loads,
                ResourceRecord.key(definition.name, resource.name),
                resource.backfill,
                resource.cursor,
            ).gaps
            for resource in definition.source.resources
            if resource.backfill is not None
        }
        LOGGER.info("Missing backfill ranges: %s", gaps, extra={"pipeline": definition.name})
        return gaps
    if args.start is None and args.end is None:
        return None
    if len(args.resource or ()) != 1:
        raise ConfigurationError(
            "--from and --to need one --resource: edges are typed per resource"
        )
    (name,) = args.resource
    backfill = next(
        (r.backfill for r in definition.source.resources if r.name == name and r.backfill), None
    )
    if backfill is None:
        raise ConfigurationError(f"{definition.name} declares no backfill for {name!r}")
    edges = [None if text is None else backfill.edge(text) for text in (args.start, args.end)]
    return {name: [(edges[0], edges[1])]}


def _status(args: argparse.Namespace) -> None:
    names = [args.pipeline] if args.pipeline else sorted(Pipeline.registry)
    frames: dict[str, tuple[pl.DataFrame, pl.DataFrame]] = {}
    rows = []
    for name in names:
        definition = Pipeline.get(name)
        uri = RunSettings.of(name).monitoring.uri
        if uri not in frames:
            sink = Sink.resolve(uri)
            frames[uri] = (sink.read(RunRecord), sink.read(LoadRecord))
        rows.extend(status(definition, *frames[uri]))
    _print(rows, STATUS_COLUMNS)


def _sink(definition: Pipeline) -> Sink:
    return Sink.resolve(RunSettings.of(definition.name).monitoring.uri)


def _print(rows: list[dict[str, Any]], columns: Sequence[str]) -> None:
    schema = dict.fromkeys(columns, pl.String)
    with pl.Config(
        tbl_rows=-1,
        tbl_cols=-1,
        tbl_width_chars=1000,
        fmt_str_lengths=200,
        tbl_hide_dataframe_shape=True,
        tbl_hide_column_data_types=True,
    ):
        print(pl.DataFrame(rows, schema=schema).fill_null("-"))


def _log_success(args: argparse.Namespace, rows: Counter[str]) -> None:
    LOGGER.info(
        "%s %s succeeded: %s",
        args.name,
        args.pipeline,
        summarise(rows),
        extra={"event": "run_succeeded", "rows": rows.total()},
    )


def _log_failure(args: argparse.Namespace, error: BaseException) -> None:
    """One line saying what failed and where, then the root cause's traceback.

    A framework error raised by the framework itself is its own explanation and has no traceback;
    anything else, including an unexpected bug (exit 1), keeps one.
    """
    target = f"{args.name} {getattr(args, 'pipeline', None) or ''}".strip()
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


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="pipeline")
    parser.add_argument("--package", default=SOURCES_PACKAGE, help="where pipelines are defined")
    commands = parser.add_subparsers(required=True)

    def command(name: str, handler: Any, help: str) -> argparse.ArgumentParser:
        sub = commands.add_parser(name, help=help)
        sub.set_defaults(command=handler, name=name)
        return sub

    command("list", _list, "pipelines ready to run here, and their resources")
    check = command("check", _check, "readiness of every pipeline, or check one offline")
    check.add_argument("pipeline", nargs="?", help="build this one without extracting")
    status_ = command("status", _status, "last runs, cursors, and backfill coverage")
    status_.add_argument("pipeline", nargs="?", help="one pipeline; all when omitted")
    resource_help = "only this resource; repeat for several"

    run = command("run", _run, "load from the stored incremental cursors")
    run.add_argument("pipeline")
    run.add_argument("--resource", action="append", help=resource_help)
    run.add_argument(
        "--allow-initial-load",
        action="store_true",
        help="load from cursor start even though the tables already hold data",
    )

    backfill = command("backfill", _backfill, "load history as declared by each resource")
    backfill.add_argument("pipeline")
    backfill.add_argument("--resource", action="append", help=resource_help)
    backfill.add_argument("--from", dest="start", help="first window edge, e.g. a logged end")
    backfill.add_argument("--to", dest="end", help="stop before this edge")
    backfill.add_argument(
        "--missing", action="store_true", help="load only the ranges monitoring has no load for"
    )
    return parser


def entrypoint() -> None:
    try:
        logs.configure(resolve(LoggingSettings(), "logging"))
    except ConfigurationError as error:
        print(f"pipeline: {error}", file=sys.stderr)
        raise SystemExit(error.exit_code) from None
    raise SystemExit(main())

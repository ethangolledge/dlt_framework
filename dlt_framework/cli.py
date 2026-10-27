"""Command line: ``pipeline list|check|run|backfill|status``.

``run`` and ``backfill`` take one or more pipeline names and run them at the same time, each on
its own thread of this one process: pipelines are independent, and DuckDB lets only one process
write a file. Separate processes or containers can still run pipelines loading elsewhere.
"""

import argparse
import logging
import sys
from collections import Counter
from collections.abc import Callable, Iterator, Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor
from typing import Any

import polars as pl
from dlt.common.runtime import signals

from dlt_framework.core import logs
from dlt_framework.core.alerts import AlertSettings
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
    ReconcileRecord,
    Record,
    ResourceRecord,
    RunRecord,
    Sink,
    StepRecord,
    status,
)
from dlt_framework.core.monitoring.health import COLUMNS as HEALTH_COLUMNS
from dlt_framework.core.monitoring.health import health
from dlt_framework.core.monitoring.records import RECORDS
from dlt_framework.core.monitoring.status import COLUMNS as STATUS_COLUMNS
from dlt_framework.core.pipeline import Pipeline, install_for
from dlt_framework.core.reconcile import reconcile
from dlt_framework.core.run import BackfillRun, IncrementalRun, Run
from dlt_framework.core.settings import LoggingSettings, RunSettings

LOGGER = logging.getLogger(__name__)
SOURCES_PACKAGE = "dlt_framework.sources"
_LIST_COLUMNS = ("resource", "write_mode", "cursor_path", "backfill_kind")


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        Pipeline.discover(args.package)
        return args.command(args) or 0
    except (Exception, KeyboardInterrupt) as error:
        _log_failure(_target(args), error)
        return exit_code(error)


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


PROFILE_EVERY_MS = 10  # what --profile samples at
UNHEALTHY = 6  # pipeline health's exit code when it found something wrong
RECONCILE_COLUMNS = ("resource", "window", "rows", "distinct_keys", "source_rows")


def _run(args: argparse.Namespace) -> int:
    return _each(args, lambda d: IncrementalRun(d, args.resource, args.allow_initial_load))


def _backfill(args: argparse.Namespace) -> int:
    return _each(args, lambda d: BackfillRun(d, args.resource, _ranges(d, args)))


def _profiled(run: Run, args: argparse.Namespace) -> Run:
    if args.profile:
        run.settings.monitoring.profile_every_ms = PROFILE_EVERY_MS
    return run


def _each(args: argparse.Namespace, run_for: Callable[[Pipeline], Run]) -> int:
    """Each named pipeline at once, one thread each; the exit code of the first named to fail.

    Every run is built before any starts, so a configuration mistake stops them all before
    anything is extracted. One failing does not stop the others. Signals are taken on the main
    thread, as dlt needs for threads: the first SIGINT or SIGTERM stops each run at its next step
    with its package kept, a second stops the process.
    """
    names = list(dict.fromkeys(args.pipelines))
    stated = [f"--{option.replace('_', '-')}" for option in args.one_pipeline if vars(args)[option]]
    if len(names) > 1 and stated:
        raise ConfigurationError(f"{', '.join(stated)} apply to one pipeline: name only that one")
    runs = {name: _profiled(run_for(Pipeline.get(name)), args) for name in names}
    with signals.intercepted_signals(), ThreadPoolExecutor(len(runs)) as pool:
        futures = {name: pool.submit(_outcome, run) for name, run in runs.items()}
    code = 0
    for name, future in futures.items():
        outcome, run_id = future.result(), runs[name].run_id
        if isinstance(outcome, Counter):
            _log_success(f"{args.name} {name}", outcome, run_id)
        else:
            _log_failure(f"{args.name} {name}", outcome, run_id)
            code = code or exit_code(outcome)
    return code


def _outcome(run: Run) -> Counter[str] | BaseException:
    try:
        return run()
    except (Exception, KeyboardInterrupt) as error:
        return error


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
    rows = []
    for definition, _, (runs, loads) in _monitored(args.pipeline, RunRecord, LoadRecord):
        rows.extend(status(definition, runs, loads))
    _print(rows, STATUS_COLUMNS)


def _health(args: argparse.Namespace) -> int:
    """Each signal as a log line (an alert event) and a table row; exit 6 when any."""
    signals = []
    records = (RunRecord, LoadRecord, StepRecord, ReconcileRecord)
    for definition, settings, frames in _monitored(args.pipeline, *records):
        runs, loads, steps, reconciliations = frames
        signals.extend(health(definition, settings.health, runs, loads, steps, reconciliations))
    for signal in signals:
        LOGGER.log(
            logging.ERROR if signal.level == "error" else logging.WARNING,
            "%s%s %s: %s",
            signal.pipeline,
            f".{signal.resource}" if signal.resource else "",
            signal.event,
            signal.detail,
            extra={"event": signal.event, "pipeline": signal.pipeline, "resource": signal.resource},
        )
    _print([signal.row() for signal in signals], HEALTH_COLUMNS)
    return UNHEALTHY if signals else 0


def _reconcile(args: argparse.Namespace) -> int:
    """Count what the destination holds (and upstream, with ``--source``), record it in
    ``reconciliations``, and show the totals and any window that disagrees."""
    run = IncrementalRun(Pipeline.get(args.pipeline), args.resource)
    records = reconcile(run, args.source)
    run.sink.write(ReconcileRecord.name, ReconcileRecord.frame(records))
    rows = [
        {
            "resource": record.resource_id.split(".", 1)[1],
            "window": ReconcileRecord.window(
                record.window_kind, record.window_start, record.window_end
            ),
            "rows": record.destination_rows,
            "distinct_keys": record.distinct_keys,
            "source_rows": record.source_rows,
        }
        for record in records
        if record.window_kind == "all"
        or (record.source_rows is not None and record.source_rows != record.destination_rows)
    ]
    _print(rows, RECONCILE_COLUMNS)
    return 0


def _compact(args: argparse.Namespace) -> None:
    """Merge each monitoring frame's files, once per sink."""
    uris = {RunSettings.of(name).monitoring.uri for name in sorted(Pipeline.registry)}
    for uri in uris:
        sink = Sink.resolve(uri)
        for record in RECORDS:
            if merged := sink.compact(record):
                LOGGER.info("Compacted %s files of %s into one", merged, record.name)


def _monitored(
    pipeline: str | None, *records: type[Record]
) -> Iterator[tuple[Pipeline, RunSettings, list[pl.DataFrame]]]:
    """Each named pipeline (all when ``None``) with its settings and ``records`` frames, read
    once per monitoring sink however many pipelines share it."""
    read: dict[str, list[pl.DataFrame]] = {}
    for name in [pipeline] if pipeline else sorted(Pipeline.registry):
        settings = RunSettings.of(name)
        uri = settings.monitoring.uri
        if uri not in read:
            sink = Sink.resolve(uri)
            read[uri] = [sink.read(record) for record in records]
        yield Pipeline.get(name), settings, read[uri]


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


def summarise(rows: Mapping[str, int]) -> str:
    """``carts=208, users=208``, or ``no rows``."""
    return ", ".join(f"{table}={count}" for table, count in sorted(rows.items())) or "no rows"


def _target(args: argparse.Namespace) -> str:
    """``run shop erp``: the command and the pipelines it names."""
    names = getattr(args, "pipelines", None) or [getattr(args, "pipeline", None) or ""]
    return " ".join([args.name, *names]).strip()


def _log_success(target: str, rows: Counter[str], run_id: str | None) -> None:
    LOGGER.info(
        "%s succeeded: %s",
        target,
        summarise(rows),
        extra={"event": "run_succeeded", "rows": rows.total(), "run_id": run_id},
    )


def _log_failure(target: str, error: BaseException, run_id: str | None = None) -> None:
    """One line saying what failed, where, on what input, and in which run; then the root
    cause's traceback, with its notes.

    A framework error raised by the framework itself is its own explanation and has no traceback;
    anything else, including an unexpected bug (exit 1), keeps one. The traceback and every
    other field are also in the run's ``failed`` monitoring row.
    """
    if isinstance(error, KeyboardInterrupt):
        LOGGER.warning(
            "%s interrupted", target, extra={"event": "run_interrupted", "run_id": run_id}
        )
        return
    cause = root_cause(error)
    fields = error_fields(error)
    location = error.location if isinstance(error, RunError) else ""
    where = f"; raised at {fields['error_where']}" if fields["error_where"] else ""
    notes = f"; {fields['error_notes']}" if fields["error_notes"] else ""
    LOGGER.error(
        "%s failed%s: %s (%s, exit %s%s%s%s)",
        target,
        f" in {location}" if location else "",
        error,
        fields["error_type"],
        fields["exit_code"],
        where,
        notes,
        f"; run {run_id}" if run_id else "",
        exc_info=None if isinstance(cause, RunError) else (type(cause), cause, cause.__traceback__),
        extra={"event": "run_failed", "run_id": run_id, **_logged(fields)},
    )


def _logged(fields: Mapping[str, Any]) -> dict[str, Any]:
    """The error fields a log line carries: the traceback is logged as such, not as a field."""
    return {key: value for key, value in fields.items() if key != "error_traceback"}


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
    health_ = command("health", _health, "what is wrong now: failed, lost, late, slow, unusual")
    health_.add_argument("pipeline", nargs="?", help="one pipeline; all when omitted")
    reconcile_ = command("reconcile", _reconcile, "count what the destination holds, per window")
    reconcile_.add_argument("pipeline")
    reconcile_.add_argument("--resource", action="append", help="only this resource; repeat")
    reconcile_.add_argument("--source", action="store_true", help="count upstream too")
    command("compact", _compact, "merge each monitoring frame's files into one")
    resource_help = "only this resource; repeat for several"
    profile_help = "sample every thread's stack into the profiles frame (slower)"

    pipelines_help = "one or more; several run at once, in this process"
    run = command("run", _run, "load from the stored incremental cursors")
    run.set_defaults(one_pipeline=("resource", "allow_initial_load"))
    run.add_argument("pipelines", nargs="+", metavar="pipeline", help=pipelines_help)
    run.add_argument("--resource", action="append", help=resource_help)
    run.add_argument("--profile", action="store_true", help=profile_help)
    run.add_argument(
        "--allow-initial-load",
        action="store_true",
        help="load from cursor start even though the tables already hold data",
    )

    backfill = command("backfill", _backfill, "load history as declared by each resource")
    backfill.set_defaults(one_pipeline=("resource",))  # --from and --to need one --resource
    backfill.add_argument("pipelines", nargs="+", metavar="pipeline", help=pipelines_help)
    backfill.add_argument("--resource", action="append", help=resource_help)
    backfill.add_argument("--profile", action="store_true", help=profile_help)
    backfill.add_argument("--from", dest="start", help="first window edge, e.g. a logged end")
    backfill.add_argument("--to", dest="end", help="stop before this edge")
    backfill.add_argument(
        "--missing", action="store_true", help="load only the ranges monitoring has no load for"
    )
    return parser


def entrypoint() -> None:
    try:
        logs.configure(resolve(LoggingSettings(), "logging"), resolve(AlertSettings(), "alerts"))
    except ConfigurationError as error:
        print(f"pipeline: {error}", file=sys.stderr)
        raise SystemExit(error.exit_code) from None
    raise SystemExit(main())

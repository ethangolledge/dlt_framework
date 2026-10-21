"""Command line: ``dlt-framework run|backfill|list|check``."""

from __future__ import annotations

import argparse
import json
import logging
import os
from collections.abc import Sequence

from dlt_framework.core.errors import RunError
from dlt_framework.core.pipeline import Pipeline
from dlt_framework.core.run import BackfillRun, IncrementalRun

LOGGER = logging.getLogger(__name__)
SOURCES_PACKAGE = "dlt_framework.sources"
_STANDARD_RECORD_KEYS = set(vars(logging.makeLogRecord({}))) | {"message", "asctime"}


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        Pipeline.discover(args.package)
        if args.command == "list":
            _list()
        elif args.command == "check":
            _check(Pipeline.get(args.pipeline))
        elif args.command == "run":
            IncrementalRun(Pipeline.get(args.pipeline), args.allow_initial_load)()
        else:
            BackfillRun(Pipeline.get(args.pipeline), args.resource, args.resume)()
    except RunError as error:
        LOGGER.error("%s", error, extra={"event": "run_failed", "error_type": type(error).__name__})
        return error.exit_code
    except KeyboardInterrupt:
        LOGGER.warning("Interrupted", extra={"event": "run_interrupted"})
        return 130
    return 0


def _list() -> None:
    for name, definition in sorted(Pipeline.registry.items()):
        source = type(definition.source).__name__
        for resource in definition.source.resources:
            cursor = resource.cursor.tracked_path if resource.cursor else "-"
            backfill = type(resource.backfill).__name__ if resource.backfill else "-"
            write = type(resource.write).__name__
            print(
                f"{name}\t{source}\t{resource.name}\t{write}\tcursor={cursor}\tbackfill={backfill}"
            )


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
    parser = argparse.ArgumentParser(prog="dlt-framework")
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


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "time": self.formatTime(record),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            **{k: v for k, v in vars(record).items() if k not in _STANDARD_RECORD_KEYS},
        }
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str)


def entrypoint() -> None:
    handler = logging.StreamHandler()
    if os.environ.get("LOG_FORMAT", "text").lower() == "json":
        handler.setFormatter(JsonFormatter())
    logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"), handlers=[handler], force=True)
    raise SystemExit(main())

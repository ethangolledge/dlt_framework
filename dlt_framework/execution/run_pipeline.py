from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import warnings
from collections.abc import Sequence
from datetime import datetime, timezone

from dlt_framework.core.backfill import parse_backfill_plan
from dlt_framework.core.configuration import load_pipeline_config
from dlt_framework.core.discovery import discover_source, instantiate_source
from dlt_framework.core.errors import (
    FrameworkError,
    TerminalRunError,
    TransientRunError,
)
from dlt_framework.core.runtime import run, validate

LOGGER = logging.getLogger(__name__)
COMMANDS = {"run", "backfill", "list", "validate"}


def main(argv: Sequence[str] | None = None) -> int:
    raw = list(sys.argv[1:] if argv is None else argv)
    legacy = bool(raw and raw[0] not in COMMANDS and raw[0] not in {"-h", "--help"})
    if legacy:
        backfill_flags = ("--from", "--to", "--chunksize")
        has_backfill_args = any(flag in raw for flag in backfill_flags)
        command = "backfill" if has_backfill_args else "run"
        raw.insert(0, command)
        warnings.warn(
            "Calling dlt-framework without an explicit command is deprecated; use "
            f"'dlt-framework {command} ...'",
            DeprecationWarning,
            stacklevel=2,
        )
    args = _parser().parse_args(raw)
    try:
        if args.command == "list":
            _list_source(args.source)
            return 0
        definition = discover_source(args.source)
        backfill = None
        if args.command == "backfill":
            backfill = parse_backfill_plan(
                resource=args.resource,
                from_value=args.from_value,
                to_value=args.to_value,
                chunk_size=args.chunksize,
            )
        config = load_pipeline_config(
            definition,
            resource=getattr(args, "resource", None),
            backfill=backfill,
            restart_backfill=getattr(args, "restart", False),
        )
        if args.command == "validate":
            source = validate(config)
            LOGGER.info(
                "Validation passed for %s (%s)",
                args.source,
                ", ".join(source.selected_resources),
                extra={"event": "validation_succeeded", "source": args.source},
            )
            return 0
        summary = run(config)
    except FrameworkError as error:
        LOGGER.error(
            "Cannot run pipeline: %s",
            error,
            extra={"event": "configuration_failed", "error_type": type(error).__name__},
        )
        return 2
    except TerminalRunError as error:
        LOGGER.error(
            "Pipeline stopped on a terminal failure: %s",
            error,
            extra={"event": "pipeline_terminal_failure"},
        )
        return 3
    except TransientRunError as error:
        LOGGER.error(
            "Pipeline exhausted transient retries: %s",
            error,
            extra={"event": "pipeline_retry_exhausted"},
        )
        return 4
    except KeyboardInterrupt:
        LOGGER.warning("Pipeline interrupted", extra={"event": "pipeline_interrupted"})
        return 130
    except Exception as error:
        LOGGER.exception(
            "Pipeline execution failed unexpectedly: %s",
            error,
            extra={"event": "pipeline_unexpected_failure"},
        )
        return 1

    LOGGER.info(
        "Pipeline completed: chunks_loaded=%s chunks_total=%s",
        summary.chunks_loaded,
        summary.chunks_total,
        extra={
            "event": "pipeline_succeeded",
            "pipeline": summary.pipeline_name,
            "resource": summary.resource,
            "chunks_loaded": summary.chunks_loaded,
            "chunks_total": summary.chunks_total,
        },
    )
    if summary.last_load_info is not None:
        LOGGER.info("%s", summary.last_load_info)
    return 0


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="dlt-framework",
        description="Validate and run one client extraction source.",
    )
    commands = parser.add_subparsers(dest="command", required=True)
    run_parser = commands.add_parser("run", help="Run a full source or one resource")
    _source_argument(run_parser)
    run_parser.add_argument("--resource", help="Run only this resource")

    backfill = commands.add_parser("backfill", help="Run one resource in bounded chunks")
    _source_argument(backfill)
    backfill.add_argument("--resource", required=True)
    backfill.add_argument("--from", dest="from_value", required=True, metavar="ISO_DATE")
    backfill.add_argument("--to", dest="to_value", required=True, metavar="ISO_DATE")
    backfill.add_argument("--chunksize", required=True, metavar="ISO_DURATION")
    backfill.add_argument(
        "--restart",
        action="store_true",
        help="Discard the saved checkpoint and safely replay this plan",
    )

    list_parser = commands.add_parser("list", help="List source resource policies")
    _source_argument(list_parser)

    validate_parser = commands.add_parser("validate", help="Validate without extracting")
    _source_argument(validate_parser)
    validate_parser.add_argument("--resource", help="Validate only this resource")
    return parser


def _source_argument(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "source",
        metavar="SOURCE",
        help="Source selector, for example rest/dummyjson",
    )


def _list_source(selector: str) -> None:
    definition = discover_source(selector)
    source = instantiate_source(definition)
    for name, resource in source.resources.items():
        schema = resource.compute_table_schema()
        primary_keys = [
            column_name
            for column_name, column in schema.get("columns", {}).items()
            if column.get("primary_key")
        ]
        policy = definition.resources.get(name)
        print(
            "\t".join(
                (
                    name,
                    f"write={schema.get('write_disposition', 'append')}",
                    f"primary_key={','.join(primary_keys) or '-'}",
                    f"empty={policy.empty if policy else 'legacy'}",
                    f"backfill={'yes' if policy and policy.backfill else 'no'}",
                )
            )
        )


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "timestamp": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        for name in (
            "event",
            "pipeline",
            "source",
            "resource",
            "chunk",
            "chunk_total",
            "chunks_loaded",
            "chunks_total",
            "operation",
            "error_type",
        ):
            if hasattr(record, name):
                payload[name] = getattr(record, name)
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str)


def configure_logging() -> None:
    handler = logging.StreamHandler()
    if os.environ.get("LOG_FORMAT", "text").strip().lower() == "json":
        handler.setFormatter(JsonFormatter())
    else:
        handler.setFormatter(logging.Formatter("%(levelname)s %(message)s"))
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO").upper(),
        handlers=[handler],
        force=True,
    )


def entrypoint() -> None:
    configure_logging()
    raise SystemExit(main())


if __name__ == "__main__":
    entrypoint()

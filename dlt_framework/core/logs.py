"""One log format for the framework and dlt, as text or JSON lines."""

import json
import logging

from dlt_framework.core.errors import redact

_STANDARD_RECORD_KEYS = set(vars(logging.makeLogRecord({}))) | {"message", "asctime"}
_TEXT_FORMAT = "%(asctime)s %(levelname)-7s %(name)s: %(message)s"


class JsonFormatter(logging.Formatter):
    """One JSON object per line; ``extra=`` fields become top-level keys."""

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


class _Handler(logging.StreamHandler):
    """Every line the process writes, framework or dlt, with secrets in URLs masked.

    dlt reuses the first handler on its logger and replaces its formatter on every pipeline
    start. Ignoring that keeps one format for every line.
    """

    def format(self, record: logging.LogRecord) -> str:
        return redact(super().format(record))

    def setFormatter(self, fmt: logging.Formatter | None) -> None:
        if self.formatter is None:
            super().setFormatter(fmt)


def configure(log_format: str, level: str) -> None:
    handler = _Handler()
    handler.setFormatter(
        JsonFormatter() if log_format == "json" else logging.Formatter(_TEXT_FORMAT)
    )
    logging.basicConfig(level=level, handlers=[handler], force=True)
    logging.getLogger("dlt").handlers = [handler]

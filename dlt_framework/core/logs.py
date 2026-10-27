"""One log format for the framework and dlt, chosen by ``[logging] format``.

A new format (logfmt, GELF, ...) is one ``logging.Formatter`` registered with
``@formatters.register("name")``.

Every line carries the fields ``bound`` holds where it was logged: a run binds ``run_id``,
``pipeline``, and ``mode``, and a load adds ``load_id``, so a log line joins to its monitoring
rows. Fields given with ``extra=`` win.
"""

import atexit
import contextvars
import json
import logging
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from types import MappingProxyType
from typing import Any

from dlt_framework.core import alerts
from dlt_framework.core.alerts import AlertSettings
from dlt_framework.core.config import Registry
from dlt_framework.core.errors import redact
from dlt_framework.core.settings import LoggingSettings

_STANDARD_RECORD_KEYS = set(vars(logging.makeLogRecord({}))) | {"message", "asctime", "run"}

formatters: Registry[logging.Formatter] = Registry("log format")

_BOUND: contextvars.ContextVar[Mapping[str, Any]] = contextvars.ContextVar(
    "log_fields", default=MappingProxyType({})
)


@contextmanager
def bound(**fields: Any) -> Iterator[None]:
    """Add ``fields`` to every line logged in this context, and threads started from it that
    copy it (dlt's extract workers do from Python 3.14)."""
    token = _BOUND.set({**_BOUND.get(), **fields})
    try:
        yield
    finally:
        _BOUND.reset(token)


class _Bound(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        for key, value in _BOUND.get().items():
            if not hasattr(record, key):
                setattr(record, key, value)
        return True


@formatters.register("text")
class TextFormatter(logging.Formatter):
    """``time LEVEL logger [run_id]: message``; the brackets only while a run is bound."""

    def __init__(self) -> None:
        super().__init__("%(asctime)s %(levelname)-7s %(name)s%(run)s: %(message)s")

    def formatMessage(self, record: logging.LogRecord) -> str:
        run = getattr(record, "run_id", None)
        record.run = f" [{run}]" if run else ""
        return super().formatMessage(record)


@formatters.register("json")
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
    """Every line the process writes, framework or dlt, with secrets in URLs masked and the
    bound fields added.

    dlt reuses the first handler on its logger and replaces its formatter on every pipeline
    start. Ignoring that keeps one format for every line.
    """

    def __init__(self) -> None:
        super().__init__()
        self.addFilter(_Bound())

    def format(self, record: logging.LogRecord) -> str:
        return redact(super().format(record))

    def setFormatter(self, fmt: logging.Formatter | None) -> None:
        if self.formatter is None:
            super().setFormatter(fmt)


def configure(settings: LoggingSettings, alerting: AlertSettings | None = None) -> None:
    """One handler for every line; with ``alerting`` set, alert events also go to its webhook,
    which is drained when the process exits."""
    handler = _Handler()
    handler.setFormatter(formatters.get(settings.format)())
    logging.basicConfig(level=settings.level, handlers=[handler], force=True)
    logging.getLogger("dlt").handlers = [handler]
    if alerting and (listener := alerts.attach(alerting, _Bound())):
        atexit.register(listener.stop)

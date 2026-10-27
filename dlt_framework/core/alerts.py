"""A fallback for notifications: event-tagged log lines posted to one webhook.

Notifying is the orchestrator's job: it sees what a process cannot report, such as an OOM kill
or a job that never started, and alerts on exit codes and on the ``event`` field of log lines.
This exists for where there is no such orchestrator. It knows no service: ``body`` is a
template, so Telegram, Slack, Teams, or a generic collector is a URL and a body in config:

    [alerts]                              # url is a secret: ALERTS__WEBHOOK_URL
    body = '{"chat_id": "123", "text": "$level $pipeline $event: $message"}'

``$name`` is any field of the log line (``message``, ``level``, ``event``, ``pipeline``,
``run_id``, the ``error_*`` fields); each is JSON-escaped and redacted.
"""

import json
import logging
import logging.handlers
import queue
import string
import sys
import time
import urllib.request
from typing import Any

from dlt.common.configuration import configspec
from dlt.common.configuration.specs import BaseConfiguration
from dlt.common.typing import TSecretStrValue

from dlt_framework.core.config import REQUIRED
from dlt_framework.core.errors import redact


@configspec
class AlertSettings(BaseConfiguration):
    """``webhook_url`` unset is off. ``events`` are the log events posted, at ``level`` or
    above; one per pipeline and event is posted per ``cooldown_s``."""

    webhook_url: TSecretStrValue | None = None
    body: str = REQUIRED
    events: list[str] = REQUIRED
    level: str = REQUIRED
    cooldown_s: float = REQUIRED
    timeout_s: float = REQUIRED


class Webhook(logging.Handler):
    """Posts ``body``, filled from the record, for each matching event; never raises."""

    def __init__(self, settings: AlertSettings) -> None:
        super().__init__(settings.level)
        assert settings.webhook_url
        self.url = str(settings.webhook_url)
        self.body = string.Template(settings.body)
        self.events = set(settings.events)
        self.cooldown_s = settings.cooldown_s
        self.timeout_s = settings.timeout_s
        self._sent: dict[tuple[Any, str], float] = {}

    def filter(self, record: logging.LogRecord) -> bool:
        event = getattr(record, "event", None)
        if event not in self.events or not super().filter(record):
            return False
        key = (getattr(record, "pipeline", None), event)
        now = time.monotonic()
        if now - self._sent.get(key, -self.cooldown_s) < self.cooldown_s:
            return False
        self._sent[key] = now
        return True

    def emit(self, record: logging.LogRecord) -> None:
        fields = {**vars(record), "message": record.getMessage(), "level": record.levelname}
        text = {key: json.dumps(redact(str(value)))[1:-1] for key, value in fields.items()}
        request = urllib.request.Request(
            self.url,
            data=self.body.safe_substitute(text).encode(),
            headers={"Content-Type": "application/json"},
        )
        try:
            with urllib.request.urlopen(request, timeout=self.timeout_s):
                pass
        except Exception as error:  # a failed alert must not fail the run
            self.handleError(record, error)

    def handleError(self, record: logging.LogRecord, error: BaseException | None = None) -> None:
        """One line on stderr, never the URL: a bot token can be in its path."""
        kind = type(error).__name__ if error else "error"
        status = getattr(error, "code", "")
        print(f"pipeline: alert webhook failed ({kind} {status})".rstrip(), file=sys.stderr)


def attach(settings: AlertSettings, bound: logging.Filter) -> logging.handlers.QueueListener | None:
    """Queue alert lines to a background thread posting them, so a slow webhook never holds up
    a run. Returns the listener to stop (and drain) at exit; ``None`` when alerts are off."""
    if not settings.webhook_url:
        return None
    lines: queue.Queue[logging.LogRecord] = queue.Queue()
    handler = logging.handlers.QueueHandler(lines)
    handler.addFilter(bound)  # bound fields are added here, in the thread that logged
    logging.getLogger().addHandler(handler)  # dlt's own lines carry no event, so root is enough
    listener = logging.handlers.QueueListener(lines, Webhook(settings), respect_handler_level=True)
    listener.start()
    return listener

"""Run outcome notifications. A new channel (Slack, Teams, email) is one class with ``send``."""

from __future__ import annotations

import logging
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Protocol

LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class RunEvent:
    pipeline: str
    mode: str
    rows: Mapping[str, int] = field(default_factory=dict)
    error: BaseException | None = None

    @property
    def succeeded(self) -> bool:
        return self.error is None

    def __str__(self) -> str:
        if self.error is not None:
            return f"{self.mode} {self.pipeline} failed: {self.error}"
        loaded = ", ".join(f"{table}={count}" for table, count in sorted(self.rows.items()))
        return f"{self.mode} {self.pipeline} succeeded: {loaded or 'no rows'}"


class Notifier(Protocol):
    def send(self, event: RunEvent) -> None: ...


class LogNotifier:
    def send(self, event: RunEvent) -> None:
        level = logging.INFO if event.succeeded else logging.ERROR
        LOGGER.log(level, "%s", event, extra={"event": "run_finished", "pipeline": event.pipeline})

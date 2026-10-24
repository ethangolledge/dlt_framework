"""Run outcome notifications for alert channels (Slack, Teams, email): one class with ``send``.

The outcome is already logged by the CLI and recorded by monitoring; notifiers only push alerts.
"""

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Protocol

from dlt_framework.core.errors import redact


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
            return f"{self.mode} {self.pipeline} failed: {redact(str(self.error))}"
        return f"{self.mode} {self.pipeline} succeeded: {summarise(self.rows)}"


def summarise(rows: Mapping[str, int]) -> str:
    """``carts=208, users=208``, or ``no rows``."""
    return ", ".join(f"{table}={count}" for table, count in sorted(rows.items())) or "no rows"


class Notifier(Protocol):
    def send(self, event: RunEvent) -> None: ...

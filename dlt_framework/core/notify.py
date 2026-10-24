"""Run outcome alerts. Channels are stated in config and picked by ``kind``:

    [notifications]
    on = ["failed"]
    channels = ["ops"]

    [notifications.ops]
    kind = "webhook"            # notifications.ops.url lives in secrets.toml

A new kind (Teams, email, PagerDuty) is one ``Notifier`` subclass registered with
``@notifiers.register("kind")``; its fields resolve from ``[notifications.<channel>]``. The outcome
is already logged by the CLI and recorded by monitoring; channels only push alerts.
"""

from abc import abstractmethod
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any

import dlt
from dlt.common.configuration import configspec
from dlt.common.configuration.specs import BaseConfiguration
from dlt.common.typing import TSecretStrValue
from dlt.sources.helpers import requests

from dlt_framework.core.config import REQUIRED, Registry, resolve
from dlt_framework.core.errors import ConfigurationError, redact
from dlt_framework.core.settings import NotificationSettings


@dataclass(frozen=True, slots=True)
class RunEvent:
    pipeline: str
    mode: str
    rows: Mapping[str, int] = field(default_factory=dict)
    error: BaseException | None = None

    @property
    def outcome(self) -> str:
        return "succeeded" if self.error is None else "failed"

    def __str__(self) -> str:
        if self.error is not None:
            return f"{self.mode} {self.pipeline} failed: {redact(str(self.error))}"
        return f"{self.mode} {self.pipeline} succeeded: {summarise(self.rows)}"


def summarise(rows: Mapping[str, int]) -> str:
    """``carts=208, users=208``, or ``no rows``."""
    return ", ".join(f"{table}={count}" for table, count in sorted(rows.items())) or "no rows"


@configspec
class Notifier(BaseConfiguration):
    @abstractmethod
    def send(self, event: RunEvent) -> None: ...


notifiers: Registry[Notifier] = Registry("notification kind")


@notifiers.register("webhook")
@configspec
class WebhookNotifier(Notifier):
    """POSTs ``{"text": "<outcome line>"}``: Slack and Mattermost incoming webhooks, or any
    collector that takes JSON. Requests retry through dlt's HTTP client (``[runtime]``)."""

    url: TSecretStrValue = REQUIRED

    def send(self, event: RunEvent) -> None:
        requests.post(self.url, json={"text": str(event)}, timeout=30).raise_for_status()


def channels(settings: NotificationSettings) -> list[Notifier]:
    """The configured channels, resolved when a run starts so a broken channel fails early."""
    resolved = []
    for channel in settings.channels:
        kind: Any = dlt.config.get(f"notifications.{channel}.kind")
        if not kind:
            raise ConfigurationError(f"notifications.{channel}.kind is not set")
        resolved.append(resolve(notifiers.get(str(kind))(), "notifications", channel))
    return resolved

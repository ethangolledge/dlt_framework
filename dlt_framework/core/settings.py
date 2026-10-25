"""Runtime settings: how runs behave in an environment, stated in ``.dlt/config.toml``.

Pipeline definitions state what lands; these state how runs behave: retries, monitoring,
guards, and logging. They resolve through dlt's configuration system, so each key
can also come from an environment variable (``RETRY__ATTEMPTS``) or a vault, and any section can
be overridden for one pipeline, field by field:

    [retry]                        every pipeline
    [pipelines.acme_shop.retry]    acme_shop only; keys it leaves out come from [retry]

Every key is required. A missing one stops the command (exit 2) and names the keys dlt tried.
"""

from dlt.common.configuration import configspec
from dlt.common.configuration.specs import BaseConfiguration
from dlt.common.typing import TSecretStrValue

from dlt_framework.core.config import REQUIRED, resolve
from dlt_framework.core.errors import ConfigurationError
from dlt_framework.core.retry import Retry


@configspec
class MonitoringSettings(BaseConfiguration):
    """``uri`` picks the sink by scheme and is a secret: it can hold a database password."""

    uri: TSecretStrValue = REQUIRED
    sample_every_s: float = REQUIRED
    flush_every_s: float = REQUIRED

    def on_resolved(self) -> None:
        if not self.uri:
            raise ConfigurationError(
                "monitoring.uri is not set; state where run monitoring goes, e.g. "
                "MONITORING__URI=file:///data/monitoring or a database URL"
            )


@configspec
class GuardSettings(BaseConfiguration):
    future_cursor_tolerance_s: float = REQUIRED


@configspec
class RunSettings(BaseConfiguration):
    retry: Retry = REQUIRED
    monitoring: MonitoringSettings = REQUIRED
    guards: GuardSettings = REQUIRED

    @classmethod
    def of(cls, pipeline: str) -> "RunSettings":
        return resolve(cls(), "pipelines", pipeline)


@configspec
class LoggingSettings(BaseConfiguration):
    """``format`` is a registered log format (``text``, ``json``)."""

    format: str = REQUIRED
    level: str = REQUIRED

"""Runtime settings: how runs behave in an environment, stated in ``.dlt/config.toml``.

Pipeline definitions state what lands; these state how runs behave: retries, monitoring,
guards, health thresholds, and logging. They resolve through dlt's configuration system, so each key
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
    """``uri`` picks the sink by scheme and is a secret: it can hold a database password.

    ``code_version`` is written on every run, so a change in behaviour can be traced to a
    release: set it per build (``MONITORING__CODE_VERSION`` = the image tag or git SHA).
    ``profile_every_ms`` samples every thread's stack at that interval into ``profiles``; 0 is
    off (``pipeline run --profile`` turns it on for one run).
    """

    uri: TSecretStrValue = REQUIRED
    sample_every_s: float = REQUIRED
    flush_every_s: float = REQUIRED
    code_version: str = REQUIRED
    profile_every_ms: float = REQUIRED

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
class HealthSettings(BaseConfiguration):
    """What ``pipeline health`` calls wrong, against each pipeline's own history.

    ``expect_every_s`` is how often the pipeline should succeed; 0 derives it from the median
    gap between recent successes, when those gaps are regular (runs by hand set no schedule).
    It is late (``run_stale``) after ``stale_after_intervals`` of those, slow after
    ``slow_after`` times the p95 of recent durations, and its rows unusual at ``rows_spike``
    times their median, or none where there usually are some. ``history_runs`` is how many
    recent runs are compared; ``memory_headroom`` the share of memory a run should leave free.
    """

    history_runs: int = REQUIRED
    expect_every_s: float = REQUIRED
    stale_after_intervals: float = REQUIRED
    slow_after: float = REQUIRED
    rows_spike: float = REQUIRED
    memory_headroom: float = REQUIRED


@configspec
class RunSettings(BaseConfiguration):
    retry: Retry = REQUIRED
    monitoring: MonitoringSettings = REQUIRED
    guards: GuardSettings = REQUIRED
    health: HealthSettings = REQUIRED

    @classmethod
    def of(cls, pipeline: str) -> "RunSettings":
        return resolve(cls(), "pipelines", pipeline)


@configspec
class LoggingSettings(BaseConfiguration):
    """``format`` is a registered log format (``text``, ``json``)."""

    format: str = REQUIRED
    level: str = REQUIRED

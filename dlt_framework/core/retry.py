"""Retrying pipeline steps on transient failures only."""

import contextlib
import logging
from collections.abc import Callable
from typing import TypeVar

import requests
import sqlalchemy.exc
from dlt.common.configuration import configspec
from dlt.common.configuration.exceptions import ConfigurationException
from dlt.common.configuration.specs import BaseConfiguration
from dlt.common.exceptions import TerminalException
from dlt.common.runtime import signals
from dlt.destinations.exceptions import DestinationTransientException
from tenacity import (
    RetryCallState,
    Retrying,
    retry_if_exception,
    stop_after_attempt,
    stop_after_delay,
    wait_exponential_jitter,
)

from dlt_framework.core.config import REQUIRED
from dlt_framework.core.errors import (
    ConfigurationError,
    ResourceExhaustedError,
    RunError,
    TerminalRunError,
    TransientRunError,
    causes,
    root_cause,
)

LOGGER = logging.getLogger(__name__)
T = TypeVar("T")
# Failures known to heal by waiting: the network, a busy or restarting server or database, and
# what dlt's destinations mark transient. Anything else is a bug or bad data, which no retry fixes.
_HEALS: tuple[type[BaseException], ...] = (
    ConnectionError,
    TimeoutError,
    requests.exceptions.ConnectionError,
    requests.exceptions.Timeout,
    sqlalchemy.exc.OperationalError,
    DestinationTransientException,
)
_RETRIED_STATUS = {408, 429, 500, 502, 503, 504}
# Out of memory: Python's, and a database's that hit its own limit. DuckDB raises it at its
# memory_limit, and dlt marks it transient, but the same work at the same limit fails again.
_OUT_OF_MEMORY: tuple[type[BaseException], ...] = (MemoryError,)
with contextlib.suppress(ImportError):  # the duckdb extra
    import duckdb

    _OUT_OF_MEMORY += (duckdb.OutOfMemoryException,)


@configspec
class Retry(BaseConfiguration):
    """Calls a pipeline step, retrying with jittered backoff while dlt deems the error transient.

    Waits grow exponentially from ``initial_wait_s`` to ``max_wait_s``; the step gives up after
    ``attempts`` or once ``give_up_after_s`` has passed, whichever comes first, so a database or
    API down for minutes is ridden out. Stated per environment in ``[retry]`` (see
    ``core.settings``). Every error leaving a step is a ``RunError`` carrying the step and the
    root cause ("HTTPError: 401 ...") instead of dlt's wrapper text.
    """

    attempts: int = REQUIRED
    initial_wait_s: float = REQUIRED
    max_wait_s: float = REQUIRED
    give_up_after_s: float = REQUIRED

    def __call__(self, step: str, call: Callable[[], T]) -> T:
        retrying = Retrying(
            stop=stop_after_attempt(self.attempts) | stop_after_delay(self.give_up_after_s),
            wait=wait_exponential_jitter(initial=self.initial_wait_s, max=self.max_wait_s),
            retry=retry_if_exception(self.is_transient),
            before_sleep=lambda state: self._log_retry(step, state),
            sleep=_sleep,
            reraise=True,
        )
        try:
            for attempt in retrying:
                with attempt:
                    return call()
        except RunError as error:
            error.step = error.step or step
            raise
        except Exception as error:
            raise self._classify(step, error, retrying.statistics["attempt_number"]) from error
        raise AssertionError("tenacity returned without a result")

    @staticmethod
    def is_transient(error: BaseException) -> bool:
        """Whether waiting can fix it: a failure known to heal somewhere in its chain, and none
        that never does. Unknown errors are not retried: they fail the run at once, and the next
        scheduled run tries again, instead of repeating a bug for ``give_up_after_s``."""
        chain = list(causes(error))
        return any(map(_heals, chain)) and not any(map(_never_heals, chain))

    @classmethod
    def _classify(cls, step: str, error: Exception, attempts: int) -> RunError:
        cause = root_cause(error)
        detail = f"{type(cause).__name__}: {cause}"
        if any(isinstance(item, _OUT_OF_MEMORY) for item in causes(error)):
            return ResourceExhaustedError(f"out of memory: {detail}", step=step)
        if any(isinstance(item, ConfigurationException) for item in causes(error)):
            return ConfigurationError(detail, step=step)  # a missing secret, read at run time
        if cls.is_transient(error):
            tries = f"{attempts} attempt{'s' if attempts > 1 else ''}"
            return TransientRunError(f"{detail} (after {tries})", step=step)
        if any(isinstance(item, ImportError) for item in causes(error)):
            detail += "; install the extra that provides it (README: Installing)"
        return TerminalRunError(detail, step=step)

    def _log_retry(self, step: str, state: RetryCallState) -> None:
        error = state.outcome.exception() if state.outcome else None
        cause = root_cause(error) if error else None
        LOGGER.warning(
            "%s failed (attempt %s/%s), retrying: %s: %s",
            step,
            state.attempt_number,
            self.attempts,
            type(cause).__name__,
            cause,
            extra={"event": "retry", "step": step},
        )


def _sleep(seconds: float) -> None:
    """Wait between attempts, waking for SIGINT or SIGTERM: a stopping run does not wait out
    its backoff, and stops with its package kept (``SignalReceivedException``)."""
    signals.sleep(seconds)
    signals.raise_if_signalled()


def _heals(error: BaseException) -> bool:
    if isinstance(error, requests.exceptions.HTTPError) and error.response is not None:
        return error.response.status_code in _RETRIED_STATUS
    return isinstance(error, _HEALS)


def _never_heals(error: BaseException) -> bool:
    """Retrying cannot fix these: no memory, no permission, a dependency not installed, or an
    error dlt or a source marks terminal (dlt only looks for that at the top of the chain)."""
    return isinstance(error, (*_OUT_OF_MEMORY, PermissionError, ImportError, TerminalException))

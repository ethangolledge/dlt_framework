"""Retrying pipeline steps on transient failures only."""

import logging
from collections.abc import Callable
from typing import TypeVar

from dlt.common.configuration import configspec
from dlt.common.configuration.specs import BaseConfiguration
from dlt.common.exceptions import TerminalException
from dlt.pipeline.helpers import retry_load
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
    ResourceExhaustedError,
    RunError,
    TerminalRunError,
    TransientRunError,
    causes,
    root_cause,
)

LOGGER = logging.getLogger(__name__)
T = TypeVar("T")
_DLT_TRANSIENT = retry_load(("sync", "extract", "load", "run"))


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
        """dlt's transient classification, minus failures that never heal by retrying."""
        return bool(_DLT_TRANSIENT(error)) and not any(map(_never_heals, causes(error)))

    @classmethod
    def _classify(cls, step: str, error: Exception, attempts: int) -> RunError:
        cause = root_cause(error)
        detail = f"{type(cause).__name__}: {cause}"
        if any(isinstance(item, MemoryError) for item in causes(error)):
            return ResourceExhaustedError(f"out of memory: {detail}", step=step)
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


def _never_heals(error: BaseException) -> bool:
    """Retrying cannot fix these: no memory, no permission, a dependency not installed, or an
    error dlt or a source marks terminal (dlt only looks for that at the top of the chain)."""
    return isinstance(error, (MemoryError, PermissionError, ImportError, TerminalException))

"""Retrying pipeline steps on transient failures only."""

import logging
from collections.abc import Callable
from dataclasses import dataclass
from typing import TypeVar

from dlt.pipeline.helpers import retry_load
from tenacity import (
    RetryCallState,
    Retrying,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential_jitter,
)

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


@dataclass(frozen=True, slots=True)
class Retry:
    """Calls a pipeline step, retrying with jittered backoff while dlt deems the error transient.

    Every error leaving a step is a ``RunError`` carrying the step and the root cause
    ("HTTPError: 401 ...") instead of dlt's wrapper text.
    """

    attempts: int = 5
    min_wait: float = 4.0
    max_wait: float = 60.0

    def __call__(self, step: str, call: Callable[[], T]) -> T:
        try:
            for attempt in Retrying(
                stop=stop_after_attempt(self.attempts),
                wait=wait_exponential_jitter(initial=self.min_wait, max=self.max_wait),
                retry=retry_if_exception(self.is_transient),
                before_sleep=lambda state: self._log_retry(step, state),
                reraise=True,
            ):
                with attempt:
                    return call()
        except RunError as error:
            error.step = error.step or step
            raise
        except Exception as error:
            raise self._classify(step, error) from error
        raise AssertionError("tenacity returned without a result")

    @staticmethod
    def is_transient(error: BaseException) -> bool:
        """dlt's transient classification, minus failures that never heal by retrying."""
        return bool(_DLT_TRANSIENT(error)) and not any(map(_never_heals, causes(error)))

    def _classify(self, step: str, error: Exception) -> RunError:
        cause = root_cause(error)
        detail = f"{type(cause).__name__}: {cause}"
        if any(isinstance(item, MemoryError) for item in causes(error)):
            return ResourceExhaustedError(f"out of memory: {detail}", step=step)
        if self.is_transient(error):
            attempts = f"{self.attempts} attempt{'s' if self.attempts > 1 else ''}"
            return TransientRunError(f"{detail} (after {attempts})", step=step)
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
    return isinstance(error, (MemoryError, PermissionError))

"""Retrying pipeline steps on transient failures only."""

from __future__ import annotations

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

from dlt_framework.core.errors import RunError, TerminalRunError, TransientRunError

LOGGER = logging.getLogger(__name__)
T = TypeVar("T")
_DLT_TRANSIENT = retry_load(("sync", "extract", "load", "run"))


@dataclass(frozen=True, slots=True)
class Retry:
    """Calls a pipeline step, retrying with jittered backoff while dlt deems the error transient."""

    attempts: int = 5
    min_wait: float = 4.0
    max_wait: float = 60.0

    def __call__(self, operation: str, step: Callable[[], T]) -> T:
        try:
            for attempt in Retrying(
                stop=stop_after_attempt(self.attempts),
                wait=wait_exponential_jitter(initial=self.min_wait, max=self.max_wait),
                retry=retry_if_exception(self.is_transient),
                before_sleep=lambda state: self._log_retry(operation, state),
                reraise=True,
            ):
                with attempt:
                    return step()
        except RunError:
            raise
        except Exception as error:
            if self.is_transient(error):
                raise TransientRunError(
                    f"{operation} failed after {self.attempts} attempts: {error}"
                ) from error
            raise TerminalRunError(f"{operation} failed: {error}") from error
        raise AssertionError("tenacity returned without a result")

    @staticmethod
    def is_transient(error: BaseException) -> bool:
        """dlt's transient classification, except local permission problems, which never heal."""
        return bool(_DLT_TRANSIENT(error)) and not _caused_by_permissions(error)

    def _log_retry(self, operation: str, state: RetryCallState) -> None:
        error = state.outcome.exception() if state.outcome else None
        LOGGER.warning(
            "%s failed (attempt %s/%s): %s",
            operation,
            state.attempt_number,
            self.attempts,
            error,
            extra={"event": "retry", "operation": operation},
        )


def _caused_by_permissions(error: BaseException | None) -> bool:
    seen: set[int] = set()
    while error is not None and id(error) not in seen:
        seen.add(id(error))
        message = str(error).casefold()
        if isinstance(error, PermissionError) or "permission denied" in message:
            return True
        error = error.__cause__ or error.__context__
    return False

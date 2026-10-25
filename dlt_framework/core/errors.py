"""Framework exceptions. Each class carries the process exit code the CLI returns for it.

A ``RunError`` records where it happened (``step``, ``resource``, ``window``) so logs and
monitoring can say exactly what failed, not only how.
"""

import re
from collections.abc import Iterator
from typing import Any, Self

_IDENTIFIER = re.compile(r"^[a-z][a-z0-9_]*$")
_URL = re.compile(r"\b[a-z][a-z0-9+.-]*://[^\s'\"<>]+", re.IGNORECASE)
_URL_PASSWORD = re.compile(r"(?<=://)([^/@:\s]+):[^/@\s]+@")
_QUERY_VALUE = re.compile(r"([?&][^=&#]+)=[^&#]*")


class RunError(Exception):
    exit_code = 1

    def __init__(self, message: str, *, step: str | None = None) -> None:
        super().__init__(message)
        self.step = step
        self.resource: str | None = None
        self.window: str | None = None

    def locate(self, resource: str, window: str) -> Self:
        """Attach the resource and window being loaded, unless a deeper frame already did."""
        self.resource = self.resource or resource
        self.window = self.window or window
        return self

    @property
    def location(self) -> str:
        """e.g. ``extract of orders, window [2025-01-01, 2025-02-01)``; empty when unknown."""
        parts = [
            f"{self.step or 'run'} of {self.resource}" if self.resource else self.step,
            f"window {self.window}" if self.window else None,
        ]
        return ", ".join(part for part in parts if part)


class DefinitionError(RunError):
    """A pipeline, source, resource, or cursor definition is invalid."""

    exit_code = 2


class ConfigurationError(RunError):
    """Runtime configuration or command-line input is invalid."""

    exit_code = 2


class GuardError(RunError):
    """A safety guard refused to run; an operator must decide what happens next."""

    exit_code = 2


class TerminalRunError(RunError):
    """A pipeline step failed in a way that retrying cannot fix."""

    exit_code = 3


class TransientRunError(RunError):
    """A pipeline step still failed after every retry attempt."""

    exit_code = 4


class ResourceExhaustedError(RunError):
    """The process ran out of memory. A kernel/Docker OOM kill (SIGKILL, 137) cannot be caught;
    monitoring samples show what memory was doing before it."""

    exit_code = 5


def exit_code(error: BaseException) -> int:
    match error:
        case RunError():
            return error.exit_code
        case KeyboardInterrupt():
            return 130
        case _:
            return 1


def causes(error: BaseException) -> Iterator[BaseException]:
    """``error`` and everything it was raised from, outermost first, including the errors an
    exception group holds (async libraries such as Scrapling raise a group of them)."""
    seen: set[int] = set()
    pending: list[BaseException] = [error]
    while pending:
        current = pending.pop(0)
        if id(current) in seen:
            continue
        seen.add(id(current))
        yield current
        if isinstance(current, BaseExceptionGroup):
            pending.extend(current.exceptions)
        if (raised_from := current.__cause__ or current.__context__) is not None:
            pending.append(raised_from)


def root_cause(error: BaseException) -> BaseException:
    """The innermost error, e.g. the ``HTTPError`` beneath dlt's ``PipelineStepFailed``."""
    *_, root = causes(error)
    return root


def error_fields(error: BaseException) -> dict[str, Any]:
    """What failed, where, and why: the failure's log fields and monitoring columns."""
    cause = root_cause(error)
    located = error if isinstance(error, RunError) else None
    return {
        "exit_code": exit_code(error),
        "error_type": type(error).__name__,
        "error_step": located.step if located else None,
        "error_resource": located.resource if located else None,
        "error_window": located.window if located else None,
        "error_cause_type": type(cause).__name__,
        "error_message": redact(str(cause)),
    }


def redact(text: str) -> str:
    """``text`` with URL passwords and query-string values masked.

    Error messages carry URLs: an HTTP 401 names the request URL, which can hold an API key, and
    a connection string holds a password. Logs and monitoring must not.
    """
    return _URL.sub(_redact_url, text)


def _redact_url(match: re.Match[str]) -> str:
    return _QUERY_VALUE.sub(r"\1=***", _URL_PASSWORD.sub(r"\1:***@", match[0]))


def require_identifier(value: str, kind: str) -> str:
    """Names become dlt state keys and table names, so they must be stable snake_case."""
    if not _IDENTIFIER.fullmatch(value):
        raise DefinitionError(f"{kind} {value!r} must be snake_case and start with a letter")
    return value

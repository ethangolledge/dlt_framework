"""Framework exceptions. Each class carries the process exit code the CLI returns for it."""

import re

_IDENTIFIER = re.compile(r"^[a-z][a-z0-9_]*$")


class RunError(Exception):
    exit_code = 1


class DefinitionError(RunError):
    """A pipeline, source, table, or cursor definition is invalid."""

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


def require_identifier(value: str, kind: str) -> str:
    """Names become dlt state keys and table names, so they must be stable snake_case."""
    if not _IDENTIFIER.fullmatch(value):
        raise DefinitionError(f"{kind} {value!r} must be snake_case and start with a letter")
    return value

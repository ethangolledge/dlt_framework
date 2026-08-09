class FrameworkError(Exception):
    """Base error for invalid framework configuration or source definitions."""


class ConfigurationError(FrameworkError):
    """Raised when required runtime configuration is missing or invalid."""


class SourceDefinitionError(FrameworkError):
    """Raised when a source selector or source module violates the contract."""


class BackfillError(FrameworkError):
    """Raised when a requested backfill is invalid or unsupported."""


class DataContractError(FrameworkError):
    """Raised when extracted data violates a declared resource policy."""


class TerminalRunError(Exception):
    """A pipeline failure that requires configuration, code, or data intervention."""


class TransientRunError(Exception):
    """A retryable failure that remained after the configured attempts."""

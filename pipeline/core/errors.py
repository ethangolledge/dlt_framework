class FrameworkError(Exception):
    """Base error for invalid framework configuration or source definitions."""


class ConfigurationError(FrameworkError):
    """Raised when required runtime configuration is missing or invalid."""


class SourceDefinitionError(FrameworkError):
    """Raised when a source selector or source module violates the contract."""

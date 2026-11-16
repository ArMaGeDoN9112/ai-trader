"""Errors that can be shown to an operator without exposing integration data."""


class RuntimeErrorInfo(ValueError):
    """A sanitized configuration or operation error suitable for CLI output."""


def error_code(error: Exception) -> str:
    return str(error) if isinstance(error, RuntimeErrorInfo) else type(error).__name__


class Rejected(ValueError):
    """Admission failed with a stable reason suitable for the local journal."""

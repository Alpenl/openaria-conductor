"""Shared stdlib-only error types for Open Aria Conductor."""

from __future__ import annotations

from collections.abc import Mapping

__all__ = ["ProviderError"]


class ProviderError(RuntimeError):
    """Typed device provider error mapped to HTTP status and API error envelope."""

    def __init__(
        self,
        code: str,
        message: str,
        *,
        status: int,
        retryable: bool = False,
        headers: Mapping[str, str] | None = None,
        details: Mapping[str, object] | None = None,
    ) -> None:
        self.code = code
        self.message = message
        self.status = status
        self.retryable = retryable
        self.headers = headers
        self.details = details
        super().__init__(message)

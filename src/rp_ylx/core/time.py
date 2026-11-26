"""UTC timestamp formatting helpers."""

from __future__ import annotations

from datetime import UTC, datetime

__all__ = ["utc_now_iso"]


def utc_now_iso() -> str:
    """Return the current UTC timestamp formatted as an ISO-8601 Z string."""
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")

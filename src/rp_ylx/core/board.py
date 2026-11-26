"""Canonical board model normalization and RDK X5 detection."""

from __future__ import annotations

RDK_X5_MODEL = "D-Robotics RDK X5 V1.0"

__all__ = ["RDK_X5_MODEL", "is_rdk_x5_model", "normalize_board_model"]


def normalize_board_model(raw: str | bytes) -> str:
    """Decode bytes if needed, strip NUL bytes, and normalize whitespace."""
    text = raw.decode("utf-8", errors="replace") if isinstance(raw, bytes) else raw
    return " ".join(text.replace("\x00", "").split())


def is_rdk_x5_model(raw: str | bytes | None) -> bool:
    """Return True when the normalized board model matches RDK X5 case-insensitively."""
    if not isinstance(raw, (str, bytes)):
        return False
    return normalize_board_model(raw).casefold() == RDK_X5_MODEL.casefold()

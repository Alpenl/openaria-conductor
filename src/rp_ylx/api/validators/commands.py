"""Request payload and identifier validators for Device API command endpoints."""

from __future__ import annotations

from collections.abc import Mapping

from rp_ylx.core.ids import valid_sha256, valid_uuid4, valid_uuid7

__all__ = [
    "valid_camera_focus_body",
    "valid_capture_body",
    "valid_delete_request",
    "valid_idempotency_key",
    "valid_session_id",
]


def valid_session_id(api_version: str, session_id: str) -> bool:
    return valid_uuid7(session_id) or (api_version == "v2" and valid_uuid4(session_id))


def valid_idempotency_key(key: str | None, *, is_single: bool) -> bool:
    """Validate single-occurrence Idempotency-Key rejecting quotes, spaces, and controls."""
    if not is_single or key is None or not 1 <= len(key) <= 128:
        return False
    return all(character != '"' and 0x21 <= ord(character) <= 0x7E for character in key)


def valid_capture_body(operation: str, body: Mapping[str, object]) -> bool:
    """Validate closed start/stop capture command request payloads."""
    if operation == "start":
        allowed = {"schema", "mode", "volume_id", "display_name", "take"}
        if set(body) - allowed or body.get("schema") != "ylx.capture-start.v2":
            return False
        mode = body.get("mode")
        if not isinstance(mode, str) or mode not in {"production", "calibration"}:
            return False
        if "volume_id" in body and not valid_uuid4(body["volume_id"]):
            return False
        if "display_name" in body and (
            not isinstance(body["display_name"], str) or not 1 <= len(body["display_name"]) <= 160
        ):
            return False
        take = body.get("take")
        if not isinstance(take, dict):
            return False
        if take.get("kind") == "new":
            return set(take) == {"kind"}
        continuation = take.get("continuation_of")
        return (
            take.get("kind") == "continue"
            and set(take) == {"kind", "continuation_of"}
            and valid_uuid7(continuation)
        )
    return (
        set(body) == {"schema", "reason"}
        and body.get("schema") == "ylx.capture-stop.v2"
        and isinstance(body.get("reason"), str)
        and body.get("reason") in {"user", "safe_swap"}
    )


def valid_camera_focus_body(body: Mapping[str, object]) -> bool:
    """Validate closed camera focus command request payloads."""
    if set(body) - {"schema", "value", "auto_enabled"}:
        return False
    if body.get("schema") != "ylx.camera-focus-set.v1":
        return False
    has_value = "value" in body
    has_auto = "auto_enabled" in body
    if not has_value and not has_auto:
        return False
    if has_value and (type(body["value"]) is not int or body["value"] < 0):
        return False
    return not (has_auto and type(body["auto_enabled"]) is not bool)


def valid_delete_request(body: object) -> bool:
    """Validate closed session deletion request payloads."""
    if not isinstance(body, Mapping) or set(body) != {"schema", "sessions"}:
        return False
    items = body.get("sessions")
    if body.get("schema") != "ylx.session-delete-request.v1" or not isinstance(items, list):
        return False
    if not 1 <= len(items) <= 200:
        return False
    seen: set[str] = set()
    for item in items:
        if not isinstance(item, Mapping) or set(item) != {"session_id", "manifest_sha256"}:
            return False
        session_id = item["session_id"]
        digest = item["manifest_sha256"]
        if not valid_uuid7(session_id) or not valid_sha256(digest) or session_id in seen:
            return False
        assert isinstance(session_id, str)
        seen.add(session_id)
    return True

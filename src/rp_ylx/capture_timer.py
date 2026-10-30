"""Optional capture timer contract, separate from legacy capture snapshots."""

import math
from collections.abc import Mapping

DEFAULT_TIMER = {"enabled": False, "duration_seconds": 600}
MAX_DURATION_SECONDS = 86400


def valid_timer_settings(value: object) -> bool:
    return (
        isinstance(value, Mapping)
        and set(value) == {"enabled", "duration_seconds"}
        and type(value.get("enabled")) is bool
        and type(value.get("duration_seconds")) is int
        and 1 <= value["duration_seconds"] <= MAX_DURATION_SECONDS
    )


def valid_timer_status(value: object) -> bool:
    if not isinstance(value, Mapping) or set(value) != {"schema", "settings", "active"}:
        return False
    if value["schema"] != "ylx.capture-timer.v1" or not valid_timer_settings(value["settings"]):
        return False
    active = value["active"]
    if active is None:
        return True
    return (
        isinstance(active, Mapping)
        and set(active) == {"session_id", "duration_seconds", "remaining_seconds"}
        and isinstance(active["session_id"], str)
        and bool(active["session_id"])
        and type(active["duration_seconds"]) is int
        and 1 <= active["duration_seconds"] <= MAX_DURATION_SECONDS
        and type(active["remaining_seconds"]) in {int, float}
        and math.isfinite(active["remaining_seconds"])
        and 0 <= active["remaining_seconds"] <= active["duration_seconds"]
    )

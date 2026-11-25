"""Read-only capture admission details, independent of the last take's outcome."""

import os
from collections.abc import Mapping
from pathlib import Path

SYSTEM_RESERVE_BYTES = 256 * 1024 * 1024
SYSTEM_RESERVE_INODES = 1024


def system_capacity(path: Path) -> tuple[int, int]:
    info = os.statvfs(path)
    return info.f_bavail * info.f_frsize, info.f_favail


def valid_readiness(value: object) -> bool:
    if not isinstance(value, Mapping) or set(value) != {
        "schema",
        "allowed",
        "code",
        "message",
        "storage",
    }:
        return False
    if (
        value["schema"] != "ylx.capture-readiness.v1"
        or type(value["allowed"]) is not bool
        or not isinstance(value["code"], str)
        or not isinstance(value["message"], str)
        or not value["message"]
        or value["allowed"] != (value["code"] == "ready")
    ):
        return False
    storage = value["storage"]
    if not isinstance(storage, Mapping) or set(storage) != {"recording", "system", "backing"}:
        return False
    for budget in storage.values():
        if budget is None:
            continue
        if not isinstance(budget, Mapping) or set(budget) != {"available_bytes", "required_bytes"}:
            return False
        if any(type(n) is not int or n < 0 for n in budget.values()):
            return False
    return True

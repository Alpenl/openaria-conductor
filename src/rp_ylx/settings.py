"""Validated dynamic settings; bootstrap identity, credentials and paths stay in JSON."""

from __future__ import annotations

import copy
import json
import os
from collections.abc import Mapping
from pathlib import Path

from rp_ylx.persistence import PersistenceError, StateDatabase

FIELDS = {
    "camera": {"width", "height", "fps", "exposure_time_absolute"},
    "audio": {"enabled", "device", "sample_rate_hz", "channels", "sample_format"},
    "recording": None,
    "storage": {"minimum_available_bytes", "minimum_available_inodes"},
}


def settings_path(bootstrap: Mapping[str, object]) -> Path:
    value = bootstrap.get("state_root")
    if not isinstance(value, str) or not value:
        raise PersistenceError("invalid bootstrap state_root")
    return Path(value) / "settings.sqlite3"


def extract(bootstrap: Mapping[str, object]) -> dict:
    result = {}
    for group, allowed in FIELDS.items():
        value = bootstrap.get(group)
        if isinstance(value, dict):
            result[group] = {k: v for k, v in value.items() if allowed is None or k in allowed}
    return result


def overlay(bootstrap: Mapping[str, object], dynamic: Mapping[str, object]) -> dict:
    result = copy.deepcopy(dict(bootstrap))
    for group, value in dynamic.items():
        if group not in FIELDS or not isinstance(value, dict):
            raise PersistenceError("unknown dynamic settings group")
        allowed = FIELDS[group]
        if allowed is not None and set(value) - allowed:
            raise PersistenceError("bootstrap fields cannot be changed through dynamic settings")
        result.setdefault(group, {}).update(value)
    return result


def effective(bootstrap: Mapping[str, object]) -> dict:
    path = settings_path(bootstrap)
    if not path.exists() and not path.with_suffix(".sqlite3.initialized").exists():
        return dict(bootstrap)
    dynamic = StateDatabase(path).get("device")
    if dynamic is None:
        raise PersistenceError("dynamic device settings are missing")
    return overlay(bootstrap, dynamic)


def initialize(path: Path) -> None:
    from rp_ylx.daemon import production_config_from_value

    base = json.loads(path.read_bytes())
    production_config_from_value(base)
    db = StateDatabase(settings_path(base))
    db.initialize_document("device", extract(base), str(path))
    production_config_from_value(effective(base))


def manage(
    config: Path, action: str, *, source: Path | None = None, destination: Path | None = None
) -> dict:
    from rp_ylx.daemon import production_config_from_value

    base = json.loads(config.read_bytes())
    production_config_from_value(base)
    initialize(config)
    db = StateDatabase(settings_path(base))
    current = db.get("device")
    if action == "set":
        if source is None:
            raise ValueError("--input is required")
        patch = json.loads(source.read_bytes())
        if not isinstance(patch, dict):
            raise ValueError("settings patch must be an object")

        def validated_update(current):
            merged = overlay(overlay(base, current), patch)
            production_config_from_value(merged)
            return extract(merged)

        db.update("device", validated_update)
        return {"saved": True, "activation": "restart service while idle"}
    if action == "backup":
        if destination is None:
            raise ValueError("--output is required")
        db.backup(destination)
        return {"backup": str(destination)}
    if action == "export":
        if destination is None:
            raise ValueError("--output is required")
        fd = os.open(destination, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(fd, "w") as stream:
            json.dump(current, stream, ensure_ascii=False, indent=2)
            stream.flush()
            os.fsync(stream.fileno())
        return {"export": str(destination)}
    return {"settings": current}

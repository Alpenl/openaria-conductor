"""Validated dynamic settings; bootstrap identity, credentials and paths stay in JSON."""

from __future__ import annotations

import copy
import json
import os
import sqlite3
import uuid
from collections.abc import Mapping
from dataclasses import asdict
from pathlib import Path

from rp_ylx.config import ProductionConfigError, production_config_from_value
from rp_ylx.core.fs import export_atomic_no_overwrite, read_bounded_json
from rp_ylx.persistence import (
    PersistenceError,
    StateDatabase,
    readonly_connection,
)

__all__ = [
    "FIELDS",
    "effective",
    "extract",
    "initialize",
    "manage",
    "overlay",
    "read_effective_readonly",
    "resolved_settings",
    "settings_path",
]

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


def read_effective_readonly(bootstrap: Mapping[str, object]) -> dict:
    """Read effective settings without opening SQLite RW or creating WAL files."""
    path = settings_path(bootstrap)
    marker = path.with_suffix(".sqlite3.initialized")
    if not os.path.lexists(path):
        if os.path.lexists(marker):
            raise PersistenceError(f"missing authoritative database: {path}")
        return dict(bootstrap)
    try:
        with readonly_connection(path) as db:
            row = db.execute("SELECT body FROM documents WHERE name='device'").fetchone()
        if row is None:
            raise PersistenceError("dynamic device settings are missing")
        dynamic = json.loads(row[0])
    except (sqlite3.Error, json.JSONDecodeError, TypeError, RecursionError) as error:
        raise PersistenceError(f"cannot read state database {path}: {error}") from error
    if not isinstance(dynamic, dict):
        raise PersistenceError("invalid state document: device")
    return overlay(bootstrap, dynamic)


def effective(bootstrap: Mapping[str, object]) -> dict:
    return read_effective_readonly(bootstrap)


def resolved_settings(bootstrap: Mapping[str, object]) -> dict:
    """Persist effective defaults as values so future firmware defaults cannot change a take."""
    config = production_config_from_value(bootstrap)
    if config.recording_encoding is None:
        raise PersistenceError("recording encoding is missing")
    return {
        "camera": {
            "width": config.width,
            "height": config.height,
            "fps": config.fps,
            "exposure_time_absolute": config.exposure_time_absolute,
        },
        "audio": {
            "enabled": config.audio_enabled,
            "device": config.audio_device,
            "sample_rate_hz": config.audio_sample_rate_hz,
            "channels": config.audio_channels,
            "sample_format": config.audio_sample_format,
        },
        "recording": asdict(config.recording_encoding),
        "storage": {
            "minimum_available_bytes": config.minimum_available_bytes,
            "minimum_available_inodes": config.minimum_available_inodes,
        },
    }


def _initialize(base: object, source: Path) -> StateDatabase:
    if not isinstance(base, dict):
        raise ProductionConfigError("生产配置顶层字段无效")
    path = settings_path(base)
    if not os.path.lexists(path) and not os.path.lexists(path.with_suffix(".sqlite3.initialized")):
        # Reject an invalid first import before creating any database files.
        production_config_from_value(base)
    db = StateDatabase(path)
    try:
        current = db.get("device")
        if current is None:
            # A crash after schema creation but before import may finish here.
            # initialize_document still refuses a missing previously imported row;
            # StateDatabase refuses a missing marked, corrupt or future database.
            db.initialize_document("device", resolved_settings(base), str(source))
            current = db.get("device")
        if current is None:
            raise PersistenceError("dynamic device settings are missing")
        # Persisted values override legacy JSON defaults before validation, exactly
        # as load_production_config does. Bootstrap identity and paths still apply.
        production_config_from_value(overlay(base, current))
    except BaseException:
        db.close()
        raise
    return db


def initialize(path: Path) -> None:
    _initialize(read_bounded_json(path), path).close()


def manage(
    config: Path, action: str, *, source: Path | None = None, destination: Path | None = None
) -> dict:
    # Use the same bootstrap snapshot for initialization, validation and patching.
    base = read_bounded_json(config)
    db = _initialize(base, config)
    try:
        current = db.get("device")
        if action == "set":
            if source is None:
                raise ValueError("--input is required")
            patch = read_bounded_json(source)
            if not isinstance(patch, dict):
                raise ValueError("settings patch must be an object")

            def validated_update(current):
                merged = overlay(overlay(base, current), patch)
                recording = patch.get("recording")
                if isinstance(recording, dict) and "preset" in recording:
                    # Stored settings contain every resolved encoder field. Merging
                    # a new preset into them leaves the old values overriding it.
                    # An explicit preset starts a fresh recording profile; only
                    # overrides supplied in this same patch take precedence.
                    merged["recording"] = dict(recording)
                # Persist actual values, never a preset whose defaults can drift
                # with later firmware or bootstrap changes. Validation and history
                # remain inside the same serialized SQLite update transaction.
                return resolved_settings(merged)

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
            _export_json(destination, current)
            return {"export": str(destination)}
        return {"settings": current}
    finally:
        db.close()


def _export_json(destination: Path, value: object) -> None:
    """Publish a complete UTF-8 export without replacing an existing name."""

    def write_json(fd: int) -> None:
        with os.fdopen(fd, "w", encoding="utf-8", closefd=False) as stream:
            json.dump(value, stream, ensure_ascii=False, indent=2)
            stream.flush()
            os.fsync(fd)

    export_atomic_no_overwrite(
        destination,
        mode=0o600,
        temp_name=f".openaria-settings-{uuid.uuid4().hex}.tmp",
        writer=write_json,
        error_factory=lambda _err, message, _target: PersistenceError(message),
    )

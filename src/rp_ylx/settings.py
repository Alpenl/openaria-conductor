"""Validated dynamic settings; bootstrap identity, credentials and paths stay in JSON."""

from __future__ import annotations

import copy
import errno
import json
import os
import uuid
from collections.abc import Mapping
from contextlib import suppress
from dataclasses import asdict
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


def resolved_settings(bootstrap: Mapping[str, object]) -> dict:
    """Persist effective defaults as values so future firmware defaults cannot change a take."""
    from rp_ylx.daemon import production_config_from_value

    config = production_config_from_value(bootstrap)
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
    from rp_ylx.daemon import ProductionConfigError, production_config_from_value

    if not isinstance(base, dict):
        raise ProductionConfigError("生产配置顶层字段无效")
    path = settings_path(base)
    if not path.exists() and not path.with_suffix(".sqlite3.initialized").exists():
        # Reject an invalid first import before creating any database files.
        production_config_from_value(base)
    db = StateDatabase(path)
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
    return db


def initialize(path: Path) -> None:
    _initialize(json.loads(path.read_bytes()), path)


def manage(
    config: Path, action: str, *, source: Path | None = None, destination: Path | None = None
) -> dict:
    # Use the same bootstrap snapshot for initialization, validation and patching.
    base = json.loads(config.read_bytes())
    db = _initialize(base, config)
    current = db.get("device")
    if action == "set":
        if source is None:
            raise ValueError("--input is required")
        patch = json.loads(source.read_bytes())
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


def _export_json(destination: Path, value: object) -> None:
    """Publish a complete UTF-8 export without replacing an existing name."""
    parent_fd = os.open(destination.parent, os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC)
    fd = None
    temporary = None
    identity = None

    def same_file() -> bool:
        if temporary is None or identity is None:
            return False
        try:
            current = os.stat(temporary, dir_fd=parent_fd, follow_symlinks=False)
        except FileNotFoundError:
            return False
        return (current.st_dev, current.st_ino) == identity

    def cleanup() -> None:
        if same_file():
            os.unlink(temporary, dir_fd=parent_fd)

    def check_parent() -> None:
        parent = os.fstat(parent_fd)
        current = destination.parent.stat()
        if (parent.st_dev, parent.st_ino) != (current.st_dev, current.st_ino):
            raise PersistenceError("export parent directory changed during write")

    try:
        try:
            os.stat(destination.name, dir_fd=parent_fd, follow_symlinks=False)
        except FileNotFoundError:
            pass
        else:
            raise FileExistsError(errno.EEXIST, "export destination already exists", destination)
        temporary = ".openaria-settings-" + uuid.uuid4().hex + ".tmp"
        fd = os.open(
            temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC,
            0o600, dir_fd=parent_fd,
        )
        info = os.fstat(fd)
        identity = (info.st_dev, info.st_ino)
        # Keep fd ownership even if stream construction, entry, flush or close
        # raises. The final hard link refers to this exact open file on Linux.
        with os.fdopen(fd, "w", encoding="utf-8", closefd=False) as stream:
            json.dump(value, stream, ensure_ascii=False, indent=2)
            stream.flush()
            os.fsync(fd)
        check_parent()
        if not same_file():
            raise PersistenceError("export staging file changed during write")
        # linkat follows our own fd, not a replaceable staging pathname, and
        # fails atomically if another exporter creates the destination first.
        os.link(
            f"/proc/self/fd/{fd}", destination.name,
            dst_dir_fd=parent_fd, follow_symlinks=True,
        )
        cleanup()
        temporary = None
        # After publication, a failed durability barrier must not delete the
        # complete final file or allow a retry to overwrite it.
        os.fsync(parent_fd)
        check_parent()
        published = os.stat(destination.name, dir_fd=parent_fd, follow_symlinks=False)
        if (published.st_dev, published.st_ino) != identity:
            raise PersistenceError("export destination changed during publication")
    except BaseException:
        with suppress(OSError):
            cleanup()
        raise
    finally:
        try:
            if fd is not None:
                os.close(fd)
        finally:
            os.close(parent_fd)

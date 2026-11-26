"""On-device Miro-Ego configuration backup and fresh-image profile restore.

Runs on RDK X5 using stdlib Python 3.10+ only during SSH backup or one-shot
first-boot recovery. Never exports recordings, logs, or runtime caches.
"""

from __future__ import annotations

import base64
import contextlib
import hashlib
import json
import os
import sys
import uuid
from pathlib import Path, PurePosixPath
from typing import Any

SCHEMA = "miro-ego.factory-profile.v1"
DEVICE_CONFIG_SCHEMA = "miro-ego.device-config.v1"
VOICE_SETTINGS_SCHEMA = "miro-ego.voice-settings.v1"
DEFAULT_VOICE_VOLUME = 80
DEFAULT_VOICE_ENABLED = False

FILES = frozenset(
    {
        "etc/machine-id",
        "etc/hostname",
        "etc/hosts",
        "etc/miro-ego/device.json",
        "etc/miro-ego/settings.json",
        "etc/miro-ego/button.json",
        "etc/miro-ego/network.json",
        "etc/default/miro-ego-wifi-watchdog",
        "var/lib/alsa/asound.state",
        "var/lib/miro-ego/network-state.json",
        "var/lib/miro-ego/voice.json",
        "var/lib/miro-ego-network/rescue.json",
    }
)
DIRECTORIES = frozenset({"etc/netplan", "etc/NetworkManager/system-connections"})
MAX_BYTES = 8 * 1024 * 1024
MAX_FILES = 128

_PUBLIC_FILES = frozenset(
    {
        "etc/machine-id",
        "etc/hostname",
        "etc/hosts",
        "etc/miro-ego/button.json",
        "etc/default/miro-ego-wifi-watchdog",
    }
)


def allowed(name: str) -> bool:
    """Return True iff *name* is a safe relative path within the config allowlist."""
    if not name or "\\" in name:
        return False
    p = PurePosixPath(name)
    return (
        str(p) == name
        and not p.is_absolute()
        and ".." not in p.parts
        and (name in FILES or str(p.parent) in DIRECTORIES)
    )


def is_rdk_x5_model(raw: bytes) -> bool:
    """Verify the device-tree model string matches D-Robotics RDK X5 V1.0."""
    text = raw.decode("utf-8", errors="replace")
    return " ".join(text.replace("\x00", "").split()).casefold() == "d-robotics rdk x5 v1.0"


def hardware() -> str:
    """Compute the physical board SHA-256 binding from device-tree serial number."""
    model = Path("/proc/device-tree/model").read_bytes()
    serial = Path("/proc/device-tree/serial-number").read_bytes().rstrip(b"\x00")
    if not is_rdk_x5_model(model) or not serial or set(serial) == {48}:
        raise RuntimeError("Unsupported or unidentified board")
    return hashlib.sha256(serial).hexdigest()


def require_idle(capture: Any) -> None:
    """Reject configuration backup if capture or safe-swap is currently active."""
    if capture is None:
        return
    if (
        not isinstance(capture, dict)
        or capture.get("active_session")
        or capture.get("pending_safe_swap")
        or capture.get("phase") in {"starting", "recording", "stopping", "deleting"}
    ):
        raise RuntimeError("Device must be idle without active capture before backup")


def _read_optional_json(path: Path) -> Any:
    if not path.exists():
        return None
    if path.is_symlink() or not path.is_file():
        raise RuntimeError(f"Unsafe state file: {path}")
    return json.loads(path.read_bytes())


def backup() -> dict[str, Any]:
    """Acquire exclusive maintenance lock and capture an allowlisted device profile."""
    import fcntl

    lock_path = Path("/run/miro-ego/maintenance.lock")
    with lock_path.open("rb") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        return backup_locked()


def backup_locked() -> dict[str, Any]:
    """Build a validated ``miro-ego.factory-profile.v1`` snapshot while locked."""
    board = hardware()
    config_path = Path("/etc/miro-ego/device.json")
    if config_path.is_symlink() or not config_path.is_file():
        raise RuntimeError("Device configuration is missing or unsafe")
    config = json.loads(config_path.read_bytes())
    if config.get("state_root") != "/var/lib/miro-ego":
        raise RuntimeError("Custom state root is not supported")
    if config.get("security", {}).get("profile") != "lab":
        raise RuntimeError("This recovery version supports HTTP lab profile only")

    capture = _read_optional_json(Path("/var/lib/miro-ego/capture-state.json"))
    require_idle(capture)

    names = set(FILES)
    for directory in DIRECTORIES:
        dir_path = Path("/", directory)
        if dir_path.is_dir() and not dir_path.is_symlink():
            names.update(str(p.relative_to("/")) for p in dir_path.glob("*"))

    files: dict[str, dict[str, str]] = {}
    total = 0
    for name in sorted(names):
        path = Path("/", name)
        if not path.exists():
            continue
        if not allowed(name) or path.is_symlink() or not path.is_file():
            raise RuntimeError("Unsupported configuration file")
        raw = path.read_bytes()
        total += len(raw)
        if total > MAX_BYTES:
            raise RuntimeError("Configuration exceeds limit")
        files[name] = {
            "data": base64.b64encode(raw).decode("ascii"),
            "sha256": hashlib.sha256(raw).hexdigest(),
        }

    voice_raw = _read_optional_json(Path("/var/lib/miro-ego/voice.json"))
    if isinstance(voice_raw, dict) and "volume" in voice_raw:
        enabled = voice_raw.get("enabled", DEFAULT_VOICE_ENABLED)
        if type(enabled) is not bool:
            raise RuntimeError("Invalid voice enabled preference")
        voice: dict[str, Any] = {"enabled": enabled, "volume": voice_raw["volume"]}
    else:
        voice = {"enabled": DEFAULT_VOICE_ENABLED, "volume": DEFAULT_VOICE_VOLUME}

    result: dict[str, Any] = {
        "schema": SCHEMA,
        "board": board,
        "files": files,
        "voice": voice,
    }
    validate(result)
    return result


def voice_settings_payload(profile: dict[str, Any]) -> dict[str, Any]:
    """Build a ``miro-ego.voice-settings.v1`` file payload matching ``miro-ego-hw``."""
    voice = profile.get("voice")
    if voice is None:
        return {
            "schema": VOICE_SETTINGS_SCHEMA,
            "enabled": DEFAULT_VOICE_ENABLED,
            "volume": DEFAULT_VOICE_VOLUME,
        }
    enabled = voice.get("enabled", DEFAULT_VOICE_ENABLED)
    if type(enabled) is not bool:
        raise RuntimeError("Invalid voice preference")
    volume = voice.get("volume")
    if type(volume) is not int or not 0 <= volume <= 100:
        raise RuntimeError("Invalid voice preference")
    return {
        "schema": VOICE_SETTINGS_SCHEMA,
        "enabled": enabled,
        "volume": volume,
    }


def validate(profile: Any) -> dict[str, bytes]:
    """Validate a Miro-Ego factory profile and return decoded ``{relpath: bytes}``."""
    if not isinstance(profile, dict):
        raise RuntimeError("Unsupported profile")
    if profile.get("schema") != SCHEMA or not isinstance(profile.get("files"), dict):
        raise RuntimeError("Unsupported profile")
    if len(profile["files"]) > MAX_FILES:
        raise RuntimeError("Too many configuration files")

    voice = profile.get("voice", {"enabled": DEFAULT_VOICE_ENABLED, "volume": DEFAULT_VOICE_VOLUME})
    if (
        not isinstance(voice, dict)
        or "volume" not in voice
        or not set(voice).issubset({"volume", "enabled"})
        or type(voice["volume"]) is not int
        or not 0 <= voice["volume"] <= 100
        or ("enabled" in voice and type(voice["enabled"]) is not bool)
    ):
        raise RuntimeError("Invalid voice preference")

    decoded: dict[str, bytes] = {}
    total = 0
    for name, entry in profile["files"].items():
        if not isinstance(name, str) or not allowed(name):
            raise RuntimeError("Profile contains a forbidden path")
        if not isinstance(entry, dict) or not isinstance(entry.get("data"), str):
            raise RuntimeError("Invalid profile entry")
        raw = base64.b64decode(entry["data"], validate=True)
        total += len(raw)
        if total > MAX_BYTES or hashlib.sha256(raw).hexdigest() != entry.get("sha256"):
            raise RuntimeError("Profile data check failed")
        decoded[name] = raw

    required = {"etc/miro-ego/device.json", "etc/machine-id"}
    if not required.issubset(decoded):
        raise RuntimeError("Profile is missing device identity")

    config = json.loads(decoded["etc/miro-ego/device.json"])
    if (
        not isinstance(config, dict)
        or config.get("state_root") != "/var/lib/miro-ego"
        or config.get("security", {}).get("profile") != "lab"
    ):
        raise RuntimeError("Unsupported bootstrap configuration")
    if "schema" in config and config["schema"] != DEVICE_CONFIG_SCHEMA:
        raise RuntimeError("Unsupported device configuration schema")

    return decoded


def require_fresh() -> None:
    """Refuse restoration if the root filesystem or ``/data`` is already initialized."""
    for name in ("/opt/miro-ego", "/var/lib/miro-ego", "/etc/miro-ego/device.json"):
        if Path(name).exists():
            raise RuntimeError(
                "Destination is not fresh; refusing to replace existing installation"
            )
    data_dir = Path("/data")
    if data_dir.exists() and any(data_dir.iterdir()):
        raise RuntimeError("Data directory is not empty")


def write_files(files: dict[str, bytes]) -> None:
    """Atomically write validated config files while refusing symlink redirects."""
    for name in files:
        if not allowed(name):
            raise RuntimeError("Forbidden destination path")
        path = Path("/", name)
        if any(p.is_symlink() for p in (path, *path.parents)):
            raise RuntimeError("Unsafe configuration destination")

    for name, raw in files.items():
        path = Path("/", name)
        missing_dirs: list[Path] = []
        cursor = path.parent
        while not cursor.exists():
            missing_dirs.append(cursor)
            if cursor.parent == cursor:
                break
            cursor = cursor.parent
        path.parent.mkdir(parents=True, exist_ok=True)
        if name.startswith("var/lib/miro-ego-network"):
            dir_mode = 0o700
        elif name.startswith(("etc/miro-ego/", "var/lib/miro-ego/")):
            dir_mode = 0o750
        else:
            dir_mode = 0o755
        for created_dir in reversed(missing_dirs):
            created_dir.chmod(dir_mode)

        temporary = path.with_name(path.name + ".miro-ego-restore-" + uuid.uuid4().hex)
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | getattr(os, "O_CLOEXEC", 0)
        try:
            fd = os.open(temporary, flags, 0o600)
            with os.fdopen(fd, "wb") as out:
                out.write(raw)
                out.flush()
                os.fsync(out.fileno())
            os.replace(temporary, path)
        finally:
            with contextlib.suppress(OSError):
                temporary.unlink(missing_ok=True)

        if name in _PUBLIC_FILES or name.endswith(".conf"):
            path.chmod(0o644)
        else:
            path.chmod(0o600)

        dir_fd = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(dir_fd)
        finally:
            os.close(dir_fd)


def restore(profile: dict[str, Any]) -> dict[str, Any]:
    """Validate board identity + freshness and write profile files to disk."""
    files = validate(profile)
    if hardware() != profile.get("board"):
        raise RuntimeError("Profile belongs to a different physical board")
    require_fresh()
    write_files(files)
    return {
        "restored": True,
        "files": len(files),
        "network_activation": "reboot after install",
    }


def main() -> None:
    if os.geteuid() != 0:
        raise RuntimeError("Root privileges required")
    if len(sys.argv) < 2:
        raise RuntimeError("Missing device operation")
    if sys.argv[1] == "backup":
        result = backup()
    elif sys.argv[1] == "restore":
        result = restore(json.load(sys.stdin))
    else:
        raise RuntimeError("Unknown device operation")
    print(json.dumps(result))


if __name__ == "__main__":
    try:
        main()
    except Exception:
        # Configuration can contain secrets; never serialize exception payloads.
        print(
            "Configuration operation failed; check board, idle state "
            "and fresh-image preconditions.",
            file=sys.stderr,
        )
        sys.exit(1)

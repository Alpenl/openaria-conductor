"""Runs on the RDK using Python 3.10+. No video or historical databases exported."""

from __future__ import annotations

import base64
import hashlib
import json
import os
import sqlite3
import sys
from pathlib import Path, PurePosixPath

SCHEMA = "openaria.factory-profile.v1"
FILES = {
    "etc/machine-id",
    "etc/hostname",
    "etc/hosts",
    "etc/rp-ylx/device.json",
    "etc/rp-ylx/recording-button.json",
    "etc/systemd/system/rp-ylx.service.d/60-voice-feedback.conf",
    "var/lib/alsa/asound.state",
    "var/lib/rp-ylx-network/rescue.json",
}
DIRECTORIES = {"etc/netplan", "etc/NetworkManager/system-connections"}
MAX_BYTES = 8 * 1024 * 1024


def allowed(name):
    p = PurePosixPath(name)
    return (
        str(p) == name
        and not p.is_absolute()
        and ".." not in p.parts
        and (name in FILES or str(p.parent) in DIRECTORIES)
    )


def hardware():
    model = Path("/proc/device-tree/model").read_bytes().rstrip(b"\0").decode()
    serial = Path("/proc/device-tree/serial-number").read_bytes().rstrip(b"\0")
    if model != "D-Robotics RDK X5 V1.0" or not serial or set(serial) == {48}:
        raise RuntimeError("Unsupported or unidentified board")
    return hashlib.sha256(serial).hexdigest()


def document(database, name, default=None):
    with sqlite3.connect("file:" + str(database) + "?mode=ro", uri=True) as db:
        row = db.execute("SELECT body FROM documents WHERE name=?", (name,)).fetchone()
    if row is None:
        if default is not None:
            return default
        raise RuntimeError("Required settings/state document missing")
    return json.loads(row[0])


def backup():
    import fcntl

    with Path("/run/rp-ylx/maintenance.lock").open("rb") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        return backup_locked()


def backup_locked():
    board = hardware()
    config = json.loads(Path("/etc/rp-ylx/device.json").read_bytes())
    if config.get("state_root") != "/var/lib/rp-ylx":
        raise RuntimeError("Custom state root is not supported")
    if config.get("security", {}).get("profile") != "lab":
        raise RuntimeError("This recovery version supports HTTP lab profile only")
    capture = document("/var/lib/rp-ylx/state.sqlite3", "capture")
    require_idle(capture)
    current = document("/var/lib/rp-ylx/settings.sqlite3", "device")
    for group, values in current.items():
        if group not in {"camera", "audio", "recording", "storage"} or not isinstance(values, dict):
            raise RuntimeError("Unknown settings group")
        config.setdefault(group, {}).update(values)
    names = set(FILES)
    for directory in DIRECTORIES:
        names.update(str(p.relative_to("/")) for p in Path("/", directory).glob("*"))
    files = {}
    total = 0
    for name in sorted(names):
        path = Path("/", name)
        if not path.exists():
            continue
        if not allowed(name) or path.is_symlink() or not path.is_file():
            raise RuntimeError("Unsupported configuration file")
        raw = json.dumps(config).encode() if name == "etc/rp-ylx/device.json" else path.read_bytes()
        total += len(raw)
        if total > MAX_BYTES:
            raise RuntimeError("Configuration exceeds limit")
        files[name] = {
            "data": base64.b64encode(raw).decode(),
            "sha256": hashlib.sha256(raw).hexdigest(),
        }
    voice = document("/var/lib/rp-ylx/state.sqlite3", "voice_feedback", {"volume": 100})
    result = {"schema": SCHEMA, "board": board, "files": files, "voice": voice}
    validate(result)
    return result


def require_idle(capture):
    # Retained entries are historical failed recordings, not active capture.
    # The caller holds the exclusive maintenance lease. They remain on the old
    # card and are never included in this configuration-only backup.
    if (
        not isinstance(capture, dict)
        or capture.get("active_session")
        or capture.get("pending_safe_swap")
    ):
        raise RuntimeError("Device must be idle without a pending safe swap before backup")


def validate(profile):
    if profile.get("schema") != SCHEMA or not isinstance(profile.get("files"), dict):
        raise RuntimeError("Unsupported profile")
    if len(profile["files"]) > 128:
        raise RuntimeError("Too many configuration files")
    voice = profile.get("voice", {"volume": 100})
    if (
        not isinstance(voice, dict)
        or set(voice) != {"volume"}
        or type(voice["volume"]) is not int
        or not 0 <= voice["volume"] <= 100
    ):
        raise RuntimeError("Invalid voice preference")
    decoded = {}
    total = 0
    for name, entry in profile["files"].items():
        if not allowed(name):
            raise RuntimeError("Profile contains a forbidden path")
        raw = base64.b64decode(entry["data"], validate=True)
        total += len(raw)
        if total > MAX_BYTES or hashlib.sha256(raw).hexdigest() != entry["sha256"]:
            raise RuntimeError("Profile data check failed")
        decoded[name] = raw
    required = {"etc/rp-ylx/device.json", "etc/machine-id"}
    if not required.issubset(decoded):
        raise RuntimeError("Profile is missing device identity")
    config = json.loads(decoded["etc/rp-ylx/device.json"])
    if (
        config.get("state_root") != "/var/lib/rp-ylx"
        or config.get("security", {}).get("profile") != "lab"
    ):
        raise RuntimeError("Unsupported bootstrap configuration")
    return decoded


def require_fresh():
    # Restore only onto a fresh vendor image, never initialized production state.
    for name in ("/opt/rp-ylx", "/var/lib/rp-ylx", "/etc/rp-ylx/device.json"):
        if Path(name).exists():
            raise RuntimeError(
                "Destination is not fresh; refusing to replace existing installation"
            )
    if Path("/data").exists() and any(Path("/data").iterdir()):
        raise RuntimeError("Data directory is not empty")


def write_files(files):
    # Prepare all paths before writing, refusing symlink redirects.
    for name in files:
        path = Path("/", name)
        if any(p.is_symlink() for p in (path, *path.parents)):
            raise RuntimeError("Unsafe configuration destination")
    for name, raw in files.items():
        path = Path("/", name)
        path.parent.mkdir(parents=True, exist_ok=True)
        import uuid

        temporary = path.with_name(path.name + ".openaria-restore-" + uuid.uuid4().hex)
        fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
        with os.fdopen(fd, "wb") as out:
            out.write(raw)
            out.flush()
            os.fsync(out.fileno())
        os.replace(temporary, path)
        if name in {"etc/machine-id", "etc/hostname", "etc/hosts"} or name.endswith(".conf"):
            path.chmod(0o644)
        fd = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)


def restore(profile):
    files = validate(profile)
    if hardware() != profile["board"]:
        raise RuntimeError("Profile belongs to a different physical board")
    require_fresh()
    write_files(files)
    # Do not reload network here: the installer still needs the initial connection.
    return {"restored": True, "files": len(files), "network_activation": "reboot after install"}


def main():
    if os.geteuid() != 0:
        raise RuntimeError("Root privileges required")
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
        # Configuration can contain secrets. Do not serialize exception payloads.
        print(
            "Configuration operation failed; check board, idle state "
            "and fresh-image preconditions.",
            file=sys.stderr,
        )
        sys.exit(1)

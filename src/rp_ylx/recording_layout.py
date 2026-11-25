"""Portable recording names; UUIDs remain the manifest/API identity.

Writers keep their existing UUID transaction directories. Only sealed recordings
are moved, using a no-replace rename on the same volume. No database is required
to discover either layout.
"""

from __future__ import annotations

import ctypes
import errno
import json
import os
import re
import stat
from collections.abc import Iterator, Mapping
from datetime import datetime
from pathlib import Path

from rp_ylx.manifest_limits import MAX_MANIFEST_BYTES

DAY = re.compile(r"^\d{4}-\d{2}-\d{2}$")
TIME = re.compile(r"^\d{2}-\d{2}-\d{2}(?:_\d+)?$")
UUID = re.compile(r"^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$")
FLAGS = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC


def open_relative_directory(root_fd: int, relative: Path) -> int:
    """Pin every component; never follow an intermediate directory symlink."""
    if (
        relative.is_absolute()
        or not relative.parts
        or any(p in {".", ".."} for p in relative.parts)
    ):
        raise OSError("invalid recording path")
    fd = os.dup(root_fd)
    try:
        for part in relative.parts:
            child = os.open(part, FLAGS, dir_fd=fd)
            if os.fstat(child).st_dev != os.fstat(root_fd).st_dev:
                os.close(child)
                raise OSError("recording crosses volume boundary")
            os.close(fd)
            fd = child
        return fd
    except BaseException:
        os.close(fd)
        raise


def _manifest_id(fd: int) -> str:
    manifest_fd = os.open("manifest.json", os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=fd)
    with os.fdopen(manifest_fd, "rb") as stream:
        info = os.fstat(stream.fileno())
        if not stat.S_ISREG(info.st_mode) or info.st_size > MAX_MANIFEST_BYTES:
            raise ValueError("invalid manifest")
        value = json.loads(stream.read(MAX_MANIFEST_BYTES + 1))
    identity = value.get("session_id") if isinstance(value, dict) else None
    if not isinstance(identity, str) or UUID.fullmatch(identity) is None:
        raise ValueError("invalid recording identity")
    return identity


def iter_recordings(root: Path, *, root_fd: int | None = None) -> Iterator[tuple[Path, str]]:
    """Scan only the legacy level and the new date/time level, not media trees."""
    fd = os.open(root, FLAGS) if root_fd is None else os.dup(root_fd)
    try:
        for name in sorted(os.listdir(fd)):
            if name.endswith(".partial") or name.startswith("."):
                continue
            info = os.stat(name, dir_fd=fd, follow_symlinks=False)
            if stat.S_ISLNK(info.st_mode) and UUID.fullmatch(name):
                # Preserve the identity for diagnostics/deletion preflight;
                # readers still reject opening the linked directory.
                yield root / name, name
                continue
            if not stat.S_ISDIR(info.st_mode):
                continue
            if DAY.fullmatch(name) is None and name != "录制时间未知":
                yield root / name, name
                continue
            day = open_relative_directory(fd, Path(name))
            try:
                for child in sorted(os.listdir(day)):
                    if TIME.fullmatch(child) is None and not child.startswith("录制_"):
                        continue
                    try:
                        recording = open_relative_directory(day, Path(child))
                        try:
                            identity = _manifest_id(recording)
                        finally:
                            os.close(recording)
                    except (OSError, ValueError, RecursionError):
                        # Keep malformed recordings visible to catalog diagnostics.
                        identity = child
                    yield root / name / child, identity
            finally:
                os.close(day)
    finally:
        os.close(fd)


def resolve_recording(root: Path, session_id: str, *, root_fd: int | None = None) -> Path:
    if UUID.fullmatch(session_id) is None:
        raise FileNotFoundError(session_id)
    matches = [
        path for path, identity in iter_recordings(root, root_fd=root_fd) if identity == session_id
    ]
    if len(matches) > 1:
        raise OSError("ambiguous recording identity")
    return matches[0] if matches else root / session_id


def manifest_identity(root: Path, manifest: Mapping[str, object]) -> object:
    if (DAY.fullmatch(root.parent.name) and TIME.fullmatch(root.name)) or (
        root.parent.name == "录制时间未知" and re.fullmatch(r"录制_\d+", root.name)
    ):
        return manifest.get("session_id") if isinstance(manifest, Mapping) else None
    return root.name


def publish_readable(path: Path, manifest: Mapping[str, object]) -> Path:
    """Move a newly sealed UUID directory; preserve all old or occupied paths."""
    if path.name != manifest.get("session_id"):
        return path
    started = manifest.get("time", {}).get("started_at")
    try:
        stamp = datetime.fromisoformat(started.replace("Z", "+00:00"))
        if stamp.tzinfo is None or not 2020 <= stamp.year < 2100:
            raise ValueError("unknown recording date")
        day, name = stamp.strftime("%Y-%m-%d"), stamp.strftime("%H-%M-%S")
    except (ValueError, TypeError, AttributeError):
        day, name = "录制时间未知", "录制"
    root_fd = os.open(path.parent, FLAGS)
    try:
        try:
            os.mkdir(day, mode=0o750, dir_fd=root_fd)
            os.fsync(root_fd)
        except FileExistsError:
            pass
        day_fd = open_relative_directory(root_fd, Path(day))
        try:
            # Linux renameat2(RENAME_NOREPLACE) also protects empty directories
            # and concurrent publications; never overwrite a user's destination.
            rename = ctypes.CDLL(None, use_errno=True).renameat2
            rename.argtypes = [
                ctypes.c_int,
                ctypes.c_char_p,
                ctypes.c_int,
                ctypes.c_char_p,
                ctypes.c_uint,
            ]
            rename.restype = ctypes.c_int
            for ordinal in range(1, 10001):
                target = name if ordinal == 1 and name != "录制" else f"{name}_{ordinal:02d}"
                if rename(root_fd, os.fsencode(path.name), day_fd, os.fsencode(target), 1) == 0:
                    os.fsync(day_fd)
                    os.fsync(root_fd)
                    return path.parent / day / target
                error = ctypes.get_errno()
                if error != errno.EEXIST:
                    raise OSError(error, os.strerror(error))
            raise OSError("too many recordings at the same time")
        finally:
            os.close(day_fd)
    finally:
        os.close(root_fd)

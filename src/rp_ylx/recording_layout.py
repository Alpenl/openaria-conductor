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
from collections.abc import Callable, Iterator, Mapping
from datetime import datetime
from pathlib import Path

from rp_ylx.manifest_limits import MAX_MANIFEST_BYTES

DAY = re.compile(r"^\d{4}-\d{2}-\d{2}$")
TIME = re.compile(r"^\d{2}-\d{2}-\d{2}(?:_\d+)?$")
UNKNOWN_TIME_CHILD = re.compile(r"^录制_\d+$")
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
    manifest_fd = os.open(
        "manifest.json",
        os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK | os.O_CLOEXEC,
        dir_fd=fd,
    )
    try:
        stream = os.fdopen(manifest_fd, "rb")
    except BaseException:
        os.close(manifest_fd)
        raise
    with stream:
        info = os.fstat(stream.fileno())
        if not stat.S_ISREG(info.st_mode) or info.st_size > MAX_MANIFEST_BYTES:
            raise ValueError("invalid manifest")
        value = json.loads(stream.read(MAX_MANIFEST_BYTES + 1))
    identity = value.get("session_id") if isinstance(value, dict) else None
    if not isinstance(identity, str) or UUID.fullmatch(identity) is None:
        raise ValueError("invalid recording identity")
    return identity


manifest_id = _manifest_id


def iter_recording_entries(
    root: Path,
    *,
    root_fd: int | None = None,
    lookup_cached_id: Callable[[str], str | None] | None = None,
) -> Iterator[tuple[Path, str, bool]]:
    """Scan only the legacy level and the new date/time level, not media trees."""
    fd = os.open(root, FLAGS) if root_fd is None else os.dup(root_fd)
    try:
        for name in sorted(os.listdir(fd)):
            if name.endswith(".partial") or name.startswith("."):
                continue
            try:
                info = os.stat(name, dir_fd=fd, follow_symlinks=False)
            except FileNotFoundError:
                continue
            if stat.S_ISLNK(info.st_mode) and UUID.fullmatch(name):
                # Preserve the identity for diagnostics/deletion preflight;
                # readers still reject opening the linked directory.
                yield root / name, name, False
                continue
            if not stat.S_ISDIR(info.st_mode):
                continue
            if DAY.fullmatch(name) is None and name != "录制时间未知":
                yield root / name, name, False
                continue
            try:
                day = open_relative_directory(fd, Path(name))
            except FileNotFoundError:
                continue
            try:
                for child in sorted(os.listdir(day)):
                    if (
                        TIME.fullmatch(child) is None
                        and UNKNOWN_TIME_CHILD.fullmatch(child) is None
                    ):
                        continue
                    if lookup_cached_id is not None:
                        cached_sid = lookup_cached_id(f"{name}/{child}")
                        if cached_sid is not None:
                            yield root / name / child, cached_sid, True
                            continue
                    try:
                        recording = open_relative_directory(day, Path(child))
                    except FileNotFoundError:
                        continue
                    try:
                        identity = _manifest_id(recording)
                    except (OSError, ValueError, RecursionError):
                        # Keep malformed recordings visible to catalog diagnostics.
                        identity = child
                    finally:
                        os.close(recording)
                    yield root / name / child, identity, False
            finally:
                os.close(day)
    finally:
        os.close(fd)


def iter_recordings(root: Path, *, root_fd: int | None = None) -> Iterator[tuple[Path, str]]:
    """Scan only the legacy level and the new date/time level, not media trees."""
    for path, identity, _ in iter_recording_entries(root, root_fd=root_fd):
        yield path, identity


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
        root.parent.name == "录制时间未知" and UNKNOWN_TIME_CHILD.fullmatch(root.name) is not None
    ):
        return manifest.get("session_id") if isinstance(manifest, Mapping) else None
    return root.name


def publish_readable(path: Path, manifest: Mapping[str, object]) -> Path:
    """Move a newly sealed UUID directory; preserve all old or occupied paths."""
    if path.name != manifest.get("session_id"):
        return path
    time_info = manifest.get("time")
    started = time_info.get("started_at") if isinstance(time_info, Mapping) else None
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

"""Canonical advisory flock helpers immune to restrictive process umasks."""

from __future__ import annotations

import fcntl
import grp
import os
import stat
from collections.abc import Iterator
from contextlib import contextmanager, suppress
from pathlib import Path

from rp_ylx.core.fs import O_CLOEXEC, O_NOFOLLOW, O_NONBLOCK

DEFAULT_LOCK_MODE = 0o660
DEFAULT_LOCK_GROUP = "rp-ylx"

__all__ = ["DEFAULT_LOCK_GROUP", "DEFAULT_LOCK_MODE", "acquire_flock"]


def _resolve_group_gid(group_name: str | None) -> int | None:
    if group_name is None or not (hasattr(os, "geteuid") and os.geteuid() == 0):
        return None
    try:
        return grp.getgrnam(group_name).gr_gid
    except KeyError:
        return None


def _ensure_lock_parent(parent: Path, *, gid: int | None) -> None:
    missing_parents: list[Path] = []
    probe = parent
    while probe != probe.parent and not probe.exists():
        missing_parents.append(probe)
        probe = probe.parent
    parent.mkdir(parents=True, exist_ok=True, mode=0o770)
    for created_dir in reversed(missing_parents):
        with suppress(OSError):
            os.chmod(created_dir, 0o770)
        if gid is not None:
            with suppress(OSError):
                os.chown(created_dir, -1, gid, follow_symlinks=False)


def _apply_lock_ownership_and_mode(fd: int, *, mode: int, gid: int | None) -> None:
    metadata = os.fstat(fd)
    if not stat.S_ISREG(metadata.st_mode):
        raise ValueError("lock path must be a regular file")
    is_root = hasattr(os, "geteuid") and os.geteuid() == 0
    if not hasattr(os, "geteuid") or is_root or metadata.st_uid == os.geteuid():
        os.fchmod(fd, mode)
    if gid is not None and metadata.st_gid != gid:
        os.fchown(fd, -1, gid)


@contextmanager
def acquire_flock(
    path: Path,
    *,
    mode: int = DEFAULT_LOCK_MODE,
    group_name: str | None = DEFAULT_LOCK_GROUP,
    nonblocking: bool = False,
    shared: bool = False,
) -> Iterator[int]:
    """Acquire an advisory flock on path, preserving mode and group across umask."""
    target = Path(path)
    gid = _resolve_group_gid(group_name)
    _ensure_lock_parent(target.parent, gid=gid)
    flags = O_NOFOLLOW | O_NONBLOCK | O_CLOEXEC
    if shared:
        try:
            fd = os.open(target, os.O_RDONLY | flags)
        except FileNotFoundError:
            fd = os.open(target, os.O_CREAT | os.O_RDWR | flags, mode)
    else:
        fd = os.open(target, os.O_CREAT | os.O_RDWR | flags, mode)
    try:
        _apply_lock_ownership_and_mode(fd, mode=mode, gid=gid)
        operation = fcntl.LOCK_SH if shared else fcntl.LOCK_EX
        if nonblocking:
            operation |= fcntl.LOCK_NB
        fcntl.flock(fd, operation)
    except BaseException:
        os.close(fd)
        raise
    try:
        yield fd
    finally:
        try:
            with suppress(OSError):
                fcntl.flock(fd, fcntl.LOCK_UN)
        finally:
            os.close(fd)

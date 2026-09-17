"""Reuse independent reads of closed files, only while their exact identity holds.

This cache is private to one live recorder. It is never loaded from a manifest,
checkpoint or disk. A changed ctime, link count, inode or digest forces a new
full read, including changes that restore the original mtime.
"""

from __future__ import annotations

import os
import stat
from dataclasses import dataclass
from pathlib import Path


def identity(metadata: os.stat_result) -> tuple[int, ...]:
    if not stat.S_ISREG(metadata.st_mode) or metadata.st_nlink != 1:
        raise ValueError("verified artifact must be an exclusively linked regular file")
    return (
        metadata.st_dev,
        metadata.st_ino,
        metadata.st_mode,
        metadata.st_nlink,
        metadata.st_size,
        metadata.st_mtime_ns,
        metadata.st_ctime_ns,
    )


@dataclass(frozen=True)
class VerifiedArtifact:
    identity: tuple[int, ...]
    sha256: str

    def matches(self, metadata: os.stat_result, size: int, digest: str) -> bool:
        return (
            metadata.st_size == size
            and self.sha256 == digest
            and identity(metadata) == self.identity
        )

    def unchanged(self, path: Path) -> bool:
        return identity(path.stat(follow_symlinks=False)) == self.identity

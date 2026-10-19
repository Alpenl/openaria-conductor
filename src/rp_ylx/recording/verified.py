"""Remember independently read digests without treating stat as content proof.

Coarse filesystem timestamps can remain identical across an in-place write.
Identity checks reject replacements cheaply; admitting content still needs a
fresh read, even for a private, in-memory verification record.
"""

from __future__ import annotations

import hashlib
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
        """Compare descriptor metadata only; this does not verify current bytes."""
        return (
            metadata.st_size == size
            and self.sha256 == digest
            and identity(metadata) == self.identity
        )

    def unchanged(self, path: Path) -> bool:
        if identity(path.stat(follow_symlinks=False)) != self.identity:
            return False
        descriptor = os.open(path, os.O_RDONLY | os.O_CLOEXEC | getattr(os, "O_NOFOLLOW", 0))
        with os.fdopen(descriptor, "rb") as source:
            if identity(os.fstat(source.fileno())) != self.identity:
                return False
            digest = hashlib.sha256()
            for block in iter(lambda: source.read(1024 * 1024), b""):
                digest.update(block)
            return (
                digest.hexdigest() == self.sha256
                and identity(os.fstat(source.fileno())) == self.identity
                and identity(path.stat(follow_symlinks=False)) == self.identity
            )

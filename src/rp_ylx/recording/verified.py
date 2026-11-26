"""Remember independently read digests without treating stat as content proof.

Coarse filesystem timestamps can remain identical across an in-place write.
Identity checks reject replacements cheaply; admitting content still needs a
fresh read, even for a private, in-memory verification record.
"""

from __future__ import annotations

import hashlib
import os
from dataclasses import dataclass
from pathlib import Path

from rp_ylx.recording.fs_safety import file_identity, open_regular


def identity(metadata: os.stat_result, *, include_inode: bool = True) -> tuple[int, ...]:
    return file_identity(metadata, include_inode=include_inode)


@dataclass(frozen=True)
class VerifiedArtifact:
    identity: tuple[int, ...]
    sha256: str
    include_inode: bool = True

    def _stat_identity(self, metadata: os.stat_result) -> tuple[int, ...]:
        if self.include_inode:
            return identity(metadata)
        return identity(metadata, include_inode=False)

    def matches(self, metadata: os.stat_result, size: int, digest: str) -> bool:
        """Compare descriptor metadata only; this does not verify current bytes."""
        return (
            metadata.st_size == size
            and self.sha256 == digest
            and self._stat_identity(metadata) == self.identity
        )

    def unchanged(self, path: Path) -> bool:
        if self._stat_identity(path.stat(follow_symlinks=False)) != self.identity:
            return False
        with open_regular(path) as source:
            pinned_before = identity(os.fstat(source.fileno()))
            if self._stat_identity(os.fstat(source.fileno())) != self.identity:
                return False
            digest = hashlib.sha256()
            for block in iter(lambda: source.read(1024 * 1024), b""):
                digest.update(block)
            return (
                digest.hexdigest() == self.sha256
                and identity(os.fstat(source.fileno())) == pinned_before
                and self._stat_identity(path.stat(follow_symlinks=False)) == self.identity
            )

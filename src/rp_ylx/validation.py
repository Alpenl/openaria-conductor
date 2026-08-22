"""Public session validation facade with explicit version dispatch."""

from __future__ import annotations

import json
import os
import stat
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from rp_ylx.contracts import SessionValidationError, validate_session
from rp_ylx.recording import DeviceRecordingError, validate_device_session_directory

MAX_MANIFEST_BYTES = 8 * 1024 * 1024


@dataclass(frozen=True, slots=True)
class PublicValidationError(RuntimeError):
    code: str
    location: str
    message: str

    def __str__(self) -> str:
        return f"{self.code}: {self.location}: {self.message}"


def _read_discriminator(directory: Path) -> dict[str, Any]:
    root_flags = os.O_RDONLY | os.O_CLOEXEC | getattr(os, "O_DIRECTORY", 0)
    file_flags = os.O_RDONLY | os.O_CLOEXEC | getattr(os, "O_NOFOLLOW", 0)
    try:
        root_fd = os.open(directory, root_flags)
        try:
            descriptor = os.open("manifest.json", file_flags, dir_fd=root_fd)
        finally:
            os.close(root_fd)
        try:
            metadata = os.fstat(descriptor)
            if not stat.S_ISREG(metadata.st_mode) or metadata.st_size > MAX_MANIFEST_BYTES:
                raise PublicValidationError(
                    "manifest_invalid", "manifest.json", "manifest must be a bounded regular file"
                )
            payload = b""
            while len(payload) <= MAX_MANIFEST_BYTES:
                block = os.read(descriptor, min(1024 * 1024, MAX_MANIFEST_BYTES + 1 - len(payload)))
                if not block:
                    break
                payload += block
        finally:
            os.close(descriptor)
    except PublicValidationError:
        raise
    except OSError as error:
        raise PublicValidationError("manifest_unreadable", "manifest.json", str(error)) from error
    try:
        manifest = json.loads(payload)
    except (UnicodeError, json.JSONDecodeError) as error:
        raise PublicValidationError("manifest_invalid", "manifest.json", str(error)) from error
    if not isinstance(manifest, dict):
        raise PublicValidationError("manifest_invalid", "manifest", "manifest must be an object")
    return manifest


def validate_public_session(directory: str | Path) -> dict[str, Any]:
    root = Path(directory)
    manifest = _read_discriminator(root)
    schema = manifest.get("schema")
    legacy_format = manifest.get("format")
    if schema is not None and legacy_format is not None:
        raise PublicValidationError(
            "conflicting_discriminator",
            "manifest",
            "schema and format discriminators cannot both be present",
        )
    try:
        if schema in {"ylx.device-session.v1", "ylx.device-session.v2"}:
            return dict(validate_device_session_directory(root))
        if legacy_format == "ylx.recording-session.v0":
            return validate_session(root)
    except SessionValidationError as error:
        raise PublicValidationError(error.code, error.location, error.message) from error
    except DeviceRecordingError as error:
        raise PublicValidationError(error.code, "manifest", error.message) from error
    if schema is None and legacy_format is None:
        raise PublicValidationError(
            "missing_discriminator", "manifest", "manifest schema or format is required"
        )
    raise PublicValidationError(
        "unsupported_version",
        "manifest",
        f"unsupported session discriminator: {schema or legacy_format}",
    )

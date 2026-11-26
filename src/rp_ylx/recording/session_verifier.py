"""Device Session directory inspection, full-content verification, and sealing checks."""

from __future__ import annotations

import hashlib
import json
import os
import stat
from collections import deque
from collections.abc import Callable, Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from rp_ylx.api.downloads import ArtifactAccessError, validated_device_session_payload
from rp_ylx.manifest_limits import MAX_MANIFEST_BYTES
from rp_ylx.native import native_session_store_or_none
from rp_ylx.recording.fs_safety import file_identity, is_exfat_or_msdos_path
from rp_ylx.recording.fs_safety import open_regular_at as _fs_open_regular_at
from rp_ylx.recording.manifest_builder import (
    DeviceRecordingError,
    DeviceSessionConfig,
    FinalizedArtifact,
    artifact_path_and_bytes,
    finalize_artifact,
    manifest_artifacts,
    read_bounded_fd,
    recording_error,
)
from rp_ylx.recording.verified import VerifiedArtifact
from rp_ylx.recording.verified import identity as verified_identity
from rp_ylx.recording_layout import manifest_identity


def open_regular_at(
    root_fd: int,
    relative: str,
    *,
    code: str,
    session_store: object | None = None,
) -> int:
    """Open a single-link regular file relative to root_fd via native or POSIX fallback."""
    native = session_store
    if native is not None and hasattr(native, "open_relative_regular"):
        try:
            descriptor = native.open_relative_regular(root_fd, relative)
        except AttributeError:
            descriptor = None
        except BaseException as error:
            converted = recording_error(error, "native_session_io_failed")
            raise DeviceRecordingError(code, converted.message) from error
        if descriptor is not None:
            if type(descriptor) is not int or descriptor < 0:
                raise DeviceRecordingError(code, "原生 artifact 打开返回无效 fd")
            return descriptor
    return _fs_open_regular_at(
        root_fd,
        relative,
        error_factory=lambda message: DeviceRecordingError(code, message),
    )


def verified_finalization(
    path: Path,
    expected_bytes: int | None,
    *,
    code: str,
    finalize_fn: Callable[..., FinalizedArtifact] | None = None,
    identity_fn: Callable[..., tuple[int, ...]] | None = None,
) -> tuple[FinalizedArtifact, VerifiedArtifact]:
    """Read fresh bytes and return a proof without mutating recorder state."""
    finalize = finalize_fn or finalize_artifact
    id_fn = identity_fn or verified_identity
    exfat = is_exfat_or_msdos_path(path)
    before = (
        id_fn(path.stat(follow_symlinks=False))
        if not exfat
        else file_identity(path.stat(follow_symlinks=False), include_inode=False)
    )
    # Same inode/size/mtime/ctime does not imply identical bytes on the
    # camera's coarse-timestamp filesystem. Always read before admission.
    result = finalize(path, expected_bytes, code=code)
    after = (
        id_fn(path.stat(follow_symlinks=False))
        if not exfat
        else file_identity(path.stat(follow_symlinks=False), include_inode=False)
    )
    if after != before:
        raise DeviceRecordingError(code, "artifact changed during independent verification")
    return result, VerifiedArtifact(before, result.sha256, include_inode=not exfat)


def verify_artifact_fd(
    descriptor: int,
    expected_bytes: int,
    expected_sha256: str,
    interrupt_check: Callable[[], None] | None = None,
    *,
    session_store: object | None = None,
) -> None:
    """Verify exact byte count and SHA-256 of an already-open artifact descriptor."""
    native = session_store
    if (
        interrupt_check is not None
        and native is not None
        and hasattr(native, "verify_fd_interruptible")
    ):
        try:
            native.verify_fd_interruptible(
                descriptor, expected_bytes, expected_sha256, interrupt_check
            )
            return
        except BaseException as error:
            raise recording_error(error, "native_session_io_failed") from error
    if interrupt_check is None and native is not None and hasattr(native, "verify_fd"):
        try:
            native.verify_fd(descriptor, expected_bytes, expected_sha256)
            return
        except BaseException as error:
            raise recording_error(error, "native_session_io_failed") from error
    before = os.fstat(descriptor)
    if before.st_size != expected_bytes:
        raise DeviceRecordingError("artifact_invalid", "artifact 大小不匹配")
    digest = hashlib.sha256()
    remaining = expected_bytes
    while remaining:
        if interrupt_check is not None:
            interrupt_check()
        block = os.read(descriptor, min(1024 * 1024, remaining))
        if not block:
            raise DeviceRecordingError("artifact_invalid", "artifact 发生短读")
        digest.update(block)
        remaining -= len(block)
    if os.read(descriptor, 1):
        raise DeviceRecordingError("artifact_invalid", "artifact 大小在校验期间变化")
    after = os.fstat(descriptor)
    identity_before = (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns)
    identity_after = (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns)
    if identity_after != identity_before:
        raise DeviceRecordingError("artifact_invalid", "artifact 在校验期间变化")
    if digest.hexdigest() != expected_sha256:
        raise DeviceRecordingError("digest_mismatch", "artifact 摘要不匹配")


def validate_device_session_directory(
    path: str | Path,
    *,
    expected_session_id: str | None = None,
    interrupt_check: Callable[[], None] | None = None,
    verified_artifacts: Mapping[str, VerifiedArtifact] | None = None,
    progress: Callable[[int, int], None] | None = None,
    open_regular_fn: Callable[..., int] | None = None,
    read_bounded_fn: Callable[..., bytes] | None = None,
    verify_fd_fn: Callable[..., None] | None = None,
    artifacts_fn: Callable[[Mapping[str, object]], list[Mapping[str, object]]] | None = None,
    executor_cls: type[ThreadPoolExecutor] = ThreadPoolExecutor,
) -> Mapping[str, object]:
    """Validate sealed manifest and full SHA-256 content of every declared artifact."""
    del verified_artifacts
    root = Path(path)
    root_flags = (
        os.O_RDONLY | os.O_CLOEXEC | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0)
    )
    open_fn = open_regular_fn or (
        lambda root_fd, relative, *, code: open_regular_at(
            root_fd, relative, code=code, session_store=native_session_store_or_none()
        )
    )
    read_fn = read_bounded_fn or (
        lambda fd, maximum, *, code: read_bounded_fd(
            fd, maximum, code=code, session_store=native_session_store_or_none()
        )
    )
    verify_fn = verify_fd_fn or (
        lambda fd, size, sha, check: verify_artifact_fd(
            fd, size, sha, check, session_store=native_session_store_or_none()
        )
    )
    collect_artifacts = artifacts_fn or manifest_artifacts

    try:
        try:
            root_fd = os.open(root, root_flags)
        except OSError as error:
            raise DeviceRecordingError(
                "manifest_invalid", f"会话目录不可安全打开：{error}"
            ) from error
        try:
            manifest_fd = open_fn(root_fd, "manifest.json", code="manifest_invalid")
            try:
                metadata = os.fstat(manifest_fd)
                if metadata.st_size > MAX_MANIFEST_BYTES:
                    raise DeviceRecordingError("manifest_invalid", "manifest 超过允许大小")
                payload = read_fn(manifest_fd, MAX_MANIFEST_BYTES, code="manifest_invalid")
            finally:
                os.close(manifest_fd)
            selected_session_id = (
                manifest_identity(root, json.loads(payload))
                if expected_session_id is None
                else expected_session_id
            )
            manifest = validated_device_session_payload(payload, selected_session_id)
            descriptors = collect_artifacts(manifest)
            if progress is not None:
                progress(0, len(descriptors))

            def verify_artifact(artifact_descriptor: Mapping[str, object]) -> None:
                if interrupt_check is not None:
                    interrupt_check()
                relative, expected_bytes = artifact_path_and_bytes(
                    artifact_descriptor,
                    code="artifact_invalid",
                )
                expected_sha256 = artifact_descriptor.get("sha256")
                if not isinstance(expected_sha256, str):
                    raise DeviceRecordingError("artifact_invalid", "manifest artifact 描述符无效")
                artifact_fd = open_fn(root_fd, relative, code="artifact_invalid")
                try:
                    verify_fn(artifact_fd, expected_bytes, expected_sha256, interrupt_check)
                finally:
                    os.close(artifact_fd)

            if interrupt_check is not None:
                for completed, descriptor in enumerate(descriptors, 1):
                    verify_artifact(descriptor)
                    if progress is not None:
                        progress(completed, len(descriptors))
            else:
                with executor_cls(max_workers=2, thread_name_prefix="session-verify") as executor:
                    pending = deque(
                        executor.submit(verify_artifact, descriptor)
                        for descriptor in descriptors[:2]
                    )
                    for completed in range(1, len(descriptors) + 1):
                        pending.popleft().result()
                        if progress is not None:
                            progress(completed, len(descriptors))
                        if completed + 1 < len(descriptors):
                            pending.append(
                                executor.submit(verify_artifact, descriptors[completed + 1])
                            )
            return manifest
        finally:
            os.close(root_fd)
    except ArtifactAccessError as error:
        raise DeviceRecordingError("manifest_invalid", str(error)) from error
    except (OSError, ValueError, json.JSONDecodeError) as error:
        raise DeviceRecordingError("manifest_invalid", str(error)) from error


def validate_artifact_bytes(
    base: Path,
    manifest: Mapping[str, object],
    artifact_identities: dict[str, tuple[int, int, int, int]],
    *,
    finalize_fn: Callable[..., FinalizedArtifact] | None = None,
) -> None:
    """Verify every manifest artifact matches its sealed size/identity (re-hashing on exFAT)."""
    finalize = finalize_fn or finalize_artifact
    exfat = is_exfat_or_msdos_path(base)
    descriptors = manifest_artifacts(manifest)
    for descriptor in descriptors:
        relative, expected_bytes = artifact_path_and_bytes(
            descriptor,
            code="artifact_invalid",
        )
        expected = artifact_identities.get(relative)
        if expected is None:
            raise DeviceRecordingError("artifact_invalid", "artifact 缺少封存身份")
        path = base / relative
        metadata = path.stat(follow_symlinks=False)
        if not stat.S_ISREG(metadata.st_mode):
            raise DeviceRecordingError("digest_mismatch", "artifact 在封存前发生变化")
        if metadata.st_size != expected_bytes:
            raise DeviceRecordingError("digest_mismatch", "artifact 在封存前发生变化")
        actual = (
            metadata.st_dev,
            metadata.st_ino,
            metadata.st_size,
            metadata.st_mtime_ns,
        )
        if actual != expected:
            if not exfat or (
                metadata.st_dev,
                metadata.st_size,
                metadata.st_mtime_ns,
            ) != (expected[0], expected[2], expected[3]):
                raise DeviceRecordingError("digest_mismatch", "artifact 在封存前发生变化")
            # exFAT does not guarantee persistent kernel st_ino across closed files;
            # re-verify content digest before admitting the refreshed inode.
            reverified = finalize(path, expected_bytes, code="digest_mismatch")
            if reverified.sha256 != descriptor.get("sha256"):
                raise DeviceRecordingError("digest_mismatch", "artifact 在封存前发生变化")
            artifact_identities[relative] = reverified.identity


def enforce_quality_policy(
    config: DeviceSessionConfig,
    frames_written: int,
    drop_events: Sequence[Mapping[str, object]],
    duration: float,
) -> None:
    """Enforce configured drop-frame limits before sealing."""
    dropped = sum(int(event["dropped"]) for event in drop_events)
    total = frames_written + dropped
    fraction = 0.0 if total == 0 else dropped / total
    contiguous = max((int(event["dropped"]) for event in drop_events), default=0)
    window_drops = max(
        (
            sum(
                int(other["dropped"])
                for other in drop_events
                if float(event["at_time_seconds"])
                <= float(other["at_time_seconds"])
                < float(event["at_time_seconds"]) + config.drop_window_seconds
            )
            for event in drop_events
        ),
        default=0,
    )
    violations: list[str] = []
    if contiguous > config.max_contiguous_dropped_frames:
        violations.append("contiguous")
    if dropped > config.max_total_dropped_frames:
        violations.append("total")
    if fraction > config.max_drop_fraction:
        violations.append("fraction")
    if window_drops > config.max_dropped_frames_per_window:
        violations.append("window")
    if violations:
        raise DeviceRecordingError(
            "drop_quality_exceeded",
            (
                f"quality policy {config.quality_policy_id} rejected recording: "
                "classification=write_backpressure "
                f"violations={','.join(violations)} contiguous={contiguous} "
                f"dropped={dropped} total={total} fraction={fraction:.9f} "
                f"window_drops={window_drops} window_seconds="
                f"{config.drop_window_seconds:.9f} duration={duration:.9f} "
                "limits=contiguous:0,total:0,fraction:0,window:0"
            ),
        )

"""Startup crash recovery and partial-directory settlement orchestration."""

from __future__ import annotations

import copy
import hashlib
import json
import math
import os
import stat
import sys
import uuid
from collections.abc import Mapping
from contextlib import ExitStack, suppress
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING

from rp_ylx.api.downloads import ArtifactAccessError
from rp_ylx.api.events import (
    InvalidSourceEvent,
    validate_recording_state,
    validate_retained_unsuccessful_outcome,
)
from rp_ylx.manifest_limits import MAX_MANIFEST_BYTES
from rp_ylx.operational_logging import operational_logger
from rp_ylx.recording.device_session import (
    DeviceRecordingError,
    fsync_directory,
    inspect_device_session_directory,
)
from rp_ylx.recording.fs_safety import copy_regular_atomic, write_json_atomic
from rp_ylx.recording.recovery import (
    MAX_RECOVERY_METADATA_BYTES,
    RECOVERY_ROLE,
    recover_device_session,
)
from rp_ylx.recording.volume import VolumeAdmission
from rp_ylx.recording_layout import open_relative_directory, resolve_recording

if TYPE_CHECKING:
    from rp_ylx.recording.coordinator import CaptureCoordinator


def _coordinator_symbol(coordinator: CaptureCoordinator, name: str, default):
    module = sys.modules.get(type(coordinator).__module__)
    return default if module is None else getattr(module, name, default)


def read_recovery_metadata(root_fd: int, parts: tuple[str, ...], limit: int) -> tuple[dict, bytes]:
    """Read only bounded regular metadata beneath an already opened session."""
    with ExitStack() as cleanup:
        current = root_fd
        for component in parts[:-1]:
            current = os.open(
                component,
                os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC,
                dir_fd=current,
            )
            cleanup.callback(os.close, current)
            if os.fstat(current).st_dev != os.fstat(root_fd).st_dev:
                raise ValueError("recovery metadata directory is on a different device")
        descriptor = os.open(
            parts[-1],
            os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK | os.O_CLOEXEC,
            dir_fd=current,
        )
        cleanup.callback(os.close, descriptor)
        before = os.fstat(descriptor)
        if (
            not stat.S_ISREG(before.st_mode)
            or before.st_nlink != 1
            or before.st_dev != os.fstat(root_fd).st_dev
            or before.st_size > limit
        ):
            raise ValueError("unsafe or oversized recovery metadata")
        with os.fdopen(descriptor, "rb", closefd=False) as stream:
            payload = stream.read(limit + 1)
        after = os.fstat(descriptor)
        if (
            len(payload) != before.st_size
            or len(payload) > limit
            or (before.st_size, before.st_mtime_ns, before.st_ctime_ns)
            != (after.st_size, after.st_mtime_ns, after.st_ctime_ns)
        ):
            raise ValueError("recovery metadata changed during read")
        value = json.loads(payload)
        if not isinstance(value, dict):
            raise ValueError("invalid recovery metadata object")
        return value, payload


def partial_directory_current(partial: Path, descriptor: int) -> bool:
    try:
        entry = partial.stat(follow_symlinks=False)
        opened = os.fstat(descriptor)
    except OSError:
        return False
    return stat.S_ISDIR(entry.st_mode) and (entry.st_dev, entry.st_ino) == (
        opened.st_dev,
        opened.st_ino,
    )


def can_recover_sessions(coordinator: CaptureCoordinator, admission: VolumeAdmission) -> bool:
    try:
        coordinator._require_capture_reserve(admission)
    except DeviceRecordingError as error:
        coordinator._recovery_deferred = True
        operational_logger("recording").event(
            "session_recovery_deferred", level="warning", reason=error.code
        )
        return False
    return True


def recover_sessions(coordinator: CaptureCoordinator) -> None:
    admission = coordinator._require_admission()
    if not coordinator._can_recover_sessions(admission):
        return
    coordinator._recovery_deferred = False
    for sessions_root in admission.catalog_roots:
        for partial in sorted(sessions_root.glob("*.partial")):
            session_id = partial.name.removesuffix(".partial")
            try:
                parsed = uuid.UUID(session_id)
            except ValueError:
                continue
            if parsed.version != 7 or str(parsed) != session_id:
                continue
            try:
                descriptor = os.open(
                    partial, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC
                )
            except OSError:
                continue
            with ExitStack() as cleanup:
                cleanup.callback(os.close, descriptor)
                if os.fstat(descriptor).st_dev != admission.device:
                    continue
                if not coordinator._can_recover_sessions(admission):
                    return
                coordinator._recover_partial_directory(partial, session_id, descriptor)
    restored = coordinator._restore_published_failure(admission)
    inspect_fn = _coordinator_symbol(
        coordinator, "inspect_device_session_directory", inspect_device_session_directory
    )
    for session_id in tuple(coordinator._retained):
        if session_id == restored:
            continue
        for sessions_root in admission.catalog_roots:
            try:
                recovered, _ = inspect_fn(resolve_recording(sessions_root, session_id))
            except (OSError, DeviceRecordingError):
                continue
            if any(log.get("role") == RECOVERY_ROLE for log in recovered["logs"]):
                coordinator._record_saved_prefix(session_id, recovered)


def recover_partial_directory(
    coordinator: CaptureCoordinator,
    partial: Path,
    session_id: str,
    descriptor: int,
) -> None:
    anchored = Path("/proc/self/fd") / str(descriptor)
    manifest = anchored / "manifest.json"
    read_fn = _coordinator_symbol(coordinator, "_read_recovery_metadata", read_recovery_metadata)
    inspect_fn = _coordinator_symbol(
        coordinator, "inspect_device_session_directory", inspect_device_session_directory
    )
    if manifest.is_file():
        final = partial.parent / session_id
        published = False
        try:
            inspect_fn(partial, expected_session_id=session_id)
            if not partial_directory_current(partial, descriptor):
                return
            if final.exists():
                raise DeviceRecordingError("session_exists", "恢复目标会话已经存在")
            archived_controls: list[str] = []
            recovery_dir = partial / "recovery"
            for control in ("capture.json", "recording.json"):
                try:
                    os.stat(control, dir_fd=descriptor, follow_symlinks=False)
                except FileNotFoundError:
                    continue
                if recovery_dir.is_symlink():
                    raise ValueError("unsafe recovery directory")
                recovery_dir.mkdir(exist_ok=True, mode=0o750)
                if copy_regular_atomic(partial / control, recovery_dir / control):
                    archived_controls.append(control)
            if archived_controls:
                fsync_directory(recovery_dir)
            for subdir_name in ("video", "audio"):
                subdir = partial / subdir_name
                if subdir.is_dir():
                    fsync_directory(subdir)
            os.fsync(descriptor)
            if not partial_directory_current(partial, descriptor):
                return
            os.rename(partial, final)
            published = True
            fsync_directory(final.parent)
            unlinked = False
            for control in archived_controls:
                try:
                    (final / control).unlink(missing_ok=True)
                    unlinked = True
                except OSError as error:
                    operational_logger("recording").event(
                        "recovery_control_unlink_failed",
                        level="warning",
                        transaction_id=session_id,
                        file=control,
                        error=str(error),
                    )
            if unlinked:
                with suppress(OSError):
                    fsync_directory(final)
            if not partial_directory_current(final, descriptor):
                return
            _, payload = read_fn(descriptor, ("manifest.json",), MAX_MANIFEST_BYTES)
            # R-06: metadata-only inspection must NOT promote an unhashed
            # session into _verified.
            coordinator._publish_recording_directory(final, json.loads(payload))
            return
        except (OSError, ValueError, RecursionError, DeviceRecordingError):
            if published or partial_directory_current(final, descriptor):
                operational_logger("recording").event(
                    "recovery_publication_check_deferred",
                    level="warning",
                    transaction_id=session_id,
                )
                return
            with suppress(OSError):
                os.unlink("manifest.json", dir_fd=descriptor)
    coordinator._settle_partial(anchored, session_id, descriptor=descriptor)
    if partial_directory_current(partial, descriptor):
        coordinator._recover_completed_prefix(partial, session_id)


def restore_published_failure(
    coordinator: CaptureCoordinator,
    admission: VolumeAdmission,
) -> str | None:
    identity = coordinator._state._recovery_session
    if identity is None or identity["volume_id"] != admission.volume_id:
        return None
    session_id = identity["session_id"]
    if session_id in coordinator._retained:
        coordinator._state._recovery_session = None
        return None
    retryable_read = False
    read_fn = _coordinator_symbol(coordinator, "_read_recovery_metadata", read_recovery_metadata)
    for sessions_root in admission.catalog_roots:
        session_opened = False
        try:
            with ExitStack() as cleanup:
                flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC
                root_fd = os.open(sessions_root, flags)
                cleanup.callback(os.close, root_fd)
                relative = resolve_recording(sessions_root, session_id).relative_to(sessions_root)
                session_fd = open_relative_directory(root_fd, relative)
                cleanup.callback(os.close, session_fd)
                session_opened = True
                if os.fstat(session_fd).st_dev != admission.device:
                    raise ValueError("recovery session is on a different device")
                state, _ = read_fn(
                    session_fd, ("recovery", "recording.json"), MAX_RECOVERY_METADATA_BYTES
                )
                receipt, receipt_bytes = read_fn(
                    session_fd, ("recovery", "interruption.json"), 1024 * 1024
                )
                manifest, _ = read_fn(session_fd, ("manifest.json",), MAX_MANIFEST_BYTES)
            outcome = {"generation_id": admission.generation_id, "recording_state": state}
            validate_retained_unsuccessful_outcome(
                {
                    "schema": "ylx.retained-unsuccessful-session-resource.v2",
                    "authority_epoch": state.get("authority_epoch"),
                    "source_revision": state.get("state_revision"),
                    "outcome": outcome,
                },
                session_id=session_id,
            )
            if (
                state["authority_epoch"] != coordinator._authority_epoch
                or state["storage"]["volume_id"] != admission.volume_id
                or state["device"]["device_id"] != coordinator._config.session.device_id
                or manifest.get("schema")
                not in {
                    "ylx.device-session.v2",
                    "ylx.device-session.v3",
                    "ylx.device-session.v4",
                }
                or manifest.get("sealed") is not True
                or manifest.get("session_id") != session_id
                or manifest.get("volume_id") != admission.volume_id
                or receipt.get("schema") != "openaria.recording-recovery.v1"
                or receipt.get("session_id") != session_id
                or receipt.get("outcome") != "interrupted"
            ):
                raise ValueError("published recovery identity mismatch")
            duration = manifest["time"]["duration_seconds"]
            saved = receipt["saved_video_seconds"]
            if (
                type(duration) not in (int, float)
                or not math.isfinite(duration)
                or type(saved) not in (int, float)
                or not math.isfinite(saved)
                or not 0 < saved <= duration
            ):
                raise ValueError("invalid recovery duration")
            digest = hashlib.sha256(receipt_bytes).hexdigest()
            expected_log = {
                "artifact_id": digest,
                "sha256": digest,
                "bytes": len(receipt_bytes),
                "path": "recovery/interruption.json",
                "role": RECOVERY_ROLE,
                "media_type": "application/json",
            }
            if expected_log not in manifest["logs"]:
                raise ValueError("recovery receipt is not bound to the published manifest")
        except FileNotFoundError:
            if not session_opened:
                retryable_read = True
            continue
        except (
            OSError,
            ValueError,
            KeyError,
            TypeError,
            OverflowError,
            RecursionError,
            InvalidSourceEvent,
        ):
            retryable_read = True
            continue
        coordinator._retained[session_id] = outcome
        coordinator._revision = max(coordinator._revision, state["state_revision"])
        coordinator._record_saved_prefix(session_id, manifest)
        coordinator._state._recovery_session = None
        return session_id
    if not retryable_read:
        coordinator._state._recovery_session = None
    return None


def recover_completed_prefix(
    coordinator: CaptureCoordinator,
    partial: Path,
    session_id: str,
) -> None:
    recover_fn = _coordinator_symbol(coordinator, "recover_device_session", recover_device_session)
    try:
        recovered = recover_fn(partial)
    except (
        OSError,
        ValueError,
        KeyError,
        TypeError,
        DeviceRecordingError,
        ArtifactAccessError,
    ):
        return
    if recovered is not None:
        coordinator._publish_recording_directory(recovered.path, recovered.manifest)
        with coordinator._catalog_lock:
            coordinator._verified[session_id] = recovered.manifest_sha256
        coordinator._record_saved_prefix(session_id, recovered.manifest)


def record_saved_prefix(
    coordinator: CaptureCoordinator,
    session_id: str,
    manifest: Mapping[str, object],
) -> None:
    retained = coordinator._retained.get(session_id)
    if retained is None:
        return
    state = copy.deepcopy(retained["recording_state"])
    if any(item.get("code") == "recording_prefix_saved" for item in state["diagnostics"]):
        return
    duration = manifest["time"]["duration_seconds"]
    state["diagnostics"].append(
        {
            "code": "recording_prefix_saved",
            "severity": "info",
            "message": f"录制已中断，已保存前 {duration:.1f} 秒，可导出",
            "at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
            "recoverable": False,
        }
    )
    state["authority_epoch"] = coordinator._authority_epoch
    state["state_revision"] = coordinator._next_revision()
    state["updated_at"] = state["diagnostics"][-1]["at"]
    retained["recording_state"] = state


def settle_partial(
    coordinator: CaptureCoordinator,
    partial: Path,
    session_id: str,
    *,
    descriptor: int,
) -> None:
    admission = coordinator._require_admission()
    with suppress(OSError):
        (partial / "manifest.json").unlink(missing_ok=True)
    read_fn = _coordinator_symbol(coordinator, "_read_recovery_metadata", read_recovery_metadata)
    try:
        current, _ = read_fn(descriptor, ("recording.json",), MAX_RECOVERY_METADATA_BYTES)
        validate_recording_state(current)
        if current["session_id"] != session_id:
            raise ValueError("partial recording state identity mismatch")
        try:
            capture, _ = read_fn(descriptor, ("capture.json",), MAX_RECOVERY_METADATA_BYTES)
        except FileNotFoundError:
            capture = None
        if capture is not None and (
            capture["session_id"] != session_id
            or capture["plan"]["session_id"] != session_id
            or current["storage"]["volume_id"] != capture["plan"]["volume_id"]
            or current["device"]["device_id"] != capture["config"]["device_id"]
        ):
            raise ValueError("partial recording state differs from capture plan")
    except FileNotFoundError:
        return
    except (OSError, ValueError, KeyError, TypeError, RecursionError) as error:
        operational_logger("recording").event(
            "recovery_state_rejected",
            level="warning",
            transaction_id=session_id,
            exception_type=type(error).__name__,
        )
        return
    if (
        current.get("authority_epoch") == coordinator._authority_epoch
        and type(current.get("state_revision")) is int
    ):
        coordinator._revision = max(coordinator._revision, int(current["state_revision"]))
    if current.get("state") in {"recoverable", "failed", "abandoned"}:
        coordinator._retained[session_id] = {
            "generation_id": admission.generation_id,
            "recording_state": current,
        }
        return
    now = datetime.now(UTC).isoformat().replace("+00:00", "Z")
    settled = copy.deepcopy(current)
    settled["state"] = "recoverable"
    settled["authority_epoch"] = coordinator._authority_epoch
    settled["state_revision"] = coordinator._next_revision()
    settled["updated_at"] = now
    settled["diagnostics"] = [
        {
            "code": "process_interrupted",
            "severity": "error",
            "message": "daemon 重启时发现未完成封存的会话",
            "at": now,
            "recoverable": True,
        }
    ]
    with suppress(OSError):
        (partial / "manifest.json").unlink(missing_ok=True)
        write_json_atomic(partial / "recording.json", settled)
    coordinator._retained[session_id] = {
        "generation_id": admission.generation_id,
        "recording_state": settled,
    }

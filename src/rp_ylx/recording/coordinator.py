"""生产录制的卷准入、协调、恢复和 DeviceProvider 投影。"""

from __future__ import annotations

import copy
import hashlib
import json
import os
import threading
import time
import uuid
from collections.abc import Callable, Mapping
from contextlib import suppress
from dataclasses import dataclass
from datetime import UTC, datetime
from http import HTTPStatus
from pathlib import Path
from typing import Protocol

from rp_ylx.api.downloads import ArtifactAccessError, DirectorySessionStore
from rp_ylx.api.events import validate_safe_swap_v3_receipt
from rp_ylx.api.gateway import CaptureCommand, CaptureCommandResult, ProviderError
from rp_ylx.api.preview import LatestPreviewBuffer, PreviewResponse
from rp_ylx.camera import FrameObservation
from rp_ylx.imu import ImuObservation
from rp_ylx.recording.device_session import (
    DeviceRecordingError,
    DeviceSessionConfig,
    DeviceSessionRecorder,
    SessionPlan,
    StorageStatus,
    fsync_directory,
    inspect_device_session_directory,
    uuid7,
    validate_device_session_directory,
    write_json_atomic,
)
from rp_ylx.runtime import collect_linux_runtime

VOLUME_MARKER = ".ylx-volume.json"
SESSIONS_DIRECTORY = "sessions"


class _Representation(Protocol):
    etag: str
    size: int
    content_type: str

    def close(self) -> None: ...

    def read(self, offset: int = 0, length: int | None = None) -> bytes: ...

    def iter_chunks(
        self,
        offset: int = 0,
        length: int | None = None,
        *,
        chunk_size: int = 1024 * 1024,
    ) -> object: ...


class CaptureSources(Protocol):
    @property
    def open_handle_count(self) -> int: ...

    def start(
        self,
        *,
        mode: str,
        generation_id: str,
        submit_frame: Callable[[FrameObservation], bool],
        submit_imu: Callable[[ImuObservation], bool],
        on_failure: Callable[[str, str], None],
    ) -> None: ...

    def stop(self) -> None: ...


@dataclass(frozen=True, slots=True)
class VolumeAdmission:
    mountpoint: Path
    sessions_root: Path
    volume_id: str
    generation_id: str
    device: int
    mount_identity: str
    total_bytes: int
    available_bytes: int
    available_inodes: int


@dataclass(frozen=True, slots=True)
class CoordinatorConfig:
    mountpoint: Path
    state_root: Path
    session: DeviceSessionConfig
    minimum_available_bytes: int = 2 * 1024 * 1024 * 1024
    minimum_available_inodes: int = 1024
    queue_capacity: int = 128
    enqueue_timeout: float = 0.05
    checkpoint_interval: float = 1.0

    def __post_init__(self) -> None:
        if (
            not self.mountpoint.is_absolute()
            or not self.state_root.is_absolute()
            or self.minimum_available_bytes < 0
            or self.minimum_available_inodes < 0
            or self.queue_capacity <= 0
            or self.enqueue_timeout < 0
            or self.checkpoint_interval < 0
        ):
            raise ValueError("录制协调器配置无效")


def initialize_capture_volume(mountpoint: str | Path, *, volume_id: str | None = None) -> str:
    """显式初始化录制卷；不会覆盖已有标记或创建挂载点。"""

    root = Path(mountpoint).resolve()
    if not root.is_dir():
        raise DeviceRecordingError("storage_unavailable", "录制卷挂载点不存在")
    selected = volume_id or str(uuid.uuid4())
    try:
        parsed = uuid.UUID(selected)
    except ValueError as error:
        raise ValueError("volume_id 必须是 UUIDv4") from error
    if parsed.version != 4 or str(parsed) != selected:
        raise ValueError("volume_id 必须是规范 UUIDv4")
    marker = root / VOLUME_MARKER
    if marker.exists():
        raise DeviceRecordingError("volume_already_initialized", "录制卷已经包含身份标记")
    write_json_atomic(
        marker,
        {
            "schema": "ylx.capture-volume.v1",
            "volume_id": selected,
            "initialized_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        },
    )
    return selected


def _read_volume_id(mountpoint: Path) -> str:
    marker = mountpoint / VOLUME_MARKER
    try:
        raw = marker.read_bytes()
        if len(raw) > 4096:
            raise ValueError("卷标记过大")
        value = json.loads(raw)
        if not isinstance(value, dict) or set(value) != {
            "schema",
            "volume_id",
            "initialized_at",
        }:
            raise ValueError("卷标记字段无效")
        selected = value["volume_id"]
        parsed = uuid.UUID(selected)
        if (
            value["schema"] != "ylx.capture-volume.v1"
            or parsed.version != 4
            or str(parsed) != selected
            or not isinstance(value["initialized_at"], str)
        ):
            raise ValueError("卷标记值无效")
        return selected
    except (OSError, ValueError, TypeError, json.JSONDecodeError) as error:
        raise DeviceRecordingError("volume_not_admitted", "录制卷缺少有效身份标记") from error


def _stat_capacity(mountpoint: Path) -> tuple[int, int, int]:
    try:
        stat = os.statvfs(mountpoint)
    except OSError as error:
        raise DeviceRecordingError("storage_unavailable", "无法读取录制卷容量") from error
    return (
        stat.f_frsize * stat.f_blocks,
        stat.f_frsize * stat.f_bavail,
        stat.f_favail,
    )


def _mount_writable(mountpoint: Path) -> bool:
    try:
        flags = os.statvfs(mountpoint).f_flag
    except OSError:
        return False
    read_only = bool(flags & getattr(os, "ST_RDONLY", 1))
    return not read_only and os.access(mountpoint, os.W_OK)


def _default_mount_identity(mountpoint: Path) -> str:
    try:
        lines = Path("/proc/self/mountinfo").read_text(encoding="utf-8").splitlines()
    except OSError:
        lines = []
    encoded_mountpoint = str(mountpoint).replace(" ", "\\040").replace("\t", "\\011")
    for line in lines:
        fields = line.split()
        if len(fields) >= 6 and fields[4] == encoded_mountpoint:
            return f"mount:{fields[0]}:{fields[2]}"
    current = mountpoint.stat()
    return f"path:{current.st_dev}:{current.st_ino}"


class _TrackedRepresentation:
    def __init__(
        self,
        representation: _Representation,
        release: Callable[[], None],
    ) -> None:
        self._representation = representation
        self._release = release
        self._closed = False
        self.etag = representation.etag
        self.size = representation.size
        self.content_type = representation.content_type
        descriptor = getattr(representation, "descriptor", None)
        if descriptor is not None:
            self.descriptor = descriptor

    def __enter__(self) -> _TrackedRepresentation:
        if self._closed:
            raise RuntimeError("会话表示已关闭")
        return self

    def __exit__(self, *args: object) -> None:
        self.close()

    def __del__(self) -> None:
        self.close()

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        try:
            self._representation.close()
        finally:
            self._release()

    def read(self, offset: int = 0, length: int | None = None) -> bytes:
        return self._representation.read(offset, length)

    def iter_chunks(
        self,
        offset: int = 0,
        length: int | None = None,
        *,
        chunk_size: int = 1024 * 1024,
    ) -> object:
        return self._representation.iter_chunks(offset, length, chunk_size=chunk_size)


class CaptureCoordinator:
    """单卷单活动会话的生产 DeviceProvider。"""

    def __init__(
        self,
        config: CoordinatorConfig,
        *,
        mount_checker: Callable[[Path], bool] | None = None,
        mount_identity: Callable[[Path], str] | None = None,
        runtime: Callable[[], Mapping[str, object]] | None = None,
        preview: LatestPreviewBuffer | None = None,
        sources: CaptureSources | None = None,
        before_write: Callable[[str, bytes], None] | None = None,
    ) -> None:
        self._config = config
        self._mount_checker = mount_checker or (lambda path: path.is_mount())
        self._mount_identity = mount_identity or _default_mount_identity
        self._runtime = runtime or collect_linux_runtime
        self._preview = preview or LatestPreviewBuffer(stream_fps=15)
        self._sources = sources
        self._before_write = before_write
        self._lock = threading.RLock()
        self._storage_lock = threading.Lock()
        self._storage_checked_at = 0.0
        self._storage_cache: StorageStatus | None = None
        self._active: DeviceSessionRecorder | None = None
        self._active_plan: SessionPlan | None = None
        self._retained: dict[str, dict[str, object]] = {}
        self._verified: dict[str, str] = {}
        self._open_representations = 0
        self._released = False
        self._media_lost = False
        self._pending_safe_swap: dict[str, object] | None = None
        self._safe_swap_resource: dict[str, object] | None = None
        self._commands: dict[tuple[str, str, str], tuple[bytes, CaptureCommandResult]] = {}
        self._config.state_root.mkdir(parents=True, exist_ok=True, mode=0o750)
        self._state_path = self._config.state_root / "capture-coordinator.json"
        persisted = self._read_local_state()
        self._authority_epoch = str(persisted.get("authority_epoch") or uuid.uuid4())
        self._revision = int(persisted.get("source_revision", 0))
        self._admission = self._admit(persisted)
        self._restore_local_state(persisted)
        self._recover_sessions()
        self._catalog_sessions()
        self._persist_local_state()

    @property
    def volume_id(self) -> str:
        return self._admission.volume_id

    @property
    def generation_id(self) -> str:
        return self._admission.generation_id

    @property
    def open_handle_count(self) -> int:
        with self._lock:
            recorder_handles = 0 if self._active is None else self._active.open_handle_count
            source_handles = 0 if self._sources is None else self._sources.open_handle_count
            return recorder_handles + source_handles + self._open_representations

    def _read_local_state(self) -> Mapping[str, object]:
        if not self._state_path.exists():
            return {}
        try:
            value = json.loads(self._state_path.read_bytes())
            return value if isinstance(value, dict) else {}
        except (OSError, ValueError, json.JSONDecodeError):
            return {}

    def _admit(self, persisted: Mapping[str, object]) -> VolumeAdmission:
        mountpoint = self._config.mountpoint.resolve()
        if not self._mount_checker(mountpoint):
            raise DeviceRecordingError("volume_not_mounted", "录制卷不是当前活动挂载点")
        try:
            mount_stat = mountpoint.stat()
        except OSError as error:
            raise DeviceRecordingError("storage_unavailable", "录制卷不可读取") from error
        volume_id = _read_volume_id(mountpoint)
        mount_identity = self._mount_identity(mountpoint)
        total_bytes, available_bytes, available_inodes = _stat_capacity(mountpoint)
        if available_bytes < self._config.minimum_available_bytes:
            raise DeviceRecordingError(
                "insufficient_space",
                f"录制卷可用空间不足：{available_bytes}/{self._config.minimum_available_bytes}",
            )
        if available_inodes < self._config.minimum_available_inodes:
            raise DeviceRecordingError(
                "insufficient_inodes",
                "录制卷可用 inode 不足："
                f"{available_inodes}/{self._config.minimum_available_inodes}",
            )
        if not _mount_writable(mountpoint):
            raise DeviceRecordingError("volume_read_only", "录制卷不可写")
        previous_binding = persisted.get("volume_binding")
        generation_id = str(uuid.uuid4())
        if isinstance(previous_binding, Mapping):
            previous_generation = previous_binding.get("generation_id")
            if (
                previous_binding.get("volume_id") == volume_id
                and previous_binding.get("device") == mount_stat.st_dev
                and previous_binding.get("mount_identity") == mount_identity
                and isinstance(previous_generation, str)
            ):
                with suppress(ValueError):
                    parsed = uuid.UUID(previous_generation)
                    if parsed.version == 4 and str(parsed) == previous_generation:
                        generation_id = previous_generation
        sessions_root = mountpoint / SESSIONS_DIRECTORY
        sessions_root_existed = sessions_root.exists()
        sessions_root.mkdir(mode=0o750, exist_ok=True)
        if sessions_root.is_symlink():
            raise DeviceRecordingError("unsafe_sessions_root", "会话目录不能是符号链接")
        if sessions_root.stat().st_dev != mount_stat.st_dev:
            raise DeviceRecordingError("cross_device_sessions", "会话目录不在录制卷文件系统内")
        if not sessions_root_existed:
            fsync_directory(mountpoint)
        return VolumeAdmission(
            mountpoint,
            sessions_root,
            volume_id,
            generation_id,
            mount_stat.st_dev,
            mount_identity,
            total_bytes,
            available_bytes,
            available_inodes,
        )

    def _restore_local_state(self, persisted: Mapping[str, object]) -> None:
        retained = persisted.get("retained")
        if isinstance(retained, Mapping):
            self._retained = {
                str(key): copy.deepcopy(value)
                for key, value in retained.items()
                if isinstance(key, str) and isinstance(value, dict)
            }
        safe_swap = persisted.get("safe_swap_resource")
        if isinstance(safe_swap, dict):
            receipt = safe_swap.get("receipt")
            if (
                isinstance(receipt, Mapping)
                and receipt.get("volume_id") == self._admission.volume_id
                and receipt.get("generation_id") == self._admission.generation_id
            ):
                self._safe_swap_resource = copy.deepcopy(safe_swap)
                self._released = True
        pending = persisted.get("pending_safe_swap")
        if (
            isinstance(pending, dict)
            and pending.get("volume_id") == self._admission.volume_id
            and pending.get("generation_id") == self._admission.generation_id
        ):
            self._pending_safe_swap = copy.deepcopy(pending)
        commands = persisted.get("commands")
        if isinstance(commands, list):
            for item in commands[-1024:]:
                if not isinstance(item, Mapping):
                    continue
                try:
                    scope = (
                        str(item["principal_id"]),
                        str(item["operation"]),
                        str(item["idempotency_key"]),
                    )
                    canonical = str(item["canonical_body"]).encode("utf-8")
                    status = int(item["status"])
                    body = copy.deepcopy(item.get("body"))
                except (KeyError, TypeError, ValueError):
                    continue
                if all(scope) and canonical:
                    self._commands[scope] = (
                        canonical,
                        CaptureCommandResult(status, body, replayed=False),
                    )

    def _persist_local_state(self) -> None:
        value = {
            "schema": "ylx.capture-coordinator-state.v1",
            "authority_epoch": self._authority_epoch,
            "source_revision": self._revision,
            "volume_binding": {
                "volume_id": self._admission.volume_id,
                "generation_id": self._admission.generation_id,
                "device": self._admission.device,
                "mount_identity": self._admission.mount_identity,
            },
            "retained": self._retained,
            "pending_safe_swap": self._pending_safe_swap,
            "safe_swap_resource": self._safe_swap_resource,
            "commands": [
                {
                    "principal_id": principal_id,
                    "operation": operation,
                    "idempotency_key": idempotency_key,
                    "canonical_body": canonical.decode("utf-8"),
                    "status": int(result.status),
                    "body": result.body,
                }
                for (
                    principal_id,
                    operation,
                    idempotency_key,
                ), (canonical, result) in self._commands.items()
            ],
        }
        write_json_atomic(self._state_path, value)

    def _next_revision(self) -> int:
        with self._lock:
            self._revision += 1
            return self._revision

    def _check_generation(
        self,
        generation_id: str | None = None,
        *,
        force: bool = False,
    ) -> StorageStatus:
        if generation_id is not None and generation_id != self._admission.generation_id:
            raise DeviceRecordingError("stale_generation", "录制命令来自过期挂载代次")
        if self._released:
            raise DeviceRecordingError("volume_released", "录制卷已经进入安全移除状态")
        if self._media_lost:
            raise DeviceRecordingError("media_lost", "录制介质已移除或被替换")
        mountpoint = self._admission.mountpoint
        try:
            if (
                not self._mount_checker(mountpoint)
                or mountpoint.stat().st_dev != self._admission.device
            ):
                self._media_lost = True
                raise DeviceRecordingError("media_lost", "录制介质已移除或被替换")
        except DeviceRecordingError:
            raise
        except OSError as error:
            raise DeviceRecordingError("media_lost", "录制介质不可访问") from error
        now = time.monotonic()
        with self._storage_lock:
            if (
                not force
                and self._storage_cache is not None
                and now - self._storage_checked_at < 0.25
            ):
                return self._storage_cache
        try:
            if (
                self._mount_identity(mountpoint) != self._admission.mount_identity
                or _read_volume_id(mountpoint) != self._admission.volume_id
            ):
                self._media_lost = True
                raise DeviceRecordingError("media_lost", "录制介质已移除或被替换")
            _, available_bytes, _ = _stat_capacity(mountpoint)
            writable = _mount_writable(mountpoint)
            if not writable:
                raise DeviceRecordingError("media_lost", "录制介质变为不可写")
            status = StorageStatus(available_bytes, True)
            with self._storage_lock:
                self._storage_cache = status
                self._storage_checked_at = now
            return status
        except DeviceRecordingError:
            raise
        except OSError as error:
            raise DeviceRecordingError("media_lost", "录制介质不可访问") from error

    def _recover_sessions(self) -> None:
        for partial in sorted(self._admission.sessions_root.glob("*.partial")):
            if not partial.is_dir():
                continue
            session_id = partial.name.removesuffix(".partial")
            try:
                parsed = uuid.UUID(session_id)
            except ValueError:
                continue
            if parsed.version != 7 or str(parsed) != session_id:
                continue
            manifest = partial / "manifest.json"
            controls_absent = (
                not (partial / "recording.json").exists()
                and not (partial / "capture.json").exists()
            )
            if manifest.is_file() and controls_absent:
                final = self._admission.sessions_root / session_id
                try:
                    validate_device_session_directory(
                        partial,
                        expected_session_id=session_id,
                    )
                    if final.exists():
                        raise DeviceRecordingError("session_exists", "恢复目标会话已经存在")
                    os.rename(partial, final)
                    fsync_directory(self._admission.sessions_root)
                    payload = (final / "manifest.json").read_bytes()
                    self._verified[session_id] = hashlib.sha256(payload).hexdigest()
                    continue
                except (OSError, DeviceRecordingError):
                    with suppress(OSError):
                        manifest.unlink(missing_ok=True)
            self._settle_partial(partial, session_id)

    def _settle_partial(self, partial: Path, session_id: str) -> None:
        with suppress(OSError):
            (partial / "manifest.json").unlink(missing_ok=True)
        try:
            current = json.loads((partial / "recording.json").read_bytes())
        except (OSError, ValueError, json.JSONDecodeError):
            current = None
        if not isinstance(current, dict):
            return
        if (
            current.get("authority_epoch") == self._authority_epoch
            and type(current.get("state_revision")) is int
        ):
            self._revision = max(self._revision, int(current["state_revision"]))
        if current.get("state") in {"recoverable", "failed", "abandoned"}:
            self._retained[session_id] = {
                "generation_id": self._admission.generation_id,
                "recording_state": current,
            }
            return
        now = datetime.now(UTC).isoformat().replace("+00:00", "Z")
        settled = copy.deepcopy(current)
        settled["state"] = "recoverable"
        settled["authority_epoch"] = self._authority_epoch
        settled["state_revision"] = self._next_revision()
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
        self._retained[session_id] = {
            "generation_id": self._admission.generation_id,
            "recording_state": settled,
        }

    def _catalog_sessions(self) -> None:
        for candidate in self._admission.sessions_root.iterdir():
            if not candidate.is_dir() or candidate.name.endswith(".partial"):
                continue
            try:
                parsed = uuid.UUID(candidate.name)
                if parsed.version != 7 or str(parsed) != candidate.name:
                    continue
                _, payload = inspect_device_session_directory(candidate)
                self._verified[candidate.name] = hashlib.sha256(payload).hexdigest()
            except (OSError, ValueError, DeviceRecordingError):
                continue

    def _recording_snapshot(self) -> tuple[str, object | None, object | None]:
        if self._active is not None:
            recording_state = self._active.current_recording_state
            if recording_state is None:
                return "blocked", None, None
            state = str(recording_state["state"])
            return (
                state,
                {
                    "generation_id": self._active_plan.generation_id,
                    "recording_state": recording_state,
                },
                None,
            )
        retained = None
        for candidate in reversed(self._retained.values()):
            state = candidate.get("recording_state")
            if isinstance(state, Mapping) and state.get("authority_epoch") == self._authority_epoch:
                retained = copy.deepcopy(candidate)
                break
        return "idle", None, retained

    def _snapshot(self) -> Mapping[str, object]:
        state, active, retained = self._recording_snapshot()
        return {
            "schema": "ylx.capture-snapshot-event.v2",
            "device_state": state,
            "active_recording": active,
            "retained_unsuccessful": retained,
            "runtime": copy.deepcopy(self._runtime()),
        }

    def capture_status(self) -> Mapping[str, object]:
        with self._lock:
            snapshot = self._snapshot()
            source_revision = self._revision
            recording = snapshot["active_recording"] or snapshot["retained_unsuccessful"]
            if isinstance(recording, Mapping):
                state = recording.get("recording_state")
                if isinstance(state, Mapping):
                    source_revision = int(state["state_revision"])
            return {
                "schema": "ylx.capture-status.v2",
                "authority_epoch": self._authority_epoch,
                "source_revision": source_revision,
                "snapshot": snapshot,
            }

    def capture_snapshot_event(self) -> Mapping[str, object]:
        status = self.capture_status()
        snapshot = status["snapshot"]
        session_id = None
        if isinstance(snapshot, Mapping):
            active = snapshot.get("active_recording")
            if isinstance(active, Mapping):
                state = active.get("recording_state")
                if isinstance(state, Mapping):
                    session_id = state.get("session_id")
        return {
            "authority_epoch": self._authority_epoch,
            "source_revision": status["source_revision"],
            "type": "snapshot",
            "occurred_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
            "session_id": session_id,
            "data": snapshot,
        }

    def device_descriptor(self, api_version: str, security_profile: str) -> Mapping[str, object]:
        try:
            status = self._check_generation(force=True)
            total_bytes, available_bytes, _ = _stat_capacity(self._admission.mountpoint)
            writable = status.writable
        except DeviceRecordingError:
            total_bytes = self._admission.total_bytes
            available_bytes = 0
            writable = False
        commit = self._config.session.commit
        return {
            "schema": f"ylx.device.{api_version}",
            "device": {
                "device_id": self._config.session.device_id,
                "device_label": self._config.session.device_label,
            },
            "hardware_fingerprint": self._config.session.hardware_fingerprint,
            "api_version": f"{api_version.removeprefix('v')}.0",
            "build": {
                "package_version": self._config.session.software_version,
                "commit": commit,
                "build_id": f"rp-ylx-{commit[:12]}",
            },
            "security_profile": security_profile,
            "capabilities": {
                "capture": not self._released,
                "preview": True,
                "range_download": True,
                "network_mutation": False,
            },
            "storage": {
                "volume_id": self._admission.volume_id,
                "total_bytes": total_bytes,
                "available_bytes": available_bytes,
                "writable": writable,
            },
            "runtime": copy.deepcopy(self._runtime()),
        }

    def _idempotent(
        self,
        operation: str,
        command: CaptureCommand,
        execute: Callable[[], CaptureCommandResult],
    ) -> CaptureCommandResult:
        scope = (command.principal_id, operation, command.idempotency_key)
        previous = self._commands.get(scope)
        if previous is not None:
            previous_body, result = previous
            if previous_body != command.canonical_body:
                raise ProviderError(
                    "idempotency_conflict",
                    "幂等键已经用于不同请求",
                    status=HTTPStatus.CONFLICT,
                )
            return CaptureCommandResult(result.status, copy.deepcopy(result.body), replayed=True)
        result = execute()
        self._commands[scope] = (command.canonical_body, copy.deepcopy(result))
        while len(self._commands) > 1024:
            self._commands.pop(next(iter(self._commands)))
        self._persist_local_state()
        return result

    def start_capture(self, command: CaptureCommand) -> CaptureCommandResult:
        with self._lock:
            return self._idempotent("start", command, lambda: self._start_capture(command.body))

    def _start_capture(self, body: Mapping[str, object]) -> CaptureCommandResult:
        if self._active is not None:
            raise ProviderError("capture_busy", "已有活动录制", status=HTTPStatus.CONFLICT)
        requested_volume = body.get("volume_id")
        if requested_volume is not None and requested_volume != self._admission.volume_id:
            raise ProviderError("volume_mismatch", "请求的录制卷未准入", status=HTTPStatus.CONFLICT)
        try:
            self._check_generation(force=True)
            _, available_bytes, available_inodes = _stat_capacity(self._admission.mountpoint)
            if available_bytes < self._config.minimum_available_bytes:
                raise DeviceRecordingError("insufficient_space", "录制卷可用空间不足")
            if available_inodes < self._config.minimum_available_inodes:
                raise DeviceRecordingError("insufficient_inodes", "录制卷可用 inode 不足")
        except DeviceRecordingError as error:
            raise ProviderError(error.code, error.message, status=HTTPStatus.CONFLICT) from error
        take = body.get("take")
        if not isinstance(take, Mapping):
            raise ProviderError("invalid_request", "take 无效", status=HTTPStatus.BAD_REQUEST)
        continuation_of = take.get("continuation_of") if take.get("kind") == "continue" else None
        take_id = uuid7()
        take_sequence = 1
        if continuation_of is not None:
            try:
                previous = self._load_manifest(str(continuation_of))
                previous_take = previous["take"]
                if not isinstance(previous_take, Mapping):
                    raise ValueError("take 无效")
                take_id = str(previous_take["take_id"])
                take_sequence = int(previous_take["sequence"]) + 1
            except (
                ArtifactAccessError,
                DeviceRecordingError,
                KeyError,
                TypeError,
                ValueError,
            ) as error:
                raise ProviderError(
                    "continuation_not_found",
                    "无法继续指定的已密封会话",
                    status=HTTPStatus.CONFLICT,
                ) from error
        session_id = uuid7()
        display_name = body.get("display_name")
        if not isinstance(display_name, str):
            display_name = datetime.now().astimezone().strftime("录制 %Y-%m-%d %H:%M:%S")
        plan = SessionPlan(
            session_id=session_id,
            volume_id=self._admission.volume_id,
            generation_id=self._admission.generation_id,
            capture_mode=str(body.get("mode")),
            display_name=display_name,
            take_id=take_id,
            take_sequence=take_sequence,
            continuation_of=None if continuation_of is None else str(continuation_of),
        )
        recorder = DeviceSessionRecorder(
            self._admission.sessions_root,
            self._config.session,
            plan,
            authority_epoch=self._authority_epoch,
            allocate_revision=self._next_revision,
            storage_status=lambda: self._check_generation(plan.generation_id),
            state_sink=lambda state: self._persist_local_state(),
            queue_capacity=self._config.queue_capacity,
            enqueue_timeout=self._config.enqueue_timeout,
            checkpoint_interval=self._config.checkpoint_interval,
            before_write=self._before_write,
        )
        self._active = recorder
        self._active_plan = plan
        self._safe_swap_resource = None
        self._pending_safe_swap = None
        self._preview.clear()
        try:
            recorder.start()
            if self._sources is not None:
                self._sources.start(
                    mode=plan.capture_mode,
                    generation_id=plan.generation_id,
                    submit_frame=lambda observation: self._submit_frame(
                        observation,
                        generation_id=plan.generation_id,
                        retain_failure=False,
                    ),
                    submit_imu=lambda observation: self._submit_imu(
                        observation,
                        generation_id=plan.generation_id,
                        retain_failure=False,
                    ),
                    on_failure=lambda code, message: self._source_failure(
                        plan.generation_id,
                        code,
                        message,
                    ),
                )
        except BaseException as error:
            if recorder.state in {"recording", "finalizing"}:
                recorder.fail(
                    "source_start_failed",
                    str(error),
                    recoverable=False,
                )
            state = recorder.current_recording_state
            if state is not None:
                self._retained[plan.session_id] = {
                    "generation_id": plan.generation_id,
                    "recording_state": state,
                }
            self._active = None
            self._active_plan = None
            self._preview.clear()
            self._persist_local_state()
            code = getattr(error, "code", "source_start_failed")
            message = getattr(error, "message", str(error))
            raise ProviderError(str(code), str(message), status=HTTPStatus.CONFLICT) from error
        self._persist_local_state()
        return CaptureCommandResult(HTTPStatus.ACCEPTED, self.capture_status())

    def _source_failure(self, generation_id: str, code: str, message: str) -> None:
        with self._lock:
            if (
                self._active is None
                or self._active_plan is None
                or self._active_plan.generation_id != generation_id
            ):
                return
            self._retain_active_failure(DeviceRecordingError(code, message))

    def stop_capture(self, command: CaptureCommand) -> CaptureCommandResult:
        scope = (command.principal_id, "stop", command.idempotency_key)
        with self._lock:
            previous = self._commands.get(scope)
            should_stop_sources = previous is None and self._active is not None
        if should_stop_sources and self._sources is not None:
            try:
                self._sources.stop()
            except BaseException as error:
                with self._lock:
                    if self._active is not None:
                        failure = DeviceRecordingError(
                            "source_stop_failed",
                            f"采集来源未能释放：{error}",
                        )
                        self._retain_active_failure(failure)
                raise ProviderError(
                    "source_stop_failed",
                    f"采集来源未能释放：{error}",
                    status=HTTPStatus.CONFLICT,
                ) from error
        if should_stop_sources:
            self._preview.clear()
        with self._lock:
            return self._idempotent("stop", command, lambda: self._stop_capture(command.body))

    def _stop_capture(self, body: Mapping[str, object]) -> CaptureCommandResult:
        reason = body.get("reason")
        if self._active is None:
            if reason == "safe_swap" and self._pending_safe_swap is not None:
                self._publish_safe_swap()
                return CaptureCommandResult(HTTPStatus.NO_CONTENT, None)
            return CaptureCommandResult(HTTPStatus.NO_CONTENT, None)
        recorder = self._active
        plan = self._active_plan
        assert plan is not None
        try:
            sealed = recorder.stop(
                before_publish=lambda: self._check_generation(
                    plan.generation_id,
                    force=True,
                )
            )
        except DeviceRecordingError as error:
            self._retain_active_failure(error)
            raise ProviderError(error.code, error.message, status=HTTPStatus.CONFLICT) from error
        self._verified[plan.session_id] = sealed.manifest_sha256
        self._active = None
        self._active_plan = None
        self._next_revision()
        if reason == "safe_swap":
            self._pending_safe_swap = {
                "session_id": plan.session_id,
                "volume_id": plan.volume_id,
                "generation_id": plan.generation_id,
                "manifest_id": sealed.manifest["manifest_id"],
                "manifest_sha256": sealed.manifest_sha256,
                "sealed_at": sealed.manifest["sealed_at"],
            }
            self._persist_local_state()
            self._publish_safe_swap()
        self._persist_local_state()
        if reason == "safe_swap":
            return CaptureCommandResult(HTTPStatus.NO_CONTENT, None)
        return CaptureCommandResult(HTTPStatus.ACCEPTED, self.capture_status())

    def _retain_active_failure(self, error: DeviceRecordingError) -> None:
        recorder = self._active
        plan = self._active_plan
        if recorder is None or plan is None:
            return
        self._preview.clear()
        media_lost = error.code == "media_lost"
        recorder.fail(
            error.code,
            error.message,
            recoverable=media_lost,
            media_lost=media_lost,
        )
        state = recorder.current_recording_state
        if state is None or state.get("state") not in {"recoverable", "failed", "abandoned"}:
            state = self._local_failure_state(plan, error, media_lost=media_lost)
        self._retained[plan.session_id] = {
            "generation_id": plan.generation_id,
            "recording_state": copy.deepcopy(state),
        }
        self._active = None
        self._active_plan = None
        self._persist_local_state()

    def _local_failure_state(
        self,
        plan: SessionPlan,
        error: DeviceRecordingError,
        *,
        media_lost: bool,
    ) -> Mapping[str, object]:
        now = datetime.now(UTC).isoformat().replace("+00:00", "Z")
        return {
            "schema": "ylx.recording-state.v1",
            "state": "recoverable" if media_lost else "failed",
            "authority_epoch": self._authority_epoch,
            "state_revision": self._next_revision(),
            "updated_at": now,
            "session_id": plan.session_id,
            "take_id": plan.take_id,
            "display_name": plan.display_name,
            "device": {
                "device_id": self._config.session.device_id,
                "device_label": self._config.session.device_label,
            },
            "storage": {
                "volume_id": plan.volume_id,
                "status": "media_lost" if media_lost else "mounted",
                "writable": False,
                "remaining_bytes": None,
            },
            "progress": {
                "elapsed_seconds": 0.0,
                "captured_frames": 0,
                "bytes_written": 0,
            },
            "diagnostics": [
                {
                    "code": error.code,
                    "severity": "error",
                    "message": error.message,
                    "at": now,
                    "recoverable": media_lost,
                }
            ],
        }

    def submit_frame(
        self,
        observation: FrameObservation,
        *,
        generation_id: str | None = None,
    ) -> bool:
        return self._submit_frame(
            observation,
            generation_id=generation_id,
            retain_failure=True,
        )

    def _submit_frame(
        self,
        observation: FrameObservation,
        *,
        generation_id: str | None,
        retain_failure: bool,
    ) -> bool:
        failure: DeviceRecordingError | None = None
        active_plan: SessionPlan | None = None
        with self._lock:
            if self._active is None:
                raise DeviceRecordingError("invalid_state", "当前没有活动录制")
            try:
                self._check_generation(generation_id or self._active_plan.generation_id)
                accepted = self._active.submit_frame(observation)
                preview_jpeg = observation.frame.left or observation.frame.raw_side_by_side
                if preview_jpeg is not None:
                    with suppress(ValueError):
                        self._preview.publish(preview_jpeg)
                return accepted
            except DeviceRecordingError as error:
                if error.code == "stale_generation" or not retain_failure:
                    raise
                failure = error
                active_plan = self._active_plan
        if self._sources is not None:
            with suppress(BaseException):
                self._sources.stop()
        with self._lock:
            if self._active_plan is active_plan and failure is not None:
                self._retain_active_failure(failure)
        assert failure is not None
        raise failure

    def submit_imu(
        self,
        observation: ImuObservation,
        *,
        generation_id: str | None = None,
    ) -> bool:
        return self._submit_imu(
            observation,
            generation_id=generation_id,
            retain_failure=True,
        )

    def _submit_imu(
        self,
        observation: ImuObservation,
        *,
        generation_id: str | None,
        retain_failure: bool,
    ) -> bool:
        failure: DeviceRecordingError | None = None
        active_plan: SessionPlan | None = None
        with self._lock:
            if self._active is None:
                raise DeviceRecordingError("invalid_state", "当前没有活动录制")
            try:
                self._check_generation(generation_id or self._active_plan.generation_id)
                return self._active.submit_imu(observation)
            except DeviceRecordingError as error:
                if error.code == "stale_generation" or not retain_failure:
                    raise
                failure = error
                active_plan = self._active_plan
        if self._sources is not None:
            with suppress(BaseException):
                self._sources.stop()
        with self._lock:
            if self._active_plan is active_plan and failure is not None:
                self._retain_active_failure(failure)
        assert failure is not None
        raise failure

    def report_media_loss(self, *, generation_id: str) -> None:
        with self._lock:
            if generation_id != self._admission.generation_id:
                raise DeviceRecordingError("stale_generation", "介质事件来自过期挂载代次")
            if self._active is None:
                return
            self._media_lost = True
            active_plan = self._active_plan
        if self._sources is not None:
            with suppress(BaseException):
                self._sources.stop()
        with self._lock:
            if self._active_plan is active_plan:
                error = DeviceRecordingError("media_lost", "录制介质在安全交接前被移除")
                self._retain_active_failure(error)

    def _publish_safe_swap(self) -> None:
        if self._pending_safe_swap is None:
            raise ProviderError("safe_swap_not_pending", "没有待发布的安全换盘", status=409)
        if self.open_handle_count != 0:
            raise ProviderError(
                "safe_swap_blocked",
                "仍有核心录制或读取句柄未释放",
                status=HTTPStatus.CONFLICT,
                retryable=True,
                details={"open_handle_count": self.open_handle_count},
            )
        released_at = datetime.now(UTC).isoformat().replace("+00:00", "Z")
        receipt = {
            "schema": "ylx.safe-swap-receipt.v3",
            **self._pending_safe_swap,
            "released_at": released_at,
            "release_state": "device-released",
            "open_handle_count": 0,
        }
        validate_safe_swap_v3_receipt(receipt)
        previous_resource = self._safe_swap_resource
        previous_pending = self._pending_safe_swap
        resource = {
            "schema": "ylx.safe-swap-receipt-resource.v3",
            "receipt": receipt,
        }
        self._released = True
        self._safe_swap_resource = resource
        self._pending_safe_swap = None
        self._next_revision()
        try:
            self._persist_local_state()
        except OSError as error:
            self._released = False
            self._safe_swap_resource = previous_resource
            self._pending_safe_swap = previous_pending
            raise ProviderError(
                "safe_swap_publish_failed",
                "安全换盘回执未能持久发布",
                status=HTTPStatus.INTERNAL_SERVER_ERROR,
                retryable=True,
            ) from error

    def current_safe_swap_receipt(self) -> object | None:
        with self._lock:
            return copy.deepcopy(self._safe_swap_resource)

    def artifact_io_state(self) -> str | None:
        with self._lock:
            if self._active is not None:
                return self._active.state
            if self._released or self._pending_safe_swap is not None:
                return "device-released"
            return None

    def _acquire_representation(self) -> Callable[[], None]:
        with self._lock:
            if self._released or self._pending_safe_swap is not None:
                raise ProviderError(
                    "volume_releasing",
                    "录制卷正在安全释放",
                    status=HTTPStatus.LOCKED,
                    retryable=True,
                )
            self._check_generation()
            self._open_representations += 1
        released = False

        def release() -> None:
            nonlocal released
            with self._lock:
                if not released:
                    released = True
                    self._open_representations -= 1

        return release

    def _open_store(self) -> DirectorySessionStore:
        return DirectorySessionStore(
            self._admission.sessions_root,
            verified_manifests=self._verified,
        )

    def open_manifest(self, session_id: str, api_version: str) -> object:
        release = self._acquire_representation()
        store = self._open_store()
        try:
            representation = store.open_manifest(session_id, api_version)
            return _TrackedRepresentation(representation, release)
        except Exception:
            release()
            raise
        finally:
            store.close()

    def open_verified_artifact(self, session_id: str, artifact_id: str, api_version: str) -> object:
        release = self._acquire_representation()
        store = self._open_store()
        try:
            representation = store.open_verified_artifact(session_id, artifact_id, api_version)
            return _TrackedRepresentation(representation, release)
        except Exception:
            release()
            raise
        finally:
            store.close()

    def _load_manifest(self, session_id: str) -> Mapping[str, object]:
        path = self._admission.sessions_root / session_id
        manifest, _ = inspect_device_session_directory(path)
        return manifest

    def retained_unsuccessful_outcome(self, session_id: str) -> object | None:
        with self._lock:
            outcome = self._retained.get(session_id)
            if outcome is None:
                return None
            state = outcome["recording_state"]
            return {
                "schema": "ylx.retained-unsuccessful-session-resource.v2",
                "authority_epoch": state["authority_epoch"],
                "source_revision": state["state_revision"],
                "outcome": copy.deepcopy(outcome),
            }

    def list_sessions(
        self,
        *,
        cursor: str | None,
        limit: int,
        take_id: str | None,
    ) -> Mapping[str, object]:
        try:
            self._check_generation(force=True)
        except DeviceRecordingError as error:
            raise ProviderError(
                error.code,
                error.message,
                status=HTTPStatus.CONFLICT,
                retryable=True,
            ) from error
        items: list[dict[str, object]] = []
        diagnostics: list[dict[str, object]] = []
        for candidate in sorted(
            self._admission.sessions_root.iterdir(), key=lambda path: path.name
        ):
            if not candidate.is_dir() or candidate.name.endswith(".partial"):
                continue
            if candidate.name in self._retained and candidate.name not in self._verified:
                diagnostics.append(
                    {
                        "quarantine_id": str(uuid.uuid4()),
                        "code": "manifest_invalid",
                        "observed_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
                        "message": "会话发布失败，未进入可下载 catalog",
                    }
                )
                continue
            try:
                manifest, manifest_payload = inspect_device_session_directory(candidate)
                take = manifest["take"]
                timing = manifest["time"]
                device = manifest["device"]
                if not all(isinstance(value, Mapping) for value in (take, timing, device)):
                    raise DeviceRecordingError("manifest_invalid", "会话清单结构无效")
                if take_id is not None and take["take_id"] != take_id:
                    continue
                total_bytes = sum(
                    int(artifact["bytes"])
                    for artifact in (
                        manifest["video"]["artifact"],
                        manifest["imu"]["artifact"],
                        manifest["frames"]["artifact"],
                    )
                )
                manifest_sha256 = hashlib.sha256(manifest_payload).hexdigest()
                self._verified[candidate.name] = manifest_sha256
                items.append(
                    {
                        "session_id": manifest["session_id"],
                        "producer_outcome": "sealed",
                        "take_id": take["take_id"],
                        "take_sequence": take["sequence"],
                        "continuation_of": take["continuation_of"],
                        "display_name": manifest["display_name"],
                        "device": {
                            "device_id": device["device_id"],
                            "device_label": device["device_label"],
                        },
                        "started_at": timing["started_at"],
                        "ended_at": timing["ended_at"],
                        "duration_seconds": timing["duration_seconds"],
                        "total_bytes": total_bytes,
                        "verification": {
                            "actor": "gateway",
                            "validator": {
                                "name": "rp-ylx-device-session-v1",
                                "version": "1",
                                "build_sha256": hashlib.sha256(
                                    b"rp-ylx-device-session-v1"
                                ).hexdigest(),
                            },
                            "manifest_sha256": manifest_sha256,
                            "verified_at": manifest["integrity"]["verified_at"],
                            "verdict": "usable",
                            "diagnostics": [],
                        },
                    }
                )
            except (OSError, DeviceRecordingError, KeyError, TypeError, ValueError) as error:
                diagnostics.append(
                    {
                        "quarantine_id": str(uuid.uuid4()),
                        "code": "manifest_invalid",
                        "observed_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
                        "message": str(error)[:512],
                    }
                )
        items.sort(key=lambda item: (str(item["started_at"]), str(item["session_id"])))
        combined: list[tuple[str, dict[str, object]]] = [
            (str(item["session_id"]), item) for item in items
        ]
        start = 0
        if cursor is not None:
            positions = [index for index, (key, _) in enumerate(combined) if key == cursor]
            if not positions:
                raise ProviderError("invalid_cursor", "会话游标无效", status=400)
            start = positions[0] + 1
        selected_items = [item for _, item in combined[start : start + limit]]
        remaining = limit - len(selected_items)
        selected_diagnostics = diagnostics[:remaining]
        consumed = start + len(selected_items)
        next_cursor = None
        if consumed < len(combined) and selected_items:
            next_cursor = str(selected_items[-1]["session_id"])
        return {
            "schema": "ylx.session-list.v2",
            "items": selected_items,
            "diagnostics": selected_diagnostics,
            "next_cursor": next_cursor,
        }

    def latest_preview(self, *, fps: int | None, accept: str) -> PreviewResponse:
        return self._preview.latest_preview(fps=fps, accept=accept)

    def close(self) -> None:
        if self._sources is not None:
            with suppress(BaseException):
                self._sources.stop()
        self._preview.clear()
        with self._lock:
            if self._active is not None:
                self._active.abort()
                state = self._active.current_recording_state
                if state is not None and self._active_plan is not None:
                    self._retained[self._active_plan.session_id] = {
                        "generation_id": self._active_plan.generation_id,
                        "recording_state": state,
                    }
                self._active = None
                self._active_plan = None
            self._persist_local_state()

    def __enter__(self) -> CaptureCoordinator:
        return self

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        self.close()

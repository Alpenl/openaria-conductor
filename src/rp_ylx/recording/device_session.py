"""Device Session v2 的有界写入、验证与原子封存。"""

from __future__ import annotations

import errno
import hashlib
import json
import math
import os
import queue
import secrets
import stat
import threading
import time
import uuid
from collections.abc import Callable, Mapping
from contextlib import suppress
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path, PurePosixPath
from typing import BinaryIO, Protocol
from zoneinfo import ZoneInfo

from rp_ylx.api.downloads import ArtifactAccessError, validate_device_session_manifest
from rp_ylx.camera import FrameObservation
from rp_ylx.imu import ImuObservation
from rp_ylx.native import (
    NativeModuleError,
    NativeRecordingPlan,
    NativeSessionTransaction,
    native_session_store,
)
from rp_ylx.native import (
    native_session_store_or_none as _session_store_or_none,
)
from rp_ylx.performance.metrics import PayloadLease, PerformanceMetrics
from rp_ylx.recording.stereo_encoder import (
    ClosedSegment,
    StereoEncoderError,
    StereoEncoderProcess,
    resolve_executable,
)


class DeviceRecordingError(RuntimeError):
    def __init__(self, code: str, message: str, *, retryable: bool = False) -> None:
        self.code = code
        self.message = message
        self.retryable = retryable
        super().__init__(f"{code}: {message}")


class _AudioRecorderAdapter(Protocol):
    """Explicit source-checkout test adapter; production audio is owned by SessionStore."""

    def start(self) -> None: ...

    def snapshot(self) -> Mapping[str, object]: ...

    def stop(self, timeout_seconds: float = 5.0) -> Mapping[str, object]: ...

    def abort(self) -> None: ...

    def close(self) -> None: ...


MAX_MANIFEST_BYTES = 8 * 1024 * 1024


@dataclass(frozen=True, slots=True)
class _FinalizedArtifact:
    sha256: str
    bytes: int
    identity: tuple[int, int, int, int]


@dataclass(frozen=True, slots=True)
class DeviceSessionConfig:
    device_id: str
    device_label: str
    hardware_fingerprint: str
    platform: str
    software_version: str
    commit: str
    width: int
    height: int
    sensor_fps: float
    frame_decimation: int = 1
    timezone: str = "Asia/Shanghai"
    quality_policy_id: str = "rdk-x5-lossless-v1"
    max_contiguous_dropped_frames: int = 0
    max_total_dropped_frames: int = 0
    max_drop_fraction: float = 0.0
    drop_window_seconds: float = 1.0
    max_dropped_frames_per_window: int = 0
    video_layout: str = "split-eyes"
    video_bitrate_kbps: int = 8192
    segment_seconds: float = 30.0
    audio_enabled: bool = False
    audio_device: str = "hw:0,0"
    audio_sample_rate_hz: int = 48_000
    audio_channels: int = 2
    audio_sample_format: str = "S16_LE"

    def __post_init__(self) -> None:
        try:
            device_id = uuid.UUID(self.device_id)
            ZoneInfo(self.timezone)
        except (ValueError, KeyError) as error:
            raise ValueError("设备身份或时区无效") from error
        if (
            device_id.version != 4
            or str(device_id) != self.device_id
            or len(self.device_label) != 12
            or not self.device_label.startswith("YLX-")
            or any(character not in "0123456789ABCDEF" for character in self.device_label[4:])
            or len(self.hardware_fingerprint) != 71
            or not self.hardware_fingerprint.startswith("sha256:")
            or any(
                character not in "0123456789abcdef" for character in self.hardware_fingerprint[7:]
            )
            or not self.platform
            or not self.software_version
            or len(self.software_version) > 64
            or len(self.commit) not in {40, 64}
            or any(character not in "0123456789abcdef" for character in self.commit)
            or self.width <= 0
            or self.width % 2
            or self.height <= 0
            or self.sensor_fps <= 0
            or self.frame_decimation <= 0
            or self.quality_policy_id != "rdk-x5-lossless-v1"
            or self.max_contiguous_dropped_frames != 0
            or self.max_total_dropped_frames != 0
            or self.max_drop_fraction != 0
            or self.drop_window_seconds != 1.0
            or self.max_dropped_frames_per_window != 0
            or self.video_layout != "split-eyes"
            or self.video_bitrate_kbps <= 0
            or self.segment_seconds <= 0
            or type(self.audio_enabled) is not bool
            or (self.audio_enabled and not self.audio_device)
            or self.audio_sample_rate_hz <= 0
            or self.audio_channels <= 0
            or self.audio_sample_format != "S16_LE"
        ):
            raise ValueError("Device Session 配置无效")


@dataclass(frozen=True, slots=True)
class SessionPlan:
    session_id: str
    volume_id: str
    generation_id: str
    capture_mode: str
    display_name: str
    take_id: str
    take_sequence: int
    continuation_of: str | None

    def __post_init__(self) -> None:
        if self.capture_mode not in {"production", "calibration"}:
            raise ValueError("capture_mode 无效")
        if not 1 <= len(self.display_name) <= 160 or self.take_sequence < 1:
            raise ValueError("会话显示名称或 take 序号无效")
        identities = {
            "session_id": (self.session_id, 7),
            "volume_id": (self.volume_id, 4),
            "generation_id": (self.generation_id, 4),
            "take_id": (self.take_id, 7),
        }
        if self.continuation_of is not None:
            identities["continuation_of"] = (self.continuation_of, 7)
        for name, (value, version) in identities.items():
            try:
                parsed = uuid.UUID(value)
            except ValueError as error:
                raise ValueError(f"{name} 不是规范 UUID") from error
            if parsed.version != version or str(parsed) != value:
                raise ValueError(f"{name} 不是规范 UUIDv{version}")
        if (self.take_sequence == 1) != (self.continuation_of is None):
            raise ValueError("take sequence 与 continuation_of 不一致")


@dataclass(frozen=True, slots=True)
class StorageStatus:
    remaining_bytes: int | None
    writable: bool
    status: str = "mounted"


@dataclass(frozen=True, slots=True)
class SealedDeviceSession:
    path: Path
    manifest: Mapping[str, object]
    manifest_bytes: bytes
    manifest_sha256: str


@dataclass(frozen=True, slots=True)
class _FrameEvent:
    observation: FrameObservation
    record_sequence: int
    payload_lease: PayloadLease | None = None


@dataclass(frozen=True, slots=True)
class _ImuEvent:
    observation: ImuObservation


_STOP = object()
_UUID7_LOCK = threading.Lock()
_UUID7_MILLISECOND = 0
_UUID7_COUNTER = 0
_ORIGINAL_PATH_OPEN = Path.open
_ORIGINAL_OS_FSYNC = os.fsync


def uuid7() -> str:
    """生成进程内单调的规范 UUIDv7，兼容 Python 3.11。"""

    global _UUID7_COUNTER, _UUID7_MILLISECOND
    with _UUID7_LOCK:
        current = time.time_ns() // 1_000_000
        if current > _UUID7_MILLISECOND:
            _UUID7_MILLISECOND = current
            _UUID7_COUNTER = secrets.randbits(12)
        else:
            _UUID7_COUNTER = (_UUID7_COUNTER + 1) & 0xFFF
            if _UUID7_COUNTER == 0:
                _UUID7_MILLISECOND += 1
        random_tail = secrets.randbits(62)
        value = (
            ((_UUID7_MILLISECOND & ((1 << 48) - 1)) << 80)
            | (0x7 << 76)
            | (_UUID7_COUNTER << 64)
            | (0b10 << 62)
            | random_tail
        )
    return str(uuid.UUID(int=value))


def json_bytes(value: object, *, pretty: bool = False) -> bytes:
    rendered = (
        json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True)
        if pretty
        else json.dumps(
            value,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )
    )
    return (rendered + "\n").encode()


def fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def write_json_atomic(path: Path, value: object) -> None:
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    try:
        with temporary.open("xb") as stream:
            payload = json_bytes(value, pretty=True)
            if stream.write(payload) != len(payload):
                raise OSError(f"{path.name} 发生短写")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        fsync_directory(path.parent)
    finally:
        with suppress(OSError):
            temporary.unlink(missing_ok=True)


def _digest(path: Path) -> str:
    result = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            result.update(block)
    return result.hexdigest()


def _strict_native_result_int(value: object, field: str, *, code: str) -> int:
    if type(value) is not int or value < 0:
        raise DeviceRecordingError(code, f"{field} 必须是非负整数")
    return value


def _finalize_artifact(
    path: Path,
    expected_bytes: int | None,
    *,
    code: str,
) -> _FinalizedArtifact:
    if expected_bytes is not None and expected_bytes < 0:
        raise DeviceRecordingError(code, "artifact 声明大小无效")
    native = _session_store_or_none()
    if native is not None and hasattr(native, "finalize_artifact"):
        try:
            result = native.finalize_artifact(os.fspath(path), expected_bytes)
        except BaseException as error:
            converted = _recording_error(error, "native_session_io_failed")
            raise DeviceRecordingError(code, converted.message) from error
        if not isinstance(result, Mapping):
            raise DeviceRecordingError(code, "原生 artifact 封存结果无效")
        digest = result.get("sha256")
        identity = result.get("identity")
        if (
            not isinstance(digest, str)
            or len(digest) != 64
            or any(character not in "0123456789abcdef" for character in digest)
            or not isinstance(identity, Mapping)
        ):
            raise DeviceRecordingError(code, "原生 artifact 封存结果字段无效")
        size = _strict_native_result_int(identity.get("size"), "artifact.identity.size", code=code)
        if expected_bytes is not None and size != expected_bytes:
            raise DeviceRecordingError(code, "artifact 大小与声明不一致")
        return _FinalizedArtifact(
            sha256=digest,
            bytes=size,
            identity=(
                _strict_native_result_int(
                    identity.get("device"), "artifact.identity.device", code=code
                ),
                _strict_native_result_int(
                    identity.get("inode"), "artifact.identity.inode", code=code
                ),
                size,
                _strict_native_result_int(
                    identity.get("modified_ns"), "artifact.identity.modified_ns", code=code
                ),
            ),
        )

    metadata = path.stat(follow_symlinks=False)
    if not stat.S_ISREG(metadata.st_mode):
        raise DeviceRecordingError(code, "artifact 不是普通文件")
    if expected_bytes is not None and metadata.st_size != expected_bytes:
        raise DeviceRecordingError(code, "artifact 大小与声明不一致")
    before = (metadata.st_dev, metadata.st_ino, metadata.st_size, metadata.st_mtime_ns)
    digest = _digest(path)
    metadata = path.stat(follow_symlinks=False)
    after = (metadata.st_dev, metadata.st_ino, metadata.st_size, metadata.st_mtime_ns)
    if not stat.S_ISREG(metadata.st_mode) or after != before:
        raise DeviceRecordingError(code, "artifact 在封存校验期间发生变化")
    return _FinalizedArtifact(sha256=digest, bytes=metadata.st_size, identity=after)


def _jpeg_payload(payload: bytes) -> bytes:
    start = payload.find(b"\xff\xd8")
    end = payload.rfind(b"\xff\xd9")
    if start < 0 or end < start:
        raise DeviceRecordingError("bad_frame", "原始 SBS 帧不是完整 JPEG")
    return payload[start : end + 2]


def _failure_details(error: BaseException) -> tuple[str, bool, bool]:
    code = getattr(error, "code", None)
    if code == "media_lost":
        return "media_lost", True, True
    if isinstance(error, OSError) and error.errno == errno.ENOSPC:
        return "storage_full", False, False
    if isinstance(error, OSError):
        return "write_failed", False, False
    if isinstance(code, str):
        return code, False, False
    return "seal_failed", False, False


def _recording_error(error: BaseException, fallback_code: str) -> DeviceRecordingError:
    if isinstance(error, DeviceRecordingError):
        return error
    raw = str(error)
    code, separator, message = raw.partition(": ")
    if not separator or not code.replace("_", "").isalnum():
        code, message = fallback_code, raw
    return DeviceRecordingError(code, message)


def _native_open_regular_at(root_fd: int, relative: str, *, code: str) -> int | None:
    native = _session_store_or_none()
    if native is None or not hasattr(native, "open_relative_regular"):
        return None
    try:
        descriptor = native.open_relative_regular(root_fd, relative)
    except AttributeError:
        return None
    except BaseException as error:
        converted = _recording_error(error, "native_session_io_failed")
        raise DeviceRecordingError(code, converted.message) from error
    if type(descriptor) is not int or descriptor < 0:
        raise DeviceRecordingError(code, "原生 artifact 打开返回无效 fd")
    return descriptor


def _native_read_bounded_fd(descriptor: int, maximum_bytes: int, *, code: str) -> bytes | None:
    native = _session_store_or_none()
    if native is None or not hasattr(native, "read_bounded_fd"):
        return None
    try:
        payload = native.read_bounded_fd(descriptor, maximum_bytes)
    except AttributeError:
        return None
    except BaseException as error:
        converted = _recording_error(error, "native_session_io_failed")
        raise DeviceRecordingError(code, converted.message) from error
    if not isinstance(payload, bytes):
        raise DeviceRecordingError(code, "原生 bounded read 返回无效 payload")
    if len(payload) > maximum_bytes:
        raise DeviceRecordingError(code, "文件超过允许大小")
    return payload


def _collect_manifest_artifacts_python(
    manifest: Mapping[str, object],
) -> list[Mapping[str, object]]:
    descriptors: list[Mapping[str, object]] = []

    def collect(value: object) -> None:
        if isinstance(value, Mapping):
            if "artifact_id" in value:
                descriptors.append(value)
            else:
                for child in value.values():
                    collect(child)
        elif isinstance(value, list):
            for child in value:
                collect(child)

    collect(manifest)
    return descriptors


def _manifest_artifacts(
    manifest: Mapping[str, object],
    *,
    manifest_bytes: bytes | None = None,
    session_id: str | None = None,
    code: str,
) -> list[Mapping[str, object]]:
    del manifest_bytes, session_id, code
    return _collect_manifest_artifacts_python(manifest)


def _seal_native_transaction(
    transaction: NativeSessionTransaction,
    partial: Path,
    final: Path,
    session_id: str,
    manifest_bytes: bytes,
    artifact_identities: Mapping[str, tuple[int, int, int, int]],
) -> str:
    try:
        result = transaction.seal(
            os.fspath(partial),
            os.fspath(final),
            session_id,
            manifest_bytes,
            dict(artifact_identities),
        )
    except BaseException as error:
        raise _recording_error(error, "native_session_store_failed") from error
    if not isinstance(result, Mapping):
        raise DeviceRecordingError("native_session_store_failed", "原生会话封存结果无效")
    manifest_sha256 = result.get("manifest_sha256")
    artifact_count = result.get("artifact_count")
    manifest_size = result.get("manifest_bytes")
    if (
        not isinstance(manifest_sha256, str)
        or len(manifest_sha256) != 64
        or any(character not in "0123456789abcdef" for character in manifest_sha256)
        or type(artifact_count) is not int
        or artifact_count < 0
        or type(manifest_size) is not int
        or manifest_size != len(manifest_bytes)
    ):
        raise DeviceRecordingError("native_session_store_failed", "原生会话封存结果字段无效")
    expected_sha256 = hashlib.sha256(manifest_bytes).hexdigest()
    if manifest_sha256 != expected_sha256:
        raise DeviceRecordingError("native_session_store_failed", "原生 manifest 摘要不一致")
    return manifest_sha256


def _artifact_path_and_bytes(
    descriptor: Mapping[str, object],
    *,
    code: str,
) -> tuple[str, int]:
    path = descriptor.get("path")
    bytes_value = descriptor.get("bytes")
    if (
        not isinstance(path, str)
        or isinstance(bytes_value, bool)
        or not isinstance(bytes_value, int)
        or bytes_value < 0
    ):
        raise DeviceRecordingError(code, "manifest artifact 描述符无效")
    return path, bytes_value


def manifest_artifact_bytes_total(
    manifest: Mapping[str, object],
    *,
    manifest_bytes: bytes | None = None,
    session_id: str | None = None,
    code: str,
) -> int:
    total = 0
    for descriptor in _manifest_artifacts(
        manifest,
        manifest_bytes=manifest_bytes,
        session_id=session_id,
        code=code,
    ):
        _, artifact_bytes = _artifact_path_and_bytes(descriptor, code=code)
        total += artifact_bytes
    return total


def _strict_int(value: object, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise DeviceRecordingError("audio_invalid", f"{field} 必须是非负整数")
    return value


def _native_uint(value: object, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise DeviceRecordingError("native_recording_sink_failed", f"{field} 必须是非负整数")
    return value


def _native_segment_uint(value: object, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise DeviceRecordingError(
            "native_recording_segment_planner_failed", f"{field} 必须是非负整数"
        )
    return value


def _active_take_uint(value: object, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise DeviceRecordingError("active_take_writer_failed", f"{field} 必须是非负整数")
    return value


def _active_take_float(value: object, field: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
        raise DeviceRecordingError("active_take_writer_failed", f"{field} 必须是有限数值")
    if value < 0:
        raise DeviceRecordingError("active_take_writer_failed", f"{field} 必须是非负数值")
    return float(value)


def _active_take_drop_events(value: object) -> list[dict[str, object]]:
    if not isinstance(value, list):
        raise DeviceRecordingError(
            "active_take_writer_failed", "active take drop_events 必须是列表"
        )
    events: list[dict[str, object]] = []
    previous_end = 0
    for raw in value:
        if not isinstance(raw, Mapping):
            raise DeviceRecordingError(
                "active_take_writer_failed", "active take drop event 必须是对象"
            )
        start = _active_take_uint(raw.get("start_frame"), "active_take.drop.start_frame")
        end = _active_take_uint(raw.get("end_frame"), "active_take.drop.end_frame")
        dropped = _active_take_uint(raw.get("dropped"), "active_take.drop.dropped")
        at_time = _active_take_float(raw.get("at_time_seconds"), "active_take.drop.at_time_seconds")
        reason = raw.get("reason")
        if (
            not isinstance(reason, str)
            or reason != "write_backpressure"
            or end <= start
            or dropped != end - start
            or start < previous_end
        ):
            raise DeviceRecordingError(
                "active_take_writer_failed", "active take drop event 字段不一致"
            )
        events.append(
            {
                "start_frame": start,
                "end_frame": end,
                "at_time_seconds": at_time,
                "reason": reason,
                "dropped": dropped,
            }
        )
        previous_end = end
    return events


def _native_segment_float(value: object, field: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
        raise DeviceRecordingError(
            "native_recording_segment_planner_failed", f"{field} 必须是有限数值"
        )
    if value < 0:
        raise DeviceRecordingError(
            "native_recording_segment_planner_failed", f"{field} 必须是非负数值"
        )
    return float(value)


def _native_segment_path(value: object, field: str) -> str:
    if not isinstance(value, str) or not value or len(value) > 1024 or "\\" in value:
        raise DeviceRecordingError("native_session_store_failed", f"{field} 路径无效")
    path = PurePosixPath(value)
    if path.is_absolute() or any(part in {"", ".", ".."} for part in path.parts):
        raise DeviceRecordingError("native_session_store_failed", f"{field} 路径无效")
    return value


class DeviceSessionRecorder:
    """只负责一个 Device Session 会话的 artifact 写入与成功封存。"""

    def __init__(
        self,
        generation_root: str | Path,
        config: DeviceSessionConfig,
        plan: SessionPlan,
        *,
        authority_epoch: str,
        allocate_revision: Callable[[], int],
        storage_status: Callable[[], StorageStatus],
        state_sink: Callable[[Mapping[str, object]], None] | None = None,
        queue_capacity: int = 128,
        enqueue_timeout: float = 0.05,
        checkpoint_interval: float = 1.0,
        before_write: Callable[[str, bytes], None] | None = None,
        metrics: PerformanceMetrics | None = None,
        encoder_factory: Callable[[Path], StereoEncoderProcess] | None = None,
        audio_recorder_factory: Callable[[Path], _AudioRecorderAdapter] | None = None,
        native_data_plane: bool = False,
    ) -> None:
        try:
            authority = uuid.UUID(authority_epoch)
        except ValueError as error:
            raise ValueError("authority_epoch 必须是 UUIDv4") from error
        if authority.version != 4 or str(authority) != authority_epoch:
            raise ValueError("authority_epoch 必须是规范 UUIDv4")
        if queue_capacity <= 0 or enqueue_timeout < 0 or checkpoint_interval < 0:
            raise ValueError("队列容量或入队超时无效")
        self._root = Path(generation_root)
        self._config = config
        self._plan = plan
        self._authority_epoch = authority_epoch
        self._allocate_revision = allocate_revision
        self._storage_status = storage_status
        self._state_sink = state_sink or (lambda state: None)
        if native_data_plane and (
            before_write is not None
            or encoder_factory is not None
            or audio_recorder_factory is not None
            or Path.open is not _ORIGINAL_PATH_OPEN
            or os.fsync is not _ORIGINAL_OS_FSYNC
        ):
            raise ValueError("Rust 数据面不能与测试写入适配器同时启用")
        self._native_transaction_enabled = native_data_plane
        self._before_write = before_write or (lambda role, payload: None)
        self._metrics = metrics
        self._queue: queue.Queue[object] = queue.Queue(maxsize=queue_capacity)
        self._queue_capacity = queue_capacity
        self._enqueue_timeout = enqueue_timeout
        self._checkpoint_interval = checkpoint_interval
        self._lock = threading.RLock()
        self._counter_lock = threading.Lock()
        self._state = "new"
        self._partial = self._root / f"{plan.session_id}.partial"
        self._final = self._root / plan.session_id
        self._segment_frames = max(
            1, round(config.segment_seconds * config.sensor_fps / config.frame_decimation)
        )
        self._encoder_factory = encoder_factory or self._default_encoder_factory
        self._encoder: StereoEncoderProcess | None = None
        self._audio_recorder_factory = audio_recorder_factory
        self._native_transaction: NativeSessionTransaction | None = None
        self._native_artifacts: dict[str, dict[str, object]] = {}
        self._audio_recorder: _AudioRecorderAdapter | None = None
        self._audio_result: Mapping[str, object] | None = None
        self._audio_segment_records: list[dict[str, object]] = []
        self._audio_bytes = 0
        self._harvester: threading.Thread | None = None
        self._harvest_stop = threading.Event()
        self._segment_lock = threading.Lock()
        self._segment_records: list[dict[str, object]] = []
        self._harvested_segments = 0
        self._segment_bytes = 0
        self._boundary_record_sequence: dict[int, int] = {}
        self._boundary_elapsed: dict[int, float] = {}
        self._files: dict[str, BinaryIO] = {}
        roles = ("frames.index", "imu.samples")
        self._digests = {role: hashlib.sha256() for role in roles}
        self._artifact_bytes = {role: 0 for role in self._digests}
        # Keyed by session-relative path: split-eye sessions repeat the
        # video.left / video.right roles once per segment.
        self._artifact_identities: dict[str, tuple[int, int, int, int]] = {}
        self._writer: threading.Thread | None = None
        self._writer_error: BaseException | None = None
        self._started_at: datetime | None = None
        self._started_monotonic_ns: int | None = None
        self._frames_written = 0
        self._imu_written = 0
        self._bytes_written = 0
        self._frame_domain = 0
        self._drop_events: list[dict[str, object]] = []
        self._native_direct_recording = False
        self._current_state: Mapping[str, object] | None = None
        self._last_checkpoint = 0.0

    @property
    def state(self) -> str:
        with self._lock:
            return self._state

    @property
    def partial_path(self) -> Path:
        return self._partial

    @property
    def current_recording_state(self) -> Mapping[str, object] | None:
        with self._lock:
            if self._current_state is None:
                return None
            return self._live_recording_state_locked(self._current_state)

    def native_recording_transaction(self) -> NativeSessionTransaction | None:
        with self._lock:
            if self._state != "recording" or self._native_transaction is None:
                return None
            self._native_direct_recording = True
            return self._native_transaction

    @property
    def open_handle_count(self) -> int:
        with self._lock:
            transaction_handles = (
                self._native_transaction.open_handle_count()
                if self._native_transaction is not None
                else 0
            )
            # 助手进程在卷上持有分段文件描述符，安全换盘必须等它退出。
            return (
                len(self._files)
                + int(self._writer is not None and self._writer.is_alive())
                + int(self._encoder is not None)
                + int(self._audio_recorder is not None)
                + int(self._harvester is not None and self._harvester.is_alive())
                + transaction_handles
            )

    def _now(self) -> datetime:
        return datetime.now(ZoneInfo(self._config.timezone))

    @staticmethod
    def _timestamp(value: datetime) -> str:
        return value.isoformat(timespec="microseconds")

    def _elapsed(self) -> float:
        if self._started_at is None:
            return 0.0
        return max(0.0, (self._now() - self._started_at).total_seconds())

    def _state_document(
        self,
        state: str,
        *,
        diagnostics: list[Mapping[str, object]],
        storage: StorageStatus | None = None,
    ) -> dict[str, object]:
        current_storage = storage or self._storage_status()
        progress: dict[str, object] = {
            "elapsed_seconds": self._elapsed(),
            "captured_frames": self._frames_written,
            "bytes_written": self._project_recording_bytes(self._bytes_written),
        }
        if state == "verifying":
            progress["verification"] = {"completed": 0, "total": 1}
        return {
            "schema": "ylx.recording-state.v1",
            "state": state,
            "authority_epoch": self._authority_epoch,
            "state_revision": self._allocate_revision(),
            "updated_at": self._timestamp(self._now()),
            "session_id": self._plan.session_id,
            "take_id": self._plan.take_id,
            "display_name": self._plan.display_name,
            "device": {
                "device_id": self._config.device_id,
                "device_label": self._config.device_label,
            },
            "storage": {
                "volume_id": self._plan.volume_id,
                "status": current_storage.status,
                "writable": current_storage.writable,
                "remaining_bytes": current_storage.remaining_bytes,
            },
            "progress": progress,
            "diagnostics": [dict(item) for item in diagnostics],
        }

    def _persist_state(
        self,
        state: str,
        *,
        diagnostics: list[Mapping[str, object]] | None = None,
        storage: StorageStatus | None = None,
    ) -> Mapping[str, object]:
        document = self._state_document(
            state,
            diagnostics=[] if diagnostics is None else diagnostics,
            storage=storage,
        )
        write_json_atomic(self._partial / "recording.json", document)
        with self._lock:
            self._current_state = document
        self._state_sink(document)
        return document

    def _live_audio_bytes(self) -> int:
        recorder = self._audio_recorder
        live_bytes: int | None = None
        snapshot = getattr(recorder, "snapshot", None) if recorder is not None else None
        if callable(snapshot):
            try:
                raw_snapshot = snapshot()
            except BaseException:
                raw_snapshot = None
            if isinstance(raw_snapshot, Mapping):
                with suppress(DeviceRecordingError):
                    _strict_int(raw_snapshot.get("sample_count"), "audio.snapshot.sample_count")
                    live_bytes = _strict_int(
                        raw_snapshot.get("bytes_written"), "audio.snapshot.bytes_written"
                    )
        with self._counter_lock:
            finalized_bytes = self._audio_bytes
        if live_bytes is None:
            return finalized_bytes
        return max(finalized_bytes, live_bytes)

    def _project_recording_bytes(self, base_bytes: int) -> int:
        with self._counter_lock:
            segment_bytes = self._segment_bytes
        return base_bytes + segment_bytes + self._live_audio_bytes()

    def _live_recording_state_locked(self, current: Mapping[str, object]) -> dict[str, object]:
        document = dict(current)
        raw_progress = document.get("progress")
        progress = dict(raw_progress) if isinstance(raw_progress, Mapping) else {}
        if self._state in {"recording", "finalizing", "verifying"}:
            progress["elapsed_seconds"] = self._elapsed()
        persisted_frames = progress.get("captured_frames")
        progress["captured_frames"] = max(
            persisted_frames if type(persisted_frames) is int else 0,
            self._frames_written,
        )
        progress["bytes_written"] = self._project_recording_bytes(self._bytes_written)
        if self._native_direct_recording:
            frames_written: int | None = None
            bytes_written: int | None = None
            transaction = self._native_transaction
            if transaction is not None:
                try:
                    transaction_snapshot = transaction.snapshot()
                except BaseException:
                    transaction_snapshot = None
                sink_snapshot = (
                    transaction_snapshot.get("sink")
                    if isinstance(transaction_snapshot, Mapping)
                    else None
                )
                if isinstance(sink_snapshot, Mapping):
                    with suppress(DeviceRecordingError):
                        frames_written = _native_uint(
                            sink_snapshot.get("frames_written"), "recording_sink.frames_written"
                        )
                    with suppress(DeviceRecordingError):
                        sink_bytes = _native_uint(
                            sink_snapshot.get("bytes_written"), "recording_sink.bytes_written"
                        )
                        bytes_written = self._project_recording_bytes(sink_bytes)
            if frames_written is not None:
                progress["captured_frames"] = frames_written
            if bytes_written is not None:
                progress["bytes_written"] = bytes_written
        document["progress"] = progress
        return document

    def start(self) -> Path:
        with self._lock:
            if self._state != "new":
                raise DeviceRecordingError("invalid_state", "v1 录制器只能开始一次")
            if not self._root.is_dir():
                raise DeviceRecordingError("storage_unavailable", "会话 generation 根不存在")
            if self._partial.exists() or self._final.exists():
                raise DeviceRecordingError("session_exists", "会话标识已经存在")
            self._storage_status()
            self._started_at = self._now()
            self._started_monotonic_ns = time.monotonic_ns()
            try:
                self._partial.mkdir(mode=0o750)
                (self._partial / "video").mkdir(mode=0o750)
                self._persist_state("recording")
                self._last_checkpoint = time.monotonic()
                write_json_atomic(
                    self._partial / "capture.json",
                    {
                        "schema": "ylx.capture-plan.v1",
                        "session_id": self._plan.session_id,
                        "capture_mode": self._plan.capture_mode,
                        "take_id": self._plan.take_id,
                        "take_sequence": self._plan.take_sequence,
                        "continuation_of": self._plan.continuation_of,
                        "started_at": self._timestamp(self._started_at),
                    },
                )
                if self._native_transaction_enabled:
                    try:
                        self._native_transaction = self._begin_native_transaction()
                    except NativeModuleError as error:
                        raise DeviceRecordingError(error.code, error.message) from error
                if self._native_transaction is None:
                    self._files = {
                        "frames.index": (self._partial / "frames.ndjson").open("xb"),
                        "imu.samples": (self._partial / "imu.ndjson").open("xb"),
                    }
                if self._native_transaction is None:
                    encoder = self._encoder_factory(self._partial)
                    encoder.start()
                    self._encoder = encoder
                    if self._config.audio_enabled:
                        if self._audio_recorder_factory is None:
                            raise DeviceRecordingError(
                                "test_adapter_missing",
                                "源码测试音频路径必须显式注入 audio_recorder_factory",
                            )
                        audio = self._audio_recorder_factory(self._partial)
                        audio.start()
                        self._audio_recorder = audio
                fsync_directory(self._partial)
                fsync_directory(self._root)
            except Exception as error:
                self._abandon_audio()
                self._abandon_encoder()
                self._close_files(ignore_errors=True)
                self._state = "failed"
                self._mark_failed("start_failed", str(error), recoverable=False)
                raise DeviceRecordingError(
                    "start_failed", f"创建 device-session 会话失败：{error}"
                ) from error
            self._state = "recording"
            if self._native_transaction is None:
                self._writer = threading.Thread(
                    target=self._writer_loop,
                    name=f"rp-ylx-session-writer-{self._plan.session_id[:8]}",
                    daemon=True,
                )
                self._writer.start()
            if self._encoder is not None or self._native_transaction is not None:
                # Segment hashes accumulate while recording so that stop() only
                # has to seal the trailing segment.
                self._harvester = threading.Thread(
                    target=self._harvest_loop,
                    name=f"rp-ylx-v1-segments-{self._plan.session_id[:8]}",
                    daemon=True,
                )
                self._harvester.start()
            return self._partial

    def _write(self, role: str, payload: bytes) -> None:
        started = self._metrics.start() if self._metrics is not None else 0
        try:
            self._before_write(role, payload)
            written = self._files[role].write(payload)
            if written != len(payload):
                raise OSError(f"{role} 发生短写：{written}/{len(payload)}")
        except BaseException:
            if self._metrics is not None:
                self._metrics.record_loss("write_failure")
            raise
        self._digests[role].update(payload)
        self._artifact_bytes[role] += written
        self._bytes_written += written
        if self._metrics is not None:
            self._metrics.finish("recording_write", started)

    def _writer_loop(self) -> None:
        while True:
            item = self._queue.get()
            try:
                if item is _STOP:
                    return
                if self._writer_error is not None:
                    continue
                if isinstance(item, _FrameEvent):
                    self._persist_frame(item)
                elif isinstance(item, _ImuEvent):
                    self._persist_imu(item.observation)
                else:
                    raise RuntimeError("v1 录制队列包含未知事件")
                self._checkpoint_if_due()
            except BaseException as error:
                self._writer_error = error
            finally:
                if isinstance(item, _FrameEvent) and item.payload_lease is not None:
                    item.payload_lease.release()
                self._queue.task_done()

    def _queue_put(self, item: object, timeout: float) -> bool:
        try:
            self._queue.put(item, timeout=timeout)
            return True
        except queue.Full:
            return False

    def _persist_frame(self, event: _FrameEvent) -> None:
        started = self._metrics.start() if self._metrics is not None else 0
        self._storage_status()
        frame = event.observation.frame
        if frame.raw_side_by_side is None:
            raise DeviceRecordingError("raw_frame_unavailable", "生产录制缺少相机原始 SBS MJPEG 帧")
        encoder = self._encoder
        if encoder is None:
            raise DeviceRecordingError("invalid_state", "split-eyes 编码器未启动")
        payload = _jpeg_payload(frame.raw_side_by_side)
        ordinal = self._frames_written
        if ordinal % self._segment_frames == 0:
            self._mark_segment_boundary(ordinal, event.record_sequence)
        segment_index = ordinal // self._segment_frames
        segment_frame = ordinal % self._segment_frames
        try:
            encoder.submit(payload)
        except StereoEncoderError as error:
            raise DeviceRecordingError(error.code, error.message) from error
        index_record = json_bytes(
            {
                "schema": "ylx.frame-index.v1",
                "session_id": self._plan.session_id,
                "frame": event.record_sequence,
                "source_sequence": frame.source_sequence,
                "host_monotonic_ns": frame.host_monotonic_ns,
                "segment_index": segment_index,
                "segment_frame": segment_frame,
            }
        )
        self._write("frames.index", index_record)
        self._frames_written += 1
        if self._metrics is not None:
            self._metrics.finish("recording_frame", started)

    def _mark_segment_boundary(self, ordinal: int, record_sequence: int) -> None:
        """记录编码序号到帧域序号与经过时长的对应关系，用于封存分段边界。"""

        assert self._started_monotonic_ns is not None
        elapsed = max(0.0, (time.monotonic_ns() - self._started_monotonic_ns) / 1e9)
        with self._segment_lock:
            self._boundary_record_sequence.setdefault(ordinal, record_sequence)
            self._boundary_elapsed.setdefault(ordinal, elapsed)

    def _default_encoder_factory(self, partial: Path) -> StereoEncoderProcess:
        return StereoEncoderProcess(
            partial / "video",
            width=self._config.width,
            height=self._config.height,
            fps=round(self._config.sensor_fps / self._config.frame_decimation),
            bitrate_kbps=self._config.video_bitrate_kbps,
            segment_frames=self._segment_frames,
        )

    def _begin_native_transaction(self) -> NativeSessionTransaction:
        assert self._started_monotonic_ns is not None
        plan = NativeRecordingPlan(
            session_root=str(self._partial),
            session_id=self._plan.session_id,
            encoder_executable=str(resolve_executable()),
            width=self._config.width,
            height=self._config.height,
            fps=round(self._config.sensor_fps / self._config.frame_decimation),
            bitrate_kbps=self._config.video_bitrate_kbps,
            segment_frames=self._segment_frames,
            recording_start_monotonic_ns=self._started_monotonic_ns,
            audio_enabled=self._config.audio_enabled,
            audio_device=self._config.audio_device,
            audio_sample_rate_hz=self._config.audio_sample_rate_hz,
            audio_channels=self._config.audio_channels,
            audio_segment_seconds=self._config.segment_seconds,
        )
        return native_session_store().begin_recording(plan)

    def _harvest_loop(self) -> None:
        while not self._harvest_stop.wait(0.2):
            try:
                self._harvest_segments()
            except BaseException as error:  # noqa: BLE001 - 交给写入线程统一失败
                self._writer_error = self._writer_error or error
                return

    def _harvest_segments(self) -> None:
        """对助手已封好的分段计算 SHA-256 并登记封存身份。"""

        transaction = self._native_transaction
        if transaction is not None:
            closed = tuple(
                ClosedSegment(
                    index=_native_segment_uint(item.get("index"), "segment.index"),
                    start_frame=_native_segment_uint(
                        item.get("start_frame"), "segment.start_frame"
                    ),
                    end_frame=_native_segment_uint(item.get("end_frame"), "segment.end_frame"),
                    left_path=_native_segment_path(item.get("left_path"), "segment.left_path"),
                    left_bytes=_native_segment_uint(item.get("left_bytes"), "segment.left_bytes"),
                    right_path=_native_segment_path(item.get("right_path"), "segment.right_path"),
                    right_bytes=_native_segment_uint(
                        item.get("right_bytes"), "segment.right_bytes"
                    ),
                )
                for item in transaction.segments()
            )
        else:
            encoder = self._encoder
            if encoder is None:
                return
            closed = encoder.segments
        while self._harvested_segments < len(closed):
            segment = closed[self._harvested_segments]
            self._segment_records.append(self._segment_record(segment))
            self._harvested_segments += 1

    def _segment_record(self, segment: ClosedSegment) -> dict[str, object]:
        artifacts: dict[str, object] = {}
        for eye, relative, declared in (
            ("left", segment.left_path, segment.left_bytes),
            ("right", segment.right_path, segment.right_bytes),
        ):
            path = self._partial / relative
            finalized = _finalize_artifact(path, declared, code="segment_invalid")
            self._artifact_identities[relative] = finalized.identity
            artifacts[eye] = {
                "artifact_id": finalized.sha256,
                "role": f"video.{eye}",
                "path": relative,
                "media_type": "video/mp4",
                "bytes": finalized.bytes,
                "sha256": finalized.sha256,
            }
            with self._counter_lock:
                self._segment_bytes += finalized.bytes
        record = {
            "index": segment.index,
            "start_ordinal": segment.start_frame,
            "end_ordinal": segment.end_frame,
            "artifacts": artifacts,
        }
        return record

    def _persist_imu(self, observation: ImuObservation) -> None:
        self._storage_status()
        for sample in observation.samples:
            self._write("imu.samples", json_bytes(sample.as_record(self._plan.session_id)))
            self._imu_written += 1

    def _raise_if_unavailable(self) -> None:
        if self._state != "recording":
            raise DeviceRecordingError("invalid_state", "v1 录制器当前不接受数据")
        if self._writer_error is not None:
            error = self._writer_error
            code = getattr(error, "code", "write_failed")
            raise DeviceRecordingError(str(code), f"录制写入已经失败：{error}")

    def _apply_active_take_snapshot(
        self,
        snapshot: Mapping[str, object],
        *,
        expected_frames_written: int | None = None,
    ) -> None:
        session_id = snapshot.get("session_id")
        if session_id != self._plan.session_id:
            raise DeviceRecordingError("active_take_writer_failed", "active take 会话身份不一致")
        frame_domain = _active_take_uint(snapshot.get("frame_domain"), "active_take.frame_domain")
        frames_written = _active_take_uint(
            snapshot.get("frames_written"), "active_take.frames_written"
        )
        pending_frames = _active_take_uint(
            snapshot.get("pending_frames"), "active_take.pending_frames"
        )
        drop_events = _active_take_drop_events(snapshot.get("drop_events"))
        if expected_frames_written is not None and frames_written != expected_frames_written:
            raise DeviceRecordingError(
                "active_take_writer_failed",
                "active take 写入帧数与控制面不一致",
            )
        with self._counter_lock:
            self._frame_domain = frame_domain
            self._drop_events = drop_events
            max_pending_frames = self._queue_capacity + 1
            if expected_frames_written is not None and pending_frames > max_pending_frames:
                raise DeviceRecordingError(
                    "active_take_writer_failed",
                    "active take pending 帧数超过录制队列容量",
                )

    def _reserve_frame_sequence(self, observation: FrameObservation) -> int:
        with self._counter_lock:
            if observation.dropped_before:
                raise DeviceRecordingError(
                    "source_sequence_gap",
                    f"source frame sequence has a gap of {observation.dropped_before}",
                )
            record_sequence = self._frame_domain
            self._frame_domain += 1
            return record_sequence

    def _reject_reserved_frame(self, observation: FrameObservation, record_sequence: int) -> None:
        del observation
        with self._counter_lock:
            self._record_drop(record_sequence, record_sequence + 1)

    def _record_drop(self, start: int, end: int) -> None:
        if end <= start:
            return
        event: dict[str, object] = {
            "start_frame": start,
            "end_frame": end,
            "at_time_seconds": self._elapsed(),
            "reason": "write_backpressure",
            "dropped": end - start,
        }
        if self._drop_events and self._drop_events[-1]["end_frame"] == start:
            previous = self._drop_events[-1]
            previous["end_frame"] = end
            previous["dropped"] = int(previous["dropped"]) + end - start
        else:
            self._drop_events.append(event)

    def submit_frame(self, observation: FrameObservation) -> bool:
        with self._lock:
            self._raise_if_unavailable()
            if self._native_transaction is not None:
                raise DeviceRecordingError(
                    "invalid_state", "生产帧数据面由 Rust CaptureEngine 独占"
                )
            self._storage_status()
            record_sequence = self._reserve_frame_sequence(observation)
            raw = observation.frame.raw_side_by_side
            lease = (
                self._metrics.retain_payload(
                    "recorder_reference",
                    len(raw) if raw is not None else 0,
                )
                if self._metrics is not None
                else None
            )
            if self._queue_put(
                _FrameEvent(observation, record_sequence, lease), self._enqueue_timeout
            ):
                if self._metrics is not None:
                    self._metrics.observe_queue(
                        depth=self._queue.qsize(), capacity=self._queue_capacity
                    )
                return True
            self._reject_reserved_frame(observation, record_sequence)
            if lease is not None:
                lease.release()
            if self._metrics is not None:
                self._metrics.observe_queue(
                    depth=self._queue.qsize(), capacity=self._queue_capacity, rejected=1
                )
                self._metrics.record_loss("queue_rejected")
            return False

    def submit_imu(self, observation: ImuObservation) -> bool:
        with self._lock:
            self._raise_if_unavailable()
            if self._native_transaction is not None:
                raise DeviceRecordingError(
                    "invalid_state", "生产 IMU 数据面由 Rust CaptureEngine 独占"
                )
            self._storage_status()
            if self._queue_put(_ImuEvent(observation), self._enqueue_timeout):
                return True
            if self._metrics is not None:
                self._metrics.observe_queue(
                    depth=self._queue.qsize(), capacity=self._queue_capacity, rejected=1
                )
                self._metrics.record_loss("queue_rejected")
            failure = DeviceRecordingError("imu_backpressure", "IMU 样本未能进入有界队列")
            self._writer_error = self._writer_error or failure
            raise failure

    def _checkpoint_if_due(self) -> None:
        now = time.monotonic()
        if (
            self._checkpoint_interval > 0
            and now - self._last_checkpoint < self._checkpoint_interval
        ):
            return
        try:
            self._persist_state("recording")
        except BaseException as error:
            code, _, _ = _failure_details(error)
            raise DeviceRecordingError(code, f"录制状态 checkpoint 失败：{error}") from error
        self._last_checkpoint = now

    def _stop_writer(self) -> None:
        writer = self._writer
        if writer is None:
            return
        while writer.is_alive():
            if self._queue_put(_STOP, 0.1):
                break
        writer.join(timeout=10)
        if writer.is_alive() and self._writer_error is None:
            self._writer_error = TimeoutError("v1 writer 未在期限内停止")
        self._writer = None

    _ROLE_PATHS = {
        "frames.index": "frames.ndjson",
        "imu.samples": "imu.ndjson",
    }

    def _sync_and_close_files(self) -> None:
        first_error: BaseException | None = None
        for role, stream in self._files.items():
            try:
                stream.flush()
                os.fsync(stream.fileno())
                metadata = os.fstat(stream.fileno())
                if not stat.S_ISREG(metadata.st_mode):
                    raise OSError(f"{role} 不是普通文件")
                identity = (
                    metadata.st_dev,
                    metadata.st_ino,
                    metadata.st_size,
                    metadata.st_mtime_ns,
                )
                if metadata.st_size != self._artifact_bytes[role]:
                    raise OSError(f"{role} 大小与累计写入字节不一致")
                self._artifact_identities[self._ROLE_PATHS[role]] = identity
            except BaseException as error:
                first_error = first_error or error
            finally:
                try:
                    stream.close()
                except BaseException as error:
                    first_error = first_error or error
        self._files = {}
        if first_error is not None:
            raise first_error

    def _apply_session_transaction_sink(self, result: Mapping[str, object]) -> None:
        if not isinstance(result, Mapping):
            raise DeviceRecordingError("native_recording_sink_failed", "原生录制写入结果无效")
        artifacts = result.get("artifacts")
        if not isinstance(artifacts, Mapping):
            raise DeviceRecordingError("native_recording_sink_failed", "原生录制 artifact 结果无效")
        frames_written = _native_uint(result.get("frames_written"), "recording_sink.frames_written")
        imu_written = _native_uint(
            result.get("imu_samples_written"), "recording_sink.imu_samples_written"
        )
        bytes_written = _native_uint(result.get("bytes_written"), "recording_sink.bytes_written")
        if frames_written != self._frames_written:
            raise DeviceRecordingError(
                "native_session_store_failed", "SessionStore sink 与 active take 帧数不一致"
            )
        self._imu_written = imu_written
        self._bytes_written = bytes_written
        for raw_role, raw_artifact in artifacts.items():
            role = str(raw_role)
            if not isinstance(raw_artifact, Mapping):
                raise DeviceRecordingError("native_recording_sink_failed", "原生 artifact 字段无效")
            relative = str(raw_artifact.get("path"))
            digest = str(raw_artifact.get("sha256"))
            size = _native_uint(raw_artifact.get("bytes"), f"{role}.bytes")
            identity = raw_artifact.get("identity")
            if (
                role != raw_artifact.get("role")
                or role not in self._digests
                or not relative
                or len(digest) != 64
                or any(character not in "0123456789abcdef" for character in digest)
                or not isinstance(identity, Mapping)
            ):
                raise DeviceRecordingError("native_recording_sink_failed", "原生 artifact 身份无效")
            device = _native_uint(identity.get("device"), f"{role}.identity.device")
            inode = _native_uint(identity.get("inode"), f"{role}.identity.inode")
            identity_size = _native_uint(identity.get("size"), f"{role}.identity.size")
            mtime_ns = _native_uint(identity.get("mtime_ns"), f"{role}.identity.mtime_ns")
            if identity_size != size:
                raise DeviceRecordingError(
                    "native_recording_sink_failed", "原生 artifact 大小不一致"
                )
            self._artifact_bytes[role] = size
            self._native_artifacts[role] = {
                "artifact_id": digest,
                "role": role,
                "path": relative,
                "bytes": size,
                "sha256": digest,
            }
            self._artifact_identities[relative] = (device, inode, identity_size, mtime_ns)

    def _close_files(self, *, ignore_errors: bool) -> None:
        try:
            self._sync_and_close_files()
        except BaseException:
            if not ignore_errors:
                raise

    def _diagnostic(self, code: str, message: str, *, recoverable: bool) -> dict[str, object]:
        return {
            "code": code,
            "severity": "error",
            "message": message[:1024] or code,
            "at": self._timestamp(self._now()),
            "recoverable": recoverable,
        }

    def _safe_storage(self, *, media_lost: bool) -> StorageStatus:
        if media_lost:
            return StorageStatus(None, False, "media_lost")
        try:
            current = self._storage_status()
            return StorageStatus(current.remaining_bytes, current.writable, "mounted")
        except BaseException:
            return StorageStatus(None, False, "mounted")

    def _mark_failed(
        self,
        code: str,
        message: str,
        *,
        recoverable: bool,
        media_lost: bool = False,
    ) -> Mapping[str, object] | None:
        state = "recoverable" if recoverable else "failed"
        self._state = state
        if not self._partial.exists():
            return None
        with suppress(OSError):
            (self._partial / "manifest.json").unlink(missing_ok=True)
        try:
            return self._persist_state(
                state,
                diagnostics=[self._diagnostic(code, message, recoverable=recoverable)],
                storage=self._safe_storage(media_lost=media_lost),
            )
        except OSError:
            return None

    def fail(
        self,
        code: str,
        message: str,
        *,
        recoverable: bool = False,
        media_lost: bool = False,
    ) -> Path:
        with self._lock:
            if self._state == "sealed":
                return self._final
            if self._state in {"failed", "recoverable", "abandoned"}:
                return self._partial
            self._state = "stopping"
        self._stop_writer()
        self._abandon_audio()
        self._abandon_encoder()
        self._close_files(ignore_errors=True)
        self._mark_failed(
            code,
            message,
            recoverable=recoverable,
            media_lost=media_lost,
        )
        return self._partial

    def _artifact(self, role: str, relative: str, media_type: str) -> dict[str, object]:
        native = self._native_artifacts.get(role)
        if native is not None:
            if native["path"] != relative:
                raise DeviceRecordingError(
                    "native_recording_sink_failed", "原生 artifact 路径不一致"
                )
            artifact = dict(native)
            artifact["media_type"] = media_type
            return artifact
        digest = self._digests[role].hexdigest()
        return {
            "artifact_id": digest,
            "role": role,
            "path": relative,
            "media_type": media_type,
            "bytes": self._artifact_bytes[role],
            "sha256": digest,
        }

    def _video_block(self, duration: float) -> dict[str, object]:
        if not self._segment_records:
            raise DeviceRecordingError("no_frames", "没有可封存的成片分段")
        segments: list[dict[str, object]] = []
        for record in self._segment_records:
            start_ordinal = int(record["start_ordinal"])
            end_ordinal = int(record["end_ordinal"])
            start_frame, start_time = self._boundary(start_ordinal, duration)
            end_frame, end_time = self._boundary(end_ordinal, duration)
            if end_frame <= start_frame or end_time <= start_time:
                raise DeviceRecordingError("segment_invalid", "分段帧域或时间域无效")
            segments.append(
                {
                    "index": int(record["index"]),
                    "start_frame": start_frame,
                    "end_frame": end_frame,
                    "start_time_seconds": start_time,
                    "end_time_seconds": end_time,
                    "artifacts": record["artifacts"],
                }
            )
        return {
            "layout": "split-eyes",
            "codec": "h264",
            "container": "mp4",
            "segments": segments,
        }

    def _finish_audio(self) -> None:
        recorder = self._audio_recorder
        if recorder is None:
            return
        try:
            result = recorder.stop(timeout_seconds=5.0)
        except BaseException as error:
            raise _recording_error(error, "audio_failed") from error
        self._audio_recorder = None
        self._apply_audio_result(result)

    def _apply_audio_result(self, result: object) -> None:
        if not isinstance(result, Mapping):
            raise DeviceRecordingError("audio_invalid", "原生音频结果无效")
        segments = result.get("segments")
        if not isinstance(segments, list) or not segments:
            raise DeviceRecordingError("audio_empty", "音频录制没有可封存分段")
        sample_rate_hz = _strict_int(result.get("sample_rate_hz"), "audio.sample_rate_hz")
        channels = _strict_int(result.get("channels"), "audio.channels")
        sample_format = str(result.get("sample_format"))
        if sample_rate_hz <= 0:
            raise DeviceRecordingError("audio_invalid", "audio.sample_rate_hz 必须是正整数")
        if channels <= 0:
            raise DeviceRecordingError("audio_invalid", "audio.channels 必须是正整数")
        if sample_format != "S16_LE":
            raise DeviceRecordingError("audio_invalid", "audio.sample_format 必须是 S16_LE")
        bytes_per_sample_frame = channels * 2
        self._audio_result = dict(result)
        self._audio_segment_records = []
        self._audio_bytes = 0
        expected_sample = 0
        for expected_index, raw_segment in enumerate(segments):
            if not isinstance(raw_segment, Mapping):
                raise DeviceRecordingError("audio_invalid", "音频分段记录无效")
            index = _strict_int(raw_segment.get("index"), "audio.segments[].index")
            start_sample = _strict_int(
                raw_segment.get("start_sample"), "audio.segments[].start_sample"
            )
            end_sample = _strict_int(raw_segment.get("end_sample"), "audio.segments[].end_sample")
            relative = str(raw_segment.get("path"))
            if (
                index != expected_index
                or start_sample != expected_sample
                or end_sample <= start_sample
            ):
                raise DeviceRecordingError("audio_invalid", "音频分段序号或采样域不连续")
            if not relative.startswith("audio/") or not relative.endswith(".wav"):
                raise DeviceRecordingError("audio_invalid", "音频 artifact 路径无效")
            path = self._partial / relative
            finalized = _finalize_artifact(path, None, code="audio_invalid")
            pcm_payload_bytes = (end_sample - start_sample) * bytes_per_sample_frame
            wav_header_bytes = finalized.bytes - pcm_payload_bytes
            if wav_header_bytes < 44 or wav_header_bytes > 65_536:
                raise DeviceRecordingError("audio_invalid", "音频 WAV header 与采样字节数不一致")
            self._artifact_identities[relative] = finalized.identity
            self._audio_bytes += finalized.bytes
            self._audio_segment_records.append(
                {
                    "index": index,
                    "start_sample": start_sample,
                    "end_sample": end_sample,
                    "start_time_seconds": float(raw_segment["start_time_seconds"]),
                    "end_time_seconds": float(raw_segment["end_time_seconds"]),
                    "pcm_payload_bytes": pcm_payload_bytes,
                    "wav_header_bytes": wav_header_bytes,
                    "artifact": {
                        "artifact_id": finalized.sha256,
                        "role": "audio.wav",
                        "path": relative,
                        "media_type": "audio/wav",
                        "bytes": finalized.bytes,
                        "sha256": finalized.sha256,
                    },
                }
            )
            expected_sample = end_sample
        sample_count = _strict_int(result.get("sample_count"), "audio.sample_count")
        if sample_count != expected_sample:
            raise DeviceRecordingError("audio_invalid", "音频总采样数与分段不一致")

    def _audio_block(self) -> dict[str, object]:
        if self._audio_result is None or not self._audio_segment_records:
            raise DeviceRecordingError("audio_invalid", "音频结果缺失")
        assert self._started_monotonic_ns is not None
        started_monotonic_ns = _strict_int(
            self._audio_result.get("started_monotonic_ns"), "audio.started_monotonic_ns"
        )
        stopped_monotonic_ns = _strict_int(
            self._audio_result.get("stopped_monotonic_ns"), "audio.stopped_monotonic_ns"
        )
        if stopped_monotonic_ns < started_monotonic_ns:
            raise DeviceRecordingError("audio_invalid", "音频单调时间回退")
        sample_rate_hz = _strict_int(
            self._audio_result.get("sample_rate_hz"), "audio.sample_rate_hz"
        )
        if sample_rate_hz <= 0:
            raise DeviceRecordingError("audio_invalid", "audio.sample_rate_hz 必须是正整数")
        start_offset_ns = started_monotonic_ns - self._started_monotonic_ns
        stop_offset_ns = stopped_monotonic_ns - self._started_monotonic_ns
        sync = {
            "clock": "host_monotonic",
            "timebase": "monotonic_ns",
            "session_start_monotonic_ns": self._started_monotonic_ns,
            "started_monotonic_ns": started_monotonic_ns,
            "stopped_monotonic_ns": stopped_monotonic_ns,
            "session_start_offset_ns": start_offset_ns,
            "session_stop_offset_ns": stop_offset_ns,
            "session_start_offset_seconds": start_offset_ns / 1e9,
            "session_stop_offset_seconds": stop_offset_ns / 1e9,
            "sample_duration_ns": 1_000_000_000 // sample_rate_hz,
        }
        if (
            sync.get("clock") != "host_monotonic"
            or sync.get("timebase") != "monotonic_ns"
            or sync.get("started_monotonic_ns") != started_monotonic_ns
            or sync.get("stopped_monotonic_ns") != stopped_monotonic_ns
        ):
            raise DeviceRecordingError("audio_invalid", "音频时间线结果无效")
        start_time_seconds = float(sync["session_start_offset_seconds"])
        end_time_seconds = float(sync["session_stop_offset_seconds"])
        if start_time_seconds < 0 or end_time_seconds <= start_time_seconds:
            raise DeviceRecordingError("audio_invalid", "音频时间线 offset 无效")
        return {
            "state": "recorded",
            "requested_mode": "enabled",
            "resolved_mode": "enabled",
            "codec": "pcm_s16le",
            "container": "wav",
            "sample_format": "S16_LE",
            "sample_rate": sample_rate_hz,
            "channels": _strict_int(self._audio_result.get("channels"), "audio.channels"),
            "sample_count": _strict_int(
                self._audio_result.get("sample_count"), "audio.sample_count"
            ),
            "sync": {
                "time_base": "host_monotonic",
                "start_time_seconds": start_time_seconds,
                "end_time_seconds": end_time_seconds,
                "video_time_reference": "session_time_seconds",
            },
            "segments": list(self._audio_segment_records),
        }

    def _boundary(self, ordinal: int, duration: float) -> tuple[int, float]:
        """把编码序号翻译成 manifest 使用的帧域序号与经过时长。"""

        transaction = self._native_transaction
        if transaction is not None:
            try:
                boundary = transaction.boundary(ordinal, duration)
            except BaseException as error:
                raise _recording_error(error, "native_session_store_failed") from error
            return (
                _native_segment_uint(boundary.get("frame"), "session_transaction.frame"),
                _native_segment_float(
                    boundary.get("time_seconds"), "session_transaction.time_seconds"
                ),
            )
        with self._segment_lock:
            record_sequence = self._boundary_record_sequence.get(ordinal)
            elapsed = self._boundary_elapsed.get(ordinal)
        if record_sequence is None or elapsed is None:
            raise DeviceRecordingError("segment_invalid", f"缺少分段边界 {ordinal}")
        return record_sequence, min(elapsed, duration)

    def _manifest(
        self,
        ended_at: datetime,
        verified_at: datetime,
        sealed_at: datetime,
        duration: float,
    ) -> dict[str, object]:
        video = self._video_block(duration)
        imu = self._artifact("imu.samples", "imu.ndjson", "application/x-ndjson")
        frames = self._artifact("frames.index", "frames.ndjson", "application/x-ndjson")
        assert self._started_at is not None
        dropped = sum(int(event["dropped"]) for event in self._drop_events)
        nominal_fps = self._config.sensor_fps / self._config.frame_decimation
        effective_fps = 0.0 if duration == 0 else self._frames_written / duration
        manifest: dict[str, object] = {
            "schema": "ylx.device-session.v2",
            "manifest_id": uuid7(),
            "sealed": True,
            "sealed_at": self._timestamp(sealed_at),
            "session_id": self._plan.session_id,
            "volume_id": self._plan.volume_id,
            "capture_mode": self._plan.capture_mode,
            "display_name": self._plan.display_name,
            "device": {
                "device_id": self._config.device_id,
                "device_label": self._config.device_label,
                "hardware_fingerprint": self._config.hardware_fingerprint,
                "platform": self._config.platform,
                "software_version": self._config.software_version,
                "commit": self._config.commit,
            },
            "time": {
                "started_at": self._timestamp(self._started_at),
                "ended_at": self._timestamp(ended_at),
                "timezone": self._config.timezone,
                "duration_seconds": duration,
                "duration_clock": "host_monotonic",
            },
            "take": {
                "take_id": self._plan.take_id,
                "sequence": self._plan.take_sequence,
                "continuation_of": self._plan.continuation_of,
            },
            "camera": {
                "width": self._config.width,
                "height": self._config.height,
                "eye_width": self._config.width // 2,
                "sensor_fps": self._config.sensor_fps,
                "frame_decimation": self._config.frame_decimation,
                "nominal_fps": nominal_fps,
                "effective_fps": effective_fps,
                "coordinate_frame": "opencv_optical",
            },
            "video": video,
            "imu": {
                "artifact": imu,
                "sample_count": self._imu_written,
                "units": "raw_int16",
                "coordinate_frame": "raw_device_axes",
            },
            "frames": {"artifact": frames, "count": self._frames_written},
            "logs": [],
            "integrity": {
                "verified_at": self._timestamp(verified_at),
                "dropped_frames": dropped,
                "drop_events": list(self._drop_events),
                "quality_policy": {
                    "policy_id": self._config.quality_policy_id,
                    "max_contiguous_dropped_frames": self._config.max_contiguous_dropped_frames,
                    "max_total_dropped_frames": self._config.max_total_dropped_frames,
                    "max_drop_fraction": self._config.max_drop_fraction,
                    "window_seconds": self._config.drop_window_seconds,
                    "max_dropped_frames_per_window": (self._config.max_dropped_frames_per_window),
                },
                "media_write_throughput_bytes_per_second": (
                    0
                    if duration == 0
                    else int(
                        (self._bytes_written + self._segment_bytes + self._audio_bytes) / duration
                    )
                ),
                "fatal_errors": [],
            },
        }
        manifest["audio"] = (
            self._audio_block()
            if self._config.audio_enabled
            else {
                "state": "not_recorded",
                "requested_mode": "disabled",
                "resolved_mode": "disabled",
                "reason": "user_disabled",
            }
        )
        return manifest

    def _validate_artifact_bytes(
        self,
        manifest: Mapping[str, object],
        *,
        root: Path | None = None,
        manifest_bytes: bytes | None = None,
    ) -> None:
        descriptors = _manifest_artifacts(
            manifest,
            manifest_bytes=manifest_bytes,
            session_id=str(manifest.get("session_id", "")),
            code="artifact_invalid",
        )
        for descriptor in descriptors:
            relative, expected_bytes = _artifact_path_and_bytes(
                descriptor,
                code="artifact_invalid",
            )
            expected = self._artifact_identities.get(relative)
            if expected is None:
                raise DeviceRecordingError("artifact_invalid", "artifact 缺少封存身份")
            path = (root or self._partial) / relative
            metadata = path.stat(follow_symlinks=False)
            actual = (
                metadata.st_dev,
                metadata.st_ino,
                metadata.st_size,
                metadata.st_mtime_ns,
            )
            if not stat.S_ISREG(metadata.st_mode) or actual != expected:
                raise DeviceRecordingError("digest_mismatch", "artifact 在封存前发生变化")
            if metadata.st_size != expected_bytes:
                raise DeviceRecordingError("artifact_invalid", "artifact 大小在封存前发生变化")

    def _enforce_quality_policy(self, duration: float) -> None:
        dropped = sum(int(event["dropped"]) for event in self._drop_events)
        total = self._frames_written + dropped
        fraction = 0.0 if total == 0 else dropped / total
        contiguous = max(
            (int(event["dropped"]) for event in self._drop_events),
            default=0,
        )
        window_drops = max(
            (
                sum(
                    int(other["dropped"])
                    for other in self._drop_events
                    if float(event["at_time_seconds"])
                    <= float(other["at_time_seconds"])
                    < float(event["at_time_seconds"]) + self._config.drop_window_seconds
                )
                for event in self._drop_events
            ),
            default=0,
        )
        violations: list[str] = []
        if contiguous > self._config.max_contiguous_dropped_frames:
            violations.append("contiguous")
        if dropped > self._config.max_total_dropped_frames:
            violations.append("total")
        if fraction > self._config.max_drop_fraction:
            violations.append("fraction")
        if window_drops > self._config.max_dropped_frames_per_window:
            violations.append("window")
        if violations:
            raise DeviceRecordingError(
                "drop_quality_exceeded",
                (
                    f"quality policy {self._config.quality_policy_id} rejected recording: "
                    "classification=write_backpressure "
                    f"violations={','.join(violations)} contiguous={contiguous} "
                    f"dropped={dropped} total={total} fraction={fraction:.9f} "
                    f"window_drops={window_drops} window_seconds="
                    f"{self._config.drop_window_seconds:.9f} duration={duration:.9f} "
                    "limits=contiguous:0,total:0,fraction:0,window:0"
                ),
            )

    def stop(self, *, before_publish: Callable[[], None] | None = None) -> SealedDeviceSession:
        with self._lock:
            if self._state == "sealed":
                payload = (self._final / "manifest.json").read_bytes()
                return SealedDeviceSession(
                    self._final,
                    json.loads(payload),
                    payload,
                    hashlib.sha256(payload).hexdigest(),
                )
            if self._state != "recording":
                raise DeviceRecordingError("invalid_state", "只有 recording 会话可以停止")
            self._state = "finalizing"
        published = False
        try:
            self._persist_state("finalizing")
            assert self._started_monotonic_ns is not None
            transaction = self._native_transaction
            if transaction is None:
                self._stop_writer()
                if self._writer_error is not None:
                    raise self._writer_error
                if self._frames_written == 0:
                    raise DeviceRecordingError("no_frames", "没有可封存的相机帧")
                self._finish_audio()
                self._sync_and_close_files()
            ended_at = self._now()
            duration = max(0.0, (time.monotonic_ns() - self._started_monotonic_ns) / 1e9)
            if transaction is None:
                self._finish_encoder(duration)
            else:
                self._harvest_stop.set()
                if self._harvester is not None:
                    self._harvester.join(timeout=10)
                    if self._harvester.is_alive():
                        raise DeviceRecordingError("segment_invalid", "分段校验线程未在期限内停止")
                    self._harvester = None
                try:
                    outcome = transaction.finish(duration, 30.0)
                except BaseException as error:
                    raise _recording_error(error, "native_session_store_failed") from error
                active_take = outcome.get("active_take")
                sink = outcome.get("sink")
                if not isinstance(active_take, Mapping) or not isinstance(sink, Mapping):
                    raise DeviceRecordingError(
                        "native_session_store_failed", "SessionTransaction 结果字段无效"
                    )
                self._apply_active_take_snapshot(active_take)
                self._frames_written = _active_take_uint(
                    active_take.get("frames_written"), "active_take.frames_written"
                )
                self._apply_session_transaction_sink(sink)
                if self._config.audio_enabled:
                    self._apply_audio_result(outcome.get("audio"))
                self._harvest_segments()
                if self._frames_written == 0:
                    raise DeviceRecordingError("no_frames", "没有可封存的相机帧")
            self._enforce_quality_policy(duration)
            self._persist_state("verifying")
            verified_at = self._now()
            sealed_at = self._now()
            manifest = self._manifest(ended_at, verified_at, sealed_at, duration)
            validate_device_session_manifest(manifest)
            payload = json_bytes(manifest)
            native_manifest_sha256 = None
            if before_publish is None and transaction is not None:
                native_manifest_sha256 = _seal_native_transaction(
                    transaction,
                    self._partial,
                    self._final,
                    self._plan.session_id,
                    payload,
                    self._artifact_identities,
                )
            if native_manifest_sha256 is None:
                self._validate_artifact_bytes(manifest, manifest_bytes=payload)
                if before_publish is not None:
                    before_publish()
                self._validate_artifact_bytes(manifest, manifest_bytes=payload)
                manifest_path = self._partial / "manifest.json"
                with manifest_path.open("xb") as stream:
                    self._before_write("manifest", payload)
                    self._validate_artifact_bytes(manifest, manifest_bytes=payload)
                    if stream.write(payload) != len(payload):
                        raise OSError("manifest.json 发生短写")
                    stream.flush()
                    os.fsync(stream.fileno())
                fsync_directory(self._partial)
                for control_name in ("recording.json", "capture.json"):
                    (self._partial / control_name).unlink(missing_ok=True)
                fsync_directory(self._partial)
                os.rename(self._partial, self._final)
            published = True
            with self._lock:
                self._state = "sealed"
                self._current_state = None
                self._native_transaction = None
            if native_manifest_sha256 is None:
                fsync_directory(self._root)
                self._validate_artifact_bytes(manifest, root=self._final, manifest_bytes=payload)
        except BaseException as error:
            if self._native_transaction is not None:
                with suppress(BaseException):
                    self._native_transaction.abort(str(error))
            self._abandon_audio()
            self._abandon_encoder()
            self._close_files(ignore_errors=True)
            if not published and self._final.exists() and not self._partial.exists():
                published = True
            if published:
                code, _, _ = _failure_details(error)
                raise DeviceRecordingError(
                    str(code),
                    f"device-session 会话发布后的独立校验失败：{error}",
                ) from error
            for failed_root in (self._partial,):
                with suppress(OSError):
                    (failed_root / "manifest.json").unlink(missing_ok=True)
            code, recoverable, media_lost = _failure_details(error)
            self._mark_failed(
                code,
                str(error),
                recoverable=recoverable,
                media_lost=media_lost,
            )
            raise DeviceRecordingError(
                str(code),
                f"device-session 会话封存失败：{error}",
            ) from error
        return SealedDeviceSession(
            self._final,
            manifest,
            payload,
            hashlib.sha256(payload).hexdigest(),
        )

    def _finish_encoder(self, duration: float) -> None:
        """收最后一段成片：关闭助手输入、登记尾段哈希、封住帧域边界。"""

        encoder = self._encoder
        if encoder is None:
            return
        self._harvest_stop.set()
        if self._harvester is not None:
            self._harvester.join(timeout=10)
            if self._harvester.is_alive():
                raise DeviceRecordingError("segment_invalid", "分段校验线程未在期限内停止")
            self._harvester = None
        if self._writer_error is not None:
            raise self._writer_error
        try:
            encoder.finish()
        except StereoEncoderError as error:
            raise DeviceRecordingError(error.code, error.message) from error
        if encoder.submitted_frames != self._frames_written:
            raise DeviceRecordingError("segment_invalid", "写入帧数与助手接收帧数不一致")
        with self._segment_lock:
            self._boundary_record_sequence.setdefault(self._frames_written, self._frame_domain)
            self._boundary_elapsed.setdefault(self._frames_written, duration)
        self._harvest_segments()
        covered = sum(
            int(record["end_ordinal"]) - int(record["start_ordinal"])
            for record in self._segment_records
        )
        if covered != self._frames_written:
            raise DeviceRecordingError(
                "segment_invalid",
                f"分段覆盖 {covered} 帧，与写入的 {self._frames_written} 帧不一致",
            )
        self._encoder = None

    def _abandon_encoder(self) -> None:
        self._harvest_stop.set()
        if self._harvester is not None:
            self._harvester.join(timeout=5)
            self._harvester = None
        if self._native_transaction is not None:
            with suppress(BaseException):
                self._native_transaction.abort("recording abandoned")
            self._native_transaction = None
        if self._encoder is not None:
            self._encoder.abort()
            self._encoder = None

    def _abandon_audio(self) -> None:
        if self._audio_recorder is None:
            return
        with suppress(BaseException):
            self._audio_recorder.abort()
        with suppress(BaseException):
            self._audio_recorder.close()
        self._audio_recorder = None

    def abort(self) -> Path:
        return self.fail(
            "process_interrupted",
            "进程退出前未完成正常封存",
            recoverable=True,
        )

    def __enter__(self) -> DeviceSessionRecorder:
        return self

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        if self.state in {"recording", "finalizing"}:
            self.abort()


def _open_regular_at(root_fd: int, relative: str, *, code: str) -> int:
    native_descriptor = _native_open_regular_at(root_fd, relative, code=code)
    if native_descriptor is not None:
        return native_descriptor

    path = PurePosixPath(relative)
    if (
        path.is_absolute()
        or not path.parts
        or any(part in {"", ".", ".."} for part in relative.split("/"))
    ):
        raise DeviceRecordingError(code, f"路径无效：{relative}")
    directory_flags = (
        os.O_RDONLY | os.O_CLOEXEC | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0)
    )
    file_flags = os.O_RDONLY | os.O_CLOEXEC | getattr(os, "O_NOFOLLOW", 0)
    current_fd = root_fd
    owned_directory_fd: int | None = None
    try:
        for component in path.parts[:-1]:
            next_fd = os.open(component, directory_flags, dir_fd=current_fd)
            if owned_directory_fd is not None:
                os.close(owned_directory_fd)
            owned_directory_fd = next_fd
            current_fd = next_fd
        descriptor = os.open(path.parts[-1], file_flags, dir_fd=current_fd)
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode) or metadata.st_nlink != 1:
            os.close(descriptor)
            raise DeviceRecordingError(code, f"不是独占普通文件：{relative}")
        return descriptor
    except DeviceRecordingError:
        raise
    except OSError as error:
        raise DeviceRecordingError(code, f"无法安全打开 {relative}：{error}") from error
    finally:
        if owned_directory_fd is not None:
            os.close(owned_directory_fd)


def _read_bounded_fd(descriptor: int, maximum_bytes: int, *, code: str) -> bytes:
    native_payload = _native_read_bounded_fd(descriptor, maximum_bytes, code=code)
    if native_payload is not None:
        return native_payload

    payload = bytearray()
    while len(payload) <= maximum_bytes:
        block = os.read(descriptor, min(1024 * 1024, maximum_bytes + 1 - len(payload)))
        if not block:
            break
        payload.extend(block)
    if len(payload) > maximum_bytes:
        raise DeviceRecordingError(code, "文件超过允许大小")
    return bytes(payload)


def _verify_artifact_fd(descriptor: int, expected_bytes: int, expected_sha256: str) -> None:
    native = _session_store_or_none()
    if native is not None and hasattr(native, "verify_fd"):
        try:
            native.verify_fd(descriptor, expected_bytes, expected_sha256)
            return
        except BaseException as error:
            raise _recording_error(error, "native_session_io_failed") from error
    before = os.fstat(descriptor)
    if before.st_size != expected_bytes:
        raise DeviceRecordingError("artifact_invalid", "artifact 大小不匹配")
    digest = hashlib.sha256()
    remaining = expected_bytes
    while remaining:
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


def inspect_device_session_directory(
    path: str | Path,
    *,
    expected_session_id: str | None = None,
) -> tuple[Mapping[str, object], bytes]:
    """校验 sealed manifest 和 artifact 身份/大小，不读取 artifact 内容。"""

    root = Path(path)
    root_flags = (
        os.O_RDONLY | os.O_CLOEXEC | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0)
    )
    try:
        root_fd = os.open(root, root_flags)
    except OSError as error:
        raise DeviceRecordingError("manifest_invalid", f"会话目录不可安全打开：{error}") from error
    try:
        manifest_fd = _open_regular_at(root_fd, "manifest.json", code="manifest_invalid")
        try:
            metadata = os.fstat(manifest_fd)
            if metadata.st_size > MAX_MANIFEST_BYTES:
                raise DeviceRecordingError("manifest_invalid", "manifest 超过允许大小")
            payload = _read_bounded_fd(manifest_fd, MAX_MANIFEST_BYTES, code="manifest_invalid")
        finally:
            os.close(manifest_fd)
        manifest = json.loads(payload)
        selected_session_id = root.name if expected_session_id is None else expected_session_id
        if not isinstance(manifest, dict) or manifest.get("session_id") != selected_session_id:
            raise DeviceRecordingError("manifest_invalid", "manifest 会话身份无效")
        validate_device_session_manifest(manifest)

        descriptors = _manifest_artifacts(
            manifest,
            manifest_bytes=payload,
            session_id=selected_session_id,
            code="manifest_invalid",
        )
        for artifact in descriptors:
            relative, expected_bytes = _artifact_path_and_bytes(
                artifact,
                code="artifact_invalid",
            )
            artifact_fd = _open_regular_at(
                root_fd,
                relative,
                code="artifact_invalid",
            )
            try:
                if os.fstat(artifact_fd).st_size != expected_bytes:
                    raise DeviceRecordingError("artifact_invalid", "artifact 大小不匹配")
            finally:
                os.close(artifact_fd)
        return manifest, payload
    except ArtifactAccessError as error:
        raise DeviceRecordingError("manifest_invalid", str(error)) from error
    except (OSError, ValueError, json.JSONDecodeError) as error:
        raise DeviceRecordingError("manifest_invalid", str(error)) from error
    finally:
        os.close(root_fd)


def validate_device_session_directory(
    path: str | Path,
    *,
    expected_session_id: str | None = None,
) -> Mapping[str, object]:
    """独立校验一个已密封 Device Session 会话的 manifest 与所有 artifact 字节。"""

    root = Path(path)
    root_flags = (
        os.O_RDONLY | os.O_CLOEXEC | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0)
    )
    try:
        try:
            root_fd = os.open(root, root_flags)
        except OSError as error:
            raise DeviceRecordingError(
                "manifest_invalid", f"会话目录不可安全打开：{error}"
            ) from error
        try:
            manifest_fd = _open_regular_at(root_fd, "manifest.json", code="manifest_invalid")
            try:
                metadata = os.fstat(manifest_fd)
                if metadata.st_size > MAX_MANIFEST_BYTES:
                    raise DeviceRecordingError("manifest_invalid", "manifest 超过允许大小")
                payload = _read_bounded_fd(manifest_fd, MAX_MANIFEST_BYTES, code="manifest_invalid")
            finally:
                os.close(manifest_fd)
            manifest = json.loads(payload)
            selected_session_id = root.name if expected_session_id is None else expected_session_id
            if not isinstance(manifest, dict) or manifest.get("session_id") != selected_session_id:
                raise DeviceRecordingError("manifest_invalid", "manifest 会话身份无效")
            validate_device_session_manifest(manifest)
            descriptors = _manifest_artifacts(
                manifest,
                manifest_bytes=payload,
                session_id=selected_session_id,
                code="manifest_invalid",
            )
            for artifact_descriptor in descriptors:
                relative, expected_bytes = _artifact_path_and_bytes(
                    artifact_descriptor,
                    code="artifact_invalid",
                )
                expected_sha256 = artifact_descriptor.get("sha256")
                if not isinstance(expected_sha256, str):
                    raise DeviceRecordingError("artifact_invalid", "manifest artifact 描述符无效")
                artifact_fd = _open_regular_at(
                    root_fd,
                    relative,
                    code="artifact_invalid",
                )
                try:
                    _verify_artifact_fd(
                        artifact_fd,
                        expected_bytes,
                        expected_sha256,
                    )
                finally:
                    os.close(artifact_fd)
            return manifest
        finally:
            os.close(root_fd)
    except ArtifactAccessError as error:
        raise DeviceRecordingError("manifest_invalid", str(error)) from error
    except (OSError, ValueError, json.JSONDecodeError) as error:
        raise DeviceRecordingError("manifest_invalid", str(error)) from error

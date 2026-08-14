"""Device Session v1 的有界写入、验证与原子封存。"""

from __future__ import annotations

import errno
import hashlib
import json
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
from typing import BinaryIO
from zoneinfo import ZoneInfo

from rp_ylx.api.downloads import ArtifactAccessError, validate_device_session_manifest
from rp_ylx.camera import FrameObservation
from rp_ylx.imu import ImuObservation


class DeviceRecordingError(RuntimeError):
    def __init__(self, code: str, message: str, *, retryable: bool = False) -> None:
        self.code = code
        self.message = message
        self.retryable = retryable
        super().__init__(f"{code}: {message}")


MAX_MANIFEST_BYTES = 8 * 1024 * 1024


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


@dataclass(frozen=True, slots=True)
class _ImuEvent:
    observation: ImuObservation


_STOP = object()
_UUID7_LOCK = threading.Lock()
_UUID7_MILLISECOND = 0
_UUID7_COUNTER = 0


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


class DeviceSessionRecorder:
    """只负责一个 v1 会话的 artifact 写入与成功封存。"""

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
        self._before_write = before_write or (lambda role, payload: None)
        self._queue: queue.Queue[object] = queue.Queue(maxsize=queue_capacity)
        self._enqueue_timeout = enqueue_timeout
        self._checkpoint_interval = checkpoint_interval
        self._lock = threading.RLock()
        self._counter_lock = threading.Lock()
        self._state = "new"
        self._partial = self._root / f"{plan.session_id}.partial"
        self._final = self._root / plan.session_id
        self._files: dict[str, BinaryIO] = {}
        self._digests = {
            role: hashlib.sha256()
            for role in ("video.raw-side-by-side", "frames.index", "imu.samples")
        }
        self._artifact_bytes = {role: 0 for role in self._digests}
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
            return None if self._current_state is None else dict(self._current_state)

    @property
    def open_handle_count(self) -> int:
        with self._lock:
            return len(self._files) + int(self._writer is not None and self._writer.is_alive())

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
            "bytes_written": self._bytes_written,
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
                self._files = {
                    "video.raw-side-by-side": (self._partial / "video/raw-sbs.mjpeg").open("xb"),
                    "frames.index": (self._partial / "frames.ndjson").open("xb"),
                    "imu.samples": (self._partial / "imu.ndjson").open("xb"),
                }
                fsync_directory(self._partial)
                fsync_directory(self._root)
            except Exception as error:
                self._close_files(ignore_errors=True)
                self._state = "failed"
                self._mark_failed("start_failed", str(error), recoverable=False)
                raise DeviceRecordingError("start_failed", f"创建 v1 会话失败：{error}") from error
            self._state = "recording"
            self._writer = threading.Thread(
                target=self._writer_loop,
                name=f"rp-ylx-v1-writer-{self._plan.session_id[:8]}",
                daemon=True,
            )
            self._writer.start()
            return self._partial

    def _write(self, role: str, payload: bytes) -> None:
        self._before_write(role, payload)
        written = self._files[role].write(payload)
        if written != len(payload):
            raise OSError(f"{role} 发生短写：{written}/{len(payload)}")
        self._digests[role].update(payload)
        self._artifact_bytes[role] += written
        self._bytes_written += written

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
                self._queue.task_done()

    def _persist_frame(self, event: _FrameEvent) -> None:
        self._storage_status()
        frame = event.observation.frame
        if frame.raw_side_by_side is None:
            raise DeviceRecordingError("raw_frame_unavailable", "生产录制缺少相机原始 SBS MJPEG 帧")
        payload = _jpeg_payload(frame.raw_side_by_side)
        offset = self._files["video.raw-side-by-side"].tell()
        self._write("video.raw-side-by-side", payload)
        self._write(
            "frames.index",
            json_bytes(
                {
                    "schema": "ylx.frame-index.v1",
                    "session_id": self._plan.session_id,
                    "frame": event.record_sequence,
                    "source_sequence": frame.source_sequence,
                    "host_monotonic_ns": frame.host_monotonic_ns,
                    "video_offset": offset,
                    "video_bytes": len(payload),
                }
            ),
        )
        self._frames_written += 1

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
            self._storage_status()
            with self._counter_lock:
                if observation.dropped_before:
                    raise DeviceRecordingError(
                        "source_sequence_gap",
                        f"source frame sequence has a gap of {observation.dropped_before}",
                    )
                record_sequence = self._frame_domain
                self._frame_domain += 1
            try:
                self._queue.put(
                    _FrameEvent(observation, record_sequence), timeout=self._enqueue_timeout
                )
                return True
            except queue.Full:
                with self._counter_lock:
                    self._record_drop(record_sequence, record_sequence + 1)
                return False

    def submit_imu(self, observation: ImuObservation) -> bool:
        with self._lock:
            self._raise_if_unavailable()
            self._storage_status()
            try:
                self._queue.put(_ImuEvent(observation), timeout=self._enqueue_timeout)
                return True
            except queue.Full:
                return False

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
            try:
                self._queue.put(_STOP, timeout=0.1)
                break
            except queue.Full:
                continue
        writer.join(timeout=10)
        if writer.is_alive() and self._writer_error is None:
            self._writer_error = TimeoutError("v1 writer 未在期限内停止")
        self._writer = None

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
                self._artifact_identities[role] = identity
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
        self._close_files(ignore_errors=True)
        self._mark_failed(
            code,
            message,
            recoverable=recoverable,
            media_lost=media_lost,
        )
        return self._partial

    def _artifact(self, role: str, relative: str, media_type: str) -> dict[str, object]:
        digest = self._digests[role].hexdigest()
        return {
            "artifact_id": digest,
            "role": role,
            "path": relative,
            "media_type": media_type,
            "bytes": self._artifact_bytes[role],
            "sha256": digest,
        }

    def _manifest(
        self,
        ended_at: datetime,
        verified_at: datetime,
        sealed_at: datetime,
        duration: float,
    ) -> dict[str, object]:
        video = self._artifact(
            "video.raw-side-by-side", "video/raw-sbs.mjpeg", "video/x-motion-jpeg"
        )
        imu = self._artifact("imu.samples", "imu.ndjson", "application/x-ndjson")
        frames = self._artifact("frames.index", "frames.ndjson", "application/x-ndjson")
        assert self._started_at is not None
        dropped = sum(int(event["dropped"]) for event in self._drop_events)
        nominal_fps = self._config.sensor_fps / self._config.frame_decimation
        effective_fps = 0.0 if duration == 0 else self._frames_written / duration
        return {
            "schema": "ylx.device-session.v1",
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
            "video": {
                "layout": "raw-side-by-side",
                "codec": "mjpeg",
                "continuous": True,
                "artifact": video,
            },
            "imu": {
                "artifact": imu,
                "sample_count": self._imu_written,
                "units": "raw_int16",
                "coordinate_frame": "opencv_optical",
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
                    0 if duration == 0 else int(self._bytes_written / duration)
                ),
                "fatal_errors": [],
            },
        }

    def _validate_artifact_bytes(
        self,
        manifest: Mapping[str, object],
        *,
        root: Path | None = None,
    ) -> None:
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
        for descriptor in descriptors:
            role = str(descriptor["role"])
            expected = self._artifact_identities.get(role)
            if expected is None:
                raise DeviceRecordingError("artifact_invalid", "artifact 缺少封存身份")
            path = (root or self._partial) / str(descriptor["path"])
            metadata = path.stat(follow_symlinks=False)
            actual = (
                metadata.st_dev,
                metadata.st_ino,
                metadata.st_size,
                metadata.st_mtime_ns,
            )
            if not stat.S_ISREG(metadata.st_mode) or actual != expected:
                raise DeviceRecordingError("digest_mismatch", "artifact 在封存前发生变化")
            if metadata.st_size != descriptor["bytes"]:
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
            self._stop_writer()
            if self._writer_error is not None:
                raise self._writer_error
            self._sync_and_close_files()
            if self._frames_written == 0:
                raise DeviceRecordingError("no_frames", "没有可封存的相机帧")
            ended_at = self._now()
            assert self._started_monotonic_ns is not None
            duration = max(0.0, (time.monotonic_ns() - self._started_monotonic_ns) / 1e9)
            self._enforce_quality_policy(duration)
            self._persist_state("verifying")
            verified_at = self._now()
            sealed_at = self._now()
            manifest = self._manifest(ended_at, verified_at, sealed_at, duration)
            validate_device_session_manifest(manifest)
            self._validate_artifact_bytes(manifest)
            if before_publish is not None:
                before_publish()
            self._validate_artifact_bytes(manifest)
            payload = json_bytes(manifest)
            manifest_path = self._partial / "manifest.json"
            with manifest_path.open("xb") as stream:
                self._before_write("manifest", payload)
                self._validate_artifact_bytes(manifest)
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
            fsync_directory(self._root)
            self._validate_artifact_bytes(manifest, root=self._final)
        except BaseException as error:
            self._close_files(ignore_errors=True)
            if published:
                code, _, _ = _failure_details(error)
                raise DeviceRecordingError(
                    str(code),
                    f"v1 会话发布后的独立校验失败：{error}",
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
            raise DeviceRecordingError(str(code), f"v1 会话封存失败：{error}") from error
        return SealedDeviceSession(
            self._final,
            manifest,
            payload,
            hashlib.sha256(payload).hexdigest(),
        )

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
        for artifact in descriptors:
            artifact_fd = _open_regular_at(
                root_fd,
                str(artifact["path"]),
                code="artifact_invalid",
            )
            try:
                if os.fstat(artifact_fd).st_size != int(artifact["bytes"]):
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
    """独立校验一个已密封 v1 会话的 manifest 与所有 artifact 字节。"""

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
            for artifact_descriptor in descriptors:
                artifact_fd = _open_regular_at(
                    root_fd, str(artifact_descriptor["path"]), code="artifact_invalid"
                )
                try:
                    _verify_artifact_fd(
                        artifact_fd,
                        int(artifact_descriptor["bytes"]),
                        str(artifact_descriptor["sha256"]),
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

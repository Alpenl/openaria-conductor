"""将相机和 IMU 观察写成 RP-03 会话并原子封存。"""

from __future__ import annotations

import hashlib
import json
import os
import queue
import threading
import time
import uuid
from collections.abc import Callable, Mapping
from contextlib import suppress
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, BinaryIO

from rp_ylx.camera import FrameObservation
from rp_ylx.contracts import validate_session
from rp_ylx.contracts.frame_stream import MAGIC, encode_frame
from rp_ylx.imu import ImuObservation
from rp_ylx.performance.metrics import PayloadLease, PerformanceMetrics


class RecordingError(RuntimeError):
    def __init__(self, code: str, message: str, *, retryable: bool = False) -> None:
        self.code = code
        self.message = message
        self.retryable = retryable
        super().__init__(f"{code}: {message}")


@dataclass(frozen=True, slots=True)
class RecordingConfig:
    device_id: str
    software_version: str
    width: int
    height: int
    fps: float
    encoding: str
    device_tick_hz: float | None = None
    imu_ranges: Mapping[str, Any] | None = None

    def __post_init__(self) -> None:
        if (
            not self.device_id
            or not self.software_version
            or self.width <= 0
            or self.height <= 0
            or self.fps <= 0
            or not self.encoding
        ):
            raise ValueError("录制配置字段无效")
        if self.device_tick_hz is not None and self.device_tick_hz <= 0:
            raise ValueError("IMU tick 频率必须大于零或为 None")


@dataclass(frozen=True, slots=True)
class _FrameEvent:
    observation: FrameObservation
    payload_lease: PayloadLease | None = None


@dataclass(frozen=True, slots=True)
class _ImuEvent:
    observation: ImuObservation


_STOP = object()


def _utc_now() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def _json_bytes(value: object, *, pretty: bool = False) -> bytes:
    if pretty:
        rendered = json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True)
    else:
        rendered = json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
    return (rendered + "\n").encode()


def _digest(path: Path) -> str:
    result = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            result.update(block)
    return result.hexdigest()


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _write_json_atomic(path: Path, value: object) -> None:
    temporary = path.with_name(f".{path.name}.tmp")
    with temporary.open("wb") as stream:
        stream.write(_json_bytes(value, pretty=True))
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, path)
    _fsync_directory(path.parent)


class SessionRecorder:
    """单会话录制器；生产者提交有界事件，writer 线程串行持久化。"""

    def __init__(
        self,
        root: str | Path,
        config: RecordingConfig,
        *,
        queue_capacity: int = 128,
        enqueue_timeout: float = 0.05,
        before_write: Callable[[str, bytes], None] | None = None,
        metrics: PerformanceMetrics | None = None,
    ) -> None:
        if queue_capacity <= 0 or enqueue_timeout < 0:
            raise ValueError("队列容量必须大于零，入队超时不能为负")
        self._root = Path(root)
        self._config = config
        self._queue: queue.Queue[object] = queue.Queue(maxsize=queue_capacity)
        self._enqueue_timeout = enqueue_timeout
        self._before_write = before_write or (lambda role, payload: None)
        self._metrics = metrics
        self._queue_capacity = queue_capacity
        self._state_lock = threading.RLock()
        self._counter_lock = threading.Lock()
        self._state = "new"
        self._session_id: str | None = None
        self._partial_path: Path | None = None
        self._final_path: Path | None = None
        self._started_at: str | None = None
        self._files: dict[str, BinaryIO] = {}
        self._writer: threading.Thread | None = None
        self._writer_error: Exception | None = None
        self._frames_written = 0
        self._imu_written = 0
        self._diagnostics_written = 0
        self._dropped_frames = 0
        self._source_gaps = 0
        self._queue_rejected_frames = 0
        self._dropped_imu = 0

    @property
    def state(self) -> str:
        with self._state_lock:
            return self._state

    @property
    def session_id(self) -> str | None:
        return self._session_id

    @property
    def partial_path(self) -> Path | None:
        return self._partial_path

    def _require_partial(self) -> Path:
        if self._partial_path is None:
            raise RecordingError("invalid_state", "会话尚未开始")
        return self._partial_path

    def _require_session_id(self) -> str:
        if self._session_id is None:
            raise RecordingError("invalid_state", "会话尚未开始")
        return self._session_id

    def _session_state(self, state: str, *, failure: dict[str, str] | None) -> dict[str, Any]:
        return {
            "format": "ylx.recording-session-state.v0",
            "session_id": self._require_session_id(),
            "state": state,
            "started_at": self._started_at,
            "updated_at": _utc_now(),
            "failure": failure,
        }

    def start(self, *, session_id: str | None = None) -> Path:
        with self._state_lock:
            if self._state != "new":
                raise RecordingError("invalid_state", "录制器只能开始一次")
            if not self._root.is_dir():
                raise RecordingError("storage_unavailable", "录制根目录不存在或不是目录")
            selected = session_id or str(uuid.uuid4())
            try:
                parsed = uuid.UUID(selected)
            except ValueError as exc:
                raise RecordingError("invalid_session_id", "session_id 必须是规范 UUID") from exc
            if str(parsed) != selected:
                raise RecordingError("invalid_session_id", "session_id 必须是小写规范 UUID")
            self._session_id = selected
            self._partial_path = self._root / f"{selected}.partial"
            self._final_path = self._root / selected
            if self._partial_path.exists() or self._final_path.exists():
                raise RecordingError("session_exists", "同一 session_id 已经存在")
            self._started_at = _utc_now()
            try:
                self._partial_path.mkdir(mode=0o750)
                (self._partial_path / "video").mkdir(mode=0o750)
                _write_json_atomic(
                    self._partial_path / "session.json",
                    self._session_state("recording", failure=None),
                )
                self._files = {
                    "video.left": (self._partial_path / "video/left.bin").open("xb"),
                    "video.right": (self._partial_path / "video/right.bin").open("xb"),
                    "frames.timeline": (self._partial_path / "frames.ndjson").open("xb"),
                    "imu.samples": (self._partial_path / "imu.ndjson").open("xb"),
                    "diagnostics.events": (self._partial_path / "diagnostics.ndjson").open("xb"),
                }
                self._write("video.left", MAGIC)
                self._write("video.right", MAGIC)
                self._write_diagnostic("info", "recording_started", "录制开始", 1)
            except Exception as exc:
                self._close_files(ignore_errors=True)
                self._mark_failed("start_failed", str(exc))
                raise RecordingError("start_failed", f"创建录制会话失败：{exc}") from exc
            self._state = "recording"
            self._writer = threading.Thread(
                target=self._writer_loop,
                name=f"rp-ylx-writer-{selected[:8]}",
                daemon=True,
            )
            self._writer.start()
            return self._partial_path

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
        if self._metrics is not None:
            self._metrics.finish("recording_write", started)

    def _write_diagnostic(self, severity: str, code: str, message: str, count: int) -> None:
        record = {
            "format": "ylx.diagnostic.v0",
            "session_id": self._require_session_id(),
            "monotonic_ns": time.monotonic_ns(),
            "severity": severity,
            "code": code,
            "message": message,
            "count": count,
        }
        self._write("diagnostics.events", _json_bytes(record))
        self._diagnostics_written += 1

    def _writer_loop(self) -> None:
        while True:
            item = self._queue.get()
            try:
                if item is _STOP:
                    return
                if self._writer_error is not None:
                    continue
                if isinstance(item, _FrameEvent):
                    self._persist_frame(item.observation)
                elif isinstance(item, _ImuEvent):
                    self._persist_imu(item.observation)
                else:
                    raise RuntimeError("录制队列出现未知事件")
            except Exception as exc:
                self._writer_error = exc
            finally:
                if isinstance(item, _FrameEvent) and item.payload_lease is not None:
                    item.payload_lease.release()
                self._queue.task_done()

    def _persist_frame(self, observation: FrameObservation) -> None:
        frame = observation.frame
        started = self._metrics.start() if self._metrics is not None else 0
        self._write("video.left", encode_frame(frame.left))
        self._write("video.right", encode_frame(frame.right))
        record = {
            "format": "ylx.frame.v0",
            "session_id": self._require_session_id(),
            "sequence": self._frames_written,
            "source_sequence": frame.source_sequence,
            "host_monotonic_ns": frame.host_monotonic_ns,
        }
        self._write("frames.timeline", _json_bytes(record))
        self._frames_written += 1
        if self._metrics is not None:
            self._metrics.finish("recording_frame", started)

    def _persist_imu(self, observation: ImuObservation) -> None:
        session_id = self._require_session_id()
        for sample in observation.samples:
            self._write("imu.samples", _json_bytes(sample.as_record(session_id)))
            self._imu_written += 1

    def _raise_if_unavailable(self) -> None:
        if self._state != "recording":
            raise RecordingError("invalid_state", "录制器不接受新数据")
        if self._writer_error is not None:
            raise RecordingError("write_failed", f"录制写入已经失败：{self._writer_error}")

    def submit_frame(self, observation: FrameObservation) -> bool:
        with self._state_lock:
            self._raise_if_unavailable()
            with self._counter_lock:
                self._dropped_frames += observation.dropped_before
                self._source_gaps += observation.dropped_before
            lease = (
                self._metrics.retain_payload(
                    "recorder_reference",
                    len(observation.frame.left) + len(observation.frame.right),
                )
                if self._metrics is not None
                else None
            )
            try:
                self._queue.put(_FrameEvent(observation, lease), timeout=self._enqueue_timeout)
                if self._metrics is not None:
                    self._metrics.observe_queue(
                        depth=self._queue.qsize(), capacity=self._queue_capacity
                    )
                return True
            except queue.Full:
                with self._counter_lock:
                    self._dropped_frames += 1
                    self._queue_rejected_frames += 1
                if lease is not None:
                    lease.release()
                if self._metrics is not None:
                    self._metrics.observe_queue(
                        depth=self._queue.qsize(), capacity=self._queue_capacity, rejected=1
                    )
                    self._metrics.record_loss("queue_rejected")
                return False

    def submit_imu(self, observation: ImuObservation) -> bool:
        with self._state_lock:
            self._raise_if_unavailable()
            with self._counter_lock:
                self._dropped_imu += observation.dropped_samples
            try:
                self._queue.put(_ImuEvent(observation), timeout=self._enqueue_timeout)
                return True
            except queue.Full:
                with self._counter_lock:
                    self._dropped_imu += len(observation.samples)
                return False

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
            self._writer_error = TimeoutError("writer 线程未在 10 秒内停止")
        self._writer = None

    def _sync_and_close_files(self) -> None:
        first_error: Exception | None = None
        for stream in self._files.values():
            try:
                stream.flush()
                os.fsync(stream.fileno())
            except Exception as exc:
                first_error = first_error or exc
            finally:
                try:
                    stream.close()
                except Exception as exc:
                    first_error = first_error or exc
        self._files = {}
        if first_error is not None:
            raise first_error

    def _close_files(self, *, ignore_errors: bool) -> None:
        try:
            self._sync_and_close_files()
        except Exception:
            if not ignore_errors:
                raise

    def _mark_failed(self, code: str, message: str, *, state: str = "failed") -> None:
        self._state = state
        if self._partial_path is None or not self._partial_path.exists():
            return
        with suppress(OSError):
            _write_json_atomic(
                self._partial_path / "session.json",
                self._session_state(state, failure={"code": code, "message": message[:1000]}),
            )

    def _artifact(self, role: str, relative: str, media_type: str, records: int) -> dict[str, Any]:
        path = self._require_partial() / relative
        return {
            "role": role,
            "path": relative,
            "media_type": media_type,
            "bytes": path.stat().st_size,
            "sha256": _digest(path),
            "records": records,
        }

    def _manifest(self, ended_at: str) -> dict[str, Any]:
        ranges = (
            None
            if self._config.imu_ranges is None
            else json.loads(json.dumps(self._config.imu_ranges))
        )
        return {
            "format": "ylx.recording-session.v0",
            "state": "sealed",
            "session_id": self._require_session_id(),
            "time": {"started_at": self._started_at, "ended_at": ended_at},
            "device": {
                "id": self._config.device_id,
                "software_version": self._config.software_version,
            },
            "capture": {
                "video": {
                    "width": self._config.width,
                    "height": self._config.height,
                    "fps": self._config.fps,
                    "encoding": self._config.encoding,
                    "coordinate_frame": "opencv_optical",
                },
                "imu": {
                    "coordinate_frame": "opencv_optical",
                    "device_tick_hz": self._config.device_tick_hz,
                    "ranges": ranges,
                },
                "clock": {"domain": "host_monotonic", "unit": "nanosecond"},
            },
            "counts": {
                "frames": self._frames_written,
                "imu_samples": self._imu_written,
                "diagnostics": self._diagnostics_written,
                "dropped_frames": self._dropped_frames,
                "dropped_imu_samples": self._dropped_imu,
            },
            "artifacts": [
                self._artifact("session.metadata", "session.json", "application/json", 1),
                self._artifact(
                    "video.left",
                    "video/left.bin",
                    "application/vnd.ylx.frame-stream",
                    self._frames_written,
                ),
                self._artifact(
                    "video.right",
                    "video/right.bin",
                    "application/vnd.ylx.frame-stream",
                    self._frames_written,
                ),
                self._artifact(
                    "frames.timeline",
                    "frames.ndjson",
                    "application/x-ndjson",
                    self._frames_written,
                ),
                self._artifact(
                    "imu.samples",
                    "imu.ndjson",
                    "application/x-ndjson",
                    self._imu_written,
                ),
                self._artifact(
                    "diagnostics.events",
                    "diagnostics.ndjson",
                    "application/x-ndjson",
                    self._diagnostics_written,
                ),
            ],
        }

    def stop(self, *, before_publish: Callable[[Path], None] | None = None) -> Path:
        with self._state_lock:
            if self._state == "sealed":
                assert self._final_path is not None
                return self._final_path
            if self._state != "recording":
                raise RecordingError("invalid_state", "只有 recording 会话可以停止")
            self._state = "stopping"
        self._stop_writer()
        if self._writer_error is not None:
            self._close_files(ignore_errors=True)
            self._mark_failed("write_failed", str(self._writer_error))
            raise RecordingError("write_failed", f"数据写入失败：{self._writer_error}")
        try:
            if self._dropped_frames:
                self._write_diagnostic(
                    "warning", "frame_dropped", "相机或有界队列发生丢帧", self._dropped_frames
                )
            if self._source_gaps:
                self._write_diagnostic(
                    "warning", "source_frame_gap", "相机来源帧序列存在缺口", self._source_gaps
                )
            if self._queue_rejected_frames:
                self._write_diagnostic(
                    "warning",
                    "frame_queue_rejected",
                    "录制有界队列拒绝帧",
                    self._queue_rejected_frames,
                )
            if self._dropped_imu:
                self._write_diagnostic(
                    "warning", "imu_dropped", "IMU transport 或有界队列发生丢样", self._dropped_imu
                )
            self._write_diagnostic("info", "recording_complete", "录制正常结束", 1)
            self._sync_and_close_files()
            ended_at = _utc_now()
            _write_json_atomic(
                self._require_partial() / "session.json",
                self._session_state("sealed", failure=None),
            )
            manifest = self._manifest(ended_at)
            _write_json_atomic(self._require_partial() / "manifest.json", manifest)
            validate_session(self._require_partial(), allow_partial=True)
        except Exception as exc:
            self._close_files(ignore_errors=True)
            with suppress(OSError):
                (self._require_partial() / "manifest.json").unlink(missing_ok=True)
            self._mark_failed("seal_failed", str(exc))
            raise RecordingError("seal_failed", f"会话封存失败：{exc}") from exc
        if before_publish is not None:
            try:
                before_publish(self._require_partial())
            except Exception as exc:
                with suppress(OSError):
                    (self._require_partial() / "manifest.json").unlink(missing_ok=True)
                code = getattr(exc, "code", "pre_publish_failed")
                message = getattr(exc, "message", str(exc))
                self._mark_failed(str(code), str(message), state="interrupted")
                raise
        try:
            _fsync_directory(self._require_partial())
            assert self._final_path is not None
            os.rename(self._require_partial(), self._final_path)
            _fsync_directory(self._root)
        except Exception as exc:
            with suppress(OSError):
                (self._require_partial() / "manifest.json").unlink(missing_ok=True)
            self._mark_failed("seal_failed", str(exc))
            raise RecordingError("seal_failed", f"会话封存失败：{exc}") from exc
        with self._state_lock:
            self._state = "sealed"
        return self._final_path

    def abort(self, *, code: str = "process_interrupted", message: str = "录制被中断") -> Path:
        with self._state_lock:
            if self._state == "sealed":
                assert self._final_path is not None
                return self._final_path
            if self._state == "failed":
                return self._require_partial()
            if self._state not in {"recording", "stopping"}:
                raise RecordingError("invalid_state", "当前会话不能中断")
            self._state = "stopping"
        self._stop_writer()
        try:
            if self._files:
                self._write_diagnostic("error", code, message, 1)
        except OSError:
            pass
        self._close_files(ignore_errors=True)
        self._mark_failed(code, message, state="interrupted")
        return self._require_partial()

    def __enter__(self) -> SessionRecorder:
        return self

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        if self.state in {"recording", "stopping"}:
            self.abort(
                code="process_interrupted",
                message="上下文退出前未完成正常封存" if exc is None else str(exc),
            )

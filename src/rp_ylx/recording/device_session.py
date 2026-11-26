"""Device Session v2 的有界写入、验证与原子封存。"""

from __future__ import annotations

import errno
import hashlib
import json
import math
import os
import queue
import sqlite3
import stat
import threading
import time
import uuid
from collections.abc import Callable, Mapping
from concurrent.futures import ThreadPoolExecutor
from contextlib import nullcontext, suppress
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import BinaryIO
from zoneinfo import ZoneInfo

from rp_ylx.api.downloads import (
    ArtifactAccessError,
    validated_device_session_payload,
)
from rp_ylx.camera import FrameObservation
from rp_ylx.imu import ImuObservation
from rp_ylx.native import (
    NativeModuleError,
    NativeSessionTransaction,
    native_session_store,
)
from rp_ylx.native import (
    native_session_store_or_none as _session_store_or_none,
)
from rp_ylx.operational_logging import operational_logger
from rp_ylx.performance.metrics import PayloadLease, PerformanceMetrics
from rp_ylx.persistence import PersistenceError
from rp_ylx.recording.audio_finalizer import (
    AudioRecorderAdapter,
    abandon_audio_recorder,
    apply_audio_result,
    build_audio_block,
    stop_and_close_audio,
    strict_int,
)
from rp_ylx.recording.encoding import RecordingEncoding
from rp_ylx.recording.fs_safety import (
    fsync_directory,
    write_json_atomic,
)
from rp_ylx.recording.live_seal import LiveSealing
from rp_ylx.recording.manifest_builder import (
    DeviceRecordingError,
    DeviceSessionConfig,
    FinalizedArtifact,
    SealedDeviceSession,
    SessionPlan,
    StorageStatus,
    build_device_session_manifest,
    json_bytes,
    manifest_artifact_bytes_total,
    uuid7,
)
from rp_ylx.recording.manifest_builder import (
    finalize_artifact as _mb_finalize_artifact,
)
from rp_ylx.recording.manifest_builder import (
    inspect_device_session_directory as _mb_inspect_device_session_directory,
)
from rp_ylx.recording.manifest_builder import (
    manifest_artifacts as _manifest_artifacts,
)
from rp_ylx.recording.manifest_builder import (
    read_bounded_fd as _mb_read_bounded_fd,
)
from rp_ylx.recording.manifest_builder import (
    recording_error as _recording_error,
)
from rp_ylx.recording.native_bridge import (
    active_take_uint,
    build_native_recording_plan,
    native_segment_float,
    native_segment_path,
    native_segment_uint,
    native_uint,
    parse_active_take_snapshot,
    parse_session_transaction_sink,
    seal_native_transaction,
)
from rp_ylx.recording.session_verifier import (
    enforce_quality_policy,
    open_regular_at,
    validate_artifact_bytes,
    verified_finalization,
    verify_artifact_fd,
)
from rp_ylx.recording.session_verifier import (
    validate_device_session_directory as _sv_validate_device_session_directory,
)
from rp_ylx.recording.stereo_encoder import (
    ENCODER_FINISH_TIMEOUT_SECONDS,
    ClosedSegment,
    StereoEncoderError,
    StereoEncoderProcess,
    resolve_executable,
)
from rp_ylx.recording.verified import VerifiedArtifact
from rp_ylx.recording.verified import identity as verified_identity

__all__ = [
    "DeviceRecordingError",
    "DeviceSessionConfig",
    "DeviceSessionRecorder",
    "RecordingEncoding",
    "SealedDeviceSession",
    "SessionPlan",
    "StorageStatus",
    "fsync_directory",
    "inspect_device_session_directory",
    "json_bytes",
    "manifest_artifact_bytes_total",
    "uuid7",
    "validate_device_session_directory",
    "write_json_atomic",
]

_STOP = object()
_ORIGINAL_PATH_OPEN = Path.open
_ORIGINAL_OS_FSYNC = os.fsync


@dataclass(frozen=True, slots=True)
class _FrameEvent:
    observation: FrameObservation
    record_sequence: int
    accepted_elapsed: float
    payload_lease: PayloadLease | None = None


@dataclass(frozen=True, slots=True)
class _ImuEvent:
    observation: ImuObservation


def _finalize_artifact(
    path: Path,
    expected_bytes: int | None,
    *,
    code: str,
) -> FinalizedArtifact:
    return _mb_finalize_artifact(
        path,
        expected_bytes,
        code=code,
        session_store=_session_store_or_none(),
    )


def _verified_finalization(
    path: Path, expected_bytes: int | None, *, code: str
) -> tuple[FinalizedArtifact, VerifiedArtifact]:
    """Read fresh bytes and return a proof without mutating recorder state."""
    return verified_finalization(
        path,
        expected_bytes,
        code=code,
        finalize_fn=_finalize_artifact,
        identity_fn=verified_identity,
    )


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


def _open_regular_at(root_fd: int, relative: str, *, code: str) -> int:
    return open_regular_at(
        root_fd,
        relative,
        code=code,
        session_store=_session_store_or_none(),
    )


def _read_bounded_fd(descriptor: int, maximum_bytes: int, *, code: str) -> bytes:
    return _mb_read_bounded_fd(
        descriptor,
        maximum_bytes,
        code=code,
        session_store=_session_store_or_none(),
    )


def _verify_artifact_fd(
    descriptor: int,
    expected_bytes: int,
    expected_sha256: str,
    interrupt_check: Callable[[], None] | None = None,
) -> None:
    verify_artifact_fd(
        descriptor,
        expected_bytes,
        expected_sha256,
        interrupt_check,
        session_store=_session_store_or_none(),
    )


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
        audio_recorder_factory: Callable[[Path], AudioRecorderAdapter] | None = None,
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
        # Serialize durable state and its callback without holding the status
        # lock during filesystem I/O. A late checkpoint must not replace a
        # newer finalizing state, either on disk or in the coordinator cache.
        self._state_write_lock = threading.RLock()
        self._writer_stopping = threading.Event()
        self._counter_lock = threading.Lock()
        self._io_lock = threading.Lock()
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
        self._audio_recorder: AudioRecorderAdapter | None = None
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
        self._verified_artifacts: dict[str, VerifiedArtifact] = {}
        self._live_sealing: LiveSealing | None = None
        self._stopped_elapsed: float | None = None
        self._verification_compaction_total = 0
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
        self.recovered_session: SealedDeviceSession | None = None
        self.recovery_attempted = False

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
                + int(self._live_sealing is not None)
            )

    def _now(self) -> datetime:
        return datetime.now(ZoneInfo(self._config.timezone))

    @staticmethod
    def _timestamp(value: datetime) -> str:
        return value.isoformat(timespec="microseconds")

    def _elapsed(self) -> float:
        if self._stopped_elapsed is not None:
            return self._stopped_elapsed
        if self._started_monotonic_ns is None:
            return 0.0
        return max(0.0, (time.monotonic_ns() - self._started_monotonic_ns) / 1e9)

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
        with self._state_write_lock:
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

    def _verification_progress(self, completed: int, total: int) -> None:
        # This is live progress, not a recovery checkpoint. Avoid fsync for each
        # compressed artifact; the original journals remain durable until seal.
        revision = self._allocate_revision()
        with self._lock:
            if self._current_state is None:
                return
            document = dict(self._current_state)
            document["state_revision"] = revision
            document["updated_at"] = self._timestamp(self._now())
            document["progress"] = {
                **document["progress"],
                "verification": {"completed": completed, "total": total},
            }
            self._current_state = document
        # Progress is observable through current_recording_state. The state
        # sink fsyncs coordinator recovery data; doing that per artifact adds
        # a disk barrier for every five-second audio segment. State transitions
        # and the final command result remain durable through _persist_state.

    def independent_verification_progress(self, completed: int, total: int) -> None:
        """Include full-content admission in live progress, retaining one final unit."""
        offset = self._verification_compaction_total
        self._verification_progress(offset + completed, offset + total + 1)

    def _live_audio_bytes(self) -> int:
        recorder = self._audio_recorder
        live_bytes: int | None = None
        snapshot = getattr(recorder, "snapshot", None) if recorder is not None else None
        if callable(snapshot):
            try:
                raw_snapshot = snapshot()
            except Exception as error:
                operational_logger("recording").event(
                    "live_audio_snapshot_failed",
                    level="warning",
                    transaction_id=self._plan.session_id,
                    exception_type=type(error).__name__,
                )
                raw_snapshot = None
            if isinstance(raw_snapshot, Mapping):
                with suppress(DeviceRecordingError):
                    strict_int(raw_snapshot.get("sample_count"), "audio.snapshot.sample_count")
                    live_bytes = strict_int(
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
                    snapshot = getattr(transaction, "diagnostic_snapshot", transaction.snapshot)
                    transaction_snapshot = snapshot()
                except Exception as error:
                    operational_logger("recording").event(
                        "native_progress_snapshot_failed",
                        level="warning",
                        transaction_id=self._plan.session_id,
                        exception_type=type(error).__name__,
                    )
                    transaction_snapshot = None
                sink_snapshot = (
                    transaction_snapshot.get("sink")
                    if isinstance(transaction_snapshot, Mapping)
                    else None
                )
                if isinstance(sink_snapshot, Mapping):
                    with suppress(DeviceRecordingError):
                        frames_written = native_uint(
                            sink_snapshot.get("frames_written"), "recording_sink.frames_written"
                        )
                    with suppress(DeviceRecordingError):
                        sink_bytes = native_uint(
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
            self._state = "starting"
        try:
            if not self._root.is_dir():
                raise DeviceRecordingError("storage_unavailable", "会话 generation 根不存在")
            if self._partial.exists() or self._final.exists():
                raise DeviceRecordingError("session_exists", "会话标识已经存在")
            self._storage_status()
        except BaseException:
            with self._lock:
                self._state = "failed"
            raise
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
                    "started_monotonic_ns": self._started_monotonic_ns,
                    "config": asdict(self._config),
                    "plan": {
                        key: value
                        for key, value in asdict(self._plan).items()
                        if key != "clock_provenance"
                    },
                },
            )
            if self._plan.clock_provenance is not None:
                write_json_atomic(
                    self._partial / "clock-calibration.json",
                    {
                        "schema": "openaria.recording-clock.v1",
                        "session_id": self._plan.session_id,
                        "started_at": self._timestamp(self._started_at),
                        "utc_offset_seconds": int(self._started_at.utcoffset().total_seconds()),
                        "calibration": self._plan.clock_provenance,
                    },
                )
            if self._native_transaction_enabled:
                try:
                    self._native_transaction = self._begin_native_transaction()
                except NativeModuleError as error:
                    raise DeviceRecordingError(error.code, error.message) from error
            if self._native_transaction is None:
                self._files = {}
                self._files["frames.index"] = (self._partial / "frames.ndjson").open("xb")
                self._files["imu.samples"] = (self._partial / "imu.ndjson").open("xb")
                encoder = self._encoder_factory(self._partial)
                self._encoder = encoder
                encoder.start()
                if self._config.audio_enabled:
                    if self._audio_recorder_factory is None:
                        raise DeviceRecordingError(
                            "test_adapter_missing",
                            "源码测试音频路径必须显式注入 audio_recorder_factory",
                        )
                    audio = self._audio_recorder_factory(self._partial)
                    self._audio_recorder = audio
                    audio.start()
            fsync_directory(self._partial)
            fsync_directory(self._root)
            if self._native_transaction is not None and self._config.lossless_storage:
                self._live_sealing = LiveSealing(
                    self._partial,
                    self._config.sensor_fps / self._config.frame_decimation,
                    self._config.frame_decimation,
                )
            if self._native_transaction is None:
                writer = threading.Thread(
                    target=self._writer_loop,
                    name=f"rp-ylx-session-writer-{self._plan.session_id[:8]}",
                    daemon=True,
                )
                writer.start()
                self._writer = writer
            if self._encoder is not None or self._native_transaction is not None:
                # Segment hashes accumulate while recording so that stop() only
                # has to seal the trailing segment.
                harvester = threading.Thread(
                    target=self._harvest_loop,
                    name=f"rp-ylx-v1-segments-{self._plan.session_id[:8]}",
                    daemon=True,
                )
                harvester.start()
                self._harvester = harvester
            with self._lock:
                self._state = "recording"
        except BaseException as error:
            self._stop_writer()
            self._abandon_audio()
            self._abandon_encoder()
            self._close_files(ignore_errors=True)
            with self._lock:
                self._state = "failed"
            self._mark_failed("start_failed", str(error), recoverable=False)
            if not isinstance(error, Exception):
                raise
            raise DeviceRecordingError(
                "start_failed", f"创建 device-session 会话失败：{error}"
            ) from error
        return self._partial

    def _write(self, role: str, payload: bytes) -> None:
        started = self._metrics.start() if self._metrics is not None else 0
        try:
            self._before_write(role, payload)
            with self._io_lock:
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
            self._mark_segment_boundary(ordinal, event.record_sequence, event.accepted_elapsed)
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

    def _mark_segment_boundary(self, ordinal: int, record_sequence: int, elapsed: float) -> None:
        """记录编码序号到帧域序号与经过时长的对应关系，用于封存分段边界。"""

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
            encoder_arguments=()
            if self._config.recording_encoding is None
            else self._config.recording_encoding.arguments(),
        )

    def _begin_native_transaction(self) -> NativeSessionTransaction:
        assert self._started_monotonic_ns is not None
        plan = build_native_recording_plan(
            partial=self._partial,
            config=self._config,
            plan=self._plan,
            encoder_executable=resolve_executable(),
            segment_frames=self._segment_frames,
            started_monotonic_ns=self._started_monotonic_ns,
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
        closed_offset = 0
        if transaction is not None:
            closed_offset = self._harvested_segments
            incremental = getattr(transaction, "segments_since", None)
            if incremental is not None:
                raw_segments = incremental(closed_offset)
            else:
                # Older native builds expose only the complete inventory.
                raw_segments = transaction.segments()
                if len(raw_segments) < closed_offset:
                    raise DeviceRecordingError("segment_invalid", "encoder segment list regressed")
                raw_segments = raw_segments[closed_offset:]
            # Native closed segments are append-only and previously harvested
            # records already have sealed identities. Revalidating their fields
            # every 200 ms made Python work grow with the recording's duration.
            # Validate every unharvested entry, including an unindexed tail.
            closed = tuple(
                ClosedSegment(
                    index=native_segment_uint(item.get("index"), "segment.index"),
                    start_frame=native_segment_uint(item.get("start_frame"), "segment.start_frame"),
                    end_frame=native_segment_uint(item.get("end_frame"), "segment.end_frame"),
                    left_path=native_segment_path(item.get("left_path"), "segment.left_path"),
                    left_bytes=native_segment_uint(item.get("left_bytes"), "segment.left_bytes"),
                    right_path=native_segment_path(item.get("right_path"), "segment.right_path"),
                    right_bytes=native_segment_uint(item.get("right_bytes"), "segment.right_bytes"),
                )
                for item in raw_segments
            )
        else:
            encoder = self._encoder
            if encoder is None:
                return
            closed = encoder.segments
        with getattr(self, "_segment_lock", nullcontext()):
            harvested_before = self._harvested_segments
            while self._harvested_segments < closed_offset + len(closed):
                segment = closed[self._harvested_segments - closed_offset]
                if transaction is not None:
                    available = transaction.snapshot()["sink"]["frames_indexed"]
                else:
                    available = self._frames_written
                if available < segment.end_frame:
                    # The helper can announce completion before the last index
                    # write returns. Commit only after that frame has an index.
                    break
                self._segment_records.append(self._segment_record(segment))
                self._harvested_segments += 1
            if self._harvested_segments == harvested_before:
                return
            segment_snapshot = list(self._segment_records)
        # Publish the eligible contiguous batch in one durable checkpoint.
        # Replaying several fsync barriers per already-closed pair magnifies
        # IO stalls while catching up. Never include an frame still queued for indexing.
        for relative in ("frames.ndjson", "imu.ndjson"):
            role_key = "frames.index" if relative == "frames.ndjson" else "imu.samples"
            with getattr(self, "_io_lock", nullcontext()):
                stream = self._files.get(role_key)
                if stream is not None and not stream.closed:
                    stream.flush()
            descriptor = os.open(
                self._partial / relative,
                os.O_RDONLY | os.O_NOFOLLOW | getattr(os, "O_CLOEXEC", 0),
            )
            try:
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
        write_json_atomic(
            self._partial / "segments.json",
            {
                "schema": "openaria.segment-checkpoint.v1",
                "session_id": self._plan.session_id,
                "segments": segment_snapshot,
                "diagnostics": self._capture_diagnostics(),
            },
            pretty=False,
        )

    def _segment_record(self, segment: ClosedSegment) -> dict[str, object]:
        artifacts: dict[str, object] = {}
        for eye, relative, declared in (
            ("left", segment.left_path, segment.left_bytes),
            ("right", segment.right_path, segment.right_bytes),
        ):
            path = self._partial / relative
            finalized = self._finalize_for_seal(path, declared, code="segment_invalid")
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
        frame_domain, drop_events = parse_active_take_snapshot(
            snapshot,
            expected_session_id=self._plan.session_id,
            expected_frames_written=expected_frames_written,
            queue_capacity=self._queue_capacity,
        )
        with self._counter_lock:
            self._frame_domain = frame_domain
            self._drop_events = drop_events

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
                _FrameEvent(observation, record_sequence, self._elapsed(), lease),
                self._enqueue_timeout,
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
        with self._state_write_lock:
            if self._writer_stopping.is_set() or self._state != "recording":
                return
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
            # Slow storage must not cause an immediate catch-up checkpoint.
            self._last_checkpoint = time.monotonic()

    def _stop_writer(self) -> None:
        writer = self._writer
        if writer is None:
            return
        # Drain accepted media; lifecycle transitions and segment checkpoints
        # retain durability without writing recording progress for every item.
        self._writer_stopping.set()
        while writer.is_alive():
            if self._queue_put(_STOP, 0.1):
                break
        writer.join(timeout=10)
        if writer.is_alive():
            if self._writer_error is None:
                self._writer_error = TimeoutError("v1 writer 未在期限内停止")
            # A join timeout cannot cancel filesystem I/O. Keep ownership until
            # the writer exits: callers close its streams and may recover the
            # same files immediately after this method returns. The error also
            # makes the writer discard queued work after its current write.
            writer.join()
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
        imu_written, bytes_written, artifact_bytes, native_artifacts, artifact_identities = (
            parse_session_transaction_sink(
                result,
                expected_frames_written=self._frames_written,
                valid_roles=self._digests,
            )
        )
        self._imu_written = imu_written
        self._bytes_written = bytes_written
        self._artifact_bytes.update(artifact_bytes)
        self._native_artifacts.update(native_artifacts)
        self._artifact_identities.update(artifact_identities)

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
        except Exception as error:
            operational_logger("recording").event(
                "storage_status_probe_failed",
                level="warning",
                transaction_id=self._plan.session_id,
                exception_type=type(error).__name__,
            )
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
        with self._lock:
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
        except (OSError, sqlite3.Error, PersistenceError) as error:
            operational_logger("recording").event(
                "mark_failed_persist_error",
                level="warning",
                transaction_id=self._plan.session_id,
                exception_type=type(error).__name__,
            )
            return None

    def _snapshot_native_progress(self, transaction: NativeSessionTransaction | None) -> None:
        if transaction is None:
            return
        try:
            snapshot = getattr(
                transaction,
                "diagnostic_snapshot",
                transaction.snapshot,
            )
            sink = snapshot().get("sink")
            if isinstance(sink, Mapping):
                self._frames_written = max(
                    self._frames_written,
                    native_uint(sink.get("frames_written"), "recording_sink.frames_written"),
                )
                self._bytes_written = max(
                    self._bytes_written,
                    native_uint(sink.get("bytes_written"), "recording_sink.bytes_written"),
                )
        except Exception as error:
            operational_logger("recording").event(
                "native_progress_snapshot_failed",
                level="warning",
                transaction_id=self._plan.session_id,
                exception_type=type(error).__name__,
            )

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
            transaction = self._native_transaction
            harvester = self._harvester
        self._stop_writer()
        if transaction is not None:
            self._harvest_stop.set()
            if harvester is not None:
                harvester.join(timeout=5)
                if not harvester.is_alive():
                    with self._lock:
                        if self._harvester is harvester:
                            self._harvester = None
                    harvester = None
            if not media_lost and harvester is None:
                # The source has detached this transaction. Drain accepted frames
                # and audio so a source gap need only lose the rejected frame.
                with suppress(Exception):
                    assert self._started_monotonic_ns is not None
                    duration = max(0.0, (time.monotonic_ns() - self._started_monotonic_ns) / 1e9)
                    transaction.finish(duration, 10.0)
            if harvester is None:
                with suppress(Exception):
                    self._harvest_segments()
            self._snapshot_native_progress(transaction)
        abort_error: Exception | None = None
        if transaction is not None:
            # abort joins native audio before recovery reads its final checkpoint.
            try:
                transaction.abort(message)
            except Exception as error:
                abort_error = error
        self._abandon_audio()
        diagnostics_payload = {"code": code, "message": message, **self._capture_diagnostics()}
        if abort_error is not None:
            diagnostics_payload["native_abort_error"] = str(abort_error)
        with suppress(Exception):
            write_json_atomic(self._partial / "failure-diagnostics.json", diagnostics_payload)
        self._abandon_encoder(abort_native=False)
        self._close_files(ignore_errors=True)
        self._mark_failed(
            code,
            message,
            recoverable=recoverable,
            media_lost=media_lost,
        )
        if not media_lost:
            self.recover_completed_segments()
        return self._partial

    def _capture_diagnostics(self) -> dict[str, object]:
        result: dict[str, object] = {"observed_monotonic_ns": time.monotonic_ns()}
        if self._metrics is not None:
            # The daemon shares these counters with preview and every recorder;
            # they are not losses or histogram percentiles for this session.
            result["capture_scope"] = "metrics_lifetime"
            try:
                result["capture"] = asdict(self._metrics.snapshot())
            except Exception as error:
                # Keep the original failure and independent encoder/index
                # evidence even when the optional metrics snapshot fails.
                result["capture_snapshot_error"] = str(error)
        if self._native_transaction is not None:
            try:
                diagnostic_snapshot = getattr(
                    self._native_transaction,
                    "diagnostic_snapshot",
                    self._native_transaction.snapshot,
                )
                snapshot = diagnostic_snapshot()
                result["encoder"] = snapshot.get("encoder_stats", {})
                result["encoder_failure"] = snapshot.get("encoder_failure")
                result["audio_failure"] = snapshot.get("audio_failure")
                sink = snapshot.get("sink", {})
                result["index_writer"] = {
                    name: sink.get(name)
                    for name in (
                        "frames_written",
                        "frames_indexed",
                        "imu_samples_written",
                        "imu_samples_indexed",
                        "index_pending_bytes",
                        "index_peak_bytes",
                        "index_max_write_ns",
                    )
                }
            except Exception as error:
                result["snapshot_error"] = str(error)
        elif self._encoder is not None:
            result["encoder"] = getattr(self._encoder, "stats", {})
        return result

    def recover_completed_segments(self) -> SealedDeviceSession | None:
        from rp_ylx.recording.recovery import recover_device_session

        # A failed attempt may have scanned a multi-hour take. The coordinator
        # must not repeat it immediately; a later explicit retry or boot may.
        self.recovery_attempted = True
        try:
            self.recovered_session = recover_device_session(self._partial)
            return self.recovered_session
        except (
            OSError,
            ValueError,
            KeyError,
            TypeError,
            DeviceRecordingError,
            ArtifactAccessError,
        ):
            # Keep the original artifacts and failure state for the next boot
            # (including ENOSPC, missing media and a torn checkpoint).
            return None

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

    def _clock_artifact(self) -> dict[str, object]:
        relative = "clock-calibration.json"
        result = self._finalize_for_seal(
            self._partial / relative, None, code="clock_metadata_invalid"
        )
        self._artifact_identities[relative] = result.identity
        return {
            "artifact_id": result.sha256,
            "sha256": result.sha256,
            "role": "log.clock-calibration",
            "path": relative,
            "media_type": "application/json",
            "bytes": result.bytes,
        }

    def _video_segments(self, duration: float) -> list[dict[str, object]]:
        with self._segment_lock:
            records = list(self._segment_records)
        if not records:
            raise DeviceRecordingError("no_frames", "没有可封存的成片分段")
        segments: list[dict[str, object]] = []
        for record in records:
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
        return segments

    def _finish_audio(self) -> None:
        recorder = self._audio_recorder
        if recorder is None:
            return
        self._audio_recorder = None
        result = stop_and_close_audio(recorder)
        self._apply_audio_result(result)

    def _apply_audio_result(self, result: object) -> None:
        audio_result, records, audio_bytes, identities, proofs = apply_audio_result(
            result,
            partial=self._partial,
            verified_finalization_fn=_verified_finalization,
        )
        # Publish only a complete inventory; failures leave existing state intact.
        self._audio_result = audio_result
        self._audio_segment_records = records
        self._audio_bytes = audio_bytes
        self._artifact_identities.update(identities)
        self._verified_artifacts.update(proofs)

    def _audio_block(self) -> dict[str, object]:
        assert self._started_monotonic_ns is not None
        return build_audio_block(
            self._audio_result,
            self._audio_segment_records,
            self._started_monotonic_ns,
        )

    def _boundary(self, ordinal: int, duration: float) -> tuple[int, float]:
        """把编码序号翻译成 manifest 使用的帧域序号与经过时长。"""

        transaction = self._native_transaction
        if transaction is not None:
            try:
                boundary = transaction.boundary(ordinal, duration)
            except BaseException as error:
                raise _recording_error(error, "native_session_store_failed") from error
            return (
                native_segment_uint(boundary.get("frame"), "session_transaction.frame"),
                native_segment_float(
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
        *,
        audio: dict[str, object] | None = None,
    ) -> dict[str, object]:
        assert self._started_at is not None
        video_segments = self._video_segments(duration)
        imu = self._artifact("imu.samples", "imu.ndjson", "application/x-ndjson")
        frames = self._artifact("frames.index", "frames.ndjson", "application/x-ndjson")
        logs = [self._clock_artifact()] if self._plan.clock_provenance is not None else []
        resolved_audio = (
            (audio if audio is not None else self._audio_block())
            if self._config.audio_enabled
            else None
        )
        return build_device_session_manifest(
            config=self._config,
            plan=self._plan,
            started_at=self._started_at,
            ended_at=ended_at,
            verified_at=verified_at,
            sealed_at=sealed_at,
            duration=duration,
            frames_written=self._frames_written,
            imu_written=self._imu_written,
            video_segments=video_segments,
            frames_artifact=frames,
            imu_artifact=imu,
            audio=resolved_audio,
            logs=logs,
            drop_events=self._drop_events,
            throughput_bytes=self._bytes_written + self._segment_bytes + self._audio_bytes,
        )

    def _validate_artifact_bytes(
        self,
        manifest: Mapping[str, object],
        *,
        root: Path | None = None,
    ) -> None:
        validate_artifact_bytes(
            root or self._partial,
            manifest,
            self._artifact_identities,
            finalize_fn=_finalize_artifact,
        )

    def _enforce_quality_policy(self, duration: float) -> None:
        enforce_quality_policy(
            self._config,
            self._frames_written,
            self._drop_events,
            duration,
        )

    def _finalize_for_seal(
        self, path: Path, expected_bytes: int | None, *, code: str
    ) -> FinalizedArtifact:
        relative = path.relative_to(self._partial).as_posix()
        result, proof = _verified_finalization(path, expected_bytes, code=code)
        self._verified_artifacts[relative] = proof
        return result

    def _drain_capture(
        self,
        transaction: NativeSessionTransaction | None,
        stop_observed_ns: int,
        finished_stage: Callable[[str], None],
    ) -> tuple[dict[str, object], object | None, object | None]:
        assert self._started_monotonic_ns is not None
        assert self._stopped_elapsed is not None
        capture_ended_at = self._now()
        live_audit = None
        prepared = None
        if transaction is not None:
            transaction.request_audio_stop()
            boundary = transaction.capture_stopped_monotonic_ns
            if boundary is not None:
                if (
                    isinstance(boundary, bool)
                    or not isinstance(boundary, int)
                    or not self._started_monotonic_ns <= boundary <= stop_observed_ns
                ):
                    raise DeviceRecordingError(
                        "native_session_store_failed", "原生采集停止边界超出已观测采集窗口"
                    )
                with self._lock:
                    self._stopped_elapsed = (boundary - self._started_monotonic_ns) / 1e9
                capture_ended_at -= timedelta(seconds=(stop_observed_ns - boundary) / 1e9)
        self._persist_state("finalizing")
        finished_stage("finalizing_checkpoint")
        if transaction is None:
            self._stop_writer()
            if self._writer_error is not None:
                raise self._writer_error
            if self._frames_written == 0:
                raise DeviceRecordingError("no_frames", "没有可封存的相机帧")
            self._finish_audio()
            self._sync_and_close_files()
        ended_at = capture_ended_at
        duration = self._stopped_elapsed
        if transaction is None:
            self._finish_encoder(duration)
        else:
            self._harvest_stop.set()
            if self._harvester is not None:
                self._harvester.join(timeout=10)
                if self._harvester.is_alive():
                    raise DeviceRecordingError("segment_invalid", "分段校验线程未在期限内停止")
                with self._lock:
                    self._harvester = None
            if self._writer_error is not None:
                raise self._writer_error
            try:
                outcome = transaction.finish(duration, ENCODER_FINISH_TIMEOUT_SECONDS)
            except BaseException as error:
                raise _recording_error(error, "native_session_store_failed") from error
            finished_stage("native_finish")
            active_take = outcome.get("active_take")
            sink = outcome.get("sink")
            if not isinstance(active_take, Mapping) or not isinstance(sink, Mapping):
                raise DeviceRecordingError(
                    "native_session_store_failed", "SessionTransaction 结果字段无效"
                )
            self._apply_active_take_snapshot(active_take)
            self._frames_written = active_take_uint(
                active_take.get("frames_written"), "active_take.frames_written"
            )
            self._apply_session_transaction_sink(sink)
            if self._live_sealing is not None:
                try:
                    live_audit = self._live_sealing.finish()
                    prepared = self._live_sealing.prepared
                    for relative, artifact in prepared.items():
                        self._verified_artifacts[relative] = artifact.source
                        target = artifact.target.relative_to(self._partial).as_posix()
                        self._verified_artifacts[target] = artifact.output
                except Exception as error:
                    operational_logger("recording").event(
                        "live_sealing_fallback",
                        level="warning",
                        transaction_id=self._plan.session_id,
                        error=str(error),
                    )
                    with suppress(BaseException):
                        self._live_sealing.close()
                    live_audit = None
                    prepared = None
                finally:
                    with self._lock:
                        self._live_sealing = None
            finished_stage("incremental_drain")
            if self._config.audio_enabled:
                self._apply_audio_result(outcome.get("audio"))
            finished_stage("audio_inventory")
            self._harvest_segments()
            if self._writer_error is not None:
                raise self._writer_error
            finished_stage("video_tail")
            if self._frames_written == 0:
                raise DeviceRecordingError("no_frames", "没有可封存的相机帧")
        audio = self._audio_block() if self._config.audio_enabled else None
        if audio is not None:
            audio_end = float(audio["sync"]["end_time_seconds"])
            assert self._audio_result is not None
            thread_end = (
                strict_int(
                    self._audio_result.get("stopped_monotonic_ns"), "audio.stopped_monotonic_ns"
                )
                - self._started_monotonic_ns
            ) / 1e9
            clock = audio.get("capture_clock")
            if isinstance(clock, Mapping):
                thread_end = max(
                    thread_end,
                    (
                        strict_int(
                            clock.get("thread_stopped_monotonic_ns"),
                            "audio.thread_stopped_monotonic_ns",
                        )
                        - self._started_monotonic_ns
                    )
                    / 1e9,
                )
            observed_end = (time.monotonic_ns() - self._started_monotonic_ns) / 1e9
            if not math.isfinite(audio_end) or max(audio_end, thread_end) > observed_end:
                raise DeviceRecordingError("audio_invalid", "音频结束时间超出已观测采集窗口")
            duration = max(duration, audio_end, thread_end)
            ended_at += timedelta(seconds=duration - self._stopped_elapsed)
        self._enforce_quality_policy(duration)
        verified_at = self._now()
        sealed_at = self._now()
        manifest = self._manifest(ended_at, verified_at, sealed_at, duration, audio=audio)
        return manifest, live_audit, prepared

    def _compact_and_audit(
        self,
        manifest: dict[str, object],
        live_audit: object | None,
        prepared: object | None,
        finished_stage: Callable[[str], None],
    ) -> tuple[dict[str, object], list[str]]:
        compacted_originals: list[str] = []
        if self._config.lossless_storage:
            from rp_ylx.recording.audit import capture_audit
            from rp_ylx.recording.storage import compact_manifest

            def finalize_compacted(path: Path) -> dict[str, object]:
                result = self._finalize_for_seal(path, None, code="compaction_invalid")
                relative = path.relative_to(self._partial).as_posix()
                self._artifact_identities[relative] = result.identity
                return {
                    "path": relative,
                    "bytes": result.bytes,
                    "sha256": result.sha256,
                    "artifact_id": result.sha256,
                }

            with ThreadPoolExecutor(max_workers=1, thread_name_prefix="clock-audit") as audit:
                audited = (
                    audit.submit(
                        capture_audit,
                        self._partial,
                        self._config.sensor_fps / self._config.frame_decimation,
                        self._config.frame_decimation,
                    )
                    if live_audit is None
                    else None
                )
                manifest, compacted_originals = compact_manifest(
                    self._partial,
                    manifest,
                    finalize_compacted,
                    prepared=prepared,
                )
                manifest["capture_audit"] = live_audit if audited is None else audited.result()
            if self._config.recording_encoding is None:
                raise DeviceRecordingError(
                    "manifest_invalid", "lossless_storage 需要显式 recording_encoding"
                )
            for relative in compacted_originals:
                self._artifact_identities.pop(relative, None)
        finished_stage("manifest_compaction")
        return manifest, compacted_originals

    def _publish_manifest(
        self,
        manifest: dict[str, object],
        payload: bytes,
        transaction: NativeSessionTransaction | None,
        before_publish: Callable[[], None] | None,
        compacted_originals: list[str],
        finished_stage: Callable[[str], None],
        mark_published: Callable[[], None],
    ) -> None:
        native_manifest_sha256 = None
        if before_publish is None and transaction is not None:
            native_manifest_sha256 = seal_native_transaction(
                transaction,
                self._partial,
                self._final,
                self._plan.session_id,
                payload,
                self._artifact_identities,
            )
        if native_manifest_sha256 is None:
            self._validate_artifact_bytes(manifest)
            if before_publish is not None:
                before_publish()
            self._validate_artifact_bytes(manifest)
            manifest_path = self._partial / "manifest.json"
            with manifest_path.open("xb") as stream:
                self._before_write("manifest", payload)
                self._validate_artifact_bytes(manifest)
                if stream.write(payload) != len(payload):
                    raise OSError("manifest.json 发生短写")
                stream.flush()
                os.fsync(stream.fileno())
            for subdir_name in ("video", "audio"):
                subdir = self._partial / subdir_name
                if subdir.is_dir():
                    fsync_directory(subdir)
            fsync_directory(self._partial)
            os.rename(self._partial, self._final)
            mark_published()
            fsync_directory(self._root)
            for control_name in ("recording.json", "capture.json"):
                (self._final / control_name).unlink(missing_ok=True)
            fsync_directory(self._final)
        else:
            mark_published()
        # Power loss before this point leaves all raw recovery journals intact.
        audio_unlinked = False
        for relative in compacted_originals:
            with suppress(OSError):
                (self._final / relative).unlink()
                if relative.startswith("audio/"):
                    audio_unlinked = True
        if compacted_originals:
            fsync_directory(self._final)
        if audio_unlinked and (self._final / "audio").is_dir():
            fsync_directory(self._final / "audio")
        with self._lock:
            self._state = "sealed"
            self._native_transaction = None
        if native_manifest_sha256 is None:
            self._validate_artifact_bytes(manifest, root=self._final)
        finished_stage("publication")

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
            assert self._started_monotonic_ns is not None
            stop_observed_ns = time.monotonic_ns()
            self._stopped_elapsed = max(0.0, (stop_observed_ns - self._started_monotonic_ns) / 1e9)
        published = False
        stage_started = time.monotonic()

        def finished_stage(stage: str) -> None:
            nonlocal stage_started
            now = time.monotonic()
            operational_logger("recording").event(
                "sealing_stage",
                stage=stage,
                transaction_id=self._plan.session_id,
                duration_ms=(now - stage_started) * 1000,
            )
            stage_started = now

        def mark_published() -> None:
            nonlocal published
            published = True

        try:
            transaction = self._native_transaction
            manifest, live_audit, prepared = self._drain_capture(
                transaction, stop_observed_ns, finished_stage
            )
            manifest, compacted_originals = self._compact_and_audit(
                manifest, live_audit, prepared, finished_stage
            )
            payload = json_bytes(manifest)
            validated_device_session_payload(payload, self._plan.session_id)
            self._publish_manifest(
                manifest,
                payload,
                transaction,
                before_publish,
                compacted_originals,
                finished_stage,
                mark_published,
            )
        except BaseException as error:
            self._stop_writer()
            self._abandon_audio()
            self._abandon_encoder(abort_native=True, abort_reason=str(error))
            self._close_files(ignore_errors=True)
            if not published and self._final.exists() and not self._partial.exists():
                published = True
            if published:
                code, _, _ = _failure_details(error)
                raise DeviceRecordingError(
                    str(code),
                    f"device-session 会话发布后的独立校验失败：{error}",
                ) from error
            with suppress(OSError):
                (self._partial / "manifest.json").unlink(missing_ok=True)
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
            dict(self._verified_artifacts),
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

    def _abandon_encoder(
        self,
        *,
        abort_native: bool = True,
        abort_reason: str = "recording abandoned",
    ) -> None:
        self._harvest_stop.set()
        with self._lock:
            harvester = self._harvester
        if harvester is not None:
            harvester.join(timeout=5)
            if not harvester.is_alive():
                with self._lock:
                    if self._harvester is harvester:
                        self._harvester = None
                harvester = None
        if harvester is None:
            # Include pairs that closed after the last periodic harvest.
            with suppress(Exception):
                self._harvest_segments()
        with self._lock:
            transaction = self._native_transaction
            self._native_transaction = None
            encoder = self._encoder
            self._encoder = None
            live_sealing = self._live_sealing
            self._live_sealing = None
        if transaction is not None and abort_native:
            # Preserve native progress before abort releases the transaction.
            # Failed recordings must not report zero frames after writing media.
            self._snapshot_native_progress(transaction)
            with suppress(BaseException):
                transaction.abort(abort_reason)
        if encoder is not None:
            with suppress(BaseException):
                encoder.abort()
        if live_sealing is not None:
            with suppress(BaseException):
                live_sealing.close()

    def _abandon_audio(self) -> None:
        with self._lock:
            recorder = self._audio_recorder
            self._audio_recorder = None
        abandon_audio_recorder(recorder)

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


def inspect_device_session_directory(
    path: str | Path,
    *,
    expected_session_id: str | None = None,
) -> tuple[Mapping[str, object], bytes]:
    """校验 sealed manifest 和 artifact 身份/大小，不读取 artifact 内容。"""
    return _mb_inspect_device_session_directory(
        path,
        expected_session_id=expected_session_id,
        open_regular_fn=_open_regular_at,
        read_bounded_fn=_read_bounded_fd,
    )


def validate_device_session_directory(
    path: str | Path,
    *,
    expected_session_id: str | None = None,
    interrupt_check: Callable[[], None] | None = None,
    verified_artifacts: Mapping[str, VerifiedArtifact] | None = None,
    progress: Callable[[int, int], None] | None = None,
) -> Mapping[str, object]:
    """校验 manifest 和所有 artifact 的当前内容。

    保留 verified_artifacts 参数以兼容调用方，但文件属性相同不能证明
    内容不变：包括本次封存/恢复在内，每次发布校验都完整读回。
    """
    return _sv_validate_device_session_directory(
        path,
        expected_session_id=expected_session_id,
        interrupt_check=interrupt_check,
        verified_artifacts=verified_artifacts,
        progress=progress,
        open_regular_fn=_open_regular_at,
        read_bounded_fn=_read_bounded_fd,
        verify_fd_fn=_verify_artifact_fd,
        artifacts_fn=_manifest_artifacts,
        executor_cls=ThreadPoolExecutor,
    )

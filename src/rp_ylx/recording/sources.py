"""将现有相机与 IMU 采集接口接入 CaptureCoordinator。"""

from __future__ import annotations

import threading
from collections.abc import Callable
from contextlib import suppress
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

from rp_ylx.api.preview import LatestPreviewBuffer
from rp_ylx.camera import CameraError, CameraMode, FrameObservation
from rp_ylx.imu import ImuObservation, decode_native_imu_observation
from rp_ylx.native import (
    NativeCaptureEngine,
    NativeCapturePlan,
    create_native_capture_engine,
)
from rp_ylx.performance.metrics import PerformanceMetrics


class CaptureCamera(Protocol):
    def open(self, mode: CameraMode, *, stable_id: str | None = None) -> object: ...

    def start(self) -> None: ...

    def read(self, *, timeout: float) -> FrameObservation: ...

    def stop(self) -> None: ...

    def close(self) -> None: ...

    def camera_focus_status(self) -> dict[str, object] | None: ...

    def set_camera_focus(
        self,
        *,
        value: int | None = None,
        auto_enabled: bool | None = None,
    ) -> dict[str, object]: ...


class CaptureImu(Protocol):
    def read(self, *, timeout: float) -> ImuObservation: ...

    def close(self) -> None: ...


class NativeContinuousCaptureSources:
    """生产路径：Rust 拥有连续相机采集、preview 和录制 fanout。"""

    keeps_preview_after_stop = True
    supports_calibration_capture = True
    requires_native_transaction = True

    def __init__(
        self,
        device: str,
        camera_mode: CameraMode,
        *,
        preview: LatestPreviewBuffer,
        read_timeout: float = 2.0,
        frame_decimation: int = 1,
        buffer_count: int = 16,
        queue_capacity: int = 64,
        metrics: PerformanceMetrics | None = None,
        engine_factory: Callable[[NativeCapturePlan, object, object | None], NativeCaptureEngine]
        | None = None,
    ) -> None:
        if read_timeout <= 0:
            raise ValueError("采集来源 read_timeout 必须大于零")
        if frame_decimation <= 0:
            raise ValueError("采集来源 frame_decimation 必须大于零")
        if buffer_count <= 0 or queue_capacity <= 0:
            raise ValueError("原生采集 buffer_count 和 queue_capacity 必须大于零")
        rounded_fps = round(camera_mode.fps)
        if abs(camera_mode.fps - rounded_fps) > max(0.01, camera_mode.fps * 0.001):
            raise ValueError("原生连续采集只接受整数帧率")
        self._device = device
        self._camera_mode = camera_mode
        self._preview = preview
        self._read_timeout = read_timeout
        self._frame_decimation = frame_decimation
        self._buffer_count = buffer_count
        self._queue_capacity = queue_capacity
        self._metrics = metrics
        self._engine_factory = engine_factory
        self._lock = threading.RLock()
        self._engine: NativeCaptureEngine | None = None
        self._recording: _EngineRecording | None = None
        self._open_handles = 0
        self._last_preview_error: tuple[str, str] | None = None

    @property
    def open_handle_count(self) -> int:
        with self._lock:
            return self._open_handles

    @property
    def last_preview_error(self) -> tuple[str, str] | None:
        with self._lock:
            return self._last_preview_error

    def latest_imu_observation(self) -> ImuObservation | None:
        with self._lock:
            engine = self._engine
        if engine is None:
            return None
        raw = engine.latest_imu_observation()
        return None if raw is None else decode_native_imu_observation(raw)

    def camera_connection_status(self) -> dict[str, object]:
        return {
            "schema": "ylx.camera-connection.v1",
            "state": "connected" if Path(self._device).exists() else "disconnected",
        }

    def start_preview(self) -> None:
        stale_engine: NativeCaptureEngine | None = None
        with self._lock:
            engine = self._engine
            if engine is not None:
                try:
                    running = engine.snapshot().get("running") is True
                except BaseException:
                    running = False
                if running or self._recording is not None:
                    return
                stale_engine = engine
                self._engine = None
                self._open_handles -= 1
        if stale_engine is not None:
            with suppress(BaseException):
                stale_engine.close(self._read_timeout + 1.0)
        native_preview = self._preview.native_owner
        if native_preview is None:
            raise RuntimeError("正式连续采集需要 Rust preview buffer")
        rounded_fps = round(self._camera_mode.fps)
        plan = NativeCapturePlan(
            device=self._device,
            width=self._camera_mode.width,
            height=self._camera_mode.height,
            fps=rounded_fps,
            encoding=self._camera_mode.encoding,
            buffer_count=self._buffer_count,
            queue_capacity=self._queue_capacity,
            frame_decimation=self._frame_decimation,
            read_timeout_seconds=self._read_timeout,
            imu_timeout_seconds=self._read_timeout,
        )
        engine: NativeCaptureEngine | None = None
        try:
            if self._engine_factory is None:
                engine = create_native_capture_engine(
                    plan,
                    native_preview,
                    metrics=None if self._metrics is None else self._metrics.native_owner,
                )
            else:
                engine = self._engine_factory(
                    plan,
                    native_preview,
                    None if self._metrics is None else self._metrics.native_owner,
                )
            engine.start_preview()
        except BaseException as error:
            if engine is not None:
                with suppress(BaseException):
                    engine.close(self._read_timeout + 1.0)
            code = str(getattr(error, "code", "camera_preview_unavailable"))
            message = str(getattr(error, "message", error)) or code
            with self._lock:
                self._last_preview_error = (code, message)
            raise
        assert engine is not None
        with self._lock:
            if self._engine is not None:
                with suppress(BaseException):
                    engine.close(self._read_timeout + 1.0)
                return
            self._engine = engine
            self._open_handles += 1
            self._last_preview_error = None

    def start(
        self,
        *,
        mode: str,
        generation_id: str,
        submit_frame: Callable[[FrameObservation], bool],
        submit_imu: Callable[[ImuObservation], bool],
        on_failure: Callable[[str, str], None],
        native_recorder: object | None = None,
    ) -> None:
        if mode not in {"production", "calibration"}:
            raise RuntimeError("采集模式无效")
        del submit_frame, submit_imu
        transaction_factory = getattr(native_recorder, "native_recording_transaction", None)
        if not callable(transaction_factory):
            raise RuntimeError("正式录制要求 Rust SessionTransaction")
        transaction = transaction_factory()
        if transaction is None:
            raise RuntimeError("Rust SessionTransaction 尚未启动")
        self.start_preview()
        tap = _EngineRecording(generation_id, on_failure)
        with self._lock:
            if self._recording is not None:
                raise RuntimeError("采集来源已经在录制")
            engine = self._engine
            if engine is None:
                raise RuntimeError("Rust CaptureEngine 未启动")
            self._recording = tap
            self._open_handles += 1
        try:
            engine.start_recording(
                transaction,
                lambda code, message: self._runtime_failure(tap, code, message),
            )
        except BaseException:
            with self._lock:
                if self._recording is tap:
                    self._recording = None
                    self._open_handles -= 1
            with suppress(BaseException):
                engine.stop_recording(self._read_timeout + 1.0)
            raise

    def _runtime_failure(self, tap: _EngineRecording, code: object, message: object) -> None:
        failure_code = str(code)
        failure_message = str(message) or failure_code
        with self._lock:
            if self._recording is tap:
                self._recording = None
                self._open_handles -= 1
            self._last_preview_error = (failure_code, failure_message)
        tap.on_failure(failure_code, failure_message)

    def stop(self) -> None:
        with self._lock:
            tap = self._recording
            self._recording = None
            engine = self._engine
            if tap is not None:
                self._open_handles -= 1
        if engine is not None:
            engine.stop_recording(self._read_timeout + 1.0)

    def _engine_for_control(self) -> NativeCaptureEngine:
        self.start_preview()
        with self._lock:
            engine = self._engine
        if engine is None:
            raise RuntimeError("Rust CaptureEngine 尚未启动")
        return engine

    def camera_focus_status(self) -> dict[str, object] | None:
        try:
            return self._engine_for_control().camera_focus_status()
        except RuntimeError as error:
            code = str(error).partition(": ")[0]
            if code == "camera_focus_unsupported":
                return None
            raise CameraError(code, str(error), retryable=True) from error

    def set_camera_focus(
        self,
        *,
        value: int | None = None,
        auto_enabled: bool | None = None,
    ) -> dict[str, object]:
        try:
            return self._engine_for_control().set_camera_focus(value, auto_enabled)
        except RuntimeError as error:
            code = str(error).partition(": ")[0]
            retryable = code in {
                "camera_focus_open_failed",
                "camera_focus_get_failed",
                "camera_focus_set_failed",
                "camera_focus_query_failed",
                "native_focus_status_failed",
                "native_focus_set_failed",
            }
            raise CameraError(code, str(error), retryable=retryable) from error

    def close(self) -> None:
        with suppress(BaseException):
            self.stop()
        with self._lock:
            engine = self._engine
            self._engine = None
            if engine is not None:
                self._open_handles -= 1
        if engine is not None:
            with suppress(BaseException):
                engine.close(self._read_timeout + 1.0)


@dataclass(slots=True)
class _EngineRecording:
    generation_id: str
    on_failure: Callable[[str, str], None]


@dataclass(slots=True)
class _RecordingTap:
    generation_id: str
    submit_frame: Callable[[FrameObservation], bool]
    submit_imu: Callable[[ImuObservation], bool]
    on_failure: Callable[[str, str], None]
    frame_decimation: int
    imu: CaptureImu | None = None
    imu_thread: threading.Thread | None = None
    failure_reported: bool = False
    first_frame: bool = True
    observed_frames: int = 0
    stopping: bool = False
    inflight_frames: int = 0
    latest_imu: ImuObservation | None = None


class ContinuousCaptureSources:
    """保持相机常开用于预览，录制时只挂接落盘回调。"""

    keeps_preview_after_stop = True
    supports_calibration_capture = True

    def __init__(
        self,
        camera_factory: Callable[[], CaptureCamera],
        imu_factory: Callable[[], CaptureImu],
        camera_mode: CameraMode,
        *,
        publish_preview: Callable[[bytes], object],
        stable_id: str | None = None,
        read_timeout: float = 2.0,
        warmup_frames: int = 0,
        frame_decimation: int = 1,
    ) -> None:
        if read_timeout <= 0:
            raise ValueError("采集来源 read_timeout 必须大于零")
        if warmup_frames < 0:
            raise ValueError("采集来源 warmup_frames 不能为负数")
        if frame_decimation <= 0:
            raise ValueError("采集来源 frame_decimation 必须大于零")
        self._camera_factory = camera_factory
        self._imu_factory = imu_factory
        self._camera_mode = camera_mode
        self._publish_preview = publish_preview
        self._stable_id = stable_id
        self._read_timeout = read_timeout
        self._warmup_frames = warmup_frames
        self._frame_decimation = frame_decimation
        self._lock = threading.RLock()
        self._condition = threading.Condition(self._lock)
        self._stop = threading.Event()
        self._camera: CaptureCamera | None = None
        self._camera_thread: threading.Thread | None = None
        self._recording: _RecordingTap | None = None
        self._preview_started = False
        self._open_handles = 0
        self._last_preview_error: tuple[str, str] | None = None

    @property
    def open_handle_count(self) -> int:
        with self._lock:
            return self._open_handles

    @property
    def last_preview_error(self) -> tuple[str, str] | None:
        with self._lock:
            return self._last_preview_error

    def start_preview(self) -> None:
        with self._lock:
            if self._camera_thread is not None or self._camera is not None:
                return
            self._stop.clear()
            self._last_preview_error = None
        camera = self._camera_factory()
        registered = False
        try:
            if self._stable_id is None:
                camera.open(self._camera_mode)
            else:
                camera.open(self._camera_mode, stable_id=self._stable_id)
            camera.start()
            with self._lock:
                if self._stop.is_set():
                    raise RuntimeError("采集来源已经停止")
                self._camera = camera
                self._open_handles += 1
                registered = True
            for _ in range(self._warmup_frames):
                if self._stop.is_set():
                    raise RuntimeError("采集来源已经停止")
                observation = camera.read(timeout=self._read_timeout)
                self._publish_observation_preview(observation)
            thread = threading.Thread(
                target=self._camera_loop,
                args=(camera,),
                name="rp-ylx-preview-camera",
                daemon=False,
            )
            with self._lock:
                if self._stop.is_set() or self._camera is not camera:
                    raise RuntimeError("采集来源已经停止")
                self._camera_thread = thread
                self._preview_started = True
            thread.start()
        except BaseException:
            if registered:
                with suppress(BaseException):
                    self.close()
            else:
                with suppress(BaseException):
                    camera.close()
            raise

    def _camera_for_control(self) -> CaptureCamera:
        self.start_preview()
        with self._lock:
            camera = self._camera
        if camera is None:
            raise RuntimeError("预览相机尚未启动")
        return camera

    def camera_focus_status(self) -> dict[str, object] | None:
        return self._camera_for_control().camera_focus_status()

    def set_camera_focus(
        self,
        *,
        value: int | None = None,
        auto_enabled: bool | None = None,
    ) -> dict[str, object]:
        return self._camera_for_control().set_camera_focus(
            value=value,
            auto_enabled=auto_enabled,
        )

    def start(
        self,
        *,
        mode: str,
        generation_id: str,
        submit_frame: Callable[[FrameObservation], bool],
        submit_imu: Callable[[ImuObservation], bool],
        on_failure: Callable[[str, str], None],
        native_recorder: object | None = None,
    ) -> None:
        del mode, native_recorder
        self.start_preview()
        imu = self._imu_factory()
        tap = _RecordingTap(
            generation_id,
            submit_frame,
            submit_imu,
            on_failure,
            self._frame_decimation,
            imu=imu,
        )
        thread = threading.Thread(
            target=self._imu_loop,
            args=(tap,),
            name="rp-ylx-capture-imu",
            daemon=False,
        )
        tap.imu_thread = thread
        with self._lock:
            if self._recording is not None:
                with suppress(BaseException):
                    imu.close()
                raise RuntimeError("采集来源已经在录制")
            self._recording = tap
            self._open_handles += 1
        try:
            thread.start()
        except BaseException:
            with self._lock:
                if self._recording is tap:
                    self._recording = None
                    self._open_handles -= 1
            with suppress(BaseException):
                imu.close()
            raise

    def _camera_loop(self, camera: CaptureCamera) -> None:
        try:
            while not self._stop.is_set():
                observation = camera.read(timeout=self._read_timeout)
                if self._stop.is_set():
                    break
                preview_jpeg = self._preview_jpeg(observation)
                tap = self._recording_snapshot()
                if preview_jpeg is not None:
                    self._publish_preview_jpeg(preview_jpeg)
                if tap is not None:
                    recording_observation = self._begin_recording_frame(tap, observation)
                    if recording_observation is None:
                        continue
                    try:
                        tap.submit_frame(recording_observation)
                    except BaseException as error:
                        self._report_recording_failure(tap, error, "camera_failed")
                    finally:
                        self._finish_recording_frame(tap)
        except BaseException as error:
            if not self._stop.is_set():
                self._record_preview_error(error, "camera_failed")
                tap = self._recording_snapshot()
                if tap is not None:
                    self._report_recording_failure(tap, error, "camera_failed")
        finally:
            self._release_camera(camera)

    def _imu_loop(self, tap: _RecordingTap) -> None:
        assert tap.imu is not None
        try:
            while not self._stop.is_set() and self._recording_snapshot() is tap:
                observation = tap.imu.read(timeout=self._read_timeout)
                if self._stop.is_set() or self._recording_snapshot() is not tap:
                    break
                with self._lock:
                    if self._recording is tap:
                        tap.latest_imu = observation
                tap.submit_imu(observation)
        except BaseException as error:
            if not self._stop.is_set() and self._recording_snapshot() is tap:
                self._report_recording_failure(tap, error, "imu_failed")
        finally:
            self._release_recording_imu(tap)

    def _preview_jpeg(self, observation: FrameObservation) -> bytes | None:
        return observation.frame.left or observation.frame.raw_side_by_side

    def _publish_preview_jpeg(self, jpeg: bytes) -> None:
        with suppress(ValueError):
            self._publish_preview(jpeg)

    def _publish_observation_preview(self, observation: FrameObservation) -> None:
        jpeg = self._preview_jpeg(observation)
        if jpeg is not None:
            self._publish_preview_jpeg(jpeg)

    def _begin_recording_frame(
        self,
        tap: _RecordingTap,
        observation: FrameObservation,
    ) -> FrameObservation | None:
        with self._condition:
            if self._recording is not tap or tap.stopping:
                return None
            if tap.first_frame:
                tap.first_frame = False
                tap.observed_frames = 1
                tap.inflight_frames += 1
                if observation.dropped_before:
                    return FrameObservation(observation.frame, dropped_before=0)
                return observation
            observed_index = tap.observed_frames
            tap.observed_frames += observation.dropped_before + 1
            if observation.dropped_before:
                tap.inflight_frames += 1
                return observation
            if observed_index % tap.frame_decimation != 0:
                return None
            tap.inflight_frames += 1
            return observation

    def _finish_recording_frame(self, tap: _RecordingTap) -> None:
        with self._condition:
            tap.inflight_frames -= 1
            self._condition.notify_all()

    def _recording_snapshot(self) -> _RecordingTap | None:
        with self._lock:
            return self._recording

    def _record_preview_error(self, error: BaseException, fallback_code: str) -> None:
        code = str(getattr(error, "code", fallback_code))
        message = str(getattr(error, "message", error)) or code
        with self._lock:
            self._last_preview_error = (code, message)

    def _report_recording_failure(
        self,
        tap: _RecordingTap,
        error: BaseException,
        fallback_code: str,
    ) -> None:
        with self._condition:
            if self._recording is not tap:
                return
            if tap.failure_reported:
                return
            tap.failure_reported = True
            tap.stopping = True
            self._condition.notify_all()
        code = str(getattr(error, "code", fallback_code))
        message = str(getattr(error, "message", error)) or code
        reporter = threading.Thread(
            target=self._finish_recording_failure,
            args=(tap, code, message),
            name="rp-ylx-capture-failure",
            daemon=False,
        )
        reporter.start()

    def _finish_recording_failure(self, tap: _RecordingTap, code: str, message: str) -> None:
        with suppress(BaseException):
            self.stop()
        tap.on_failure(code, message)

    def _release_camera(self, camera: CaptureCamera) -> None:
        with self._lock:
            if self._camera is not camera:
                return
            self._camera = None
            self._open_handles -= 1
            self._camera_thread = None
        with suppress(BaseException):
            camera.stop()
        with suppress(BaseException):
            camera.close()

    def _release_recording_imu(self, tap: _RecordingTap) -> None:
        with self._lock:
            imu = tap.imu
            if imu is None:
                return
            if self._recording is tap:
                self._recording = None
            tap.imu = None
            tap.imu_thread = None
            self._open_handles -= 1
        with suppress(BaseException):
            imu.close()

    def stop(self) -> None:
        timed_out = False
        with self._condition:
            tap = self._recording
            if tap is not None:
                tap.stopping = True
                self._recording = None
                self._condition.wait_for(
                    lambda: tap.inflight_frames == 0,
                    timeout=self._read_timeout + 1.0,
                )
                timed_out = bool(tap.inflight_frames)
        if tap is not None:
            if tap.imu is not None:
                with suppress(BaseException):
                    tap.imu.close()
            thread = tap.imu_thread
            if thread is not None and thread is not threading.current_thread():
                thread.join(timeout=self._read_timeout + 1.0)
                if thread.is_alive():
                    raise RuntimeError("IMU 采集线程未能在关闭期限内退出")
            if tap.imu is not None:
                self._release_recording_imu(tap)
            if timed_out:
                raise RuntimeError("相机录制提交未能在关闭期限内退出")

    def close(self) -> None:
        self._stop.set()
        with suppress(BaseException):
            self.stop()
        with self._lock:
            camera = self._camera
            camera_thread = self._camera_thread
        if camera is not None:
            with suppress(BaseException):
                camera.close()
        if camera_thread is not None and camera_thread is not threading.current_thread():
            camera_thread.join(timeout=self._read_timeout + 1.0)
            if camera_thread.is_alive():
                raise RuntimeError("预览相机线程未能在关闭期限内退出")
        with self._lock:
            self._recording = None
            self._preview_started = False

"""将现有相机与 IMU 采集接口接入 CaptureCoordinator。"""

from __future__ import annotations

import threading
from collections.abc import Callable
from contextlib import suppress
from dataclasses import dataclass
from typing import Protocol

from rp_ylx.camera import CameraMode, FrameObservation
from rp_ylx.imu import ImuObservation


class CaptureCamera(Protocol):
    def open(self, mode: CameraMode, *, stable_id: str | None = None) -> object: ...

    def start(self) -> None: ...

    def read(self, *, timeout: float) -> FrameObservation: ...

    def stop(self) -> None: ...

    def close(self) -> None: ...


class CaptureImu(Protocol):
    def read(self, *, timeout: float) -> ImuObservation: ...

    def close(self) -> None: ...


class ThreadedCaptureSources:
    """每次录制创建并释放一组真实相机/IMU 来源。"""

    def __init__(
        self,
        camera_factory: Callable[[], CaptureCamera],
        imu_factory: Callable[[], CaptureImu],
        camera_mode: CameraMode,
        *,
        stable_id: str | None = None,
        read_timeout: float = 2.0,
        warmup_frames: int = 0,
    ) -> None:
        if read_timeout <= 0:
            raise ValueError("采集来源 read_timeout 必须大于零")
        if warmup_frames < 0:
            raise ValueError("采集来源 warmup_frames 不能为负数")
        self._camera_factory = camera_factory
        self._imu_factory = imu_factory
        self._camera_mode = camera_mode
        self._stable_id = stable_id
        self._read_timeout = read_timeout
        self._warmup_frames = warmup_frames
        self._lock = threading.RLock()
        self._stop = threading.Event()
        self._camera: CaptureCamera | None = None
        self._imu: CaptureImu | None = None
        self._threads: list[threading.Thread] = []
        self._failure_thread: threading.Thread | None = None
        self._open_handles = 0
        self._failure_reported = False
        self._on_failure: Callable[[str, str], None] | None = None

    @property
    def open_handle_count(self) -> int:
        with self._lock:
            return self._open_handles

    def start(
        self,
        *,
        mode: str,
        generation_id: str,
        submit_frame: Callable[[FrameObservation], bool],
        submit_imu: Callable[[ImuObservation], bool],
        on_failure: Callable[[str, str], None],
    ) -> None:
        del mode, generation_id
        with self._lock:
            if self._threads or self._camera is not None or self._imu is not None:
                raise RuntimeError("采集来源已经启动")
            self._stop.clear()
            self._failure_reported = False
            self._on_failure = on_failure
        camera = self._camera_factory()
        imu = self._imu_factory()
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
                self._imu = imu
                self._open_handles = 2
                registered = True
            for _ in range(self._warmup_frames):
                if self._stop.is_set():
                    raise RuntimeError("采集来源已经停止")
                camera.read(timeout=self._read_timeout)
        except BaseException:
            if registered:
                with suppress(BaseException):
                    self.stop()
            else:
                with suppress(BaseException):
                    camera.close()
                with suppress(BaseException):
                    imu.close()
            raise
        try:
            with self._lock:
                if self._stop.is_set() or self._camera is not camera or self._imu is not imu:
                    raise RuntimeError("采集来源已经停止")
                camera_thread = threading.Thread(
                    target=self._camera_loop,
                    args=(camera, submit_frame),
                    name="rp-ylx-capture-camera",
                    daemon=False,
                )
                imu_thread = threading.Thread(
                    target=self._imu_loop,
                    args=(imu, submit_imu),
                    name="rp-ylx-capture-imu",
                    daemon=False,
                )
                self._threads = [camera_thread, imu_thread]
        except BaseException:
            with suppress(BaseException):
                self.stop()
            raise
        try:
            camera_thread.start()
            imu_thread.start()
        except BaseException:
            self.stop()
            raise

    def _camera_loop(
        self,
        camera: CaptureCamera,
        submit: Callable[[FrameObservation], bool],
    ) -> None:
        error: BaseException | None = None
        try:
            while not self._stop.is_set():
                observation = camera.read(timeout=self._read_timeout)
                if self._stop.is_set():
                    break
                submit(observation)
        except BaseException as caught:
            if not self._stop.is_set():
                error = caught
                self._stop.set()
        finally:
            self._release_camera(camera)
        if error is not None:
            self._report_failure(error, "camera_failed")

    def _imu_loop(
        self,
        imu: CaptureImu,
        submit: Callable[[ImuObservation], bool],
    ) -> None:
        error: BaseException | None = None
        try:
            while not self._stop.is_set():
                observation = imu.read(timeout=self._read_timeout)
                if self._stop.is_set():
                    break
                submit(observation)
        except BaseException as caught:
            if not self._stop.is_set():
                error = caught
                self._stop.set()
        finally:
            self._release_imu(imu)
        if error is not None:
            self._report_failure(error, "imu_failed")

    def _report_failure(self, error: BaseException, fallback_code: str) -> None:
        with self._lock:
            if self._failure_reported:
                return
            self._failure_reported = True
            callback = self._on_failure
        if callback is None:
            return
        code = str(getattr(error, "code", fallback_code))
        message = str(getattr(error, "message", error)) or code
        reporter = threading.Thread(
            target=self._finish_failure,
            args=(callback, code, message),
            name="rp-ylx-capture-failure",
            daemon=False,
        )
        with self._lock:
            self._failure_thread = reporter
        reporter.start()

    def _finish_failure(
        self,
        callback: Callable[[str, str], None],
        code: str,
        message: str,
    ) -> None:
        try:
            self.stop()
            callback(code, message)
        finally:
            with self._lock:
                if self._failure_thread is threading.current_thread():
                    self._failure_thread = None

    def _release_camera(self, camera: CaptureCamera) -> None:
        with suppress(BaseException):
            camera.stop()
        with suppress(BaseException):
            camera.close()
        with self._lock:
            if self._camera is camera:
                self._camera = None
                self._open_handles -= 1

    def _release_imu(self, imu: CaptureImu) -> None:
        with suppress(BaseException):
            imu.close()
        with self._lock:
            if self._imu is imu:
                self._imu = None
                self._open_handles -= 1

    def stop(self) -> None:
        self._stop.set()
        current = threading.current_thread()
        with self._lock:
            threads = tuple(self._threads)
            failure_thread = self._failure_thread
            camera = self._camera
            imu = self._imu
        if camera is not None:
            with suppress(BaseException):
                camera.close()
        if imu is not None:
            with suppress(BaseException):
                imu.close()
        for thread in threads:
            if thread is not current and thread.ident is not None:
                thread.join(timeout=self._read_timeout + 1.0)
        if failure_thread is not None and failure_thread is not current:
            failure_thread.join(timeout=self._read_timeout + 2.0)
        still_alive = [thread for thread in threads if thread is not current and thread.is_alive()]
        if (
            failure_thread is not None
            and failure_thread is not current
            and failure_thread.is_alive()
        ):
            still_alive.append(failure_thread)
        if still_alive:
            raise RuntimeError("采集来源线程未能在关闭期限内退出")
        with self._lock:
            self._threads = [thread for thread in self._threads if thread is current]
            if camera is not None and self._camera is camera:
                self._release_camera(camera)
            if imu is not None and self._imu is imu:
                self._release_imu(imu)
            if current not in self._threads:
                self._threads.clear()
            self._on_failure = None


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


class ContinuousCaptureSources:
    """保持相机常开用于预览，录制时只挂接落盘回调。"""

    keeps_preview_after_stop = True

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

    def start(
        self,
        *,
        mode: str,
        generation_id: str,
        submit_frame: Callable[[FrameObservation], bool],
        submit_imu: Callable[[ImuObservation], bool],
        on_failure: Callable[[str, str], None],
    ) -> None:
        del mode
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
                self._publish_observation_preview(observation)
                tap = self._recording_snapshot()
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
                tap.submit_imu(observation)
        except BaseException as error:
            if not self._stop.is_set() and self._recording_snapshot() is tap:
                self._report_recording_failure(tap, error, "imu_failed")
        finally:
            self._release_recording_imu(tap)

    def _publish_observation_preview(self, observation: FrameObservation) -> None:
        jpeg = observation.frame.left or observation.frame.raw_side_by_side
        if jpeg is not None:
            with suppress(ValueError):
                self._publish_preview(jpeg)

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
            if self._recording is not tap or tap.failure_reported:
                return
            tap.failure_reported = True
            tap.stopping = True
            self._recording = None
            self._condition.notify_all()
        code = str(getattr(error, "code", fallback_code))
        message = str(getattr(error, "message", error)) or code
        reporter = threading.Thread(
            target=tap.on_failure,
            args=(code, message),
            name="rp-ylx-capture-failure",
            daemon=False,
        )
        reporter.start()

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

"""将现有相机与 IMU 采集接口接入 CaptureCoordinator。"""

from __future__ import annotations

import threading
from collections.abc import Callable
from contextlib import suppress
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
    ) -> None:
        if read_timeout <= 0:
            raise ValueError("采集来源 read_timeout 必须大于零")
        self._camera_factory = camera_factory
        self._imu_factory = imu_factory
        self._camera_mode = camera_mode
        self._stable_id = stable_id
        self._read_timeout = read_timeout
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
        try:
            if self._stable_id is None:
                camera.open(self._camera_mode)
            else:
                camera.open(self._camera_mode, stable_id=self._stable_id)
            camera.start()
        except BaseException:
            with suppress(BaseException):
                camera.close()
            with suppress(BaseException):
                imu.close()
            raise
        with self._lock:
            self._camera = camera
            self._imu = imu
            self._open_handles = 2
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

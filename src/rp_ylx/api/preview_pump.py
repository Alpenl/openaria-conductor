"""Background bridge from a real camera controller to the preview cache."""

from __future__ import annotations

import logging
import threading
from collections.abc import Callable
from typing import Protocol

from rp_ylx.api.mock_device import MockDevice
from rp_ylx.camera.models import CameraMode, FrameObservation
from rp_ylx.performance.metrics import PayloadLease, PerformanceMetrics


class PreviewController(Protocol):
    """The small controller surface needed by :class:`CameraPreviewPump`."""

    state: str

    def open(self, mode: CameraMode, *, stable_id: str | None = None) -> object: ...

    def start(self) -> None: ...

    def read(self, *, timeout: float) -> FrameObservation: ...

    def stop(self) -> None: ...

    def close(self) -> None: ...


class CameraPreviewPump:
    """Continuously publish camera frames into a bounded device preview cache.

    The worker is deliberately a plain, non-daemon thread.  Callers must stop
    the pump and join it before process shutdown so camera resources are not
    left behind.  A controller is injected to keep this bridge testable; the
    hardware CLI supplies ``CameraController(V4L2DiscoveryBackend())``.
    """

    def __init__(
        self,
        device: MockDevice,
        controller: PreviewController,
        mode: CameraMode,
        *,
        stable_id: str | None = None,
        read_timeout: float = 2.0,
        on_error: Callable[[BaseException], None] | None = None,
        logger: logging.Logger | None = None,
        thread_name: str = "rp-ylx-camera-preview",
        metrics: PerformanceMetrics | None = None,
    ) -> None:
        if read_timeout <= 0:
            raise ValueError("read_timeout 必须大于零")
        if not thread_name:
            raise ValueError("thread_name 不能为空")
        self._device = device
        self._controller = controller
        self._mode = mode
        self._stable_id = stable_id
        self._read_timeout = read_timeout
        self._on_error = on_error
        self._logger = logger or logging.getLogger(__name__)
        self._thread_name = thread_name
        self._stop_event = threading.Event()
        self._lock = threading.Lock()
        self._thread: threading.Thread | None = None
        self._state = "new"
        self._error: BaseException | None = None
        self._frames_published = 0
        self._metrics = metrics
        self._preview_lease: PayloadLease | None = None

    @property
    def state(self) -> str:
        with self._lock:
            return self._state

    @property
    def error(self) -> BaseException | None:
        with self._lock:
            return self._error

    @property
    def frames_published(self) -> int:
        with self._lock:
            return self._frames_published

    @property
    def thread(self) -> threading.Thread | None:
        with self._lock:
            return self._thread

    @property
    def is_alive(self) -> bool:
        thread = self.thread
        return thread is not None and thread.is_alive()

    def start(self) -> None:
        """Open and start the controller, then launch the pump worker."""

        with self._lock:
            if self._state not in {"new", "stopped"} or (
                self._thread is not None and self._thread.is_alive()
            ):
                raise RuntimeError(f"预览 pump 当前不能启动：{self._state}")
            self._state = "starting"
            self._error = None
            self._frames_published = 0
        self._stop_event.clear()
        try:
            if self._stable_id is None:
                self._controller.open(self._mode)
            else:
                self._controller.open(self._mode, stable_id=self._stable_id)
            self._controller.start()
        except BaseException as exc:
            try:
                self._controller.close()
            except BaseException as close_error:
                self._logger.error("关闭启动失败的相机时出错：%s", close_error)
            with self._lock:
                self._state = "failed"
                self._error = exc
            raise

        thread = threading.Thread(target=self._run, name=self._thread_name, daemon=False)
        with self._lock:
            self._thread = thread
            self._state = "running"
        try:
            thread.start()
        except BaseException as exc:
            self._stop_event.set()
            try:
                self._controller.close()
            except BaseException as close_error:
                self._logger.error("启动预览线程失败后关闭相机时出错：%s", close_error)
            with self._lock:
                self._state = "failed"
                self._error = exc
                self._thread = None
            raise

    def join(self, timeout: float | None = None) -> None:
        """Wait for a worker that has already stopped or failed."""

        thread = self.thread
        if thread is not None:
            thread.join(timeout)

    def stop(self, timeout: float = 2.0) -> None:
        """Request shutdown and ensure the worker has released the camera."""

        if timeout < 0:
            raise ValueError("timeout 不能为负数")
        thread = self.thread
        self._stop_event.set()
        if thread is None:
            try:
                self._controller.close()
            finally:
                with self._lock:
                    if self._state == "new":
                        self._state = "stopped"
            return

        thread.join(timeout)
        if thread.is_alive():
            # A blocking driver read should honor its timeout.  Closing here
            # is the final unblock path for a driver that does not.
            try:
                self._controller.close()
            except BaseException as exc:
                self._record_error(exc, report=not self._stop_event.is_set())
            thread.join(timeout)
        if thread.is_alive():
            raise RuntimeError("预览 pump 线程未能在关闭期限内退出")
        with self._lock:
            if self._state == "running":
                self._state = "stopped"

    close = stop

    def __enter__(self) -> CameraPreviewPump:
        self.start()
        return self

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        self.stop()

    def _run(self) -> None:
        try:
            while not self._stop_event.is_set():
                observation = self._controller.read(timeout=self._read_timeout)
                if self._stop_event.is_set():
                    break
                frame = observation.frame
                started = self._metrics.start() if self._metrics is not None else 0
                self._device.publish_preview_pair(
                    frame.left,
                    frame.right,
                    source_sequence=frame.source_sequence,
                    capture_monotonic_ns=frame.host_monotonic_ns,
                )
                if self._metrics is not None:
                    self._metrics.finish("preview_publish", started)
                    lease = self._metrics.retain_payload(
                        "preview_reference", len(frame.left) + len(frame.right)
                    )
                    previous, self._preview_lease = self._preview_lease, lease
                    if previous is not None:
                        previous.release()
                with self._lock:
                    self._frames_published += 1
        except BaseException as exc:
            if not self._stop_event.is_set():
                self._record_error(exc)
        finally:
            self._release_controller()
            lease, self._preview_lease = self._preview_lease, None
            if lease is not None:
                lease.release()
            with self._lock:
                if self._state == "running":
                    self._state = "stopped" if self._error is None else "failed"

    def _record_error(self, error: BaseException, *, report: bool = True) -> None:
        with self._lock:
            if self._error is not None:
                return
            self._error = error
            self._state = "failed"
        if not report:
            return
        self._logger.error("硬件预览相机采集失败：%s", error)
        try:
            self._device.set_fault("hardware_unavailable", f"相机预览采集失败：{error}")
        except BaseException as fault_error:
            self._logger.error("记录相机故障状态时出错：%s", fault_error)
        if self._on_error is not None:
            try:
                self._on_error(error)
            except BaseException as callback_error:
                self._logger.error("相机故障回调失败：%s", callback_error)

    def _release_controller(self) -> None:
        try:
            if getattr(self._controller, "state", None) == "streaming":
                self._controller.stop()
        except BaseException as exc:
            self._record_error(exc, report=not self._stop_event.is_set())
        try:
            self._controller.close()
        except BaseException as exc:
            self._record_error(exc, report=not self._stop_event.is_set())


__all__ = ["CameraPreviewPump", "PreviewController"]

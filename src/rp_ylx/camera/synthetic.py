"""用于无硬件测试的双目相机 backend。"""

from __future__ import annotations

from collections.abc import Iterable

from rp_ylx.camera.models import CameraDescriptor, CameraError, CameraMode, StereoFrame


class SyntheticCameraStream:
    def __init__(self, items: Iterable[StereoFrame | Exception]) -> None:
        self._items = iter(items)
        self.started = False
        self.stopped = False
        self.closed = False

    def start(self) -> None:
        if self.closed:
            raise CameraError("invalid_state", "模拟相机已关闭")
        self.started = True

    def read(self, timeout: float) -> StereoFrame:
        if not self.started or self.closed:
            raise CameraError("invalid_state", "模拟相机尚未采集")
        try:
            item = next(self._items)
        except StopIteration as exc:
            raise CameraError("disconnected", "模拟相机数据结束", retryable=True) from exc
        if isinstance(item, Exception):
            raise item
        return item

    def stop(self) -> None:
        self.stopped = True
        self.started = False

    def close(self) -> None:
        self.closed = True
        self.started = False


class SyntheticCameraBackend:
    def __init__(
        self,
        descriptors: Iterable[CameraDescriptor],
        *,
        frames: dict[str, Iterable[StereoFrame | Exception]] | None = None,
        open_errors: dict[str, Exception] | None = None,
    ) -> None:
        self._descriptors = tuple(descriptors)
        self._frames = frames or {}
        self._open_errors = open_errors or {}
        self.opened_streams: list[SyntheticCameraStream] = []

    def discover(self) -> tuple[CameraDescriptor, ...]:
        return self._descriptors

    def open(self, descriptor: CameraDescriptor, mode: CameraMode) -> SyntheticCameraStream:
        if error := self._open_errors.get(descriptor.stable_id):
            raise error
        stream = SyntheticCameraStream(self._frames.get(descriptor.stable_id, ()))
        self.opened_streams.append(stream)
        return stream

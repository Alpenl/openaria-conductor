"""不会堆积工作或反压采集的 latest-only JPEG 预览传输。"""

from __future__ import annotations

import threading
import time
from collections.abc import Iterator
from dataclasses import dataclass

from rp_ylx.native import NativeModuleError, NativePreviewBuffer, create_native_preview_buffer

MULTIPART_BOUNDARY = "ylx-preview"


class PreviewFrameUnavailable(RuntimeError):
    """尚未发布预览帧时，在发送 HTTP 响应头前抛出。"""


@dataclass(frozen=True, slots=True)
class PreviewResponse:
    content_type: str
    body: bytes | Iterator[bytes]
    content_length: int | None


@dataclass(frozen=True, slots=True)
class _Frame:
    sequence: int
    jpeg: bytes


class LatestPreviewBuffer:
    """单槽预览缓冲区；发布新帧会替换旧帧，不会排队。"""

    def __init__(self, *, stream_fps: int) -> None:
        if stream_fps < 1:
            raise ValueError("stream_fps must be at least 1")
        self._stream_fps = stream_fps
        try:
            self._native: NativePreviewBuffer | None = create_native_preview_buffer(stream_fps)
        except NativeModuleError:
            self._native = None
        self._condition = threading.Condition()
        self._latest: _Frame | None = None
        self._sequence = 0

    def publish(self, jpeg: bytes) -> int:
        if not isinstance(jpeg, bytes) or not jpeg:
            raise ValueError("preview JPEG must be non-empty bytes")
        if self._native is not None:
            return self._native.publish(jpeg)
        with self._condition:
            self._sequence += 1
            self._latest = _Frame(self._sequence, jpeg)
            self._condition.notify_all()
            return self._sequence

    def clear(self) -> None:
        if self._native is not None:
            self._native.clear()
            return
        with self._condition:
            self._latest = None
            self._condition.notify_all()

    @property
    def native_owner(self) -> NativePreviewBuffer | None:
        return self._native

    def latest_preview(self, *, fps: int | None, accept: str) -> PreviewResponse:
        if accept == "multipart/x-mixed-replace":
            return self.multipart_response(fps)
        return self.jpeg_response()

    def jpeg_response(self) -> PreviewResponse:
        frame = self._snapshot()
        return PreviewResponse("image/jpeg", frame.jpeg, len(frame.jpeg))

    def multipart_response(self, requested_fps: int | None) -> PreviewResponse:
        if requested_fps is not None and requested_fps < 1:
            raise ValueError("fps must be at least 1")
        self._snapshot()
        fps = self._stream_fps if requested_fps is None else min(requested_fps, self._stream_fps)
        body: Iterator[bytes]
        if self._native is None:
            body = MultipartPreview(self, fps=fps)
        else:
            body = self._native.multipart_stream(fps)
        return PreviewResponse(
            f"multipart/x-mixed-replace; boundary={MULTIPART_BOUNDARY}",
            body,
            None,
        )

    def _snapshot(self) -> _Frame:
        if self._native is not None:
            try:
                sequence, jpeg = self._native.jpeg()
            except RuntimeError as exc:
                _raise_native_preview_error(exc)
            return _Frame(sequence, jpeg)
        with self._condition:
            if self._latest is None:
                raise PreviewFrameUnavailable("no preview frame is currently available")
            return self._latest

    def _wait_after(self, sequence: int, stop: threading.Event, timeout: float) -> _Frame | None:
        with self._condition:
            self._condition.wait_for(
                lambda: (
                    stop.is_set()
                    or (self._latest is not None and self._latest.sequence != sequence)
                ),
                timeout=timeout,
            )
            if stop.is_set():
                return None
            return self._latest

    def _wake_streams(self) -> None:
        if self._native is not None:
            self._native.wake_streams()
            return
        with self._condition:
            self._condition.notify_all()


class MultipartPreview(Iterator[bytes]):
    """可关闭的限速流；每次传输只读取当时的最新帧。"""

    def __init__(self, buffer: LatestPreviewBuffer, *, fps: int) -> None:
        self._buffer = buffer
        self._period = 1.0 / fps
        self._stop = threading.Event()
        self._last_sequence = 0
        self._next_delivery = 0.0

    def __iter__(self) -> MultipartPreview:
        return self

    def __next__(self) -> bytes:
        while not self._stop.is_set():
            now = time.monotonic()
            if now < self._next_delivery:
                self._stop.wait(self._next_delivery - now)
                continue
            frame = self._buffer._wait_after(
                self._last_sequence,
                self._stop,
                timeout=0.25,
            )
            if frame is None or frame.sequence == self._last_sequence:
                continue
            self._last_sequence = frame.sequence
            self._next_delivery = time.monotonic() + self._period
            return multipart_part(frame.jpeg)
        raise StopIteration

    def close(self) -> None:
        self._stop.set()
        self._buffer._wake_streams()


def multipart_part(jpeg: bytes) -> bytes:
    header = (
        f"--{MULTIPART_BOUNDARY}\r\nContent-Type: image/jpeg\r\nContent-Length: {len(jpeg)}\r\n\r\n"
    ).encode("ascii")
    return header + jpeg + b"\r\n"


def _raise_native_preview_error(error: RuntimeError) -> None:
    raw = str(error)
    code, separator, message = raw.partition(": ")
    if separator and code == "preview_unavailable":
        raise PreviewFrameUnavailable(message) from error
    raise error

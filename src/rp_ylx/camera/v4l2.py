"""Linux V4L2 discovery and a small mmap MJPEG capture stream."""

from __future__ import annotations

import errno
import hashlib
import io
import mmap
import os
import re
import select
import shutil
import struct
import subprocess
import time
from collections.abc import Callable
from contextlib import suppress
from pathlib import Path

from rp_ylx.camera.models import (
    CameraDescriptor,
    CameraError,
    CameraMode,
    CameraStream,
    StereoFrame,
)
from rp_ylx.camera.turbojpeg import lossless_crop_sbs_jpeg
from rp_ylx.native import NativeCamera, NativeModuleError, create_native_camera
from rp_ylx.performance.metrics import PerformanceMetrics

_FORMAT = re.compile(r"\[\d+\]:\s+'([^']+)'")
_SIZE = re.compile(r"Size:\s+Discrete\s+(\d+)x(\d+)")
_FPS = re.compile(r"\((\d+(?:\.\d+)?)\s+fps\)")

V4L2_BUF_TYPE_VIDEO_CAPTURE = 1
V4L2_MEMORY_MMAP = 1
V4L2_CAP_VIDEO_CAPTURE = 0x00000001
V4L2_CAP_STREAMING = 0x04000000
V4L2_BUF_FLAG_ERROR = 0x00000040
V4L2_BUF_FLAG_TIMESTAMP_MONOTONIC = 0x00002000
_IOC_READ = 2
_IOC_WRITE = 1


def _ioc(direction: int, number: int, size: int) -> int:
    return (direction << 30) | (size << 16) | (ord("V") << 8) | number


# These are the videodev2 ABI sizes for the target 64-bit Linux userspace (validated on RDK X5).
VIDIOC_QUERYCAP = _ioc(_IOC_READ, 0, 104)
VIDIOC_S_FMT = _ioc(_IOC_READ | _IOC_WRITE, 5, 208)
VIDIOC_S_PARM = _ioc(_IOC_READ | _IOC_WRITE, 22, 204)
VIDIOC_REQBUFS = _ioc(_IOC_READ | _IOC_WRITE, 8, 20)
VIDIOC_QUERYBUF = _ioc(_IOC_READ | _IOC_WRITE, 9, 88)
VIDIOC_QBUF = _ioc(_IOC_READ | _IOC_WRITE, 15, 88)
VIDIOC_DQBUF = _ioc(_IOC_READ | _IOC_WRITE, 17, 88)
VIDIOC_STREAMON = _ioc(_IOC_WRITE, 18, 4)
VIDIOC_STREAMOFF = _ioc(_IOC_WRITE, 19, 4)

_PIX_FORMAT_OFFSET = 8
_BUFFER_INDEX_OFFSET = 0
_BUFFER_BYTESUSED_OFFSET = 8
_BUFFER_FLAGS_OFFSET = 12
_BUFFER_TIMESTAMP_OFFSET = 24
_BUFFER_SEQUENCE_OFFSET = 56
_BUFFER_MEMORY_OFFSET = 60
_BUFFER_OFFSET_OFFSET = 64
_BUFFER_LENGTH_OFFSET = 72
_JPEG_SOI = b"\xff\xd8"
_JPEG_EOI = b"\xff\xd9"
_FOURCC_MJPEG = int.from_bytes(b"MJPG", "little")
_FOURCC_JPEG = int.from_bytes(b"JPEG", "little")

Ioctl = Callable[[int, int, bytearray], None]
OpenFile = Callable[[str, int], int]
CloseFile = Callable[[int], None]
MmapFactory = Callable[[int, int, int, int, int], mmap.mmap]
WaitReadable = Callable[[int, float], bool]
ClockNs = Callable[[], int]


def _linux_ioctl(fd: int, request: int, payload: bytearray) -> None:
    import fcntl

    fcntl.ioctl(fd, request, payload, True)


def _wait_readable(fd: int, timeout: float) -> bool:
    ready, _, _ = select.select([fd], [], [], timeout)
    return bool(ready)


def _mmap_buffer(fd: int, length: int, prot: int, flags: int, offset: int) -> mmap.mmap:
    return mmap.mmap(fd, length, flags=flags, prot=prot, offset=offset)


def _fourcc(encoding: str) -> int:
    normalized = encoding.lower().replace("-", "")
    if normalized in {"mjpg", "mjpeg", "motionjpeg"}:
        return _FOURCC_MJPEG
    if normalized == "jpeg":
        return _FOURCC_JPEG
    raise CameraError("unsupported_mode", f"V4L2 编码不受支持：{encoding}")


def _fourcc_name(value: int) -> str:
    return value.to_bytes(4, "little").decode("ascii", errors="replace").lower()


def _jpeg_markers(payload: bytes) -> list[tuple[int, int]]:
    """Return complete JPEG byte ranges, tolerating MJPEG padding."""

    ranges: list[tuple[int, int]] = []
    cursor = 0
    while True:
        start = payload.find(_JPEG_SOI, cursor)
        if start < 0:
            break
        end = payload.find(_JPEG_EOI, start + len(_JPEG_SOI))
        if end < 0:
            break
        end += len(_JPEG_EOI)
        ranges.append((start, end))
        cursor = end
    return ranges


def _jpeg_dimensions(payload: bytes) -> tuple[int, int] | None:
    """Read JPEG SOF dimensions without decoding the image."""

    if not payload.startswith(_JPEG_SOI):
        return None
    cursor = 2
    while cursor + 4 <= len(payload):
        if payload[cursor] != 0xFF:
            cursor += 1
            continue
        while cursor < len(payload) and payload[cursor] == 0xFF:
            cursor += 1
        if cursor >= len(payload):
            return None
        marker = payload[cursor]
        cursor += 1
        if marker in {0xD8, 0xD9}:
            continue
        if marker == 0xDA:
            return None
        if cursor + 2 > len(payload):
            return None
        length = int.from_bytes(payload[cursor : cursor + 2], "big")
        if length < 2 or cursor + length > len(payload):
            return None
        if marker in {
            0xC0,
            0xC1,
            0xC2,
            0xC3,
            0xC5,
            0xC6,
            0xC7,
            0xC9,
            0xCA,
            0xCB,
            0xCD,
            0xCE,
            0xCF,
        }:
            if length < 7:
                return None
            height = int.from_bytes(payload[cursor + 3 : cursor + 5], "big")
            width = int.from_bytes(payload[cursor + 5 : cursor + 7], "big")
            return width, height
        cursor += length
    return None


def _reencode_sbs_jpeg(payload: bytes, width: int, height: int) -> tuple[bytes, bytes]:
    """Crop one SBS JPEG when the driver does not provide two JPEG payloads.

    Pillow is the runtime fallback for per-eye JPEGs from a single side-by-side
    JPEG. Concatenated eye JPEGs never take this path and remain byte-for-byte
    unchanged.
    """

    try:
        from PIL import Image
    except ImportError as exc:
        raise CameraError(
            "unsupported_format",
            "单个并排 JPEG 需要 Pillow 才能生成左右眼 JPEG；请安装 pillow",
        ) from exc
    if width <= 0 or height <= 0 or width % 2:
        raise CameraError("bad_frame", "并排 JPEG 的宽度必须是正偶数")
    try:
        with Image.open(io.BytesIO(payload)) as image:
            if image.size != (width, height):
                raise CameraError(
                    "bad_frame",
                    f"JPEG 尺寸为 {image.width}x{image.height}，期望 {width}x{height}",
                )
            eye_width = width // 2
            output: list[bytes] = []
            for box in ((0, 0, eye_width, height), (eye_width, 0, width, height)):
                eye = image.crop(box)
                encoded = io.BytesIO()
                try:
                    eye.save(encoded, format="JPEG", quality="keep", subsampling="keep")
                except (KeyError, ValueError):
                    encoded.seek(0)
                    encoded.truncate(0)
                    eye.save(encoded, format="JPEG", quality=95, subsampling=0)
                output.append(encoded.getvalue())
            return output[0], output[1]
    except CameraError:
        raise
    except (OSError, ValueError) as exc:
        raise CameraError("bad_frame", f"无法解码并排 JPEG：{exc}") from exc


def split_sbs_mjpeg(payload: bytes, width: int, height: int) -> tuple[bytes, bytes]:
    """Split a side-by-side MJPEG payload into left/right JPEG byte strings.

    Some UVC drivers expose two complete JPEGs in one buffer; those are returned
    without re-encoding. The YLX stream normally exposes one 3840x1080 JPEG,
    which is cropped with Pillow as a compatibility fallback.
    """

    if not payload:
        raise CameraError("bad_frame", "V4L2 返回空图像缓冲区")
    ranges = _jpeg_markers(payload)
    if len(ranges) >= 2:
        if len(ranges) != 2:
            raise CameraError("bad_frame", "V4L2 图像缓冲区包含超过两张 JPEG")
        left_start, left_end = ranges[0]
        right_start, right_end = ranges[1]
        return payload[left_start:left_end], payload[right_start:right_end]
    if len(ranges) != 1:
        raise CameraError("bad_frame", "V4L2 图像缓冲区不是完整 JPEG")
    start, end = ranges[0]
    trailing = payload[end:]
    if start != 0 or trailing.strip(b"\x00"):
        payload = payload[start:end]
    dimensions = _jpeg_dimensions(payload)
    if dimensions is not None and dimensions != (width, height):
        raise CameraError(
            "bad_frame",
            f"JPEG 尺寸为 {dimensions[0]}x{dimensions[1]}，期望 {width}x{height}",
        )
    lossless = (
        lossless_crop_sbs_jpeg(payload, width, height)
        if width > 0 and height > 0 and width % 2 == 0
        else None
    )
    eye_dimensions = (width // 2, height)
    if lossless is not None and all(_jpeg_dimensions(eye) == eye_dimensions for eye in lossless):
        return lossless
    return _reencode_sbs_jpeg(payload, width, height)


def split_sbs_mjpeg_native(payload: bytes, width: int, height: int) -> tuple[bytes, bytes]:
    """生产 60 FPS 拆分，只允许原始双 JPEG 或 TurboJPEG 无损裁剪。"""

    if not payload:
        raise CameraError("bad_frame", "V4L2 返回空图像缓冲区")
    ranges = _jpeg_markers(payload)
    if len(ranges) == 2:
        left_start, left_end = ranges[0]
        right_start, right_end = ranges[1]
        return payload[left_start:left_end], payload[right_start:right_end]
    if len(ranges) != 1:
        raise CameraError("bad_frame", "V4L2 图像缓冲区不是单张或双张完整 JPEG")
    start, end = ranges[0]
    trailing = payload[end:]
    selected = payload[start:end] if start != 0 or trailing.strip(b"\x00") else payload
    dimensions = _jpeg_dimensions(selected)
    if dimensions is not None and dimensions != (width, height):
        raise CameraError(
            "bad_frame",
            f"JPEG 尺寸为 {dimensions[0]}x{dimensions[1]}，期望 {width}x{height}",
        )
    lossless = (
        lossless_crop_sbs_jpeg(selected, width, height)
        if width > 0 and height > 0 and width % 2 == 0
        else None
    )
    eye_dimensions = (width // 2, height)
    if lossless is not None and all(_jpeg_dimensions(eye) == eye_dimensions for eye in lossless):
        return lossless
    raise CameraError(
        "native_split_unavailable",
        "正式 60 FPS 路径需要可用的 TurboJPEG 无损裁剪，不允许 Pillow 回退",
    )


def parse_v4l2_formats(output: str) -> tuple[CameraMode, ...]:
    encoding: str | None = None
    size: tuple[int, int] | None = None
    modes: set[CameraMode] = set()
    for line in output.splitlines():
        if match := _FORMAT.search(line):
            encoding = match.group(1).lower()
            size = None
        elif match := _SIZE.search(line):
            size = (int(match.group(1)), int(match.group(2)))
        elif match := _FPS.search(line):
            if encoding is not None and size is not None:
                modes.add(CameraMode(size[0], size[1], float(match.group(1)), encoding))
    return tuple(sorted(modes))


def _text(path: Path) -> str | None:
    try:
        return path.read_text(encoding="utf-8", errors="replace").strip("\x00\n ") or None
    except OSError:
        return None


def _usb_identity(entry: Path) -> str:
    try:
        resolved = entry.resolve()
    except OSError:
        resolved = entry
    for parent in (resolved, *resolved.parents):
        vendor = _text(parent / "idVendor")
        product = _text(parent / "idProduct")
        if vendor and product:
            serial = _text(parent / "serial") or parent.name
            endpoint_name = _text(entry / "name") or "unknown"
            endpoint_index = _text(entry / "index") or "unknown"
            source = f"{vendor.lower()}:{product.lower()}:{serial}:{endpoint_name}:{endpoint_index}"
            return "v4l2:" + hashlib.sha256(source.encode()).hexdigest()[:24]
    return "v4l2:" + hashlib.sha256(str(resolved).encode()).hexdigest()[:24]


class V4L2CameraStream:
    """Capture one single-plane V4L2 MJPEG stream using memory-mapped buffers."""

    def __init__(
        self,
        device: str | Path,
        mode: CameraMode,
        *,
        buffer_count: int = 4,
        clock_ns: ClockNs = time.monotonic_ns,
        open_file: OpenFile = os.open,
        close_file: CloseFile = os.close,
        ioctl: Ioctl = _linux_ioctl,
        mmap_factory: MmapFactory = _mmap_buffer,
        wait_readable: WaitReadable = _wait_readable,
        split_frame: Callable[[bytes, int, int], tuple[bytes, bytes]] | None = split_sbs_mjpeg,
        metrics: PerformanceMetrics | None = None,
        close_splitter: Callable[[], None] | None = None,
    ) -> None:
        if buffer_count < 2:
            raise ValueError("buffer_count must be at least two")
        self._device = Path(device)
        self._mode = mode
        self._buffer_count = buffer_count
        self._clock_ns = clock_ns
        self._close_file = close_file
        self._ioctl = ioctl
        self._mmap_factory = mmap_factory
        self._wait_readable = wait_readable
        self._split_frame = split_frame
        self._metrics = metrics
        self._close_splitter = close_splitter
        self._fd: int | None = None
        self._buffers: list[mmap.mmap] = []
        self._streaming = False
        self._closed = False
        self._last_sequence: int | None = None
        self._sequence_epoch = 0
        self._last_timestamp_ns: int | None = None
        flags = os.O_RDWR | os.O_NONBLOCK | getattr(os, "O_CLOEXEC", 0)
        self._fd = open_file(os.fspath(self._device), flags)
        try:
            self._configure()
        except BaseException:
            self.close()
            raise

    @property
    def closed(self) -> bool:
        return self._closed

    @property
    def streaming(self) -> bool:
        return self._streaming

    def _require_fd(self) -> int:
        if self._fd is None or self._closed:
            raise CameraError("invalid_state", "V4L2 stream is closed")
        return self._fd

    def _configure(self) -> None:
        fd = self._require_fd()
        capabilities = bytearray(104)
        self._ioctl(fd, VIDIOC_QUERYCAP, capabilities)
        capability = struct.unpack_from("<I", capabilities, 84)[0]
        device_caps = struct.unpack_from("<I", capabilities, 88)[0]
        effective_caps = device_caps if capability & 0x80000000 else capability
        if not effective_caps & V4L2_CAP_VIDEO_CAPTURE:
            raise CameraError("unsupported_device", "V4L2 节点不支持单平面视频采集")
        if not effective_caps & V4L2_CAP_STREAMING:
            raise CameraError("unsupported_device", "V4L2 节点不支持 mmap 流式采集")

        pixel_format = _fourcc(self._mode.encoding)
        fmt = bytearray(208)
        struct.pack_into("<I", fmt, 0, V4L2_BUF_TYPE_VIDEO_CAPTURE)
        struct.pack_into(
            "<IIIIIIIIIIII",
            fmt,
            _PIX_FORMAT_OFFSET,
            self._mode.width,
            self._mode.height,
            pixel_format,
            1,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
        )
        self._ioctl(fd, VIDIOC_S_FMT, fmt)
        width, height, actual_format = struct.unpack_from("<III", fmt, _PIX_FORMAT_OFFSET)
        if (width, height, actual_format) != (self._mode.width, self._mode.height, pixel_format):
            raise CameraError(
                "unsupported_mode",
                "V4L2 驱动未接受精确模式 "
                f"{self._mode.width}x{self._mode.height}@{self._mode.fps:g} "
                f"{_fourcc_name(pixel_format)}，返回 "
                f"{width}x{height} {_fourcc_name(actual_format)}",
            )

        parm = bytearray(204)
        struct.pack_into("<I", parm, 0, V4L2_BUF_TYPE_VIDEO_CAPTURE)
        struct.pack_into("<II", parm, 12, 1, max(1, round(self._mode.fps)))
        try:
            self._ioctl(fd, VIDIOC_S_PARM, parm)
        except OSError as exc:
            if exc.errno not in {errno.EINVAL, errno.ENOTTY}:
                raise
        else:
            numerator, denominator = struct.unpack_from("<II", parm, 12)
            if numerator <= 0 or denominator <= 0:
                raise CameraError("unsupported_mode", "V4L2 驱动返回无效帧率")
            actual_fps = denominator / numerator
            if abs(actual_fps - self._mode.fps) > max(0.5, self._mode.fps * 0.02):
                raise CameraError(
                    "unsupported_mode",
                    f"V4L2 驱动返回帧率 {actual_fps:g}，期望 {self._mode.fps:g}",
                )

        request = bytearray(20)
        struct.pack_into(
            "<III", request, 0, self._buffer_count, V4L2_BUF_TYPE_VIDEO_CAPTURE, V4L2_MEMORY_MMAP
        )
        self._ioctl(fd, VIDIOC_REQBUFS, request)
        actual_count = struct.unpack_from("<I", request, 0)[0]
        if actual_count < 2:
            raise CameraError("buffer_setup_failed", "V4L2 驱动未提供至少两个 mmap 缓冲区")
        for index in range(actual_count):
            query = bytearray(88)
            struct.pack_into("<IIII", query, 0, index, V4L2_BUF_TYPE_VIDEO_CAPTURE, 0, 0)
            struct.pack_into("<I", query, _BUFFER_MEMORY_OFFSET, V4L2_MEMORY_MMAP)
            self._ioctl(fd, VIDIOC_QUERYBUF, query)
            offset = struct.unpack_from("<I", query, _BUFFER_OFFSET_OFFSET)[0]
            length = struct.unpack_from("<I", query, _BUFFER_LENGTH_OFFSET)[0]
            if length <= 0:
                raise CameraError("buffer_setup_failed", "V4L2 驱动返回空 mmap 缓冲区")
            self._buffers.append(
                self._mmap_factory(
                    fd, length, mmap.PROT_READ | mmap.PROT_WRITE, mmap.MAP_SHARED, offset
                )
            )

    def start(self) -> None:
        fd = self._require_fd()
        if self._streaming:
            raise CameraError("invalid_state", "V4L2 stream 已经开始")
        if self._last_sequence is not None:
            self._sequence_epoch += self._last_sequence + 1
            self._last_sequence = None
        try:
            for index in range(len(self._buffers)):
                queue = bytearray(88)
                struct.pack_into("<IIII", queue, 0, index, V4L2_BUF_TYPE_VIDEO_CAPTURE, 0, 0)
                struct.pack_into("<I", queue, _BUFFER_MEMORY_OFFSET, V4L2_MEMORY_MMAP)
                self._ioctl(fd, VIDIOC_QBUF, queue)
            stream_type = bytearray(struct.pack("<I", V4L2_BUF_TYPE_VIDEO_CAPTURE))
            self._ioctl(fd, VIDIOC_STREAMON, stream_type)
            self._streaming = True
        except BaseException:
            self.close()
            raise

    def _timestamp_from_buffer(self, payload: bytes, flags: int) -> int:
        driver_monotonic = bool(flags & V4L2_BUF_FLAG_TIMESTAMP_MONOTONIC)
        if driver_monotonic:
            seconds, micros = struct.unpack_from("<qq", payload, _BUFFER_TIMESTAMP_OFFSET)
            timestamp = seconds * 1_000_000_000 + micros * 1_000
        else:
            timestamp = 0
        if timestamp <= 0:
            timestamp = self._clock_ns()
        if self._last_timestamp_ns is not None and timestamp <= self._last_timestamp_ns:
            if driver_monotonic:
                raise CameraError("timestamp_regression", "V4L2 驱动单调时间戳重复或回退")
            timestamp = max(self._clock_ns(), self._last_timestamp_ns + 1)
        self._last_timestamp_ns = timestamp
        return timestamp

    def _source_sequence(self, raw_sequence: int) -> int:
        if self._last_sequence is not None and raw_sequence < self._last_sequence:
            if self._last_sequence - raw_sequence > 0x80000000:
                self._sequence_epoch += 1 << 32
            else:
                raise CameraError("sequence_regression", "V4L2 源帧序号回退")
        sequence = self._sequence_epoch + raw_sequence
        self._last_sequence = raw_sequence
        return sequence

    def read(self, timeout: float) -> StereoFrame:
        fd = self._require_fd()
        if timeout <= 0:
            raise ValueError("timeout must be greater than zero")
        if not self._streaming:
            raise CameraError("invalid_state", "V4L2 stream 尚未开始")
        deadline = time.monotonic() + timeout
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                with suppress(BaseException):
                    self.close()
                raise TimeoutError("V4L2 frame did not arrive before timeout")
            try:
                started = self._metrics.start() if self._metrics is not None else 0
                ready = self._wait_readable(fd, remaining)
                if self._metrics is not None:
                    self._metrics.finish("v4l2_wait", started)
            except BaseException:
                with suppress(BaseException):
                    self.close()
                raise
            if not ready:
                with suppress(BaseException):
                    self.close()
                raise TimeoutError("V4L2 frame did not arrive before timeout")
            dequeue = bytearray(88)
            struct.pack_into("<IIII", dequeue, 0, 0, V4L2_BUF_TYPE_VIDEO_CAPTURE, 0, 0)
            struct.pack_into("<I", dequeue, _BUFFER_MEMORY_OFFSET, V4L2_MEMORY_MMAP)
            try:
                started = self._metrics.start() if self._metrics is not None else 0
                self._ioctl(fd, VIDIOC_DQBUF, dequeue)
                if self._metrics is not None:
                    self._metrics.finish("v4l2_dequeue", started)
            except OSError as exc:
                if exc.errno in {errno.EAGAIN, errno.EWOULDBLOCK}:
                    continue
                self.close()
                raise
            index = struct.unpack_from("<I", dequeue, _BUFFER_INDEX_OFFSET)[0]
            bytes_used = struct.unpack_from("<I", dequeue, _BUFFER_BYTESUSED_OFFSET)[0]
            flags = struct.unpack_from("<I", dequeue, _BUFFER_FLAGS_OFFSET)[0]
            raw_sequence = struct.unpack_from("<I", dequeue, _BUFFER_SEQUENCE_OFFSET)[0]
            if index >= len(self._buffers):
                self.close()
                raise CameraError("bad_frame", "V4L2 返回了无效缓冲区索引")
            frame: StereoFrame | None = None
            frame_error: BaseException | None = None
            try:
                if flags & V4L2_BUF_FLAG_ERROR:
                    raise CameraError("bad_frame", "V4L2 驱动标记采集缓冲区损坏")
                if bytes_used <= 0 or bytes_used > len(self._buffers[index]):
                    raise CameraError("bad_frame", "V4L2 返回了无效图像长度")
                source_sequence = self._source_sequence(raw_sequence)
                host_timestamp = self._timestamp_from_buffer(dequeue, flags)
                started = self._metrics.start() if self._metrics is not None else 0
                raw_side_by_side = bytes(self._buffers[index][:bytes_used])
                if self._metrics is not None:
                    self._metrics.finish("sbs_copy", started)
                    self._metrics.record_copy("sbs_input", bytes_used)
                if self._split_frame is None:
                    left, right = b"", b""
                else:
                    started = self._metrics.start() if self._metrics is not None else 0
                    left, right = self._split_frame(
                        raw_side_by_side, self._mode.width, self._mode.height
                    )
                    if self._metrics is not None:
                        self._metrics.finish("jpeg_split", started)
                        self._metrics.record_copy("left_output", len(left))
                        self._metrics.record_copy("right_output", len(right))
                lease = (
                    self._metrics.retain_payload(
                        "camera_frame", len(raw_side_by_side) + len(left) + len(right)
                    )
                    if self._metrics is not None
                    else None
                )
                frame = StereoFrame(
                    source_sequence,
                    host_timestamp,
                    left,
                    right,
                    True,
                    raw_side_by_side,
                    lease,
                )
            except BaseException as exc:
                frame_error = exc
            try:
                queue = bytearray(88)
                struct.pack_into("<IIII", queue, 0, index, V4L2_BUF_TYPE_VIDEO_CAPTURE, 0, 0)
                struct.pack_into("<I", queue, _BUFFER_MEMORY_OFFSET, V4L2_MEMORY_MMAP)
                started = self._metrics.start() if self._metrics is not None else 0
                self._ioctl(fd, VIDIOC_QBUF, queue)
                if self._metrics is not None:
                    self._metrics.finish("v4l2_requeue", started)
            except BaseException:
                self.close()
                raise
            if frame_error is not None:
                with suppress(BaseException):
                    self.close()
                raise frame_error
            assert frame is not None
            return frame

    def stop(self) -> None:
        if self._fd is None or self._closed or not self._streaming:
            return
        try:
            stream_type = bytearray(struct.pack("<I", V4L2_BUF_TYPE_VIDEO_CAPTURE))
            self._ioctl(self._fd, VIDIOC_STREAMOFF, stream_type)
        except BaseException:
            self.close()
            raise
        finally:
            self._streaming = False

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        first_error: BaseException | None = None
        if self._fd is not None and self._streaming:
            try:
                stream_type = bytearray(struct.pack("<I", V4L2_BUF_TYPE_VIDEO_CAPTURE))
                self._ioctl(self._fd, VIDIOC_STREAMOFF, stream_type)
            except BaseException as exc:
                first_error = exc
            self._streaming = False
        for buffer in self._buffers:
            try:
                buffer.close()
            except BaseException as exc:
                first_error = first_error or exc
        self._buffers.clear()
        if self._fd is not None:
            try:
                self._close_file(self._fd)
            except BaseException as exc:
                first_error = first_error or exc
            self._fd = None
        if self._close_splitter is not None:
            try:
                self._close_splitter()
            except BaseException as exc:
                first_error = first_error or exc
            self._close_splitter = None
        if first_error is not None:
            raise first_error

    def __enter__(self) -> V4L2CameraStream:
        return self

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        self.close()


def v4l2_stream_factory(device: Path, mode: CameraMode) -> CameraStream:
    """Default stream factory used by :class:`V4L2DiscoveryBackend`."""

    return V4L2CameraStream(device, mode)


class NativeV4L2CameraStream:
    """Thin CameraStream adapter over the deep Rust capture module."""

    def __init__(
        self,
        owner: NativeCamera,
        *,
        metrics: PerformanceMetrics | None = None,
    ) -> None:
        self._owner = owner
        self._closed = False
        self._metrics = metrics
        self._queue_rejected = 0
        self._accounted_application_drops = 0

    def _observe_native_stats(self) -> None:
        if self._metrics is None:
            return
        stats = self._owner.stats()
        expected = {
            "capacity",
            "depth",
            "peak_depth",
            "enqueued",
            "delivered",
            "rejected",
        }
        if (
            not isinstance(stats, dict)
            or set(stats) != expected
            or any(
                isinstance(value, bool) or not isinstance(value, int) for value in stats.values()
            )
            or any(value < 0 for value in stats.values())
            or stats["capacity"] <= 0
            or stats["depth"] > stats["capacity"]
            or stats["peak_depth"] > stats["capacity"]
            or stats["rejected"] < self._queue_rejected
        ):
            raise CameraError("invalid_native_stats", "原生相机返回了无效有界队列统计")
        rejected = stats["rejected"] - self._queue_rejected
        self._queue_rejected = stats["rejected"]
        self._metrics.observe_queue(
            depth=stats["depth"],
            capacity=stats["capacity"],
            rejected=rejected,
            peak_depth=stats["peak_depth"],
        )

    def _record_tail_rejections(self) -> None:
        if self._metrics is None:
            return
        tail = self._queue_rejected - self._accounted_application_drops
        if tail < 0:
            raise CameraError("invalid_native_stats", "应用丢帧计数超过原生队列拒绝总数")
        if tail:
            self._metrics.record_loss("queue_rejected", tail)
            self._accounted_application_drops += tail

    @staticmethod
    def _translate(exc: Exception) -> BaseException:
        raw = str(exc)
        code, separator, message = raw.partition(": ")
        if not separator or not code.replace("_", "").isalnum():
            return CameraError("native_camera_failed", raw)
        if code == "frame_timeout":
            return TimeoutError(message)
        return CameraError(code, message)

    def start(self) -> None:
        if self._closed:
            raise CameraError("invalid_state", "原生相机已关闭")
        try:
            self._owner.start()
        except Exception as exc:
            with suppress(Exception):
                self._owner.close()
            self._closed = True
            raise self._translate(exc) from exc

    def read(self, timeout: float) -> StereoFrame:
        if self._closed:
            raise CameraError("invalid_state", "原生相机已关闭")
        try:
            result = self._owner.read(timeout)
        except Exception as exc:
            with suppress(Exception):
                self._owner.close()
            self._closed = True
            raise self._translate(exc) from exc
        if (
            not isinstance(result, tuple)
            or len(result) != 6
            or isinstance(result[0], bool)
            or not isinstance(result[0], int)
            or isinstance(result[1], bool)
            or not isinstance(result[1], int)
            or isinstance(result[2], bool)
            or not isinstance(result[2], int)
            or any(not isinstance(payload, bytes) for payload in result[3:])
        ):
            with suppress(Exception):
                self._owner.close()
            self._closed = True
            raise CameraError("invalid_native_frame", "原生相机返回了无效帧结构")
        sequence, timestamp, application_dropped, left, right, raw_side_by_side = result
        if sequence < 0 or timestamp <= 0 or application_dropped < 0 or not raw_side_by_side:
            with suppress(Exception):
                self._owner.close()
            self._closed = True
            raise CameraError("invalid_native_frame", "原生相机返回了无效帧内容")
        try:
            self._observe_native_stats()
        except Exception:
            with suppress(Exception):
                self._owner.close()
            self._closed = True
            raise
        if self._metrics is not None:
            self._metrics.record_copy("sbs_input", len(raw_side_by_side))
            self._metrics.record_copy("left_output", len(left))
            self._metrics.record_copy("right_output", len(right))
            lease = self._metrics.retain_payload(
                "camera_frame",
                len(raw_side_by_side) + len(left) + len(right),
            )
        else:
            lease = None
        self._accounted_application_drops += application_dropped
        return StereoFrame(
            sequence,
            timestamp,
            left,
            right,
            True,
            raw_side_by_side,
            lease,
            application_dropped,
        )

    def stop(self) -> None:
        if self._closed:
            return
        try:
            self._owner.stop()
            self._observe_native_stats()
            self._record_tail_rejections()
        except Exception as exc:
            with suppress(Exception):
                self._owner.close()
            self._closed = True
            raise self._translate(exc) from exc

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        try:
            self._owner.close()
            self._observe_native_stats()
            self._record_tail_rejections()
        except Exception as exc:
            raise self._translate(exc) from exc

    @property
    def closed(self) -> bool:
        return self._closed


def v4l2_production_stream_factory(
    device: Path,
    mode: CameraMode,
    *,
    metrics: PerformanceMetrics | None = None,
) -> CameraStream:
    """Production stream factory requiring the complete Rust data plane."""

    rounded_fps = round(mode.fps)
    if abs(mode.fps - rounded_fps) > max(0.01, mode.fps * 0.001):
        raise CameraError("unsupported_mode", "原生相机只接受整数帧率")
    try:
        owner = create_native_camera(
            str(device),
            mode.width,
            mode.height,
            rounded_fps,
            mode.encoding,
            buffer_count=16,
            queue_capacity=64,
            split_eyes=False,
        )
    except NativeModuleError as exc:
        raise CameraError(exc.code, exc.message) from exc
    return NativeV4L2CameraStream(owner, metrics=metrics)


class V4L2DiscoveryBackend:
    """Discover V4L2 devices and open the real mmap stream by default."""

    def __init__(
        self,
        *,
        sys_root: Path = Path("/sys"),
        dev_root: Path = Path("/dev"),
        v4l2_ctl: str | None = None,
        stream_factory: Callable[[Path, CameraMode], CameraStream] | None = v4l2_stream_factory,
    ) -> None:
        self._sys_root = sys_root
        self._dev_root = dev_root
        self._v4l2_ctl = shutil.which("v4l2-ctl") if v4l2_ctl is None else v4l2_ctl
        self._stream_factory = stream_factory

    def _modes(self, node: Path) -> tuple[CameraMode, ...]:
        if not self._v4l2_ctl:
            return ()
        try:
            result = subprocess.run(
                [self._v4l2_ctl, "--device", str(node), "--list-formats-ext"],
                capture_output=True,
                text=True,
                timeout=5,
                check=False,
            )
        except (OSError, subprocess.SubprocessError):
            return ()
        return parse_v4l2_formats(result.stdout) if result.returncode == 0 else ()

    def discover(self) -> tuple[CameraDescriptor, ...]:
        base = self._sys_root / "class" / "video4linux"
        try:
            entries = sorted(base.iterdir(), key=lambda path: path.name)
        except OSError:
            return ()
        devices = [
            CameraDescriptor(
                stable_id=_usb_identity(entry),
                node=str(self._dev_root / entry.name),
                name=_text(entry / "name") or entry.name,
                modes=self._modes(self._dev_root / entry.name),
            )
            for entry in entries
        ]
        return tuple(sorted(devices, key=lambda device: device.stable_id))

    def open(self, descriptor: CameraDescriptor, mode: CameraMode) -> CameraStream:
        if self._stream_factory is None:
            raise CameraError(
                "backend_unavailable",
                "V4L2 stream factory is disabled; no camera stream was opened",
            )
        return self._stream_factory(Path(descriptor.node), mode)

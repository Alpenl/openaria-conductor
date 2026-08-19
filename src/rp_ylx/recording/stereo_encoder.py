"""驱动板上 `ylx-stereo-encoder` 助手进程的边录边编码接口。

录制守护进程继续拥有采集、帧序、IMU 和丢帧记账；本模块只负责把每一帧并排
MJPEG 交给助手进程，并把助手封好的分段回报给录制器。助手独立成进程的原因是
JPU/VPU 卡死时只应让本次录制失败，而不是拖垮守护进程。
"""

from __future__ import annotations

import json
import os
import shutil
import struct
import subprocess
import sys
import threading
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path

from rp_ylx.native import NativeModuleError, NativeSessionIo, create_native_session_io

_FRAME_MAGIC = b"YLXF"
_HEADER = struct.Struct("<4sI")
_READY_TIMEOUT_SECONDS = 15.0
_DEFAULT_EXECUTABLE = "ylx-stereo-encoder"
_SESSION_IO_LOCK = threading.Lock()
_SESSION_IO: NativeSessionIo | None = None
_SESSION_IO_UNAVAILABLE = False


class StereoEncoderError(RuntimeError):
    def __init__(self, code: str, message: str) -> None:
        self.code = code
        self.message = message
        super().__init__(f"{code}: {message}")


@dataclass(frozen=True, slots=True)
class ClosedSegment:
    """助手已经封好、可独立校验的一组左右眼分段。"""

    index: int
    start_frame: int
    end_frame: int
    left_path: str
    left_bytes: int
    right_path: str
    right_bytes: int


def resolve_executable(explicit: str | Path | None = None) -> Path:
    """定位助手可执行文件；找不到即视为本设备不具备边录边编码能力。"""

    if explicit is not None:
        candidate = Path(explicit)
        if candidate.is_file():
            return candidate
        raise StereoEncoderError("encoder_unavailable", f"助手不存在：{candidate}")
    override = os.environ.get("RP_YLX_STEREO_ENCODER")
    if override:
        return resolve_executable(override)
    # systemd 不会把发行版的 bin 目录放进 PATH，所以先看解释器旁边和
    # package-owned runtime 所属 release 的 bin 目录。
    executable = Path(sys.executable).resolve()
    beside = executable.parent / _DEFAULT_EXECUTABLE
    if beside.is_file():
        return beside
    if executable.parent.name == "bin" and executable.parent.parent.name == "runtime":
        release_bin = executable.parent.parent.parent / "bin" / _DEFAULT_EXECUTABLE
        if release_bin.is_file():
            return release_bin
    for entry in os.environ.get("PYTHONPATH", "").split(os.pathsep):
        if not entry:
            continue
        release_bin = Path(entry).resolve().parent / "bin" / _DEFAULT_EXECUTABLE
        if release_bin.is_file():
            return release_bin
    located = shutil.which(_DEFAULT_EXECUTABLE)
    if located is None:
        raise StereoEncoderError("encoder_unavailable", "未安装 ylx-stereo-encoder")
    return Path(located)


class StereoEncoderProcess:
    """一次录制对应一个助手进程；不复用、不重启。"""

    def __init__(
        self,
        out_dir: str | Path,
        *,
        executable: str | Path | None = None,
        width: int,
        height: int,
        fps: int,
        bitrate_kbps: int = 8192,
        segment_frames: int = 900,
        path_prefix: str = "video/",
    ) -> None:
        if width <= 0 or width % 4 or height <= 0 or fps <= 0 or segment_frames <= 0:
            raise ValueError("边录边编码参数无效")
        self._executable = resolve_executable(executable)
        self._out_dir = Path(out_dir)
        self._width = width
        self._height = height
        self._fps = fps
        self._bitrate_kbps = bitrate_kbps
        self._segment_frames = segment_frames
        self._path_prefix = path_prefix
        self._process: subprocess.Popen[bytes] | None = None
        self._reader: threading.Thread | None = None
        self._lock = threading.Lock()
        self._segments: list[ClosedSegment] = []
        self._failure: StereoEncoderError | None = None
        self._ready = threading.Event()
        self._done = threading.Event()
        self._stats: dict[str, int] = {}
        self._submitted = 0

    @property
    def segment_frames(self) -> int:
        return self._segment_frames

    @property
    def segments(self) -> tuple[ClosedSegment, ...]:
        with self._lock:
            return tuple(self._segments)

    @property
    def stats(self) -> dict[str, int]:
        with self._lock:
            return dict(self._stats)

    @property
    def submitted_frames(self) -> int:
        return self._submitted

    def start(self) -> None:
        if self._process is not None:
            raise StereoEncoderError("invalid_state", "助手进程只能启动一次")
        command = [
            str(self._executable),
            "--out-dir",
            str(self._out_dir),
            "--path-prefix",
            self._path_prefix,
            "--width",
            str(self._width),
            "--height",
            str(self._height),
            "--fps",
            str(self._fps),
            "--bitrate-kbps",
            str(self._bitrate_kbps),
            "--segment-frames",
            str(self._segment_frames),
        ]
        try:
            self._process = subprocess.Popen(  # noqa: S603 - 固定参数，无 shell
                command,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.DEVNULL,
                bufsize=0,
            )
        except OSError as error:
            raise StereoEncoderError("encoder_unavailable", f"无法启动助手：{error}") from error
        self._reader = threading.Thread(
            target=self._read_events, name="rp-ylx-stereo-encoder-events", daemon=True
        )
        self._reader.start()
        if not self._ready.wait(_READY_TIMEOUT_SECONDS):
            self.abort()
            raise StereoEncoderError("encoder_unavailable", "助手未在期限内就绪")
        self._raise_if_failed()

    def submit(self, jpeg: bytes) -> None:
        """提交一帧并排 MJPEG。助手拒绝即视为本次录制失败，不做静默丢帧。"""

        process = self._process
        if process is None or process.stdin is None:
            raise StereoEncoderError("invalid_state", "助手进程未启动")
        self._raise_if_failed()
        try:
            native = _session_io_or_none()
            if native is None:
                _writev_all(process.stdin.fileno(), (_HEADER.pack(_FRAME_MAGIC, len(jpeg)), jpeg))
            else:
                written = native.write_encoder_frame(process.stdin.fileno(), jpeg)
                if written != _HEADER.size + len(jpeg):
                    raise BrokenPipeError("encoder pipe native write was short")
        except (BrokenPipeError, OSError, RuntimeError) as error:
            self._raise_if_failed()
            raise StereoEncoderError("encoder_failed", f"助手写入失败：{error}") from error
        self._submitted += 1

    def finish(self, *, timeout: float = 30.0) -> Sequence[ClosedSegment]:
        """关闭输入、等待助手封完最后一段，返回全部已封分段。"""

        process = self._process
        if process is None:
            raise StereoEncoderError("invalid_state", "助手进程未启动")
        if process.stdin is not None:
            try:
                process.stdin.write(_HEADER.pack(_FRAME_MAGIC, 0))
                process.stdin.flush()
                process.stdin.close()
            except OSError:
                pass
        try:
            code = process.wait(timeout=timeout)
        except subprocess.TimeoutExpired as error:
            self.abort()
            raise StereoEncoderError("encoder_failed", "助手未在期限内退出") from error
        if self._reader is not None:
            self._reader.join(timeout=timeout)
        self._raise_if_failed()
        if code != 0:
            raise StereoEncoderError("encoder_failed", f"助手退出码 {code}")
        return self.segments

    def abort(self) -> None:
        process = self._process
        if process is None:
            return
        if process.stdin is not None:
            with _suppress_os_error():
                process.stdin.close()
        if process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                with _suppress_os_error():
                    process.wait(timeout=5)
        if self._reader is not None:
            self._reader.join(timeout=5)

    def _raise_if_failed(self) -> None:
        with self._lock:
            failure = self._failure
        if failure is not None:
            raise failure

    def _read_events(self) -> None:
        process = self._process
        assert process is not None and process.stdout is not None
        try:
            for line in process.stdout:
                self._handle_event(line)
        except OSError as error:
            with self._lock:
                self._failure = self._failure or StereoEncoderError(
                    "encoder_failed", f"助手事件流中断：{error}"
                )
        finally:
            self._ready.set()
            self._done.set()

    def _handle_event(self, line: bytes) -> None:
        text = line.strip()
        if not text:
            return
        try:
            event = json.loads(text)
        except ValueError:
            with self._lock:
                self._failure = self._failure or StereoEncoderError(
                    "encoder_failed", "助手输出不是 JSON"
                )
            return
        kind = event.get("event")
        if kind == "ready":
            self._ready.set()
        elif kind == "segment":
            segment = ClosedSegment(
                index=int(event["index"]),
                start_frame=int(event["start_frame"]),
                end_frame=int(event["end_frame"]),
                left_path=str(event["left"]["path"]),
                left_bytes=int(event["left"]["bytes"]),
                right_path=str(event["right"]["path"]),
                right_bytes=int(event["right"]["bytes"]),
            )
            with self._lock:
                self._segments.append(segment)
        elif kind == "done":
            with self._lock:
                self._stats = {
                    key: int(value)
                    for key, value in event.items()
                    if key != "event" and isinstance(value, int)
                }
        elif kind == "error":
            with self._lock:
                self._failure = self._failure or StereoEncoderError(
                    str(event.get("code", "encoder_failed")),
                    str(event.get("message", "助手报告失败")),
                )
            self._ready.set()


class _suppress_os_error:  # noqa: N801 - 与 contextlib.suppress 同用途的轻量写法
    def __enter__(self) -> None:
        return None

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> bool:
        return isinstance(exc, OSError)


def _writev_all(descriptor: int, chunks: Sequence[bytes]) -> None:
    views = [memoryview(chunk) for chunk in chunks if chunk]
    index = 0
    offset = 0
    while index < len(views):
        current = [views[index][offset:], *views[index + 1 :]]
        written = os.writev(descriptor, current)
        if written <= 0:
            raise BrokenPipeError("encoder pipe wrote zero bytes")
        while index < len(views) and written >= len(views[index]) - offset:
            written -= len(views[index]) - offset
            index += 1
            offset = 0
        if written:
            offset += written


def _session_io_or_none() -> NativeSessionIo | None:
    global _SESSION_IO, _SESSION_IO_UNAVAILABLE
    if _SESSION_IO_UNAVAILABLE:
        return None
    if _SESSION_IO is not None:
        return _SESSION_IO
    with _SESSION_IO_LOCK:
        if _SESSION_IO_UNAVAILABLE:
            return None
        if _SESSION_IO is not None:
            return _SESSION_IO
        try:
            _SESSION_IO = create_native_session_io()
        except NativeModuleError:
            _SESSION_IO_UNAVAILABLE = True
            return None
        return _SESSION_IO

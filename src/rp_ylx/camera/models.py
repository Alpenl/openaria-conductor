"""与具体相机驱动无关的数据类型。"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol


class CameraError(RuntimeError):
    def __init__(self, code: str, message: str, *, retryable: bool = False) -> None:
        self.code = code
        self.message = message
        self.retryable = retryable
        super().__init__(f"{code}: {message}")


@dataclass(frozen=True, slots=True, order=True)
class CameraMode:
    width: int
    height: int
    fps: float
    encoding: str

    def __post_init__(self) -> None:
        if self.width <= 0 or self.height <= 0 or self.fps <= 0 or not self.encoding:
            raise ValueError("相机模式尺寸、帧率和编码必须有效")


@dataclass(frozen=True, slots=True)
class CameraDescriptor:
    stable_id: str
    node: str
    name: str
    modes: tuple[CameraMode, ...]

    def __post_init__(self) -> None:
        if not self.stable_id or not self.node or not self.name:
            raise ValueError("相机描述必须包含稳定 ID、设备节点和名称")


@dataclass(frozen=True, slots=True)
class StereoFrame:
    source_sequence: int
    host_monotonic_ns: int
    left: bytes
    right: bytes
    valid: bool = True
    raw_side_by_side: bytes | None = None


@dataclass(frozen=True, slots=True)
class FrameObservation:
    frame: StereoFrame
    dropped_before: int


class CameraStream(Protocol):
    def start(self) -> None: ...

    def read(self, timeout: float) -> StereoFrame: ...

    def stop(self) -> None: ...

    def close(self) -> None: ...


class CameraBackend(Protocol):
    def discover(self) -> tuple[CameraDescriptor, ...]: ...

    def open(self, descriptor: CameraDescriptor, mode: CameraMode) -> CameraStream: ...

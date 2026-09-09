"""Validated device recording settings; bitrate is per eye, in kb/s."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import asdict, dataclass


@dataclass(frozen=True, slots=True)
class RecordingEncoding:
    codec: str = "h264"
    rate_control: str = "cbr"
    bitrate_kbps: int = 8192
    min_qp: int = 20
    max_qp: int = 51
    intra_qp: int = 22
    initial_qp: int = 24
    gop_frames: int = 30
    vbv_ms: int = 3000

    def __post_init__(self) -> None:
        if self.codec not in {"h264", "hevc"}:
            raise ValueError("recording.codec must be h264 or hevc")
        if self.rate_control not in {"cbr", "vbr", "fixqp", "avbr"}:
            raise ValueError("recording.rate_control must be cbr, vbr, fixqp or avbr")
        for name in ("bitrate_kbps", "gop_frames", "vbv_ms"):
            value = getattr(self, name)
            maximum = {"bitrate_kbps": 100_000, "gop_frames": 300, "vbv_ms": 3000}[name]
            if type(value) is not int or not 1 <= value <= maximum:
                raise ValueError(f"recording.{name} must be an integer in 1..{maximum}")
        for name in ("min_qp", "max_qp", "intra_qp", "initial_qp"):
            value = getattr(self, name)
            if type(value) is not int or not 0 <= value <= 51:
                raise ValueError(f"recording.{name} must be an integer in 0..51")
        if not self.min_qp <= self.intra_qp <= self.max_qp:
            raise ValueError("intra_qp must be within min_qp..max_qp")
        if not self.min_qp <= self.initial_qp <= self.max_qp:
            raise ValueError("initial_qp must be within min_qp..max_qp")

    @classmethod
    def from_mapping(cls, value: object) -> RecordingEncoding:
        if not isinstance(value, Mapping):
            raise ValueError("recording must be an object")
        settings = dict(value)
        preset = settings.pop("preset", "high")
        if preset not in {"standard", "high"}:
            raise ValueError("recording.preset must be standard or high")
        defaults = (
            {}
            if preset == "standard"
            else {
                "bitrate_kbps": 16384,
                "min_qp": 18,
                "max_qp": 32,
                "intra_qp": 20,
                "initial_qp": 22,
            }
        )
        if set(settings) - set(cls.__dataclass_fields__):
            raise ValueError("recording contains unknown settings")
        return cls(**(defaults | settings))

    def arguments(self) -> tuple[str, ...]:
        names = {"rate_control": "rc-mode", "gop_frames": "intra-period"}
        return tuple(
            item
            for key, value in asdict(self).items()
            for item in ("--" + names.get(key, key.replace("_", "-")), str(value))
        )

    def manifest(self) -> dict[str, object]:
        effective = asdict(self)
        if self.rate_control in {"vbr", "fixqp"}:
            for key in ("bitrate_kbps", "min_qp", "max_qp", "vbv_ms"):
                effective[key] = None
        if self.rate_control == "vbr":
            effective["initial_qp"] = None
        return {
            "schema": "openaria.recording-encoding.v1",
            **effective,
            "profile": "high" if self.codec == "h264" else "main",
            "pixel_format": "yuv420p",
            "bit_depth": 8,
            "b_frames": 0,
        }

"""Stdlib-only bootstrap configuration schema, validation, and token loading."""

from __future__ import annotations

import ipaddress
import math
from collections.abc import Mapping
from dataclasses import asdict, dataclass
from pathlib import Path

from rp_ylx.core.fs import read_bearer_token_file, read_bounded_json
from rp_ylx.core.ids import valid_sha256, valid_uuid4

PRODUCTION_CONFIG_SCHEMA = "ylx.production-config.v1"
_MAX_BOOTSTRAP_CONFIG_BYTES = 64 * 1024
_MAX_BEARER_TOKEN_FILE_BYTES = 4096

__all__ = [
    "PRODUCTION_CONFIG_SCHEMA",
    "ProductionConfig",
    "ProductionConfigError",
    "RecordingEncoding",
    "load_bootstrap_config",
    "production_config_from_value",
    "read_bearer_token",
]


class ProductionConfigError(ValueError):
    """Raised when bootstrap or production device configuration is invalid."""

    def __init__(self, message: str, *, code: str = "invalid_production_config") -> None:
        self.code = code
        self.message = message
        super().__init__(f"{code}: {message}")


@dataclass(frozen=True, slots=True)
class RecordingEncoding:
    """Validated device recording settings; bitrate is per eye, in kb/s."""

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
        for name, maximum in (("bitrate_kbps", 100_000), ("gop_frames", 300), ("vbv_ms", 3000)):
            value = getattr(self, name)
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
        preset = settings.pop("preset", "efficient")
        if preset not in {"standard", "high", "efficient"}:
            raise ValueError("recording.preset must be standard, high or efficient")
        defaults = (
            {}
            if preset == "standard"
            else {
                "codec": "hevc" if preset == "efficient" else "h264",
                "bitrate_kbps": 12288 if preset == "efficient" else 16384,
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


@dataclass(frozen=True, slots=True)
class ProductionConfig:
    """Validated production device service configuration."""

    host: str
    port: int
    camera_device: Path
    mountpoint: Path
    state_root: Path
    device_id: str
    device_label: str
    hardware_fingerprint: str
    isolated_network: bool
    security_profile: str = "lab"
    bearer_token_file: Path | None = None
    tls_certificate_file: Path | None = None
    tls_private_key_file: Path | None = None
    principal_id: str = "device-owner"
    data_plane: str = "rust"
    width: int = 3840
    height: int = 1080
    fps: int = 60
    # 0.5 records 30fps split-eye H.264: dual 1080p60 leaves the VPU no headroom.
    frame_decimation: int = 2
    video_layout: str = "split-eyes"
    video_bitrate_kbps: int = 8192
    recording_encoding: RecordingEncoding | None = None
    exposure_time_absolute: int | str | None = "auto"
    segment_seconds: float = 5.0
    audio_enabled: bool = True
    audio_device: str = "hw:CARD=D2UQ2,DEV=0"
    audio_sample_rate_hz: int = 48_000
    audio_channels: int = 2
    audio_sample_format: str = "S16_LE"
    minimum_available_bytes: int = 2 * 1024 * 1024 * 1024
    minimum_available_inodes: int = 1024

    def __post_init__(self) -> None:
        if self.exposure_time_absolute not in (None, "auto") and (
            type(self.exposure_time_absolute) is not int
            or not 1 <= self.exposure_time_absolute <= 100
        ):
            raise ProductionConfigError(
                "camera.exposure_time_absolute 必须为 auto、null 或 1..100 整数"
            )
        try:
            address = ipaddress.ip_address(self.host)
        except ValueError as error:
            raise ProductionConfigError("生产服务地址或设备 UUID 无效") from error
        int_fields = (
            self.port,
            self.width,
            self.height,
            self.fps,
            self.frame_decimation,
            self.video_bitrate_kbps,
            self.audio_sample_rate_hz,
            self.audio_channels,
            self.minimum_available_bytes,
            self.minimum_available_inodes,
        )
        if (
            type(self.isolated_network) is not bool
            or not self.isolated_network
            or any(type(field) is not int for field in int_fields)
            or address.is_multicast
            or (address.is_unspecified and self.host not in {"0.0.0.0", "::"})
            or not 1 <= self.port <= 65535
            or not self.camera_device.is_absolute()
            or not self.mountpoint.is_absolute()
            or not self.state_root.is_absolute()
            or not valid_uuid4(self.device_id)
            or len(self.device_label) != 12
            or not self.device_label.startswith("YLX-")
            or any(character not in "0123456789ABCDEF" for character in self.device_label[4:])
            or not self.hardware_fingerprint.startswith("sha256:")
            or not valid_sha256(self.hardware_fingerprint[7:])
            or self.security_profile not in {"customer", "lab"}
            or (
                self.security_profile == "lab"
                and any(
                    path is not None
                    for path in (
                        self.bearer_token_file,
                        self.tls_certificate_file,
                        self.tls_private_key_file,
                    )
                )
            )
            or (
                self.security_profile == "customer"
                and (
                    self.bearer_token_file is None
                    or not self.bearer_token_file.is_absolute()
                    or self.tls_certificate_file is None
                    or not self.tls_certificate_file.is_absolute()
                    or self.tls_private_key_file is None
                    or not self.tls_private_key_file.is_absolute()
                    or not 1 <= len(self.principal_id) <= 128
                    or any(not 0x21 <= ord(character) <= 0x7E for character in self.principal_id)
                )
            )
            or self.width <= 0
            or self.width % 2
            or self.height <= 0
            or self.fps <= 0
            or self.frame_decimation <= 0
            or self.video_layout != "split-eyes"
            or self.video_bitrate_kbps <= 0
            or type(self.segment_seconds) not in {int, float}
            or not math.isfinite(self.segment_seconds)
            or self.segment_seconds <= 0
            or type(self.audio_enabled) is not bool
            or (self.audio_enabled and not self.audio_device)
            or self.audio_sample_rate_hz <= 0
            or self.audio_channels <= 0
            or self.audio_sample_format != "S16_LE"
            or self.minimum_available_bytes < 0
            or self.minimum_available_inodes < 0
            or self.data_plane != "rust"
        ):
            raise ProductionConfigError("生产服务配置无效")


def _integer(value: object, name: str) -> int:
    if type(value) is not int:
        raise ProductionConfigError(f"{name} 必须是整数")
    return value


def _require_str(value: object, name: str) -> str:
    if not isinstance(value, str):
        raise ProductionConfigError(f"{name} 必须是字符串")
    return value


def production_config_from_value(value: object) -> ProductionConfig:
    """Validate and construct ProductionConfig from a decoded JSON object."""
    required = {
        "schema",
        "listen",
        "camera",
        "storage",
        "state_root",
        "device",
        "security",
    }
    optional = {"audio", "recording"}
    if not isinstance(value, dict):
        raise ProductionConfigError("生产配置顶层字段无效")
    top_level = set(value)
    if not required <= top_level or top_level - required - optional:
        raise ProductionConfigError("生产配置顶层字段无效")
    listen = value["listen"]
    camera = value["camera"]
    storage = value["storage"]
    device = value["device"]
    security = value["security"]
    audio = value.get("audio")
    try:
        encoding = RecordingEncoding.from_mapping(value.get("recording", {}))
    except (TypeError, ValueError) as error:
        raise ProductionConfigError(str(error)) from error
    security_profile = security.get("profile") if isinstance(security, dict) else None
    security_valid = (
        isinstance(security, dict)
        and type(security.get("isolated_network")) is bool
        and (
            security_profile == "lab"
            and set(security) == {"profile", "isolated_network"}
            or security_profile == "customer"
            and set(security)
            == {
                "profile",
                "isolated_network",
                "bearer_token_file",
                "tls_certificate_file",
                "tls_private_key_file",
                "principal_id",
            }
            and isinstance(security.get("bearer_token_file"), str)
            and isinstance(security.get("tls_certificate_file"), str)
            and isinstance(security.get("tls_private_key_file"), str)
            and isinstance(security.get("principal_id"), str)
        )
    )
    if (
        value["schema"] != PRODUCTION_CONFIG_SCHEMA
        or not all(isinstance(item, dict) for item in (listen, camera, storage, device, security))
        or (audio is not None and not isinstance(audio, dict))
        or set(listen) != {"host", "port"}
        or (set(camera) - {"exposure_time_absolute"})
        not in (
            {"device", "width", "height", "fps"},
            {"device", "width", "height", "fps", "data_plane"},
        )
        or (
            audio is not None
            and set(audio) != {"enabled", "device", "sample_rate_hz", "channels", "sample_format"}
        )
        or (audio is not None and type(audio.get("enabled")) is not bool)
        or set(storage) != {"mountpoint", "minimum_available_bytes", "minimum_available_inodes"}
        or set(device) != {"device_id", "device_label", "hardware_fingerprint"}
        or not security_valid
    ):
        raise ProductionConfigError("生产配置结构无效")
    try:
        return ProductionConfig(
            host=_require_str(listen["host"], "listen.host"),
            port=_integer(listen["port"], "listen.port"),
            camera_device=Path(_require_str(camera["device"], "camera.device")),
            mountpoint=Path(_require_str(storage["mountpoint"], "storage.mountpoint")),
            state_root=Path(_require_str(value["state_root"], "state_root")),
            device_id=_require_str(device["device_id"], "device.device_id"),
            device_label=_require_str(device["device_label"], "device.device_label"),
            hardware_fingerprint=_require_str(
                device["hardware_fingerprint"], "device.hardware_fingerprint"
            ),
            isolated_network=security["isolated_network"],
            security_profile=_require_str(security_profile, "security.profile"),
            bearer_token_file=(
                None
                if security_profile == "lab"
                else Path(_require_str(security["bearer_token_file"], "security.bearer_token_file"))
            ),
            tls_certificate_file=(
                None
                if security_profile == "lab"
                else Path(
                    _require_str(security["tls_certificate_file"], "security.tls_certificate_file")
                )
            ),
            tls_private_key_file=(
                None
                if security_profile == "lab"
                else Path(
                    _require_str(security["tls_private_key_file"], "security.tls_private_key_file")
                )
            ),
            principal_id=(
                "device-owner"
                if security_profile == "lab"
                else _require_str(security["principal_id"], "security.principal_id")
            ),
            data_plane=_require_str(camera.get("data_plane", "rust"), "camera.data_plane"),
            width=_integer(camera["width"], "camera.width"),
            height=_integer(camera["height"], "camera.height"),
            fps=_integer(camera["fps"], "camera.fps"),
            recording_encoding=encoding,
            exposure_time_absolute=camera.get("exposure_time_absolute", "auto"),
            video_bitrate_kbps=encoding.bitrate_kbps,
            audio_enabled=True if audio is None else audio["enabled"],
            audio_device=(
                "hw:CARD=D2UQ2,DEV=0"
                if audio is None
                else _require_str(audio["device"], "audio.device")
            ),
            audio_sample_rate_hz=(
                48_000
                if audio is None
                else _integer(audio["sample_rate_hz"], "audio.sample_rate_hz")
            ),
            audio_channels=2 if audio is None else _integer(audio["channels"], "audio.channels"),
            audio_sample_format=(
                "S16_LE"
                if audio is None
                else _require_str(audio["sample_format"], "audio.sample_format")
            ),
            minimum_available_bytes=_integer(
                storage["minimum_available_bytes"], "storage.minimum_available_bytes"
            ),
            minimum_available_inodes=_integer(
                storage["minimum_available_inodes"], "storage.minimum_available_inodes"
            ),
        )
    except KeyError as error:
        raise ProductionConfigError(f"生产配置缺少字段：{error}") from error


def read_bearer_token(path: Path) -> str:
    """Safely read and validate a customer bearer token file (mode must exclude 0o027)."""
    try:
        return read_bearer_token_file(path, max_bytes=_MAX_BEARER_TOKEN_FILE_BYTES)
    except ValueError as error:
        raise ProductionConfigError(str(error)) from error


def load_bootstrap_config(path: str | Path) -> ProductionConfig:
    """Load and validate ProductionConfig solely from device.json without opening SQLite."""
    source = Path(path)
    try:
        value = read_bounded_json(source, max_bytes=_MAX_BOOTSTRAP_CONFIG_BYTES)
    except (OSError, ValueError) as error:
        raise ProductionConfigError(f"无法读取生产配置：{error}") from error
    if not isinstance(value, dict):
        raise ProductionConfigError("生产配置顶层字段无效")
    return production_config_from_value(value)

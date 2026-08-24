"""RDK X5 生产录制服务的配置与生命周期接线。"""

from __future__ import annotations

import hashlib
import ipaddress
import json
import os
import signal
import ssl
import stat
import threading
import time
import uuid
from collections.abc import Callable, Mapping
from contextlib import suppress
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from rp_ylx import __commit__, __version__
from rp_ylx.api import AuditEvent, Principal, SecurityPolicy, create_gateway_server
from rp_ylx.api.events import EventReplayBuffer
from rp_ylx.api.preview import LatestPreviewBuffer
from rp_ylx.camera import (
    CameraController,
    CameraMode,
    V4L2DiscoveryBackend,
    v4l2_production_stream_factory,
)
from rp_ylx.cli_helpers import stable_id_for_device
from rp_ylx.imu import ImuCollector, NativeImuCollector
from rp_ylx.mdns import MdnsPublisher
from rp_ylx.native import NativeModuleError, native_capabilities
from rp_ylx.operational_logging import operational_logger
from rp_ylx.performance.metrics import PerformanceMetrics
from rp_ylx.recording import (
    CaptureCoordinator,
    ContinuousCaptureSources,
    CoordinatorConfig,
    DeviceSessionConfig,
    NativeContinuousCaptureSources,
)

PRODUCTION_CONFIG_SCHEMA = "ylx.production-config.v1"
LAB_OPERATIONS = frozenset(
    {
        "getDevice",
        "getCaptureStatus",
        "streamCaptureEvents",
        "listSessions",
        "getSession",
        "getRetainedUnsuccessfulSessionOutcome",
        "getCurrentSafeSwapReceipt",
        "getPreview",
        "getCameraFocus",
        "getNetworkStatus",
        "scanNetworks",
        "streamNetworkEvents",
        "setCameraFocus",
        "headSessionArtifact",
        "getSessionArtifact",
        "startCapture",
        "stopCapture",
    }
)
CUSTOMER_OPERATIONS = LAB_OPERATIONS | frozenset(
    {
        "createNetworkCredentialReference",
        "applyNetworkDesiredState",
        "retryNetworkTransaction",
        "forgetNetworkClientProfile",
    }
)
_OPERATIONAL_LOG = operational_logger("production-service")


class ProductionConfigError(ValueError):
    def __init__(self, message: str, *, code: str = "invalid_production_config") -> None:
        self.code = code
        self.message = message
        super().__init__(f"{code}: {message}")


@dataclass(frozen=True, slots=True)
class ProductionConfig:
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
    segment_seconds: float = 30.0
    audio_enabled: bool = True
    audio_device: str = "hw:0,0"
    audio_sample_rate_hz: int = 48_000
    audio_channels: int = 2
    audio_sample_format: str = "S16_LE"
    minimum_available_bytes: int = 2 * 1024 * 1024 * 1024
    minimum_available_inodes: int = 1024

    def __post_init__(self) -> None:
        try:
            address = ipaddress.ip_address(self.host)
            device_id = uuid.UUID(self.device_id)
        except ValueError as error:
            raise ProductionConfigError("生产服务地址或设备 UUID 无效") from error
        if (
            not self.isolated_network
            or address.is_multicast
            or address.is_unspecified
            and self.host not in {"0.0.0.0", "::"}
            or not 1 <= self.port <= 65535
            or not self.camera_device.is_absolute()
            or not self.mountpoint.is_absolute()
            or not self.state_root.is_absolute()
            or device_id.version != 4
            or str(device_id) != self.device_id
            or len(self.device_label) != 12
            or not self.device_label.startswith("YLX-")
            or any(character not in "0123456789ABCDEF" for character in self.device_label[4:])
            or len(self.hardware_fingerprint) != 71
            or not self.hardware_fingerprint.startswith("sha256:")
            or any(
                character not in "0123456789abcdef" for character in self.hardware_fingerprint[7:]
            )
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
            or self.video_layout not in {"split-eyes", "raw-side-by-side"}
            or self.video_bitrate_kbps <= 0
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


def load_production_config(path: str | Path) -> ProductionConfig:
    source = Path(path)
    try:
        raw = source.read_bytes()
        if len(raw) > 64 * 1024:
            raise ProductionConfigError("生产配置文件过大")
        value = json.loads(raw)
    except (OSError, json.JSONDecodeError) as error:
        raise ProductionConfigError(f"无法读取生产配置：{error}") from error
    required = {
        "schema",
        "listen",
        "camera",
        "storage",
        "state_root",
        "device",
        "security",
    }
    optional = {"audio"}
    if not isinstance(value, dict):
        raise ProductionConfigError("生产配置顶层字段无效")
    top_level = set(value)
    if top_level != required and top_level != required | optional:
        raise ProductionConfigError("生产配置顶层字段无效")
    listen = value["listen"]
    camera = value["camera"]
    storage = value["storage"]
    device = value["device"]
    security = value["security"]
    audio = value.get("audio")
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
        or set(camera)
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
            host=str(listen["host"]),
            port=_integer(listen["port"], "listen.port"),
            camera_device=Path(str(camera["device"])),
            mountpoint=Path(str(storage["mountpoint"])),
            state_root=Path(str(value["state_root"])),
            device_id=str(device["device_id"]),
            device_label=str(device["device_label"]),
            hardware_fingerprint=str(device["hardware_fingerprint"]),
            isolated_network=security["isolated_network"],
            security_profile=str(security_profile),
            bearer_token_file=(
                None if security_profile == "lab" else Path(str(security["bearer_token_file"]))
            ),
            tls_certificate_file=(
                None if security_profile == "lab" else Path(str(security["tls_certificate_file"]))
            ),
            tls_private_key_file=(
                None if security_profile == "lab" else Path(str(security["tls_private_key_file"]))
            ),
            principal_id=(
                "device-owner" if security_profile == "lab" else str(security["principal_id"])
            ),
            data_plane=str(camera.get("data_plane", "rust")),
            width=_integer(camera["width"], "camera.width"),
            height=_integer(camera["height"], "camera.height"),
            fps=_integer(camera["fps"], "camera.fps"),
            audio_enabled=True if audio is None else audio["enabled"],
            audio_device="hw:0,0" if audio is None else str(audio["device"]),
            audio_sample_rate_hz=(
                48_000
                if audio is None
                else _integer(audio["sample_rate_hz"], "audio.sample_rate_hz")
            ),
            audio_channels=2 if audio is None else _integer(audio["channels"], "audio.channels"),
            audio_sample_format="S16_LE" if audio is None else str(audio["sample_format"]),
            minimum_available_bytes=_integer(
                storage["minimum_available_bytes"], "storage.minimum_available_bytes"
            ),
            minimum_available_inodes=_integer(
                storage["minimum_available_inodes"], "storage.minimum_available_inodes"
            ),
        )
    except KeyError as error:
        raise ProductionConfigError(f"生产配置缺少字段：{error}") from error


def _read_bearer_token(path: Path) -> str:
    try:
        descriptor = os.open(
            path,
            os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0),
        )
        try:
            metadata = os.fstat(descriptor)
            if not stat.S_ISREG(metadata.st_mode):
                raise ProductionConfigError("Bearer token 文件必须是普通文件且不能是符号链接")
            if metadata.st_mode & 0o027:
                raise ProductionConfigError("Bearer token 文件权限必须禁止组写入和其他用户访问")
            raw = os.read(descriptor, 4097)
        finally:
            os.close(descriptor)
    except ProductionConfigError:
        raise
    except OSError as error:
        raise ProductionConfigError("无法读取 Bearer token 文件") from error
    if len(raw) > 4096:
        raise ProductionConfigError("Bearer token 文件过大")
    try:
        token = raw.rstrip(b"\r\n").decode("ascii")
    except UnicodeDecodeError as error:
        raise ProductionConfigError("Bearer token 必须是 ASCII") from error
    if (
        not 32 <= len(token) <= 512
        or any(not 0x21 <= ord(character) <= 0x7E for character in token)
        or raw.rstrip(b"\r\n") != raw.removesuffix(b"\n").removesuffix(b"\r")
    ):
        raise ProductionConfigError("Bearer token 格式无效")
    return token


def _security_policy(config: ProductionConfig) -> SecurityPolicy:
    if config.security_profile == "lab":
        return SecurityPolicy.lab(allowed_operations=LAB_OPERATIONS)
    assert config.bearer_token_file is not None
    token = _read_bearer_token(config.bearer_token_file)
    principal = Principal(
        config.principal_id,
        permissions={operation: None for operation in CUSTOMER_OPERATIONS},
    )
    return SecurityPolicy.customer(tokens={token: principal}, csrf_token=token)


def _validate_tls_file(path: Path, *, private: bool) -> None:
    try:
        descriptor = os.open(
            path,
            os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0),
        )
        try:
            metadata = os.fstat(descriptor)
        finally:
            os.close(descriptor)
    except OSError as error:
        raise ProductionConfigError("无法读取 TLS 文件") from error
    if not stat.S_ISREG(metadata.st_mode):
        raise ProductionConfigError("TLS 文件必须是普通文件且不能是符号链接")
    if private and metadata.st_mode & 0o027:
        raise ProductionConfigError("TLS 私钥权限必须禁止组写入和其他用户访问")


def _enable_customer_tls(server: Any, config: ProductionConfig) -> None:
    if config.security_profile != "customer":
        return
    assert config.tls_certificate_file is not None
    assert config.tls_private_key_file is not None
    _validate_tls_file(config.tls_certificate_file, private=False)
    _validate_tls_file(config.tls_private_key_file, private=True)
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.minimum_version = ssl.TLSVersion.TLSv1_2
    try:
        context.load_cert_chain(
            certfile=str(config.tls_certificate_file),
            keyfile=str(config.tls_private_key_file),
        )
        server.socket = context.wrap_socket(server.socket, server_side=True)
    except (OSError, ssl.SSLError) as error:
        raise ProductionConfigError("无法启用 Customer TLS") from error


def _audit_sink(path: Path) -> Callable[[AuditEvent], None]:
    lock = threading.Lock()

    def write(event: AuditEvent) -> None:
        value = {
            "schema": "ylx.api-audit-event.v1",
            "recorded_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
            "request_id": event.request_id,
            "principal_id": event.principal_id,
            "operation_id": event.operation_id,
            "resource_id": event.resource_id,
            "outcome": event.outcome,
        }
        payload = (json.dumps(value, ensure_ascii=False, separators=(",", ":")) + "\n").encode()
        flags = os.O_WRONLY | os.O_APPEND | os.O_CREAT | getattr(os, "O_CLOEXEC", 0)
        flags |= getattr(os, "O_NOFOLLOW", 0)
        with lock:
            descriptor = os.open(path, flags, 0o600)
            try:
                metadata = os.fstat(descriptor)
                if not stat.S_ISREG(metadata.st_mode):
                    raise OSError("API audit target is not a regular file")
                if metadata.st_mode & 0o777 != 0o600:
                    os.fchmod(descriptor, 0o600)
                remaining = memoryview(payload)
                while remaining:
                    written = os.write(descriptor, remaining)
                    if written <= 0:
                        raise OSError("API audit record was not fully written")
                    remaining = remaining[written:]
            finally:
                os.close(descriptor)

    return write


@dataclass(slots=True)
class ProductionService:
    coordinator: CaptureCoordinator
    server: Any
    event_pump: CaptureEventPump
    mdns_publisher: MdnsPublisher

    def close(self) -> None:
        self.server.server_close()
        self.mdns_publisher.close()
        self.event_pump.close()
        self.coordinator.close()


class CaptureEventPump:
    """把 coordinator 的持久 revision 与同 revision 录制进度发布到 SSE 重放缓存。"""

    def __init__(
        self,
        coordinator: CaptureCoordinator,
        event_buffer: EventReplayBuffer,
        *,
        interval: float = 1.0,
        progress_interval: float = 0.2,
    ) -> None:
        if interval <= 0 or progress_interval <= 0:
            raise ValueError("事件泵 interval 必须大于零")
        self._coordinator = coordinator
        self._event_buffer = event_buffer
        self._interval = interval
        self._progress_interval = progress_interval
        self._stop = threading.Event()
        initial = coordinator.capture_snapshot_event()
        self._last_source = (initial["authority_epoch"], initial["source_revision"])
        self._last_progress_key = self._progress_key(initial)
        self._last_progress_at = 0.0
        self._thread = threading.Thread(
            target=self._run,
            name="rp-ylx-capture-events",
            daemon=False,
        )
        self._thread.start()

    def _run(self) -> None:
        while not self._stop.wait(
            self._progress_interval if self._last_progress_key is not None else self._interval
        ):
            now = time.monotonic()
            event = self._coordinator.capture_snapshot_event()
            source = (event["authority_epoch"], event["source_revision"])
            if source == self._last_source:
                progress_event = self._progress_event(event)
                progress_key = self._progress_key(event)
                if (
                    progress_event is not None
                    and progress_key != self._last_progress_key
                    and (
                        self._last_progress_at == 0.0
                        or now - self._last_progress_at >= self._progress_interval
                    )
                ):
                    self._event_buffer.publish(progress_event)
                    self._last_progress_key = progress_key
                    self._last_progress_at = now
                continue
            self._event_buffer.publish(event)
            self._last_source = source
            self._last_progress_key = self._progress_key(event)
            self._last_progress_at = now

    @staticmethod
    def _active_recording(event: Mapping[str, object]) -> Mapping[str, object] | None:
        data = event.get("data")
        if not isinstance(data, Mapping):
            return None
        active = data.get("active_recording")
        return active if isinstance(active, Mapping) else None

    @classmethod
    def _progress_key(cls, event: Mapping[str, object]) -> tuple[object, ...] | None:
        active = cls._active_recording(event)
        if active is None:
            return None
        state = active.get("recording_state")
        if not isinstance(state, Mapping):
            return None
        progress = state.get("progress")
        if not isinstance(progress, Mapping):
            return None
        return (
            event.get("authority_epoch"),
            event.get("source_revision"),
            state.get("session_id"),
            state.get("state"),
            progress.get("elapsed_seconds"),
            progress.get("captured_frames"),
            progress.get("bytes_written"),
        )

    @classmethod
    def _progress_event(cls, event: Mapping[str, object]) -> dict[str, object] | None:
        active = cls._active_recording(event)
        if active is None:
            return None
        state = active.get("recording_state")
        if not isinstance(state, Mapping):
            return None
        phase = state.get("state")
        if phase not in {"recording", "finalizing", "verifying"}:
            return None
        session_id = state.get("session_id")
        if not isinstance(session_id, str):
            return None
        progress = state.get("progress")
        if not isinstance(progress, Mapping):
            return None
        elapsed = progress.get("elapsed_seconds")
        frames = progress.get("captured_frames")
        if not isinstance(elapsed, (int, float)) or isinstance(elapsed, bool):
            return None
        if type(frames) is not int or frames < 0:
            return None
        return {
            "authority_epoch": event["authority_epoch"],
            "source_revision": event["source_revision"],
            "type": "progress",
            "occurred_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
            "session_id": session_id,
            "data": {
                "schema": "ylx.capture-progress-event.v2",
                "phase": phase,
                "elapsed_seconds": float(elapsed),
                "completed_units": frames,
                "total_units": None,
                "unit": "frames",
            },
        }

    def close(self) -> None:
        self._stop.set()
        self._thread.join(timeout=self._interval + 1.0)
        if self._thread.is_alive():
            raise RuntimeError("事件泵线程未能在关闭期限内退出")


def build_production_service(
    config: ProductionConfig,
    *,
    camera_backend_factory: Callable[[], object] | None = None,
    imu_source_factory: Callable[[Path], object] | None = None,
    mount_checker: Callable[[Path], bool] | None = None,
    mdns_publisher_factory: Callable[[int, str], MdnsPublisher] | None = None,
) -> ProductionService:
    if __commit__ == "unknown" or len(__commit__) != 40:
        raise ProductionConfigError("生产服务必须从带精确提交身份的安装包启动")
    if camera_backend_factory is None:
        try:
            capabilities = native_capabilities()
        except NativeModuleError as exc:
            raise ProductionConfigError(exc.message, code=exc.code) from exc
        if not capabilities.module_available or "native_camera" not in capabilities.features:
            raise ProductionConfigError(
                "正式 Rust 数据面缺少完整 V4L2/TurboJPEG 原生相机能力",
                code="native_camera_unavailable",
            )
        if "camera_frame_validator" not in capabilities.features:
            raise ProductionConfigError(
                "正式采集缺少 Rust 相机帧连续性校验能力",
                code="native_camera_frame_validator_unavailable",
            )
        if config.audio_enabled and "native_audio" not in capabilities.features:
            raise ProductionConfigError(
                "正式采集缺少 Rust/ALSA 原生音频能力",
                code="native_audio_unavailable",
            )
        if "native_timeline" not in capabilities.features:
            raise ProductionConfigError(
                "正式采集缺少 Rust 统一录制时间线能力",
                code="native_timeline_unavailable",
            )
        if "native_imu" not in capabilities.features:
            raise ProductionConfigError(
                "正式采集缺少 Rust/UVC XU 原生 IMU 能力",
                code="native_imu_unavailable",
            )
        if "recording_codec" not in capabilities.features:
            raise ProductionConfigError(
                "正式录制缺少 Rust 热路径编码能力",
                code="native_recording_unavailable",
            )
        if "recording_sink" not in capabilities.features:
            raise ProductionConfigError(
                "正式录制缺少 Rust 热路径写入能力",
                code="native_recording_sink_unavailable",
            )
        if "recording_imu_batch" not in capabilities.features:
            raise ProductionConfigError(
                "正式录制缺少 Rust IMU batch 写入能力",
                code="native_recording_imu_batch_unavailable",
            )
        if "active_take_writer" not in capabilities.features:
            raise ProductionConfigError(
                "正式录制缺少 Rust active take 写入状态能力",
                code="native_active_take_writer_unavailable",
            )
        if "recording_frame_gate" not in capabilities.features:
            raise ProductionConfigError(
                "正式录制缺少 Rust 录制帧门控能力",
                code="native_recording_frame_gate_unavailable",
            )
        if "capture_fanout" not in capabilities.features:
            raise ProductionConfigError(
                "正式持续采集缺少 Rust fanout 热路径能力",
                code="native_capture_fanout_unavailable",
            )
        if "continuous_capture_runtime" not in capabilities.features:
            raise ProductionConfigError(
                "正式持续采集缺少 Rust 连续采集 runtime 能力",
                code="native_continuous_capture_runtime_unavailable",
            )
        if "continuous_capture_raw_sink" not in capabilities.features:
            raise ProductionConfigError(
                "正式持续采集缺少 Rust raw-SBS 直写能力",
                code="native_continuous_capture_raw_sink_unavailable",
            )
        if "continuous_capture_split_sink" not in capabilities.features:
            raise ProductionConfigError(
                "正式持续采集缺少 Rust split-eyes 直写能力",
                code="native_continuous_capture_split_sink_unavailable",
            )
        if "recording_event_queue" not in capabilities.features:
            raise ProductionConfigError(
                "正式录制缺少 Rust 录制事件队列能力",
                code="native_recording_event_queue_unavailable",
            )
        if "artifact_finalize" not in capabilities.features:
            raise ProductionConfigError(
                "正式录制缺少 Rust artifact 封存能力",
                code="native_artifact_finalize_unavailable",
            )
        if "stereo_encoder_events" not in capabilities.features:
            raise ProductionConfigError(
                "正式录制缺少 Rust 编码助手事件解析能力",
                code="native_stereo_encoder_events_unavailable",
            )
        if "stereo_encoder_pipe" not in capabilities.features:
            raise ProductionConfigError(
                "正式录制缺少 Rust 编码助手写入能力",
                code="native_stereo_encoder_pipe_unavailable",
            )
        if "session_io" not in capabilities.features:
            raise ProductionConfigError(
                "正式录制缺少 Rust 会话 I/O 校验能力",
                code="native_session_io_unavailable",
            )
        if "device_session_artifacts" not in capabilities.features:
            raise ProductionConfigError(
                "正式下载缺少 Rust device-session artifact 清单能力",
                code="native_device_session_artifacts_unavailable",
            )
        if "device_session_finalizer" not in capabilities.features:
            raise ProductionConfigError(
                "正式录制缺少 Rust device-session 封存发布能力",
                code="native_device_session_finalizer_unavailable",
            )
        if "preview_buffer" not in capabilities.features:
            raise ProductionConfigError(
                "正式预览缺少 Rust latest-only 缓冲能力",
                code="native_preview_buffer_unavailable",
            )
        if "performance_metrics" not in capabilities.features:
            raise ProductionConfigError(
                "正式采集缺少 Rust 性能指标累计能力",
                code="native_metrics_unavailable",
            )
    mode = CameraMode(config.width, config.height, float(config.fps), "mjpg")
    use_native_continuous_sources = camera_backend_factory is None and imu_source_factory is None
    if camera_backend_factory is None:

        def production_backend() -> V4L2DiscoveryBackend:
            return V4L2DiscoveryBackend(stream_factory=v4l2_production_stream_factory)

        camera_backend_factory = production_backend
    preview = LatestPreviewBuffer(stream_fps=15)
    metrics = PerformanceMetrics()
    if imu_source_factory is None:

        def imu_factory() -> NativeImuCollector:
            return NativeImuCollector(config.camera_device)

    else:

        def imu_factory() -> ImuCollector:
            return ImuCollector(imu_source_factory(config.camera_device))

    if use_native_continuous_sources:
        sources = NativeContinuousCaptureSources(
            str(config.camera_device),
            imu_factory,
            mode,
            preview=preview,
            frame_decimation=config.frame_decimation,
            metrics=metrics,
        )
    else:
        selector = CameraController(camera_backend_factory())
        stable_id = stable_id_for_device(selector, config.camera_device)
        sources = ContinuousCaptureSources(
            lambda: CameraController(camera_backend_factory()),
            imu_factory,
            mode,
            publish_preview=preview.publish,
            stable_id=stable_id,
            warmup_frames=max(1, config.fps),
            frame_decimation=config.frame_decimation,
        )
    coordinator = None
    server = None
    event_pump = None
    mdns_publisher = None
    try:
        try:
            sources.start_preview()
        except BaseException as error:
            code = getattr(error, "code", "camera_preview_unavailable")
            if not config.camera_device.exists():
                code = "camera_not_connected"
            _OPERATIONAL_LOG.event(
                "camera_preview_degraded",
                level="warning",
                error_code=code if isinstance(code, str) else "camera_preview_unavailable",
                exception_type=type(error).__name__,
                retryable=True,
            )
        config.state_root.mkdir(parents=True, exist_ok=True, mode=0o750)
        session_config = DeviceSessionConfig(
            device_id=config.device_id,
            device_label=config.device_label,
            hardware_fingerprint=config.hardware_fingerprint,
            platform="D-Robotics RDK X5 V1.0 + YLX 2UQ2",
            software_version=__version__,
            commit=__commit__,
            width=config.width,
            height=config.height,
            sensor_fps=float(config.fps),
            frame_decimation=config.frame_decimation,
            video_layout=config.video_layout,
            video_bitrate_kbps=config.video_bitrate_kbps,
            segment_seconds=config.segment_seconds,
            audio_enabled=config.audio_enabled,
            audio_device=config.audio_device,
            audio_sample_rate_hz=config.audio_sample_rate_hz,
            audio_channels=config.audio_channels,
            audio_sample_format=config.audio_sample_format,
        )
        coordinator = CaptureCoordinator(
            CoordinatorConfig(
                config.mountpoint,
                config.state_root,
                session_config,
                minimum_available_bytes=config.minimum_available_bytes,
                minimum_available_inodes=config.minimum_available_inodes,
                queue_capacity=1024,
            ),
            mount_checker=mount_checker,
            preview=preview,
            sources=sources,
            metrics=metrics,
        )
        security = _security_policy(config)
        event_buffer = EventReplayBuffer()
        server = create_gateway_server(
            config.host,
            config.port,
            coordinator,
            security=security,
            audit_sink=_audit_sink(config.state_root / "api-audit.ndjson"),
            event_buffer=event_buffer,
            external_scheme="https" if config.security_profile == "customer" else "http",
        )
        _enable_customer_tls(server, config)
        event_pump = CaptureEventPump(coordinator, event_buffer)
        publisher_factory = mdns_publisher_factory or MdnsPublisher
        mdns_publisher = publisher_factory(
            config.port,
            scheme="https" if config.security_profile == "customer" else "http",
        )
        mdns_publisher.start()
    except BaseException:
        if mdns_publisher is not None:
            with suppress(BaseException):
                mdns_publisher.close()
        if event_pump is not None:
            with suppress(BaseException):
                event_pump.close()
        if server is not None:
            with suppress(BaseException):
                server.server_close()
        if coordinator is not None:
            with suppress(BaseException):
                coordinator.close()
        else:
            with suppress(BaseException):
                sources.close()
        raise
    return ProductionService(coordinator, server, event_pump, mdns_publisher)


def run_production_service(config_path: str | Path) -> None:
    started_ns = time.monotonic_ns()
    _OPERATIONAL_LOG.event("production_service_starting")
    try:
        config = load_production_config(config_path)
        service = build_production_service(config)
    except BaseException as error:
        code = getattr(error, "code", "service_start_failed")
        _OPERATIONAL_LOG.event(
            "production_service_start_failed",
            level="error",
            error_code=code if isinstance(code, str) else "service_start_failed",
            exception_type=type(error).__name__,
            duration_ms=(time.monotonic_ns() - started_ns) / 1_000_000,
        )
        raise
    _OPERATIONAL_LOG.event(
        "production_service_ready",
        security_profile=config.security_profile,
        data_plane=config.data_plane,
        port=config.port,
        duration_ms=(time.monotonic_ns() - started_ns) / 1_000_000,
    )
    previous: dict[int, Any] = {}
    stop_signal: str | None = None

    def stop(signum: int, frame: object) -> None:
        nonlocal stop_signal
        del frame
        stop_signal = signal.Signals(signum).name
        raise KeyboardInterrupt

    for signum in (signal.SIGINT, signal.SIGTERM):
        previous[signum] = signal.signal(signum, stop)
    try:
        with suppress(KeyboardInterrupt):
            service.server.serve_forever()
    finally:
        context: dict[str, object] = {"outcome": "shutdown_requested"}
        if stop_signal is not None:
            context["signal"] = stop_signal
        _OPERATIONAL_LOG.event("production_service_stopping", **context)
        try:
            service.close()
        except BaseException as error:
            code = getattr(error, "code", "service_shutdown_failed")
            _OPERATIONAL_LOG.event(
                "production_service_shutdown_failed",
                level="error",
                error_code=code if isinstance(code, str) else "service_shutdown_failed",
                exception_type=type(error).__name__,
            )
            raise
        finally:
            for signum, handler in previous.items():
                signal.signal(signum, handler)
        _OPERATIONAL_LOG.event("production_service_stopped", outcome="closed")


def default_device_identity(seed: bytes) -> Mapping[str, str]:
    """为首次安装生成稳定写入配置的身份；升级不会重新生成。"""

    device_id = str(uuid.uuid4())
    digest = hashlib.sha256(seed + device_id.encode()).hexdigest()
    return {
        "device_id": device_id,
        "device_label": f"YLX-{digest[:8].upper()}",
        "hardware_fingerprint": f"sha256:{digest}",
    }

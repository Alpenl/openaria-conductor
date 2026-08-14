"""RDK X5 生产录制服务的配置与生命周期接线。"""

from __future__ import annotations

import hashlib
import ipaddress
import json
import signal
import threading
import uuid
from collections.abc import Callable, Mapping
from contextlib import suppress
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from rp_ylx import __commit__, __version__
from rp_ylx.api import AuditEvent, SecurityPolicy, create_gateway_server
from rp_ylx.api.events import EventReplayBuffer
from rp_ylx.camera import (
    CameraController,
    CameraMode,
    V4L2DiscoveryBackend,
    v4l2_production_stream_factory,
)
from rp_ylx.cli_helpers import stable_id_for_device
from rp_ylx.imu import ImuCollector, UvcXuImuSource
from rp_ylx.recording import (
    CaptureCoordinator,
    CoordinatorConfig,
    DeviceSessionConfig,
    ThreadedCaptureSources,
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
        "headSessionArtifact",
        "getSessionArtifact",
        "startCapture",
        "stopCapture",
    }
)


class ProductionConfigError(ValueError):
    pass


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
    width: int = 3840
    height: int = 1080
    fps: int = 60
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
            or self.width <= 0
            or self.width % 2
            or self.height <= 0
            or self.fps <= 0
            or self.minimum_available_bytes < 0
            or self.minimum_available_inodes < 0
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
    if not isinstance(value, dict) or set(value) != required:
        raise ProductionConfigError("生产配置顶层字段无效")
    listen = value["listen"]
    camera = value["camera"]
    storage = value["storage"]
    device = value["device"]
    security = value["security"]
    if (
        value["schema"] != PRODUCTION_CONFIG_SCHEMA
        or not all(isinstance(item, dict) for item in (listen, camera, storage, device, security))
        or set(listen) != {"host", "port"}
        or set(camera) != {"device", "width", "height", "fps"}
        or set(storage) != {"mountpoint", "minimum_available_bytes", "minimum_available_inodes"}
        or set(device) != {"device_id", "device_label", "hardware_fingerprint"}
        or set(security) != {"profile", "isolated_network"}
        or security.get("profile") != "lab"
        or type(security.get("isolated_network")) is not bool
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
            width=_integer(camera["width"], "camera.width"),
            height=_integer(camera["height"], "camera.height"),
            fps=_integer(camera["fps"], "camera.fps"),
            minimum_available_bytes=_integer(
                storage["minimum_available_bytes"], "storage.minimum_available_bytes"
            ),
            minimum_available_inodes=_integer(
                storage["minimum_available_inodes"], "storage.minimum_available_inodes"
            ),
        )
    except KeyError as error:
        raise ProductionConfigError(f"生产配置缺少字段：{error}") from error


def _audit_sink(path: Path) -> Callable[[AuditEvent], None]:
    lock = threading.Lock()

    def write(event: AuditEvent) -> None:
        value = {
            "request_id": event.request_id,
            "principal_id": event.principal_id,
            "operation_id": event.operation_id,
            "resource_id": event.resource_id,
            "outcome": event.outcome,
        }
        payload = (json.dumps(value, ensure_ascii=False, separators=(",", ":")) + "\n").encode()
        with lock, path.open("ab", buffering=0) as stream:
            stream.write(payload)

    return write


@dataclass(slots=True)
class ProductionService:
    coordinator: CaptureCoordinator
    server: Any
    event_pump: CaptureEventPump

    def close(self) -> None:
        self.server.server_close()
        self.event_pump.close()
        self.coordinator.close()


class CaptureEventPump:
    """把 coordinator 的持久 source revision 变化发布到有界 SSE 重放缓存。"""

    def __init__(
        self,
        coordinator: CaptureCoordinator,
        event_buffer: EventReplayBuffer,
        *,
        interval: float = 0.25,
    ) -> None:
        if interval <= 0:
            raise ValueError("事件泵 interval 必须大于零")
        self._coordinator = coordinator
        self._event_buffer = event_buffer
        self._interval = interval
        self._stop = threading.Event()
        initial = coordinator.capture_snapshot_event()
        self._last_source = (initial["authority_epoch"], initial["source_revision"])
        self._thread = threading.Thread(
            target=self._run,
            name="rp-ylx-capture-events",
            daemon=False,
        )
        self._thread.start()

    def _run(self) -> None:
        while not self._stop.wait(self._interval):
            event = self._coordinator.capture_snapshot_event()
            source = (event["authority_epoch"], event["source_revision"])
            if source == self._last_source:
                continue
            self._event_buffer.publish(event)
            self._last_source = source

    def close(self) -> None:
        self._stop.set()
        self._thread.join(timeout=self._interval + 1.0)
        if self._thread.is_alive():
            raise RuntimeError("事件泵线程未能在关闭期限内退出")


def build_production_service(
    config: ProductionConfig,
    *,
    camera_backend_factory: Callable[[], object] | None = None,
    imu_source_factory: Callable[[Path], object] = UvcXuImuSource,
    mount_checker: Callable[[Path], bool] | None = None,
) -> ProductionService:
    if __commit__ == "unknown" or len(__commit__) != 40:
        raise ProductionConfigError("生产服务必须从带精确提交身份的安装包启动")
    mode = CameraMode(config.width, config.height, float(config.fps), "mjpg")
    if camera_backend_factory is None:

        def production_backend() -> V4L2DiscoveryBackend:
            return V4L2DiscoveryBackend(stream_factory=v4l2_production_stream_factory)

        camera_backend_factory = production_backend
    selector = CameraController(camera_backend_factory())
    stable_id = stable_id_for_device(selector, config.camera_device)
    sources = ThreadedCaptureSources(
        lambda: CameraController(camera_backend_factory()),
        lambda: ImuCollector(imu_source_factory(config.camera_device)),
        mode,
        stable_id=stable_id,
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
        sources=sources,
    )
    security = SecurityPolicy.lab(allowed_operations=LAB_OPERATIONS)
    event_buffer = EventReplayBuffer()
    try:
        server = create_gateway_server(
            config.host,
            config.port,
            coordinator,
            security=security,
            audit_sink=_audit_sink(config.state_root / "api-audit.ndjson"),
            event_buffer=event_buffer,
        )
        event_pump = CaptureEventPump(coordinator, event_buffer)
    except BaseException:
        coordinator.close()
        raise
    return ProductionService(coordinator, server, event_pump)


def run_production_service(config_path: str | Path) -> None:
    config = load_production_config(config_path)
    service = build_production_service(config)
    previous: dict[int, Any] = {}

    def stop(signum: int, frame: object) -> None:
        del signum, frame
        raise KeyboardInterrupt

    for signum in (signal.SIGINT, signal.SIGTERM):
        previous[signum] = signal.signal(signum, stop)
    try:
        with suppress(KeyboardInterrupt):
            service.server.serve_forever()
    finally:
        service.close()
        for signum, handler in previous.items():
            signal.signal(signum, handler)


def default_device_identity(seed: bytes) -> Mapping[str, str]:
    """为首次安装生成稳定写入配置的身份；升级不会重新生成。"""

    device_id = str(uuid.uuid4())
    digest = hashlib.sha256(seed + device_id.encode()).hexdigest()
    return {
        "device_id": device_id,
        "device_label": f"YLX-{digest[:8].upper()}",
        "hardware_fingerprint": f"sha256:{digest}",
    }

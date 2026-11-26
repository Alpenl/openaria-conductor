"""RDK X5 生产录制服务的配置与生命周期接线。"""

from __future__ import annotations

import fcntl
import json
import math
import os
import signal
import ssl
import stat
import threading
import time
from collections.abc import Callable, Mapping
from contextlib import suppress
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from rp_ylx import __commit__, __version__
from rp_ylx.api import AuditEvent, Principal, SecurityPolicy, create_gateway_server
from rp_ylx.api.events import EventReplayBuffer
from rp_ylx.api.preview import LatestPreviewBuffer
from rp_ylx.camera import (
    CameraController,
    CameraMode,
)
from rp_ylx.cli_helpers import stable_id_for_device
from rp_ylx.config import PRODUCTION_CONFIG_SCHEMA as PRODUCTION_CONFIG_SCHEMA
from rp_ylx.config import (
    ProductionConfig,
    ProductionConfigError,
    production_config_from_value,
    read_bearer_token,
)
from rp_ylx.core.board import RDK_X5_MODEL
from rp_ylx.core.fs import O_CLOEXEC, O_NOFOLLOW, O_NONBLOCK, read_bounded_json
from rp_ylx.core.time import utc_now_iso

__all__ = [
    "CUSTOMER_OPERATIONS",
    "CaptureEventPump",
    "CaptureRecoveryMonitor",
    "CaptureTimerMonitor",
    "LAB_OPERATIONS",
    "PRODUCTION_CONFIG_SCHEMA",
    "ProductionConfig",
    "ProductionConfigError",
    "ProductionService",
    "StorageReserveMonitor",
    "build_production_service",
    "load_production_config",
    "run_production_service",
]
from rp_ylx.imu import ImuCollector
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

LAB_OPERATIONS = frozenset(
    {
        "getDevice",
        "syncDeviceClock",
        "getVoice",
        "setVoiceVolume",
        "getCaptureTimer",
        "setCaptureTimer",
        "getDeviceClock",
        "inspectDeviceClock",
        "firmwareStatus",
        "firmwareUpdate",
        "getCaptureStatus",
        "getCaptureReadiness",
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
        "createNetworkCredentialReference",
        "applyNetworkDesiredState",
        "retryNetworkTransaction",
        "forgetNetworkClientProfile",
        "setCameraFocus",
        "headSessionArtifact",
        "getSessionArtifact",
        "startCapture",
        "stopCapture",
        "deleteSessions",
    }
)
CUSTOMER_OPERATIONS = LAB_OPERATIONS
_OPERATIONAL_LOG = operational_logger("production-service")
API_AUDIT_FILE_BYTES = 32 * 1024 * 1024
_MAX_AUDIT_LOG_BYTES = API_AUDIT_FILE_BYTES


def load_production_config(path: str | Path) -> ProductionConfig:
    source = Path(path)
    try:
        value = read_bounded_json(source, max_bytes=64 * 1024)
    except (OSError, ValueError, UnicodeDecodeError, RecursionError) as error:
        raise ProductionConfigError(f"无法读取生产配置：{error}") from error
    from rp_ylx.settings import effective

    if not isinstance(value, dict):
        raise ProductionConfigError("生产配置顶层字段无效")
    return production_config_from_value(effective(value))


def _security_policy(config: ProductionConfig) -> SecurityPolicy:
    if config.security_profile == "lab":
        return SecurityPolicy.lab(allowed_operations=LAB_OPERATIONS)
    assert config.bearer_token_file is not None
    token = read_bearer_token(config.bearer_token_file)
    principal = Principal(
        config.principal_id,
        permissions={operation: None for operation in CUSTOMER_OPERATIONS},
    )
    return SecurityPolicy.customer(tokens={token: principal}, csrf_token=token)


def _validate_tls_file(path: Path, *, private: bool) -> None:
    try:
        descriptor = os.open(
            path,
            os.O_RDONLY | O_NONBLOCK | O_CLOEXEC | O_NOFOLLOW,
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
        # Defer TLS handshake to worker threads so a stalled TCP client never
        # blocks the single accept loop.
        server.socket = context.wrap_socket(
            server.socket, server_side=True, do_handshake_on_connect=False
        )
    except (OSError, ssl.SSLError) as error:
        raise ProductionConfigError("无法启用 Customer TLS") from error


def _audit_sink(path: Path, *, max_bytes: int | None = None) -> Callable[[AuditEvent], None]:
    rotated_path = path.with_name(path.name + ".1")
    flags = os.O_WRONLY | os.O_APPEND | os.O_CREAT | O_CLOEXEC | O_NOFOLLOW | O_NONBLOCK

    poll_lock = threading.Lock()
    pending_polls = 0
    next_poll_report = 0.0

    def _validate_regular_single_link(info: os.stat_result) -> None:
        if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
            raise OSError("API audit target must be an exclusively linked regular file")

    def _rotate_audit_file() -> None:
        try:
            rotated_info = rotated_path.lstat()
        except FileNotFoundError:
            pass
        else:
            _validate_regular_single_link(rotated_info)
        os.replace(path, rotated_path)

    def append(event: AuditEvent, poll_count: int = 0) -> None:
        value = {
            "schema": "ylx.api-audit-event.v1",
            "recorded_at": utc_now_iso(),
            "request_id": event.request_id,
            "principal_id": event.principal_id,
            "operation_id": event.operation_id,
            "resource_id": event.resource_id,
            "outcome": event.outcome,
        }
        if poll_count:
            value["poll_count"] = poll_count
        payload = (json.dumps(value, ensure_ascii=False, separators=(",", ":")) + "\n").encode()
        limit = API_AUDIT_FILE_BYTES if max_bytes is None else max_bytes
        if len(payload) > limit:
            raise OSError("API audit record exceeds the log file budget")
        while True:
            # Reject special files before open can block or affect a device.
            # The descriptor check below also covers replacement after lstat.
            try:
                existing = path.lstat()
            except FileNotFoundError:
                pass
            else:
                _validate_regular_single_link(existing)
            descriptor = os.open(path, flags, 0o600)
            try:
                fcntl.flock(descriptor, fcntl.LOCK_EX)
                metadata = os.fstat(descriptor)
                # Another writer may have rotated this inode while we waited.
                try:
                    current = path.lstat()
                except FileNotFoundError:
                    continue
                if not os.path.samestat(metadata, current):
                    continue
                _validate_regular_single_link(metadata)
                if metadata.st_mode & 0o777 != 0o600:
                    os.fchmod(descriptor, 0o600)
                if metadata.st_size > 0 and metadata.st_size + len(payload) > limit:
                    _rotate_audit_file()
                    continue
                remaining = memoryview(payload)
                while remaining:
                    written = os.write(descriptor, remaining)
                    if written <= 0:
                        raise OSError("API audit record was not fully written")
                    remaining = remaining[written:]
                return
            finally:
                os.close(descriptor)

    def write(event: AuditEvent) -> None:
        nonlocal pending_polls, next_poll_report
        if (event.operation_id, event.outcome, event.principal_id) != (
            "getCaptureStatus",
            "allowed",
            "isolated-lab-device",
        ):
            append(event)
            return
        with poll_lock:
            pending_polls += 1
            now = time.monotonic()
            if now >= next_poll_report:
                append(event, pending_polls)
                pending_polls = 0
                next_poll_report = now + 60

    return write


@dataclass(slots=True)
class ProductionService:
    coordinator: CaptureCoordinator
    server: Any
    event_pump: CaptureEventPump
    mdns_publisher: MdnsPublisher
    storage_monitor: StorageReserveMonitor | None = None
    capture_timer_monitor: CaptureTimerMonitor | None = None
    recovery_monitor: CaptureRecoveryMonitor | None = None

    def close(self) -> None:
        error = _close_service_components(
            (
                ("server", self.server, "server_close"),
                ("recovery_monitor", self.recovery_monitor, "close"),
                ("capture_timer_monitor", self.capture_timer_monitor, "close"),
                ("storage_monitor", self.storage_monitor, "close"),
                ("event_pump", self.event_pump, "close"),
                ("mdns_publisher", self.mdns_publisher, "close"),
                ("coordinator", self.coordinator, "close"),
            )
        )
        if error is not None:
            raise error


def _close_service_components(
    components: tuple[tuple[str, Any, str], ...],
) -> BaseException | None:
    """Attempt every owned cleanup, preserving the first failure for the caller."""
    first_error: BaseException | None = None
    for stage, component, method in components:
        if component is None:
            continue
        try:
            getattr(component, method)()
        except BaseException as error:
            if first_error is None:
                first_error = error
            with suppress(BaseException):
                _OPERATIONAL_LOG.event(
                    "production_service_component_close_failed",
                    level="error",
                    stage=stage,
                    exception_type=type(error).__name__,
                )
    return first_error


def _report_worker_failure(
    component: str, event: str, error: Exception, next_report_at: float | None
) -> float:
    now = time.monotonic()
    if next_report_at is not None and now < next_report_at:
        return next_report_at
    # Advance before logging, so a failing sink also has a bounded retry rate.
    next_report_at = now + 30.0
    with suppress(Exception):
        operational_logger(component).event(
            event,
            level="error",
            exception_type=type(error).__name__,
            retryable=True,
        )
    return next_report_at


def _start_background_worker(
    target: Callable[[], None], stop: threading.Event, *, name: str, daemon: bool
) -> threading.Thread:
    admitted = threading.Event()
    thread: threading.Thread | None = None

    def run_admitted() -> None:
        admitted.wait()
        if not stop.is_set():
            target()

    try:
        thread = threading.Thread(target=run_admitted, name=name, daemon=daemon)
        thread.start()
        admitted.set()
        return thread
    except BaseException as error:
        # The constructor has not returned an owner to its caller. Stop even a
        # target that is scheduled only after this failed admission unwinds.
        with suppress(BaseException):
            stop.set()
        with suppress(BaseException):
            admitted.set()
        outcome = "start_not_confirmed"
        try:
            if thread is not None and thread.ident is not None:
                outcome = "stop_requested"
                thread.join(timeout=2.0)
                if not thread.is_alive():
                    outcome = "stopped"
        except BaseException as cleanup_error:
            with suppress(BaseException):
                _OPERATIONAL_LOG.event(
                    "background_worker_cleanup_failed",
                    level="error",
                    stage=name,
                    exception_type=type(cleanup_error).__name__,
                )
        with suppress(BaseException):
            _OPERATIONAL_LOG.event(
                "background_worker_start_failed",
                level="error",
                stage=name,
                exception_type=type(error).__name__,
                outcome=outcome,
            )
        raise


class StorageReserveMonitor:
    """A separate thread keeps a slow finalization off the SSE/status event pump."""

    _thread_name = "storage-reserve"
    _interval = 1.0
    _failure_event = "storage_reserve_monitor_failed"

    def __init__(self, coordinator: CaptureCoordinator) -> None:
        self.coordinator = coordinator
        self.stop = threading.Event()
        self.thread = _start_background_worker(
            self._run, self.stop, name=self._thread_name, daemon=True
        )

    def _step(self) -> None:
        self.coordinator.enforce_storage_reserve()

    def _run(self) -> None:
        next_report_at: float | None = None
        while not self.stop.wait(self._interval):
            try:
                self._step()
            except Exception as error:
                next_report_at = _report_worker_failure(
                    "recording", self._failure_event, error, next_report_at
                )

    def close(self) -> None:
        self.stop.set()
        self.thread.join(timeout=5.0)
        if self.thread.is_alive():
            _OPERATIONAL_LOG.event(
                "background_worker_close_timeout",
                level="warning",
                stage=self._thread_name,
            )


class CaptureRecoveryMonitor(StorageReserveMonitor):
    """Failure publication retries must not wait behind the storage monitor."""

    _thread_name = "capture-recovery"
    _interval = 2.0
    _failure_event = "capture_recovery_retry_failed"

    def _step(self) -> None:
        self.coordinator.reconcile_capture_recovery()


class CaptureTimerMonitor(StorageReserveMonitor):
    """Independent of storage probes, browsers and SSE; checks at 100 ms intervals."""

    _thread_name = "capture-timer"
    _interval = 0.1
    _failure_event = "capture_timer_monitor_failed"

    def _step(self) -> None:
        self.coordinator.enforce_capture_timer()


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
        if (
            type(interval) not in {int, float}
            or not math.isfinite(interval)
            or interval <= 0
            or type(progress_interval) not in {int, float}
            or not math.isfinite(progress_interval)
            or progress_interval <= 0
        ):
            raise ValueError("事件泵 interval 必须大于零")
        self._coordinator = coordinator
        self._event_buffer = event_buffer
        self._interval = interval
        self._progress_interval = progress_interval
        self._stop = threading.Event()
        epoch, revision, state = coordinator.capture_recording_progress()
        self._last_source = (epoch, revision)
        self._last_progress_key = self._progress_key(self._last_source, state)
        self._last_progress_at = 0.0
        self._thread = _start_background_worker(
            self._run, self._stop, name="rp-ylx-capture-events", daemon=False
        )

    def _run(self) -> None:
        next_report_at: float | None = None
        while not self._stop.wait(
            self._progress_interval if self._last_progress_key is not None else self._interval
        ):
            try:
                self._poll()
            except Exception as error:
                next_report_at = _report_worker_failure(
                    "recording", "capture_event_pump_failed", error, next_report_at
                )

    def _poll(self) -> None:
        now = time.monotonic()
        epoch, revision, state = self._coordinator.capture_recording_progress()
        source = (epoch, revision)
        # Idle sampling also retries preview startup when a camera reconnects.
        if source != self._last_source or state is None:
            event = self._coordinator.capture_snapshot_event()
            source = (event["authority_epoch"], event["source_revision"])
            state = self._recording_state(event)
            if source != self._last_source:
                self._event_buffer.publish(event)
                self._last_source = source
                self._last_progress_key = self._progress_key(source, state)
                self._last_progress_at = now
                return
        progress_event = self._progress_event(source, state)
        progress_key = self._progress_key(source, state)
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

    @staticmethod
    def _recording_state(event: Mapping[str, object]) -> Mapping[str, object] | None:
        data = event.get("data")
        if not isinstance(data, Mapping):
            return None
        active = data.get("active_recording")
        if not isinstance(active, Mapping):
            return None
        state = active.get("recording_state")
        return state if isinstance(state, Mapping) else None

    @staticmethod
    def _progress_key(
        source: tuple[str, int], state: Mapping[str, object] | None
    ) -> tuple[object, ...] | None:
        if state is None:
            return None
        progress = state.get("progress")
        if not isinstance(progress, Mapping):
            return None
        return (
            *source,
            state.get("session_id"),
            state.get("state"),
            progress.get("elapsed_seconds"),
            progress.get("captured_frames"),
            progress.get("bytes_written"),
        )

    @staticmethod
    def _progress_event(
        source: tuple[str, int], state: Mapping[str, object] | None
    ) -> dict[str, object] | None:
        if state is None:
            return None
        phase = state.get("state")
        if phase not in {"recording", "finalizing", "verifying", "encoding"}:
            return None
        session_id = state.get("session_id")
        if not isinstance(session_id, str):
            return None
        progress = state.get("progress")
        if not isinstance(progress, Mapping):
            return None
        elapsed = progress.get("elapsed_seconds")
        frames = progress.get("captured_frames")
        if type(elapsed) not in {int, float} or not math.isfinite(elapsed):
            return None
        if type(frames) is not int or frames < 0:
            return None
        return {
            "authority_epoch": source[0],
            "source_revision": source[1],
            "type": "progress",
            "occurred_at": utc_now_iso(),
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


def _validate_native_data_plane(config: ProductionConfig) -> None:
    try:
        capabilities = native_capabilities()
    except NativeModuleError as exc:
        raise ProductionConfigError(exc.message, code=exc.code) from exc
    if not capabilities.module_available:
        raise ProductionConfigError(
            "正式服务缺少 Rust 原生模块",
            code="native_module_unavailable",
        )
    required = [
        (
            "capture_engine",
            "native_capture_engine_unavailable",
            "正式采集缺少 Rust CaptureEngine 深层资源 owner",
        ),
        (
            "session_store",
            "native_session_store_unavailable",
            "正式录制缺少 Rust SessionStore 事务 owner",
        ),
        (
            "preview_buffer",
            "native_preview_buffer_unavailable",
            "正式预览缺少 Rust latest-only 缓冲能力",
        ),
        (
            "performance_metrics",
            "native_metrics_unavailable",
            "正式采集缺少 Rust 性能指标累计能力",
        ),
    ]
    if config.audio_enabled:
        required.append(
            (
                "native_audio",
                "native_audio_unavailable",
                "正式采集缺少 Rust/ALSA 原生音频能力",
            )
        )
    for feature, code, message in required:
        if feature not in capabilities.features:
            raise ProductionConfigError(message, code=code)


def _build_capture_sources(
    config: ProductionConfig,
    *,
    using_native_data_plane: bool,
    preview: LatestPreviewBuffer,
    metrics: PerformanceMetrics,
    camera_backend_factory: Callable[[], object] | None,
    imu_source_factory: Callable[[Path], object] | None,
) -> Any:
    mode = CameraMode(config.width, config.height, float(config.fps), "mjpg")
    if using_native_data_plane:
        return NativeContinuousCaptureSources(
            str(config.camera_device),
            mode,
            preview=preview,
            frame_decimation=config.frame_decimation,
            metrics=metrics,
            exposure_time_absolute=config.exposure_time_absolute,
        )
    assert camera_backend_factory is not None
    assert imu_source_factory is not None

    def imu_factory() -> ImuCollector:
        return ImuCollector(imu_source_factory(config.camera_device))

    selector = CameraController(camera_backend_factory())
    stable_id = stable_id_for_device(selector, config.camera_device)
    return ContinuousCaptureSources(
        lambda: CameraController(camera_backend_factory()),
        imu_factory,
        mode,
        publish_preview=preview.publish,
        stable_id=stable_id,
        warmup_frames=max(1, config.fps),
        frame_decimation=config.frame_decimation,
    )


def _start_preview_best_effort(sources: Any, camera_device: Path) -> None:
    try:
        sources.start_preview()
    except Exception as error:
        code = getattr(error, "code", "camera_preview_unavailable")
        if not camera_device.exists():
            code = "camera_not_connected"
        _OPERATIONAL_LOG.event(
            "camera_preview_degraded",
            level="warning",
            error_code=code if isinstance(code, str) else "camera_preview_unavailable",
            exception_type=type(error).__name__,
            retryable=True,
        )


def _build_session_config(config: ProductionConfig) -> DeviceSessionConfig:
    return DeviceSessionConfig(
        device_id=config.device_id,
        device_label=config.device_label,
        hardware_fingerprint=config.hardware_fingerprint,
        platform=f"{RDK_X5_MODEL} + YLX 2UQ2",
        software_version=__version__,
        commit=__commit__,
        width=config.width,
        height=config.height,
        sensor_fps=float(config.fps),
        frame_decimation=config.frame_decimation,
        video_layout=config.video_layout,
        video_bitrate_kbps=config.video_bitrate_kbps,
        recording_encoding=config.recording_encoding,
        lossless_storage=True,
        segment_seconds=config.segment_seconds,
        audio_enabled=config.audio_enabled,
        audio_device=config.audio_device,
        audio_sample_rate_hz=config.audio_sample_rate_hz,
        audio_channels=config.audio_channels,
        audio_sample_format=config.audio_sample_format,
    )


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
    using_native_data_plane = camera_backend_factory is None and imu_source_factory is None
    if (camera_backend_factory is None) != (imu_source_factory is None):
        raise ProductionConfigError(
            "测试采集适配器必须同时提供 camera_backend_factory 和 imu_source_factory",
            code="incomplete_test_capture_adapter",
        )
    if using_native_data_plane:
        _validate_native_data_plane(config)
    preview = LatestPreviewBuffer(stream_fps=25, thumbnails=using_native_data_plane)
    metrics = PerformanceMetrics()
    sources = _build_capture_sources(
        config,
        using_native_data_plane=using_native_data_plane,
        preview=preview,
        metrics=metrics,
        camera_backend_factory=camera_backend_factory,
        imu_source_factory=imu_source_factory,
    )
    coordinator = None
    server = None
    event_pump = None
    mdns_publisher = None
    storage_monitor = None
    capture_timer_monitor = None
    recovery_monitor = None
    try:
        _start_preview_best_effort(sources, config.camera_device)
        config.state_root.mkdir(parents=True, exist_ok=True, mode=0o750)
        session_config = _build_session_config(config)
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
        external_scheme = "https" if config.security_profile == "customer" else "http"
        server = create_gateway_server(
            config.host,
            config.port,
            coordinator,
            security=security,
            audit_sink=_audit_sink(config.state_root / "api-audit.ndjson"),
            event_buffer=event_buffer,
            external_scheme=external_scheme,
        )
        _enable_customer_tls(server, config)
        event_pump = CaptureEventPump(coordinator, event_buffer)
        publisher_factory = mdns_publisher_factory or MdnsPublisher
        mdns_publisher = publisher_factory(config.port, scheme=external_scheme)
        mdns_publisher.start()
        storage_monitor = StorageReserveMonitor(coordinator)
        capture_timer_monitor = CaptureTimerMonitor(coordinator)
        recovery_monitor = CaptureRecoveryMonitor(coordinator)
        return ProductionService(
            coordinator,
            server,
            event_pump,
            mdns_publisher,
            storage_monitor,
            capture_timer_monitor,
            recovery_monitor,
        )
    except BaseException:
        _close_service_components(
            (
                ("server", server, "server_close"),
                ("recovery_monitor", recovery_monitor, "close"),
                ("capture_timer_monitor", capture_timer_monitor, "close"),
                ("storage_monitor", storage_monitor, "close"),
                ("event_pump", event_pump, "close"),
                ("mdns_publisher", mdns_publisher, "close"),
                ("coordinator", coordinator, "close"),
                ("sources", sources if coordinator is None else None, "close"),
            )
        )
        raise


def run_production_service(config_path: str | Path) -> None:
    started_ns = time.monotonic_ns()
    _OPERATIONAL_LOG.event("production_service_starting")
    service: ProductionService | None = None
    previous: dict[int, Any] = {}
    stop_requested = threading.Event()
    stop_signal: str | None = None
    primary_error: BaseException | None = None
    ready = False

    def stop(signum: int, frame: object) -> None:
        nonlocal stop_signal
        del frame
        if stop_requested.is_set():
            return
        stop_requested.set()
        stop_signal = signal.Signals(signum).name
        raise KeyboardInterrupt

    def cleanup_failed(stage: str, error: BaseException) -> None:
        nonlocal primary_error
        if primary_error is None:
            primary_error = error
        with suppress(BaseException):
            code = getattr(error, "code", "service_shutdown_failed")
            _OPERATIONAL_LOG.event(
                "production_service_shutdown_failed",
                level="error",
                stage=stage,
                error_code=code if isinstance(code, str) else "service_shutdown_failed",
                exception_type=type(error).__name__,
            )

    try:
        with suppress(KeyboardInterrupt):
            from rp_ylx.settings import initialize

            initialize(Path(config_path))
            config = load_production_config(config_path)
            service = build_production_service(config)
            for signum in (signal.SIGINT, signal.SIGTERM):
                previous[signum] = signal.signal(signum, stop)
            _OPERATIONAL_LOG.event(
                "production_service_ready",
                security_profile=config.security_profile,
                data_plane=config.data_plane,
                port=config.port,
                duration_ms=(time.monotonic_ns() - started_ns) / 1_000_000,
            )
            ready = True
            service.server.serve_forever()
    except BaseException as error:
        primary_error = error
        if not ready:
            with suppress(BaseException):
                code = getattr(error, "code", "service_start_failed")
                _OPERATIONAL_LOG.event(
                    "production_service_start_failed",
                    level="error",
                    error_code=code if isinstance(code, str) else "service_start_failed",
                    exception_type=type(error).__name__,
                    duration_ms=(time.monotonic_ns() - started_ns) / 1_000_000,
                )
    finally:
        # Disarm stop handler before closing components so a second signal
        # cannot raise KeyboardInterrupt inside coordinator.close().
        stop_requested.set()
        if service is not None:
            try:
                context: dict[str, object] = {"outcome": "shutdown_requested"}
                if stop_signal is not None:
                    context["signal"] = stop_signal
                _OPERATIONAL_LOG.event("production_service_stopping", **context)
            except BaseException as error:
                cleanup_failed("stopping_log", error)
            try:
                service.close()
            except BaseException as error:
                cleanup_failed("service_close", error)
        for signum, handler in previous.items():
            try:
                signal.signal(signum, handler)
            except BaseException as error:
                cleanup_failed(f"restore_{signal.Signals(signum).name}", error)
        if primary_error is None and (ready or stop_requested.is_set()):
            _OPERATIONAL_LOG.event("production_service_stopped", outcome="closed")
    if primary_error is not None:
        raise primary_error

"""Hardware & OS proxies: network IPC, capture leases, clock sync, camera focus, live IMU."""

from __future__ import annotations

import copy
import errno
import fcntl
import os
import stat
import threading
import time
import uuid
from collections.abc import Callable, Mapping
from contextlib import suppress
from dataclasses import dataclass
from http import HTTPStatus
from pathlib import Path
from typing import Protocol

from rp_ylx.api.gateway import (
    NetworkCommand,
    NetworkCommandResult,
    NetworkCredentialCommand,
    ProviderError,
)
from rp_ylx.camera import CameraError, FrameObservation
from rp_ylx.camera_focus import valid_camera_focus_status
from rp_ylx.imu import ImuObservation, ImuSample, RawVector3
from rp_ylx.network import NetworkError, network_operation_lock_path
from rp_ylx.network import network_status as collect_network_status
from rp_ylx.network import network_status_v1 as project_network_status_v1
from rp_ylx.network_control import CONTROL_RESPONSE_TIMEOUT_SECONDS, NetworkControlClientError
from rp_ylx.network_control import request_control as request_network_control
from rp_ylx.operational_logging import operational_logger
from rp_ylx.update import MaintenanceBusyError, maintenance_lock

LIVE_IMU_STALE_SECONDS = 2.0


class CaptureSources(Protocol):
    @property
    def supports_calibration_capture(self) -> bool: ...

    @property
    def open_handle_count(self) -> int: ...

    def start(
        self,
        *,
        mode: str,
        generation_id: str,
        submit_frame: Callable[[FrameObservation], bool],
        submit_imu: Callable[[ImuObservation], bool],
        on_failure: Callable[[str, str], None],
        native_recorder: object | None = None,
    ) -> None: ...

    def stop(self) -> None: ...

    def camera_connection_status(self) -> dict[str, object]: ...

    def camera_focus_status(self) -> dict[str, object] | None: ...

    def set_camera_focus(
        self,
        *,
        value: int | None = None,
        auto_enabled: bool | None = None,
    ) -> dict[str, object]: ...


@dataclass(frozen=True, slots=True)
class LatestImuReceipt:
    session_id: str | None
    sample: ImuSample
    sample_key: tuple[int, int, int, int, int]
    received_at_monotonic: float


class CaptureLeaseManager:
    """Manages shared maintenance and exclusive network-operation OS file leases."""

    def __init__(self, lock_path_fn: Callable[[], Path] | None = None) -> None:
        self._lock_path_fn = lock_path_fn or network_operation_lock_path
        self._network_operation_lease: int | None = None
        self._maintenance_lease: object | None = None

    @property
    def network_operation_lease(self) -> int | None:
        return self._network_operation_lease

    @network_operation_lease.setter
    def network_operation_lease(self, value: int | None) -> None:
        self._network_operation_lease = value

    @property
    def maintenance_lease(self) -> object | None:
        return self._maintenance_lease

    @maintenance_lease.setter
    def maintenance_lease(self, value: object | None) -> None:
        self._maintenance_lease = value

    def acquire(self) -> None:
        if self._network_operation_lease is not None:
            raise ProviderError(
                "network_state_unavailable",
                "录制网络互斥租约状态无效",
                status=HTTPStatus.INTERNAL_SERVER_ERROR,
                retryable=True,
            )
        maintenance = maintenance_lock(shared=True)
        try:
            maintenance.__enter__()
        except (OSError, ValueError) as error:
            raise ProviderError(
                "maintenance_active",
                "设备正在维护，暂时不能开始录制",
                status=HTTPStatus.CONFLICT,
                retryable=isinstance(error, MaintenanceBusyError),
            ) from error
        descriptor = -1
        try:
            descriptor = os.open(
                self._lock_path_fn(),
                os.O_RDONLY
                | os.O_CREAT
                | os.O_NONBLOCK
                | getattr(os, "O_CLOEXEC", 0)
                | getattr(os, "O_NOFOLLOW", 0),
                0o660,
            )
            metadata = os.fstat(descriptor)
            if not stat.S_ISREG(metadata.st_mode):
                raise OSError(errno.EINVAL, "network operation lock is not a regular file")
            if metadata.st_uid == os.geteuid():
                os.fchmod(descriptor, 0o660)
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BaseException as error:
            try:
                self.close_capture_leases(maintenance, descriptor if descriptor >= 0 else None)
            except BaseException as cleanup_error:
                if isinstance(error, Exception) and not isinstance(cleanup_error, Exception):
                    raise cleanup_error from error
                error.add_note(f"capture lease cleanup failed ({type(cleanup_error).__name__})")
            if not isinstance(error, OSError):
                raise
            if error.errno in {errno.EACCES, errno.EAGAIN, errno.EWOULDBLOCK}:
                raise ProviderError(
                    "network_mutation_active",
                    "网络事务执行期间不能开始录制",
                    status=HTTPStatus.CONFLICT,
                    retryable=True,
                ) from error
            raise ProviderError(
                "network_state_unavailable",
                "无法取得录制网络互斥租约",
                status=HTTPStatus.INTERNAL_SERVER_ERROR,
                retryable=True,
            ) from error
        self._network_operation_lease = descriptor
        self._maintenance_lease = maintenance

    def release(self) -> None:
        maintenance = self._maintenance_lease
        self._maintenance_lease = None
        descriptor = self._network_operation_lease
        self._network_operation_lease = None
        self.close_capture_leases(maintenance, descriptor)

    @staticmethod
    def close_capture_leases(maintenance: object | None, descriptor: int | None) -> None:
        # These leases are independently owned. Failure/cancellation while
        # releasing one must not strand the other or prevent later capture.
        try:
            if maintenance is not None:
                maintenance.__exit__(None, None, None)  # type: ignore[attr-defined]
        finally:
            if descriptor is not None:
                try:
                    with suppress(OSError):
                        fcntl.flock(descriptor, fcntl.LOCK_UN)
                finally:
                    with suppress(OSError):
                        os.close(descriptor)


class NetworkControllerProxy:
    """Privileged network controller IPC proxy (never holds lifecycle locks across IPC)."""

    def __init__(
        self,
        request_control_fn: Callable[..., Mapping[str, object]] | None = None,
        collect_network_status_fn: Callable[[], Mapping[str, object]] | None = None,
    ) -> None:
        self._request_control = request_control_fn or request_network_control
        self._collect_network_status = collect_network_status_fn or collect_network_status

    @staticmethod
    def network_mutation_unavailable(
        reason: str = "controller_unavailable",
        message: str = "网络写入控制器暂时不可用",
    ) -> ProviderError:
        return ProviderError(
            "network_mutation_unavailable",
            message,
            status=HTTPStatus.SERVICE_UNAVAILABLE,
            retryable=True,
            details={"reason": reason},
        )

    @classmethod
    def network_provider_error(
        cls,
        response: Mapping[str, object],
        *,
        operation: str,
        body: Mapping[str, object] | None = None,
    ) -> ProviderError:
        error = response.get("error")
        controller_code = error.get("code") if isinstance(error, Mapping) else None
        code = str(controller_code or "network_controller_unavailable")
        mutation_operations = {"apply", "retry", "forget"}
        if code == "idempotency_conflict" and operation in mutation_operations:
            return ProviderError(
                "idempotency_conflict",
                "幂等键已经用于不同的网络请求",
                status=HTTPStatus.CONFLICT,
                details={
                    "idempotency_scope": "network-mutation",
                    "original_transaction_id": None,
                },
            )
        if code == "transaction_not_found" and operation == "retry":
            transaction_id = body.get("transaction_id") if isinstance(body, Mapping) else None
            try:
                parsed_transaction_id = uuid.UUID(str(transaction_id))
            except (AttributeError, TypeError, ValueError):
                parsed_transaction_id = None
            if (
                parsed_transaction_id is not None
                and parsed_transaction_id.version == 7
                and str(parsed_transaction_id) == transaction_id
            ):
                return ProviderError(
                    "network_transaction_not_found",
                    "指定的网络事务不存在",
                    status=HTTPStatus.NOT_FOUND,
                    details={"transaction_id": transaction_id},
                )
        if code in {"controller_busy", "recovery_required"}:
            return cls.network_mutation_unavailable(
                "recovery_required",
                "另一个网络事务仍在执行或等待恢复",
            )
        invalid_details: tuple[str, str] | None = None
        if code == "transaction_not_retryable":
            invalid_details = ("transaction_id", "active_state")
        elif code == "credential_ref_invalid":
            invalid_details = (
                "desired.wifi_client.credential_ref",
                "credential_ref_not_allowed",
            )
        elif code == "credential_ref_expired":
            invalid_details = (
                "desired.wifi_client.credential_ref",
                "credential_ref_expired",
            )
        elif code == "inline_secret_rejected":
            invalid_details = ("desired", "unsafe_secret_field")
        elif code in {"network_config_invalid", "config_invalid"}:
            invalid_details = ("desired.mode", "unsupported_mode")
        elif code == "candidate_invalid":
            desired = body.get("desired") if isinstance(body, Mapping) else None
            mode = desired.get("mode") if isinstance(desired, Mapping) else None
            invalid_details = (
                ("desired.ethernet.static_ipv4", "invalid_static_ipv4")
                if mode == "ethernet-static"
                else ("desired.wifi_client", "security_mismatch")
                if mode == "wifi-client"
                else ("desired.mode", "unsupported_mode")
            )
        elif code == "credential_invalid" and operation != "credentials":
            invalid_details = ("desired.wifi_client", "security_mismatch")
        if invalid_details is not None and operation in {"apply", "retry"}:
            return ProviderError(
                "invalid_network_desired_state",
                "网络目标状态或凭据引用无效",
                status=HTTPStatus.UNPROCESSABLE_ENTITY,
                details={"field": invalid_details[0], "reason": invalid_details[1]},
            )
        if code == "credential_invalid" and operation == "credentials":
            return ProviderError(
                "invalid_request",
                "网络凭据不符合要求",
                status=HTTPStatus.BAD_REQUEST,
            )
        unavailable_reasons = {
            "network_controller_not_enabled": "not_enabled",
            "network_manager_unavailable": "network_manager_unavailable",
            "rescue_ap_unavailable": "rescue_ap_not_validated",
            "rescue_ap_not_validated": "rescue_ap_not_validated",
            "concurrency_unsupported": "unsupported_concurrency",
            "capture_active": "capture_active",
        }
        return cls.network_mutation_unavailable(
            unavailable_reasons.get(code, "controller_unavailable")
        )

    def controller_available(self) -> bool:
        try:
            response = self._request_control("health")
        except NetworkControlClientError:
            return False
        capabilities = response.get("capabilities")
        return (
            response.get("ok") is True
            and response.get("operation") == "health"
            and isinstance(capabilities, Mapping)
            and capabilities.get("mutation_enabled") is True
        )

    def network_status(
        self,
        runtime: Mapping[str, object],
        *,
        capture_active: bool,
    ) -> Mapping[str, object]:
        try:
            legacy_status = self._collect_network_status()
            status = project_network_status_v1(runtime, legacy_status=legacy_status)
        except NetworkError as error:
            raise ProviderError(
                "network_status_unavailable",
                "网络状态暂时不可用",
                status=HTTPStatus.SERVICE_UNAVAILABLE,
                retryable=True,
            ) from error
        if capture_active:
            status = copy.deepcopy(status)
            capability = status.get("mutation_capability")
            if isinstance(capability, dict):
                capability["enabled"] = False
                capability["disabled_reason"] = "capture_active"
        return status

    def scan_networks(self) -> Mapping[str, object]:
        try:
            response = self._request_control(
                "scan",
                timeout_seconds=CONTROL_RESPONSE_TIMEOUT_SECONDS,
            )
        except NetworkControlClientError as exc:
            raise ProviderError(
                "network_scan_unavailable",
                "无线网络扫描暂时不可用",
                status=HTTPStatus.SERVICE_UNAVAILABLE,
                retryable=True,
            ) from exc
        if response.get("ok") is not True or response.get("status") != HTTPStatus.OK:
            raise ProviderError(
                "network_scan_unavailable",
                "无线网络扫描暂时不可用",
                status=HTTPStatus.SERVICE_UNAVAILABLE,
                retryable=response.get("retryable") is True,
            )
        body = response.get("body")
        if not isinstance(body, Mapping):
            raise self.network_mutation_unavailable()
        return body

    def create_network_credential(self, command: NetworkCredentialCommand) -> Mapping[str, object]:
        try:
            response = self._request_control(
                "create_credential",
                principal_id=command.principal_id,
                body={
                    "schema": "ylx.network-credential-request.v1",
                    "passphrase": command.passphrase,
                },
                timeout_seconds=CONTROL_RESPONSE_TIMEOUT_SECONDS,
            )
        except NetworkControlClientError as exc:
            raise self.network_mutation_unavailable() from exc
        if response.get("ok") is not True:
            raise self.network_provider_error(response, operation="credentials")
        body = response.get("body")
        if response.get("status") != HTTPStatus.CREATED or not isinstance(body, Mapping):
            raise self.network_mutation_unavailable()
        return body

    def require_network_idle(self) -> None:
        try:
            network_response = self._request_control("status")
        except NetworkControlClientError as error:
            raise ProviderError(
                "network_state_unavailable",
                "无法确认网络事务为空闲状态",
                status=HTTPStatus.INTERNAL_SERVER_ERROR,
                retryable=True,
            ) from error
        network_body = network_response.get("body")
        transaction = network_body.get("transaction") if isinstance(network_body, Mapping) else None
        if not (
            network_response.get("ok") is True
            and network_response.get("operation") == "status"
            and network_response.get("status") == HTTPStatus.OK
            and isinstance(transaction, Mapping)
            and "current" in transaction
        ):
            raise ProviderError(
                "network_state_unavailable",
                "无法确认网络事务为空闲状态",
                status=HTTPStatus.INTERNAL_SERVER_ERROR,
                retryable=True,
            )
        if transaction.get("current") is not None:
            raise ProviderError(
                "network_mutation_active",
                "网络事务执行期间不能开始录制",
                status=HTTPStatus.CONFLICT,
                retryable=True,
            )

    def network_command(
        self,
        operation: str,
        command: NetworkCommand,
    ) -> NetworkCommandResult:
        """Execute a network controller command outside any coordinator lock."""
        try:
            response = self._request_control(
                operation,
                principal_id=command.principal_id,
                idempotency_key=command.idempotency_key,
                body=command.body,
                timeout_seconds=CONTROL_RESPONSE_TIMEOUT_SECONDS,
            )
        except NetworkControlClientError as exc:
            raise self.network_mutation_unavailable() from exc
        if response.get("ok") is not True:
            raise self.network_provider_error(response, operation=operation, body=command.body)
        status = response.get("status")
        if type(status) is not int:
            raise self.network_mutation_unavailable()
        return NetworkCommandResult(
            status,
            response.get("body"),
            replayed=response.get("replayed") is True,
        )


class ClockSyncProxy:
    """Privileged clock-sync IPC proxy."""

    def __init__(
        self, request_control_fn: Callable[..., Mapping[str, object]] | None = None
    ) -> None:
        self._request_control = request_control_fn or request_network_control

    def clock_control(
        self, operation: str, principal_id: str, body: Mapping[str, object]
    ) -> Mapping[str, object]:
        try:
            response = self._request_control(
                operation,
                principal_id=principal_id,
                body=body,
                timeout_seconds=3.0,
            )
        except NetworkControlClientError as exc:
            raise ProviderError(
                "clock_sync_unavailable",
                "设备校时服务暂时不可用",
                status=HTTPStatus.SERVICE_UNAVAILABLE,
                retryable=True,
            ) from exc
        if response.get("ok") is not True:
            error = response.get("error", {})
            code = str(error.get("code", "clock_sync_unavailable"))
            status = (
                HTTPStatus.CONFLICT
                if code
                in {
                    "clock_challenge_expired",
                    "clock_changed",
                    "clock_ntp_active",
                    "capture_active",
                }
                else HTTPStatus.SERVICE_UNAVAILABLE
            )
            raise ProviderError(
                code,
                str(error.get("message", "设备日期尚未校准，请重试")),
                status=status,
                retryable=True,
            )
        return response["body"]  # type: ignore[return-value]


class CameraFocusProxy:
    """Serializes V4L2 camera focus reads/writes outside the capture lifecycle lock."""

    def __init__(self) -> None:
        self._focus_lock = threading.Lock()

    @staticmethod
    def camera_connection_status(sources: CaptureSources | None) -> dict[str, object]:
        status_reader = getattr(sources, "camera_connection_status", None)
        if not callable(status_reader):
            return {"schema": "ylx.camera-connection.v1", "state": "connected"}
        status = status_reader()
        if (
            not isinstance(status, Mapping)
            or set(status) != {"schema", "state"}
            or status.get("schema") != "ylx.camera-connection.v1"
            or status.get("state") not in {"connected", "disconnected"}
        ):
            raise RuntimeError("camera connection status 无效")
        return dict(status)

    @staticmethod
    def camera_not_connected() -> ProviderError:
        return ProviderError(
            "camera_not_connected",
            "相机未接入",
            status=HTTPStatus.SERVICE_UNAVAILABLE,
            retryable=True,
        )

    @staticmethod
    def camera_focus_status(
        sources: CaptureSources | None,
        *,
        raise_errors: bool = False,
    ) -> dict[str, object] | None:
        if sources is None:
            return None
        try:
            return sources.camera_focus_status()
        except CameraError:
            if raise_errors:
                raise
            return None
        except Exception as exc:
            if raise_errors:
                raise CameraError("camera_focus_unavailable", str(exc), retryable=True) from exc
            return None

    def require_camera_connected(
        self,
        sources: CaptureSources | None,
        *,
        restore_focus: bool,
        get_pending_focus: Callable[[], tuple[dict[str, object] | None, bool]],
        set_pending_focus: Callable[[dict[str, object] | None, bool], None],
    ) -> None:
        if self.camera_connection_status(sources)["state"] != "connected":
            raise self.camera_not_connected()
        if not restore_focus:
            return
        with self._focus_lock:
            pending, invalid = get_pending_focus()
            if pending is not None and (
                not isinstance(pending, dict)
                or not set(pending) <= {"value", "auto_enabled"}
                or (pending.get("value") is not None and type(pending["value"]) is not int)
                or (
                    pending.get("auto_enabled") is not None
                    and type(pending["auto_enabled"]) is not bool
                )
                or all(pending.get(key) is None for key in ("value", "auto_enabled"))
            ):
                if not invalid:
                    operational_logger("recording").event(
                        "camera_focus_restore_rejected",
                        level="warning",
                        error_code="invalid_camera_focus",
                    )
                invalid = True
                set_pending_focus(pending, True)
            if invalid:
                raise ProviderError(
                    "camera_focus_restore_failed",
                    "已保存的焦距设置损坏，请重新设置焦距",
                    status=HTTPStatus.SERVICE_UNAVAILABLE,
                    retryable=False,
                )
            if pending is not None and sources is not None:
                try:
                    result = sources.set_camera_focus(**pending)
                    if not valid_camera_focus_status(result):
                        raise CameraError(
                            "camera_focus_unavailable", "焦距恢复响应无效", retryable=True
                        )
                except CameraError as error:
                    if error.code == "camera_focus_unsupported":
                        set_pending_focus(None, False)
                        operational_logger("recording").event(
                            "camera_focus_restore_skipped", level="warning", error_code=error.code
                        )
                        return
                    operational_logger("recording").event(
                        "camera_focus_restore_failed", level="warning", error_code=error.code
                    )
                    raise ProviderError(
                        "camera_focus_restore_failed",
                        "已保存的焦距设置未能恢复，请重试或重新设置焦距",
                        status=HTTPStatus.SERVICE_UNAVAILABLE,
                        retryable=error.retryable,
                    ) from error
                except Exception as error:
                    operational_logger("recording").event(
                        "camera_focus_restore_failed",
                        level="warning",
                        exception_type=type(error).__name__,
                    )
                    raise ProviderError(
                        "camera_focus_restore_failed",
                        "已保存的焦距设置暂时无法恢复",
                        status=HTTPStatus.SERVICE_UNAVAILABLE,
                        retryable=True,
                    ) from error
                set_pending_focus(None, False)

    @staticmethod
    def validate_focus_body(body: Mapping[str, object]) -> tuple[int | None, bool | None]:
        value = body.get("value")
        auto_enabled = body.get("auto_enabled")
        if value is None and auto_enabled is None:
            raise ProviderError(
                "invalid_camera_focus",
                "焦距请求必须包含 value 或 auto_enabled",
                status=HTTPStatus.BAD_REQUEST,
            )
        if value is not None and type(value) is not int:
            raise ProviderError(
                "invalid_camera_focus",
                "焦距 value 必须是整数",
                status=HTTPStatus.BAD_REQUEST,
            )
        if auto_enabled is not None and type(auto_enabled) is not bool:
            raise ProviderError(
                "invalid_camera_focus",
                "auto_enabled 必须是布尔值",
                status=HTTPStatus.BAD_REQUEST,
            )
        return value, auto_enabled

    def apply_camera_focus(
        self,
        sources: CaptureSources,
        *,
        value: int | None,
        auto_enabled: bool | None,
    ) -> dict[str, object]:
        with self._focus_lock:
            try:
                result = sources.set_camera_focus(value=value, auto_enabled=auto_enabled)
            except CameraError as error:
                if error.code in {"invalid_camera_focus", "invalid_focus_value"}:
                    status = HTTPStatus.BAD_REQUEST
                elif error.code == "camera_focus_unsupported":
                    status = HTTPStatus.NOT_FOUND
                elif error.code == "camera_focus_auto_unsupported":
                    status = HTTPStatus.UNPROCESSABLE_ENTITY
                elif error.retryable:
                    status = HTTPStatus.SERVICE_UNAVAILABLE
                else:
                    status = HTTPStatus.CONFLICT
                raise ProviderError(
                    error.code,
                    error.message,
                    status=status,
                    retryable=error.retryable,
                ) from error
            except Exception as exc:
                raise ProviderError(
                    "camera_focus_unavailable",
                    str(exc),
                    status=HTTPStatus.SERVICE_UNAVAILABLE,
                    retryable=True,
                ) from exc
            if not valid_camera_focus_status(result):
                raise ProviderError(
                    "camera_focus_unavailable",
                    "相机返回的焦距状态无效",
                    status=HTTPStatus.SERVICE_UNAVAILABLE,
                    retryable=True,
                )
            return result


class LiveImuTracker:
    """Tracks latest fresh IMU sample across live recording and source polling."""

    def __init__(self, *, monotonic_fn: Callable[[], float] | None = None) -> None:
        self._latest_imu: LatestImuReceipt | None = None
        self._monotonic = monotonic_fn or time.monotonic

    @property
    def latest_imu(self) -> LatestImuReceipt | None:
        return self._latest_imu

    @latest_imu.setter
    def latest_imu(self, value: LatestImuReceipt | None) -> None:
        self._latest_imu = value

    @staticmethod
    def raw_imu_vector(vector: RawVector3) -> Mapping[str, object]:
        return {"x": vector.x, "y": vector.y, "z": vector.z}

    @classmethod
    def live_imu_from_sample(
        cls, session_id: str | None, sample: ImuSample
    ) -> Mapping[str, object]:
        return {
            "session_id": session_id,
            "clock": {
                "time_base": "host_monotonic",
                "timestamp_ns": sample.host_monotonic_ns,
            },
            "raw": {
                "units": "raw_int16",
                "accelerometer": cls.raw_imu_vector(sample.accelerometer),
                "gyroscope": cls.raw_imu_vector(sample.gyroscope),
            },
            "sync": {"quality": sample.sync_quality},
        }

    @staticmethod
    def imu_sample_key(sample: ImuSample) -> tuple[int, int, int, int, int]:
        return (
            sample.sequence,
            sample.packet_sequence,
            sample.sample_index,
            sample.device_timestamp_raw,
            sample.host_monotonic_ns,
        )

    @staticmethod
    def fresh_live_imu(
        receipt: LatestImuReceipt | None,
        session_id: str | None,
        observed_at_monotonic: float,
    ) -> tuple[str | None, ImuSample] | None:
        if receipt is None or receipt.session_id != session_id:
            return None
        if observed_at_monotonic - receipt.received_at_monotonic > LIVE_IMU_STALE_SECONDS:
            return None
        return receipt.session_id, receipt.sample

    def store_latest_imu_locked(
        self,
        session_id: str | None,
        sample: ImuSample,
        *,
        observed_at_monotonic: float,
    ) -> LatestImuReceipt:
        key = self.imu_sample_key(sample)
        cached = self._latest_imu
        if (
            cached is not None
            and cached.session_id == session_id
            and (cached.sample_key == key or cached.received_at_monotonic > observed_at_monotonic)
        ):
            return cached
        receipt = LatestImuReceipt(session_id, sample, key, observed_at_monotonic)
        self._latest_imu = receipt
        return receipt

    def snapshot_locked(self, session_id: str | None) -> Mapping[str, object] | None:
        latest = self.fresh_live_imu(self._latest_imu, session_id, self._monotonic())
        return None if latest is None else self.live_imu_from_sample(latest[0], latest[1])

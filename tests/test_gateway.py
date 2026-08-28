from __future__ import annotations

import hashlib
import http.client
import json
import threading
import unittest
from contextlib import redirect_stderr
from copy import deepcopy
from io import StringIO
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from rp_ylx.api import (
    AuditEvent,
    CaptureCommand,
    CaptureCommandResult,
    NetworkCommand,
    NetworkCommandResult,
    NetworkCredentialCommand,
    Principal,
    ProviderError,
    SecurityPolicy,
    create_gateway_server,
)
from rp_ylx.api.gateway import _NetworkEventReplayBuffer
from rp_ylx.web import read_asset

WEB_ASSET_EXPECTATIONS = {
    "/": ("index.html", "text/html; charset=utf-8"),
    "/styles.css": ("styles.css", "text/css; charset=utf-8"),
    "/app.js": ("app.js", "text/javascript; charset=utf-8"),
}
WEB_SECURITY_HEADERS = {
    "Cache-Control": "no-store",
    "Cross-Origin-Resource-Policy": "same-origin",
    "Referrer-Policy": "no-referrer",
    "X-Content-Type-Options": "nosniff",
    "X-Frame-Options": "DENY",
}
WEB_HEADER_NAMES = (
    "Content-Type",
    "Content-Length",
    "Cache-Control",
    "Content-Security-Policy",
    "Cross-Origin-Resource-Policy",
    "Referrer-Policy",
    "X-Content-Type-Options",
    "X-Frame-Options",
)
UNKNOWN_WEB_PATHS = (
    "/index.html",
    "/assets.json",
    "/missing.js",
    "/nested/app.js",
    "/%2e%2e/index.html",
    "/api-client.js",
    "/state.js",
    "/event-stream.js",
    "/preview.js",
    "/chunk-unknown.js",
)

DEVICE_V3 = {
    "schema": "ylx.device.v3",
    "device": {
        "device_id": "550e8400-e29b-41d4-a716-446655440000",
        "device_label": "YLX-30D5872D",
    },
    "hardware_fingerprint": "sha256:" + "a" * 64,
    "api_version": "3.0",
    "build": {
        "package_version": "0.5.0",
        "commit": "2db57ae68e04197397b8ac84f4d71548aa2fcb36",
        "build_id": "rdk-x5-test",
    },
    "security_profile": "customer",
    "capabilities": {
        "capture": True,
        "preview": True,
        "range_download": True,
        "network_mutation": False,
    },
    "storage": {
        "volume_id": "6ba7b810-9dad-41d1-80b4-00c04fd430c8",
        "total_bytes": 128_000_000_000,
        "available_bytes": 96_000_000_000,
        "writable": True,
    },
    "runtime": {
        "observed_at": "2026-08-08T10:24:15+08:00",
        "connection_method": "ethernet_lan",
        "temperature_celsius": 53.5,
        "network": {
            "ap": {
                "state": "active",
                "interface": "wlan0",
                "addresses": ["10.42.0.1/24"],
                "peer_or_ssid": "YLX-30D5872D",
            },
            "wifi_client": {
                "state": "disconnected",
                "interface": "wlan1",
                "addresses": [],
                "peer_or_ssid": None,
            },
            "wired": {
                "state": "connected",
                "interface": "eth0",
                "addresses": ["192.0.2.24/24"],
                "peer_or_ssid": None,
            },
            "default_route": "wired",
        },
        "live_imu": None,
        "camera": {
            "schema": "ylx.camera-connection.v1",
            "state": "connected",
        },
        "camera_focus": None,
    },
}

NETWORK_STATUS = {
    "schema": "ylx.network-status.v1",
    "authority_epoch": "4fa85f64-5717-4562-b3fc-2c963f66afa6",
    "source_revision": 12,
    "observed_at": "2026-08-23T12:58:00+08:00",
    "saved": True,
    "verified": True,
    "desired": {
        "mode": "wifi-client",
        "wifi_client": {
            "ssid": "studio-wifi",
            "security": "wpa2-personal",
            "credential_state": "stored",
        },
        "ethernet": None,
    },
    "observed": {
        "ap": {
            "state": "disabled",
            "interface": "wlan0",
            "addresses": [],
            "peer_or_ssid": None,
        },
        "wifi_client": {
            "state": "connected",
            "interface": "wlan0",
            "addresses": ["192.168.110.36/24"],
            "peer_or_ssid": "studio-wifi",
        },
        "wired": {
            "state": "disconnected",
            "interface": "eth0",
            "addresses": [],
            "peer_or_ssid": None,
        },
        "default_route": "wifi_client",
        "mdns": {
            "hostname": "rp-ylx.local",
            "service": "_ylx-capture._tcp",
            "aliases": ["_http._tcp"],
            "port": 8080,
        },
        "devices": [
            {"interface": "wlan0", "type": "wifi", "state": "connected"},
            {"interface": "eth0", "type": "ethernet", "state": "disconnected"},
        ],
    },
    "transaction": {"current": None, "latest": None},
    "mutation_capability": {
        "enabled": False,
        "disabled_reason": "not_enabled",
        "operations": ["apply", "retry", "forget"],
        "idempotency_key_required": True,
        "secret_handling": "opaque_credential_reference_only",
        "active_state_policy": "idle_only",
    },
    "concurrency_capability": {
        "rescue_ap_required": True,
        "same_phy_ap_sta": "unverified",
        "exclusive_client_failure_timeout_seconds": 10,
        "max_managed_interfaces": 1,
        "max_ap_interfaces": 1,
    },
}

NETWORK_TRANSACTION_ID = "0198d2a0-41a0-7b7a-a751-0e86a39d4db1"
NETWORK_RECEIPT = {
    "schema": "ylx.network-transaction-receipt.v1",
    "accepted_at": "2026-08-23T12:58:03+08:00",
    "transaction": {
        "schema": "ylx.network-transaction.v1",
        "authority_epoch": "4fa85f64-5717-4562-b3fc-2c963f66afa6",
        "source_revision": 13,
        "transaction_id": NETWORK_TRANSACTION_ID,
        "operation": "apply",
        "status": "accepted",
        "stage": "accepted",
        "desired": {
            "mode": "wifi-client",
            "wifi_client": {
                "ssid": "studio-wifi",
                "security": "wpa2-personal",
                "credential_state": "pending_input",
            },
            "ethernet": None,
        },
        "accepted_at": "2026-08-23T12:58:03+08:00",
        "updated_at": "2026-08-23T12:58:03+08:00",
        "deadline": None,
        "recovery_action": "await_device",
        "rescue": {
            "ap_validated": False,
            "fallback_mode": "hotspot",
            "failure_trigger_seconds": 10,
        },
        "error": None,
    },
}

NETWORK_SCAN = {
    "schema": "ylx.network-scan.v1",
    "authority_epoch": NETWORK_STATUS["authority_epoch"],
    "source_revision": NETWORK_STATUS["source_revision"],
    "scanned_at": "2026-08-23T12:58:01+08:00",
    "networks": [
        {
            "ssid": "studio-wifi",
            "hidden": False,
            "security": "wpa2-personal",
            "signal_dbm": -46,
            "credential_required": True,
        },
        {
            "ssid": None,
            "hidden": True,
            "security": "open",
            "signal_dbm": -72,
            "credential_required": False,
        },
    ],
}

NETWORK_CREDENTIAL_RECEIPT = {
    "schema": "ylx.network-credential-receipt.v1",
    "credential_ref": "cred-setup-token-001",
    "issued_at": "2026-08-23T12:58:02+08:00",
    "expires_at": "2026-08-23T13:00:02+08:00",
    "ttl_seconds": 120,
    "single_use": True,
}

SESSION_ID = "01989f6a-2c00-7a1b-8c2d-3e4f50617283"
NEXT_SESSION_ID = "01989f6a-2c02-7c3d-ae4f-5061728394a5"

CAPTURE_STATUS = {
    "schema": "ylx.capture-status.v2",
    "authority_epoch": "4fa85f64-5717-4562-b3fc-2c963f66afa6",
    "source_revision": 45,
    "snapshot": {
        "schema": "ylx.capture-snapshot-event.v2",
        "device_state": "idle",
        "active_recording": None,
        "retained_unsuccessful": None,
        "runtime": deepcopy(DEVICE_V3["runtime"]),
    },
}


def _active_capture_status(session_id: str = SESSION_ID) -> dict[str, object]:
    status = deepcopy(CAPTURE_STATUS)
    snapshot = status["snapshot"]
    assert isinstance(snapshot, dict)
    snapshot["device_state"] = "recording"
    snapshot["active_recording"] = {
        "generation_id": "7d516b70-d8ab-47d1-b2dc-5b1250138789",
        "recording_state": {
            "schema": "ylx.recording-state.v1",
            "state": "recording",
            "authority_epoch": status["authority_epoch"],
            "state_revision": status["source_revision"],
            "updated_at": "2026-08-08T10:24:16+08:00",
            "session_id": session_id,
            "take_id": "01989f69-f000-7c3d-ae4f-5061728394a5",
            "display_name": "active live imu relation",
            "device": deepcopy(DEVICE_V3["device"]),
            "storage": {
                "volume_id": "6ba7b810-9dad-41d1-80b4-00c04fd430c8",
                "status": "mounted",
                "writable": True,
                "remaining_bytes": 1024,
            },
            "progress": {
                "elapsed_seconds": 1.5,
                "captured_frames": 45,
                "bytes_written": 4096,
            },
            "diagnostics": [],
        },
    }
    return status


def _runtime_for_api(runtime: dict[str, object], api_version: str) -> dict[str, object]:
    projected = deepcopy(runtime)
    if api_version in {"v2", "v3"}:
        projected["live_imu"] = None
        projected.pop("camera", None)
        projected.pop("camera_focus", None)
    return projected


def _device_for_api(api_version: str) -> dict[str, object]:
    device = deepcopy(DEVICE_V3)
    device["schema"] = f"ylx.device.{api_version}"
    device["api_version"] = f"{api_version.removeprefix('v')}.0"
    device["runtime"] = _runtime_for_api(device["runtime"], api_version)
    if api_version == "v4":
        device["capabilities"].update(
            {
                "session_list": True,
                "session_detail": True,
                "artifact_download": True,
                "capture_status": True,
                "session_deletion": False,
            }
        )
        device["capabilities"]["calibration_capture"] = {
            "supported": True,
            "enabled": True,
            "disabled_reason": None,
            "required_video_layout": "split-eyes",
        }
    return device


def _capture_status_for_api(
    api_version: str,
    status: dict[str, object] | None = None,
) -> dict[str, object]:
    projected = deepcopy(CAPTURE_STATUS if status is None else status)
    projected["schema"] = (
        "ylx.capture-status.v2" if api_version in {"v2", "v3"} else "ylx.capture-status.v4"
    )
    snapshot = projected["snapshot"]
    assert isinstance(snapshot, dict)
    snapshot["schema"] = (
        "ylx.capture-snapshot-event.v2"
        if api_version in {"v2", "v3"}
        else "ylx.capture-snapshot-event.v4"
    )
    runtime = snapshot["runtime"]
    assert isinstance(runtime, dict)
    snapshot["runtime"] = _runtime_for_api(runtime, api_version)
    return projected


RAW_LIVE_IMU = {
    "session_id": SESSION_ID,
    "clock": {"time_base": "host_monotonic", "timestamp_ns": 10_091},
    "raw": {
        "units": "raw_int16",
        "accelerometer": {"x": 1, "y": 2, "z": 3},
        "gyroscope": {"x": 4, "y": 5, "z": 6},
    },
    "sync": {"quality": "good"},
}

CAMERA_FOCUS_STATUS = {
    "schema": "ylx.camera-focus.v1",
    "value": 42,
    "minimum": 0,
    "maximum": 255,
    "step": 1,
    "default": 32,
    "auto_supported": True,
    "auto_enabled": False,
}

SESSION_LIST = {
    "schema": "ylx.session-list.v2",
    "items": [],
    "diagnostics": [
        {
            "quarantine_id": "550e8400-e29b-41d4-a716-446655440000",
            "code": "manifest_unreadable",
            "observed_at": "2026-08-08T10:25:04+08:00",
            "message": "manifest.json 不是有效 JSON",
        }
    ],
    "next_cursor": None,
}
SESSION_LIST_V4 = {
    **deepcopy(SESSION_LIST),
    "schema": "ylx.session-list.v3",
    "catalog_revision": "sha256:" + "c" * 64,
}

MANIFEST_BYTES = b'{ "schema": "ylx.device-session.v1", "sealed": true }\n'
MANIFEST_DIGEST = hashlib.sha256(MANIFEST_BYTES).hexdigest()
RETAINED_OUTCOME = {
    "schema": "ylx.retained-unsuccessful-session-resource.v2",
    "authority_epoch": "4fa85f64-5717-4562-b3fc-2c963f66afa6",
    "source_revision": 51,
    "outcome": {
        "generation_id": "7d516b70-d8ab-47d1-b2dc-5b1250138789",
        "recording_state": {
            "schema": "ylx.recording-state.v1",
            "state": "failed",
            "authority_epoch": "4fa85f64-5717-4562-b3fc-2c963f66afa6",
            "state_revision": 51,
            "updated_at": "2026-08-08T10:25:04+08:00",
            "session_id": SESSION_ID,
            "take_id": "01989f69-f000-7c3d-ae4f-5061728394a5",
            "display_name": "失败录制",
            "device": deepcopy(DEVICE_V3["device"]),
            "storage": {
                "volume_id": "6ba7b810-9dad-41d1-80b4-00c04fd430c8",
                "status": "mounted",
                "writable": True,
                "remaining_bytes": 96_000_000_000,
            },
            "progress": {
                "elapsed_seconds": 4.0,
                "captured_frames": 120,
                "bytes_written": 8_192,
            },
            "diagnostics": [
                {
                    "code": "capture_failed",
                    "severity": "error",
                    "message": "采集失败",
                    "at": "2026-08-08T10:25:04+08:00",
                    "recoverable": False,
                }
            ],
        },
    },
}
SAFE_SWAP_V3 = {
    "schema": "ylx.safe-swap-receipt-resource.v3",
    "receipt": {
        "schema": "ylx.safe-swap-receipt.v3",
        "session_id": SESSION_ID,
        "volume_id": "6ba7b810-9dad-41d1-80b4-00c04fd430c8",
        "generation_id": "7d516b70-d8ab-47d1-b2dc-5b1250138789",
        "manifest_id": "01989f6a-2c01-7b2c-9d3e-4f5061728394",
        "manifest_sha256": MANIFEST_DIGEST,
        "sealed_at": "2026-08-08T10:24:33+08:00",
        "released_at": "2026-08-08T10:25:03+08:00",
        "release_state": "unmounted",
        "open_handle_count": 0,
    },
}
SAFE_SWAP_V2 = {
    "schema": "ylx.safe-swap-receipt-resource.v2",
    "receipt": {
        **SAFE_SWAP_V3["receipt"],
        "schema": "ylx.safe-swap-receipt.v2",
        "handle_audit": {
            "schema": "ylx.safe-swap-handle-audit.v1",
            "scope": "integrated-m4",
            "participant_set_authority": "m4-qualified-deployment-record",
            "binding_context_sha256": "1" * 64,
            "deployment_record_sha256": "2" * 64,
            "participant_authority_sha256": "3" * 64,
            "expected_participant_set_sha256": "4" * 64,
            "expected_participant_ids": ["gateway"],
            "admission_fence": {
                "schema": "ylx.safe-swap-admission-fence.v1",
                "fence_id": "550e8400-e29b-41d4-a716-446655440000",
                "generation_id": "7d516b70-d8ab-47d1-b2dc-5b1250138789",
                "state": "held",
                "held_until": "receipt-durable-publish",
            },
            "acknowledgements": [
                {
                    "participant_id": "gateway",
                    "access_paths": ["gateway-validation"],
                    "generation_id": "7d516b70-d8ab-47d1-b2dc-5b1250138789",
                    "fence_id": "550e8400-e29b-41d4-a716-446655440000",
                    "open_handle_count": 0,
                    "release_state": "drained",
                    "acknowledged_at": "2026-08-08T10:25:02+08:00",
                }
            ],
        },
    },
}


class MemoryRepresentation:
    def __init__(self, payload: bytes, content_type: str = "application/json") -> None:
        self.payload = payload
        self.size = len(payload)
        self.content_type = content_type
        self.etag = f'"{hashlib.sha256(payload).hexdigest()}"'

    def __enter__(self) -> MemoryRepresentation:
        return self

    def __exit__(self, *args: object) -> None:
        return None

    def iter_chunks(
        self,
        offset: int = 0,
        length: int | None = None,
        *,
        chunk_size: int = 1024 * 1024,
    ) -> object:
        del chunk_size
        end = None if length is None else offset + length
        yield self.payload[offset:end]


class FaultingManifestRepresentation(MemoryRepresentation):
    def iter_chunks(
        self,
        offset: int = 0,
        length: int | None = None,
        *,
        chunk_size: int = 1024 * 1024,
    ) -> object:
        del offset, length, chunk_size
        raise OSError("manifest source read failed")


class DeviceProvider:
    def __init__(self) -> None:
        self.commands: dict[tuple[str, str, str], tuple[bytes, CaptureCommandResult]] = {}
        self.safe_swap: object | None = deepcopy(SAFE_SWAP_V3)
        self.status: object = deepcopy(CAPTURE_STATUS)
        self.focus: object | None = None
        self.network: object = deepcopy(NETWORK_STATUS)
        self.network_scan: object = deepcopy(NETWORK_SCAN)
        self.network_status_error: ProviderError | None = None
        self.network_scan_error: ProviderError | None = None
        self.network_credentials: list[NetworkCredentialCommand] = []
        self.network_credential_result: object = deepcopy(NETWORK_CREDENTIAL_RECEIPT)
        self.network_commands: list[tuple[str, NetworkCommand]] = []
        self.network_results: dict[str, NetworkCommandResult] = {}
        self.network_errors: dict[str, ProviderError] = {}
        self.camera_error: ProviderError | None = None
        self.live_imu: object | None = None
        self.manifest_representation: object | None = None
        self.list_error: ProviderError | None = None
        self.stop_status = 204
        self.device_schema_override: str | None = None
        self.device_extra: dict[str, object] = {}

    def device_descriptor(self, api_version: str, security_profile: str) -> dict[str, object]:
        descriptor = deepcopy(DEVICE_V3)
        descriptor["schema"] = f"ylx.device.{api_version}"
        descriptor["api_version"] = f"{api_version.removeprefix('v')}.0"
        descriptor["security_profile"] = security_profile
        descriptor["runtime"]["camera_focus"] = deepcopy(self.focus)
        descriptor["runtime"]["live_imu"] = deepcopy(self.live_imu)
        if api_version == "v4":
            descriptor["capabilities"].update(
                {
                    "session_list": True,
                    "session_detail": True,
                    "artifact_download": True,
                    "capture_status": True,
                    "session_deletion": False,
                }
            )
            descriptor["capabilities"]["calibration_capture"] = {
                "supported": True,
                "enabled": True,
                "disabled_reason": None,
                "required_video_layout": "split-eyes",
            }
        if self.device_schema_override is not None:
            descriptor["schema"] = self.device_schema_override
        descriptor.update(deepcopy(self.device_extra))
        return descriptor

    def capture_status(self) -> object:
        status = deepcopy(self.status)
        if isinstance(status, dict):
            snapshot = status.get("snapshot")
            if isinstance(snapshot, dict):
                runtime = snapshot.get("runtime")
                if isinstance(runtime, dict):
                    runtime["camera_focus"] = deepcopy(self.focus)
        return status

    def start_capture(self, command: CaptureCommand) -> CaptureCommandResult:
        if self.camera_error is not None:
            raise self.camera_error
        return self._command("start", command, status=202, body=self.status)

    def stop_capture(self, command: CaptureCommand) -> CaptureCommandResult:
        body = self.status if self.stop_status == 202 else None
        return self._command("stop", command, status=self.stop_status, body=body)

    def camera_focus_status(self) -> object | None:
        if self.camera_error is not None:
            raise self.camera_error
        return deepcopy(self.focus)

    def network_status(self) -> object:
        if self.network_status_error is not None:
            raise self.network_status_error
        return deepcopy(self.network)

    def scan_networks(self) -> object:
        if self.network_scan_error is not None:
            raise self.network_scan_error
        return deepcopy(self.network_scan)

    def create_network_credential(self, command: NetworkCredentialCommand) -> object:
        self.network_credentials.append(command)
        return deepcopy(self.network_credential_result)

    def _network_command(self, operation: str, command: NetworkCommand) -> NetworkCommandResult:
        self.network_commands.append((operation, command))
        if operation in self.network_errors:
            raise self.network_errors[operation]
        result = self.network_results.get(operation)
        if result is None:
            raise ProviderError(
                "network_mutation_unavailable",
                "网络写入控制器尚未启用",
                status=503,
                retryable=True,
                details={"reason": "not_enabled"},
            )
        return NetworkCommandResult(result.status, deepcopy(result.body), result.replayed)

    def apply_network_desired_state(self, command: NetworkCommand) -> NetworkCommandResult:
        return self._network_command("apply", command)

    def retry_network_transaction(self, command: NetworkCommand) -> NetworkCommandResult:
        return self._network_command("retry", command)

    def forget_network_client_profile(self, command: NetworkCommand) -> NetworkCommandResult:
        return self._network_command("forget", command)

    def set_camera_focus(self, command: CaptureCommand) -> CaptureCommandResult:
        if self.camera_error is not None:
            raise self.camera_error
        if self.focus is None:
            raise ProviderError(
                "camera_focus_unsupported",
                "当前相机没有可读取的焦距控制",
                status=404,
            )
        next_focus = deepcopy(self.focus)
        if not isinstance(next_focus, dict):
            return self._command("set_camera_focus", command, status=200, body=next_focus)
        body = command.body
        if body.get("auto_enabled") is True:
            next_focus["auto_enabled"] = True
        if "value" in body:
            next_focus["value"] = body["value"]
            if next_focus.get("auto_supported") is True:
                next_focus["auto_enabled"] = False
        result = self._command("set_camera_focus", command, status=200, body=next_focus)
        if not result.replayed:
            self.focus = deepcopy(result.body)
        return result

    def list_sessions(
        self,
        *,
        cursor: str | None,
        limit: int,
        take_id: str | None,
        api_version: str,
    ) -> dict[str, object]:
        self.last_list_query = (cursor, limit, take_id)
        if self.list_error is not None:
            raise self.list_error
        return deepcopy(SESSION_LIST_V4 if api_version == "v4" else SESSION_LIST)

    def open_manifest(self, session_id: str, api_version: str) -> MemoryRepresentation:
        if api_version not in {"v2", "v3", "v4"}:
            raise AssertionError("gateway 传递了未知 API 版本")
        if session_id != SESSION_ID:
            raise ProviderError("not_found", "会话不存在", status=404)
        if self.manifest_representation is not None:
            return self.manifest_representation  # type: ignore[return-value]
        return MemoryRepresentation(MANIFEST_BYTES)

    def retained_unsuccessful_outcome(self, session_id: str) -> object | None:
        return deepcopy(RETAINED_OUTCOME) if session_id == SESSION_ID else None

    def current_safe_swap_receipt(self) -> object | None:
        return deepcopy(self.safe_swap)

    def _command(
        self,
        operation: str,
        command: CaptureCommand,
        *,
        status: int,
        body: object | None,
    ) -> CaptureCommandResult:
        scope = (command.principal_id, operation, command.idempotency_key)
        previous = self.commands.get(scope)
        if previous:
            previous_body, result = previous
            if previous_body != command.canonical_body:
                raise ProviderError("idempotency_conflict", "幂等键已用于不同请求", status=409)
            return CaptureCommandResult(result.status, deepcopy(result.body), replayed=True)
        result = CaptureCommandResult(status, deepcopy(body), replayed=False)
        self.commands[scope] = (command.canonical_body, result)
        return result


class GatewayHttpTest(unittest.TestCase):
    def setUp(self) -> None:
        reader = Principal(
            "reader",
            permissions={
                "getDevice": None,
                "getCaptureStatus": None,
                "startCapture": None,
                "stopCapture": None,
                "listSessions": None,
                "getSession": {SESSION_ID},
                "getRetainedUnsuccessfulSessionOutcome": {SESSION_ID},
                "getCurrentSafeSwapReceipt": None,
                "getCameraFocus": None,
                "setCameraFocus": None,
                "getNetworkStatus": None,
                "scanNetworks": None,
                "createNetworkCredentialReference": None,
                "streamNetworkEvents": None,
                "applyNetworkDesiredState": None,
                "retryNetworkTransaction": None,
                "forgetNetworkClientProfile": None,
            },
        )
        denied = Principal("denied", permissions={})
        self.policy = SecurityPolicy.customer(
            tokens={"reader-token": reader, "denied-token": denied},
            allowed_origins={"http://127.0.0.1:4173"},
            csrf_token="browser-csrf-token",
        )
        self.server = create_gateway_server("127.0.0.1", 0, DeviceProvider(), security=self.policy)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.base = f"http://127.0.0.1:{self.server.server_port}"

    def tearDown(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=2)

    def test_expected_client_disconnect_does_not_emit_server_traceback(self) -> None:
        for error in (BrokenPipeError("closed"), ConnectionResetError("reset")):
            with self.subTest(error=type(error).__name__):
                stderr = StringIO()
                with redirect_stderr(stderr):
                    try:
                        raise error
                    except (BrokenPipeError, ConnectionResetError):
                        self.server.handle_error(object(), ("127.0.0.1", 12345))
                self.assertEqual(stderr.getvalue(), "")

    def request(
        self,
        path: str,
        *,
        token: str | None = None,
        headers: dict[str, str] | None = None,
        body: object | None = None,
        method: str | None = None,
    ) -> tuple[int, bytes, object]:
        request_headers = dict(headers or {})
        if token is not None:
            request_headers["Authorization"] = f"Bearer {token}"
        data = None
        if body is not None:
            data = json.dumps(body).encode()
            request_headers.setdefault("Content-Type", "application/json")
            if token is not None and request_headers.get("Origin") == self.base:
                request_headers.setdefault("X-CSRF-Token", "browser-csrf-token")
        request = Request(self.base + path, headers=request_headers, data=data, method=method)
        try:
            with urlopen(request, timeout=2) as response:
                return response.status, response.read(), response.headers
        except HTTPError as error:
            return error.code, error.read(), error.headers

    def raw_request(
        self,
        method: str,
        path: str,
        headers: tuple[tuple[str, str], ...],
        body: bytes = b"",
    ) -> tuple[int, bytes, object]:
        connection = http.client.HTTPConnection("127.0.0.1", self.server.server_port, timeout=2)
        try:
            connection.putrequest(method, path)
            for name, value in headers:
                connection.putheader(name, value)
            connection.endheaders(body)
            response = connection.getresponse()
            return response.status, response.read(), response.headers
        finally:
            connection.close()

    @staticmethod
    def read_sse_event(response: http.client.HTTPResponse) -> bytes:
        lines: list[bytes] = []
        while True:
            line = response.readline()
            if not line:
                raise AssertionError("SSE 连接在完整事件前关闭")
            lines.append(line)
            if line in {b"\n", b"\r\n"}:
                return b"".join(lines)

    @staticmethod
    def decode_sse_event(payload: bytes) -> tuple[int, dict[str, object]]:
        delivery_line = next(line for line in payload.splitlines() if line.startswith(b"id: "))
        data_line = next(line for line in payload.splitlines() if line.startswith(b"data: "))
        return int(delivery_line.removeprefix(b"id: ")), json.loads(
            data_line.removeprefix(b"data: ")
        )

    def test_embedded_web_is_anonymous_same_origin_and_closed_to_unknown_paths(self) -> None:
        for path, (name, content_type) in WEB_ASSET_EXPECTATIONS.items():
            with self.subTest(path=path):
                status, payload, headers = self.request(path)
                self.assertEqual(status, 200)
                self.assertEqual(payload, read_asset(name))
                self.assertEqual(headers["Content-Type"], content_type)
                self.assertEqual(headers["Content-Length"], str(len(payload)))
                for header_name, expected in WEB_SECURITY_HEADERS.items():
                    self.assertEqual(headers[header_name], expected)
                self.assertIn("connect-src 'self'", headers["Content-Security-Policy"])
                self.assertIn("frame-ancestors 'none'", headers["Content-Security-Policy"])

        for path in UNKNOWN_WEB_PATHS:
            with self.subTest(path=path):
                status, payload, headers = self.request(path)
                self.assertEqual(status, 404)
                self.assertEqual(headers["Content-Type"], "application/problem+json")
                self.assertEqual(json.loads(payload)["error"]["code"], "not_found")

        status, payload, _ = self.request("/api/v3/device")
        self.assertEqual(status, 401)
        self.assertEqual(json.loads(payload)["error"]["code"], "unauthorized")

    def test_embedded_web_head_matches_get_headers_without_body(self) -> None:
        for path in WEB_ASSET_EXPECTATIONS:
            with self.subTest(path=path):
                get_status, get_payload, get_headers = self.request(path)
                head_status, head_payload, head_headers = self.request(path, method="HEAD")

                self.assertEqual(get_status, 200)
                self.assertEqual(head_status, 200)
                self.assertEqual(head_payload, b"")
                for header_name in WEB_HEADER_NAMES:
                    self.assertEqual(head_headers[header_name], get_headers[header_name])
                self.assertEqual(head_headers["Content-Length"], str(len(get_payload)))

    def test_embedded_web_head_closed_set_returns_problem_without_body(self) -> None:
        for path in UNKNOWN_WEB_PATHS:
            with self.subTest(path=path):
                status, payload, headers = self.request(path, method="HEAD")
                self.assertEqual(status, 404)
                self.assertEqual(payload, b"")
                self.assertEqual(headers["Content-Type"], "application/problem+json")
                self.assertIsNotNone(headers["Content-Length"])

    def test_embedded_web_same_origin_command_requires_explicit_csrf(self) -> None:
        status, payload, headers = self.request(
            "/api/v3/capture/start",
            token="reader-token",
            headers={
                "Origin": self.base,
                "X-CSRF-Token": "browser-csrf-token",
                "Idempotency-Key": "embedded-web-start",
            },
            body={
                "schema": "ylx.capture-start.v2",
                "mode": "production",
                "display_name": "内嵌页面录制",
                "take": {"kind": "new"},
            },
        )

        self.assertEqual(status, 202)
        self.assertEqual(json.loads(payload), _capture_status_for_api("v3"))
        self.assertIsNone(headers["Access-Control-Allow-Origin"])

        status, payload, _ = self.request(
            "/api/v3/capture/stop",
            token="reader-token",
            headers={
                "Origin": self.base,
                "X-CSRF-Token": "browser-csrf-token",
                "Idempotency-Key": "embedded-web-stop",
            },
            body={"schema": "ylx.capture-stop.v2", "reason": "user"},
        )
        self.assertEqual(status, 204)
        self.assertEqual(payload, b"")

        status, payload, _ = self.request(
            "/api/v3/capture/start",
            token="reader-token",
            headers={
                "Origin": self.base,
                "X-CSRF-Token": "",
                "Idempotency-Key": "embedded-web-missing-csrf",
            },
            body={
                "schema": "ylx.capture-start.v2",
                "mode": "production",
                "take": {"kind": "new"},
            },
        )
        self.assertEqual(status, 403)
        self.assertEqual(json.loads(payload)["error"]["code"], "csrf_forbidden")

        status, payload, _ = self.request(
            "/api/v3/capture/start",
            token="reader-token",
            headers={
                "X-CSRF-Token": "browser-csrf-token",
                "Idempotency-Key": "embedded-web-missing-origin",
            },
            body={
                "schema": "ylx.capture-start.v2",
                "mode": "production",
                "take": {"kind": "new"},
            },
        )
        self.assertEqual(status, 403)
        self.assertEqual(json.loads(payload)["error"]["code"], "origin_forbidden")

    def test_same_origin_scheme_comes_only_from_trusted_server_configuration(self) -> None:
        status, _, _ = self.request(
            "/api/v3/capture/start",
            token="reader-token",
            headers={
                "Origin": self.base,
                "X-CSRF-Token": "browser-csrf-token",
                "X-Forwarded-Proto": "https",
                "Idempotency-Key": "trusted-http-scheme",
            },
            body={
                "schema": "ylx.capture-start.v2",
                "mode": "production",
                "take": {"kind": "new"},
            },
        )
        self.assertEqual(status, 202)

        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=2)
        self.server = create_gateway_server(
            "127.0.0.1",
            0,
            DeviceProvider(),
            security=self.policy,
            external_scheme="https",
        )
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.base = f"http://127.0.0.1:{self.server.server_port}"
        https_origin = f"https://127.0.0.1:{self.server.server_port}"
        http_origin = f"http://127.0.0.1:{self.server.server_port}"

        status, _, _ = self.request(
            "/api/v3/capture/start",
            token="reader-token",
            headers={
                "Origin": https_origin,
                "X-CSRF-Token": "browser-csrf-token",
                "X-Forwarded-Proto": "http",
                "Idempotency-Key": "trusted-https-scheme",
            },
            body={
                "schema": "ylx.capture-start.v2",
                "mode": "production",
                "take": {"kind": "new"},
            },
        )
        self.assertEqual(status, 202)

        status, payload, _ = self.request(
            "/api/v3/capture/start",
            token="reader-token",
            headers={
                "Origin": http_origin,
                "X-Forwarded-Proto": "https",
                "Idempotency-Key": "forged-forwarded-scheme",
            },
            body={
                "schema": "ylx.capture-start.v2",
                "mode": "production",
                "take": {"kind": "new"},
            },
        )
        self.assertEqual(status, 403)
        self.assertEqual(json.loads(payload)["error"]["code"], "origin_forbidden")

    def test_customer_authenticates_and_authorizes_each_versioned_device_resource(self) -> None:
        for path in ("/api/v2/device", "/api/v3/device", "/api/v4/device"):
            with self.subTest(path=path, credential="missing"):
                status, payload, headers = self.request(path)
                self.assertEqual(status, 401)
                self.assertEqual(headers["WWW-Authenticate"], "Bearer")
                self.assertEqual(json.loads(payload)["error"]["code"], "unauthorized")

            with self.subTest(path=path, credential="invalid"):
                status, payload, _ = self.request(path, token="wrong-token")
                self.assertEqual(status, 401)
                self.assertEqual(json.loads(payload)["error"]["code"], "unauthorized")

            with self.subTest(path=path, credential="forbidden"):
                status, payload, _ = self.request(path, token="denied-token")
                self.assertEqual(status, 403)
                self.assertEqual(json.loads(payload)["error"]["code"], "forbidden")

            with self.subTest(path=path, credential="allowed"):
                status, payload, headers = self.request(path, token="reader-token")
                self.assertEqual(status, 200)
                self.assertEqual(headers["Cache-Control"], "no-store")
                version = path.split("/")[2]
                device = json.loads(payload)
                self.assertEqual(device["schema"], f"ylx.device.{version}")
                self.assertEqual(device, _device_for_api(version))

        status, payload, _ = self.request("/api/v5/device", token="reader-token")
        self.assertEqual(status, 404)
        self.assertEqual(json.loads(payload)["error"]["code"], "not_found")

    def test_raw_live_imu_is_exposed_only_by_v4_device_and_status(self) -> None:
        status_with_raw = _active_capture_status()
        status_with_raw["snapshot"]["runtime"]["live_imu"] = deepcopy(RAW_LIVE_IMU)
        self.server.provider.status = status_with_raw
        self.server.provider.live_imu = deepcopy(RAW_LIVE_IMU)

        for version in ("v2", "v3"):
            with self.subTest(version=version, resource="device"):
                status, payload, _ = self.request(f"/api/{version}/device", token="reader-token")
                self.assertEqual(status, 200)
                device = json.loads(payload)
                self.assertNotIn("camera_focus", device["runtime"])
                self.assertIsNone(device["runtime"]["live_imu"])
                self.assertNotIn(b"raw_int16", payload)

            with self.subTest(version=version, resource="status"):
                status, payload, _ = self.request(
                    f"/api/{version}/capture/status", token="reader-token"
                )
                self.assertEqual(status, 200)
                snapshot = json.loads(payload)["snapshot"]
                self.assertNotIn("camera_focus", snapshot["runtime"])
                self.assertIsNone(snapshot["runtime"]["live_imu"])
                self.assertNotIn(b"raw_int16", payload)

        for resource in ("device", "capture/status"):
            with self.subTest(version="v4", resource=resource):
                status, payload, _ = self.request(f"/api/v4/{resource}", token="reader-token")
                self.assertEqual(status, 200)
                body = json.loads(payload)
                runtime = body["runtime"] if resource == "device" else body["snapshot"]["runtime"]
                self.assertEqual(runtime["live_imu"], RAW_LIVE_IMU)
                self.assertEqual(runtime["live_imu"]["clock"]["time_base"], "host_monotonic")

    def test_v4_active_recording_is_authoritative_when_live_imu_is_null(self) -> None:
        active = _active_capture_status()
        runtime = active["snapshot"]["runtime"]
        self.assertIsNone(runtime["live_imu"])
        self.server.provider.status = active

        status, payload, _ = self.request(
            "/api/v4/capture/status",
            token="reader-token",
        )
        self.assertEqual(status, 200)
        resource = json.loads(payload)
        self.assertEqual(resource["snapshot"]["device_state"], "recording")
        self.assertEqual(
            resource["snapshot"]["active_recording"]["recording_state"]["session_id"],
            SESSION_ID,
        )
        self.assertIsNone(resource["snapshot"]["runtime"]["live_imu"])

    def test_v4_capture_status_rejects_live_imu_without_matching_active_session(self) -> None:
        idle_with_live_imu = deepcopy(CAPTURE_STATUS)
        idle_with_live_imu["snapshot"]["runtime"]["live_imu"] = deepcopy(RAW_LIVE_IMU)
        self.server.provider.status = idle_with_live_imu

        status, payload, _ = self.request("/api/v4/capture/status", token="reader-token")
        self.assertEqual(status, 500)
        self.assertEqual(json.loads(payload)["error"]["code"], "invalid_source_state")

        mismatch = _active_capture_status(session_id=NEXT_SESSION_ID)
        mismatch["snapshot"]["runtime"]["live_imu"] = deepcopy(RAW_LIVE_IMU)
        self.server.provider.status = mismatch

        status, payload, _ = self.request("/api/v4/capture/status", token="reader-token")
        self.assertEqual(status, 500)
        self.assertEqual(json.loads(payload)["error"]["code"], "invalid_source_state")

        matching = _active_capture_status()
        matching["snapshot"]["runtime"]["live_imu"] = deepcopy(RAW_LIVE_IMU)
        self.server.provider.status = matching

        status, payload, _ = self.request("/api/v4/capture/status", token="reader-token")
        self.assertEqual(status, 200)
        self.assertEqual(json.loads(payload), _capture_status_for_api("v4", matching))

    def test_repeated_authorization_headers_fail_closed_regardless_of_order(self) -> None:
        for values in (
            ("Bearer reader-token", "Bearer wrong-token"),
            ("Bearer wrong-token", "Bearer reader-token"),
        ):
            with self.subTest(values=values):
                connection = http.client.HTTPConnection(
                    "127.0.0.1", self.server.server_port, timeout=2
                )
                try:
                    connection.putrequest("GET", "/api/v3/device")
                    for value in values:
                        connection.putheader("Authorization", value)
                    connection.endheaders()
                    response = connection.getresponse()
                    payload = response.read()
                finally:
                    connection.close()

                self.assertEqual(response.status, 401)
                self.assertEqual(response.headers["WWW-Authenticate"], "Bearer")
                self.assertEqual(json.loads(payload)["error"]["code"], "unauthorized")

    def test_repeated_origin_is_forbidden_without_cors_echo(self) -> None:
        status, payload, headers = self.raw_request(
            "GET",
            "/api/v3/device",
            (
                ("Authorization", "Bearer reader-token"),
                ("Origin", "http://127.0.0.1:4173"),
                ("Origin", "http://127.0.0.1:4173"),
            ),
        )

        self.assertEqual(status, 403)
        self.assertEqual(json.loads(payload)["error"]["code"], "origin_forbidden")
        self.assertIsNone(headers["Access-Control-Allow-Origin"])

    def test_repeated_command_control_headers_fail_closed(self) -> None:
        body = json.dumps(
            {
                "schema": "ylx.capture-start.v2",
                "mode": "production",
                "take": {"kind": "new"},
            },
            separators=(",", ":"),
        ).encode()
        common = (
            ("Authorization", "Bearer reader-token"),
            ("Origin", "http://127.0.0.1:4173"),
            ("X-CSRF-Token", "browser-csrf-token"),
            ("Idempotency-Key", "raw-header-test"),
            ("Content-Type", "application/json"),
            ("Content-Length", str(len(body))),
        )
        cases = (
            ("Content-Type", "application/json", 400, "invalid_request"),
            ("Content-Length", str(len(body)), 400, "invalid_request"),
            ("Idempotency-Key", "raw-header-test", 400, "invalid_request"),
            ("X-CSRF-Token", "browser-csrf-token", 403, "csrf_forbidden"),
        )
        for name, value, expected_status, expected_code in cases:
            with self.subTest(header=name):
                status, payload, response_headers = self.raw_request(
                    "POST",
                    "/api/v3/capture/start",
                    (*common, (name, value)),
                    body,
                )

                self.assertEqual(status, expected_status)
                self.assertEqual(json.loads(payload)["error"]["code"], expected_code)
                self.assertEqual(response_headers["Connection"], "close")

    def test_ambiguous_or_illegal_http_request_framing_fails_closed(self) -> None:
        body = b"{}"
        common = (
            ("Authorization", "Bearer reader-token"),
            ("Origin", self.base),
            ("Idempotency-Key", "framing-test"),
            ("Content-Type", "application/json"),
        )
        cases = (
            (("Transfer-Encoding", "chunked"),),
            (("Transfer-Encoding", "identity"), ("Content-Length", str(len(body)))),
            (("Content-Length", "02"),),
            (("Content-Length", "+2"),),
            (("Content-Length", "2, 2"),),
            (("Content-Length", "65537"),),
        )
        for extra_headers in cases:
            with self.subTest(extra_headers=extra_headers):
                status, payload, response_headers = self.raw_request(
                    "POST",
                    "/api/v3/capture/start",
                    (*common, *extra_headers),
                    body,
                )
                self.assertEqual(status, 400)
                self.assertEqual(json.loads(payload)["error"]["code"], "invalid_request")
                self.assertEqual(response_headers["Connection"], "close")
                self.assertEqual(self.server.provider.commands, {})

    def test_bodyless_methods_and_unread_unknown_bodies_fail_closed(self) -> None:
        for method, path, expected_status in (
            ("GET", "/api/v3/device", 400),
            ("HEAD", "/api/v3/device", 400),
            ("OPTIONS", "/api/v3/device", 400),
            ("POST", "/api/v3/unknown", 404),
            ("PUT", "/api/v3/device", 405),
        ):
            with self.subTest(method=method, path=path):
                status, payload, response_headers = self.raw_request(
                    method,
                    path,
                    (
                        ("Authorization", "Bearer reader-token"),
                        ("Content-Length", "2"),
                        ("Content-Type", "application/json"),
                    ),
                    b"{}",
                )
                self.assertEqual(status, expected_status)
                if method != "HEAD":
                    self.assertEqual(json.loads(payload)["schema"], "ylx.api-error.v2")
                self.assertEqual(response_headers["Connection"], "close")

    def test_lab_profile_origin_and_audit_are_enforced_before_the_handler(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=2)
        audit: list[AuditEvent] = []
        self.server = create_gateway_server(
            "127.0.0.1",
            0,
            DeviceProvider(),
            security=SecurityPolicy.lab(
                allowed_operations={"getDevice"},
                allowed_origins={"http://127.0.0.1:4173"},
            ),
            audit_sink=audit.append,
        )
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.base = f"http://127.0.0.1:{self.server.server_port}"

        status, payload, _ = self.request(
            "/api/v3/device", headers={"Origin": "https://example.invalid"}
        )
        self.assertEqual(status, 403)
        self.assertEqual(json.loads(payload)["error"]["code"], "origin_forbidden")

        status, payload, headers = self.request(
            "/api/v3/device", headers={"Origin": "http://127.0.0.1:4173"}
        )
        self.assertEqual(status, 200)
        self.assertEqual(json.loads(payload)["security_profile"], "lab")
        self.assertEqual(headers["Access-Control-Allow-Origin"], "http://127.0.0.1:4173")
        self.assertEqual(headers["Vary"], "Origin")

        status, payload, _ = self.request(
            "/api/v4/device",
            headers={"Origin": "http://127.0.0.1:4173"},
        )
        self.assertEqual(status, 200)
        capabilities = json.loads(payload)["capabilities"]
        self.assertEqual(
            {
                name: capabilities[name]
                for name in (
                    "session_list",
                    "session_detail",
                    "artifact_download",
                    "capture_status",
                    "session_deletion",
                )
            },
            {
                "session_list": True,
                "session_detail": True,
                "artifact_download": True,
                "capture_status": True,
                "session_deletion": False,
            },
        )

        status, payload, _ = self.request(
            f"/api/v4/sessions/{SESSION_ID}",
            headers={"Origin": "http://127.0.0.1:4173"},
            method="DELETE",
        )
        self.assertEqual(status, 405)
        self.assertEqual(json.loads(payload)["error"]["code"], "method_not_allowed")

        self.assertEqual(
            [event.outcome for event in audit],
            ["origin_forbidden", "allowed", "allowed"],
        )
        self.assertEqual(audit[-1].principal_id, "isolated-lab-device")
        self.assertEqual(audit[-1].operation_id, "getDevice")
        self.assertNotIn("token", repr(audit))

    def test_status_is_exact_and_commands_are_principal_scoped_and_csrf_protected(self) -> None:
        for version in ("v2", "v3", "v4"):
            status, payload, _ = self.request(
                f"/api/{version}/capture/status", token="reader-token"
            )
            self.assertEqual(status, 200)
            self.assertEqual(
                payload,
                json.dumps(_capture_status_for_api(version), separators=(",", ":")).encode(),
            )

        start = {
            "schema": "ylx.capture-start.v2",
            "mode": "production",
            "take": {"kind": "new"},
        }
        mutation_headers = {
            "Origin": "http://127.0.0.1:4173",
            "X-CSRF-Token": "browser-csrf-token",
            "Idempotency-Key": "same-visible-key",
        }
        status, first, headers = self.request(
            "/api/v3/capture/start",
            token="reader-token",
            headers=mutation_headers,
            body=start,
        )
        self.assertEqual(status, 202)
        self.assertIsNone(headers["Idempotency-Replayed"])
        self.assertEqual(json.loads(first), _capture_status_for_api("v3"))

        status, repeated, headers = self.request(
            "/api/v3/capture/start",
            token="reader-token",
            headers=mutation_headers,
            body=start,
        )
        self.assertEqual(status, 202)
        self.assertEqual(repeated, first)
        self.assertEqual(headers["Idempotency-Replayed"], "true")

        changed = {**start, "display_name": "另一个请求"}
        status, payload, _ = self.request(
            "/api/v3/capture/start",
            token="reader-token",
            headers=mutation_headers,
            body=changed,
        )
        self.assertEqual(status, 409)
        self.assertEqual(json.loads(payload)["error"]["code"], "idempotency_conflict")

        missing_csrf = {
            key: value for key, value in mutation_headers.items() if key != "X-CSRF-Token"
        }
        status, payload, _ = self.request(
            "/api/v3/capture/stop",
            token="reader-token",
            headers=missing_csrf,
            body={"schema": "ylx.capture-stop.v2", "reason": "user"},
        )
        self.assertEqual(status, 403)
        self.assertEqual(json.loads(payload)["error"]["code"], "csrf_forbidden")

        status, payload, _ = self.request(
            "/api/v3/capture/stop",
            token="reader-token",
            headers={**mutation_headers, "Idempotency-Key": "stop-on-idle"},
            body={"schema": "ylx.capture-stop.v2", "reason": "user"},
        )
        self.assertEqual(status, 204)
        self.assertEqual(payload, b"")

        status, payload, _ = self.request(
            "/api/v4/capture/start",
            token="reader-token",
            headers={**mutation_headers, "Idempotency-Key": "v4-start"},
            body=start,
        )
        self.assertEqual(status, 202)
        self.assertEqual(json.loads(payload), _capture_status_for_api("v4"))

        status, payload, _ = self.request(
            "/api/v4/capture/stop",
            token="reader-token",
            headers={**mutation_headers, "Idempotency-Key": "v4-stop"},
            body={"schema": "ylx.capture-stop.v2", "reason": "user"},
        )
        self.assertEqual(status, 204)
        self.assertEqual(payload, b"")

    def test_camera_focus_is_v4_only_readable_settable_and_projected_to_runtime(self) -> None:
        self.server.provider.focus = deepcopy(CAMERA_FOCUS_STATUS)

        for version in ("v2", "v3"):
            with self.subTest(version=version, operation="read"):
                status, payload, _ = self.request(
                    f"/api/{version}/camera/focus",
                    token="reader-token",
                )
                self.assertEqual(status, 404)

            with self.subTest(version=version, operation="write"):
                status, payload, _ = self.request(
                    f"/api/{version}/camera/focus",
                    token="reader-token",
                    headers={"Origin": self.base, "Idempotency-Key": f"{version}-focus"},
                    body={"schema": "ylx.camera-focus-set.v1", "value": 64},
                )
                self.assertEqual(status, 404)

        status, payload, _ = self.request("/api/v4/camera/focus", token="reader-token")
        self.assertEqual(status, 200)
        self.assertEqual(json.loads(payload), CAMERA_FOCUS_STATUS)

        headers = {
            "Origin": self.base,
            "Idempotency-Key": "focus-64",
        }
        body = {
            "schema": "ylx.camera-focus-set.v1",
            "value": 64,
            "auto_enabled": False,
        }
        status, first, response_headers = self.request(
            "/api/v4/camera/focus",
            token="reader-token",
            headers=headers,
            body=body,
        )
        self.assertEqual(status, 200)
        updated = {**CAMERA_FOCUS_STATUS, "value": 64, "auto_enabled": False}
        self.assertEqual(json.loads(first), updated)
        self.assertIsNone(response_headers["Idempotency-Replayed"])

        status, repeated, response_headers = self.request(
            "/api/v4/camera/focus",
            token="reader-token",
            headers=headers,
            body=body,
        )
        self.assertEqual(status, 200)
        self.assertEqual(repeated, first)
        self.assertEqual(response_headers["Idempotency-Replayed"], "true")

        status, payload, _ = self.request("/api/v4/camera/focus", token="reader-token")
        self.assertEqual(status, 200)
        self.assertEqual(json.loads(payload), updated)

        for version in ("v2", "v3"):
            with self.subTest(version=version, operation="runtime-redacted"):
                status, payload, _ = self.request(f"/api/{version}/device", token="reader-token")
                self.assertEqual(status, 200)
                self.assertNotIn("camera_focus", json.loads(payload)["runtime"])

                status, payload, _ = self.request(
                    f"/api/{version}/capture/status",
                    token="reader-token",
                )
                self.assertEqual(status, 200)
                self.assertNotIn("camera_focus", json.loads(payload)["snapshot"]["runtime"])

        status, payload, _ = self.request("/api/v4/device", token="reader-token")
        self.assertEqual(status, 200)
        self.assertEqual(json.loads(payload)["runtime"]["camera_focus"], updated)

        status, payload, _ = self.request("/api/v4/capture/status", token="reader-token")
        self.assertEqual(status, 200)
        self.assertEqual(json.loads(payload)["snapshot"]["runtime"]["camera_focus"], updated)

        status, payload, _ = self.request(
            "/api/v4/camera/focus",
            token="reader-token",
            headers=headers,
            body={**body, "value": 65},
        )
        self.assertEqual(status, 409)
        self.assertEqual(json.loads(payload)["error"]["code"], "idempotency_conflict")

        status, payload, _ = self.request(
            "/api/v4/camera/focus",
            token="reader-token",
            headers={"Origin": self.base, "Idempotency-Key": "invalid-focus"},
            body={"schema": "ylx.camera-focus-set.v1", "value": True},
        )
        self.assertEqual(status, 400)
        self.assertEqual(json.loads(payload)["error"]["code"], "invalid_request")

    def test_disconnected_camera_errors_are_typed_for_capture_and_focus(self) -> None:
        self.server.provider.camera_error = ProviderError(
            "camera_not_connected",
            "相机未接入",
            status=503,
            retryable=True,
        )
        requests = (
            ("/api/v4/camera/focus", None, None),
            (
                "/api/v4/camera/focus",
                {"Origin": self.base, "Idempotency-Key": "camera-disconnected-focus"},
                {"schema": "ylx.camera-focus-set.v1", "value": 64},
            ),
            (
                "/api/v4/capture/start",
                {"Origin": self.base, "Idempotency-Key": "camera-disconnected-capture"},
                {
                    "schema": "ylx.capture-start.v2",
                    "mode": "production",
                    "take": {"kind": "new"},
                },
            ),
        )

        for path, headers, body in requests:
            with self.subTest(path=path, method="GET" if body is None else "POST"):
                status, payload, response_headers = self.request(
                    path,
                    token="reader-token",
                    headers=headers,
                    body=body,
                )
                self.assertEqual(status, 503)
                self.assertEqual(response_headers["YLX-Error-Code"], "camera_not_connected")
                error = json.loads(payload)["error"]
                self.assertEqual(error["code"], "camera_not_connected")
                self.assertTrue(error["retryable"])

    def test_calibration_unavailable_error_is_typed_for_capture(self) -> None:
        self.server.provider.camera_error = ProviderError(
            "calibration_unavailable",
            "当前采集源不能生成标定所需的分眼 H.264 分段会话",
            status=503,
            retryable=False,
            details={"reason": "capture_source_unsupported"},
        )
        status, payload, response_headers = self.request(
            "/api/v4/capture/start",
            token="reader-token",
            headers={"Origin": self.base, "Idempotency-Key": "calibration-unavailable"},
            body={
                "schema": "ylx.capture-start.v2",
                "mode": "calibration",
                "take": {"kind": "new"},
            },
        )
        self.assertEqual(status, 503)
        self.assertEqual(response_headers["YLX-Error-Code"], "calibration_unavailable")
        self.assertEqual(
            json.loads(payload)["error"],
            {
                "code": "calibration_unavailable",
                "message": "当前采集源不能生成标定所需的分眼 H.264 分段会话",
                "request_id": json.loads(payload)["error"]["request_id"],
                "retryable": False,
                "details": {"reason": "capture_source_unsupported"},
            },
        )

    def test_network_status_and_events_are_v4_only_and_strictly_validated(self) -> None:
        for version in ("v2", "v3"):
            with self.subTest(version=version):
                status, _, _ = self.request(f"/api/{version}/network", token="reader-token")
                self.assertEqual(status, 404)
                status, _, _ = self.request(f"/api/{version}/network/events", token="reader-token")
                self.assertEqual(status, 404)

        status, payload, headers = self.request("/api/v4/network", token="reader-token")
        self.assertEqual(status, 200)
        self.assertEqual(json.loads(payload), NETWORK_STATUS)
        self.assertEqual(headers["Cache-Control"], "no-store")

        status, payload, _ = self.request(
            "/api/v4/network",
            token="reader-token",
            body={"schema": "ylx.network-apply-request.v1", "desired": NETWORK_STATUS["desired"]},
        )
        self.assertEqual(status, 404)
        self.assertEqual(json.loads(payload)["error"]["code"], "not_found")

        self.server.provider.network = {**NETWORK_STATUS, "psk": "must-never-leak"}
        status, payload, _ = self.request("/api/v4/network", token="reader-token")
        self.assertEqual(status, 500)
        self.assertEqual(json.loads(payload)["error"]["code"], "invalid_source_state")
        self.server.provider.network = deepcopy(NETWORK_STATUS)

        connection = http.client.HTTPConnection("127.0.0.1", self.server.server_port, timeout=2)
        try:
            connection.request(
                "GET",
                "/api/v4/network/events",
                headers={"Authorization": "Bearer reader-token"},
            )
            response = connection.getresponse()
            self.assertEqual(response.status, 200)
            self.assertEqual(response.headers["Content-Type"], "text/event-stream")
            self.assertEqual(response.headers["Connection"], "keep-alive")
            self.assertIsNone(response.headers["Content-Length"])

            payload = self.read_sse_event(response)
            self.assertIn(b"event: snapshot\n", payload)
            initial_delivery_id, event = self.decode_sse_event(payload)
            self.assertEqual(event["schema"], "ylx.network-event.v1")
            self.assertEqual(event["sse_delivery_id"], str(initial_delivery_id))
            self.assertEqual(event["type"], "snapshot")
            self.assertIsNone(event["transaction_id"])
            self.assertEqual(event["data"], NETWORK_STATUS)

            changed = deepcopy(NETWORK_STATUS)
            changed["source_revision"] = 13
            changed["observed_at"] = "2026-08-23T12:58:01+08:00"
            self.server.provider.network = changed
            payload = self.read_sse_event(response)
            changed_delivery_id, changed_event = self.decode_sse_event(payload)
            self.assertEqual(changed_delivery_id, initial_delivery_id + 1)
            self.assertEqual(changed_event["sse_delivery_id"], str(changed_delivery_id))
            self.assertEqual(changed_event["data"], changed)
        finally:
            connection.close()

        replay_connection = http.client.HTTPConnection(
            "127.0.0.1", self.server.server_port, timeout=2
        )
        try:
            replay_connection.request(
                "GET",
                "/api/v4/network/events",
                headers={
                    "Authorization": "Bearer reader-token",
                    "Last-Event-ID": str(initial_delivery_id),
                },
            )
            replay_response = replay_connection.getresponse()
            self.assertEqual(replay_response.status, 200)
            replayed = self.read_sse_event(replay_response)
            replayed_delivery_id, _ = self.decode_sse_event(replayed)
            self.assertEqual(replayed_delivery_id, changed_delivery_id)
        finally:
            replay_connection.close()

        latest_connection = http.client.HTTPConnection(
            "127.0.0.1", self.server.server_port, timeout=2
        )
        try:
            latest_connection.request(
                "GET",
                "/api/v4/network/events",
                headers={
                    "Authorization": "Bearer reader-token",
                    "Last-Event-ID": str(changed_delivery_id),
                },
            )
            latest_response = latest_connection.getresponse()
            self.assertEqual(latest_response.status, 200)
            next_status = deepcopy(changed)
            next_status["source_revision"] = 14
            next_status["observed_at"] = "2026-08-23T12:58:02+08:00"
            self.server.provider.network = next_status
            next_payload = self.read_sse_event(latest_response)
            next_delivery_id, next_event = self.decode_sse_event(next_payload)
            self.assertEqual(next_delivery_id, changed_delivery_id + 1)
            self.assertEqual(next_event["data"], next_status)
        finally:
            latest_connection.close()

        status, payload, _ = self.request(
            "/api/v4/network/events",
            token="reader-token",
            headers={"Last-Event-ID": "bad-cursor"},
        )
        self.assertEqual(status, 400)
        self.assertEqual(json.loads(payload)["error"]["code"], "invalid_request")

    def test_network_sse_rejects_revision_regressions_and_emits_projection_changes(self) -> None:
        buffer = _NetworkEventReplayBuffer(initial_delivery_id=100)
        initial = buffer.replay(None, NETWORK_STATUS)[0]
        advanced = deepcopy(NETWORK_STATUS)
        advanced["source_revision"] = 13
        advanced["observed_at"] = "2026-08-23T12:58:01+08:00"
        buffer.observe(advanced)

        regressed = deepcopy(NETWORK_STATUS)
        regressed["source_revision"] = 11
        regressed["observed_at"] = "2026-08-23T12:58:02+08:00"
        delivered = buffer.events_after(initial.delivery_id, regressed)
        self.assertEqual([event.source_event["source_revision"] for event in delivered], [13])

        observed_projection = deepcopy(advanced)
        observed_projection["observed_at"] = "2026-08-23T12:58:03+08:00"
        observed_projection["observed"]["wifi_client"]["addresses"] = ["192.0.2.37/24"]
        observed_events = buffer.events_after(delivered[-1].delivery_id, observed_projection)
        self.assertEqual(len(observed_events), 1)
        self.assertEqual(observed_events[0].source_event["type"], "snapshot")
        self.assertEqual(observed_events[0].source_event["data"], observed_projection)

        timestamp_only = deepcopy(observed_projection)
        timestamp_only["observed_at"] = "2026-08-23T12:58:04+08:00"
        self.assertFalse(buffer.events_after(observed_events[-1].delivery_id, timestamp_only))

        projected = deepcopy(timestamp_only)
        projected["mutation_capability"] = {
            "enabled": False,
            "disabled_reason": "capture_active",
            "rescue_ap_required": True,
            "rescue_ap_validated": True,
            "operations": ["apply", "retry", "forget"],
        }
        projected_events = buffer.events_after(observed_events[-1].delivery_id, projected)
        self.assertEqual(len(projected_events), 1)
        self.assertEqual(projected_events[0].source_event["type"], "snapshot")
        self.assertEqual(projected_events[0].source_event["data"], projected)

    def test_network_sse_restart_forces_resync_even_when_delivery_id_would_collide(self) -> None:
        before_restart = _NetworkEventReplayBuffer(initial_delivery_id=100)
        old_event = before_restart.replay(None, NETWORK_STATUS)[0]
        after_restart = _NetworkEventReplayBuffer(initial_delivery_id=old_event.delivery_id)

        resync = after_restart.replay(old_event.delivery_id, NETWORK_STATUS)

        self.assertEqual(len(resync), 1)
        self.assertEqual(resync[0].source_event["type"], "snapshot")
        self.assertGreater(resync[0].delivery_id, old_event.delivery_id)

    def test_network_scan_and_transient_credentials_follow_v4_security_contract(self) -> None:
        status, payload, headers = self.request(
            "/api/v4/network/scan",
            token="reader-token",
        )
        self.assertEqual(status, 200)
        self.assertEqual(json.loads(payload), NETWORK_SCAN)
        self.assertEqual(headers["Cache-Control"], "no-store")

        status, _, _ = self.request("/api/v3/network/scan", token="reader-token")
        self.assertEqual(status, 404)

        passphrase = "one-request-secret"
        status, payload, headers = self.request(
            "/api/v4/network/credentials",
            token="reader-token",
            headers={"Origin": self.base},
            body={
                "schema": "ylx.network-credential-request.v1",
                "passphrase": passphrase,
            },
        )
        self.assertEqual(status, 201)
        self.assertEqual(json.loads(payload), NETWORK_CREDENTIAL_RECEIPT)
        self.assertEqual(headers["Cache-Control"], "no-store")
        self.assertNotIn(passphrase.encode(), payload)
        self.assertEqual(len(self.server.provider.network_credentials), 1)
        self.assertEqual(self.server.provider.network_credentials[0].principal_id, "reader")
        self.assertEqual(self.server.provider.network_credentials[0].passphrase, passphrase)

        self.server.provider.network_credentials.clear()
        status, payload, _ = self.request(
            "/api/v4/network/credentials",
            token="reader-token",
            headers={"Origin": self.base},
            body={
                "schema": "ylx.network-credential-request.v1",
                "passphrase": "short",
            },
        )
        self.assertEqual(status, 400)
        self.assertEqual(json.loads(payload)["error"]["code"], "invalid_request")
        self.assertEqual(self.server.provider.network_credentials, [])

        self.server.provider.network_scan = {**NETWORK_SCAN, "password": "must-not-leak"}
        status, payload, _ = self.request("/api/v4/network/scan", token="reader-token")
        self.assertEqual(status, 500)
        self.assertEqual(json.loads(payload)["error"]["code"], "invalid_source_state")

    def test_network_read_errors_are_closed_and_unknown_provider_errors_do_not_leak(self) -> None:
        cases = (
            (
                "/api/v4/network",
                "network_status_error",
                "network_status_unavailable",
                "网络状态暂时不可用",
            ),
            (
                "/api/v4/network/events",
                "network_status_error",
                "network_status_unavailable",
                "网络状态暂时不可用",
            ),
            (
                "/api/v4/network/scan",
                "network_scan_error",
                "network_scan_unavailable",
                "无线网络扫描暂时不可用",
            ),
        )
        for path, attribute, code, message in cases:
            with self.subTest(path=path, kind="valid"):
                setattr(
                    self.server.provider,
                    attribute,
                    ProviderError(code, message, status=503, retryable=True),
                )
                status, payload, headers = self.request(path, token="reader-token")
                self.assertEqual(status, 503)
                self.assertEqual(json.loads(payload)["error"]["code"], code)
                self.assertEqual(headers["YLX-Error-Code"], code)

            with self.subTest(path=path, kind="unknown"):
                setattr(
                    self.server.provider,
                    attribute,
                    ProviderError(
                        "unexpected_network_error",
                        "must-not-leak-provider-detail",
                        status=503,
                        retryable=True,
                    ),
                )
                status, payload, headers = self.request(path, token="reader-token")
                self.assertEqual(status, 500)
                self.assertEqual(json.loads(payload)["error"]["code"], "invalid_source_state")
                self.assertNotIn(b"must-not-leak-provider-detail", payload)
                self.assertIsNone(headers["YLX-Error-Code"])

            setattr(self.server.provider, attribute, None)

    def test_network_mutation_routes_fail_closed_before_side_effects(self) -> None:
        apply_body = {
            "schema": "ylx.network-apply-request.v1",
            "desired": {
                "mode": "wifi-client",
                "wifi_client": {
                    "ssid": "studio-wifi",
                    "security": "wpa2-personal",
                    "credential_ref": "cred-setup-token-001",
                },
                "ethernet": None,
            },
        }
        headers = {"Origin": self.base, "Idempotency-Key": "network-apply-1"}

        status, payload, response_headers = self.request(
            "/api/v4/network/apply",
            token="reader-token",
            headers=headers,
            body=apply_body,
        )
        self.assertEqual(status, 503)
        unavailable = json.loads(payload)["error"]
        self.assertEqual(unavailable["code"], "network_mutation_unavailable")
        self.assertTrue(unavailable["retryable"])
        self.assertEqual(unavailable["details"], {"reason": "not_enabled"})
        self.assertEqual(response_headers["YLX-Error-Code"], "network_mutation_unavailable")
        self.assertEqual(len(self.server.provider.network_commands), 1)

        self.server.provider.network_commands.clear()
        for path, body in (
            (
                "/api/v4/network/retry",
                {
                    "schema": "ylx.network-retry-request.v1",
                    "transaction_id": NETWORK_TRANSACTION_ID,
                },
            ),
            ("/api/v4/network/forget", {"schema": "ylx.network-forget-request.v1"}),
        ):
            with self.subTest(path=path):
                status, payload, _ = self.request(
                    path,
                    token="reader-token",
                    headers={"Origin": self.base, "Idempotency-Key": f"{path.rsplit('/', 1)[1]}-1"},
                    body=body,
                )
                self.assertEqual(status, 503)
                self.assertEqual(
                    json.loads(payload)["error"]["code"], "network_mutation_unavailable"
                )
        self.assertEqual(
            [operation for operation, _ in self.server.provider.network_commands],
            ["retry", "forget"],
        )

        self.server.provider.network_commands.clear()
        self.server.provider.network_errors["apply"] = ProviderError(
            "network_mutation_unavailable",
            "malformed typed error",
            status=503,
            retryable=True,
        )
        status, payload, response_headers = self.request(
            "/api/v4/network/apply",
            token="reader-token",
            headers={"Origin": self.base, "Idempotency-Key": "malformed-network-error"},
            body=apply_body,
        )
        self.assertEqual(status, 500)
        self.assertEqual(json.loads(payload)["error"]["code"], "invalid_source_state")
        self.assertIsNone(response_headers["YLX-Error-Code"])
        self.server.provider.network_errors.clear()

        self.server.provider.network_commands.clear()
        status, payload, _ = self.request(
            "/api/v4/network/apply",
            token="reader-token",
            headers={"Origin": self.base},
            body=apply_body,
        )
        self.assertEqual(status, 400)
        self.assertEqual(json.loads(payload)["error"]["code"], "invalid_request")
        self.assertEqual(self.server.provider.network_commands, [])

        status, payload, _ = self.request(
            "/api/v4/network/apply",
            token="reader-token",
            headers={"Origin": self.base, "Idempotency-Key": "network-secret"},
            body={
                "schema": "ylx.network-apply-request.v1",
                "desired": {
                    "mode": "wifi-client",
                    "wifi_client": {
                        "ssid": "studio-wifi",
                        "security": "wpa2-personal",
                        "credential_ref": "cred-setup-token-001",
                        "psk": "must-not-pass",
                    },
                    "ethernet": None,
                },
            },
        )
        self.assertEqual(status, 400)
        self.assertEqual(json.loads(payload)["error"]["code"], "invalid_request")
        self.assertEqual(self.server.provider.network_commands, [])

        status, payload, _ = self.request(
            "/api/v4/network/apply",
            token="denied-token",
            headers=headers,
            body=apply_body,
        )
        self.assertEqual(status, 403)
        self.assertEqual(json.loads(payload)["error"]["code"], "forbidden")
        self.assertEqual(self.server.provider.network_commands, [])

    def test_lab_profile_exposes_controller_capability_and_allows_network_mutations(self) -> None:
        lab_provider = DeviceProvider()
        lab_provider.network["mutation_capability"] = {
            **lab_provider.network["mutation_capability"],
            "enabled": True,
            "disabled_reason": None,
        }
        for operation in ("apply", "retry", "forget"):
            receipt = deepcopy(NETWORK_RECEIPT)
            receipt["transaction"]["operation"] = operation
            if operation == "forget":
                receipt["transaction"]["desired"] = {
                    "mode": "hotspot",
                    "wifi_client": None,
                    "ethernet": None,
                }
            lab_provider.network_results[operation] = NetworkCommandResult(202, receipt)

        allowed_operations = {
            "getNetworkStatus",
            "scanNetworks",
            "streamNetworkEvents",
            "createNetworkCredentialReference",
            "applyNetworkDesiredState",
            "retryNetworkTransaction",
            "forgetNetworkClientProfile",
        }
        lab_server = create_gateway_server(
            "127.0.0.1",
            0,
            lab_provider,
            security=SecurityPolicy.lab(allowed_operations=allowed_operations),
        )
        lab_thread = threading.Thread(target=lab_server.serve_forever, daemon=True)
        lab_thread.start()
        lab_base = f"http://127.0.0.1:{lab_server.server_port}"
        try:
            connection = http.client.HTTPConnection("127.0.0.1", lab_server.server_port, timeout=2)
            try:
                connection.request("GET", "/api/v4/network")
                response = connection.getresponse()
                self.assertEqual(response.status, 200)
                lab_status = json.loads(response.read())
                self.assertTrue(lab_status["mutation_capability"]["enabled"])
                self.assertIsNone(lab_status["mutation_capability"]["disabled_reason"])

                credential_body = json.dumps(
                    {
                        "schema": "ylx.network-credential-request.v1",
                        "passphrase": "trusted-lab-passphrase",
                    }
                ).encode()
                connection.request(
                    "POST",
                    "/api/v4/network/credentials",
                    body=credential_body,
                    headers={"Origin": lab_base, "Content-Type": "application/json"},
                )
                response = connection.getresponse()
                self.assertEqual(response.status, 201)
                self.assertEqual(json.loads(response.read()), NETWORK_CREDENTIAL_RECEIPT)

                requests = (
                    (
                        "apply",
                        {
                            "schema": "ylx.network-apply-request.v1",
                            "desired": {
                                "mode": "wifi-client",
                                "wifi_client": {
                                    "ssid": "studio-wifi",
                                    "security": "wpa2-personal",
                                    "credential_ref": "cred-setup-token-001",
                                },
                                "ethernet": None,
                            },
                        },
                    ),
                    (
                        "retry",
                        {
                            "schema": "ylx.network-retry-request.v1",
                            "transaction_id": NETWORK_TRANSACTION_ID,
                        },
                    ),
                    ("forget", {"schema": "ylx.network-forget-request.v1"}),
                )
                for operation, request_body in requests:
                    connection.request(
                        "POST",
                        f"/api/v4/network/{operation}",
                        body=json.dumps(request_body).encode(),
                        headers={
                            "Origin": lab_base,
                            "Content-Type": "application/json",
                            "Idempotency-Key": f"lab-network-{operation}",
                        },
                    )
                    response = connection.getresponse()
                    self.assertEqual(response.status, 202)
                    self.assertEqual(
                        json.loads(response.read())["transaction"]["operation"], operation
                    )

                self.assertEqual(len(lab_provider.network_credentials), 1)
                self.assertEqual(
                    lab_provider.network_credentials[0].principal_id,
                    "isolated-lab-device",
                )
                self.assertEqual(
                    [operation for operation, _ in lab_provider.network_commands],
                    ["apply", "retry", "forget"],
                )
                self.assertTrue(
                    all(
                        command.principal_id == "isolated-lab-device"
                        for _, command in lab_provider.network_commands
                    )
                )
            finally:
                connection.close()
        finally:
            lab_server.shutdown()
            lab_server.server_close()
            lab_thread.join(timeout=2)

    def test_network_mutation_accepts_valid_provider_receipts_and_replay_header(self) -> None:
        self.server.provider.network_results["apply"] = NetworkCommandResult(
            202, NETWORK_RECEIPT, replayed=True
        )

        status, payload, headers = self.request(
            "/api/v4/network/apply",
            token="reader-token",
            headers={"Origin": self.base, "Idempotency-Key": "network-accepted"},
            body={
                "schema": "ylx.network-apply-request.v1",
                "desired": {
                    "mode": "wifi-client",
                    "wifi_client": {
                        "ssid": "studio-wifi",
                        "security": "wpa2-personal",
                        "credential_ref": "cred-setup-token-001",
                    },
                    "ethernet": None,
                },
            },
        )

        self.assertEqual(status, 202)
        self.assertEqual(json.loads(payload), NETWORK_RECEIPT)
        self.assertEqual(headers["Idempotency-Replayed"], "true")

    def test_status_start_stop_and_device_are_major_specific_closed_wire_shapes(self) -> None:
        start = {
            "schema": "ylx.capture-start.v2",
            "mode": "production",
            "take": {"kind": "new"},
        }
        stop = {"schema": "ylx.capture-stop.v2", "reason": "user"}
        self.server.provider.stop_status = 202

        for version in ("v2", "v3", "v4"):
            with self.subTest(version=version, resource="device"):
                status, payload, _ = self.request(f"/api/{version}/device", token="reader-token")
                self.assertEqual(status, 200)
                self.assertEqual(json.loads(payload), _device_for_api(version))

            with self.subTest(version=version, resource="status"):
                status, payload, _ = self.request(
                    f"/api/{version}/capture/status",
                    token="reader-token",
                )
                self.assertEqual(status, 200)
                self.assertEqual(json.loads(payload), _capture_status_for_api(version))

            with self.subTest(version=version, resource="start"):
                status, payload, _ = self.request(
                    f"/api/{version}/capture/start",
                    token="reader-token",
                    headers={
                        "Origin": self.base,
                        "Idempotency-Key": f"major-start-{version}",
                    },
                    body=start,
                )
                self.assertEqual(status, 202)
                self.assertEqual(json.loads(payload), _capture_status_for_api(version))

            with self.subTest(version=version, resource="stop"):
                status, payload, _ = self.request(
                    f"/api/{version}/capture/stop",
                    token="reader-token",
                    headers={
                        "Origin": self.base,
                        "Idempotency-Key": f"major-stop-{version}",
                    },
                    body=stop,
                )
                self.assertEqual(status, 202)
                self.assertEqual(json.loads(payload), _capture_status_for_api(version))

    def test_major_http_validators_reject_extra_properties_and_bad_consts(self) -> None:
        start = {
            "schema": "ylx.capture-start.v2",
            "mode": "production",
            "take": {"kind": "new"},
        }
        stop = {"schema": "ylx.capture-stop.v2", "reason": "user"}
        self.server.provider.stop_status = 202
        status_mutations = {
            "status-extra": lambda status: status["snapshot"]["runtime"].update(
                {"unexpected": "forbidden"}
            ),
            "status-const": lambda status: status.update({"schema": "ylx.capture-status.v5"}),
            "snapshot-const": lambda status: status["snapshot"].update(
                {"schema": "ylx.capture-snapshot-event.v5"}
            ),
        }

        for version in ("v2", "v3", "v4"):
            with self.subTest(version=version, mutation="device-extra"):
                self.server.provider.device_extra = {"unexpected": "forbidden"}
                status, payload, _ = self.request(f"/api/{version}/device", token="reader-token")
                self.assertEqual(status, 500)
                self.assertEqual(json.loads(payload)["error"]["code"], "invalid_source_state")
                self.server.provider.device_extra = {}

            with self.subTest(version=version, mutation="device-const"):
                self.server.provider.device_schema_override = "ylx.device.v5"
                status, payload, _ = self.request(f"/api/{version}/device", token="reader-token")
                self.assertEqual(status, 500)
                self.assertEqual(json.loads(payload)["error"]["code"], "invalid_source_state")
                self.server.provider.device_schema_override = None

            for mutation, mutate in status_mutations.items():
                malformed = deepcopy(CAPTURE_STATUS)
                mutate(malformed)
                self.server.provider.status = malformed
                for resource, path, body in (
                    ("status", f"/api/{version}/capture/status", None),
                    ("start", f"/api/{version}/capture/start", start),
                    ("stop", f"/api/{version}/capture/stop", stop),
                ):
                    with self.subTest(version=version, mutation=mutation, resource=resource):
                        status, payload, _ = self.request(
                            path,
                            token="reader-token",
                            headers={
                                "Origin": self.base,
                                "Idempotency-Key": f"bad-{mutation}-{resource}-{version}",
                            },
                            body=body,
                        )
                        self.assertEqual(status, 500)
                        self.assertEqual(
                            json.loads(payload)["error"]["code"],
                            "invalid_source_state",
                        )

        self.server.provider.status = deepcopy(CAPTURE_STATUS)

    def test_invalid_provider_capture_status_fails_closed_for_reads_and_commands(self) -> None:
        self.server.provider.status = {"schema": "ylx.capture-status.v2"}

        status, payload, _ = self.request(
            "/api/v3/capture/status",
            token="reader-token",
        )
        self.assertEqual(status, 500)
        self.assertEqual(json.loads(payload)["error"]["code"], "invalid_source_state")

        headers = {
            "Origin": "http://127.0.0.1:4173",
            "X-CSRF-Token": "browser-csrf-token",
        }
        status, payload, _ = self.request(
            "/api/v3/capture/start",
            token="reader-token",
            headers={**headers, "Idempotency-Key": "invalid-start-status"},
            body={
                "schema": "ylx.capture-start.v2",
                "mode": "production",
                "take": {"kind": "new"},
            },
        )
        self.assertEqual(status, 500)
        self.assertEqual(json.loads(payload)["error"]["code"], "invalid_source_state")

        self.server.provider.stop_status = 202
        status, payload, _ = self.request(
            "/api/v3/capture/stop",
            token="reader-token",
            headers={**headers, "Idempotency-Key": "invalid-stop-status"},
            body={"schema": "ylx.capture-stop.v2", "reason": "user"},
        )
        self.assertEqual(status, 500)
        self.assertEqual(json.loads(payload)["error"]["code"], "invalid_source_state")

    def test_invalid_provider_camera_focus_fails_closed(self) -> None:
        self.server.provider.focus = {"schema": "ylx.camera-focus.v1"}

        status, payload, _ = self.request("/api/v3/camera/focus", token="reader-token")
        self.assertEqual(status, 404)

        status, payload, _ = self.request(
            "/api/v4/camera/focus",
            token="reader-token",
            headers={"Origin": self.base, "Idempotency-Key": "bad-provider-focus"},
            body={"schema": "ylx.camera-focus-set.v1", "value": 44},
        )
        self.assertEqual(status, 500)
        self.assertEqual(json.loads(payload)["error"]["code"], "invalid_source_state")

    def test_session_listing_validates_query_and_preserves_provider_projection(self) -> None:
        take_id = "01989f69-f000-7c3d-ae4f-5061728394a5"
        status, payload, _ = self.request(
            f"/api/v3/sessions?cursor=next-page&limit=17&take_id={take_id}",
            token="reader-token",
        )
        self.assertEqual(status, 200)
        self.assertEqual(json.loads(payload), SESSION_LIST)
        self.assertEqual(self.server.provider.last_list_query, ("next-page", 17, take_id))

        for query in ("limit=0", "limit=201", "limit=abc", "cursor=", "take_id=wrong"):
            with self.subTest(query=query):
                status, payload, _ = self.request(f"/api/v3/sessions?{query}", token="reader-token")
                self.assertEqual(status, 400)
                self.assertEqual(json.loads(payload)["error"]["code"], "invalid_request")

        status, payload, _ = self.request("/api/v4/sessions", token="reader-token")
        self.assertEqual(status, 200)
        self.assertEqual(json.loads(payload), SESSION_LIST_V4)

        current_revision = "sha256:" + "e" * 64
        self.server.provider.list_error = ProviderError(
            "catalog_changed",
            "会话目录已变化，请从第一页重新加载",
            status=409,
            retryable=True,
            details={"catalog_revision": current_revision},
        )
        status, payload, _ = self.request(
            "/api/v4/sessions?cursor=opaque-old-page",
            token="reader-token",
        )
        self.assertEqual(status, 409)
        error = json.loads(payload)["error"]
        self.assertEqual(error["code"], "catalog_changed")
        self.assertTrue(error["retryable"])
        self.assertEqual(error["details"]["catalog_revision"], current_revision)

    def test_manifest_outcome_and_safe_swap_are_exact_persisted_resources(self) -> None:
        status, manifest, headers = self.request(
            f"/api/v3/sessions/{SESSION_ID}", token="reader-token"
        )
        self.assertEqual(status, 200)
        self.assertEqual(manifest, MANIFEST_BYTES)
        self.assertEqual(headers["Content-Type"], "application/json")
        self.assertEqual(headers["ETag"], f'"{MANIFEST_DIGEST}"')
        self.assertEqual(headers["YLX-Manifest-SHA256"], MANIFEST_DIGEST)
        self.assertEqual(headers.get_all("ETag"), [f'"{MANIFEST_DIGEST}"'])
        self.assertEqual(headers.get_all("YLX-Manifest-SHA256"), [MANIFEST_DIGEST])
        self.assertEqual(hashlib.sha256(manifest).hexdigest(), MANIFEST_DIGEST)

        status, payload, _ = self.request(
            f"/api/v3/sessions/{SESSION_ID}/unsuccessful-outcome", token="reader-token"
        )
        self.assertEqual(status, 200)
        self.assertEqual(json.loads(payload), RETAINED_OUTCOME)

        status, payload, _ = self.request("/api/v3/capture/safe-swap", token="reader-token")
        self.assertEqual(status, 200)
        self.assertEqual(json.loads(payload), SAFE_SWAP_V3)

        self.assertNotIn("handle_audit", json.loads(payload)["receipt"])

        status, payload, _ = self.request("/api/v4/capture/safe-swap", token="reader-token")
        self.assertEqual(status, 200)
        self.assertEqual(json.loads(payload), SAFE_SWAP_V3)
        self.assertNotIn("handle_audit", json.loads(payload)["receipt"])

        for mutation in ("wrapper_extra", "receipt_extra", "boolean_handle_count"):
            with self.subTest(mutation=mutation):
                malformed = deepcopy(SAFE_SWAP_V3)
                if mutation == "wrapper_extra":
                    malformed["deployment"] = "forbidden"
                elif mutation == "receipt_extra":
                    malformed["receipt"]["handle_audit"] = {}
                else:
                    malformed["receipt"]["open_handle_count"] = False
                self.server.provider.safe_swap = malformed
                status, payload, _ = self.request("/api/v3/capture/safe-swap", token="reader-token")
                self.assertEqual(status, 404)
                self.assertEqual(json.loads(payload)["error"]["code"], "not_found")

        self.server.provider.safe_swap = deepcopy(SAFE_SWAP_V2)
        status, payload, _ = self.request("/api/v2/capture/safe-swap", token="reader-token")
        self.assertEqual(status, 200)
        self.assertEqual(json.loads(payload), SAFE_SWAP_V2)

        status, payload, _ = self.request("/api/v3/capture/safe-swap", token="reader-token")
        self.assertEqual(status, 404)
        self.assertEqual(json.loads(payload)["error"]["code"], "not_found")

        status, payload, _ = self.request("/api/v4/capture/safe-swap", token="reader-token")
        self.assertEqual(status, 404)
        self.assertEqual(json.loads(payload)["error"]["code"], "not_found")

        self.server.provider.safe_swap = deepcopy(SAFE_SWAP_V3)
        status, payload, _ = self.request("/api/v2/capture/safe-swap", token="reader-token")
        self.assertEqual(status, 404)
        self.assertEqual(json.loads(payload)["error"]["code"], "not_found")

    def test_manifest_representation_metadata_and_body_mismatch_fail_closed(self) -> None:
        missing_etag = MemoryRepresentation(MANIFEST_BYTES)
        del missing_etag.etag
        malformed_etag = MemoryRepresentation(MANIFEST_BYTES)
        malformed_etag.etag = '"not-a-sha256"'
        body_changed = MemoryRepresentation(MANIFEST_BYTES + b"changed")
        body_changed.etag = f'"{MANIFEST_DIGEST}"'
        wrong_length = MemoryRepresentation(MANIFEST_BYTES)
        wrong_length.size += 1
        oversized = MemoryRepresentation(MANIFEST_BYTES)
        oversized.size = 8 * 1024 * 1024 + 1
        read_failed = FaultingManifestRepresentation(MANIFEST_BYTES)

        for label, representation in (
            ("missing-etag", missing_etag),
            ("malformed-etag", malformed_etag),
            ("body-changed", body_changed),
            ("wrong-length", wrong_length),
            ("oversized", oversized),
            ("read-failed", read_failed),
        ):
            with self.subTest(label=label):
                self.server.provider.manifest_representation = representation
                status, payload, headers = self.request(
                    f"/api/v4/sessions/{SESSION_ID}",
                    token="reader-token",
                )
                self.assertEqual(status, 500)
                self.assertEqual(json.loads(payload)["error"]["code"], "invalid_source_state")
                self.assertIsNone(headers["ETag"])
                self.assertIsNone(headers["YLX-Manifest-SHA256"])

    def test_cors_preflight_is_limited_to_known_routes_and_origins(self) -> None:
        headers = {
            "Origin": "http://127.0.0.1:4173",
            "Access-Control-Request-Method": "POST",
            "Access-Control-Request-Headers": (
                "authorization, content-type, idempotency-key, x-csrf-token"
            ),
        }
        status, payload, response_headers = self.request(
            "/api/v3/capture/start", headers=headers, method="OPTIONS"
        )
        self.assertEqual(status, 204)
        self.assertEqual(payload, b"")
        self.assertEqual(response_headers["Access-Control-Allow-Origin"], "http://127.0.0.1:4173")
        self.assertEqual(response_headers["Access-Control-Allow-Methods"], "POST, OPTIONS")
        allowed_headers = {
            value.strip().lower()
            for value in response_headers["Access-Control-Allow-Headers"].split(",")
        }
        self.assertEqual(
            allowed_headers,
            {"authorization", "content-type", "idempotency-key", "x-csrf-token"},
        )

        for path, method, requested, expected in (
            (
                "/api/v3/capture/events",
                "GET",
                "authorization, last-event-id",
                {"authorization", "last-event-id"},
            ),
            (
                f"/api/v3/sessions/{SESSION_ID}/artifacts/{'a' * 64}",
                "GET",
                "authorization, if-range, range",
                {"authorization", "if-range", "range"},
            ),
            (
                "/api/v3/device",
                "GET",
                "authorization",
                {"authorization"},
            ),
            (
                "/api/v4/camera/focus",
                "POST",
                "authorization, content-type, idempotency-key, x-csrf-token",
                {"authorization", "content-type", "idempotency-key", "x-csrf-token"},
            ),
            (
                "/api/v4/network/events",
                "GET",
                "authorization, last-event-id",
                {"authorization", "last-event-id"},
            ),
            (
                "/api/v4/network/scan",
                "GET",
                "authorization",
                {"authorization"},
            ),
            (
                "/api/v4/network/credentials",
                "POST",
                "authorization, content-type, x-csrf-token",
                {"authorization", "content-type", "x-csrf-token"},
            ),
            (
                "/api/v4/network/apply",
                "POST",
                "authorization, content-type, idempotency-key, x-csrf-token",
                {"authorization", "content-type", "idempotency-key", "x-csrf-token"},
            ),
        ):
            with self.subTest(path=path, method=method):
                status, _, response_headers = self.request(
                    path,
                    headers={
                        "Origin": "http://127.0.0.1:4173",
                        "Access-Control-Request-Method": method,
                        "Access-Control-Request-Headers": requested,
                    },
                    method="OPTIONS",
                )
                self.assertEqual(status, 204)
                self.assertEqual(
                    {
                        value.strip().lower()
                        for value in response_headers["Access-Control-Allow-Headers"].split(",")
                    },
                    expected,
                )

        for path, origin in (
            ("/api/v3/capture/start", "https://example.invalid"),
            ("/api/v3/network", "http://127.0.0.1:4173"),
        ):
            with self.subTest(path=path, origin=origin):
                status, payload, _ = self.request(
                    path,
                    headers={**headers, "Origin": origin},
                    method="OPTIONS",
                )
                self.assertEqual(status, 403 if "capture" in path else 404)
                self.assertEqual(json.loads(payload)["schema"], "ylx.api-error.v2")

        self.server.provider.safe_swap = None
        status, payload, _ = self.request("/api/v3/capture/safe-swap", token="reader-token")
        self.assertEqual(status, 404)
        self.assertEqual(json.loads(payload)["error"]["code"], "not_found")


if __name__ == "__main__":
    unittest.main()

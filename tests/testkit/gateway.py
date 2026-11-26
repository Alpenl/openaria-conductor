"""HTTP gateway test doubles, sample payloads, and GatewayHarness."""

from __future__ import annotations

import hashlib
import http.client
import json
import threading
import unittest
from contextlib import ExitStack
from copy import deepcopy
from dataclasses import dataclass
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from rp_ylx.api import (
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
from rp_ylx.api.preview import LatestPreviewBuffer, PreviewResponse

from . import ManagedHarness

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

DEVICE_V3: dict[str, object] = {
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

NETWORK_STATUS: dict[str, object] = {
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
NETWORK_RECEIPT: dict[str, object] = {
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

NETWORK_SCAN: dict[str, object] = {
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

NETWORK_CREDENTIAL_RECEIPT: dict[str, object] = {
    "schema": "ylx.network-credential-receipt.v1",
    "credential_ref": "cred-setup-token-001",
    "issued_at": "2026-08-23T12:58:02+08:00",
    "expires_at": "2026-08-23T13:00:02+08:00",
    "ttl_seconds": 120,
    "single_use": True,
}

SESSION_ID = "01989f6a-2c00-7a1b-8c2d-3e4f50617283"
NEXT_SESSION_ID = "01989f6a-2c02-7c3d-ae4f-5061728394a5"

CAPTURE_STATUS: dict[str, object] = {
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


def active_capture_status(session_id: str = SESSION_ID) -> dict[str, object]:
    """Return a capture-status payload with an active recording for `session_id`."""
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


def runtime_for_api(runtime: dict[str, object], api_version: str) -> dict[str, object]:
    """Project a runtime dict to the schema fields exposed in `api_version`."""
    projected = deepcopy(runtime)
    if api_version in {"v2", "v3"}:
        projected["live_imu"] = None
        projected.pop("camera", None)
        projected.pop("camera_focus", None)
    return projected


def device_for_api(api_version: str) -> dict[str, object]:
    """Build a sample device descriptor payload for `api_version`."""
    device = deepcopy(DEVICE_V3)
    device["schema"] = f"ylx.device.{api_version}"
    device["api_version"] = f"{api_version.removeprefix('v')}.0"
    runtime = device["runtime"]
    assert isinstance(runtime, dict)
    device["runtime"] = runtime_for_api(runtime, api_version)
    if api_version == "v4":
        capabilities = device["capabilities"]
        assert isinstance(capabilities, dict)
        capabilities.update(
            {
                "session_list": True,
                "session_detail": True,
                "artifact_download": True,
                "capture_status": True,
                "session_deletion": False,
            }
        )
        capabilities["calibration_capture"] = {
            "supported": True,
            "enabled": True,
            "disabled_reason": None,
            "required_video_layout": "split-eyes",
        }
    return device


def capture_status_for_api(
    api_version: str,
    status: dict[str, object] | None = None,
) -> dict[str, object]:
    """Project a capture-status payload to the schema fields exposed in `api_version`."""
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
    snapshot["runtime"] = runtime_for_api(runtime, api_version)
    return projected


RAW_LIVE_IMU: dict[str, object] = {
    "session_id": SESSION_ID,
    "clock": {"time_base": "host_monotonic", "timestamp_ns": 10_091},
    "raw": {
        "units": "raw_int16",
        "accelerometer": {"x": 1, "y": 2, "z": 3},
        "gyroscope": {"x": 4, "y": 5, "z": 6},
    },
    "sync": {"quality": "good"},
}

CAMERA_FOCUS_STATUS: dict[str, object] = {
    "schema": "ylx.camera-focus.v1",
    "value": 42,
    "minimum": 0,
    "maximum": 255,
    "step": 1,
    "default": 32,
    "auto_supported": True,
    "auto_enabled": False,
}

SESSION_LIST: dict[str, object] = {
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
SESSION_LIST_V4: dict[str, object] = {
    **deepcopy(SESSION_LIST),
    "schema": "ylx.session-list.v3",
    "catalog_revision": "sha256:" + "c" * 64,
}

MANIFEST_BYTES = b'{ "schema": "ylx.device-session.v1", "sealed": true }\n'
MANIFEST_DIGEST = hashlib.sha256(MANIFEST_BYTES).hexdigest()
RETAINED_OUTCOME: dict[str, object] = {
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
SAFE_SWAP_V3: dict[str, object] = {
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
SAFE_SWAP_V2: dict[str, object] = {
    "schema": "ylx.safe-swap-receipt-resource.v2",
    "receipt": {
        **deepcopy(SAFE_SWAP_V3["receipt"]),  # type: ignore[arg-type]
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
    """In-memory artifact/manifest representation implementing gateway streaming interface."""

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
    """MemoryRepresentation whose iter_chunks raises OSError on read."""

    def iter_chunks(
        self,
        offset: int = 0,
        length: int | None = None,
        *,
        chunk_size: int = 1024 * 1024,
    ) -> object:
        del offset, length, chunk_size
        raise OSError("manifest source read failed")


ARTIFACT_BYTES = b"immutable-session-artifact"
ARTIFACT_ID = hashlib.sha256(ARTIFACT_BYTES).hexdigest()


@dataclass(frozen=True, slots=True)
class ArtifactDescriptor:
    """Minimal artifact descriptor metadata matching gateway download expectations."""

    artifact_id: str
    path: str
    media_type: str
    bytes: int
    sha256: str


class MemoryArtifact(MemoryRepresentation):
    """In-memory video artifact representation with descriptor and read()."""

    def __init__(self, payload: bytes, content_type: str = "video/mp4") -> None:
        super().__init__(payload, content_type=content_type)
        digest = hashlib.sha256(payload).hexdigest()
        self.descriptor = ArtifactDescriptor(
            artifact_id=digest,
            path="video/left.mp4",
            media_type=content_type,
            bytes=len(payload),
            sha256=digest,
        )

    def read(self, offset: int = 0, length: int | None = None) -> bytes:
        end = None if length is None else offset + length
        return self.payload[offset:end]


class MemoryArtifactProvider:
    """In-memory artifact provider serving ARTIFACT_BYTES for SESSION_ID/ARTIFACT_ID."""

    def __init__(
        self,
        *,
        io_state: str | None = None,
        access_error: Exception | None = None,
    ) -> None:
        self.io_state = io_state
        self.access_error = access_error
        self.open_count = 0

    def artifact_io_state(self) -> str | None:
        return self.io_state

    def open_verified_artifact(
        self, session_id: str, artifact_id: str, api_version: str
    ) -> MemoryArtifact:
        self.open_count += 1
        if api_version not in {"v2", "v3", "v4"}:
            raise AssertionError("gateway passed unknown API version")
        if self.access_error is not None:
            raise self.access_error
        if session_id != SESSION_ID or artifact_id != ARTIFACT_ID:
            raise AssertionError("gateway changed the authorized artifact identity")
        return MemoryArtifact(ARTIFACT_BYTES)


class DirectoryArtifactProvider:
    """DirectorySessionStore-backed provider with optional open hooks for race tests."""

    def __init__(self, store: object) -> None:
        self.store = store
        self.io_state: str | None = None
        self.after_open: object | None = None
        self.after_manifest_open: object | None = None

    def artifact_io_state(self) -> str | None:
        return self.io_state

    def open_manifest(self, session_id: str, api_version: str) -> object:
        locked = self.store.open_manifest(session_id, api_version)  # type: ignore[attr-defined]
        if callable(self.after_manifest_open):
            self.after_manifest_open()
        return locked

    def open_verified_artifact(self, session_id: str, artifact_id: str, api_version: str) -> object:
        locked = self.store.open_verified_artifact(  # type: ignore[attr-defined]
            session_id, artifact_id, api_version
        )
        if callable(self.after_open):
            self.after_open()
        return locked


class DeviceProvider:
    """Configurable in-memory gateway provider test double."""

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
        runtime = descriptor["runtime"]
        assert isinstance(runtime, dict)
        runtime["camera_focus"] = deepcopy(self.focus)
        runtime["live_imu"] = deepcopy(self.live_imu)
        if api_version == "v4":
            capabilities = descriptor["capabilities"]
            assert isinstance(capabilities, dict)
            capabilities.update(
                {
                    "session_list": True,
                    "session_detail": True,
                    "artifact_download": True,
                    "capture_status": True,
                    "session_deletion": False,
                }
            )
            capabilities["calibration_capture"] = {
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


def default_customer_policy() -> SecurityPolicy:
    """Create the default customer SecurityPolicy used by gateway HTTP tests."""
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
    return SecurityPolicy.customer(
        tokens={"reader-token": reader, "denied-token": denied},
        allowed_origins={"http://127.0.0.1:4173"},
        csrf_token="browser-csrf-token",
    )


class GatewayHarness(ManagedHarness):
    """Live localhost HTTP gateway server harness with request and SSE helpers."""

    def __init__(
        self,
        *,
        provider: object | None = None,
        security: SecurityPolicy | None = None,
        host: str = "127.0.0.1",
        port: int = 0,
        **server_kwargs: object,
    ) -> None:
        super().__init__()
        self._initial_provider = provider
        self._initial_security = security
        self._host = host
        self._port = port
        self._server_kwargs = server_kwargs

    def _configure(self, stack: ExitStack) -> None:
        self.provider = (
            DeviceProvider() if self._initial_provider is None else self._initial_provider
        )
        self.policy = (
            default_customer_policy() if self._initial_security is None else self._initial_security
        )
        self.server = create_gateway_server(
            self._host,
            self._port,
            self.provider,
            security=self.policy,
            **self._server_kwargs,  # type: ignore[arg-type]
        )
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.base = f"http://{self._host}:{self.server.server_port}"
        stack.callback(self._shutdown_server)

    def _shutdown_server(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=2)

    def request(
        self,
        path: str,
        *,
        token: str | None = None,
        headers: dict[str, str] | None = None,
        body: object | None = None,
        method: str | None = None,
    ) -> tuple[int, bytes, object]:
        """Issue a JSON/HTTP request against the live test gateway server."""
        request_headers = dict(headers or {})
        if token is not None:
            request_headers["Authorization"] = f"Bearer {token}"
        data = None
        if body is not None:
            data = json.dumps(body).encode()
            request_headers.setdefault("Content-Type", "application/json")
            if token is not None and request_headers.get("Origin") == self.base:
                request_headers.setdefault("X-CSRF-Token", "browser-csrf-token")
        req = Request(self.base + path, headers=request_headers, data=data, method=method)
        try:
            with urlopen(req, timeout=2) as response:
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
        """Issue a raw HTTPConnection request preserving exact header tuples."""
        connection = http.client.HTTPConnection(self._host, self.server.server_port, timeout=2)
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


class GatewayMixin(unittest.TestCase):
    """unittest.TestCase mixin provisioning a GatewayHarness as self.harness."""

    harness: GatewayHarness

    def setUp(self) -> None:
        super().setUp()
        self.harness = GatewayHarness().setUp()
        self.addCleanup(self.harness.tearDown)


AUTHORITY_EPOCH = "4fa85f64-5717-4562-b3fc-2c963f66afa6"
PREVIEW_JPEG = b"\xff\xd8frozen-preview\xff\xd9"
PREVIEW_PART = (
    b"--ylx-preview\r\nContent-Type: image/jpeg\r\nContent-Length: 18\r\n\r\n"
    + PREVIEW_JPEG
    + b"\r\n"
)
SNAPSHOT_EVENT: dict[str, object] = {
    "authority_epoch": AUTHORITY_EPOCH,
    "source_revision": 1,
    "type": "snapshot",
    "occurred_at": "2026-09-05T00:00:00Z",
    "session_id": None,
    "data": {
        "schema": "ylx.capture-snapshot-event.v2",
        "device_state": "idle",
        "active_recording": None,
        "retained_unsuccessful": None,
        "runtime": {
            "observed_at": "2026-09-05T00:00:00Z",
            "connection_method": "ethernet_lan",
            "temperature_celsius": 42.0,
            "network": {
                "ap": {
                    "state": "disconnected",
                    "interface": "wlan0",
                    "addresses": [],
                    "peer_or_ssid": None,
                },
                "wifi_client": {
                    "state": "connected",
                    "interface": "wlan0",
                    "addresses": ["192.0.2.2/24"],
                    "peer_or_ssid": "test-network",
                },
                "wired": {
                    "state": "connected",
                    "interface": "eth0",
                    "addresses": ["198.51.100.2/24"],
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
    },
}


class LifecycleProvider:
    """Minimal provider for preview/SSE lifecycle and stream-start failure tests."""

    def __init__(self) -> None:
        self.preview = LatestPreviewBuffer(stream_fps=30)
        self.preview.publish(PREVIEW_JPEG)

    def latest_preview(self, *, fps: int | None, accept: str) -> PreviewResponse:
        return self.preview.latest_preview(fps=fps, accept=accept)

    def capture_snapshot_event(self) -> dict[str, object]:
        return deepcopy(SNAPSHOT_EVENT)

    def capture_status(self) -> dict[str, object]:
        return {
            "schema": "ylx.capture-status.v2",
            "authority_epoch": AUTHORITY_EPOCH,
            "source_revision": 1,
            "snapshot": deepcopy(SNAPSHOT_EVENT["data"]),
        }

    def network_status(self) -> dict[str, object]:
        return deepcopy(NETWORK_STATUS)


__all__ = [
    "ARTIFACT_BYTES",
    "ARTIFACT_ID",
    "AUTHORITY_EPOCH",
    "ArtifactDescriptor",
    "CAMERA_FOCUS_STATUS",
    "CAPTURE_STATUS",
    "DEVICE_V3",
    "DeviceProvider",
    "DirectoryArtifactProvider",
    "FaultingManifestRepresentation",
    "GatewayHarness",
    "GatewayMixin",
    "LifecycleProvider",
    "MANIFEST_BYTES",
    "MANIFEST_DIGEST",
    "MemoryArtifact",
    "MemoryArtifactProvider",
    "MemoryRepresentation",
    "NETWORK_CREDENTIAL_RECEIPT",
    "NETWORK_RECEIPT",
    "NETWORK_SCAN",
    "NETWORK_STATUS",
    "NETWORK_TRANSACTION_ID",
    "NEXT_SESSION_ID",
    "PREVIEW_JPEG",
    "PREVIEW_PART",
    "RAW_LIVE_IMU",
    "RETAINED_OUTCOME",
    "SAFE_SWAP_V2",
    "SAFE_SWAP_V3",
    "SESSION_ID",
    "SESSION_LIST",
    "SESSION_LIST_V4",
    "SNAPSHOT_EVENT",
    "UNKNOWN_WEB_PATHS",
    "WEB_ASSET_EXPECTATIONS",
    "WEB_HEADER_NAMES",
    "WEB_SECURITY_HEADERS",
    "active_capture_status",
    "capture_status_for_api",
    "default_customer_policy",
    "device_for_api",
    "runtime_for_api",
]

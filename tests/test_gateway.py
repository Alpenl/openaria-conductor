from __future__ import annotations

import hashlib
import http.client
import json
import threading
import unittest
from copy import deepcopy
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from rp_ylx.api import (
    AuditEvent,
    CaptureCommand,
    CaptureCommandResult,
    Principal,
    ProviderError,
    SecurityPolicy,
    create_gateway_server,
)
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
        "camera_focus": None,
    },
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
        projected.pop("camera_focus", None)
    return projected


def _device_for_api(api_version: str) -> dict[str, object]:
    device = deepcopy(DEVICE_V3)
    device["schema"] = f"ylx.device.{api_version}"
    device["api_version"] = f"{api_version.removeprefix('v')}.0"
    device["runtime"] = _runtime_for_api(device["runtime"], api_version)
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


class DeviceProvider:
    def __init__(self) -> None:
        self.commands: dict[tuple[str, str, str], tuple[bytes, CaptureCommandResult]] = {}
        self.safe_swap: object | None = deepcopy(SAFE_SWAP_V3)
        self.status: object = deepcopy(CAPTURE_STATUS)
        self.focus: object | None = None
        self.live_imu: object | None = None
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
        return self._command("start", command, status=202, body=self.status)

    def stop_capture(self, command: CaptureCommand) -> CaptureCommandResult:
        body = self.status if self.stop_status == 202 else None
        return self._command("stop", command, status=self.stop_status, body=body)

    def camera_focus_status(self) -> object | None:
        return deepcopy(self.focus)

    def set_camera_focus(self, command: CaptureCommand) -> CaptureCommandResult:
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
        self, *, cursor: str | None, limit: int, take_id: str | None
    ) -> dict[str, object]:
        self.last_list_query = (cursor, limit, take_id)
        return deepcopy(SESSION_LIST)

    def open_manifest(self, session_id: str, api_version: str) -> MemoryRepresentation:
        if api_version not in {"v2", "v3", "v4"}:
            raise AssertionError("gateway 传递了未知 API 版本")
        if session_id != SESSION_ID:
            raise ProviderError("not_found", "会话不存在", status=404)
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

    def test_embedded_web_same_origin_command_does_not_need_a_second_secret(self) -> None:
        status, payload, headers = self.request(
            "/api/v3/capture/start",
            token="reader-token",
            headers={
                "Origin": self.base,
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
                "Idempotency-Key": "embedded-web-stop",
            },
            body={"schema": "ylx.capture-stop.v2", "reason": "user"},
        )
        self.assertEqual(status, 204)
        self.assertEqual(payload, b"")

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
                status, payload, _ = self.raw_request(
                    "POST",
                    "/api/v3/capture/start",
                    (*common, (name, value)),
                    body,
                )

                self.assertEqual(status, expected_status)
                self.assertEqual(json.loads(payload)["error"]["code"], expected_code)

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

        self.assertEqual([event.outcome for event in audit], ["origin_forbidden", "allowed"])
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

    def test_manifest_outcome_and_safe_swap_are_exact_persisted_resources(self) -> None:
        status, manifest, headers = self.request(
            f"/api/v3/sessions/{SESSION_ID}", token="reader-token"
        )
        self.assertEqual(status, 200)
        self.assertEqual(manifest, MANIFEST_BYTES)
        self.assertEqual(headers["Content-Type"], "application/json")
        self.assertEqual(headers["ETag"], f'"{MANIFEST_DIGEST}"')
        self.assertEqual(headers["YLX-Manifest-SHA256"], MANIFEST_DIGEST)

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

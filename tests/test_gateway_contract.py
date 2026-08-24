from __future__ import annotations

import hashlib
import json
import re
import threading
import unittest
from copy import deepcopy
from importlib.resources import files
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from rp_ylx.api import (
    CaptureCommand,
    CaptureCommandResult,
    Principal,
    ProviderError,
    SecurityPolicy,
    create_gateway_server,
)

REPO_ROOT = Path(__file__).resolve().parents[1]
CONTRACT_GOLDENS = {
    "v2": {
        "filename": "ylx-device-v2.openapi.yaml",
        "sha256": "274216d7f140b296dacf70fb669e37eb7be2ccf48f51e9d354a5245e01e05599",
        "bytes": 67_834,
        "info_version": "2.0.0",
        "server_suffix": "/api/v2",
        "lifecycle": "frozen_compat",
    },
    "v3": {
        "filename": "ylx-device-v3.openapi.yaml",
        "sha256": "72b70dd6d9ab87e70abc0bf4af519435435bba05a33d512d4c394f25b1ef4297",
        "bytes": 68_520,
        "info_version": "3.0.0",
        "server_suffix": "/api/v3",
        "lifecycle": "frozen_compat",
    },
    "v4": {
        "filename": "ylx-device-v4.openapi.yaml",
        "sha256": "b74654c9e36d7d1b49bf6c13b3f712d25b6f38847e32b09662b0c3bcc57cc1f4",
        "bytes": 117_734,
        "info_version": "4.0.0",
        "server_suffix": "/api/v4",
        "lifecycle": "current",
    },
}

SCHEMA_GOLDENS = {
    "ylx-device-session-v1.schema.json": (
        "38a4ca96bbaa171d809f72134537c65d1a5de36db66cb96be3006c20215c0bad"
    ),
    "ylx-device-session-v2.schema.json": (
        "8dc6096981f3fc50f9b4418000431955e0ba9424c7c0257cd2e129251a6a715b"
    ),
    "ylx-recording-state-v1.schema.json": (
        "1bdedf7025380e712906bdde9bd980d1f2aa8e5e70ce23a1939948cbc76150f7"
    ),
}
SCHEMA_REFERENCES_BY_VERSION = {
    "v2": {"ylx-device-session-v1.schema.json", "ylx-recording-state-v1.schema.json"},
    "v3": {"ylx-device-session-v1.schema.json", "ylx-recording-state-v1.schema.json"},
    "v4": {"ylx-device-session-v2.schema.json", "ylx-recording-state-v1.schema.json"},
}

COMMON_ROUTE_GOLDEN = {
    ("/device", "get", "getDevice"),
    ("/capture/status", "get", "getCaptureStatus"),
    ("/capture/start", "post", "startCapture"),
    ("/capture/stop", "post", "stopCapture"),
    ("/capture/events", "get", "streamCaptureEvents"),
    ("/capture/safe-swap", "get", "getCurrentSafeSwapReceipt"),
    ("/preview", "get", "getPreview"),
    ("/sessions", "get", "listSessions"),
    ("/sessions/{session_id}", "get", "getSession"),
    (
        "/sessions/{session_id}/unsuccessful-outcome",
        "get",
        "getRetainedUnsuccessfulSessionOutcome",
    ),
    ("/sessions/{session_id}/artifacts/{artifact_id}", "get", "getSessionArtifact"),
    ("/sessions/{session_id}/artifacts/{artifact_id}", "head", "headSessionArtifact"),
}
ROUTE_GOLDENS = {
    "v2": COMMON_ROUTE_GOLDEN,
    "v3": COMMON_ROUTE_GOLDEN,
    "v4": COMMON_ROUTE_GOLDEN
    | {
        ("/network", "get", "getNetworkStatus"),
        ("/network/scan", "get", "scanNetworks"),
        ("/network/credentials", "post", "createNetworkCredentialReference"),
        ("/network/events", "get", "streamNetworkEvents"),
        ("/network/apply", "post", "applyNetworkDesiredState"),
        ("/network/retry", "post", "retryNetworkTransaction"),
        ("/network/forget", "post", "forgetNetworkClientProfile"),
        ("/camera/focus", "get", "getCameraFocus"),
        ("/camera/focus", "post", "setCameraFocus"),
    },
}

SESSION_ID = "01989f6a-2c00-7a1b-8c2d-3e4f50617283"
MANIFEST_WIRE = b'{ "schema": "ylx.device-session.v1", "sealed": true }\n'
MANIFEST_SHA256 = "5967f1d39a638ce5737c1ef922d3eb6c5f08fd3fc32099751e0f9f419795f5d6"
STATUS_WIRE_LEGACY = (
    b'{"schema":"ylx.capture-status.v2",'
    b'"authority_epoch":"4fa85f64-5717-4562-b3fc-2c963f66afa6",'
    b'"source_revision":7,"snapshot":{"schema":"ylx.capture-snapshot-event.v2",'
    b'"device_state":"idle","active_recording":null,"retained_unsuccessful":null,'
    b'"runtime":{"observed_at":"2026-08-08T10:25:01+08:00",'
    b'"connection_method":"ethernet_lan","temperature_celsius":52.0,'
    b'"network":{"ap":{"state":"active","interface":"wlan0",'
    b'"addresses":["10.42.0.1/24"],"peer_or_ssid":"YLX-30D5872D"},'
    b'"wifi_client":{"state":"disconnected","interface":"wlan1",'
    b'"addresses":[],"peer_or_ssid":null},"wired":{"state":"connected",'
    b'"interface":"eth0","addresses":["192.0.2.24/24"],'
    b'"peer_or_ssid":null},"default_route":"wired"},"live_imu":null}}}'
)
STATUS_WIRE_V4 = (
    b'{"schema":"ylx.capture-status.v4",'
    b'"authority_epoch":"4fa85f64-5717-4562-b3fc-2c963f66afa6",'
    b'"source_revision":7,"snapshot":{"schema":"ylx.capture-snapshot-event.v4",'
    b'"device_state":"idle","active_recording":null,"retained_unsuccessful":null,'
    b'"runtime":{"observed_at":"2026-08-08T10:25:01+08:00",'
    b'"connection_method":"ethernet_lan","temperature_celsius":52.0,'
    b'"network":{"ap":{"state":"active","interface":"wlan0",'
    b'"addresses":["10.42.0.1/24"],"peer_or_ssid":"YLX-30D5872D"},'
    b'"wifi_client":{"state":"disconnected","interface":"wlan1",'
    b'"addresses":[],"peer_or_ssid":null},"wired":{"state":"connected",'
    b'"interface":"eth0","addresses":["192.0.2.24/24"],'
    b'"peer_or_ssid":null},"default_route":"wired"},"live_imu":null,'
    b'"camera":{"schema":"ylx.camera-connection.v1","state":"connected"},'
    b'"camera_focus":null}}}'
)
STATUS_WIRE_BY_VERSION = {
    "v2": STATUS_WIRE_LEGACY,
    "v3": STATUS_WIRE_LEGACY,
    "v4": STATUS_WIRE_V4,
}
SESSION_LIST_WIRE = (
    b'{"schema":"ylx.session-list.v2","items":[],"diagnostics":[],"next_cursor":null}'
)
DEVICE_COMMON = {
    "device": {
        "device_id": "550e8400-e29b-41d4-a716-446655440000",
        "device_label": "YLX-30D5872D",
    },
    "hardware_fingerprint": "sha256:" + "a" * 64,
    "build": {
        "package_version": "0.5.0",
        "commit": "2db57ae68e04197397b8ac84f4d71548aa2fcb36",
        "build_id": "rdk-x5-wire-test",
    },
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
        "observed_at": "2026-08-08T10:25:01+08:00",
        "connection_method": "ethernet_lan",
        "temperature_celsius": 52.0,
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


def _device_common_for_api(api_version: str) -> dict[str, object]:
    common = deepcopy(DEVICE_COMMON)
    if api_version in {"v2", "v3"}:
        runtime = common["runtime"]
        assert isinstance(runtime, dict)
        runtime.pop("camera", None)
        runtime.pop("camera_focus", None)
    return common


def _sha256(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _declared_routes(contract: str) -> set[tuple[str, str, str]]:
    routes: set[tuple[str, str, str]] = set()
    path: str | None = None
    method: str | None = None
    for line in contract.splitlines():
        path_match = re.fullmatch(r"  (/[^:]+):", line)
        if path_match:
            path = path_match.group(1)
            method = None
            continue
        method_match = re.fullmatch(r"    (get|head|post):", line)
        if path is not None and method_match:
            method = method_match.group(1)
            continue
        operation_match = re.fullmatch(r"      operationId: ([A-Za-z][A-Za-z0-9]*)", line)
        if path is not None and method is not None and operation_match:
            routes.add((path, method, operation_match.group(1)))
    return routes


class GatewayContractResourceTest(unittest.TestCase):
    def test_frozen_openapi_resources_match_the_authoritative_bytes(self) -> None:
        api_resources = files("rp_ylx.api")

        for version, golden in CONTRACT_GOLDENS.items():
            with self.subTest(version=version):
                payload = api_resources.joinpath(golden["filename"]).read_bytes()
                self.assertEqual(_sha256(payload), golden["sha256"])
                self.assertEqual(len(payload), golden["bytes"])

    def test_frozen_openapi_versions_servers_and_operations_are_exact(self) -> None:
        api_resources = files("rp_ylx.api")

        for version, golden in CONTRACT_GOLDENS.items():
            with self.subTest(version=version):
                contract = api_resources.joinpath(golden["filename"]).read_text(encoding="utf-8")
                info = re.search(
                    r"^info:\n(?:^  .*\n)*?^  version: ([^\n]+)$",
                    contract,
                    flags=re.MULTILINE,
                )
                self.assertIsNotNone(info)
                self.assertEqual(info.group(1), golden["info_version"])

                server = re.search(
                    r"^  - url: https://\{device_host\}(/api/v[234])$",
                    contract,
                    re.M,
                )
                self.assertIsNotNone(server)
                self.assertEqual(server.group(1), golden["server_suffix"])
                self.assertEqual(_declared_routes(contract), ROUTE_GOLDENS[version])
                if version in {"v2", "v3"}:
                    self.assertNotIn("raw_int16", contract)
                else:
                    self.assertIn("raw_int16", contract)

    def test_device_api_support_manifest_matches_central_identity(self) -> None:
        support = json.loads(
            (REPO_ROOT / "contracts" / "ylx-device-api-support.json").read_text(encoding="utf-8")
        )

        self.assertEqual(
            support,
            {
                "schema": "ylx.device-api-consumer-support.v1",
                "consumer": "openaria-conductor",
                "supported_device_api_majors": [2, 3, 4],
                "unknown_major_policy": "fail_closed",
                "required_contracts": [
                    {
                        "major": int(version.removeprefix("v")),
                        "path": f"openapi/{golden['filename']}",
                        "sha256": golden["sha256"],
                        "bytes": golden["bytes"],
                        "info_version": golden["info_version"],
                        "server_base_path": golden["server_suffix"],
                        "lifecycle": golden["lifecycle"],
                    }
                    for version, golden in CONTRACT_GOLDENS.items()
                ],
            },
        )

    def test_every_external_schema_reference_resolves_to_a_frozen_resource(self) -> None:
        api_resources = files("rp_ylx.api")
        schema_resources = files("rp_ylx").joinpath("schemas")

        for version, golden in CONTRACT_GOLDENS.items():
            contract = api_resources.joinpath(golden["filename"]).read_text(encoding="utf-8")
            referenced = set(re.findall(r'\$ref: "\.\./schemas/([^"#]+)(?:#[^"]+)?"', contract))
            with self.subTest(version=version):
                self.assertEqual(referenced, SCHEMA_REFERENCES_BY_VERSION[version])

        for filename, expected_sha256 in SCHEMA_GOLDENS.items():
            with self.subTest(schema=filename):
                payload = schema_resources.joinpath(filename).read_bytes()
                self.assertEqual(_sha256(payload), expected_sha256)
                self.assertIsInstance(json.loads(payload), dict)


class _Manifest:
    size = len(MANIFEST_WIRE)
    etag = f'"{MANIFEST_SHA256}"'
    content_type = "application/json"

    def __enter__(self) -> _Manifest:
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
        yield MANIFEST_WIRE[offset:end]


class _WireProvider:
    def device_descriptor(self, api_version: str, security_profile: str) -> dict[str, object]:
        version = f"{api_version.removeprefix('v')}.0"
        return {
            "schema": f"ylx.device.{api_version}",
            **deepcopy(DEVICE_COMMON),
            "api_version": version,
            "security_profile": security_profile,
        }

    def capture_status(self) -> dict[str, object]:
        return {
            "schema": "ylx.capture-status.v2",
            "authority_epoch": "4fa85f64-5717-4562-b3fc-2c963f66afa6",
            "source_revision": 7,
            "snapshot": {
                "schema": "ylx.capture-snapshot-event.v2",
                "device_state": "idle",
                "active_recording": None,
                "retained_unsuccessful": None,
                "runtime": {
                    "observed_at": "2026-08-08T10:25:01+08:00",
                    "connection_method": "ethernet_lan",
                    "temperature_celsius": 52.0,
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
            },
        }

    def start_capture(self, command: CaptureCommand) -> CaptureCommandResult:
        del command
        return CaptureCommandResult(202, self.capture_status())

    def stop_capture(self, command: CaptureCommand) -> CaptureCommandResult:
        del command
        return CaptureCommandResult(204, None)

    def list_sessions(
        self, *, cursor: str | None, limit: int, take_id: str | None
    ) -> dict[str, object]:
        del cursor, limit, take_id
        return {
            "schema": "ylx.session-list.v2",
            "items": [],
            "diagnostics": [],
            "next_cursor": None,
        }

    def open_manifest(self, session_id: str, api_version: str) -> _Manifest:
        if api_version not in {"v2", "v3", "v4"}:
            raise AssertionError("gateway 传递了未知 API 版本")
        if session_id != SESSION_ID:
            raise ProviderError("not_found", "会话不存在", status=404)
        return _Manifest()


class GatewayV2WireGoldenTest(unittest.TestCase):
    def setUp(self) -> None:
        operations = {
            "getDevice": None,
            "getCaptureStatus": None,
            "startCapture": None,
            "stopCapture": None,
            "listSessions": None,
            "getSession": {SESSION_ID},
        }
        security = SecurityPolicy.customer(
            tokens={"wire-token": Principal("wire-reader", permissions=operations)},
            allowed_origins=set(),
            csrf_token="wire-token",
        )
        self.server = create_gateway_server("127.0.0.1", 0, _WireProvider(), security=security)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.base_url = f"http://127.0.0.1:{self.server.server_port}"

    def tearDown(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=2)

    def request(
        self,
        path: str,
        *,
        token: str | None = "wire-token",
        body: bytes | None = None,
        idempotency_key: str | None = None,
    ) -> tuple[int, bytes, object]:
        headers: dict[str, str] = {}
        if token is not None:
            headers["Authorization"] = f"Bearer {token}"
        if body is not None:
            headers["Content-Type"] = "application/json"
            headers["Origin"] = self.base_url
            headers["X-CSRF-Token"] = "wire-token"
        if idempotency_key is not None:
            headers["Idempotency-Key"] = idempotency_key
        request = Request(self.base_url + path, headers=headers, data=body)
        try:
            with urlopen(request, timeout=2) as response:
                return response.status, response.read(), response.headers
        except HTTPError as error:
            return error.code, error.read(), error.headers

    def test_v2_v3_and_v4_wire_contracts_share_the_frozen_common_payloads(self) -> None:
        start_wire = b'{"schema":"ylx.capture-start.v2","mode":"production","take":{"kind":"new"}}'
        stop_wire = b'{"schema":"ylx.capture-stop.v2","reason":"user"}'

        for version, device_wire in (
            (
                "v2",
                json.dumps(
                    {
                        "schema": "ylx.device.v2",
                        **_device_common_for_api("v2"),
                        "api_version": "2.0",
                        "security_profile": "customer",
                    },
                    separators=(",", ":"),
                ).encode(),
            ),
            (
                "v3",
                json.dumps(
                    {
                        "schema": "ylx.device.v3",
                        **_device_common_for_api("v3"),
                        "api_version": "3.0",
                        "security_profile": "customer",
                    },
                    separators=(",", ":"),
                ).encode(),
            ),
            (
                "v4",
                json.dumps(
                    {
                        "schema": "ylx.device.v4",
                        **_device_common_for_api("v4"),
                        "api_version": "4.0",
                        "security_profile": "customer",
                    },
                    separators=(",", ":"),
                ).encode(),
            ),
        ):
            with self.subTest(version=version, resource="device"):
                status, payload, _ = self.request(f"/api/{version}/device")
                self.assertEqual((status, payload), (200, device_wire))

            with self.subTest(version=version, resource="status"):
                status, payload, _ = self.request(f"/api/{version}/capture/status")
                self.assertEqual((status, payload), (200, STATUS_WIRE_BY_VERSION[version]))

            with self.subTest(version=version, resource="start"):
                status, payload, headers = self.request(
                    f"/api/{version}/capture/start",
                    body=start_wire,
                    idempotency_key=f"start-{version}",
                )
                self.assertEqual((status, payload), (202, STATUS_WIRE_BY_VERSION[version]))
                self.assertIsNone(headers["Idempotency-Replayed"])

            with self.subTest(version=version, resource="stop"):
                status, payload, _ = self.request(
                    f"/api/{version}/capture/stop",
                    body=stop_wire,
                    idempotency_key=f"stop-{version}",
                )
                self.assertEqual((status, payload), (204, b""))

            with self.subTest(version=version, resource="session-list"):
                status, payload, _ = self.request(f"/api/{version}/sessions")
                self.assertEqual((status, payload), (200, SESSION_LIST_WIRE))

            with self.subTest(version=version, resource="manifest"):
                status, payload, headers = self.request(f"/api/{version}/sessions/{SESSION_ID}")
                self.assertEqual((status, payload), (200, MANIFEST_WIRE))
                self.assertEqual(headers["Content-Length"], str(len(MANIFEST_WIRE)))
                self.assertEqual(headers["ETag"], f'"{MANIFEST_SHA256}"')
                self.assertEqual(headers["YLX-Manifest-SHA256"], MANIFEST_SHA256)

    def test_v2_v3_and_v4_errors_keep_the_same_frozen_envelope(self) -> None:
        for version in ("v2", "v3", "v4"):
            with self.subTest(version=version):
                status, payload, headers = self.request(f"/api/{version}/device", token=None)
                error = json.loads(payload)
                request_id = error["error"].pop("request_id")
                self.assertEqual(status, 401)
                self.assertEqual(headers["WWW-Authenticate"], "Bearer")
                self.assertEqual(
                    error,
                    {
                        "schema": "ylx.api-error.v2",
                        "error": {
                            "code": "unauthorized",
                            "message": "Bearer 凭据缺失或无效",
                            "retryable": False,
                        },
                    },
                )
                self.assertRegex(
                    request_id,
                    r"^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$",
                )


if __name__ == "__main__":
    unittest.main()

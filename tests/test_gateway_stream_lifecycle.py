from __future__ import annotations

import json
import os
import shutil
import socket
import ssl
import struct
import subprocess
import tempfile
import threading
import time
import unittest
from copy import deepcopy
from pathlib import Path
from urllib.request import Request, urlopen

from rp_ylx.api.events import EventReplayBuffer
from rp_ylx.api.gateway import create_gateway_server
from rp_ylx.api.preview import LatestPreviewBuffer, PreviewResponse
from rp_ylx.api.security import Principal, SecurityPolicy
from tests.test_gateway import NETWORK_STATUS

AUTHORITY_EPOCH = "4fa85f64-5717-4562-b3fc-2c963f66afa6"
PREVIEW_JPEG = b"\xff\xd8frozen-preview\xff\xd9"
PREVIEW_PART = (
    b"--ylx-preview\r\nContent-Type: image/jpeg\r\nContent-Length: 18\r\n\r\n"
    + PREVIEW_JPEG
    + b"\r\n"
)
SNAPSHOT_EVENT = {
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


class _LifecycleProvider:
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


class _RawStreamClient:
    def __init__(self, port: int, *, tls: bool) -> None:
        connection = socket.create_connection(("127.0.0.1", port), timeout=2)
        if tls:
            context = ssl.create_default_context()
            context.check_hostname = False
            context.verify_mode = ssl.CERT_NONE
            self.socket = context.wrap_socket(connection, server_hostname="localhost")
        else:
            self.socket = connection
        self.socket.settimeout(2)

    def request_stream(
        self,
        path: str,
        accept: str,
        marker: bytes,
        *,
        last_event_id: str | None = None,
    ) -> bytes:
        cursor = "" if last_event_id is None else f"Last-Event-ID: {last_event_id}\r\n"
        request = (
            f"GET {path} HTTP/1.1\r\n"
            "Host: 127.0.0.1\r\n"
            "Authorization: Bearer reader-token\r\n"
            f"Accept: {accept}\r\n"
            f"{cursor}"
            "Connection: close\r\n\r\n"
        ).encode("ascii")
        self.socket.sendall(request)
        payload = bytearray()
        while marker not in payload:
            chunk = self.socket.recv(4096)
            if not chunk:
                break
            payload.extend(chunk)
            if len(payload) > 256 * 1024:
                raise AssertionError("stream response exceeded test bound")
        return bytes(payload)

    def abort(self) -> None:
        try:
            self.socket.setsockopt(
                socket.SOL_SOCKET,
                socket.SO_LINGER,
                struct.pack("ii", 1, 0),
            )
        finally:
            self.socket.close()


class GatewayStreamLifecycleTest(unittest.TestCase):
    def setUp(self) -> None:
        self.provider = _LifecycleProvider()
        reader = Principal(
            "reader",
            permissions={
                "getCaptureStatus": None,
                "getPreview": None,
                "streamCaptureEvents": None,
                "streamNetworkEvents": None,
            },
        )
        self.event_buffer = EventReplayBuffer()
        self.server = create_gateway_server(
            "127.0.0.1",
            0,
            self.provider,
            security=SecurityPolicy.customer(tokens={"reader-token": reader}),
            event_buffer=self.event_buffer,
            sse_heartbeat_seconds=60.0,
            max_sse_connections=1,
            max_preview_streams=1,
        )
        self.temp_directory: tempfile.TemporaryDirectory[str] | None = None
        self.thread: threading.Thread | None = None

    def tearDown(self) -> None:
        if self.thread is not None:
            self.server.shutdown()
            self.server.server_close()
            self.thread.join(timeout=2)
            self.assertFalse(self.thread.is_alive())
        else:
            self.server.server_close()
        if self.temp_directory is not None:
            self.temp_directory.cleanup()

    def start(self, *, tls: bool = False) -> None:
        if tls:
            openssl = shutil.which("openssl")
            self.assertIsNotNone(openssl, "TLS lifecycle tests require openssl")
            self.temp_directory = tempfile.TemporaryDirectory()
            root = Path(self.temp_directory.name)
            certificate = root / "device.crt"
            private_key = root / "device.key"
            subprocess.run(
                [
                    str(openssl),
                    "req",
                    "-x509",
                    "-newkey",
                    "rsa:2048",
                    "-nodes",
                    "-days",
                    "1",
                    "-subj",
                    "/CN=localhost",
                    "-keyout",
                    str(private_key),
                    "-out",
                    str(certificate),
                ],
                check=True,
                capture_output=True,
            )
            context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
            context.minimum_version = ssl.TLSVersion.TLSv1_2
            context.load_cert_chain(certificate, private_key)
            self.server.socket = context.wrap_socket(self.server.socket, server_side=True)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    def open_preview(self, *, tls: bool = False) -> tuple[_RawStreamClient, bytes]:
        client = _RawStreamClient(self.server.server_port, tls=tls)
        response = client.request_stream(
            "/api/v4/preview?fps=30",
            "multipart/x-mixed-replace",
            PREVIEW_PART,
        )
        return client, response

    def open_events(
        self,
        *,
        tls: bool = False,
        last_event_id: str | None = None,
    ) -> tuple[_RawStreamClient, bytes]:
        client = _RawStreamClient(self.server.server_port, tls=tls)
        response = client.request_stream(
            "/api/v4/capture/events",
            "text/event-stream",
            b"\n\n",
            last_event_id=last_event_id,
        )
        return client, response

    def open_network_events(
        self,
        *,
        tls: bool = False,
        last_event_id: str | None = None,
    ) -> tuple[_RawStreamClient, bytes]:
        client = _RawStreamClient(self.server.server_port, tls=tls)
        response = client.request_stream(
            "/api/v4/network/events",
            "text/event-stream",
            b"\n\n",
            last_event_id=last_event_id,
        )
        return client, response

    def assert_ok(self, response: bytes) -> None:
        self.assertTrue(response.startswith(b"HTTP/1.1 200 "), response[:200])

    def wait_for_no_streams(self, timeout: float = 1.0) -> None:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            snapshot = self.server.stream_lifecycle_snapshot()
            if all(value["active"] == 0 for value in snapshot.values()):
                return
            time.sleep(0.005)
        self.fail(f"stream sessions did not drain: {self.server.stream_lifecycle_snapshot()}")

    def test_frozen_preview_abort_reconnects_within_250_ms(self) -> None:
        self.start()
        first, first_response = self.open_preview()
        self.assert_ok(first_response)

        started = time.monotonic()
        first.abort()
        second, second_response = self.open_preview()
        elapsed = time.monotonic() - started
        try:
            self.assert_ok(second_response)
            self.assertLess(elapsed, 0.25)
        finally:
            second.abort()
        self.wait_for_no_streams()

        snapshot = self.server.stream_lifecycle_snapshot()["preview"]
        self.assertEqual(snapshot["rejected"], 0)
        self.assertGreaterEqual(snapshot["close_reasons"].get("peer_disconnect", 0), 2)

    def test_publishing_preview_during_abort_does_not_block_or_lose_latest_frame(self) -> None:
        self.start()
        first, first_response = self.open_preview()
        self.assert_ok(first_response)
        first.abort()

        latest = b"\xff\xd8continued-preview\xff\xd9"
        started = time.monotonic()
        self.provider.preview.publish(latest)
        second = _RawStreamClient(self.server.server_port, tls=False)
        response = second.request_stream(
            "/api/v4/preview?fps=30",
            "multipart/x-mixed-replace",
            latest,
        )
        try:
            self.assert_ok(response)
            self.assertIn(latest, response)
            self.assertLess(time.monotonic() - started, 0.25)
        finally:
            second.abort()

    def test_capture_sse_abort_reconnects_without_waiting_for_heartbeat(self) -> None:
        self.start()
        first, first_response = self.open_events()
        self.assert_ok(first_response)

        started = time.monotonic()
        first.abort()
        second, second_response = self.open_events()
        elapsed = time.monotonic() - started
        try:
            self.assert_ok(second_response)
            self.assertLess(elapsed, 0.25)
        finally:
            second.abort()
        self.wait_for_no_streams()

        snapshot = self.server.stream_lifecycle_snapshot()["sse"]
        self.assertEqual(snapshot["rejected"], 0)
        self.assertGreaterEqual(snapshot["close_reasons"].get("peer_disconnect", 0), 2)

    def test_network_sse_abort_reconnects_without_waiting_for_poll_or_heartbeat(self) -> None:
        self.start()
        first, first_response = self.open_network_events()
        self.assert_ok(first_response)

        started = time.monotonic()
        first.abort()
        second, second_response = self.open_network_events()
        elapsed = time.monotonic() - started
        try:
            self.assert_ok(second_response)
            self.assertLess(elapsed, 0.25)
        finally:
            second.abort()
        self.wait_for_no_streams()

        snapshot = self.server.stream_lifecycle_snapshot()["sse"]
        self.assertEqual(snapshot["rejected"], 0)
        self.assertGreaterEqual(snapshot["close_reasons"].get("peer_disconnect", 0), 2)

    def test_capture_and_network_sse_share_one_capacity_owner(self) -> None:
        self.start()
        capture, capture_response = self.open_events()
        self.assert_ok(capture_response)
        blocked, blocked_response = self.open_network_events()
        try:
            self.assertTrue(blocked_response.startswith(b"HTTP/1.1 503 "), blocked_response[:200])
            headers = blocked_response.partition(b"\r\n\r\n")[0].lower()
            self.assertIn(b"retry-after: 1\r\n", headers + b"\r\n")
        finally:
            blocked.abort()
            capture.abort()

        network, network_response = self.open_network_events()
        try:
            self.assert_ok(network_response)
            snapshot = self.server.stream_lifecycle_snapshot()["sse"]
            self.assertEqual(snapshot["active"], 1)
            self.assertEqual(snapshot["capacity"], 1)
        finally:
            network.abort()
        self.wait_for_no_streams()

    def test_capture_sse_last_event_id_replays_only_disconnected_interval(self) -> None:
        self.start()
        first, first_response = self.open_events()
        self.assert_ok(first_response)
        first_payload = first_response.partition(b"\r\n\r\n")[2]
        first_id = first_payload.split(b"\n", 1)[0].removeprefix(b"id: ").decode("ascii")
        first.abort()

        next_event = deepcopy(SNAPSHOT_EVENT)
        next_event["source_revision"] = 2
        next_event["occurred_at"] = "2026-09-05T00:00:01Z"
        published_id = self.event_buffer.publish(next_event)
        second, second_response = self.open_events(last_event_id=first_id)
        try:
            self.assert_ok(second_response)
            payload = second_response.partition(b"\r\n\r\n")[2]
            self.assertEqual(payload.count(b"\nevent: "), 1)
            self.assertIn(f"id: {published_id}\n".encode("ascii"), payload)
            self.assertNotIn(f"id: {first_id}\n".encode("ascii"), payload)
        finally:
            second.abort()

    def test_customer_tls_preview_and_sse_abort_reconnect_immediately(self) -> None:
        self.start(tls=True)
        for opener in (self.open_preview, self.open_events, self.open_network_events):
            with self.subTest(stream=opener.__name__):
                first, first_response = opener(tls=True)
                self.assert_ok(first_response)
                started = time.monotonic()
                first.abort()
                second, second_response = opener(tls=True)
                elapsed = time.monotonic() - started
                try:
                    self.assert_ok(second_response)
                    self.assertLess(elapsed, 0.25)
                finally:
                    second.abort()
                self.wait_for_no_streams()

    def test_live_streams_do_not_block_control_requests(self) -> None:
        self.start()
        preview, preview_response = self.open_preview()
        events, events_response = self.open_events()
        self.assert_ok(preview_response)
        self.assert_ok(events_response)
        try:
            streams = self.server.stream_lifecycle_snapshot()
            self.assertEqual(streams["preview"]["active"], 1)
            self.assertEqual(streams["sse"]["active"], 1)
            self.assertGreaterEqual(streams["preview"]["oldest_age_seconds"], 0.0)
            self.assertGreaterEqual(streams["sse"]["oldest_age_seconds"], 0.0)
            started = time.monotonic()
            request = Request(
                f"http://127.0.0.1:{self.server.server_port}/api/v4/capture/status",
                headers={"Authorization": "Bearer reader-token"},
            )
            with urlopen(request, timeout=1) as response:
                body = json.load(response)
            self.assertLess(time.monotonic() - started, 1.0)
            self.assertEqual(body["schema"], "ylx.capture-status.v4")
        finally:
            preview.abort()
            events.abort()

    def test_capacity_errors_are_retryable_503_with_retry_after(self) -> None:
        self.start()
        for opener in (self.open_preview, self.open_events, self.open_network_events):
            with self.subTest(stream=opener.__name__):
                active, active_response = opener()
                self.assert_ok(active_response)
                blocked, blocked_response = opener()
                try:
                    self.assertTrue(
                        blocked_response.startswith(b"HTTP/1.1 503 "), blocked_response[:200]
                    )
                    headers = blocked_response.partition(b"\r\n\r\n")[0].lower()
                    self.assertIn(b"retry-after: 1\r\n", headers + b"\r\n")
                finally:
                    blocked.abort()
                    active.abort()
                self.wait_for_no_streams()

    def test_one_thousand_abort_reconnects_leave_threads_and_fds_bounded(self) -> None:
        self.start()
        baseline_fds = len(os.listdir("/proc/self/fd"))
        baseline_threads = threading.active_count()
        openers = (self.open_preview, self.open_events, self.open_network_events)
        for index in range(1000):
            opener = openers[index % len(openers)]
            client, response = opener()
            self.assert_ok(response)
            client.abort()
        self.wait_for_no_streams(timeout=2.0)

        deadline = time.monotonic() + 1.0
        while time.monotonic() < deadline and any(
            thread.name.startswith("gateway-") for thread in threading.enumerate()
        ):
            time.sleep(0.01)
        self.assertFalse(
            [thread.name for thread in threading.enumerate() if thread.name.startswith("gateway-")]
        )
        self.assertLessEqual(len(os.listdir("/proc/self/fd")), baseline_fds + 2)
        self.assertLessEqual(threading.active_count(), baseline_threads + 2)
        snapshot = self.server.stream_lifecycle_snapshot()
        self.assertEqual(snapshot["preview"]["rejected"], 0)
        self.assertEqual(snapshot["sse"]["rejected"], 0)

    def test_server_shutdown_cancels_idle_streams_within_deadline(self) -> None:
        self.start()
        preview, preview_response = self.open_preview()
        events, events_response = self.open_events()
        self.assert_ok(preview_response)
        self.assert_ok(events_response)

        started = time.monotonic()
        self.server.shutdown()
        elapsed = time.monotonic() - started
        preview.socket.close()
        events.socket.close()
        self.assertLess(elapsed, 2.0)
        self.wait_for_no_streams()
        snapshot = self.server.stream_lifecycle_snapshot()
        self.assertGreaterEqual(snapshot["preview"]["close_reasons"]["server_shutdown"], 1)
        self.assertGreaterEqual(snapshot["sse"]["close_reasons"]["server_shutdown"], 1)


if __name__ == "__main__":
    unittest.main()

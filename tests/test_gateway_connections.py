from __future__ import annotations

import http.client
import os
import socket
import tempfile
import threading
import time
import unittest
from http.server import ThreadingHTTPServer
from unittest.mock import patch

from rp_ylx.api.gateway import create_gateway_server
from rp_ylx.api.security import SecurityPolicy
from rp_ylx.native import native_session_store_or_none
from tests.test_gateway import SESSION_ID, DeviceProvider, MemoryRepresentation


class GatewayConnectionsTest(unittest.TestCase):
    def setUp(self) -> None:
        self.server = create_gateway_server(
            "127.0.0.1",
            0,
            DeviceProvider(),
            security=SecurityPolicy.lab(allowed_operations={"getDevice", "getSessionArtifact"}),
            request_timeout_seconds=60,
        )
        self.server.max_http_connections = 2
        self.condition = threading.Condition()
        self.active = 0
        self.peak = 0
        original = self.server.finish_request

        def tracked(*args):
            with self.condition:
                self.active += 1
                self.peak = max(self.peak, self.active)
                self.condition.notify_all()
            try:
                original(*args)
            finally:
                with self.condition:
                    self.active -= 1
                    self.condition.notify_all()

        self.server.finish_request = tracked
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    def tearDown(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(2)
        self.assertFalse(self.thread.is_alive())

    def wait_active(self, expected: int) -> None:
        with self.condition:
            self.assertTrue(
                self.condition.wait_for(lambda: self.active == expected, timeout=1),
                f"active handlers: {self.active}; expected {expected}",
            )

    def request_device(self) -> None:
        client = http.client.HTTPConnection(*self.server.server_address, timeout=2)
        try:
            client.request("GET", "/api/v4/device", headers={"Connection": "close"})
            response = client.getresponse()
            self.assertEqual(response.status, 200)
            self.assertTrue(response.read())
        finally:
            client.close()

    def test_excess_connections_rejected_and_capacity_reused(self) -> None:
        # Active provider calls retain their slots; idle inputs may be reclaimed.
        clients = []
        release = threading.Event()
        entered = 0
        original = self.server.provider.device_descriptor

        def gated(*args):
            nonlocal entered
            with self.condition:
                entered += 1
                self.condition.notify_all()
            release.wait(3)
            return original(*args)

        self.server.provider.device_descriptor = gated
        try:
            for count in range(1, 3):
                client = socket.create_connection(self.server.server_address, timeout=1)
                clients.append(client)
                client.sendall(
                    b"GET /api/v4/device HTTP/1.1\r\nHost: local\r\nConnection: close\r\n\r\n"
                )
                with self.condition:
                    self.assertTrue(
                        self.condition.wait_for(
                            lambda expected=count: entered == expected, timeout=1
                        )
                    )
            with socket.create_connection(self.server.server_address, timeout=0.5) as excess:
                self.assertEqual(excess.recv(1), b"", "excess connection was not rejected")
            self.assertEqual(self.peak, 2)
            release.set()
            self.wait_active(0)
            self.request_device()
            self.assertLessEqual(self.peak, 2)
        finally:
            release.set()
            for client in clients:
                client.close()
        self.wait_active(0)

    def test_shutdown_releases_idle_requests_without_waiting_for_read_timeout(self) -> None:
        with socket.create_connection(self.server.server_address, timeout=2) as client:
            client.sendall(b"GET /api/v4/device HTTP/1.1\r\nHost: local")
            self.wait_active(1)
            begin = time.monotonic()
            self.server.shutdown()
            self.server.server_close()
            self.wait_active(0)
            self.assertLess(time.monotonic() - begin, 1)
            self.assertEqual(client.recv(1), b"")

    def test_shutdown_releases_blocked_download_and_representation(self) -> None:
        self.check_blocked_download(native=False)

    def test_shutdown_releases_blocked_native_download(self) -> None:
        with patch.object(ThreadingHTTPServer, "handle_error") as unexpected:
            self.check_blocked_download(native=True)
        unexpected.assert_not_called()

    def check_blocked_download(self, *, native: bool) -> None:
        store = native_session_store_or_none() if native else None
        if native and store is None:
            self.skipTest("native session store unavailable")
        entered = threading.Event()
        closed = threading.Event()
        payload = b"bounded-download" * 300000
        with tempfile.TemporaryFile() as source:
            source.write(payload)
            source.flush()

            class FileRepresentation(MemoryRepresentation):
                def send_to(self, descriptor, offset, length):
                    entered.set()
                    if store is not None:
                        return store.sendfile(descriptor, source.fileno(), offset, length)
                    sent = 0
                    while sent < length:
                        sent += os.sendfile(
                            descriptor, source.fileno(), offset + sent, length - sent
                        )
                    return sent

                def __exit__(self, *args):
                    closed.set()

            self.server.provider.artifact_io_state = lambda: None
            self.server.provider.open_verified_artifact = lambda *args: FileRepresentation(payload)
            original_get = self.server.get_request

            def small_send_buffer():
                connection, address = original_get()
                connection.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 16384)
                return connection, address

            self.server.get_request = small_send_buffer
            client = http.client.HTTPConnection(*self.server.server_address, timeout=2)
            try:
                client.request("GET", f"/api/v3/sessions/{SESSION_ID}/artifacts/{'a' * 64}")
                self.assertEqual(client.getresponse().status, 200)
                self.assertTrue(entered.wait(1))
                time.sleep(0.1)
                self.assertFalse(closed.is_set(), "receiver did not apply backpressure")
                begin = time.monotonic()
                self.server.shutdown()
                self.server.server_close()
                self.assertTrue(closed.wait(1), "blocked download retained representation")
                self.wait_active(0)
                self.assertLess(time.monotonic() - begin, 1)
            finally:
                client.close()

    def test_thread_start_failure_does_not_consume_connection_capacity(self) -> None:
        self.server.max_http_connections = 1
        left, right = socket.socketpair()
        try:
            with (
                patch.object(
                    ThreadingHTTPServer,
                    "process_request",
                    side_effect=RuntimeError("thread start failed"),
                ),
                self.assertRaisesRegex(RuntimeError, "thread start failed"),
            ):
                self.server.process_request(left, ("local", 0))
        finally:
            left.close()
            right.close()
        self.request_device()
        self.wait_active(0)

    def test_connection_limit_requires_positive_integer(self) -> None:
        for value in (0, -1, True, 1.5):
            with self.subTest(limit=value), self.assertRaises(ValueError):
                create_gateway_server(
                    "127.0.0.1",
                    0,
                    DeviceProvider(),
                    security=SecurityPolicy.lab(allowed_operations={"getDevice"}),
                    max_http_connections=value,
                )

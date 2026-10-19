from __future__ import annotations

import http.client
import os
import socket
import tempfile
import threading
import time
import unittest

from rp_ylx.api.gateway import create_gateway_server
from rp_ylx.api.security import SecurityPolicy
from tests.test_gateway import SESSION_ID, DeviceProvider, MemoryRepresentation


class GatewayIdleTest(unittest.TestCase):
    def setUp(self) -> None:
        self.server = create_gateway_server(
            "127.0.0.1",
            0,
            DeviceProvider(),
            security=SecurityPolicy.lab(
                allowed_operations={"getDevice", "startCapture", "getSessionArtifact"}
            ),
        )
        self.server.request_timeout_seconds = 0.15
        self.entered = threading.Event()
        self.finished = threading.Event()
        original = self.server.finish_request

        def tracked(*args):
            self.entered.set()
            try:
                original(*args)
            finally:
                self.finished.set()

        self.server.finish_request = tracked
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    def tearDown(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=2)
        self.assertFalse(self.thread.is_alive())

    def check_incomplete(self, payload: bytes, *, body: bool = False) -> None:
        with socket.create_connection(self.server.server_address, timeout=2) as client:
            if payload:
                client.sendall(payload)
            self.assertTrue(self.entered.wait(1))
            self.assertTrue(self.finished.wait(1), "abandoned request retained its handler")
            response = bytearray()
            while chunk := client.recv(4096):
                response.extend(chunk)
            if body:
                # An absolute input deadline may shut down the socket before
                # the idle-timeout error response can be written.
                self.assertTrue(not response or response.startswith(b"HTTP/1.1 400 "), response)
                self.assertEqual(self.server.provider.commands, {})
            else:
                self.assertEqual(response, b"")

    def test_idle_connection_expires(self) -> None:
        self.check_incomplete(b"")

    def test_partial_headers_expire(self) -> None:
        self.check_incomplete(b"GET /api/v4/device HTTP/1.1\r\nHost: local")

    def test_partial_json_body_expires(self) -> None:
        self.check_incomplete(
            b"POST /api/v4/capture/start HTTP/1.1\r\nHost: local\r\n"
            b"Content-Type: application/json\r\nContent-Length: 20\r\n"
            b"Idempotency-Key: idle-body-test\r\n\r\n{",
            body=True,
        )

    def test_keepalive_expires_after_successful_request(self) -> None:
        client = http.client.HTTPConnection(*self.server.server_address, timeout=2)
        try:
            client.request("GET", "/api/v4/device")
            response = client.getresponse()
            self.assertEqual(response.status, 200)
            response.read()
            self.assertTrue(self.finished.wait(1), "idle keepalive retained its handler")
            self.assertEqual(client.sock.recv(1), b"")
        finally:
            client.close()

    def test_connection_timeout_requires_finite_positive_value(self) -> None:
        for value in (0, -1, float("nan"), float("inf")):
            with self.subTest(timeout=value), self.assertRaises(ValueError):
                create_gateway_server(
                    "127.0.0.1",
                    0,
                    DeviceProvider(),
                    security=SecurityPolicy.lab(allowed_operations={"getDevice"}),
                    request_timeout_seconds=value,
                )

    def test_raw_descriptor_download_survives_receiver_backpressure(self) -> None:
        payload = b"native-download" * 300000
        entered_send = threading.Event()
        finished_send = threading.Event()
        errors = []
        with tempfile.TemporaryFile() as source:
            source.write(payload)
            source.flush()

            class FileRepresentation(MemoryRepresentation):
                def send_to(self, descriptor, offset, length):
                    entered_send.set()
                    try:
                        sent = 0
                        while sent < length:
                            sent += os.sendfile(
                                descriptor, source.fileno(), offset + sent, length - sent
                            )
                        return sent
                    except OSError as exc:
                        errors.append(repr(exc))
                        raise
                    finally:
                        if offset + length == len(payload):
                            finished_send.set()

            self.server.provider.artifact_io_state = lambda: None
            self.server.provider.open_verified_artifact = lambda *args: FileRepresentation(payload)
            original_get = self.server.get_request

            def small_send_buffer():
                connection, address = original_get()
                connection.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 16384)
                return connection, address

            self.server.get_request = small_send_buffer
            client = http.client.HTTPConnection(*self.server.server_address, timeout=5)
            try:
                client.request("GET", f"/api/v3/sessions/{SESSION_ID}/artifacts/{'a' * 64}")
                response = client.getresponse()
                self.assertEqual(response.status, 200)
                self.assertTrue(entered_send.wait(1))
                time.sleep(0.3)
                self.assertEqual(errors, [], "raw sendfile failed under receiver backpressure")
                self.assertFalse(finished_send.is_set(), "receiver did not apply backpressure")
                self.assertEqual(response.read(), payload)
                self.assertTrue(finished_send.wait(1))
                self.assertEqual(errors, [])
            finally:
                client.close()

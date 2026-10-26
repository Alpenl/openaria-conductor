from __future__ import annotations

import http.client
import json
import socket
import threading
import time
import unittest

from rp_ylx.api.gateway import create_gateway_server
from rp_ylx.api.security import SecurityPolicy
from tests.test_gateway import DeviceProvider


class GatewayReadDeadlineTest(unittest.TestCase):
    def setUp(self) -> None:
        self.provider = DeviceProvider()
        self.server = create_gateway_server(
            "127.0.0.1",
            0,
            self.provider,
            security=SecurityPolicy.lab(allowed_operations={"getDevice", "startCapture"}),
            request_timeout_seconds=0.3,
        )
        self.finished = threading.Event()
        original = self.server.finish_request

        def tracked(*args):
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
        self.thread.join(2)
        self.assertFalse(self.thread.is_alive())

    @staticmethod
    def headers(length: int) -> bytes:
        return (
            "POST /api/v3/capture/start HTTP/1.1\r\nHost: local\r\n"
            "Content-Type: application/json\r\n"
            f"Content-Length: {length}\r\nIdempotency-Key: input-budget\r\n\r\n"
        ).encode()

    def check_drip(self, prefix: bytes, *, header_delay: float = 0) -> None:
        done = threading.Event()
        with socket.create_connection(self.server.server_address, timeout=2) as client:
            if header_delay:
                client.sendall(prefix[:8])
                time.sleep(header_delay)
                client.sendall(prefix[8:])
            else:
                client.sendall(prefix)

            def drip():
                while not done.wait(0.05):
                    try:
                        client.sendall(b"x")
                    except OSError:
                        return

            sender = threading.Thread(target=drip, daemon=True)
            sender.start()
            try:
                wait = 0.6 if header_delay else 1
                self.assertTrue(self.finished.wait(wait), "trickle input retained the handler")
                self.assertEqual(self.provider.commands, {})
            finally:
                done.set()
                sender.join(1)
                self.assertFalse(sender.is_alive())

    def test_trickled_request_line_expires(self) -> None:
        self.check_drip(b"G")

    def test_trickled_headers_expire(self) -> None:
        self.check_drip(b"GET /api/v4/device HTTP/1.1\r\nHost: ")

    def test_trickled_body_expires_without_executing_command(self) -> None:
        self.check_drip(self.headers(2048) + b"{")

    def test_headers_and_body_share_one_input_budget(self) -> None:
        self.server.request_timeout_seconds = 1
        self.check_drip(self.headers(2048) + b"{", header_delay=0.65)

    def test_fragmented_valid_command_within_budget_runs_once(self) -> None:
        payload = json.dumps(
            {"schema": "ylx.capture-start.v2", "mode": "production", "take": {"kind": "new"}}
        ).encode()
        with socket.create_connection(self.server.server_address, timeout=2) as client:
            client.sendall(self.headers(len(payload)) + payload[:10])
            time.sleep(0.1)
            client.sendall(payload[10:])
            response = http.client.HTTPResponse(client)
            response.begin()
            self.assertEqual(response.status, 202)
            response.read()
            response.close()
            self.assertEqual(len(self.provider.commands), 1)

    def test_keepalive_idle_wait_does_not_consume_next_input_budget(self) -> None:
        self.server.request_timeout_seconds = 0.4
        with socket.create_connection(self.server.server_address, timeout=2) as client:
            for _ in range(2):
                time.sleep(0.25)
                client.sendall(b"GET /api/v4/device HTTP/1.1\r\nHost: ")
                time.sleep(0.25)
                client.sendall(b"local\r\n\r\n")
                response = http.client.HTTPResponse(client)
                response.begin()
                self.assertEqual(response.status, 200)
                response.read()
                response.close()

    def test_buffered_pipelined_requests_are_preserved(self) -> None:
        with socket.create_connection(self.server.server_address, timeout=2) as client:
            client.sendall(
                b"GET /api/v4/device HTTP/1.1\r\nHost: local\r\n\r\n"
                b"GET /api/v4/device HTTP/1.1\r\nHost: local\r\n"
                b"Connection: close\r\n\r\n"
            )
            received = bytearray()
            while chunk := client.recv(8192):
                received.extend(chunk)
            self.assertEqual(received.count(b"HTTP/1.1 200 OK\r\n"), 2)
            self.assertTrue(self.finished.wait(1))

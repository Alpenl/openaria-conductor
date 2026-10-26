from __future__ import annotations

import http.client
import json
import socket
import threading
import time
import unittest
from contextlib import contextmanager

from rp_ylx.api.gateway import create_gateway_server
from rp_ylx.api.security import SecurityPolicy
from tests.test_gateway import DeviceProvider


class GatewayAdmissionTest(unittest.TestCase):
    def setUp(self) -> None:
        self.provider = DeviceProvider()
        self.server = create_gateway_server(
            "127.0.0.1",
            0,
            self.provider,
            security=SecurityPolicy.lab(
                allowed_operations={"getDevice", "startCapture", "stopCapture"}
            ),
            max_http_connections=2,
            request_timeout_seconds=30,
        )
        self.condition = threading.Condition()
        self.active = self.peak = 0
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
        self.clients = []
        self.release = threading.Event()

    def tearDown(self) -> None:
        self.release.set()
        for client in self.clients:
            client.close()
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(2)
        self.assertFalse(self.thread.is_alive())
        self.assertLessEqual(self.peak, 2)

    def wait_active(self, expected):
        with self.condition:
            self.assertTrue(self.condition.wait_for(lambda: self.active == expected, timeout=1))

    def open_client(self, payload=b""):
        client = socket.create_connection(self.server.server_address, timeout=2)
        self.clients.append(client)
        if payload:
            client.sendall(payload)
        return client

    def query(self):
        client = http.client.HTTPConnection(*self.server.server_address, timeout=2)
        try:
            client.request("GET", "/api/v4/device", headers={"Connection": "close"})
            response = client.getresponse()
            self.assertEqual(response.status, 200)
            self.assertTrue(response.read())
        finally:
            client.close()

    def test_idle_connections_yield_capacity_for_query(self):
        first = self.open_client()
        self.wait_active(1)
        self.open_client()
        self.wait_active(2)
        start = time.monotonic()
        self.query()
        self.assertLess(time.monotonic() - start, 1)
        self.assertEqual(first.recv(1), b"")

    def test_partial_input_yields_capacity_for_stop_without_starting_capture(self):
        body_entered = threading.Event()
        original_read = self.server.request_read
        calls = {}

        @contextmanager
        def tracked_read(connection, expires_at):
            with original_read(connection, expires_at):
                calls[connection] = calls.get(connection, 0) + 1
                if calls[connection] == 2:
                    body_entered.set()
                yield

        self.server.request_read = tracked_read
        self.open_client(
            b"POST /api/v3/capture/start HTTP/1.1\r\nHost: local\r\n"
            b"Content-Type: application/json\r\nContent-Length: 2048\r\n"
            b"Idempotency-Key: partial-start\r\n\r\n{"
        )
        self.wait_active(1)
        self.assertTrue(body_entered.wait(1))
        self.open_client(b"GET /api/v4/device HTTP/1.1\r\nHost: ")
        self.wait_active(2)
        client = http.client.HTTPConnection(*self.server.server_address, timeout=2)
        try:
            client.request(
                "POST",
                "/api/v3/capture/stop",
                body=json.dumps({"schema": "ylx.capture-stop.v2", "reason": "user"}),
                headers={
                    "Content-Type": "application/json",
                    "Idempotency-Key": "saturated-stop",
                    "Connection": "close",
                },
            )
            response = client.getresponse()
            self.assertEqual(response.status, 204)
            response.read()
            self.assertEqual(len(self.provider.commands), 1)
            self.assertTrue(all("saturated-stop" in key for key in self.provider.commands))
        finally:
            client.close()

    def test_completed_keepalive_yields_capacity(self):
        for count in range(1, 3):
            client = self.open_client(b"GET /api/v4/device HTTP/1.1\r\nHost: local\r\n\r\n")
            response = http.client.HTTPResponse(client)
            response.begin()
            self.assertEqual(response.status, 200)
            response.read()
            response.close()
            self.wait_active(count)
        self.query()

    def test_active_provider_is_preserved_when_idle_connection_reclaimed(self):
        entered = threading.Event()
        original = self.provider.device_descriptor
        calls = 0

        def gated(*args):
            nonlocal calls
            calls += 1
            if calls == 1:
                entered.set()
                self.release.wait(3)
            return original(*args)

        self.provider.device_descriptor = gated
        active = self.open_client(
            b"GET /api/v4/device HTTP/1.1\r\nHost: local\r\nConnection: close\r\n\r\n"
        )
        self.assertTrue(entered.wait(1))
        idle = self.open_client()
        self.wait_active(2)
        self.query()
        self.assertEqual(idle.recv(1), b"")
        self.release.set()
        response = http.client.HTTPResponse(active)
        response.begin()
        self.assertEqual(response.status, 200)
        response.read()
        response.close()

    def test_capacity_remains_bounded_if_cancelled_handler_cleanup_stalls(self):
        original = self.server._http_finished
        entered = threading.Event()

        def gated(sock):
            entered.set()
            self.release.wait(3)
            original(sock)

        self.server._http_finished = gated
        self.open_client()
        self.wait_active(1)
        self.open_client()
        self.wait_active(2)
        with socket.create_connection(self.server.server_address, timeout=1) as excess:
            start = time.monotonic()
            self.assertEqual(excess.recv(1), b"")
            self.assertLess(time.monotonic() - start, 0.5)
        self.assertTrue(entered.wait(0.5))
        self.assertLessEqual(self.peak, 2)
        self.release.set()

    def test_cancelled_completed_body_never_executes_command(self):
        self.server.max_http_connections = 1
        read_complete = threading.Event()
        original_read = self.server.request_read
        calls = 0

        @contextmanager
        def pause_after_body(connection, expires_at):
            nonlocal calls
            calls += 1
            call_number = calls
            with original_read(connection, expires_at):
                yield
                if call_number == 2:
                    read_complete.set()
                    self.release.wait(3)

        self.server.request_read = pause_after_body
        payload = json.dumps(
            {"schema": "ylx.capture-start.v2", "mode": "production", "take": {"kind": "new"}}
        ).encode()
        headers = (
            "POST /api/v3/capture/start HTTP/1.1\r\nHost: local\r\n"
            "Content-Type: application/json\r\n"
            f"Content-Length: {len(payload)}\r\nIdempotency-Key: cancelled-body\r\n\r\n"
        ).encode()
        self.open_client(headers + payload)
        self.assertTrue(read_complete.wait(1))
        with socket.create_connection(self.server.server_address, timeout=1) as excess:
            self.assertEqual(excess.recv(1), b"")
        self.release.set()
        self.wait_active(0)
        self.assertEqual(self.provider.commands, {})
        self.query()
        self.assertLessEqual(self.peak, 1)

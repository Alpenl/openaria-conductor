from __future__ import annotations

import http.client
import json
import socket
import threading
import unittest

from rp_ylx.api.downloads import ArtifactAccessError
from rp_ylx.api.gateway import create_gateway_server
from rp_ylx.api.security import SecurityPolicy
from tests.test_gateway import SESSION_ID, DeviceProvider, MemoryRepresentation


class GatewayDownloadCapacityTest(unittest.TestCase):
    def setUp(self):
        self.provider = DeviceProvider()
        self.provider.artifact_io_state = lambda: None
        self.server = create_gateway_server(
            "127.0.0.1",
            0,
            self.provider,
            security=SecurityPolicy.lab(
                allowed_operations={
                    "getDevice",
                    "stopCapture",
                    "getSession",
                    "getSessionArtifact",
                    "headSessionArtifact",
                }
            ),
        )
        self.server.max_session_downloads = 1
        self.payload = b" " * (4 * 1024 * 1024 - 2) + b"{}"
        self.closed = threading.Event()
        self.opened = 0
        self.release = threading.Event()
        owner = self

        class Representation(MemoryRepresentation):
            def __exit__(self, *args):
                owner.closed.set()

        def open_representation(*args):
            self.opened += 1
            return Representation(self.payload)

        self.provider.open_verified_artifact = open_representation
        self.provider.open_manifest = open_representation
        original = self.server.get_request

        def small_send_buffer():
            connection, address = original()
            connection.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 16384)
            return connection, address

        self.server.get_request = small_send_buffer
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.clients = []
        self.artifact = f"/api/v3/sessions/{SESSION_ID}/artifacts/{'a' * 64}"
        self.manifest = f"/api/v3/sessions/{SESSION_ID}"

    def tearDown(self):
        self.release.set()
        for client, response in self.clients:
            response.close()
            client.close()
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(2)
        self.assertFalse(self.thread.is_alive())

    def blocked_download(self, path):
        client = http.client.HTTPConnection(*self.server.server_address, timeout=2)
        client.connect()
        client.sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 16384)
        client.request("GET", path, headers={"Connection": "close"})
        response = client.getresponse()
        self.clients.append((client, response))
        self.assertEqual(response.status, 200)
        self.assertFalse(self.closed.is_set())
        return client, response

    def request(self, method, path, body=None, headers=None):
        client = http.client.HTTPConnection(*self.server.server_address, timeout=2)
        try:
            client.request(
                method, path, body=body, headers={"Connection": "close", **(headers or {})}
            )
            response = client.getresponse()
            return response.status, dict(response.getheaders()), response.read()
        finally:
            client.close()

    def assert_capacity(self, method, path):
        status, headers, body = self.request(method, path)
        self.assertEqual(status, 503)
        self.assertEqual(headers["Retry-After"], "1")
        self.assertEqual(headers["YLX-Error-Code"], "download_capacity_exhausted")
        if method == "HEAD":
            self.assertEqual(body, b"")
        else:
            error = json.loads(body)["error"]
            self.assertEqual(error["code"], "download_capacity_exhausted")
            self.assertTrue(error["retryable"])

    def test_stalled_artifact_shares_limit_with_manifest_and_head(self):
        self.blocked_download(self.artifact)
        for method, path in [
            ("HEAD", self.artifact),
            ("GET", self.manifest),
            ("GET", self.artifact),
        ]:
            self.assert_capacity(method, path)
        self.assertEqual(self.opened, 1)
        self.assertEqual(self.request("GET", "/api/v4/device")[0], 200)
        status, _, _ = self.request(
            "POST",
            "/api/v3/capture/stop",
            json.dumps({"schema": "ylx.capture-stop.v2", "reason": "user"}),
            {"Content-Type": "application/json", "Idempotency-Key": "download-stop"},
        )
        self.assertEqual(status, 204)
        self.assertEqual(len(self.provider.commands), 1)

    def test_stalled_manifest_reserves_same_limit(self):
        self.blocked_download(self.manifest)
        self.assert_capacity("HEAD", self.artifact)
        self.assertEqual(self.opened, 1)

    def test_provider_open_counts_before_headers(self):
        entered = threading.Event()
        original = self.provider.open_verified_artifact

        def gated(*args):
            entered.set()
            self.release.wait(3)
            return original(*args)

        self.provider.open_verified_artifact = gated
        client = http.client.HTTPConnection(*self.server.server_address, timeout=2)
        try:
            client.request("HEAD", self.artifact, headers={"Connection": "close"})
            self.assertTrue(entered.wait(1))
            self.assert_capacity("GET", self.manifest)
            self.assertEqual(self.opened, 0)
            self.release.set()
            response = client.getresponse()
            self.assertEqual(response.status, 200)
            response.read()
        finally:
            self.release.set()
            client.close()

    def test_disconnect_releases_capacity(self):
        client, response = self.blocked_download(self.artifact)
        response.close()
        client.close()
        self.assertTrue(self.closed.wait(1))
        self.assertEqual(self.request("HEAD", self.artifact)[0], 200)

    def test_provider_failure_releases_capacity(self):
        original = self.provider.open_verified_artifact

        def fail(*args):
            raise ArtifactAccessError("not_found", "missing")

        self.provider.open_verified_artifact = fail
        self.assertEqual(self.request("HEAD", self.artifact)[0], 404)
        self.provider.open_verified_artifact = original
        self.assertEqual(self.request("HEAD", self.artifact)[0], 200)

    def test_rejected_range_releases_capacity(self):
        self.assertEqual(
            self.request("GET", self.artifact, headers={"Range": "bytes=999999999-"})[0], 416
        )
        self.assertEqual(self.request("HEAD", self.artifact)[0], 200)

    def test_limit_requires_positive_integer(self):
        for value in [0, -1, True, 1.5]:
            with self.subTest(value=value), self.assertRaises(ValueError):
                create_gateway_server(
                    "127.0.0.1",
                    0,
                    DeviceProvider(),
                    security=SecurityPolicy.lab(allowed_operations={"getDevice"}),
                    max_session_downloads=value,
                )

    def test_download_limit_leaves_slot_when_http_total_is_smaller(self):
        self.server.max_http_connections = 2
        self.server.max_session_downloads = 8
        self.blocked_download(self.artifact)
        self.assert_capacity("HEAD", self.artifact)
        self.assertEqual(self.request("GET", "/api/v4/device")[0], 200)

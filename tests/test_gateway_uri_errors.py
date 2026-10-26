from __future__ import annotations

import http.client
import json
import socket
import sys
import threading
import unittest

from rp_ylx.api.gateway import create_gateway_server
from rp_ylx.api.security import SecurityPolicy
from tests.test_gateway import DeviceProvider


class GatewayUriErrorsTest(unittest.TestCase):
    def setUp(self):
        self.provider = DeviceProvider()
        self.server = create_gateway_server(
            "127.0.0.1",
            0,
            self.provider,
            security=SecurityPolicy.lab(
                allowed_operations={"getDevice", "startCapture", "stopCapture"}
            ),
        )
        self.errors = []
        self.server.handle_error = lambda *_: self.errors.append(type(sys.exc_info()[1]).__name__)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(2)
        self.assertFalse(self.thread.is_alive())

    def raw(self, method, target, tail=b"", headers=b""):
        with socket.create_connection(self.server.server_address, timeout=2) as connection:
            connection.sendall(
                method.encode() + b" " + target.encode() + b" HTTP/1.1\r\n"
                b"Host: local\r\nConnection: close\r\n" + headers + b"\r\n" + tail
            )
            response = http.client.HTTPResponse(connection, method=method)
            response.begin()
            return response.status, response.read(), dict(response.getheaders())

    def healthy(self):
        code, body, _ = self.raw("GET", "/api/v4/device")
        self.assertEqual(code, 200)
        self.assertEqual(json.loads(body)["schema"], "ylx.device.v4")

    def test_malformed_target_returns_error_for_all_supported_methods(self):
        for target in ("http://[/api/v4/device", "http://[bad]/api/v4/device"):
            for method in ("GET", "HEAD", "OPTIONS", "POST", "PUT", "PATCH", "DELETE"):
                with self.subTest(target=target, method=method):
                    code, body, headers = self.raw(method, target)
                    self.assertEqual(code, 400)
                    self.assertEqual(headers["Connection"], "close")
                    if method == "HEAD":
                        self.assertEqual(body, b"")
                    else:
                        self.assertEqual(json.loads(body)["error"]["code"], "invalid_request")
        self.assertEqual(self.provider.commands, {})
        self.healthy()
        self.assertEqual(self.errors, [])

    def test_bad_target_with_unread_body_never_runs_pipelined_command(self):
        body = b'{"schema":"ylx.capture-start.v2","mode":"production","take":{"kind":"new"}}'
        pipeline = (
            b"POST /api/v4/capture/start HTTP/1.1\r\nHost: local\r\n"
            b"Content-Type: application/json\r\nIdempotency-Key: must-not-run\r\n"
            + b"Content-Length: "
            + str(len(body)).encode()
            + b"\r\n\r\n"
            + body
        )
        code, _, headers = self.raw(
            "POST",
            "http://[/api/v4/capture/start",
            body + pipeline,
            b"Content-Length: " + str(len(body)).encode() + b"\r\n",
        )
        self.assertEqual(code, 400)
        self.assertEqual(headers["Connection"], "close")
        self.assertEqual(self.provider.commands, {})
        self.assertEqual(self.errors, [])

    def test_malformed_origin_get_returns_forbidden_and_keepalive_stays_usable(self):
        client = http.client.HTTPConnection(*self.server.server_address, timeout=2)
        try:
            for origin in ("http://[", "http://[bad]"):
                client.request("GET", "/api/v4/device", headers={"Origin": origin})
                response = client.getresponse()
                self.assertEqual(response.status, 403)
                self.assertEqual(json.loads(response.read())["error"]["code"], "origin_forbidden")
                client.request("GET", "/api/v4/device")
                response = client.getresponse()
                self.assertEqual(response.status, 200)
                response.read()
        finally:
            client.close()
        self.assertEqual(self.errors, [])

    def test_malformed_origin_post_closes_unread_body_without_command(self):
        payload = b'{"schema":"ylx.capture-start.v2","mode":"production","take":{"kind":"new"}}'
        code, body, headers = self.raw(
            "POST",
            "/api/v4/capture/start",
            payload,
            b"Origin: http://[\r\nContent-Type: application/json\r\n"
            b"Idempotency-Key: bad-origin\r\nContent-Length: "
            + str(len(payload)).encode()
            + b"\r\n",
        )
        self.assertEqual(code, 403)
        self.assertEqual(json.loads(body)["error"]["code"], "origin_forbidden")
        self.assertEqual(headers["Connection"], "close")
        self.assertEqual(self.provider.commands, {})
        self.healthy()
        self.assertEqual(self.errors, [])

    def test_valid_absolute_targets_still_route(self):
        for target in ("http://local/api/v4/device", "http://[::1]/api/v4/device"):
            with self.subTest(target=target):
                self.assertEqual(self.raw("GET", target)[0], 200)
        self.assertEqual(self.errors, [])

    def test_valid_same_origin_still_routes(self):
        code, body, _ = self.raw("GET", "/api/v4/device", headers=b"Origin: http://local\r\n")
        self.assertEqual(code, 200)
        self.assertEqual(json.loads(body)["schema"], "ylx.device.v4")
        self.assertEqual(self.errors, [])

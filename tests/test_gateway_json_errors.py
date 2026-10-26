from __future__ import annotations

import http.client
import json
import sys
import threading
import unittest

from rp_ylx.api.gateway import create_gateway_server
from rp_ylx.api.security import SecurityPolicy
from tests.test_gateway import DeviceProvider


class GatewayJsonErrorsTest(unittest.TestCase):
    def setUp(self) -> None:
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
        self.server.handle_error = lambda *_: self.errors.append(type(sys.exception()).__name__)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    def tearDown(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(2)
        self.assertFalse(self.thread.is_alive())

    def reject(self, version: str, operation: str, payload: bytes) -> None:
        before = dict(self.provider.commands)
        previous_errors = len(self.errors)
        client = http.client.HTTPConnection(*self.server.server_address, timeout=2)
        try:
            client.request(
                "POST",
                f"/api/{version}/capture/{operation}",
                body=payload,
                headers={"Content-Type": "application/json", "Idempotency-Key": "invalid-json"},
            )
            response = client.getresponse()
            self.assertEqual(response.status, 400)
            self.assertEqual(json.loads(response.read())["error"]["code"], "invalid_request")
            client.request("GET", f"/api/{version}/device")
            response = client.getresponse()
            self.assertEqual(response.status, 200)
            response.read()
            self.assertEqual(self.provider.commands, before)
            self.assertEqual(self.errors[previous_errors:], [])
        finally:
            client.close()

    def valid_command(self) -> None:
        client = http.client.HTTPConnection(*self.server.server_address, timeout=2)
        try:
            body = {"schema": "ylx.capture-start.v2", "mode": "production", "take": {"kind": "new"}}
            client.request(
                "POST",
                "/api/v4/capture/start",
                body=json.dumps(body),
                headers={
                    "Content-Type": "application/json",
                    "Idempotency-Key": "valid-after-invalid",
                },
            )
            response = client.getresponse()
            self.assertEqual(response.status, 202)
            response.read()
            self.assertEqual(len(self.provider.commands), 1)
        finally:
            client.close()

    def test_excessively_nested_json_returns_structured_error(self) -> None:
        depth = max(sys.getrecursionlimit() * 2, 10000)
        for version in ("v3", "v4"):
            for prefix, suffix in ((b"[", b"]"), (b'{"x":', b"}")):
                with self.subTest(version=version, prefix=prefix):
                    self.reject(
                        version, "start", b'{"x":' + prefix * depth + b"0" + suffix * depth + b"}"
                    )
        self.valid_command()

    def test_overlong_integer_returns_structured_error(self) -> None:
        old_limit = sys.get_int_max_str_digits()
        try:
            sys.set_int_max_str_digits(4300)
            for version in ("v3", "v4"):
                with self.subTest(version=version):
                    self.reject(version, "start", b'{"x":' + b"9" * 5000 + b"}")
        finally:
            sys.set_int_max_str_digits(old_limit)
        self.valid_command()

    def test_invalid_start_mode_types_return_structured_error(self) -> None:
        for version in ("v3", "v4"):
            for value in ([], {}, ["production"], None, 0, True):
                with self.subTest(version=version, value=value):
                    self.reject(
                        version,
                        "start",
                        json.dumps(
                            {
                                "schema": "ylx.capture-start.v2",
                                "mode": value,
                                "take": {"kind": "new"},
                            }
                        ).encode(),
                    )
        self.valid_command()

    def test_invalid_stop_reason_types_return_structured_error(self) -> None:
        for version in ("v3", "v4"):
            for value in ([], {}, ["user"], None, 0, True):
                with self.subTest(version=version, value=value):
                    self.reject(
                        version,
                        "stop",
                        json.dumps({"schema": "ylx.capture-stop.v2", "reason": value}).encode(),
                    )
        self.valid_command()

from __future__ import annotations

import http.client
import json
import sys
import threading
import unittest

from rp_ylx.api.gateway import create_gateway_server
from rp_ylx.api.security import SecurityPolicy
from tests.test_gateway import DeviceProvider


class GatewayJsonUnicodeTest(unittest.TestCase):
    def setUp(self):
        self.provider = DeviceProvider()
        self.server = create_gateway_server(
            "127.0.0.1",
            0,
            self.provider,
            security=SecurityPolicy.lab(
                allowed_operations={
                    "getDevice",
                    "startCapture",
                    "createNetworkCredentialReference",
                }
            ),
        )
        self.errors = []
        self.server.handle_error = lambda *_: self.errors.append(type(sys.exc_info()[1]).__name__)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.client = http.client.HTTPConnection(*self.server.server_address, timeout=2)

    def tearDown(self):
        self.client.close()
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(2)
        self.assertFalse(self.thread.is_alive())

    def post(self, path, payload, key="unicode-test"):
        self.client.request(
            "POST",
            path,
            body=payload,
            headers={
                "Content-Type": "application/json",
                "Idempotency-Key": key,
            },
        )
        response = self.client.getresponse()
        return response.status, json.loads(response.read())

    def start_body(self, name):
        return {
            "schema": "ylx.capture-start.v2",
            "mode": "production",
            "take": {"kind": "new"},
            "display_name": name,
        }

    def healthy(self):
        self.client.request("GET", "/api/v4/device")
        response = self.client.getresponse()
        self.assertEqual(response.status, 200)
        self.assertEqual(json.loads(response.read())["schema"], "ylx.device.v4")

    def assert_rejected(self, path, body):
        status, result = self.post(path, json.dumps(body).encode())
        self.assertEqual(status, 400)
        self.assertEqual(result["error"]["code"], "invalid_request")
        self.assertEqual(self.provider.commands, {})
        self.assertEqual(self.provider.network_credentials, [])
        self.healthy()
        self.assertEqual(self.errors, [])

    def test_unpaired_surrogates_in_capture_name_return_400(self):
        for name in ("\ud800", "\udfff", "a\ud800z", "\udfff\ud800"):
            with self.subTest(name=ascii(name)):
                self.assert_rejected("/api/v4/capture/start", self.start_body(name))

    def test_unpaired_surrogates_in_credentials_return_400(self):
        for passphrase in ("password\ud800", "password\udfff"):
            with self.subTest(passphrase=ascii(passphrase)):
                self.assert_rejected(
                    "/api/v4/network/credentials",
                    {
                        "schema": "ylx.network-credential-request.v1",
                        "passphrase": passphrase,
                    },
                )

    def test_surrogates_in_nested_values_and_keys_rejected(self):
        for value in ({"\ud800": "value"}, [{"name": "\udfff"}]):
            with self.subTest(value=ascii(value)):
                self.assert_rejected("/api/v4/capture/start", {"extra": value})

    def test_valid_unicode_capture_names_and_paired_escapes_preserved(self):
        for index, escaped in enumerate((False, True)):
            name = "中文录像🎥"
            payload = json.dumps(self.start_body(name), ensure_ascii=escaped).encode()
            status, _ = self.post("/api/v4/capture/start", payload, key=f"valid-{index}")
            self.assertEqual(status, 202)
            canonical, _ = next(reversed(self.provider.commands.values()))
            self.assertEqual(json.loads(canonical)["display_name"], name)
        self.assertEqual(len(self.provider.commands), 2)
        self.assertEqual(self.errors, [])

    def test_valid_unicode_credentials_preserved(self):
        phrase = "测试密码🎥"
        status, _ = self.post(
            "/api/v4/network/credentials",
            json.dumps(
                {
                    "schema": "ylx.network-credential-request.v1",
                    "passphrase": phrase,
                }
            ).encode(),
        )
        self.assertEqual(status, 201)
        self.assertEqual(self.provider.network_credentials[0].passphrase, phrase)
        self.assertEqual(self.errors, [])

    def test_bad_request_does_not_reserve_idempotency_key(self):
        self.assert_rejected("/api/v4/capture/start", self.start_body("\ud800"))
        payload = json.dumps(self.start_body("valid")).encode()
        status, _ = self.post("/api/v4/capture/start", payload)
        self.assertEqual(status, 202)
        self.assertEqual(len(self.provider.commands), 1)
        self.assertEqual(self.errors, [])

    def test_invalid_utf8_surrogate_bytes_return_400(self):
        payload = json.dumps(self.start_body("\ud800"), ensure_ascii=False).encode(
            "utf-8", errors="surrogatepass"
        )
        status, result = self.post("/api/v4/capture/start", payload)
        self.assertEqual(status, 400)
        self.assertEqual(result["error"]["code"], "invalid_request")
        self.assertEqual(self.provider.commands, {})
        self.healthy()
        self.assertEqual(self.errors, [])

    def test_existing_utf16_json_decoding_preserved(self):
        name = "中文🎥"
        payload = json.dumps(self.start_body(name), ensure_ascii=False).encode("utf-16")
        status, _ = self.post("/api/v4/capture/start", payload)
        self.assertEqual(status, 202)
        canonical, _ = next(iter(self.provider.commands.values()))
        self.assertEqual(json.loads(canonical)["display_name"], name)
        self.assertEqual(self.errors, [])

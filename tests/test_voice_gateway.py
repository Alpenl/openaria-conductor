from __future__ import annotations

import json
import threading
import unittest
from unittest.mock import Mock
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from rp_ylx.api import Principal, ProviderError, SecurityPolicy, create_gateway_server
from rp_ylx.daemon import LAB_OPERATIONS


class VoiceGatewayTest(unittest.TestCase):
    def setUp(self):
        self.provider = Mock()
        self.status = {"schema": "ylx.voice.v1", "enabled": True, "volume": 50}
        self.provider.voice_status.return_value = self.status
        self.provider.set_voice_volume.return_value = self.status
        policy = SecurityPolicy.customer(
            tokens={
                "owner": Principal("owner", permissions={"getVoice": None, "setVoiceVolume": None}),
                "viewer": Principal("viewer", permissions={"getVoice": None}),
            },
            allowed_origins={"http://device.test"},
            csrf_token="csrf",
        )
        self.server = create_gateway_server("127.0.0.1", 0, self.provider, security=policy)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.addCleanup(self.server.server_close)
        self.addCleanup(self.server.shutdown)
        self.url = f"http://127.0.0.1:{self.server.server_address[1]}/api/v4/voice"

    def request(self, body=None, token="owner", csrf="csrf"):
        headers = {
            "Authorization": f"Bearer {token}",
            "Origin": "http://device.test",
            "X-CSRF-Token": csrf,
            "Content-Type": "application/json",
        }
        data = None if body is None else json.dumps(body).encode()
        try:
            with urlopen(Request(self.url, data=data, headers=headers), timeout=3) as response:
                return response.status, json.load(response)
        except HTTPError as error:
            return error.code, json.load(error)

    def test_read_write_and_permissions(self):
        body = {"schema": "ylx.voice-set.v1", "volume": 50}
        self.assertEqual(self.request(), (200, self.status))
        self.assertEqual(self.request(body)[0], 200)
        self.provider.set_voice_volume.assert_called_once_with(50)
        self.assertEqual(self.request(body, token="viewer")[0], 403)
        self.assertEqual(self.request(body, csrf="wrong")[0], 403)
        self.assertTrue({"getVoice", "setVoiceVolume"} <= LAB_OPERATIONS)

    def test_bad_input_and_provider_failures(self):
        body = {"schema": "ylx.voice-set.v1", "volume": 50}
        for volume in (-1, 101, True, 0.5, "50", None):
            self.assertEqual(self.request({**body, "volume": volume})[0], 400)
        self.assertEqual(self.request({**body, "extra": 1})[0], 400)
        self.provider.set_voice_volume.assert_not_called()
        self.provider.set_voice_volume.side_effect = ProviderError(
            "capture_busy", "busy", status=409
        )
        self.assertEqual(self.request(body)[0], 409)
        self.provider.set_voice_volume.side_effect = OSError("disk full")
        self.assertEqual(self.request(body)[0], 500)
        self.provider.voice_status.return_value = {**self.status, "volume": True}
        self.assertEqual(self.request()[0], 500)

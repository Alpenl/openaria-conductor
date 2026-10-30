from __future__ import annotations

import json
import threading
import unittest
from unittest.mock import Mock
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from rp_ylx.api import Principal, ProviderError, SecurityPolicy, create_gateway_server
from rp_ylx.capture_timer import valid_timer_status
from rp_ylx.daemon import LAB_OPERATIONS


class CaptureTimerGatewayTest(unittest.TestCase):
    def setUp(self):
        self.provider = Mock()
        self.status = {
            "schema": "ylx.capture-timer.v1",
            "settings": {"enabled": False, "duration_seconds": 600},
            "active": None,
        }
        self.provider.capture_timer_status.return_value = self.status
        self.provider.set_capture_timer.return_value = self.status
        policy = SecurityPolicy.customer(
            tokens={
                "owner": Principal(
                    "owner", permissions={"getCaptureTimer": None, "setCaptureTimer": None}
                ),
                "viewer": Principal("viewer", permissions={"getCaptureTimer": None}),
            },
            allowed_origins={"http://device.test"},
            csrf_token="csrf",
        )
        self.server = create_gateway_server("127.0.0.1", 0, self.provider, security=policy)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.addCleanup(self.server.server_close)
        self.addCleanup(self.server.shutdown)
        self.url = f"http://127.0.0.1:{self.server.server_address[1]}/api/v4/capture/timer"

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

    def test_get_post_and_lab_permissions(self):
        self.assertEqual(self.request(), (200, self.status))
        body = {"schema": "ylx.capture-timer-set.v1", "enabled": True, "duration_seconds": 600}
        self.assertEqual(self.request(body)[0], 200)
        self.provider.set_capture_timer.assert_called_once_with(
            {"enabled": True, "duration_seconds": 600}
        )
        self.assertTrue({"getCaptureTimer", "setCaptureTimer"} <= LAB_OPERATIONS)

    def test_permissions_and_csrf_are_enforced(self):
        body = {"schema": "ylx.capture-timer-set.v1", "enabled": True, "duration_seconds": 600}
        self.assertEqual(self.request(body, token="viewer")[0], 403)
        self.assertEqual(self.request(body, csrf="wrong")[0], 403)
        self.assertEqual(self.request(token="unknown")[0], 401)
        self.provider.set_capture_timer.assert_not_called()

    def test_invalid_inputs_never_reach_provider(self):
        good = {"schema": "ylx.capture-timer-set.v1", "enabled": True, "duration_seconds": 600}
        for patch in [
            {"duration_seconds": True},
            {"duration_seconds": 1.5},
            {"duration_seconds": 0},
            {"duration_seconds": 86401},
            {"enabled": 1},
            {"extra": 1},
            {"schema": "wrong"},
        ]:
            with self.subTest(patch=patch):
                self.assertEqual(self.request({**good, **patch})[0], 400)
        self.provider.set_capture_timer.assert_not_called()

    def test_busy_and_persistence_errors_remain_visible(self):
        body = {"schema": "ylx.capture-timer-set.v1", "enabled": True, "duration_seconds": 600}
        self.provider.set_capture_timer.side_effect = ProviderError(
            "capture_busy", "busy", status=409
        )
        self.assertEqual(self.request(body)[0], 409)
        self.provider.set_capture_timer.side_effect = OSError("disk full")
        self.assertEqual(self.request(body)[0], 500)

    def test_invalid_provider_status_rejected(self):
        self.provider.capture_timer_status.return_value = {
            **self.status,
            "active": {
                "session_id": "take",
                "duration_seconds": 600,
                "remaining_seconds": float("nan"),
            },
        }
        self.assertEqual(self.request()[0], 500)
        self.assertFalse(valid_timer_status({**self.status, "settings": None}))

    def test_preflight_allows_get_and_post(self):
        request = Request(
            self.url,
            method="OPTIONS",
            headers={
                "Origin": "http://device.test",
                "Access-Control-Request-Method": "POST",
                "Access-Control-Request-Headers": "Content-Type,X-CSRF-Token,Authorization",
            },
        )
        with urlopen(request, timeout=3) as response:
            self.assertIn("POST", response.headers["Access-Control-Allow-Methods"])

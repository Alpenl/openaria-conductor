from __future__ import annotations

import json
import threading
import unittest
import uuid
from unittest.mock import Mock, patch
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from rp_ylx.api import Principal, SecurityPolicy, create_gateway_server
from rp_ylx.daemon import LAB_OPERATIONS
from rp_ylx.firmware_control import FirmwareError


class FirmwareGatewayTest(unittest.TestCase):
    def setUp(self):
        policy = SecurityPolicy.customer(
            tokens={
                "owner": Principal(
                    "owner", permissions={"firmwareStatus": None, "firmwareUpdate": None}
                ),
                "viewer": Principal("viewer", permissions={"firmwareStatus": None}),
            },
            allowed_origins={"http://device.test"},
            csrf_token="csrf",
        )
        self.server = create_gateway_server("127.0.0.1", 0, Mock(), security=policy)
        thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        thread.start()
        self.addCleanup(self.server.server_close)
        self.addCleanup(self.server.shutdown)
        p = patch("rp_ylx.api.gateway.request_firmware", return_value={"schema": "test"})
        self.control = p.start()
        self.addCleanup(p.stop)

    def call(self, path, body=None, headers=None):
        request = Request(
            f"http://127.0.0.1:{self.server.server_port}/api/v4/firmware{path}",
            data=json.dumps(body).encode() if body is not None else None,
            headers=headers or {},
        )
        try:
            response = urlopen(request, timeout=3)
        except HTTPError as error:
            response = error
        with response:
            return response.status, json.loads(response.read())

    def test_lab_default_permissions_include_firmware_operations(self):
        self.assertTrue({"firmwareStatus", "firmwareUpdate"}.issubset(LAB_OPERATIONS))

    def test_reads_require_auth_and_checks_validate_force(self):
        self.assertEqual(self.call("")[0], 401)
        headers = {"Authorization": "Bearer viewer"}
        self.assertEqual(self.call("/check?force=true", headers=headers)[0], 200)
        self.control.assert_called_once_with("check", force=True)
        self.assertEqual(self.call("/check?force=wrong", headers=headers)[0], 400)

    def test_mutations_require_permission_origin_csrf_idempotency_and_pinned_commit(self):
        key = str(uuid.uuid4())
        body = {"commit": "a" * 40}
        headers = {"Authorization": "Bearer viewer", "Content-Type": "application/json"}
        self.assertEqual(self.call("/update", body, headers)[0], 403)
        headers["Authorization"] = "Bearer owner"
        self.assertEqual(self.call("/update", body, headers)[0], 403)
        headers.update({"Origin": "http://device.test", "X-CSRF-Token": "csrf"})
        self.assertEqual(self.call("/update", body, headers)[0], 400)
        headers["Idempotency-Key"] = key
        self.assertEqual(self.call("/update", {**body, "command": "id"}, headers)[0], 400)
        self.control.assert_not_called()
        self.assertEqual(self.call("/update", body, headers)[0], 202)
        self.control.assert_called_once_with("update", key=key, commit="a" * 40)

    def test_supervisor_failure_is_explicit_not_latest_or_success(self):
        self.control.side_effect = FirmwareError(
            "update_service_unavailable", "更新服务不可用", 503
        )
        status, body = self.call("", headers={"Authorization": "Bearer owner"})
        self.assertEqual(status, 503)
        self.assertEqual(body["error"]["code"], "update_service_unavailable")

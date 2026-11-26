from __future__ import annotations

import json
import threading
import unittest
import uuid
from copy import deepcopy
from unittest.mock import patch
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from rp_ylx.api import Principal, SecurityPolicy, create_gateway_server
from rp_ylx.daemon import LAB_OPERATIONS
from rp_ylx.firmware_control import FirmwareError
from tests.testkit import FIRMWARE_STATUS, firmware_task


class _EmptyProvider:
    pass


class _FirmwareProvider:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, object]]] = []

    def firmware_status(self) -> dict[str, object]:
        self.calls.append(("status", {}))
        return deepcopy(FIRMWARE_STATUS)

    def firmware_check(self, *, force: bool = False) -> dict[str, object]:
        self.calls.append(("check", {"force": force}))
        return deepcopy(FIRMWARE_STATUS)

    def firmware_update(self, *, key: str, commit: str) -> dict[str, object]:
        self.calls.append(("update", {"key": key, "commit": commit}))
        return firmware_task(task_id=key, action="update", target_commit=commit)

    def firmware_rollback(self, *, key: str, commit: str) -> dict[str, object]:
        self.calls.append(("rollback", {"key": key, "commit": commit}))
        return firmware_task(task_id=key, action="rollback", target_commit=commit)


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
        self.server = create_gateway_server("127.0.0.1", 0, _EmptyProvider(), security=policy)
        thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        thread.start()
        self.addCleanup(thread.join, 2)
        self.addCleanup(self.server.server_close)
        self.addCleanup(self.server.shutdown)

        def fake_control(operation: str, **fields: object) -> dict[str, object]:
            if operation in {"update", "rollback"}:
                return firmware_task(
                    task_id=str(fields["key"]),
                    action=operation,
                    target_commit=str(fields["commit"]),
                )
            return deepcopy(FIRMWARE_STATUS)

        p = patch("rp_ylx.api.gateway.request_firmware", side_effect=fake_control)
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
        status, payload = self.call("/check?force=true", headers=headers)
        self.assertEqual(status, 200)
        self.assertEqual(payload["schema"], "openaria.firmware-status.v1")
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
        status, payload = self.call("/update", body, headers)
        self.assertEqual(status, 202)
        self.assertEqual(payload["action"], "update")
        self.assertEqual(payload["target_commit"], "a" * 40)
        self.control.assert_called_once_with("update", key=key, commit="a" * 40)

    def test_rollback_endpoint_validates_and_dispatches(self):
        key = str(uuid.uuid4())
        headers = {
            "Authorization": "Bearer owner",
            "Content-Type": "application/json",
            "Origin": "http://device.test",
            "X-CSRF-Token": "csrf",
            "Idempotency-Key": key,
        }
        status, payload = self.call("/rollback", {"commit": "b" * 40}, headers)
        self.assertEqual(status, 202)
        self.assertEqual(payload["action"], "rollback")
        self.assertEqual(payload["target_commit"], "b" * 40)
        self.control.assert_called_once_with("rollback", key=key, commit="b" * 40)

    def test_supervisor_failure_is_explicit_not_latest_or_success(self):
        self.control.side_effect = FirmwareError(
            "update_service_unavailable", "更新服务不可用", 503
        )
        status, body = self.call("", headers={"Authorization": "Bearer owner"})
        self.assertEqual(status, 503)
        self.assertEqual(body["error"]["code"], "update_service_unavailable")
        self.assertTrue(body["error"]["retryable"])

    def test_4xx_firmware_error_marks_retryable_false(self):
        self.control.side_effect = FirmwareError("firmware_conflict", "任务冲突", 409)
        status, body = self.call("", headers={"Authorization": "Bearer owner"})
        self.assertEqual(status, 409)
        self.assertEqual(body["error"]["code"], "firmware_conflict")
        self.assertFalse(body["error"]["retryable"])

    def test_firmware_slot_saturation_returns_503_firmware_busy(self):
        acquired = 0
        while self.server.firmware_slots.acquire(blocking=False):
            acquired += 1
        self.assertGreater(acquired, 0)
        try:
            status, body = self.call("", headers={"Authorization": "Bearer owner"})
        finally:
            for _ in range(acquired):
                self.server.firmware_slots.release()
        self.assertEqual(status, 503)
        self.assertEqual(body["error"]["code"], "firmware_busy")
        self.assertTrue(body["error"]["retryable"])
        self.control.assert_not_called()

    def test_provider_firmware_methods_dispatch_without_control_socket(self):
        provider = _FirmwareProvider()
        self.server.provider = provider
        owner_headers = {
            "Authorization": "Bearer owner",
            "Content-Type": "application/json",
            "Origin": "http://device.test",
            "X-CSRF-Token": "csrf",
        }
        status, payload = self.call("", headers={"Authorization": "Bearer viewer"})
        self.assertEqual(status, 200)
        self.assertEqual(payload["schema"], "openaria.firmware-status.v1")

        status, _ = self.call("/check?force=true", headers={"Authorization": "Bearer viewer"})
        self.assertEqual(status, 200)

        key_update = str(uuid.uuid4())
        status, payload = self.call(
            "/update",
            {"commit": "a" * 40},
            {**owner_headers, "Idempotency-Key": key_update},
        )
        self.assertEqual(status, 202)
        self.assertEqual(payload["action"], "update")

        key_rollback = str(uuid.uuid4())
        status, payload = self.call(
            "/rollback",
            {"commit": "b" * 40},
            {**owner_headers, "Idempotency-Key": key_rollback},
        )
        self.assertEqual(status, 202)
        self.assertEqual(payload["action"], "rollback")
        self.control.assert_not_called()
        self.assertEqual(
            provider.calls,
            [
                ("status", {}),
                ("check", {"force": True}),
                ("update", {"key": key_update, "commit": "a" * 40}),
                ("rollback", {"key": key_rollback, "commit": "b" * 40}),
            ],
        )

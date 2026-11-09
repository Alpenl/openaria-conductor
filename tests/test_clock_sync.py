from __future__ import annotations

import fcntl
import json
import os
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import Mock, patch
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from rp_ylx.api import Principal, SecurityPolicy, create_gateway_server
from rp_ylx.api.gateway import ProviderError
from rp_ylx.clock_sync import ClockSyncError, ClockSynchronizer, valid_clock_response
from rp_ylx.network_control import NetworkController, handle_control_request
from rp_ylx.recording.coordinator import CaptureCoordinator

CLIENT_MS = 1789524000000


class ClockSyncTest(unittest.TestCase):
    def setUp(self) -> None:
        self.wall = 946684800_000000000  # RTC reset to 2000-01-01.
        self.mono = 10_000000000
        self.clock = Mock()
        self.clock.time_ns.side_effect = lambda: self.wall
        self.clock.monotonic_ns.side_effect = lambda: self.mono
        self.clock.clock_settime_ns.side_effect = self.set_wall
        time_patch = patch("rp_ylx.clock_sync.time", self.clock)
        time_patch.start()
        self.addCleanup(time_patch.stop)
        ntp_patch = patch("rp_ylx.clock_sync.ntp_synchronized", return_value=False)
        self.ntp = ntp_patch.start()
        self.addCleanup(ntp_patch.stop)
        self.syncer = ClockSynchronizer()

    def set_wall(self, clock: object, nanoseconds: int) -> None:
        self.wall = nanoseconds

    def request(self, principal: str = "phone") -> dict:
        status = self.syncer.status(principal)
        self.assertTrue(valid_clock_response(status))
        return {
            "schema": "ylx.clock-sync-request.v1",
            "challenge": status["challenge"],
            "unix_time_ms": CLIENT_MS,
        }

    def test_offline_boot_gets_date_and_another_client_cannot_overwrite_it(self) -> None:
        body = self.request()
        result = self.syncer.sync("phone", body)
        self.assertTrue(result["applied"])
        self.assertEqual(result["source"], "client")
        self.assertEqual(self.wall, CLIENT_MS * 1_000_000)
        self.assertTrue(valid_clock_response(result))
        self.assertIsNone(self.syncer.status("laptop")["challenge"])
        with self.assertRaises(ClockSyncError):
            self.syncer.sync("phone", body)
        self.clock.clock_settime_ns.assert_called_once()

    def test_new_controller_after_reboot_can_initialize_again(self) -> None:
        self.syncer.sync("phone", self.request())
        self.wall = 946684800_000000000
        self.syncer = ClockSynchronizer()
        self.assertTrue(self.syncer.sync("phone", self.request())["applied"])
        self.assertEqual(self.clock.clock_settime_ns.call_count, 2)

    def test_ntp_wins_even_if_it_synchronizes_between_challenge_and_post(self) -> None:
        body = self.request()
        self.ntp.return_value = True
        self.wall = CLIENT_MS * 1_000_000
        result = self.syncer.sync("phone", body)
        self.assertEqual(result["source"], "ntp")
        self.assertFalse(result["applied"])
        self.assertIsNone(self.syncer.status("phone")["challenge"])
        self.clock.clock_settime_ns.assert_not_called()

    def test_expired_and_wrong_principal_challenges_cannot_set_time(self) -> None:
        body = self.request()
        self.mono += 5_000000000
        with self.assertRaisesRegex(ClockSyncError, "过期"):
            self.syncer.sync("phone", body)
        with self.assertRaises(ClockSyncError):
            self.syncer.sync("laptop", self.request())
        self.clock.clock_settime_ns.assert_not_called()

    def test_external_clock_change_requires_a_fresh_challenge(self) -> None:
        body = self.request()
        self.wall += 60_000000000
        with self.assertRaisesRegex(ClockSyncError, "已改变"):
            self.syncer.sync("phone", body)
        self.clock.clock_settime_ns.assert_not_called()

    def test_bad_dates_unknown_fields_and_boolean_timestamps_are_rejected(self) -> None:
        body = self.request()
        for value in (True, 946684800000, 4102444800000, "2026-09-16", float("inf")):
            with self.subTest(value=value), self.assertRaises(ClockSyncError):
                self.syncer.sync("phone", {**body, "unix_time_ms": value})
        with self.assertRaises(ClockSyncError):
            self.syncer.sync("phone", {**body, "command": "date"})
        self.clock.clock_settime_ns.assert_not_called()

    def test_permission_failure_does_not_claim_success(self) -> None:
        self.clock.clock_settime_ns.side_effect = PermissionError("missing CAP_SYS_TIME")
        with self.assertRaisesRegex(ClockSyncError, "校准失败"):
            self.syncer.sync("phone", self.request())
        self.assertEqual(self.syncer.status("phone")["source"], "unsynchronized")

    def test_matching_clock_is_accepted_without_a_step(self) -> None:
        self.wall = CLIENT_MS * 1_000_000
        self.assertFalse(self.syncer.sync("phone", self.request())["applied"])
        self.assertEqual(self.syncer.status("phone")["source"], "client")
        self.clock.clock_settime_ns.assert_not_called()

    def test_ntp_query_time_is_compensated_and_rechecked_against_expiry(self) -> None:
        body = self.request()

        def slow_query() -> bool:
            self.mono += 500_000000
            self.wall += 500_000000
            return False

        self.ntp.side_effect = slow_query
        self.syncer.sync("phone", body)
        self.assertEqual(self.wall, CLIENT_MS * 1_000_000 + 500_000000)

    def test_privileged_controller_obeys_real_capture_flock(self) -> None:
        controller = NetworkController.__new__(NetworkController)
        controller._clock = self.syncer
        controller._log_control_failure = Mock()
        body = self.request()
        request = {
            "schema": "ylx.network-control-request.v1",
            "operation": "clock_sync",
            "principal_id": "phone",
            "body": body,
        }
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "capture.lock"
            with patch.dict(os.environ, {"RP_YLX_NETWORK_OPERATION_LOCK_PATH": str(path)}):
                with path.open("w") as capture:
                    fcntl.flock(capture, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    result = handle_control_request(request, controller=controller)
                    self.assertFalse(result["ok"])
                    self.assertEqual(result["error"]["code"], "capture_active")
                    self.clock.clock_settime_ns.assert_not_called()
                result = handle_control_request(request, controller=controller)
                self.assertTrue(result["ok"])
                self.assertEqual(result["body"]["source"], "client")

    def test_coordinator_refuses_active_capture_before_calling_controller(self) -> None:
        coordinator = CaptureCoordinator.__new__(CaptureCoordinator)
        coordinator._lock = threading.RLock()
        coordinator._active = object()
        with (
            patch("rp_ylx.recording.coordinator.request_network_control") as request,
            self.assertRaises(ProviderError) as raised,
        ):
            coordinator.sync_clock("phone", self.request())
        self.assertEqual(raised.exception.status, 409)
        request.assert_not_called()

    def extended_request(self, *, replace=False):
        status = self.syncer.status("phone", extended=True)
        self.assertTrue(valid_clock_response(status))
        return {
            "schema": "ylx.clock-sync-request.v2",
            "challenge": status["challenge"],
            "unix_time_ms": CLIENT_MS,
            "replace": replace,
        }

    def test_service_restart_preserves_reference_but_new_boot_never_does(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "clock.json"
            self.syncer = ClockSynchronizer(path, boot_id="boot-one")
            self.syncer.sync("phone", self.extended_request())
            self.mono += 60_000000000
            self.wall += 60_000000000
            restarted = ClockSynchronizer(path, boot_id="boot-one")
            status = restarted.status("phone", extended=True)
            self.assertEqual(status["source"], "client")
            self.assertEqual(status["age_seconds"], 60)
            self.assertEqual(status["synchronized_at_ms"], CLIENT_MS)
            rebooted = ClockSynchronizer(path, boot_id="boot-two")
            self.assertEqual(rebooted.status("phone")["source"], "unsynchronized")
            self.wall += 10_000000000
            self.assertEqual(restarted.status("phone")["source"], "unsynchronized")

    def test_explicit_resync_and_stale_second_phone(self):
        self.syncer.sync("phone", self.extended_request())
        a = self.extended_request(replace=True)
        b = self.extended_request(replace=True)
        self.syncer.sync("phone", {**a, "unix_time_ms": CLIENT_MS + 120000})
        with self.assertRaises(ClockSyncError):
            self.syncer.sync("phone", b)
        self.assertEqual(self.wall, (CLIENT_MS + 120000) * 1_000_000)
        auto = self.extended_request()
        result = self.syncer.sync("phone", auto)
        self.assertFalse(result["applied"])
        self.assertEqual(self.wall, (CLIENT_MS + 120000) * 1_000_000)

    def test_silent_clock_set_failure_fails_readback(self):
        self.clock.clock_settime_ns.side_effect = lambda *_: None
        with self.assertRaisesRegex(ClockSyncError, "回读"):
            self.syncer.sync("phone", self.extended_request())
        self.assertEqual(self.syncer.status("phone")["source"], "unsynchronized")

    def test_persistence_failure_and_corrupt_state_do_not_authorize_capture(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "clock.json"
            path.write_text("not json")
            self.syncer = ClockSynchronizer(path)
            body = self.extended_request()
            with (
                patch("rp_ylx.clock_sync.os.replace", side_effect=OSError("disk full")),
                self.assertRaisesRegex(ClockSyncError, "保存失败"),
            ):
                self.syncer.sync("phone", body)
            self.assertEqual(self.syncer.status("phone")["source"], "unsynchronized")

    def test_explicit_resync_cannot_override_active_ntp(self):
        self.wall = CLIENT_MS * 1_000_000
        self.ntp.return_value = True
        body = self.extended_request(replace=True)
        with self.assertRaisesRegex(ClockSyncError, "网络校时"):
            self.syncer.sync("phone", body)
        self.clock.clock_settime_ns.assert_not_called()


class ClockGatewayTest(unittest.TestCase):
    def setUp(self) -> None:
        self.provider = Mock()
        self.status = {
            "schema": "ylx.clock-status.v1",
            "source": "unsynchronized",
            "unix_time_ms": 946684800000,
            "challenge": "a" * 32,
            "expires_in_ms": 5000,
            "applied": False,
        }
        self.provider.clock_status.return_value = self.status
        self.provider.sync_clock.return_value = {
            **self.status,
            "source": "client",
            "challenge": None,
            "expires_in_ms": 0,
            "unix_time_ms": CLIENT_MS,
            "applied": True,
        }
        policy = SecurityPolicy.customer(
            tokens={
                "owner": Principal(
                    "phone", permissions={"getDeviceClock": None, "syncDeviceClock": None}
                ),
                "viewer": Principal("viewer", permissions={"getDevice": None}),
            },
            allowed_origins={"http://phone.test"},
            csrf_token="csrf",
        )
        self.server = create_gateway_server("127.0.0.1", 0, self.provider, security=policy)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    def tearDown(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=2)

    def call(self, path: str, *, data: dict | None = None, headers: dict | None = None) -> tuple:
        request = Request(
            f"http://127.0.0.1:{self.server.server_port}{path}",
            data=None if data is None else json.dumps(data).encode(),
            headers=headers or {},
        )
        try:
            response = urlopen(request, timeout=3)
        except HTTPError as error:
            response = error
        with response:
            return response.status, json.loads(response.read())

    def test_read_and_write_require_permissions_origin_csrf_and_valid_time(self) -> None:
        self.assertEqual(self.call("/api/v4/clock")[0], 401)
        self.assertEqual(
            self.call("/api/v4/clock", headers={"Authorization": "Bearer viewer"})[0], 403
        )
        status, body = self.call("/api/v4/clock", headers={"Authorization": "Bearer owner"})
        self.assertEqual(status, 200)
        self.assertEqual(body, self.status)
        self.provider.clock_status.assert_called_once_with("phone")
        data = {
            "schema": "ylx.clock-sync-request.v1",
            "challenge": "a" * 32,
            "unix_time_ms": CLIENT_MS,
        }
        headers = {"Authorization": "Bearer owner", "Content-Type": "application/json"}
        self.assertEqual(self.call("/api/v4/clock/sync", data=data, headers=headers)[0], 403)
        headers["Origin"] = "http://phone.test"
        self.assertEqual(self.call("/api/v4/clock/sync", data=data, headers=headers)[0], 403)
        headers["X-CSRF-Token"] = "csrf"
        self.assertEqual(
            self.call("/api/v4/clock/sync", data={**data, "unix_time_ms": True}, headers=headers)[
                0
            ],
            400,
        )
        self.provider.sync_clock.assert_not_called()
        self.assertEqual(self.call("/api/v4/clock/sync", data=data, headers=headers)[0], 200)
        self.provider.sync_clock.assert_called_once_with("phone", data)

    def test_extended_clock_uses_same_permissions_and_explicit_replace(self):
        self.provider.clock_inspect.return_value = {
            **self.status,
            "schema": "ylx.clock-status.v2",
            "boot_id": "a4578322-241e-4b9c-bb54-a860d5a2aa37",
            "synchronized_at_ms": None,
            "age_seconds": None,
        }
        self.assertEqual(self.call("/api/v4/clock/check")[0], 401)
        self.assertEqual(
            self.call("/api/v4/clock/check", headers={"Authorization": "Bearer viewer"})[0], 403
        )
        headers = {
            "Authorization": "Bearer owner",
            "Content-Type": "application/json",
            "Origin": "http://phone.test",
            "X-CSRF-Token": "csrf",
        }
        status, body = self.call("/api/v4/clock/check", headers=headers)
        self.assertEqual(status, 200)
        self.assertEqual(body["schema"], "ylx.clock-status.v2")
        self.provider.clock_inspect.assert_called_once_with("phone")
        data = {
            "schema": "ylx.clock-sync-request.v2",
            "replace": True,
            "challenge": body["challenge"],
            "unix_time_ms": CLIENT_MS,
        }
        self.provider.sync_clock.return_value = {
            **body,
            "source": "client",
            "unix_time_ms": CLIENT_MS,
            "synchronized_at_ms": CLIENT_MS,
            "age_seconds": 0,
        }
        self.assertEqual(self.call("/api/v4/clock/sync", data=data, headers=headers)[0], 200)
        self.provider.sync_clock.assert_called_once_with("phone", data)
        bad_headers = {key: value for key, value in headers.items() if key != "X-CSRF-Token"}
        self.assertEqual(self.call("/api/v4/clock/sync", data=data, headers=bad_headers)[0], 403)

    def test_clock_errors_propagate_and_legacy_routes_are_not_changed(self) -> None:
        self.provider.clock_status.side_effect = ProviderError(
            "clock_sync_unavailable",
            "设备校时服务暂时不可用",
            status=503,
            retryable=True,
        )
        headers = {"Authorization": "Bearer owner"}
        status, body = self.call("/api/v4/clock", headers=headers)
        self.assertEqual(status, 503)
        self.assertTrue(body["error"]["retryable"])
        self.assertEqual(self.call("/api/v3/clock", headers=headers)[0], 404)


if __name__ == "__main__":
    unittest.main()

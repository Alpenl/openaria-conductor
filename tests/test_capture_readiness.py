import sqlite3
import threading
import time
import unittest
from unittest.mock import patch

from rp_ylx.api import ProviderError
from rp_ylx.capture_readiness import SYSTEM_RESERVE_BYTES, valid_readiness
from rp_ylx.daemon import CaptureRecoveryMonitor
from tests import test_capture_coordinator as fixtures


class CaptureReadinessTest(unittest.TestCase):
    def setUp(self):
        self.fixture = fixtures.CaptureCoordinatorTest()
        self.fixture.setUp()
        self.addCleanup(self.fixture.tearDown)
        self.c = self.fixture.coordinator()
        self.addCleanup(self.c.close)

    def fail(self):
        self.c._source_failure(self.c.generation_id, "encoder_failed", "injected slow disk")

    def test_physical_volume_still_checks_system_database_space(self):
        with patch("rp_ylx.recording.coordinator.system_capacity", return_value=(1, 10000)):
            status = self.c.capture_readiness()
            self.assertTrue(valid_readiness(status))
            self.assertFalse(status["allowed"])
            self.assertIn("系统", status["message"])
            self.assertIsNone(status["storage"]["backing"])
            self.assertFalse(self.c.device_descriptor("v4", "lab")["capabilities"]["capture"])
            with self.assertRaises(ProviderError):
                self.c.start_capture(fixtures.start_command("no-state-space"))
            self.assertIsNone(self.c._active)
        self.assertTrue(self.c.capture_readiness()["allowed"])
        self.c.start_capture(fixtures.start_command("state-space-restored"))

    def test_system_low_space_stops_active_recording(self):
        self.c.start_capture(fixtures.start_command("system-low"))
        self.c.submit_frame(fixtures.frame())
        with patch("rp_ylx.recording.coordinator.system_capacity", return_value=(1, 10000)):
            self.c.enforce_storage_reserve()
        self.assertIsNone(self.c._active)

    def test_failed_commit_is_retried_without_browser_or_duplicate_recovery(self):
        self.c.start_capture(fixtures.start_command("failed-commit"))
        recorder = self.c._active
        with patch.object(recorder, "fail", wraps=recorder.fail) as fail:
            with (
                patch.object(
                    self.c._state_db,
                    "put_capture_state",
                    side_effect=sqlite3.OperationalError("database full"),
                ),
                self.assertRaises(sqlite3.OperationalError),
            ):
                self.fail()
            self.assertEqual(self.c.capture_readiness()["code"], "recovery_pending")
            monitor = CaptureRecoveryMonitor(self.c)
            try:
                deadline = time.monotonic() + 6
                while self.c._active is not None and time.monotonic() < deadline:
                    time.sleep(0.02)
                self.assertIsNone(self.c._active)
                self.assertEqual(fail.call_count, 1)
            finally:
                monitor.close()
        self.assertIsNone(self.c._failure_projection)
        self.c.start_capture(fixtures.start_command("next-after-failure"))

    def test_unexpected_cleanup_exception_does_not_strand_failed_owner(self):
        self.c.start_capture(fixtures.start_command("cleanup-retry"))
        with (
            patch.object(self.c._active, "fail", side_effect=OSError("temporary IO")),
            self.assertRaises(OSError),
        ):
            self.fail()
        self.assertEqual(self.c.capture_readiness()["code"], "recovery_pending")
        self.c.reconcile_capture_recovery()
        self.assertIsNone(self.c._active)
        self.c.start_capture(fixtures.start_command("next-after-cleanup"))

    def test_live_failure_worker_is_never_bypassed(self):
        self.c.start_capture(fixtures.start_command("live-worker"))
        entered, release = threading.Event(), threading.Event()
        recorder = self.c._active
        original = recorder.fail

        def slow(*args, **kwargs):
            entered.set()
            if not release.wait(5):
                raise TimeoutError("test gate")
            return original(*args, **kwargs)

        with patch.object(recorder, "fail", side_effect=slow) as fail:
            worker = threading.Thread(target=self.fail)
            worker.start()
            try:
                self.assertTrue(entered.wait(2))
                self.c.reconcile_capture_recovery()
                self.assertEqual(fail.call_count, 1)
                with self.assertRaises(ProviderError):
                    self.c.start_capture(fixtures.start_command("must-wait"))
            finally:
                release.set()
                worker.join(5)
            self.assertFalse(worker.is_alive())
        self.c.start_capture(fixtures.start_command("after-drain"))

    def test_completed_stop_commit_retry_does_not_repeat_media_stop(self):
        self.c.start_capture(fixtures.start_command("normal-stop"))
        self.c.submit_frame(fixtures.frame())
        original = self.c._state_db.put_capture_state

        def fail_final(*args, **kwargs):
            if self.c._pending_stop_state_commit is not None:
                raise OSError("temporary state write failure")
            return original(*args, **kwargs)

        with (
            patch.object(self.c._state_db, "put_capture_state", side_effect=fail_final),
            self.assertRaises(OSError),
        ):
            self.c.stop_capture(fixtures.stop_command("stop"))
        self.assertIsNone(self.c._active)
        self.assertIsNotNone(self.c._pending_stop_state_commit)
        self.c.reconcile_capture_recovery()
        self.assertIsNone(self.c._pending_stop_state_commit)
        self.c.start_capture(fixtures.start_command("next-after-stop"))

    def test_readiness_validator_rejects_invalid_budgets(self):
        value = self.c.capture_readiness()
        self.assertTrue(valid_readiness(value))
        value["storage"]["system"] = {
            "available_bytes": True,
            "required_bytes": SYSTEM_RESERVE_BYTES,
        }
        self.assertFalse(valid_readiness(value))

"""A transient maintenance lease must remain distinguishable from bad lock input."""

import errno
import os
import unittest
from unittest.mock import patch

from rp_ylx import update
from rp_ylx.api import ProviderError
from tests import test_capture_coordinator as fixtures


class MaintenanceRetryTests(unittest.TestCase):
    def setUp(self):
        self.fixture = fixtures.CaptureCoordinatorTest()
        self.fixture.setUp()
        self.addCleanup(self.fixture.tearDown)
        self.path = self.fixture.root / "maintenance.lock"
        env = patch.dict(os.environ, {"OPENARIA_MAINTENANCE_LOCK": str(self.path)})
        env.start()
        self.addCleanup(env.stop)
        self.coordinator = self.fixture.coordinator()
        self.addCleanup(self.coordinator.close)

    def test_same_start_request_succeeds_after_maintenance_releases(self):
        command = fixtures.start_command("retry-after-maintenance")
        with update.maintenance_lock():
            with self.assertRaises(ProviderError) as caught:
                self.coordinator.start_capture(command)
            self.assertEqual(caught.exception.code, "maintenance_active")
            self.assertEqual(caught.exception.status, 409)
            self.assertTrue(caught.exception.retryable)
            self.assertIsInstance(caught.exception.__cause__, update.MaintenanceBusyError)
            self.assertIsNone(self.coordinator._active)
            self.assertIsNone(self.coordinator._network_operation_lease)
            self.assertNotIn(
                (command.principal_id, "start", command.idempotency_key),
                self.coordinator._commands,
            )
        self.coordinator.start_capture(command)
        self.assertEqual(self.coordinator.capture_status()["snapshot"]["device_state"], "recording")
        self.assertTrue(self.coordinator.submit_frame(fixtures.frame()))
        self.coordinator.stop_capture(fixtures.stop_command("completed-after-maintenance"))
        self.assertEqual(self.coordinator.capture_status()["snapshot"]["device_state"], "idle")
        with update.maintenance_lock():
            pass

    def test_busy_error_remains_compatible_with_standalone_value_error_handlers(self):
        with update.maintenance_lock():
            with self.assertRaises(ValueError) as caught, update.maintenance_lock(shared=True):
                self.fail("capture acquired maintenance owner's lease")
            self.assertIsInstance(caught.exception, update.MaintenanceBusyError)
            self.assertIsInstance(caught.exception.__cause__, BlockingIOError)

    def test_invalid_directory_is_not_advertised_as_retryable_contention(self):
        self.path.mkdir()
        with self.assertRaises(ProviderError) as caught:
            self.coordinator.start_capture(fixtures.start_command("invalid-maintenance"))
        self.assertFalse(caught.exception.retryable)
        self.assertIsNone(self.coordinator._active)
        self.assertTrue(self.path.is_dir())

    def test_symlink_is_not_advertised_as_retryable_contention(self):
        target = self.fixture.root / "unrelated"
        target.write_bytes(b"preserve")
        self.path.symlink_to(target)
        with self.assertRaises(ProviderError) as caught:
            self.coordinator.start_capture(fixtures.start_command("unsafe-maintenance"))
        self.assertFalse(caught.exception.retryable)
        self.assertIsNone(self.coordinator._active)
        self.assertEqual(target.read_bytes(), b"preserve")

    def test_io_error_is_not_advertised_as_retryable_contention(self):
        original = os.open

        def open_file(path, *args, **kwargs):
            if path == self.path:
                raise OSError(errno.EIO, "injected lock read failure")
            return original(path, *args, **kwargs)

        with (
            patch.object(update.os, "open", side_effect=open_file),
            self.assertRaises(ProviderError) as caught,
        ):
            self.coordinator.start_capture(fixtures.start_command("unreadable-maintenance"))
        self.assertFalse(caught.exception.retryable)
        self.assertIsNone(self.coordinator._active)

"""Capture's maintenance and network locks have independent cleanup ownership."""

import errno
import fcntl
import os
import unittest
from unittest.mock import patch

from rp_ylx import update
from rp_ylx.api import ProviderError
from tests import test_capture_coordinator as fixtures


class ExitFailure:
    def __init__(self, lease, error):
        self.lease = lease
        self.error = error

    def __enter__(self):
        return self.lease.__enter__()

    def __exit__(self, *args):
        self.lease.__exit__(*args)
        raise self.error


class CaptureLeaseCleanupTest(unittest.TestCase):
    def setUp(self):
        self.fixture = fixtures.CaptureCoordinatorTest()
        self.fixture.setUp()
        self.addCleanup(self.fixture.tearDown)
        self.maintenance = self.fixture.root / "maintenance.lock"
        env = patch.dict(os.environ, {"OPENARIA_MAINTENANCE_LOCK": str(self.maintenance)})
        env.start()
        self.addCleanup(env.stop)
        self.coordinator = self.fixture.coordinator()
        self.addCleanup(self.coordinator.close)
        self.network = self.coordinator._network_operation_lock_path()

    def assert_released(self, descriptor):
        self.assertIsNone(self.coordinator._network_operation_lease)
        self.assertIsNone(getattr(self.coordinator, "_maintenance_lease", None))
        with self.assertRaises(OSError) as caught:
            os.fstat(descriptor)
        self.assertEqual(caught.exception.errno, errno.EBADF)
        peer = os.open(self.network, os.O_RDONLY)
        try:
            fcntl.flock(peer, fcntl.LOCK_EX | fcntl.LOCK_NB)
        finally:
            os.close(peer)
        with update.maintenance_lock():
            pass

    def test_maintenance_exit_errors_release_network_and_allow_reacquisition(self):
        for error in (
            OSError(errno.EIO, "close report"),
            RuntimeError("cleanup"),
            KeyboardInterrupt(),
        ):
            with self.subTest(error=type(error).__name__):
                self.coordinator._acquire_network_operation_lease()
                descriptor = self.coordinator._network_operation_lease
                self.coordinator._maintenance_lease = ExitFailure(
                    self.coordinator._maintenance_lease, error
                )
                with self.assertRaises(type(error)) as caught:
                    self.coordinator._release_network_operation_lease()
                self.assertIs(caught.exception, error)
                self.assert_released(descriptor)
                self.coordinator._acquire_network_operation_lease()
                self.coordinator._release_network_operation_lease()

    def test_cancelled_or_failed_network_stat_releases_both_open_descriptors(self):
        original = os.fstat
        for error in (KeyboardInterrupt(), MemoryError("metadata"), RuntimeError("stat")):
            opened = []

            def stat(fd, *, error=error, opened=opened):
                if os.readlink(f"/proc/self/fd/{fd}") == str(self.network):
                    opened.append(fd)
                    raise error
                return original(fd)

            with (
                self.subTest(error=type(error).__name__),
                patch.object(os, "fstat", side_effect=stat),
                self.assertRaises(type(error)) as caught,
            ):
                self.coordinator._acquire_network_operation_lease()
            self.assertIs(caught.exception, error)
            self.assertEqual(len(opened), 1)
            self.assert_released(opened[0])

    def test_cancellation_after_real_network_lock_acquisition_releases_it(self):
        original = fcntl.flock
        opened = []
        error = KeyboardInterrupt()

        def lock(fd, operation):
            result = original(fd, operation)
            if operation == fcntl.LOCK_EX | fcntl.LOCK_NB:
                opened.append(fd)
                raise error
            return result

        with patch.object(fcntl, "flock", side_effect=lock), self.assertRaises(KeyboardInterrupt):
            self.coordinator._acquire_network_operation_lease()
        self.assertEqual(len(opened), 1)
        self.assert_released(opened[0])

    def test_unlock_cancellation_still_closes_network_descriptor(self):
        self.coordinator._acquire_network_operation_lease()
        descriptor = self.coordinator._network_operation_lease
        original = fcntl.flock

        def unlock(fd, operation):
            if fd == descriptor and operation == fcntl.LOCK_UN:
                raise KeyboardInterrupt()
            return original(fd, operation)

        with patch.object(fcntl, "flock", side_effect=unlock), self.assertRaises(KeyboardInterrupt):
            self.coordinator._release_network_operation_lease()
        self.assert_released(descriptor)

    def test_busy_error_is_preserved_if_cleanup_also_reports_io_error(self):
        peer = os.open(self.network, os.O_CREAT | os.O_RDONLY, 0o600)
        self.addCleanup(os.close, peer)
        fcntl.flock(peer, fcntl.LOCK_EX | fcntl.LOCK_NB)
        original = update.maintenance_lock

        def lease(**kwargs):
            return ExitFailure(original(**kwargs), OSError(errno.EIO, "private cleanup detail"))

        before = len(os.listdir("/proc/self/fd"))
        with (
            patch.object(update, "maintenance_lock", side_effect=lease),
            self.assertRaises(ProviderError) as caught,
        ):
            self.coordinator._acquire_network_operation_lease()
        self.assertEqual(caught.exception.code, "network_mutation_active")
        self.assertTrue(caught.exception.retryable)
        self.assertIsInstance(caught.exception.__cause__, BlockingIOError)
        self.assertIn("OSError", caught.exception.__cause__.__notes__[0])
        self.assertNotIn("private cleanup detail", caught.exception.__cause__.__notes__[0])
        self.assertEqual(len(os.listdir("/proc/self/fd")), before)
        with original():
            pass

    def test_repeated_cleanup_cannot_close_reused_unrelated_descriptor(self):
        self.coordinator._acquire_network_operation_lease()
        self.coordinator._release_network_operation_lease()
        unrelated = os.open(self.fixture.root / "unrelated", os.O_CREAT | os.O_RDWR, 0o600)
        try:
            self.coordinator._release_network_operation_lease()
            self.coordinator._release_network_operation_lease()
            os.write(unrelated, b"preserved")
        finally:
            os.close(unrelated)
        self.assertEqual((self.fixture.root / "unrelated").read_bytes(), b"preserved")

    def test_primary_cancellation_survives_secondary_cleanup_error(self):
        cancelled = KeyboardInterrupt()
        original_stat, original_lease = os.fstat, update.maintenance_lock
        opened = []

        def stat(fd):
            if os.readlink(f"/proc/self/fd/{fd}") == str(self.network):
                opened.append(fd)
                raise cancelled
            return original_stat(fd)

        def lease(**kwargs):
            return ExitFailure(original_lease(**kwargs), OSError(errno.EIO, "cleanup"))

        with (
            patch.object(os, "fstat", side_effect=stat),
            patch.object(update, "maintenance_lock", side_effect=lease),
            self.assertRaises(KeyboardInterrupt) as caught,
        ):
            self.coordinator._acquire_network_operation_lease()
        self.assertIs(caught.exception, cancelled)
        self.assertIn("OSError", cancelled.__notes__[0])
        self.assertEqual(len(opened), 1)
        self.assert_released(opened[0])

    def test_cleanup_cancellation_is_not_converted_to_retryable_busy_error(self):
        peer = os.open(self.network, os.O_CREAT | os.O_RDONLY, 0o600)
        self.addCleanup(os.close, peer)
        fcntl.flock(peer, fcntl.LOCK_EX | fcntl.LOCK_NB)
        original = update.maintenance_lock
        cancelled = KeyboardInterrupt()

        def lease(**kwargs):
            return ExitFailure(original(**kwargs), cancelled)

        before = len(os.listdir("/proc/self/fd"))
        with (
            patch.object(update, "maintenance_lock", side_effect=lease),
            self.assertRaises(KeyboardInterrupt) as caught,
        ):
            self.coordinator._acquire_network_operation_lease()
        self.assertIs(caught.exception, cancelled)
        self.assertIsInstance(cancelled.__cause__, BlockingIOError)
        self.assertEqual(len(os.listdir("/proc/self/fd")), before)
        with original():
            pass

    def test_public_stop_cleanup_error_does_not_block_the_next_recording(self):
        self.coordinator.start_capture(fixtures.start_command("cleanup-error-first"))
        self.assertTrue(self.coordinator.submit_frame(fixtures.frame()))
        descriptor = self.coordinator._network_operation_lease
        self.coordinator._maintenance_lease = ExitFailure(
            self.coordinator._maintenance_lease, OSError(errno.EIO, "close report")
        )
        with self.assertRaises(OSError):
            self.coordinator.stop_capture(fixtures.stop_command("cleanup-error-stop"))
        self.assert_released(descriptor)
        self.coordinator.start_capture(fixtures.start_command("cleanup-error-second"))
        self.assertIsNotNone(self.coordinator._active)

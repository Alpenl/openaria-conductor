from __future__ import annotations

import errno
import io
import json
import os
import unittest
from contextlib import contextmanager, redirect_stdout
from pathlib import Path
from unittest.mock import patch

from rp_ylx import deployment as deploy
from tests import test_deployment as fixtures


class ActivationSuccessCleanupTest(unittest.TestCase):
    def setUp(self):
        self.fixture = fixtures.ReleaseManagerTest()
        self.fixture.setUp()
        self.addCleanup(self.fixture.tearDown)
        self.manager = self.fixture.manager()
        self.manager._ensure_layout()
        self.path = self.manager.state_root / "activation-failure.json"

    def old_diagnostic(self):
        self.manager._record_activation_failure(
            deploy.DeploymentError("service_unhealthy", "old health failure"), "c" * 40
        )
        return self.path.read_bytes()

    @contextmanager
    def unlink_failure(self, error):
        original = Path.unlink

        def unlink(path, *args, **kwargs):
            if path == self.path:
                raise error
            return original(path, *args, **kwargs)

        with patch.object(Path, "unlink", unlink):
            yield

    def assert_healthy_warning(self, result, current, previous=None):
        self.assertTrue(result["installed"])
        self.assertEqual(result["current"], current * 40)
        self.assertEqual(result["previous"], previous * 40 if previous else None)
        self.assertFalse(result["transaction_pending"])
        self.assertEqual(len(result["warnings"]), 1)
        warning = result["warnings"][0]
        self.assertEqual(warning["code"], "activation_diagnostic_cleanup_failed")
        self.assertIn("固件已激活", warning["message"])
        self.assertNotIn("private failure detail", warning["message"])
        self.assertNotIn(("systemctl", "disable", "--now", "rp-ylx.service"), self.fixture.commands)

    def test_first_install_with_real_diagnostic_directory_succeeds(self):
        self.path.mkdir()
        (self.path / "evidence").write_bytes(b"preserve")
        with patch.object(
            self.manager, "health_checker", wraps=self.manager.health_checker
        ) as check:
            result = self.manager.install(self.fixture.bundle("a"))
        check.assert_called_once()
        self.assert_healthy_warning(result, "a")
        self.assertEqual(result["activation_failure"]["code"], "diagnostic_unreadable")
        self.assertEqual((self.path / "evidence").read_bytes(), b"preserve")
        self.assertEqual(self.fixture.manager().recover()["current"], "a" * 40)

    def test_upgrade_does_not_restore_previous_for_cleanup_failure(self):
        self.manager.install(self.fixture.bundle("a"))
        self.fixture.commands.clear()
        self.path.mkdir()
        result = self.manager.install(self.fixture.bundle("b"))
        self.assert_healthy_warning(result, "b", "a")
        self.assertTrue(self.path.is_dir())

    def test_healthy_rollback_keeps_requested_release_despite_cleanup_failure(self):
        self.manager.install(self.fixture.bundle("a"))
        self.manager.install(self.fixture.bundle("b"))
        self.fixture.commands.clear()
        original = self.old_diagnostic()
        with self.unlink_failure(PermissionError("private failure detail")):
            result = self.manager.rollback()
        self.assert_healthy_warning(result, "a", "b")
        self.assertEqual(self.path.read_bytes(), original)
        self.assertEqual(result["activation_failure"]["code"], "service_unhealthy")

    def test_reinstall_cleanup_warning_does_not_stop_healthy_services(self):
        bundle = self.fixture.bundle("a")
        self.manager.install(bundle)
        self.fixture.commands.clear()
        self.path.mkdir()
        self.assert_healthy_warning(self.manager.install(bundle), "a")

    def test_io_permission_and_readonly_failures_preserve_diagnostic(self):
        bundle = self.fixture.bundle("a")
        original = self.old_diagnostic()
        for code in (errno.EIO, errno.EACCES, errno.EROFS):
            with self.subTest(errno=code):
                with self.unlink_failure(OSError(code, "private failure detail")):
                    result = self.manager.install(bundle)
                self.assert_healthy_warning(result, "a")
                self.assertEqual(self.path.read_bytes(), original)

    def test_retry_cleans_diagnostic_and_does_not_reuse_warning(self):
        bundle = self.fixture.bundle("a")
        self.old_diagnostic()
        with self.unlink_failure(OSError(errno.EIO, "temporary")):
            self.assert_healthy_warning(self.manager.install(bundle), "a")
        result = self.manager.install(bundle)
        self.assertNotIn("warnings", result)
        self.assertIsNone(result["activation_failure"])
        self.assertFalse(self.path.exists())

    def test_no_activate_preserves_old_diagnostic_without_cleanup_warning(self):
        original = self.old_diagnostic()
        with self.unlink_failure(AssertionError("must not clean without health check")):
            result = self.manager.install(self.fixture.bundle("a"), activate=False)
            self.manager.install(self.fixture.bundle("b"), activate=False)
            rolled_back = self.manager.rollback(activate=False)
        self.assertNotIn("warnings", result)
        self.assertNotIn("warnings", rolled_back)
        self.assertEqual(self.path.read_bytes(), original)

    def test_real_fifo_is_unlinked_without_reading_or_waiting_for_writer(self):
        os.mkfifo(self.path)
        result = self.manager.install(self.fixture.bundle("a"))
        self.assertNotIn("warnings", result)
        self.assertFalse(os.path.lexists(self.path))

    def test_symlink_cleanup_preserves_target(self):
        target = self.fixture.root / "evidence"
        target.mkdir()
        (target / "keep").write_bytes(b"original")
        self.path.symlink_to(target, target_is_directory=True)
        result = self.manager.install(self.fixture.bundle("a"))
        self.assertNotIn("warnings", result)
        self.assertFalse(os.path.lexists(self.path))
        self.assertEqual((target / "keep").read_bytes(), b"original")

    def test_cli_reports_success_and_warning_in_structured_output(self):
        self.path.mkdir()
        output = io.StringIO()
        with (
            patch.object(deploy, "ReleaseManager", return_value=self.manager),
            redirect_stdout(output),
        ):
            code = deploy.main(["install", str(self.fixture.bundle("a"))])
        self.assertEqual(code, 0)
        result = json.loads(output.getvalue())
        self.assertTrue(result["ok"])
        self.assert_healthy_warning(result, "a")

    def test_interrupt_during_optional_cleanup_propagates_without_rollback(self):
        self.old_diagnostic()
        cancelled = KeyboardInterrupt()
        with self.unlink_failure(cancelled), self.assertRaises(KeyboardInterrupt) as caught:
            self.manager.install(self.fixture.bundle("a"))
        self.assertIs(caught.exception, cancelled)
        self.assertEqual(self.manager.status()["current"], "a" * 40)
        self.assertIsNone(self.manager._recovery_descriptor)
        self.assertNotIn(("systemctl", "disable", "--now", "rp-ylx.service"), self.fixture.commands)

    def test_cleanup_still_holds_deployment_write_lock(self):
        self.old_diagnostic()
        original = Path.unlink
        peer = self.fixture.manager()
        checked = False

        def unlink(path, *args, **kwargs):
            nonlocal checked
            if path == self.path:
                self.assertTrue(self.manager._recovery_exclusive)
                with self.assertRaises(deploy.DeploymentError) as caught:
                    peer.uninstall()
                self.assertEqual(caught.exception.code, "deployment_busy")
                checked = True
                raise OSError(errno.EIO, "temporary")
            return original(path, *args, **kwargs)

        with patch.object(Path, "unlink", unlink):
            self.assert_healthy_warning(self.manager.install(self.fixture.bundle("a")), "a")
        self.assertTrue(checked)

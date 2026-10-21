from __future__ import annotations

import errno
import io
import json
import unittest
from contextlib import contextmanager, redirect_stderr, redirect_stdout
from unittest.mock import patch

from rp_ylx import deployment as deploy
from tests import test_deployment as fixtures


class ActivationFailureCleanupTest(unittest.TestCase):
    def setUp(self):
        self.fixture = fixtures.ReleaseManagerTest()
        self.fixture.setUp()
        self.addCleanup(self.fixture.tearDown)
        self.manager = self.fixture.manager()
        self.good_health = self.manager.health_checker
        self.health_error = deploy.DeploymentError("service_unhealthy", "health probe failed")
        self.path = self.manager.state_root / "activation-failure.json"

    def fail_health(self):
        raise self.health_error

    @contextmanager
    def diagnostic_failure(self, error=None):
        if error is None:
            error = OSError(errno.ENOSPC, "diagnostic storage unavailable")
        original = deploy._write_atomic

        def write(path, value):
            if path == self.path:
                raise error
            return original(path, value)

        with patch.object(deploy, "_write_atomic", side_effect=write):
            yield

    def assert_health_failure(self, callback):
        with self.assertRaises(deploy.DeploymentError) as caught:
            callback()
        error = caught.exception
        self.assertEqual(error.code, "service_unhealthy")
        self.assertIn(self.health_error.message, error.message)
        self.assertIn("激活失败诊断未能保存", error.message)
        self.assertIs(error.__cause__, self.health_error)
        return error

    def test_first_install_stops_failed_release_despite_enospc_diagnostic(self):
        bundle = self.fixture.bundle("a")
        self.manager.health_checker = self.fail_health
        with self.diagnostic_failure():
            self.assert_health_failure(lambda: self.manager.install(bundle))
        status = self.manager.status()
        self.assertFalse(status["installed"])
        self.assertIsNone(status["current"])
        self.assertEqual(status["previous"], "a" * 40)
        self.assertFalse(status["transaction_pending"])
        self.assertIn(("systemctl", "disable", "--now", "rp-ylx.service"), self.fixture.commands)
        self.assertFalse(self.fixture.manager().recover()["installed"])

    def test_upgrade_restores_old_release_and_config_despite_diagnostic_failure(self):
        self.manager.install(self.fixture.bundle("a"))
        config = (self.manager.config_root / "device.json").read_bytes()
        self.manager.health_checker = self.fail_health
        with self.diagnostic_failure():
            self.assert_health_failure(lambda: self.manager.install(self.fixture.bundle("b")))
        status = self.manager.status()
        self.assertEqual(status["current"], "a" * 40)
        self.assertEqual(status["previous"], "b" * 40)
        self.assertFalse(status["transaction_pending"])
        self.assertEqual((self.manager.config_root / "device.json").read_bytes(), config)
        self.assertEqual(self.fixture.manager().recover()["current"], "a" * 40)

    def test_failed_rollback_restores_the_release_it_replaced(self):
        self.manager.install(self.fixture.bundle("a"))
        self.manager.install(self.fixture.bundle("b"))
        self.manager.health_checker = self.fail_health
        with self.diagnostic_failure():
            self.assert_health_failure(self.manager.rollback)
        status = self.manager.status()
        self.assertEqual(status["current"], "b" * 40)
        self.assertEqual(status["previous"], "a" * 40)
        self.assertFalse(status["transaction_pending"])

    def test_repeated_install_stops_services_without_losing_previous(self):
        self.manager.install(self.fixture.bundle("a"))
        bundle = self.fixture.bundle("b")
        self.manager.install(bundle)
        self.fixture.commands.clear()
        self.manager.health_checker = self.fail_health
        with self.diagnostic_failure():
            self.assert_health_failure(lambda: self.manager.install(bundle))
        status = self.manager.status()
        self.assertEqual(status["current"], "b" * 40)
        self.assertEqual(status["previous"], "a" * 40)
        self.assertIn(("systemctl", "disable", "--now", "rp-ylx.service"), self.fixture.commands)

    def test_ordinary_diagnostic_failures_do_not_replace_primary_error(self):
        bundle = self.fixture.bundle("a")
        self.manager.health_checker = self.fail_health
        for failure in (
            PermissionError("private failure detail"),
            OSError(errno.EIO, "I/O"),
            RuntimeError("serialization"),
            deploy.DeploymentError("diagnostic_failed", "diagnostic"),
        ):
            with self.diagnostic_failure(failure):
                error = self.assert_health_failure(lambda: self.manager.install(bundle))
            self.assertIn(type(failure).__name__, error.message)
            self.assertNotIn("private failure detail", error.message)
            self.assertFalse(self.manager.status()["installed"])

    def test_normal_diagnostic_preserves_original_error_and_payload(self):
        self.manager.health_checker = self.fail_health
        with self.assertRaises(deploy.DeploymentError) as caught:
            self.manager.install(self.fixture.bundle("a"))
        self.assertIs(caught.exception, self.health_error)
        self.assertEqual(caught.exception.message, "health probe failed")
        self.assertEqual(json.loads(self.path.read_bytes())["code"], "service_unhealthy")
        self.assertFalse(self.manager.status()["installed"])

    def test_previous_diagnostic_is_not_deleted_to_hide_write_failure(self):
        self.manager._ensure_layout()
        raw = b'{"schema":"ylx.activation-failure.v1","code":"old","message":"old"}'
        self.path.write_bytes(raw)
        self.manager.health_checker = self.fail_health
        with self.diagnostic_failure():
            self.assert_health_failure(lambda: self.manager.install(self.fixture.bundle("a")))
        self.assertEqual(self.path.read_bytes(), raw)

    def test_cleanup_failure_has_priority_and_mentions_diagnostic_failure(self):
        self.manager.health_checker = self.fail_health
        cleanup_error = deploy.DeploymentError("unit_deactivation_failed", "could not stop")
        with (
            self.diagnostic_failure(),
            patch.object(
                self.manager, "_deactivate_systemd_units", side_effect=cleanup_error
            ) as stop,
            self.assertRaises(deploy.DeploymentError) as caught,
        ):
            self.manager.install(self.fixture.bundle("a"))
        self.assertEqual(caught.exception.code, "activation_cleanup_failed")
        self.assertIn("激活失败诊断未能保存", caught.exception.message)
        self.assertIn("could not stop", caught.exception.message)
        self.assertIs(caught.exception.__cause__, cleanup_error)
        stop.assert_called_once()
        self.assertEqual(self.manager.status()["current"], "a" * 40)

    def test_rollback_storage_failure_is_not_reported_as_success(self):
        self.manager.install(self.fixture.bundle("a"))
        original = deploy._write_atomic
        failed_health = False

        def health():
            nonlocal failed_health
            failed_health = True
            raise self.health_error

        def fail_storage(path, value):
            if failed_health and path in (self.path, self.manager.transaction):
                raise OSError(errno.ENOSPC, "injected storage full")
            return original(path, value)

        self.manager.health_checker = health
        self.fixture.commands.clear()
        with (
            patch.object(deploy, "_write_atomic", side_effect=fail_storage),
            self.assertRaises(deploy.DeploymentError) as caught,
        ):
            self.manager.install(self.fixture.bundle("b"))
        self.assertEqual(caught.exception.code, "activation_cleanup_failed")
        self.assertIn("激活失败诊断未能保存", caught.exception.message)
        self.assertEqual(self.fixture.commands.count(("systemctl", "stop", "rp-ylx.service")), 2)
        self.assertEqual(self.manager.status()["current"], "b" * 40)

    def test_diagnostic_keyboard_interrupt_still_stops_failed_first_release(self):
        cancelled = KeyboardInterrupt()
        self.manager.health_checker = self.fail_health
        with self.diagnostic_failure(cancelled), self.assertRaises(KeyboardInterrupt) as caught:
            self.manager.install(self.fixture.bundle("a"))
        self.assertIs(caught.exception, cancelled)
        self.assertFalse(self.manager.status()["installed"])
        self.assertIs(caught.exception.__cause__, self.health_error)
        self.assertFalse(self.fixture.manager().recover()["installed"])

    def test_diagnostic_system_exit_restores_old_release_then_propagates(self):
        self.manager.install(self.fixture.bundle("a"))
        self.manager.health_checker = self.fail_health
        cancelled = SystemExit(77)
        with self.diagnostic_failure(cancelled), self.assertRaises(SystemExit) as caught:
            self.manager.install(self.fixture.bundle("b"))
        self.assertIs(caught.exception, cancelled)
        self.assertEqual(caught.exception.code, 77)
        self.assertEqual(self.fixture.manager().recover()["current"], "a" * 40)

    def test_cleanup_cancellation_is_not_converted_to_normal_failure(self):
        cancelled = KeyboardInterrupt()
        self.manager.health_checker = self.fail_health
        with (
            self.diagnostic_failure(),
            patch.object(self.manager, "_deactivate_systemd_units", side_effect=cancelled),
            self.assertRaises(KeyboardInterrupt) as caught,
        ):
            self.manager.install(self.fixture.bundle("a"))
        self.assertIs(caught.exception, cancelled)
        self.assertIn("activation_cleanup_failed", " ".join(caught.exception.__notes__))
        self.assertIn("激活失败诊断未能保存", " ".join(caught.exception.__notes__))

    def test_diagnostic_cancellation_reports_cleanup_failure_without_swallowing_cancel(self):
        cancelled = KeyboardInterrupt()
        cleanup_error = deploy.DeploymentError("unit_deactivation_failed", "could not stop")
        self.manager.health_checker = self.fail_health
        with (
            self.diagnostic_failure(cancelled),
            patch.object(self.manager, "_deactivate_systemd_units", side_effect=cleanup_error),
            self.assertRaises(KeyboardInterrupt) as caught,
        ):
            self.manager.install(self.fixture.bundle("a"))
        self.assertIs(caught.exception, cancelled)
        self.assertIs(caught.exception.__cause__, cleanup_error)
        self.assertIn("activation_cleanup_failed", " ".join(caught.exception.__notes__))

    def test_cli_returns_primary_code_and_safe_diagnostic_notice(self):
        self.manager.health_checker = self.fail_health
        bundle = self.fixture.bundle("a")
        output, errors = io.StringIO(), io.StringIO()
        with (
            self.diagnostic_failure(PermissionError("private failure detail")),
            patch.object(deploy, "ReleaseManager", return_value=self.manager),
            redirect_stdout(output),
            redirect_stderr(errors),
        ):
            result = deploy.main(["install", str(bundle)])
        self.assertEqual(result, 2)
        self.assertEqual(output.getvalue(), "")
        error = json.loads(errors.getvalue())["error"]
        self.assertEqual(error["code"], "service_unhealthy")
        self.assertIn("激活失败诊断未能保存", error["message"])
        self.assertNotIn("private failure detail", errors.getvalue())
        self.assertFalse(self.manager.status()["installed"])

    def test_rollback_keeps_maintenance_write_guard(self):
        self.manager.install(self.fixture.bundle("a"))
        self.manager.health_checker = self.fail_health
        original = self.manager._restore_release
        peer = self.fixture.manager()

        def restore(old, failed):
            self.assertTrue(self.manager._recovery_exclusive)
            with self.assertRaises(deploy.DeploymentError) as busy:
                peer.uninstall()
            self.assertEqual(busy.exception.code, "deployment_busy")
            return original(old, failed)

        with (
            self.diagnostic_failure(),
            patch.object(self.manager, "_restore_release", side_effect=restore),
        ):
            self.assert_health_failure(lambda: self.manager.install(self.fixture.bundle("b")))
        self.assertEqual(self.manager.status()["current"], "a" * 40)

    def test_healthy_retry_after_failed_diagnostic_completes(self):
        self.manager.install(self.fixture.bundle("a"))
        bundle = self.fixture.bundle("b")
        self.manager.health_checker = self.fail_health
        with self.diagnostic_failure():
            self.assert_health_failure(lambda: self.manager.install(bundle))
        self.manager.health_checker = self.good_health
        status = self.manager.install(bundle)
        self.assertEqual(status["current"], "b" * 40)
        self.assertEqual(status["previous"], "a" * 40)
        self.assertFalse(status["transaction_pending"])
        self.assertIsNone(status["activation_failure"])

    def test_actual_diagnostic_directory_failure_still_deactivates(self):
        self.manager._ensure_layout()
        self.path.mkdir()
        self.manager.health_checker = self.fail_health
        error = self.assert_health_failure(lambda: self.manager.install(self.fixture.bundle("a")))
        self.assertIn("IsADirectoryError", error.message)
        self.assertTrue(self.path.is_dir())
        self.assertFalse(self.manager.status()["installed"])
        self.assertEqual(list(self.manager.state_root.glob(".activation-failure.json.*.tmp")), [])

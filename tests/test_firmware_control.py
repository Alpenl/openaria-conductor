from __future__ import annotations

import json
import os
import tempfile
import threading
import time
import unittest
import uuid
from pathlib import Path
from unittest.mock import Mock, patch

from rp_ylx import update
from rp_ylx.firmware_control import FirmwareError, FirmwareSupervisor, dispatch


class FirmwareControlTest(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name)
        self.install = self.root / "install"
        self.install.mkdir()
        self.state = self.root / "state"
        self.make_release("a", "0.2.1", "current")
        self.make_release("b", "0.1.0", "previous")
        self.supervisor = FirmwareSupervisor(self.state, self.install)
        self.manifest = {
            "version": "0.2.2",
            "commit": "c" * 40,
            "release_notes": "修复录制问题",
            "published_at": "2026-09-17T00:00:00Z",
        }
        p = patch("rp_ylx.update.fetch_manifest", return_value=self.manifest)
        self.fetch = p.start()
        self.addCleanup(p.stop)
        p = patch("rp_ylx.update.require_idle")
        self.idle = p.start()
        self.addCleanup(p.stop)

    def make_release(self, char, version, link):
        target = self.install / "releases" / (char * 40)
        target.mkdir(parents=True, exist_ok=True)
        (target / "release.json").write_text(json.dumps({"version": version, "commit": char * 40}))
        path = self.install / link
        # Production switches links atomically; readers must never observe the
        # unlink/create gap introduced by the former concurrent test fixture.
        temporary = path.with_name(path.name + ".next")
        temporary.symlink_to(target)
        temporary.replace(path)

    def wait_task(self):
        deadline = time.monotonic() + 3
        while time.monotonic() < deadline:
            status = self.supervisor.status()
            if status["task"]["status"] in {"succeeded", "failed"}:
                return status
            time.sleep(0.01)
        self.fail("firmware worker did not complete")

    def test_check_cache_force_and_error_never_claim_latest(self):
        self.assertIsNone(self.supervisor.status()["checked_at"])
        result = self.supervisor.check()
        self.assertTrue(result["has_update"])
        self.supervisor.check()
        self.fetch.assert_called_once()
        self.fetch.side_effect = OSError("offline")
        result = self.supervisor.check(force=True)
        self.assertIn("无法", result["warning"])
        self.assertEqual(result["available"]["version"], "0.2.2")

    def test_channel_rejects_other_series_and_does_not_offer_downgrade(self):
        for version in ("0.3.0", "0.2.0", "0.1.0", "0.2.2-rc.1"):
            self.fetch.return_value = {**self.manifest, "version": version}
            self.assertIsNotNone(self.supervisor.check(force=True)["warning"])
        self.fetch.return_value = {**self.manifest, "version": "0.2.1"}
        self.assertTrue(self.supervisor.check(force=True)["has_update"])
        self.fetch.return_value = {**self.manifest, "version": "0.2.1", "commit": "a" * 40}
        self.assertFalse(self.supervisor.check(force=True)["has_update"])

    def test_legacy_channel_is_distinguished_from_network_failure(self):
        self.supervisor.check(force=True)
        self.fetch.return_value = {**self.manifest, "version": "0.1.0"}
        status = self.supervisor.check(force=True)
        self.assertIn("已连接 OSS", status["warning"])
        self.assertIsNone(status["available"])
        self.assertFalse(status["has_update"])
        self.assertIsNotNone(status["checked_at"])
        self.fetch.side_effect = ValueError("invalid checksum")
        self.assertIn("清单校验失败", self.supervisor.check(force=True)["warning"])
        self.fetch.side_effect = OSError("offline")
        self.assertIn("无法连接 OSS", self.supervisor.check(force=True)["warning"])

    def test_task_survives_new_browser_and_retries_are_idempotent(self):
        calls = []

        def runner(task):
            calls.append(task["id"])
            self.make_release("c", "0.2.2", "current")

        self.supervisor.runner = runner
        key = str(uuid.uuid4())
        result = self.supervisor.start("update", key, "c" * 40)
        self.assertEqual(result["status"], "accepted")
        self.assertEqual(self.wait_task()["task"]["status"], "succeeded")
        restored = FirmwareSupervisor(self.state, self.install, runner=runner)
        self.assertEqual(restored.status()["current"]["version"], "0.2.2")
        self.assertEqual(restored.start("update", key, "c" * 40)["status"], "succeeded")
        self.assertEqual(calls, [key])
        with self.assertRaises(FirmwareError):
            restored.start("rollback", key, "b" * 40)

    def test_busy_recording_and_changed_release_do_not_start_worker(self):
        self.supervisor.runner = Mock()
        self.idle.side_effect = ValueError("recording")
        with self.assertRaises(FirmwareError) as raised:
            self.supervisor.start("update", str(uuid.uuid4()), "c" * 40)
        self.assertEqual(raised.exception.code, "capture_active")
        self.idle.side_effect = None
        with self.assertRaises(FirmwareError) as raised:
            self.supervisor.start("update", str(uuid.uuid4()), "d" * 40)
        self.assertEqual(raised.exception.code, "release_changed")
        self.supervisor.runner.assert_not_called()

    def test_parallel_start_is_rejected_and_failed_task_is_observable(self):
        started, finish = threading.Event(), threading.Event()

        def runner(task):
            started.set()
            finish.wait(2)
            raise RuntimeError("failed activation")

        self.supervisor.runner = runner
        self.supervisor.start("update", str(uuid.uuid4()), "c" * 40)
        self.assertTrue(started.wait(2))
        try:
            with self.assertRaises(FirmwareError) as raised:
                self.supervisor.start("rollback", str(uuid.uuid4()), "b" * 40)
            self.assertEqual(raised.exception.code, "update_busy")
        finally:
            finish.set()
        result = self.wait_task()
        self.assertEqual(result["task"]["status"], "failed")
        self.assertEqual(result["current"]["commit"], "a" * 40)

    def test_rollback_works_without_fetching_channel(self):
        self.fetch.side_effect = OSError("offline")
        self.supervisor.runner = lambda task: self.make_release("b", "0.1.0", "current")
        self.supervisor.start("rollback", str(uuid.uuid4()), "b" * 40)
        self.assertEqual(self.wait_task()["task"]["status"], "succeeded")
        self.fetch.assert_not_called()

    def test_restart_marks_unfinished_task_failed_not_successful(self):
        self.state.mkdir()
        (self.state / "task.json").write_text(
            json.dumps({"id": str(uuid.uuid4()), "status": "running"})
        )
        result = FirmwareSupervisor(self.state, self.install).status()
        self.assertEqual(result["task"]["status"], "failed")

    def test_commands_reject_arbitrary_paths_and_arguments(self):
        for request in (
            {"operation": "shell", "command": "id"},
            {"operation": "update", "key": str(uuid.uuid4()), "commit": "../x"},
            {"operation": "check", "force": "true"},
            {
                "operation": "update",
                "key": str(uuid.uuid4()),
                "commit": "c" * 40,
                "manifest": "https://untrusted.test/manifest",
            },
        ):
            with self.assertRaises(FirmwareError):
                dispatch(self.supervisor, request)

    def test_real_maintenance_flock_excludes_capture_and_update_both_ways(self):
        with patch.dict(os.environ, {"OPENARIA_MAINTENANCE_LOCK": str(self.root / "maintenance")}):
            with (
                update.maintenance_lock(shared=True),
                self.assertRaises(ValueError),
                update.maintenance_lock(),
            ):
                self.fail("update must not run during capture")
            with (
                update.maintenance_lock(),
                self.assertRaises(ValueError),
                update.maintenance_lock(shared=True),
            ):
                self.fail("capture must not run during update")
            with update.maintenance_lock(shared=True):
                pass

    def test_first_install_creates_missing_runtime_lock_directory(self):
        path = self.root / "run/rp-ylx/maintenance.lock"
        with (
            patch.dict(os.environ, {"OPENARIA_MAINTENANCE_LOCK": str(path)}),
            update.maintenance_lock(),
        ):
            self.assertTrue(path.is_file())
            with self.assertRaises(ValueError), update.maintenance_lock():
                self.fail("first-install lock did not exclude a second updater")

    def test_capture_coordinator_holds_maintenance_lease_for_recording_lifetime(self):
        from rp_ylx.api.gateway import ProviderError
        from rp_ylx.recording.coordinator import CaptureCoordinator

        coordinator = CaptureCoordinator.__new__(CaptureCoordinator)
        coordinator._network_operation_lease = None
        with (
            patch.dict(os.environ, {"OPENARIA_MAINTENANCE_LOCK": str(self.root / "maintenance")}),
            patch.object(
                coordinator, "_network_operation_lock_path", return_value=self.root / "network"
            ),
        ):
            with update.maintenance_lock(), self.assertRaises(ProviderError) as raised:
                coordinator._acquire_network_operation_lease()
            self.assertEqual(raised.exception.code, "maintenance_active")
            coordinator._acquire_network_operation_lease()
            try:
                with self.assertRaises(ValueError), update.maintenance_lock():
                    self.fail("updater entered during a capture lease")
            finally:
                coordinator._release_network_operation_lease()
            with update.maintenance_lock():
                pass

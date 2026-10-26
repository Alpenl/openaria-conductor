from __future__ import annotations

import errno
import json
import os
import select
import socket
import subprocess
import sys
import threading
import unittest
from pathlib import Path
from unittest.mock import patch

from rp_ylx import deployment as deploy
from tests import test_deployment as fixtures

CHILD = """import json,runpy,sys,time
from pathlib import Path
d=runpy.run_path(sys.argv[1]);root=Path(sys.argv[2]);action=sys.argv[3]
def forbidden(command):raise AssertionError('system runner must not execute')
m=d['ReleaseManager'](install_root=root/'opt/rp-ylx',config_root=root/'etc/rp-ylx',
 state_root=root/'var/lib/rp-ylx',system_root=root,target_machine='aarch64',
 target_model='D-Robotics RDK X5 V1.0',library_finder=lambda name:'library',
 executable_finder=lambda name:'executable',runner=forbidden)
if action=='hold':
 with m._maintenance_operation():
  print('ready',flush=True);time.sleep(60)
else:
 try:
  if action=='install':value=m.install(root/'unused')
  elif action=='service':value=m.recover(service_start=True)
  else:value=getattr(m,action)()
 except d['DeploymentError'] as error:value={'error':error.code}
 print(json.dumps(value))
"""


class DeploymentMutexTest(unittest.TestCase):
    def setUp(self):
        self.fixture = fixtures.ReleaseManagerTest()
        self.fixture.setUp()
        self.addCleanup(self.fixture.tearDown)
        self.manager = self.fixture.manager()
        self.peer = self.fixture.manager()
        self.lock_root = self.fixture.root / "var/lib/openaria-update"
        self.lock_path = self.lock_root / "deployment-operation.lock"

    def assert_code(self, code, callback):
        with self.assertRaises(deploy.DeploymentError) as error:
            callback()
        self.assertEqual(error.exception.code, code)

    def child(self, action):
        result = subprocess.run(
            [sys.executable, "-I", "-c", CHILD, deploy.__file__, str(self.fixture.root), action],
            capture_output=True,
            text=True,
            timeout=8,
            check=True,
        )
        return json.loads(result.stdout)

    def test_every_public_mutation_rejects_competition_before_actions(self):
        with self.manager._maintenance_operation():
            for manager in (self.manager, self.peer):
                for callback in (
                    lambda manager=manager: manager.install(self.fixture.root / "unused"),
                    manager.rollback,
                    manager.recover,
                    manager.uninstall,
                    lambda manager=manager: manager.recover(service_start=True),
                ):
                    self.assert_code("deployment_busy", callback)
        self.assertFalse(self.fixture.commands)
        self.assertFalse(self.manager.install_root.exists())
        self.assertFalse(self.manager.state_root.exists())

    def test_cross_process_mutations_are_rejected(self):
        with self.manager._maintenance_operation():
            for action in ("install", "rollback", "recover", "uninstall", "service"):
                self.assertEqual(self.child(action), {"error": "deployment_busy"})
        self.assertFalse(self.fixture.commands)

    def test_shared_system_assets_use_one_lock_despite_different_layouts(self):
        self.peer.install_root = self.fixture.root / "other-install"
        self.peer.state_root = self.fixture.root / "other-state"
        with self.manager._maintenance_operation():
            self.assert_code("deployment_busy", self.peer.recover)
        self.assertFalse(self.peer.install_root.exists())
        self.assertFalse(self.peer.state_root.exists())

    def test_independent_system_roots_do_not_contend(self):
        self.peer.system_root = self.fixture.root / "other-system"
        with self.manager._maintenance_operation(), self.peer._maintenance_operation():
            self.assertFalse(self.peer.status()["installed"])

    def test_status_remains_available_and_lock_files_survive_release(self):
        with self.manager._maintenance_operation():
            identity = self.lock_path.stat().st_ino
            self.assertFalse(self.peer.status()["installed"])
            self.assertFalse(os.get_inheritable(self.manager._recovery_descriptor))
        self.assertEqual(self.lock_path.stat().st_ino, identity)
        with self.peer._maintenance_operation():
            self.assertEqual(self.lock_path.stat().st_ino, identity)

    def test_original_concurrent_install_is_rejected_then_retry_keeps_rollback(self):
        first = self.fixture.bundle("a")
        second = self.fixture.bundle("b")
        ready = threading.Event()
        resume = threading.Event()
        errors = []
        results = []
        original = self.manager._link_target
        current_reads = 0

        def pause(name):
            nonlocal current_reads
            value = original(name)
            if name == "current":
                current_reads += 1
                if current_reads == 2:
                    ready.set()
                    if not resume.wait(20):
                        raise TimeoutError("test schedule")
            return value

        def install():
            try:
                with patch.object(self.manager, "_link_target", side_effect=pause):
                    results.append(self.manager.install(first))
            except BaseException as error:
                errors.append(error)

        worker = threading.Thread(target=install)
        worker.start()
        try:
            self.assertTrue(ready.wait(10))
            self.assert_code("deployment_busy", lambda: self.peer.install(second))
            self.assertTrue((self.manager.releases / ("a" * 40) / "release.json").is_file())
        finally:
            resume.set()
            worker.join(timeout=30)
        self.assertFalse(worker.is_alive())
        self.assertEqual(errors, [])
        self.assertEqual(results[0]["current"], "a" * 40)
        retried = self.peer.install(second)
        self.assertEqual(retried["current"], "b" * 40)
        self.assertEqual(retried["previous"], "a" * 40)
        self.assertFalse(retried["transaction_pending"])
        self.assertEqual(self.peer.recover()["current"], "b" * 40)

    def test_install_rollback_and_uninstall_release_locks_for_retry(self):
        self.manager.install(self.fixture.bundle("a"))
        self.peer.install(self.fixture.bundle("b"))
        self.assertEqual(self.manager.rollback()["current"], "a" * 40)
        self.assertFalse(self.peer.uninstall()["installed"])
        self.assertFalse(self.manager.recover()["installed"])
        self.assertTrue(self.lock_path.exists())

    def test_service_dependency_can_check_committed_release_during_activation(self):
        original = self.manager.runner
        delegated = []

        def start_service(command):
            original(command)
            if tuple(command) == ("systemctl", "enable", "--now", "rp-ylx.service"):
                delegated.append(self.peer.recover(service_start=True))
                self.assert_code("deployment_busy", self.peer.recover)
                self.assert_code("deployment_busy", self.peer.uninstall)

        self.manager.runner = start_service
        self.manager.install(self.fixture.bundle("a"))
        self.assertEqual(len(delegated), 1)
        self.assertTrue(delegated[0]["recovery_delegated"])
        self.assertEqual(delegated[0]["current"], "a" * 40)

    def test_cross_process_service_check_is_readonly_and_does_not_prune(self):
        self.manager.install(self.fixture.bundle("a"), activate=False)
        staging = self.manager.releases / ".keep.staging-test"
        staging.mkdir()
        with self.manager._maintenance_operation(), self.manager._service_recovery_readers():
            result = self.child("service")
            self.assertTrue(result["recovery_delegated"])
            self.assertEqual(result["current"], "a" * 40)
            self.assertTrue(staging.exists())
            self.assertEqual(self.child("recover"), {"error": "deployment_busy"})
        self.peer.recover()
        self.assertFalse(staging.exists())

    def test_delegated_recovery_rejects_pending_transaction_and_missing_metadata(self):
        self.manager.install(self.fixture.bundle("a"), activate=False)
        transaction = self.manager._transaction_document("install", None, "a" * 40, "prepared")
        with self.manager._maintenance_operation(), self.manager._service_recovery_readers():
            self.manager.transaction.write_text(json.dumps(transaction))
            self.assert_code("deployment_busy", lambda: self.peer.recover(service_start=True))
            self.assertEqual(json.loads(self.manager.transaction.read_bytes()), transaction)
            self.manager.transaction.unlink()
            (self.manager.releases / ("a" * 40) / "release.json").unlink()
            self.assert_code("release_incomplete", lambda: self.peer.recover(service_start=True))

    def test_service_check_without_contention_performs_normal_recovery(self):
        self.assertFalse(self.peer.recover(service_start=True)["installed"])
        self.assertNotIn("recovery_delegated", self.peer.recover(service_start=True))

    def test_process_termination_releases_kernel_locks(self):
        process = subprocess.Popen(
            [sys.executable, "-I", "-c", CHILD, deploy.__file__, str(self.fixture.root), "hold"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        try:
            readable, _, _ = select.select([process.stdout], [], [], 8)
            self.assertTrue(readable)
            self.assertEqual(process.stdout.readline().strip(), "ready")
            self.assert_code("deployment_busy", self.manager.recover)
            process.kill()
            process.wait(timeout=8)
            self.assertFalse(self.manager.recover()["installed"])
        finally:
            if process.poll() is None:
                process.kill()
            process.communicate(timeout=8)
            process.stdout.close()
            process.stderr.close()

    def test_failure_and_cancellation_release_both_locks(self):
        for error in (RuntimeError("failure"), KeyboardInterrupt()):
            with (
                patch.object(self.manager, "_install_locked", side_effect=error),
                self.assertRaises(type(error)),
            ):
                self.manager.install(self.fixture.root / "unused")
            self.assertFalse(self.peer.recover()["installed"])

    def test_cleanup_closes_all_descriptors_without_masking_cancellation(self):
        original = deploy.os.close
        closed = []

        def close(fd):
            closed.append(fd)
            original(fd)
            raise OSError("close report")

        with (
            patch.object(deploy.os, "close", side_effect=close),
            self.assertRaises(KeyboardInterrupt),
            self.manager._maintenance_operation(),
        ):
            raise KeyboardInterrupt()
        self.assertEqual(len(closed), 4)
        self.assertEqual(len(set(closed)), 4)
        self.assertFalse(self.peer.recover()["installed"])

    def test_flock_io_error_is_typed_and_retry_succeeds(self):
        with patch.object(deploy.fcntl, "flock", side_effect=OSError(errno.EIO, "flock")):
            self.assert_code("deployment_lock_invalid", self.manager.recover)
        self.assertFalse(self.peer.recover()["installed"])

    def test_lock_special_files_and_links_are_rejected_without_target_changes(self):
        self.lock_root.mkdir(parents=True)
        target = self.fixture.root / "target"
        target.write_bytes(b"keep")
        target.chmod(0o600)
        for kind in ("symlink", "hardlink", "fifo", "directory", "socket"):
            peer = None
            if kind == "symlink":
                self.lock_path.symlink_to(target)
            elif kind == "hardlink":
                os.link(target, self.lock_path)
            elif kind == "fifo":
                os.mkfifo(self.lock_path)
            elif kind == "directory":
                self.lock_path.mkdir()
            else:
                peer = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
                peer.bind(str(self.lock_path))
            try:
                self.assert_code("deployment_lock_invalid", self.manager.recover)
                self.assertEqual(target.read_bytes(), b"keep")
                self.assertEqual(target.stat().st_mode & 0o777, 0o600)
            finally:
                if peer:
                    peer.close()
                if kind == "directory":
                    self.lock_path.rmdir()
                else:
                    self.lock_path.unlink()

    def test_untrusted_lock_directory_and_file_modes_are_rejected(self):
        self.lock_root.mkdir(parents=True)
        self.lock_root.chmod(0o777)
        self.assert_code("deployment_lock_invalid", self.manager.recover)
        self.lock_root.chmod(0o700)
        self.lock_path.write_bytes(b"keep")
        self.lock_path.chmod(0o666)
        self.assert_code("deployment_lock_invalid", self.manager.recover)
        self.assertEqual(self.lock_path.read_bytes(), b"keep")

    def test_symlink_lock_directory_is_rejected(self):
        self.lock_root.parent.mkdir(parents=True)
        target = self.fixture.root / "target-directory"
        target.mkdir()
        self.lock_root.symlink_to(target, target_is_directory=True)
        self.assert_code("deployment_lock_invalid", self.manager.recover)
        self.assertEqual(list(target.iterdir()), [])

    def test_lock_replacement_during_acquisition_is_rejected(self):
        original = deploy.fcntl.flock
        replaced = False

        def replace(fd, mode):
            nonlocal replaced
            original(fd, mode)
            if not replaced:
                replaced = True
                self.lock_path.rename(self.lock_path.with_suffix(".old"))
                self.lock_path.touch(mode=0o600)

        with patch.object(deploy.fcntl, "flock", side_effect=replace):
            self.assert_code("deployment_lock_invalid", self.manager.recover)
        self.assertFalse(self.peer.recover()["installed"])

    def test_activation_failure_reacquires_write_guard_before_rollback(self):
        self.manager.install(self.fixture.bundle("a"))
        original = self.manager._record_activation_failure

        def record(error, commit):
            self.assertTrue(self.manager._recovery_exclusive)
            self.assert_code("deployment_busy", lambda: self.peer.recover(service_start=True))
            original(error, commit)

        self.manager.health_checker = lambda: (_ for _ in ()).throw(
            deploy.DeploymentError("service_unhealthy", "test")
        )
        with patch.object(self.manager, "_record_activation_failure", side_effect=record):
            self.assert_code(
                "service_unhealthy", lambda: self.manager.install(self.fixture.bundle("b"))
            )
        self.assertEqual(self.manager.status()["current"], "a" * 40)
        self.assertFalse(self.manager.status()["transaction_pending"])

    def test_reacquire_failure_does_not_run_unprotected_cleanup(self):
        original = deploy.fcntl.flock

        def fail(fd, mode):
            if mode == deploy.fcntl.LOCK_EX:
                raise OSError(errno.EIO, "reacquire")
            return original(fd, mode)

        with patch.object(deploy.fcntl, "flock", side_effect=fail):
            self.assert_code(
                "deployment_lock_invalid", lambda: self.manager.install(self.fixture.bundle("a"))
            )
        self.assertFalse((self.manager.state_root / "activation-failure.json").exists())
        self.assertEqual(self.peer.recover()["current"], "a" * 40)

    def test_reacquire_failure_preserves_primary_health_error(self):
        original = deploy.fcntl.flock

        def fail(fd, mode):
            if mode == deploy.fcntl.LOCK_EX:
                raise OSError(errno.EIO, "reacquire")
            return original(fd, mode)

        self.manager.health_checker = lambda: (_ for _ in ()).throw(
            deploy.DeploymentError("service_unhealthy", "test")
        )
        with patch.object(deploy.fcntl, "flock", side_effect=fail):
            self.assert_code(
                "service_unhealthy", lambda: self.manager.install(self.fixture.bundle("a"))
            )
        self.assertFalse((self.manager.state_root / "activation-failure.json").exists())
        self.assertEqual(self.peer.recover()["current"], "a" * 40)

    def test_recovery_service_uses_guarded_start_mode_and_writable_lock_root(self):
        text = (Path(deploy.__file__).parent / "deploy/rp-ylx-recover.service").read_text()
        self.assertIn("ExecStart=/usr/local/sbin/rp-ylx-deploy recover --service-start\n", text)
        self.assertIn("ReadWritePaths=/var/lib/openaria-update\n", text)

    def test_service_check_cannot_accept_an_uninstalled_state(self):
        with self.manager._maintenance_operation(), self.manager._service_recovery_readers():
            self.assert_code("deployment_busy", lambda: self.peer.recover(service_start=True))
        self.assertFalse(self.manager.state_root.exists())

    def test_reacquire_waits_for_readers_before_allowing_mutation(self):
        entered = threading.Event()
        finish = threading.Event()
        writable = threading.Event()
        errors = []

        def operation():
            try:
                with self.manager._maintenance_operation():
                    with self.manager._service_recovery_readers():
                        entered.set()
                        if not finish.wait(10):
                            raise TimeoutError("reader schedule")
                    self.assertTrue(self.manager._recovery_exclusive)
                    writable.set()
            except BaseException as error:
                errors.append(error)

        worker = threading.Thread(target=operation)
        worker.start()
        try:
            self.assertTrue(entered.wait(10))
            with self.peer._deployment_lock("recovery", shared=True):
                finish.set()
                self.assertFalse(writable.wait(0.05))
                self.assert_code("deployment_busy", self.peer.uninstall)
        finally:
            finish.set()
            worker.join(timeout=30)
        self.assertFalse(worker.is_alive())
        self.assertEqual(errors, [])
        self.assertTrue(writable.is_set())

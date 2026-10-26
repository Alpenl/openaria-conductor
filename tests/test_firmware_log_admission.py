from __future__ import annotations

import os
import socket
import stat
import subprocess
import sys
import unittest
import uuid
from unittest.mock import Mock, patch

import tests.test_firmware_control as fixtures
from rp_ylx import firmware_control as control


class FirmwareLogAdmissionTest(unittest.TestCase):
    def setUp(self):
        self.fixture = fixtures.FirmwareControlTest()
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        self.s = self.fixture.supervisor
        self.s.state.mkdir()
        self.path = self.s.state / "last-update.log"
        self.task = {"action": "update", "target_commit": "c" * 40}
        self.process = Mock()
        self.process.wait.return_value = 0
        self.context = Mock()
        self.context.__enter__ = Mock(return_value=self.process)
        self.context.__exit__ = Mock(return_value=False)

    def run_update(self):
        self.s._run_update(self.task)

    def assert_rejected(self):
        before = len(os.listdir("/proc/self/fd"))
        with patch.object(control.subprocess, "Popen") as child:
            with self.assertRaises((OSError, ValueError)):
                self.run_update()
            child.assert_not_called()
        self.assertEqual(len(os.listdir("/proc/self/fd")), before)

    def test_symlink_and_hardlink_targets_preserve_bytes_and_permissions(self):
        target = self.fixture.root / "unrelated"
        target.write_bytes(b"must be preserved")
        target.chmod(0o644)
        for kind in ["symlink", "hardlink"]:
            with self.subTest(kind=kind):
                if kind == "symlink":
                    self.path.symlink_to(target)
                else:
                    os.link(target, self.path)
                self.assert_rejected()
                self.assertEqual(target.read_bytes(), b"must be preserved")
                self.assertEqual(stat.S_IMODE(target.stat().st_mode), 0o644)
                self.path.unlink()

    def test_dangling_symlink_does_not_create_target(self):
        target = self.fixture.root / "absent"
        self.path.symlink_to(target)
        self.assert_rejected()
        self.assertFalse(target.exists())
        self.assertTrue(self.path.is_symlink())

    def test_fifo_directory_and_socket_are_rejected_before_child_start(self):
        for kind in ["fifo", "directory", "socket"]:
            with self.subTest(kind=kind):
                peer = None
                if kind == "fifo":
                    os.mkfifo(self.path)
                elif kind == "directory":
                    self.path.mkdir()
                else:
                    peer = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
                    peer.bind(str(self.path))
                try:
                    self.assert_rejected()
                finally:
                    if peer:
                        peer.close()
                    if kind == "directory":
                        self.path.rmdir()
                    else:
                        self.path.unlink()

    def test_symlink_parent_is_rejected_without_opening_log_target(self):
        original = self.s.state.with_name("original-state")
        self.s.state.rename(original)
        self.s.state.symlink_to(original, target_is_directory=True)
        (original / self.path.name).write_bytes(b"preserve")
        self.assert_rejected()
        self.assertEqual((original / self.path.name).read_bytes(), b"preserve")

    def test_new_log_is_private_and_both_child_streams_share_owned_fd(self):
        seen = []

        def spawn(command, **kwargs):
            fd = kwargs["stdout"]
            self.assertIsInstance(fd, int)
            self.assertEqual(fd, kwargs["stderr"])
            self.assertTrue(kwargs["start_new_session"])
            self.assertEqual(command[-2:], ["--expected-commit", "c" * 40])
            self.assertFalse(os.get_inheritable(fd))
            os.write(fd, b"child stdout and stderr\n")
            seen.append(fd)
            return self.context

        with patch.object(control.subprocess, "Popen", side_effect=spawn):
            self.run_update()
        self.assertEqual(self.path.read_bytes(), b"child stdout and stderr\n")
        self.assertEqual(stat.S_IMODE(self.path.stat().st_mode), 0o600)
        with self.assertRaises(OSError):
            os.fstat(seen[0])
        self.process.wait.assert_called_once_with(timeout=3600)

    def test_existing_regular_log_is_truncated_only_after_admission(self):
        self.path.write_bytes(b"old log")
        self.path.chmod(0o666)
        inode = self.path.stat().st_ino
        with patch.object(control.subprocess, "Popen", return_value=self.context):
            self.run_update()
        self.assertEqual(self.path.read_bytes(), b"")
        self.assertEqual(self.path.stat().st_ino, inode)
        self.assertEqual(stat.S_IMODE(self.path.stat().st_mode), 0o600)

    def test_child_start_failure_closes_directory_and_log_descriptors(self):
        before = len(os.listdir("/proc/self/fd"))
        error = OSError("spawn unavailable")
        with (
            patch.object(control.subprocess, "Popen", side_effect=error),
            self.assertRaises(OSError) as raised,
        ):
            self.run_update()
        self.assertIs(raised.exception, error)
        self.assertEqual(len(os.listdir("/proc/self/fd")), before)
        self.assertEqual(stat.S_IMODE(self.path.stat().st_mode), 0o600)

    def test_nonzero_child_exit_preserves_log_and_closes_descriptors(self):
        before = len(os.listdir("/proc/self/fd"))

        def spawn(*_, **kwargs):
            os.write(kwargs["stdout"], b"installer failed")
            self.process.wait.return_value = 1
            return self.context

        with (
            patch.object(control.subprocess, "Popen", side_effect=spawn),
            self.assertRaises(RuntimeError),
        ):
            self.run_update()
        self.assertEqual(self.path.read_bytes(), b"installer failed")
        self.assertEqual(len(os.listdir("/proc/self/fd")), before)

    def test_create_race_does_not_overwrite_new_file(self):
        original = control.os.open

        def create(path, flags, *args, **kwargs):
            if path == self.path.name:
                self.path.write_bytes(b"new unrelated file")
            return original(path, flags, *args, **kwargs)

        with patch.object(control.os, "open", side_effect=create):
            self.assert_rejected()
        self.assertEqual(self.path.read_bytes(), b"new unrelated file")

    def test_existing_file_replaced_before_open_is_not_truncated(self):
        self.path.write_bytes(b"old log")
        old = self.path.with_name("old-log")
        original = control.os.open

        def replace(path, flags, *args, **kwargs):
            if path == self.path.name:
                self.path.rename(old)
                self.path.write_bytes(b"replacement")
            return original(path, flags, *args, **kwargs)

        with patch.object(control.os, "open", side_effect=replace):
            self.assert_rejected()
        self.assertEqual(old.read_bytes(), b"old log")
        self.assertEqual(self.path.read_bytes(), b"replacement")

    def test_replacement_after_open_is_detected_before_chmod_or_truncate(self):
        self.path.write_bytes(b"old log")
        self.path.chmod(0o644)
        old = self.path.with_name("old-log")
        original = control.os.fstat
        changed = []

        def replace(fd):
            info = original(fd)
            if stat.S_ISREG(info.st_mode) and not changed:
                self.path.rename(old)
                self.path.write_bytes(b"replacement")
                changed.append(True)
            return info

        with patch.object(control.os, "fstat", side_effect=replace):
            self.assert_rejected()
        self.assertEqual(old.read_bytes(), b"old log")
        self.assertEqual(stat.S_IMODE(old.stat().st_mode), 0o644)
        self.assertEqual(self.path.read_bytes(), b"replacement")

    def test_hardlink_added_after_open_is_detected_before_chmod(self):
        self.path.write_bytes(b"preserve")
        self.path.chmod(0o644)
        alias = self.fixture.root / "alias"
        original = control.os.fstat
        changed = []

        def link(fd):
            info = original(fd)
            if stat.S_ISREG(info.st_mode) and not changed:
                os.link(self.path, alias)
                changed.append(True)
            return info

        with patch.object(control.os, "fstat", side_effect=link):
            self.assert_rejected()
        self.assertEqual(alias.read_bytes(), b"preserve")
        self.assertEqual(stat.S_IMODE(alias.stat().st_mode), 0o644)

    def test_parent_replacement_keeps_both_log_files_unchanged(self):
        self.path.write_bytes(b"old log")
        old = self.s.state.with_name("old-state")
        original = control.os.fstat
        changed = []

        def replace(fd):
            info = original(fd)
            if stat.S_ISREG(info.st_mode) and not changed:
                self.s.state.rename(old)
                self.s.state.mkdir()
                self.path.write_bytes(b"new log")
                changed.append(True)
            return info

        with patch.object(control.os, "fstat", side_effect=replace):
            self.assert_rejected()
        self.assertEqual((old / self.path.name).read_bytes(), b"old log")
        self.assertEqual(self.path.read_bytes(), b"new log")

    def test_chmod_and_truncate_failures_do_not_start_child_or_leak_fds(self):
        for operation in ["fchmod", "ftruncate"]:
            with self.subTest(operation=operation):
                self.path.write_bytes(b"preserve")
                with patch.object(control.os, operation, side_effect=OSError("storage failure")):
                    self.assert_rejected()
                self.assertEqual(self.path.read_bytes(), b"preserve")

    def test_fstat_failure_closes_already_opened_directory_and_file(self):
        self.path.write_bytes(b"preserve")
        original = control.os.fstat

        def fail(fd):
            info = original(fd)
            if stat.S_ISREG(info.st_mode):
                raise OSError("fstat failure")
            return info

        with patch.object(control.os, "fstat", side_effect=fail):
            self.assert_rejected()
        self.assertEqual(self.path.read_bytes(), b"preserve")

    def test_cleanup_error_preserves_primary_and_attempts_both_closes(self):
        before = len(os.listdir("/proc/self/fd"))
        original = control.os.close
        closed = []
        primary = RuntimeError("child spawn failed")

        def close(fd):
            original(fd)
            closed.append(fd)
            if len(closed) == 1:
                raise OSError("close reported error")

        with (
            patch.object(control.os, "close", side_effect=close),
            patch.object(control.subprocess, "Popen", side_effect=primary),
            self.assertRaises(RuntimeError) as raised,
        ):
            self.run_update()
        self.assertIs(raised.exception, primary)
        self.assertEqual(len(closed), 2)
        self.assertEqual(len(os.listdir("/proc/self/fd")), before)

    def test_cleanup_failure_without_primary_is_reported_after_both_closes(self):
        before = len(os.listdir("/proc/self/fd"))
        original = control.os.close
        closed = []

        def close(fd):
            original(fd)
            closed.append(fd)
            if len(closed) == 1:
                raise OSError("close reported error")

        with (
            patch.object(control.os, "close", side_effect=close),
            patch.object(control.subprocess, "Popen", return_value=self.context),
            self.assertRaises(OSError),
        ):
            self.run_update()
        self.assertEqual(len(closed), 2)
        self.assertEqual(len(os.listdir("/proc/self/fd")), before)

    def test_cancellation_during_file_admission_closes_owned_descriptors(self):
        before = len(os.listdir("/proc/self/fd"))
        for error in [KeyboardInterrupt(), SystemExit(7)]:
            with (
                patch.object(control.os, "fchmod", side_effect=error),
                patch.object(control.subprocess, "Popen") as child,
            ):
                with self.assertRaises(type(error)) as raised:
                    self.run_update()
                self.assertIs(raised.exception, error)
                child.assert_not_called()
            self.assertEqual(len(os.listdir("/proc/self/fd")), before)

    def test_failed_log_admission_finishes_task_and_repair_allows_new_update(self):
        target = self.fixture.root / "unrelated"
        target.write_bytes(b"preserve")
        self.path.symlink_to(target)
        with patch.object(control.subprocess, "Popen") as child:
            self.s.start("update", str(uuid.uuid4()), "c" * 40)
            worker = self.s._worker
            if worker is not None:
                worker.join(3)
                self.assertFalse(worker.is_alive())
            self.assertEqual(self.s.status()["task"]["status"], "failed")
            child.assert_not_called()
        self.assertEqual(target.read_bytes(), b"preserve")
        self.path.unlink()

        def spawn(*_, **kwargs):
            os.write(kwargs["stdout"], b"successful stub update")
            self.fixture.make_release("c", "0.2.2", "current")
            return self.context

        with patch.object(control.subprocess, "Popen", side_effect=spawn):
            self.s.start("update", str(uuid.uuid4()), "c" * 40)
            worker = self.s._worker
            if worker is not None:
                worker.join(3)
                self.assertFalse(worker.is_alive())
        self.assertEqual(self.s.status()["task"]["status"], "succeeded")
        self.assertEqual(self.path.read_bytes(), b"successful stub update")

    def test_real_benign_child_writes_both_streams_to_private_log(self):
        real_popen = subprocess.Popen
        children = []
        before = len(os.listdir("/proc/self/fd"))

        def spawn(command, **kwargs):
            self.assertEqual(command[:2], ["/usr/bin/python3", "/usr/local/lib/openaria/update.py"])
            child = real_popen(
                [sys.executable, "-c", "import os;os.write(1,b'out\\n');os.write(2,b'err\\n')"],
                **kwargs,
            )
            children.append(child)
            return child

        with patch.object(control.subprocess, "Popen", side_effect=spawn):
            self.run_update()
        self.assertEqual(self.path.read_bytes(), b"out\nerr\n")
        self.assertEqual(children[0].returncode, 0)
        self.assertEqual(stat.S_IMODE(self.path.stat().st_mode), 0o600)
        self.assertEqual(len(os.listdir("/proc/self/fd")), before)

    def test_timeout_preserves_primary_and_releases_log_after_group_cleanup(self):
        before = len(os.listdir("/proc/self/fd"))
        timeout = subprocess.TimeoutExpired(["blocked-installer-stub"], 3600)
        self.process.wait.side_effect = [timeout, 0]
        self.process.pid = 12345
        with (
            patch.object(control.subprocess, "Popen", return_value=self.context),
            patch.object(control.os, "killpg") as kill,
            self.assertRaises(subprocess.TimeoutExpired) as raised,
        ):
            self.run_update()
        self.assertIs(raised.exception, timeout)
        kill.assert_called_once_with(12345, control.signal.SIGKILL)
        self.assertEqual(self.process.wait.call_count, 2)
        self.assertEqual(len(os.listdir("/proc/self/fd")), before)

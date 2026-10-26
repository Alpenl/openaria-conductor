from __future__ import annotations

import errno
import json
import os
import socket
import subprocess
import sys
import threading
import unittest
from unittest.mock import patch

from rp_ylx import deployment as deploy
from tests import test_deployment as fixtures


class DeploymentLinkStateTest(unittest.TestCase):
    def setUp(self):
        self.fixture = fixtures.ReleaseManagerTest()
        self.fixture.setUp()
        self.addCleanup(self.fixture.tearDown)
        self.manager = self.fixture.manager()
        self.manager._ensure_layout()
        self.current = self.manager.install_root / "current"
        self.previous = self.manager.install_root / "previous"

    def assert_invalid(self, callback=None):
        with self.assertRaises(deploy.DeploymentError) as error:
            (callback or self.manager.status)()
        self.assertEqual(error.exception.code, "install_state_invalid")
        self.assertFalse(self.fixture.commands)

    def test_missing_links_are_uninstalled(self):
        status = self.manager.status()
        self.assertFalse(status["installed"])
        self.assertIsNone(status["current"])
        self.assertIsNone(status["previous"])

    def test_status_does_not_create_missing_layout(self):
        root = self.fixture.root / "missing"
        manager = deploy.ReleaseManager(install_root=root / "install", state_root=root / "state")
        self.assertFalse(manager.status()["installed"])
        self.assertFalse(root.exists())

    def test_dangling_valid_version_link_preserves_existing_semantics(self):
        self.manager._switch("current", "a" * 40)
        self.assertFalse((self.manager.releases / ("a" * 40)).exists())
        status = self.manager.status()
        self.assertTrue(status["installed"])
        self.assertEqual(status["current"], "a" * 40)

    def test_current_is_observed_once_during_install_uninstall_and_upgrade(self):
        for initial, following in ((None, "a" * 40), ("a" * 40, None), ("a" * 40, "b" * 40)):
            self.manager._switch("current", initial)
            original = self.manager._link_target
            reads = []

            def switch_after_read(name, original=original, reads=reads, following=following):
                value = original(name)
                reads.append(name)
                if name == "current":
                    self.manager._switch("current", following)
                return value

            with patch.object(self.manager, "_link_target", side_effect=switch_after_read):
                status = self.manager.status()
            self.assertEqual(status["installed"], status["current"] is not None)
            self.assertEqual(status["current"], initial)
            self.assertEqual(reads.count("current"), 1)
            self.assertEqual(self.manager.status()["current"], following)

    def test_removal_at_readlink_returns_absence(self):
        original = deploy.os.readlink
        for name in ("current", "previous"):
            self.manager._switch(name, "a" * 40)
            path = self.manager.install_root / name

            def remove(link, *args, path=path, **kwargs):
                if link == path:
                    path.unlink()
                return original(link, *args, **kwargs)

            with patch.object(deploy.os, "readlink", side_effect=remove):
                status = self.manager.status()
            self.assertIsNone(status[name])
            self.assertEqual(status["installed"], status["current"] is not None)

    def test_atomic_replacement_at_readlink_returns_complete_new_target(self):
        self.manager._switch("current", "a" * 40)
        original = deploy.os.readlink

        def replace(link, *args, **kwargs):
            if link == self.current:
                self.manager._switch("current", "b" * 40)
            return original(link, *args, **kwargs)

        with patch.object(deploy.os, "readlink", side_effect=replace):
            status = self.manager.status()
        self.assertTrue(status["installed"])
        self.assertEqual(status["current"], "b" * 40)

    def test_nonlinks_are_errors_and_remain_untouched(self):
        for path in (self.current, self.previous):
            for kind in ("file", "directory", "fifo", "socket"):
                peer = None
                if kind == "file":
                    path.write_bytes(b"keep")
                elif kind == "directory":
                    path.mkdir()
                elif kind == "fifo":
                    os.mkfifo(path)
                else:
                    peer = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
                    peer.bind(str(path))
                try:
                    self.assert_invalid()
                    self.assertTrue(os.path.lexists(path))
                    if kind == "file":
                        self.assertEqual(path.read_bytes(), b"keep")
                finally:
                    if peer:
                        peer.close()
                    if kind == "directory":
                        path.rmdir()
                    else:
                        path.unlink()

    def test_invalid_targets_return_install_state_error(self):
        for target in (
            "/outside",
            "../outside",
            "releases/../outside",
            "releases/short",
            "releases/" + "A" * 40,
            "releases/" + "g" * 40,
        ):
            self.current.symlink_to(target)
            try:
                self.assert_invalid()
                self.assertEqual(os.readlink(self.current), target)
            finally:
                self.current.unlink()

    def test_io_errors_are_typed_instead_of_absence(self):
        self.manager._switch("current", "a" * 40)
        for error in (
            PermissionError("denied"),
            OSError(errno.EIO, "I/O"),
            NotADirectoryError("parent"),
        ):
            with patch.object(deploy.os, "readlink", side_effect=error):
                self.assert_invalid()
        self.assertEqual(self.manager.status()["current"], "a" * 40)

    def test_parent_that_is_a_file_is_not_an_uninstalled_layout(self):
        root = self.fixture.root / "bad-root"
        root.write_bytes(b"keep")
        manager = deploy.ReleaseManager(install_root=root, state_root=self.manager.state_root)
        self.assert_invalid(manager.status)
        self.assertEqual(root.read_bytes(), b"keep")

    def test_cancellation_is_not_swallowed(self):
        with (
            patch.object(deploy.os, "readlink", side_effect=KeyboardInterrupt),
            self.assertRaises(KeyboardInterrupt),
        ):
            self.manager.status()

    def test_diagnostic_warning_and_pending_transaction_remain_visible(self):
        self.manager._switch("current", "a" * 40)
        self.manager._switch("previous", "b" * 40)
        (self.manager.state_root / "activation-failure.json").write_bytes(b"\xff")
        self.manager.transaction.symlink_to(self.fixture.root / "missing")
        status = self.manager.status()
        self.assertTrue(status["installed"])
        self.assertEqual(status["current"], "a" * 40)
        self.assertEqual(status["previous"], "b" * 40)
        self.assertTrue(status["transaction_pending"])
        self.assertEqual(status["activation_failure"]["code"], "diagnostic_unreadable")

    def test_actual_standalone_cli_rejects_fifo_and_recovers(self):
        os.mkfifo(self.current)
        code = r"""import contextlib,io,json,runpy,sys
from pathlib import Path
d=runpy.run_path(sys.argv[1]);root=Path(sys.argv[2]);args=['--install-root',str(root),'--state-root',str(root/'state'),'status']
output=io.StringIO();error=io.StringIO()
with contextlib.redirect_stdout(output),contextlib.redirect_stderr(error): result=d['main'](args)
assert result==2 and output.getvalue()==''
assert json.loads(error.getvalue())['error']['code']=='install_state_invalid'
(root/'current').unlink();(root/'current').symlink_to('releases/'+'a'*40)
output=io.StringIO();error=io.StringIO()
with contextlib.redirect_stdout(output),contextlib.redirect_stderr(error): result=d['main'](args)
value=json.loads(output.getvalue())
assert result==0 and error.getvalue()=='' and value['installed'] and value['current']=='a'*40
print(json.dumps({'recovered':True}))
"""
        result = subprocess.run(
            [sys.executable, "-I", "-c", code, deploy.__file__, str(self.manager.install_root)],
            capture_output=True,
            text=True,
            timeout=3,
            check=True,
        )
        self.assertEqual(json.loads(result.stdout), {"recovered": True})

    def test_install_rejects_corrupt_current_before_system_commands(self):
        self.current.write_bytes(b"keep")
        self.assert_invalid(lambda: self.manager.install(self.fixture.root / "unused-bundle"))
        self.assertEqual(self.current.read_bytes(), b"keep")
        self.assertFalse(self.manager.transaction.exists())

    def test_rollback_rejects_corrupt_previous_before_system_commands(self):
        self.manager._switch("current", "a" * 40)
        self.previous.write_bytes(b"keep")
        self.assert_invalid(self.manager.rollback)
        self.assertEqual(self.previous.read_bytes(), b"keep")
        self.assertEqual(os.readlink(self.current), "releases/" + "a" * 40)

    def test_repair_is_visible_on_the_next_query(self):
        self.current.write_bytes(b"bad")
        self.assert_invalid()
        self.current.unlink()
        self.manager._switch("current", "a" * 40)
        self.assertEqual(self.manager.status()["current"], "a" * 40)
        self.manager._switch("current", None)
        self.assertFalse(self.manager.status()["installed"])

    def test_real_thread_switching_keeps_each_current_pair_consistent(self):
        barrier = threading.Barrier(2)
        errors = []

        def switch():
            try:
                barrier.wait(timeout=10)
                for value in ("a" * 40, "b" * 40, None) * 10:
                    self.manager._switch("current", value)
            except BaseException as error:
                errors.append(error)

        worker = threading.Thread(target=switch)
        worker.start()
        try:
            barrier.wait(timeout=10)
            for _ in range(100):
                status = self.manager.status()
                self.assertEqual(status["installed"], status["current"] is not None)
                self.assertIn(status["current"], (None, "a" * 40, "b" * 40))
        finally:
            worker.join(timeout=30)
        self.assertFalse(worker.is_alive())
        self.assertEqual(errors, [])
        self.assertFalse(self.manager.status()["installed"])

from __future__ import annotations

import errno
import json
import os
import socket
import subprocess
import sys
import unittest
from unittest.mock import patch

from rp_ylx import deployment as deploy
from tests import test_deployment as fixtures


class ActivationDiagnosticTest(unittest.TestCase):
    def setUp(self):
        self.fixture = fixtures.ReleaseManagerTest()
        self.fixture.setUp()
        self.addCleanup(self.fixture.tearDown)
        self.manager = self.fixture.manager()
        self.manager._ensure_layout()
        self.path = self.manager.state_root / "activation-failure.json"
        self.value = {
            "schema": "ylx.activation-failure.v1",
            "release_commit": "a" * 40,
            "code": "service_unhealthy",
            "message": "启动失败，请检查服务",
        }

    def write(self, value=None):
        self.path.write_text(json.dumps(self.value if value is None else value))

    def assert_warning(self):
        status = self.manager.status()
        self.assertEqual(status["activation_failure"]["code"], "diagnostic_unreadable")
        self.assertFalse(status["installed"])
        self.assertIsNone(status["current"])
        self.assertIsNone(status["previous"])
        self.assertFalse(status["transaction_pending"])
        json.dumps(status, ensure_ascii=False, allow_nan=False).encode("utf-8")
        self.assertFalse(self.fixture.commands)

    def test_absent_diagnostic_is_not_a_failure(self):
        self.assertIsNone(self.manager.status()["activation_failure"])

    def test_writer_output_and_optional_fields_remain_readable(self):
        self.manager._record_activation_failure(
            deploy.DeploymentError(self.value["code"], self.value["message"]), "a" * 40
        )
        self.assertEqual(self.manager.status()["activation_failure"], self.value)
        value = dict(self.value)
        value.pop("release_commit")
        value["detail"] = {"attempt": 2}
        self.write(value)
        self.assertEqual(self.manager.status()["activation_failure"], value)

    def test_fifo_in_standalone_bootstrap_returns_and_recovers(self):
        os.mkfifo(self.path)
        code = r"""import json,runpy,sys
from pathlib import Path
d=runpy.run_path(sys.argv[1])
root=Path(sys.argv[2])
m=d['ReleaseManager'](state_root=root,install_root=root/'install')
assert m.status()['activation_failure']['code']=='diagnostic_unreadable'
p=root/'activation-failure.json';p.unlink()
value={'schema':'ylx.activation-failure.v1','code':'service_unhealthy','message':'failed'}
p.write_text(json.dumps(value))
assert m.status()['activation_failure']==value
print(json.dumps({'recovered':True}))
"""
        result = subprocess.run(
            [sys.executable, "-I", "-c", code, deploy.__file__, str(self.manager.state_root)],
            capture_output=True,
            text=True,
            timeout=3,
            check=True,
        )
        self.assertEqual(json.loads(result.stdout), {"recovered": True})

    def test_special_files_preserve_targets(self):
        target = self.fixture.root / "target.json"
        target.write_text(json.dumps(self.value))
        for kind in ("fifo", "directory", "socket", "symlink", "hardlink", "dangling"):
            peer = None
            if kind == "fifo":
                os.mkfifo(self.path)
            elif kind == "directory":
                self.path.mkdir()
            elif kind == "socket":
                peer = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
                peer.bind(str(self.path))
            elif kind == "symlink":
                self.path.symlink_to(target)
            elif kind == "hardlink":
                os.link(target, self.path)
            else:
                self.path.symlink_to(self.fixture.root / "absent")
            try:
                self.assert_warning()
                self.assertEqual(json.loads(target.read_bytes()), self.value)
            finally:
                if peer:
                    peer.close()
                if kind == "directory":
                    self.path.rmdir()
                else:
                    self.path.unlink()

    def test_corruption_is_visible_and_repaired_file_is_reread(self):
        for raw in (b"null", b"[]", b"{}", b"{", b"\xff", b'"text"'):
            self.path.write_bytes(raw)
            self.assert_warning()
            self.assertEqual(self.path.read_bytes(), raw)
            self.write()
            self.assertEqual(self.manager.status()["activation_failure"], self.value)

    def test_schema_and_fields_are_validated(self):
        for field in ("schema", "code", "message"):
            for bad in (None, [], {}, True, 3):
                self.write({**self.value, field: bad})
                self.assert_warning()
            value = dict(self.value)
            value.pop(field)
            self.write(value)
            self.assert_warning()
        for commit in (None, [], "../bad", "A" * 40, int("1" * 40)):
            self.write({**self.value, "release_commit": commit})
            self.assert_warning()
        self.write({**self.value, "schema": "unknown"})
        self.assert_warning()

    def test_unsafe_json_never_poison_status_serialization(self):
        for suffix in ('"code":"duplicate"', '"extra":NaN', '"extra":1e999', '"extra":"\\ud800"'):
            self.path.write_text(json.dumps(self.value)[:-1] + "," + suffix + "}")
            self.assert_warning()
        self.path.write_text('{"extra":' + "[" * 1000 + "]" * 1000 + "}")
        self.assert_warning()
        self.write({**self.value, "extra": [[[[0]]]]})
        self.assertEqual(self.manager.status()["activation_failure"]["extra"], [[[[0]]]])

    def test_exact_size_boundary_and_oversize(self):
        raw = json.dumps(self.value).encode()
        raw += b" " * (deploy.MAX_ACTIVATION_DIAGNOSTIC_BYTES - len(raw))
        self.path.write_bytes(raw)
        self.assertEqual(self.manager.status()["activation_failure"], self.value)
        self.path.write_bytes(raw + b" ")
        self.assert_warning()

    def test_symlink_parent_is_rejected_without_changing_contents(self):
        self.write()
        directory = self.path.parent
        saved = directory.with_name("saved-state")
        directory.rename(saved)
        directory.symlink_to(saved, target_is_directory=True)
        self.assert_warning()
        self.assertEqual(json.loads((saved / self.path.name).read_bytes()), self.value)

    def test_disappearing_file_warns_once_then_becomes_absent(self):
        self.write()
        original = deploy._read_deployment_json

        def remove(path, **kwargs):
            path.unlink()
            return original(path, **kwargs)

        with patch.object(deploy, "_read_deployment_json", side_effect=remove):
            self.assert_warning()
        self.assertIsNone(self.manager.status()["activation_failure"])

    def test_io_failure_preserves_file_and_retries(self):
        self.write()
        for error in (OSError(errno.EIO, "read failed"), PermissionError("denied")):
            with patch.object(deploy.os, "read", side_effect=error):
                self.assert_warning()
            self.assertEqual(json.loads(self.path.read_bytes()), self.value)
            self.assertEqual(self.manager.status()["activation_failure"], self.value)

    def test_stat_failure_is_diagnostic_warning(self):
        original = type(self.path).lstat

        def fail(path, *args, **kwargs):
            if path == self.path:
                raise PermissionError("denied")
            return original(path, *args, **kwargs)

        with patch.object(type(self.path), "lstat", new=fail):
            self.assert_warning()

    def test_replaced_with_fifo_after_stat_does_not_block(self):
        self.write()
        original = deploy.os.open

        def replace(path, flags, *args, **kwargs):
            if path == self.path.name:
                self.assertTrue(flags & os.O_NONBLOCK)
                self.path.unlink()
                os.mkfifo(self.path)
            return original(path, flags, *args, **kwargs)

        with patch.object(deploy.os, "open", side_effect=replace):
            self.assert_warning()

    def test_replacement_during_read_is_not_reported_as_trusted_diagnostic(self):
        self.write()
        original = deploy.os.read
        replaced = False

        def replace(fd, size):
            nonlocal replaced
            data = original(fd, size)
            if not replaced:
                replaced = True
                self.path.unlink()
                self.write({**self.value, "message": "new failure"})
            return data

        with patch.object(deploy.os, "read", side_effect=replace):
            self.assert_warning()
        self.assertEqual(self.manager.status()["activation_failure"]["message"], "new failure")

    def test_cancellation_propagates_and_closes_both_descriptors(self):
        self.write()
        original = deploy.os.close
        closed = []

        def close(fd):
            closed.append(fd)
            original(fd)

        with (
            patch.object(deploy.os, "read", side_effect=KeyboardInterrupt),
            patch.object(deploy.os, "close", side_effect=close),
            self.assertRaises(KeyboardInterrupt),
        ):
            self.manager.status()
        self.assertEqual(len(closed), 2)
        self.assertEqual(len(set(closed)), 2)
        self.assertEqual(self.manager.status()["activation_failure"], self.value)

    def test_diagnostic_failure_retains_real_install_and_transaction_status(self):
        for name, commit in (("current", "a" * 40), ("previous", "b" * 40)):
            (self.manager.releases / commit).mkdir()
            self.manager._switch(name, commit)
        self.manager.transaction.symlink_to(self.fixture.root / "missing")
        self.path.write_bytes(b"\xff")
        status = self.manager.status()
        self.assertTrue(status["installed"])
        self.assertEqual(status["current"], "a" * 40)
        self.assertEqual(status["previous"], "b" * 40)
        self.assertTrue(status["transaction_pending"])
        self.assertEqual(status["activation_failure"]["code"], "diagnostic_unreadable")
        self.assertFalse(self.fixture.commands)

from __future__ import annotations

import json
import os
import socket
import subprocess
import sys
import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch

from rp_ylx import deployment as deploy
from tests import test_deployment as fixtures


class DeploymentMetadataTest(unittest.TestCase):
    def setUp(self):
        self.fixture = fixtures.ReleaseManagerTest()
        self.fixture.setUp()
        self.addCleanup(self.fixture.tearDown)
        self.root = self.fixture.install_root
        self.release = self.root / "releases" / ("a" * 40)
        self.release.mkdir(parents=True)
        (self.root / "current").symlink_to("releases/" + "a" * 40)
        self.path = self.release / "release.json"
        self.value = {"schema": deploy.RELEASE_SCHEMA, "commit": "a" * 40, "version": "0.2.7"}
        self.path.write_text(json.dumps(self.value))
        self.original = self.path.read_bytes()
        self.manager = self.fixture.manager()

    def read(self, kind):
        if kind == "launcher":
            return deploy._current_release_metadata(self.root)[1]
        return self.manager._release_metadata(self.release)

    def assert_invalid(self, kind):
        with self.assertRaises(deploy.DeploymentError) as error:
            self.read(kind)
        self.assertEqual(error.exception.code, "release_incomplete")
        self.assertNotIn(str(self.path), str(error.exception))

    def test_valid_metadata_matches_in_both_readers(self):
        for kind in ("launcher", "manager"):
            self.assertEqual(self.read(kind), self.value)

    def test_standalone_bootstrap_rejects_fifo_and_recovers_without_package_imports(self):
        self.path.unlink()
        os.mkfifo(self.path)
        code = """import json,runpy,sys
from pathlib import Path
d=runpy.run_path(sys.argv[1]);root=Path(sys.argv[2]);path=root/'releases'/('a'*40)/'release.json'
try:d['_current_release_metadata'](root)
except d['DeploymentError'] as error:code=error.code
else:raise AssertionError('FIFO accepted')
path.unlink();path.write_text(json.dumps({'schema':d['RELEASE_SCHEMA'],'commit':'a'*40}))
release,value=d['_current_release_metadata'](root)
print(json.dumps({'code':code,'recovered':value['commit']=='a'*40}))
"""
        result = subprocess.run(
            [sys.executable, "-I", "-c", code, deploy.__file__, str(self.root)],
            capture_output=True,
            text=True,
            timeout=3,
            check=True,
        )
        self.assertEqual(
            json.loads(result.stdout), {"code": "release_incomplete", "recovered": True}
        )

    def test_legacy_metadata_keeps_optional_fields_optional(self):
        self.value.pop("version")
        self.path.write_text(json.dumps(self.value))
        for kind in ("launcher", "manager"):
            self.assertEqual(self.read(kind), self.value)

    def test_fifo_readers_return_promptly_in_real_subprocesses(self):
        self.path.unlink()
        os.mkfifo(self.path)
        code = """import json,sys
from pathlib import Path
from rp_ylx import deployment as d
root=Path(sys.argv[1])
try:
    if sys.argv[2]=='launcher':d._current_release_metadata(root)
    else:d.ReleaseManager._release_metadata(None,root/'releases'/('a'*40))
except d.DeploymentError as error:
    print(json.dumps({'code':error.code}))
else:raise AssertionError('FIFO accepted')
"""
        for kind in ("launcher", "manager"):
            with self.subTest(reader=kind):
                result = subprocess.run(
                    [sys.executable, "-c", code, str(self.root), kind],
                    capture_output=True,
                    text=True,
                    timeout=3,
                    check=True,
                )
                self.assertEqual(json.loads(result.stdout), {"code": "release_incomplete"})

    def test_special_metadata_files_rejected_before_file_open(self):
        target = self.fixture.root / "external.json"
        target.write_bytes(self.original)
        original_open = deploy.os.open

        def open_directory_only(path, *args, **kwargs):
            self.assertNotEqual(path, "release.json", "unsafe file must not be opened")
            return original_open(path, *args, **kwargs)

        for kind in ("fifo", "directory", "socket", "symlink", "hardlink"):
            self.path.unlink()
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
            else:
                os.link(target, self.path)
            try:
                with patch.object(deploy.os, "open", side_effect=open_directory_only):
                    for reader in ("launcher", "manager"):
                        self.assert_invalid(reader)
                self.assertEqual(target.read_bytes(), self.original)
            finally:
                if peer:
                    peer.close()
                if kind == "directory":
                    self.path.rmdir()
                else:
                    self.path.unlink()
                self.path.write_bytes(self.original)

    def test_symlink_release_directory_is_rejected(self):
        target = self.release.with_name("saved")
        self.release.rename(target)
        self.release.symlink_to(target, target_is_directory=True)
        for reader in ("launcher", "manager"):
            self.assert_invalid(reader)
        self.assertEqual((target / "release.json").read_bytes(), self.original)

    def test_exact_size_boundary_and_oversize_rejection(self):
        value = {**self.value, "padding": ""}
        value["padding"] = "x" * (65536 - len(json.dumps(value).encode()))
        self.path.write_text(json.dumps(value))
        for reader in ("launcher", "manager"):
            self.assertEqual(self.read(reader), value)
        with self.path.open("ab") as stream:
            stream.write(b" ")
        for reader in ("launcher", "manager"):
            self.assert_invalid(reader)

    def test_malformed_json_and_invalid_unicode_preserve_original_bytes(self):
        for payload in (
            b"{",
            b"null",
            b"[]",
            b"\xff",
            b"[" * 1000 + b"]" * 1000,
            json.dumps({**self.value, "extra": "\ud800"}).encode(),
        ):
            self.path.write_bytes(payload)
            for reader in ("launcher", "manager"):
                self.assert_invalid(reader)
            self.assertEqual(self.path.read_bytes(), payload)

    def test_duplicate_keys_and_nonfinite_numbers_are_rejected(self):
        for suffix in ('"commit":"' + "a" * 40 + '"', '"extra":NaN', '"extra":1e999'):
            raw = json.dumps(self.value)[:-1] + "," + suffix + "}"
            self.path.write_text(raw)
            for reader in ("launcher", "manager"):
                self.assert_invalid(reader)

    def test_schema_and_commit_identity_are_required(self):
        for fields in ({"schema": "wrong"}, {"schema": None}, {"commit": "b" * 40}, {"commit": []}):
            self.path.write_text(json.dumps({**self.value, **fields}))
            for reader in ("launcher", "manager"):
                self.assert_invalid(reader)

    def test_missing_metadata_then_repair(self):
        self.path.unlink()
        for reader in ("launcher", "manager"):
            self.assert_invalid(reader)
        self.path.write_bytes(self.original)
        self.assertEqual(self.read("launcher"), self.value)
        self.assertEqual(self.read("manager"), self.value)

    def test_replacement_before_open_is_rejected_then_repaired(self):
        replacement = self.path.with_name("next")
        replacement.write_bytes(self.original)
        original_open = deploy.os.open

        def replace(path, *args, **kwargs):
            if path == "release.json":
                replacement.replace(self.path)
            return original_open(path, *args, **kwargs)

        with patch.object(deploy.os, "open", side_effect=replace):
            self.assert_invalid("manager")
        self.assertEqual(self.read("manager"), self.value)

    def test_fifo_replacement_between_stat_and_open_does_not_block(self):
        original_open = deploy.os.open

        def replace(path, flags, *args, **kwargs):
            if path == "release.json":
                self.assertTrue(flags & os.O_NONBLOCK)
                self.assertTrue(flags & os.O_NOFOLLOW)
                self.path.unlink()
                os.mkfifo(self.path)
            return original_open(path, flags, *args, **kwargs)

        with patch.object(deploy.os, "open", side_effect=replace):
            self.assert_invalid("launcher")

    def test_replacement_during_read_is_rejected(self):
        replacement = self.path.with_name("next")
        replacement.write_bytes(self.original)
        original_read = deploy.os.read

        def read(fd, count):
            data = original_read(fd, count)
            if replacement.exists():
                replacement.replace(self.path)
            return data

        with patch.object(deploy.os, "read", side_effect=read):
            self.assert_invalid("manager")
        self.assertEqual(self.read("manager"), self.value)

    def test_directory_replacement_during_read_is_rejected(self):
        original_read = deploy.os.read
        moved = self.release.with_name("moved")

        def read(fd, count):
            if not moved.exists():
                self.release.rename(moved)
                self.release.mkdir()
                self.path.write_bytes(self.original)
            return original_read(fd, count)

        with patch.object(deploy.os, "read", side_effect=read):
            self.assert_invalid("launcher")
        self.assertEqual(self.read("launcher"), self.value)

    def test_growth_during_read_is_bounded(self):
        original_read = deploy.os.read
        requested = []

        def read(fd, count):
            if not requested:
                with self.path.open("ab") as stream:
                    stream.write(b" " * 131072)
            requested.append(count)
            return original_read(fd, count)

        with patch.object(deploy.os, "read", side_effect=read):
            self.assert_invalid("manager")
        self.assertEqual(sum(requested), 65537)

    def test_io_failures_are_typed_and_later_read_recovers(self):
        for operation in ("open", "read", "fstat"):
            with patch.object(deploy.os, operation, side_effect=OSError("fault")):
                self.assert_invalid("manager")
        self.assertEqual(self.read("manager"), self.value)

    def test_cancellation_closes_both_descriptors(self):
        original_close = deploy.os.close
        with (
            patch.object(deploy.os, "read", side_effect=KeyboardInterrupt),
            patch.object(deploy.os, "close", wraps=original_close) as close,
            self.assertRaises(KeyboardInterrupt),
        ):
            self.read("manager")
        self.assertEqual(close.call_count, 2)

    def test_cleanup_attempts_both_closes_and_preserves_primary(self):
        original_close = deploy.os.close
        closed = []
        primary = KeyboardInterrupt("primary")

        def close(fd):
            original_close(fd)
            closed.append(fd)
            raise OSError("close fault")

        with (
            patch.object(deploy.os, "read", side_effect=primary),
            patch.object(deploy.os, "close", side_effect=close),
            self.assertRaises(KeyboardInterrupt) as error,
        ):
            self.read("manager")
        self.assertIs(error.exception, primary)
        self.assertEqual(len(closed), 2)

    def test_cleanup_error_without_primary_is_typed(self):
        original_close = deploy.os.close
        closed = []

        def close(fd):
            original_close(fd)
            closed.append(fd)
            raise OSError("close fault")

        with patch.object(deploy.os, "close", side_effect=close):
            self.assert_invalid("manager")
        self.assertEqual(len(closed), 2)

    def test_launcher_does_not_execute_invalid_release_and_recovers(self):
        executable = self.release / "bin/rp-ylx"
        executable.parent.mkdir()
        executable.write_text("test executable")
        executable.chmod(0o700)
        executor, notifier = Mock(), Mock()
        self.path.write_bytes(b"[]")
        with self.assertRaises(deploy.DeploymentError):
            deploy.serve_network_control_launcher(self.root, executor=executor, notifier=notifier)
        executor.assert_not_called()
        notifier.assert_not_called()
        self.path.write_text(
            json.dumps({**self.value, "network_control_protocol": deploy.NETWORK_CONTROL_PROTOCOL})
        )
        self.assertEqual(
            deploy.serve_network_control_launcher(self.root, executor=executor, notifier=notifier),
            0,
        )
        executor.assert_called_once()

    def test_existing_release_reuse_refuses_damage_without_running_stage(self):
        stage = Mock()
        self.manager.stage_installer = stage
        bundle = SimpleNamespace(commit="a" * 40)
        self.path.write_bytes(b"[]")
        with self.assertRaises(deploy.DeploymentError):
            self.manager._prepare_release(bundle)
        stage.assert_not_called()
        self.path.write_bytes(self.original)
        self.assertEqual(self.manager._prepare_release(bundle), self.release)
        stage.assert_not_called()

    def test_settle_rejects_corrupt_target_before_switching_or_deleting_transaction(self):
        self.manager._ensure_layout()
        transaction = self.manager._transaction_document("install", None, "a" * 40, "prepared")
        deploy._write_atomic(self.manager.transaction, transaction)
        before = self.manager.transaction.read_bytes()
        (self.root / "current").unlink()
        self.path.write_bytes(b"[]")
        with self.assertRaises(deploy.DeploymentError):
            self.manager._settle(transaction)
        self.assertFalse(os.path.lexists(self.root / "current"))
        self.assertEqual(self.manager.transaction.read_bytes(), before)
        self.path.write_bytes(self.original)
        self.manager._settle(transaction)
        self.assertEqual(os.readlink(self.root / "current"), "releases/" + "a" * 40)

    def test_public_rollback_rejects_before_quiesce_then_recovers(self):
        fixture = fixtures.ReleaseManagerTest()
        fixture.setUp()
        self.addCleanup(fixture.tearDown)
        manager = fixture.manager()
        manager.install(fixture.bundle("a"), activate=False)
        manager.install(fixture.bundle("b"), activate=False)
        path = manager.releases / ("a" * 40) / "release.json"
        saved = path.read_bytes()
        path.write_bytes(b"[]")
        fixture.commands.clear()
        with self.assertRaises(deploy.DeploymentError) as error:
            manager.rollback(activate=False)
        self.assertEqual(error.exception.code, "release_incomplete")
        self.assertEqual(fixture.commands, [])
        self.assertEqual(manager.status()["current"], "b" * 40)
        self.assertFalse(manager.transaction.exists())
        path.write_bytes(saved)
        self.assertEqual(manager.rollback(activate=False)["current"], "a" * 40)

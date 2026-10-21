from __future__ import annotations

import json
import os
import socket
import subprocess
import sys
import unittest
from unittest.mock import Mock, patch

from rp_ylx import deployment as deploy
from tests import test_deployment as fixtures


class InstallTransactionTest(unittest.TestCase):
    def setUp(self):
        self.fixture = fixtures.ReleaseManagerTest()
        self.fixture.setUp()
        self.addCleanup(self.fixture.tearDown)
        self.manager = self.fixture.manager()
        self.manager._ensure_layout()
        self.path = self.manager.transaction
        self.value = self.manager._transaction_document("install", None, "a" * 40, "prepared")

    def write(self, value=None):
        self.path.write_text(json.dumps(self.value if value is None else value))

    def assert_invalid(self):
        with self.assertRaises(deploy.DeploymentError) as error:
            self.manager._read_transaction()
        self.assertEqual(error.exception.code, "install_state_invalid")
        self.assertNotIn(str(self.path), str(error.exception))

    def release(self, character):
        path = self.manager.releases / (character * 40)
        path.mkdir(exist_ok=True)
        (path / "release.json").write_text(
            json.dumps({"schema": deploy.RELEASE_SCHEMA, "commit": character * 40})
        )

    def test_genuine_absence_is_no_pending_transaction(self):
        self.assertIsNone(self.manager._read_transaction())
        self.assertFalse(self.manager.status()["transaction_pending"])

    def test_offline_bundle_rejects_numeric_commit_without_coercion(self):
        bundle = self.fixture.bundle("1")
        path = bundle / "bundle.json"
        value = json.loads(path.read_bytes())
        value["commit"] = int("1" * 40)
        path.write_text(json.dumps(value))
        with self.assertRaises(deploy.DeploymentError) as error:
            deploy.load_bundle(bundle)
        self.assertEqual(error.exception.code, "bundle_invalid")

    def test_current_and_legacy_writer_formats_remain_accepted(self):
        for action in ("install", "rollback"):
            for state in ("prepared", "switched"):
                value = self.manager._transaction_document(action, "a" * 40, "b" * 40, state)
                for snapshot_required in (True, False):
                    if not snapshot_required:
                        value.pop("config_snapshot_required")
                    self.write(value)
                    self.assertEqual(self.manager._read_transaction(), value)

    def test_fifo_standalone_reader_returns_promptly(self):
        os.mkfifo(self.path)
        code = """import json,runpy,sys
from pathlib import Path
from types import SimpleNamespace
d=runpy.run_path(sys.argv[1])
try:d['ReleaseManager']._read_transaction(SimpleNamespace(transaction=Path(sys.argv[2])))
except d['DeploymentError'] as error:print(json.dumps({'code':error.code}))
else:raise AssertionError('FIFO accepted')
"""
        result = subprocess.run(
            [sys.executable, "-I", "-c", code, deploy.__file__, str(self.path)],
            capture_output=True,
            text=True,
            timeout=3,
            check=True,
        )
        self.assertEqual(json.loads(result.stdout), {"code": "install_state_invalid"})

    def test_special_files_rejected_without_following_or_changing_targets(self):
        target = self.fixture.root / "target.json"
        target.write_text(json.dumps(self.value))
        original_open = deploy.os.open

        def open_directory_only(path, *args, **kwargs):
            self.assertNotEqual(path, self.path.name)
            return original_open(path, *args, **kwargs)

        for kind in ("fifo", "directory", "socket", "symlink", "hardlink"):
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
                    self.assert_invalid()
                self.assertEqual(json.loads(target.read_bytes()), self.value)
            finally:
                if peer:
                    peer.close()
                if kind == "directory":
                    self.path.rmdir()
                else:
                    self.path.unlink()

    def test_dangling_link_is_pending_and_blocks_recovery(self):
        self.path.symlink_to(self.fixture.root / "missing")
        self.assert_invalid()
        self.assertTrue(self.manager.status()["transaction_pending"])
        with self.assertRaises(deploy.DeploymentError) as error:
            self.manager.recover()
        self.assertEqual(error.exception.code, "install_state_invalid")
        self.assertTrue(self.path.is_symlink())

    def test_symlink_state_directory_is_rejected(self):
        self.write()
        directory = self.path.parent
        target = directory.with_name("saved-state")
        directory.rename(target)
        directory.symlink_to(target, target_is_directory=True)
        self.assert_invalid()
        self.assertEqual(json.loads((target / self.path.name).read_bytes()), self.value)

    def test_json_null_and_corrupt_contents_are_not_absence(self):
        for raw in (b"null", b"[]", b"{}", b"{", b"\xff", b'"text"', b"[" * 1000 + b"]" * 1000):
            self.path.write_bytes(raw)
            self.assert_invalid()
            self.assertEqual(self.path.read_bytes(), raw)

    def test_duplicate_keys_and_nonfinite_unicode_values_are_rejected(self):
        for suffix in ('"action":"rollback"', '"extra":NaN', '"extra":1e999', '"extra":"\\ud800"'):
            self.path.write_text(json.dumps(self.value)[:-1] + "," + suffix + "}")
            self.assert_invalid()

    def test_unhashable_actions_and_states_return_typed_error(self):
        for field in ("action", "state"):
            for value in ([], {}, None, True, 1):
                self.write({**self.value, field: value})
                self.assert_invalid()

    def test_fields_snapshot_marker_and_commits_are_validated(self):
        for field in self.value:
            if field == "config_snapshot_required":
                continue
            value = dict(self.value)
            value.pop(field)
            self.write(value)
            self.assert_invalid()
        for changes in (
            {"extra": 1},
            {"schema": []},
            {"config_snapshot_required": 1},
            {"config_snapshot_required": False},
            {"new_current": []},
            {"new_current": 10**39},
            {"old_current": 10**39},
            {"new_current": "A" * 40},
            {"old_current": False},
            {"old_current": "../bad"},
        ):
            self.write({**self.value, **changes})
            self.assert_invalid()

    def test_transaction_id_must_match_writer_uuid4_format(self):
        for value in (
            None,
            [],
            "",
            "not-a-uuid",
            "aaaaaaaa-aaaa-1aaa-8aaa-aaaaaaaaaaaa",
            "AAAAAAAA-AAAA-4AAA-8AAA-AAAAAAAAAAAA",
        ):
            self.write({**self.value, "transaction_id": value})
            self.assert_invalid()

    def test_exact_size_boundary_and_preopen_oversize_rejection(self):
        raw = json.dumps(self.value).encode()
        self.path.write_bytes(raw + b" " * (65536 - len(raw)))
        self.assertEqual(self.manager._read_transaction(), self.value)
        with self.path.open("ab") as stream:
            stream.write(b" ")
        self.assert_invalid()

    def test_disappearance_after_presence_check_is_not_no_transaction(self):
        self.write()
        original = deploy._read_deployment_json

        def disappear(path, **kwargs):
            path.unlink()
            return original(path, **kwargs)

        with patch.object(deploy, "_read_deployment_json", side_effect=disappear):
            self.assert_invalid()
        self.assertIsNone(self.manager._read_transaction())

    def test_replacement_before_open_is_rejected_then_repaired(self):
        self.write()
        replacement = self.path.with_name("next")
        replacement.write_text(json.dumps(self.value))
        original_open = deploy.os.open

        def replace(path, *args, **kwargs):
            if path == self.path.name:
                replacement.replace(self.path)
            return original_open(path, *args, **kwargs)

        with patch.object(deploy.os, "open", side_effect=replace):
            self.assert_invalid()
        self.assertEqual(self.manager._read_transaction(), self.value)

    def test_fifo_replacement_before_open_cannot_wait_for_writer(self):
        self.write()
        original_open = deploy.os.open

        def replace(path, flags, *args, **kwargs):
            if path == self.path.name:
                self.assertTrue(flags & os.O_NONBLOCK)
                self.assertTrue(flags & os.O_NOFOLLOW)
                self.path.unlink()
                os.mkfifo(self.path)
            return original_open(path, flags, *args, **kwargs)

        with patch.object(deploy.os, "open", side_effect=replace):
            self.assert_invalid()

    def test_growth_during_read_is_bounded(self):
        self.write()
        original_read = deploy.os.read
        requested = []

        def read(fd, count):
            if not requested:
                with self.path.open("ab") as stream:
                    stream.write(b" " * 131072)
            requested.append(count)
            return original_read(fd, count)

        with patch.object(deploy.os, "read", side_effect=read):
            self.assert_invalid()
        self.assertEqual(sum(requested), 65537)

    def test_io_error_is_typed_and_read_recovers(self):
        self.write()
        for operation in ("open", "read", "fstat"):
            with patch.object(deploy.os, operation, side_effect=OSError("fault")):
                self.assert_invalid()
        self.assertEqual(self.manager._read_transaction(), self.value)

    def test_cancellation_propagates_and_both_descriptors_close(self):
        self.write()
        original_close = deploy.os.close
        with (
            patch.object(deploy.os, "read", side_effect=KeyboardInterrupt),
            patch.object(deploy.os, "close", wraps=original_close) as close,
            self.assertRaises(KeyboardInterrupt),
        ):
            self.manager._read_transaction()
        self.assertEqual(close.call_count, 2)

    def test_recovery_preserves_damage_links_and_staging_then_retries(self):
        self.release("a")
        self.release("b")
        self.manager._switch("current", "a" * 40)
        staging = self.manager.releases / ".keep.staging-test"
        staging.mkdir()
        (staging / "sentinel").write_bytes(b"keep")
        self.path.write_bytes(b'{"action":[]')
        before = self.path.read_bytes()
        with self.assertRaises(deploy.DeploymentError) as error:
            self.manager.recover()
        self.assertEqual(error.exception.code, "install_state_invalid")
        self.assertEqual(self.path.read_bytes(), before)
        self.assertEqual(os.readlink(self.manager.install_root / "current"), "releases/" + "a" * 40)
        self.assertTrue(staging.is_dir())
        self.assertEqual(self.fixture.commands, [])
        value = self.manager._transaction_document("install", "a" * 40, "b" * 40, "prepared")
        value.pop("config_snapshot_required")
        self.write(value)
        result = self.manager.recover()
        self.assertEqual(result["current"], "b" * 40)
        self.assertEqual(result["previous"], "a" * 40)
        self.assertFalse(result["transaction_pending"])

    def test_install_and_rollback_reject_damage_before_system_actions(self):
        self.path.write_bytes(b"[]")
        stage, runner = Mock(), Mock()
        self.manager.stage_installer = stage
        self.manager.runner = runner
        for operation in (
            lambda: self.manager.install(self.fixture.root / "unused"),
            self.manager.rollback,
        ):
            with self.assertRaises(deploy.DeploymentError) as error:
                operation()
            self.assertEqual(error.exception.code, "install_state_invalid")
        stage.assert_not_called()
        runner.assert_not_called()

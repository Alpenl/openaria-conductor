from __future__ import annotations

import json
import os
import socket
import subprocess
import sys
import threading
import unittest
import uuid
from unittest.mock import Mock, patch

from rp_ylx import firmware_control as control
from tests import test_firmware_control as fixtures


class InstalledReleaseTest(unittest.TestCase):
    def setUp(self):
        self.fixture = fixtures.FirmwareControlTest()
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        self.root = self.fixture.install
        self.path = self.root / "releases" / ("a" * 40) / "release.json"
        self.original = self.path.read_bytes()
        self.supervisor = self.fixture.supervisor

    def read(self):
        return control.installed_release("current", self.root)

    def assert_invalid(self):
        with self.assertRaises(control.FirmwareError) as error:
            self.read()
        self.assertEqual(error.exception.code, "installed_release_invalid")
        self.assertEqual(error.exception.status, 503)
        self.assertNotIn(str(self.path), str(error.exception))

    def test_valid_absolute_and_relative_targets(self):
        expected = {"version": "0.2.1", "commit": "a" * 40}
        self.assertEqual(self.read(), expected)
        link = self.root / "current"
        link.unlink()
        link.symlink_to("releases/" + "a" * 40)
        self.assertEqual(self.read(), expected)

    def test_missing_or_unmanaged_install_keeps_unsupported_behavior(self):
        link = self.root / "current"
        link.unlink()
        self.assertIsNone(self.read())
        link.mkdir()
        self.assertIsNone(self.read())

    def test_dangling_or_looping_release_link_is_typed_error(self):
        link = self.root / "current"
        for target in ("missing", "current"):
            link.unlink()
            link.symlink_to(target)
            self.assert_invalid()

    def test_foreign_directory_and_invalid_commit_are_rejected(self):
        link = self.root / "current"
        for target in (self.fixture.root / ("a" * 40), self.root / "releases" / "wrong"):
            target.mkdir()
            (target / "release.json").write_bytes(self.original)
            link.unlink()
            link.symlink_to(target)
            self.assert_invalid()

    def test_fifo_query_returns_promptly_in_real_subprocess(self):
        self.path.unlink()
        os.mkfifo(self.path)
        code = """import json,sys
from pathlib import Path
from rp_ylx.firmware_control import installed_release,FirmwareError
try:
    installed_release('current',Path(sys.argv[1]))
except FirmwareError as error:
    print(json.dumps({'code':error.code,'status':error.status}))
else:
    raise AssertionError('FIFO accepted')
"""
        result = subprocess.run(
            [sys.executable, "-c", code, str(self.root)],
            capture_output=True,
            text=True,
            timeout=3,
            check=True,
        )
        self.assertEqual(
            json.loads(result.stdout), {"code": "installed_release_invalid", "status": 503}
        )

    def test_special_files_rejected_before_open_and_targets_preserved(self):
        target = self.fixture.root / "other.json"
        target.write_bytes(self.original)
        for kind in ("fifo", "directory", "socket", "symlink", "hardlink"):
            with self.subTest(kind=kind):
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
                    with patch.object(control.os, "open", side_effect=AssertionError("opened")):
                        self.assert_invalid()
                    self.assertEqual(target.read_bytes(), self.original)
                finally:
                    if peer:
                        peer.close()
                    if kind == "directory":
                        self.path.rmdir()
                    else:
                        self.path.unlink()
                    self.path.write_bytes(self.original)

    def test_oversize_rejected_before_open(self):
        self.path.write_bytes(b" " * (control.LIMIT + 1))
        with patch.object(control.os, "open", side_effect=AssertionError("opened")):
            self.assert_invalid()

    def test_exact_file_size_boundary_and_extra_metadata(self):
        value = {"version": "0.2.1", "commit": "a" * 40, "extra": ""}
        empty = json.dumps(value).encode()
        value["extra"] = "x" * (control.LIMIT - len(empty))
        self.path.write_text(json.dumps(value))
        self.assertEqual(self.path.stat().st_size, control.LIMIT)
        self.assertEqual(self.read()["commit"], "a" * 40)

    def test_malformed_json_utf8_duplicates_and_nonfinite_values(self):
        for payload in (
            b"{",
            b"[]",
            b"null",
            b"\xff",
            b'"text"',
            b'{"version":"0.2.1","version":"0.2.2","commit":"' + b"a" * 40 + b'"}',
            b'{"version":"0.2.1","commit":"' + b"a" * 40 + b'","extra":NaN}',
            b'{"version":"0.2.1","commit":"' + b"a" * 40 + b'","extra":"\\ud800"}',
            b"[" * 1000 + b"]" * 1000,
        ):
            with self.subTest(payload=payload[:20]):
                self.path.write_bytes(payload)
                self.assert_invalid()
                self.assertEqual(self.path.read_bytes(), payload)

    def test_invalid_identity_and_version_cannot_enter_status_response(self):
        for fields in (
            {"commit": "b" * 40},
            {"commit": None},
            {"version": []},
            {"version": ""},
            {"version": "0.2.1\n"},
            {"version": "0.2.1-rc.1"},
            {"version": "0.2." + "9" * 5000},
            {"version": "\ud800"},
        ):
            self.path.write_text(json.dumps({"version": "0.2.1", "commit": "a" * 40, **fields}))
            self.assert_invalid()

    def test_missing_metadata_is_error_then_restoration_recovers(self):
        self.path.unlink()
        self.assert_invalid()
        self.path.write_bytes(self.original)
        self.assertEqual(self.supervisor.status()["current"]["commit"], "a" * 40)

    def test_open_and_read_errors_are_typed(self):
        for name in ("open", "read", "fstat"):
            with (
                self.subTest(operation=name),
                patch.object(control.os, name, side_effect=OSError("io")),
            ):
                self.assert_invalid()
        self.assertEqual(self.read()["commit"], "a" * 40)

    def test_cancellation_propagates_and_descriptor_is_closed(self):
        original_close = control.os.close
        with (
            patch.object(control.os, "read", side_effect=KeyboardInterrupt),
            patch.object(control.os, "close", wraps=original_close) as close,
            self.assertRaises(KeyboardInterrupt),
        ):
            self.read()
        self.assertEqual(close.call_count, 1)
        self.assertEqual(self.read()["commit"], "a" * 40)

    def test_replacement_before_open_is_rejected_then_new_file_can_be_read(self):
        replacement = self.path.with_name("next.json")
        replacement.write_text(json.dumps({"version": "0.2.3", "commit": "a" * 40}))
        original_open = control.os.open

        def replace(path, *args, **kwargs):
            replacement.replace(self.path)
            return original_open(path, *args, **kwargs)

        with patch.object(control.os, "open", side_effect=replace):
            self.assert_invalid()
        self.assertEqual(self.read()["version"], "0.2.3")

    def test_regular_file_replaced_by_fifo_before_open_never_blocks(self):
        original_open = control.os.open

        def replace(path, flags, *args, **kwargs):
            self.assertTrue(flags & os.O_NONBLOCK)
            self.assertTrue(flags & os.O_NOFOLLOW)
            self.path.unlink()
            os.mkfifo(self.path)
            return original_open(path, flags, *args, **kwargs)

        with patch.object(control.os, "open", side_effect=replace):
            self.assert_invalid()

    def test_replacement_during_read_is_rejected(self):
        original_read = control.os.read
        replacement = self.path.with_name("next.json")
        replacement.write_bytes(self.original)
        replaced = []

        def read(fd, count):
            data = original_read(fd, count)
            if not replaced:
                replacement.replace(self.path)
                replaced.append(True)
            return data

        with patch.object(control.os, "read", side_effect=read):
            self.assert_invalid()
        self.assertEqual(self.read()["commit"], "a" * 40)

    def test_growth_during_read_is_bounded_and_rejected(self):
        original_read = control.os.read
        requested = []
        grown = []

        def read(fd, count):
            requested.append(count)
            if not grown:
                with self.path.open("ab") as stream:
                    stream.write(b" " * (control.LIMIT * 2))
                grown.append(True)
            return original_read(fd, count)

        with patch.object(control.os, "read", side_effect=read):
            self.assert_invalid()
        self.assertEqual(sum(requested), control.LIMIT + 1)

    def test_atomic_current_switch_returns_consistent_release(self):
        self.fixture.make_release("c", "0.2.3", "next")
        original_read = control.os.read
        switched = []

        def read(fd, count):
            if not switched:
                (self.root / "next").replace(self.root / "current")
                switched.append(True)
            return original_read(fd, count)

        with patch.object(control.os, "read", side_effect=read):
            self.assertEqual(self.read(), {"commit": "a" * 40, "version": "0.2.1"})
        self.assertEqual(self.read(), {"commit": "c" * 40, "version": "0.2.3"})

    def test_update_and_rollback_fail_before_runner_when_metadata_invalid(self):
        self.path.write_bytes(b"[]")
        runner = Mock()
        self.supervisor.runner = runner
        for action, commit in (("update", "c" * 40), ("rollback", "b" * 40)):
            with self.assertRaises(control.FirmwareError) as error:
                self.supervisor.start(action, str(uuid.uuid4()), commit)
            self.assertEqual(error.exception.code, "installed_release_invalid")
        runner.assert_not_called()
        self.fixture.idle.assert_not_called()
        self.fixture.fetch.assert_not_called()

    def test_real_socket_error_then_repair_without_service_restart(self):
        path = str(self.fixture.root / "control.sock")
        server = control.ControlServer(path, control.ControlHandler)
        server.supervisor = self.supervisor
        thread = threading.Thread(target=lambda: server.serve_forever(poll_interval=0.01))
        thread.start()
        try:
            with patch.object(control, "SOCKET", path):
                self.path.unlink()
                os.mkfifo(self.path)
                with self.assertRaises(control.FirmwareError) as error:
                    control.request_firmware("status")
                self.assertEqual(error.exception.code, "installed_release_invalid")
                self.path.unlink()
                self.path.write_bytes(self.original)
                result = control.request_firmware("status")
                self.assertEqual(result["current"]["commit"], "a" * 40)
        finally:
            server.shutdown()
            server.server_close()
            thread.join(3)
        self.assertFalse(thread.is_alive())

from __future__ import annotations

import errno
import json
import os
import shutil
import stat
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from rp_ylx import firmware_control, update


class UpdateAtomicCleanupTest(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name)
        self.path = self.root / "task.json"
        self.old = b'{"old":true}'
        self.path.write_bytes(self.old)

    def assert_old_only(self):
        self.assertEqual(self.path.read_bytes(), self.old)
        self.assertEqual(set(self.root.iterdir()), {self.path})

    def test_normal_unicode_publication_creates_private_parent_and_file(self):
        path = self.root / "nested" / "state.json"
        update.atomic_json(path, {"message": "写入完成"})
        self.assertEqual(json.loads(path.read_text(encoding="utf-8")), {"message": "写入完成"})
        self.assertEqual(stat.S_IMODE(path.stat().st_mode), 0o600)
        self.assertEqual(stat.S_IMODE(path.parent.stat().st_mode), 0o700)
        self.assertEqual(list(path.parent.iterdir()), [path])

    def test_serialization_failure_preserves_original_and_removes_partial_output(self):
        for writer in (update.atomic_json, firmware_control.atomic_json):
            with self.subTest(writer=writer.__module__), self.assertRaises(TypeError):
                writer(self.path, {"valid_prefix": "x" * 10000, "invalid": object()})
            self.assert_old_only()

    def test_file_fsync_failure_cleans_temporary_and_closes_descriptor(self):
        before = len(os.listdir("/proc/self/fd"))
        error = OSError(errno.ENOSPC, "injected full")
        with (
            patch.object(update.os, "fsync", side_effect=error),
            self.assertRaises(OSError) as raised,
        ):
            firmware_control.atomic_json(self.path, {"new": True})
        self.assertIs(raised.exception, error)
        self.assert_old_only()
        self.assertEqual(len(os.listdir("/proc/self/fd")), before)

    def test_replace_failure_preserves_original_and_removes_temporary(self):
        with (
            patch.object(update.os, "replace", side_effect=OSError("replace failed")),
            self.assertRaises(OSError),
        ):
            update.atomic_json(self.path, {"new": True})
        self.assert_old_only()

    def test_directory_fsync_failure_preserves_published_file_and_closes_descriptor(self):
        before = len(os.listdir("/proc/self/fd"))
        real = os.fsync

        def fsync(fd):
            if stat.S_ISDIR(os.fstat(fd).st_mode):
                raise OSError("directory fsync failed")
            return real(fd)

        with patch.object(update.os, "fsync", side_effect=fsync), self.assertRaises(OSError):
            update.atomic_json(self.path, {"new": True})
        self.assertEqual(json.loads(self.path.read_bytes()), {"new": True})
        self.assertEqual(list(self.root.iterdir()), [self.path])
        self.assertEqual(len(os.listdir("/proc/self/fd")), before)

    def test_directory_open_failure_does_not_roll_back_published_state(self):
        real = os.open

        def open_file(path, flags, *args, **kwargs):
            if flags & os.O_DIRECTORY:
                raise OSError("directory open failed")
            return real(path, flags, *args, **kwargs)

        with patch.object(update.os, "open", side_effect=open_file), self.assertRaises(OSError):
            update.atomic_json(self.path, {"new": True})
        self.assertEqual(json.loads(self.path.read_bytes()), {"new": True})
        self.assertEqual(list(self.root.iterdir()), [self.path])

    def test_exception_after_replace_does_not_delete_published_state(self):
        real = os.replace

        def replace_then_fail(source, target):
            real(source, target)
            raise OSError("failure after replacement")

        with (
            patch.object(update.os, "replace", side_effect=replace_then_fail),
            self.assertRaises(OSError),
        ):
            update.atomic_json(self.path, {"new": True})
        self.assertEqual(json.loads(self.path.read_bytes()), {"new": True})
        self.assertEqual(list(self.root.iterdir()), [self.path])

    def test_cancellation_cleans_temporary_and_preserves_control_exception(self):
        for error in (KeyboardInterrupt(), SystemExit(7)):
            with (
                self.subTest(error=type(error).__name__),
                patch.object(update.os, "fsync", side_effect=error),
                self.assertRaises(type(error)) as raised,
            ):
                update.atomic_json(self.path, {"new": True})
            self.assertIs(raised.exception, error)
            self.assert_old_only()

    def test_cleanup_failure_does_not_mask_original_error(self):
        error = OSError(errno.ENOSPC, "original failure")
        with (
            patch.object(update.os, "fsync", side_effect=error),
            patch.object(Path, "unlink", side_effect=OSError(errno.EACCES, "cleanup failed")),
            self.assertRaises(OSError) as raised,
        ):
            update.atomic_json(self.path, {"new": True})
        self.assertIs(raised.exception, error)
        self.assertEqual(self.path.read_bytes(), self.old)
        self.assertEqual(len(list(self.root.iterdir())), 2)

    def test_repeated_failures_then_recovery_have_no_file_or_descriptor_growth(self):
        before = len(os.listdir("/proc/self/fd"))
        with patch.object(update.os, "fsync", side_effect=OSError(errno.ENOSPC, "full")):
            for _ in range(100):
                with self.assertRaises(OSError):
                    firmware_control.atomic_json(self.path, {"payload": "x" * 16000})
        self.assert_old_only()
        self.assertEqual(len(os.listdir("/proc/self/fd")), before)
        firmware_control.atomic_json(self.path, {"recovered": True})
        self.assertEqual(json.loads(self.path.read_bytes()), {"recovered": True})

    def _stream_failure(self, method):
        factory = tempfile.NamedTemporaryFile
        error = OSError(errno.ENOSPC, "stream failure")

        class FailingStream:
            def __init__(self, *args, **kwargs):
                self.stream = factory(*args, **kwargs)
                self.name = self.stream.name

            def __enter__(self):
                self.stream.__enter__()
                return self

            def __exit__(self, *args):
                return self.stream.__exit__(*args)

            def write(self, value):
                if method == "write":
                    self.stream.write(value[:3])
                    raise error
                return self.stream.write(value)

            def flush(self):
                if method == "flush":
                    raise error
                return self.stream.flush()

            def fileno(self):
                return self.stream.fileno()

        before = len(os.listdir("/proc/self/fd"))
        with (
            patch.object(update.tempfile, "NamedTemporaryFile", FailingStream),
            self.assertRaises(OSError) as raised,
        ):
            update.atomic_json(self.path, {"new": True})
        self.assertIs(raised.exception, error)
        self.assert_old_only()
        self.assertEqual(len(os.listdir("/proc/self/fd")), before)

    def test_partial_write_failure_cleans_temporary(self):
        self._stream_failure("write")

    def test_flush_failure_cleans_temporary(self):
        self._stream_failure("flush")

    def test_network_bootstrap_failure_cleans_temp_and_retry_succeeds(self):
        state = self.root / "network"
        profiles = self.root / "profiles"
        profiles.mkdir()
        fields = {
            "GENERAL.CON-UUID": "ab012345-6789-4567-890a-bcdef0123456",
            "802-11-wireless.mode": "infrastructure",
            "802-11-wireless.ssid": "test-network",
            "802-11-wireless-security.key-mgmt": "",
            "IP4.GATEWAY": "192.0.2.1",
            "connection.stable-id": "",
        }
        mutations = []

        def nmcli(argv, **kwargs):
            if "-g" in argv:
                return subprocess.CompletedProcess(argv, 0, fields[argv[argv.index("-g") + 1]])
            mutations.append(argv)
            if "clone" in argv:
                path = profiles / (argv[-1] + ".nmconnection")
                path.write_text("test-only-profile")
                path.chmod(0o600)
            elif "delete" in argv:
                (profiles / (argv[-1] + ".nmconnection")).unlink()
            else:
                self.assertIn("modify", argv)
            return subprocess.CompletedProcess(argv, 0, "")

        with patch.object(update.subprocess, "run", side_effect=nmcli):
            with (
                patch.object(update.os, "fsync", side_effect=OSError("full")),
                self.assertRaises(OSError),
            ):
                update.prepare_first_install_network(state_dir=state, profile_dir=profiles)
            self.assertEqual(list(state.iterdir()), [])
            self.assertEqual(list(profiles.iterdir()), [])
            update.prepare_first_install_network(state_dir=state, profile_dir=profiles)
        self.assertEqual([p.name for p in state.iterdir()], ["lkg-wlan0.json"])
        self.assertEqual(
            json.loads((state / "lkg-wlan0.json").read_bytes())["config"]["ssid"], "test-network"
        )
        self.assertFalse(any("up" in argv for argv in mutations))

    def test_standalone_firmware_writer_uses_only_colocated_standard_library_updater(self):
        directory = self.root / "standalone"
        directory.mkdir()
        for module in (update, firmware_control):
            shutil.copy2(module.__file__, directory)
        code = """import sys,json
from pathlib import Path
from unittest.mock import patch
sys.path.insert(0,sys.argv[1])
import firmware_control
p=Path(sys.argv[1])/'state'/'task.json'
with patch('os.fsync',side_effect=OSError('full')):
 try:firmware_control.atomic_json(p,{'partial':True})
 except OSError:pass
assert not list(p.parent.iterdir())
firmware_control.atomic_json(p,{'recovered':True})
assert json.loads(p.read_bytes())=={'recovered':True}
assert 'rp_ylx' not in sys.modules
"""
        result = subprocess.run(
            [sys.executable, "-I", "-c", code, str(directory)],
            capture_output=True,
            text=True,
            timeout=10,
        )
        self.assertEqual(result.returncode, 0, result.stderr)

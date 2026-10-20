"""File validation must reject special files before a blocking open or read."""

import os
import subprocess
import sys
import unittest


class RegularArtifactReadTests(unittest.TestCase):
    def test_fifo_inputs_and_replacements_are_rejected_without_a_writer(self):
        script = r"""
import os, sys, tempfile
from pathlib import Path
from unittest.mock import patch
from rp_ylx.recording.live_seal import _Journal, _read_back
from rp_ylx.recording.storage import open_metadata
from rp_ylx.spectacular.adapter import _read_json_object

with tempfile.TemporaryDirectory() as temporary:
    path = Path(temporary) / 'artifact.json'
    path.write_bytes(b'{}')
    proof, before = _read_back(path), path.stat()
    path.unlink()
    os.mkfifo(path)
    stat_path = Path.stat
    def prior_stat(value, *args, **kwargs):
        return before if value == path else stat_path(value, *args, **kwargs)
    try:
        with patch.object(Path, 'stat', prior_stat):
            mode = sys.argv[1]
            if mode == 'readback':
                _read_back(path)
            elif mode == 'journal':
                _Journal(path)
            elif mode == 'metadata':
                with open_metadata(path) as stream:
                    stream.read(1)
            elif mode == 'proof':
                assert proof.unchanged(path) is False
            elif mode == 'calibration':
                _read_json_object(path, 'manifest')
            else:
                raise AssertionError(mode)
    except (OSError, ValueError):
        pass
    else:
        if mode != 'proof':
            raise AssertionError('special file admitted')
    assert not path.with_suffix('.json.zst.tmp').exists()
print('rejected')
"""
        for mode in ("readback", "journal", "metadata", "proof", "calibration"):
            with self.subTest(mode=mode):
                result = subprocess.run(
                    [sys.executable, "-c", script, mode],
                    capture_output=True,
                    text=True,
                    timeout=3,
                )
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual(result.stdout.strip(), "rejected")

    def test_session_python_fallback_rejects_fifo_without_writer(self):
        script = r"""
import os, tempfile
from pathlib import Path
from unittest.mock import patch
from rp_ylx.recording import device_session as module
with tempfile.TemporaryDirectory() as temp:
    root = Path(temp)
    os.mkfifo(root / "manifest.json")
    with patch.object(module, "_session_store_or_none", return_value=None):
        try:
            module.inspect_device_session_directory(root)
        except module.DeviceRecordingError as error:
            assert error.code == "manifest_invalid", error.code
        else:
            raise AssertionError("FIFO manifest admitted")
print("rejected")
"""
        result = subprocess.run(
            [sys.executable, "-c", script], capture_output=True, text=True, timeout=3
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.strip(), "rejected")

    @unittest.skipUnless(os.path.isdir("/proc/self/fd"), "Linux fd accounting required")
    def test_session_python_fallback_releases_file_on_stat_failure_or_cancellation(self):
        script = r"""
import os, tempfile
from pathlib import Path
from unittest.mock import patch
from rp_ylx.recording import device_session as module
with tempfile.TemporaryDirectory() as temp:
    root = Path(temp)
    (root / "nested").mkdir()
    (root / "nested" / "payload.bin").write_bytes(b"test")
    root_fd = os.open(root, os.O_RDONLY | os.O_DIRECTORY)
    try:
        for failure in (OSError("stat failed"), MemoryError("stat memory"),
                        KeyboardInterrupt("stat interrupt"), SystemExit("stat exit")):
            before = len(os.listdir("/proc/self/fd"))
            held = []
            with patch.object(module, "_native_open_regular_at", return_value=None), \
                 patch.object(module.os, "fstat", side_effect=failure):
                for _ in range(10):
                    try:
                        module._open_regular_at(
                            root_fd, "nested/payload.bin", code="artifact_invalid"
                        )
                    except BaseException as error:
                        held.append(error)
                        if isinstance(failure, OSError):
                            assert isinstance(error, module.DeviceRecordingError), type(error)
                        else:
                            assert error is failure
                    else:
                        raise AssertionError("stat failure ignored")
            assert len(held) == 10
            after = len(os.listdir("/proc/self/fd"))
            assert before == after, (type(failure).__name__, before, after)
        with patch.object(module, "_native_open_regular_at", return_value=None):
            descriptor = module._open_regular_at(
                root_fd, "nested/payload.bin", code="artifact_invalid"
            )
            try:
                assert os.read(descriptor, 4) == b"test"
            finally:
                os.close(descriptor)
    finally:
        os.close(root_fd)
print("released")
"""
        result = subprocess.run(
            [sys.executable, "-c", script], capture_output=True, text=True, timeout=5
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.strip(), "released")

    @unittest.skipUnless(os.path.isdir("/proc/self/fd"), "Linux fd accounting required")
    def test_rejected_journals_do_not_leak_descriptors_or_create_outputs(self):
        import tempfile
        from pathlib import Path

        from rp_ylx.recording.live_seal import _Journal

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            path = root / "linked.ndjson"
            original = root / "original.ndjson"
            original.write_bytes(b"{}\n")
            os.link(original, path)
            before = len(os.listdir("/proc/self/fd"))
            for _ in range(20):
                with self.assertRaises(ValueError):
                    _Journal(path)
            self.assertEqual(len(os.listdir("/proc/self/fd")), before)
            self.assertFalse(path.with_suffix(".ndjson.zst.tmp").exists())


if __name__ == "__main__":
    unittest.main()

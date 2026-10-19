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

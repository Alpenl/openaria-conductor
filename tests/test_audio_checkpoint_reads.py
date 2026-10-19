from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from rp_ylx.recording.live_seal import _read_audio_checkpoint
from rp_ylx.recording.verified import identity

RACE = r"""
import json, os, sys
from pathlib import Path
sys.path[:0] = json.loads(sys.argv[2])
from rp_ylx.recording.live_seal import LiveSealing
kind = sys.argv[1]
root = Path(sys.argv[3])
(root / 'audio').mkdir()
checkpoint = root / 'audio/checkpoint.json'
checkpoint.write_text('{"segments": []}')
other = root / 'replacement.json'
other.write_text('{"segments": []}')
sealing = object.__new__(LiveSealing)
sealing.root = root
sealing.prepared = {}
sealing._audio_checkpoint_identity = None
original = Path.stat
swapped = False
def race(path, *args, **kwargs):
    global swapped
    metadata = original(path, *args, **kwargs)
    if path == checkpoint and not swapped:
        swapped = True
        path.unlink()
        if kind == 'fifo': os.mkfifo(path)
        elif kind == 'symlink': path.symlink_to(other)
        elif kind == 'hardlink': os.link(other, path)
    return metadata
Path.stat = race
try:
    sealing._audio()
except (ValueError, OSError):
    assert sealing._audio_checkpoint_identity is None
    assert sealing.prepared == {}
    print('rejected')
else:
    raise AssertionError('replacement checkpoint was accepted')
"""


class AudioCheckpointReadsTest(unittest.TestCase):
    def check_replacement(self, kind: str) -> None:
        try:
            with tempfile.TemporaryDirectory(prefix="audio-checkpoint-race-") as directory:
                result = subprocess.run(
                    [sys.executable, "-c", RACE, kind, json.dumps(sys.path), directory],
                    capture_output=True,
                    text=True,
                    timeout=3,
                )
        except subprocess.TimeoutExpired:
            self.fail(f"{kind} checkpoint replacement blocked the reader")
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual(result.stdout.strip(), "rejected")

    def test_fifo_replacement_between_stat_and_open_never_blocks(self) -> None:
        self.check_replacement("fifo")

    def test_symlink_replacement_between_stat_and_open_is_rejected(self) -> None:
        self.check_replacement("symlink")

    def test_hardlink_replacement_between_stat_and_open_is_rejected(self) -> None:
        self.check_replacement("hardlink")


class CheckpointPublicationRaceTest(unittest.TestCase):
    def setUp(self) -> None:
        temporary = tempfile.TemporaryDirectory(prefix="audio-checkpoint-publish-")
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.path = self.root / "checkpoint.json"
        self.path.write_bytes(b'{"segments": []}')
        self.expected = identity(self.path.stat())
        self.opened = []

    def publish(self) -> None:
        temporary = self.root / "checkpoint.tmp"
        temporary.write_bytes(b'{"segments": [], "generation": 2}')
        temporary.replace(self.path)

    def read_then(self, action):
        original = os.fdopen
        opened = self.opened

        def wrapped(descriptor, mode):
            source = original(descriptor, mode)
            opened.append(descriptor)

            class Reader:
                def __enter__(self):
                    return self

                def __exit__(self, *args):
                    source.close()

                def fileno(self):
                    return source.fileno()

                def read(self, size):
                    data = source.read(size)
                    action()
                    return data

            return Reader()

        return patch("rp_ylx.recording.live_seal.os.fdopen", wrapped)

    def assert_closed(self) -> None:
        self.assertTrue(self.opened)
        for descriptor in self.opened:
            with self.assertRaises(OSError):
                os.fstat(descriptor)

    def test_opened_inode_unlinked_by_publication_is_retried(self) -> None:
        original = os.open

        def replaced_after_open(*args, **kwargs):
            descriptor = original(*args, **kwargs)
            self.opened.append(descriptor)
            self.publish()
            return descriptor

        with patch("rp_ylx.recording.live_seal.os.open", replaced_after_open):
            self.assertIsNone(_read_audio_checkpoint(self.path, self.expected, force=False))
        self.assert_closed()
        latest = _read_audio_checkpoint(self.path, identity(self.path.stat()), force=False)
        self.assertEqual(json.loads(latest)["generation"], 2)

    def test_publication_during_read_retries_then_reads_latest_generation(self) -> None:
        with self.read_then(self.publish):
            self.assertIsNone(_read_audio_checkpoint(self.path, self.expected, force=False))
        self.assert_closed()
        latest = _read_audio_checkpoint(self.path, identity(self.path.stat()), force=False)
        self.assertEqual(json.loads(latest)["generation"], 2)

    def test_forced_final_read_rejects_inode_unlinked_during_read(self) -> None:
        with self.read_then(self.publish), self.assertRaises(ValueError):
            _read_audio_checkpoint(self.path, self.expected, force=True)
        self.assert_closed()

    def test_inplace_growth_during_read_is_rejected(self) -> None:
        def grow():
            with self.path.open("ab") as stream:
                stream.write(b" ")

        with self.read_then(grow), self.assertRaisesRegex(ValueError, "changed during read"):
            _read_audio_checkpoint(self.path, self.expected, force=False)
        self.assert_closed()

    def test_disappearance_after_stat_retries_only_while_recording(self) -> None:
        self.path.unlink()
        self.assertIsNone(_read_audio_checkpoint(self.path, self.expected, force=False))
        with self.assertRaises(FileNotFoundError):
            _read_audio_checkpoint(self.path, self.expected, force=True)

    def test_regular_replacement_after_stat_is_rejected_by_final_read(self) -> None:
        self.publish()
        with self.assertRaisesRegex(ValueError, "changed before final read"):
            _read_audio_checkpoint(self.path, self.expected, force=True)
        latest = _read_audio_checkpoint(self.path, self.expected, force=False)
        self.assertEqual(json.loads(latest)["generation"], 2)

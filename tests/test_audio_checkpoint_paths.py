import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from tests import test_live_sealing as fixtures


class AudioCheckpointPathsTests(unittest.TestCase):
    def fixture(self, root, count):
        sealing = fixtures.LiveSealingTests().audio_sealing(root)
        segments = [{"path": f"audio/audio_{i:05d}.wav", "index": i} for i in range(count)]
        sealing.prepared = {s["path"]: object() for s in segments}
        (root / "audio").mkdir()
        checkpoint = root / "audio/checkpoint.json"
        checkpoint.write_text(json.dumps({"segments": segments}))
        return sealing, segments, checkpoint

    def test_prepared_canonical_history_does_not_reparse_paths(self):
        with tempfile.TemporaryDirectory() as directory:
            sealing, _, _ = self.fixture(Path(directory), 2520)
            with patch("rp_ylx.recording.live_seal.Path", wraps=Path) as paths:
                sealing._audio(force=True)
            self.assertEqual(paths.call_count, 0)

    def test_prepared_history_still_checks_modified_index(self):
        with tempfile.TemporaryDirectory() as directory:
            sealing, segments, checkpoint = self.fixture(Path(directory), 2)
            segments[0]["index"] = 1
            checkpoint.write_text(json.dumps({"segments": segments}))
            with self.assertRaisesRegex(ValueError, "checkpoint path"):
                sealing._audio(force=True)
            self.assertIsNone(sealing._audio_checkpoint_identity)

    def test_prepared_history_rejects_escape_and_absolute_paths(self):
        for path in ("../audio/audio_00000.wav", "/audio/audio_00000.wav"):
            with self.subTest(path=path), tempfile.TemporaryDirectory() as directory:
                sealing, segments, checkpoint = self.fixture(Path(directory), 1)
                segments[0]["path"] = path
                sealing.prepared[path] = object()
                checkpoint.write_text(json.dumps({"segments": segments}))
                with self.assertRaisesRegex(ValueError, "checkpoint path"):
                    sealing._audio(force=True)

    def test_normalized_legacy_path_remains_accepted(self):
        for path in ("audio//audio_00000.wav", "./audio/audio_00000.wav"):
            with self.subTest(path=path), tempfile.TemporaryDirectory() as directory:
                sealing, segments, checkpoint = self.fixture(Path(directory), 1)
                segments[0]["path"] = path
                sealing.prepared = {path: object()}
                checkpoint.write_text(json.dumps({"segments": segments}))
                sealing._audio(force=True)
                self.assertIsNotNone(sealing._audio_checkpoint_identity)

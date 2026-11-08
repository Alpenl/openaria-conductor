import array
import io
import os
import subprocess
import threading
import unittest
import wave
from pathlib import Path
from unittest.mock import patch

from rp_ylx import voice_feedback as voice


class VoiceFeedbackTest(unittest.TestCase):
    def test_disabled_does_not_start_worker(self):
        with (
            patch.dict(os.environ, {"OPENARIA_VOICE_ENABLED": "0"}),
            patch.object(voice, "VoiceFeedback") as make,
        ):
            voice.notify("started")
            make.assert_not_called()

    def test_worker_creation_failure_never_escapes(self):
        with (
            patch.dict(os.environ, {"OPENARIA_VOICE_ENABLED": "1"}),
            patch.object(voice, "_instance", None),
            patch.object(voice, "VoiceFeedback", side_effect=OSError("unavailable")),
        ):
            voice.notify("started")

    def test_latest_pending_state_replaces_stale_start(self):
        with patch.object(threading.Thread, "start"):
            player = voice.VoiceFeedback("test")
        player.notify("started", "a")
        player.notify("saved", "a")
        player.notify("saved", "a")
        self.assertEqual(player.pending.qsize(), 1)
        self.assertEqual(player.pending.get_nowait()[0], "saved")

    def test_playback_timeout_does_not_kill_worker(self):
        failed = threading.Event()
        played = threading.Event()

        def run(*args, **kwargs):
            self.assertEqual(kwargs["timeout"], 5)
            if not failed.is_set():
                failed.set()
                raise subprocess.TimeoutExpired("aplay", 5)
            played.set()

        with patch.object(voice.subprocess, "run", side_effect=run):
            player = voice.VoiceFeedback("test")
            player.notify("started", "a")
            self.assertTrue(failed.wait(2))
            player.notify("saved", "a")
            self.assertTrue(played.wait(2))


class VoiceVolumeTest(unittest.TestCase):
    def test_gain_scales_pcm_and_preserves_format(self):
        path = Path(voice.__file__).with_name("voice_assets") / "started.wav"

        def samples(volume):
            with wave.open(io.BytesIO(voice.scaled_wave(path, volume)), "rb") as f:
                return f.getparams(), array.array("h", f.readframes(f.getnframes()))

        params, full = samples(100)
        half_params, half = samples(50)
        _, silent = samples(0)
        self.assertEqual(params, half_params)
        self.assertEqual(list(half), [int(x * 0.5) for x in full])
        self.assertFalse(any(silent))
        with self.assertRaises(ValueError):
            voice.scaled_wave(path, True)

    def test_mute_clears_pending_cue_without_enqueuing_playback(self):
        with patch.object(threading.Thread, "start"):
            player = voice.VoiceFeedback("test")
        player.notify("started", "one", 100)
        player.notify("saved", "one", 0)
        self.assertTrue(player.pending.empty())

import os
import subprocess
import threading
import unittest
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

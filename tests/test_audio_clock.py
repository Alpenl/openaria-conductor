import copy
import unittest

from rp_ylx.audio_clock import audio_clock_report
from tests.testkit import audio_fixture


class AudioClockTests(unittest.TestCase):
    def test_sample_clock_and_thread_span_are_independent(self):
        report = audio_clock_report(audio_fixture(), 3)
        self.assertEqual(report["continuity"], "verified")
        self.assertEqual(report["actual_sample_rate"], 10000)
        self.assertEqual(report["pcm_duration_seconds"], 2)
        self.assertAlmostEqual(report["thread_span_seconds"], 2.01)

    def test_tampered_sync_is_rejected(self):
        audio = audio_fixture()
        audio["sync"]["end_time_seconds"] = 1.1
        with self.assertRaisesRegex(ValueError, "sample clock evidence"):
            audio_clock_report(audio, 3)

    def test_tampered_sample_count_is_rejected(self):
        audio = audio_fixture()
        audio["sample_count"] += 1000
        with self.assertRaises(ValueError):
            audio_clock_report(audio, 3)

    def test_discontinuities_and_forged_clock_proofs_are_rejected(self):
        mutations = [
            ("xrun_count", 1),
            ("suspend_count", 1),
            ("continuity", "unknown"),
            ("period_frames", 0),
            ("queue_peak_frames", 262145),
            ("sample_start_monotonic_ns", 1200000000),
            ("thread_stopped_monotonic_ns", 2100000000),
            ("max_residual_ns", 1_000_000),
        ]
        for field, value in mutations:
            with self.subTest(field=field):
                audio = audio_fixture()
                audio["capture_clock"][field] = value
                with self.assertRaises(ValueError):
                    audio_clock_report(audio, 3)
        audio = audio_fixture()
        audio["capture_clock"]["anchors"][1][1] += 50_000_000
        with self.assertRaises(ValueError):
            audio_clock_report(audio, 3)

    def test_legacy_thread_timestamps_never_prove_continuity(self):
        audio = audio_fixture()
        del audio["capture_clock"]
        audio["sync"]["end_time_seconds"] = 7.1
        original = copy.deepcopy(audio)
        report = audio_clock_report(audio, 8)
        self.assertEqual(report["continuity"], "unknown")
        self.assertAlmostEqual(report["duration_difference_seconds"], 5)
        self.assertEqual(audio, original)

    def test_no_audio_is_explicit(self):
        self.assertEqual(
            audio_clock_report({"state": "not_recorded"}, 3), {"continuity": "no-audio"}
        )

import copy
import math
import unittest

from rp_ylx.audio_clock import audio_clock_report, audio_sample_time_ns, raw_audio_clock_model
from tests.test_audio_clock import audio_fixture


def raw_fixture():
    audio = audio_fixture()
    rate, seconds = audio["sample_rate"], 600
    count = rate * seconds
    origin = 1_000_000_000
    anchors = []

    def mono(raw):
        elapsed = raw - 2_000_000_000
        # Ordinary -500 ppm slew for 300 s, not an audio interruption.
        correction = min(max(elapsed - 100_000_000_000, 0), 300_000_000_000) / 2000
        return round(1_100_000_000 + elapsed - correction)

    for second in range(seconds + 1):
        sample = second * rate + 1
        raw = 2_000_000_000 + sample * 1_000_000_000 // rate
        anchors.append([sample, raw, mono(raw + 20_000), raw + 10_000, raw + 30_000])
    clock = audio["capture_clock"]
    model = raw_audio_clock_model(
        anchors, rate, count, clock["period_frames"], clock["buffer_frames"]
    )
    clock.update(
        schema="openaria.audio-clock.v2",
        timestamp_source="alsa_htimestamp_dma_raw",
        anchors=anchors,
        thread_started_monotonic_ns=1_050_000_000,
        thread_stopped_monotonic_ns=math.ceil(model["end_ns"]) + 1_000_000,
        sample_start_monotonic_ns=round(model["start_ns"]),
        sample_end_monotonic_ns=round(model["end_ns"]),
        max_residual_ns=math.ceil(model["max_residual_ns"]),
        session_start_monotonic_ns=origin,
    )
    audio["sample_count"] = count
    audio["sync"]["start_time_seconds"] = (clock["sample_start_monotonic_ns"] - origin) / 1e9
    audio["sync"]["end_time_seconds"] = (clock["sample_end_monotonic_ns"] - origin) / 1e9
    segment = audio["segments"][0]
    segment.update(
        end_sample=count,
        end_time_seconds=count / rate,
        pcm_payload_bytes=count * audio["channels"] * 2,
    )
    segment["artifact"]["bytes"] = segment["pcm_payload_bytes"] + segment["wav_header_bytes"]
    audio["segments"] = [segment]
    return audio


class RawAudioClockTests(unittest.TestCase):
    def test_slew_preserves_piecewise_session_timeline_and_raw_continuity(self):
        audio = raw_fixture()
        report = audio_clock_report(audio, 601)
        self.assertEqual(report["continuity"], "verified")
        self.assertEqual(report["mapping"], "piecewise-linear")
        self.assertAlmostEqual(report["actual_sample_rate"], audio["sample_rate"])
        self.assertEqual(report["max_residual_seconds"], 0)
        points = [(n, round(t * 1e9)) for n, t in report["sample_timeline"]]
        # Halfway through the slew the timeline differs from a global fit.
        actual = audio_sample_time_ns(points, 250 * audio["sample_rate"])
        expected = (0.1 + 250 - 150 * 0.0005) * 1e9
        self.assertLess(abs(actual - expected), 100)
        affine = points[0][1] + 250 / 600 * (points[-1][1] - points[0][1])
        self.assertGreater(abs(actual - affine), 10_000_000)

    def test_slew_does_not_hide_real_audio_discontinuity(self):
        audio = raw_fixture()
        rows = audio["capture_clock"]["anchors"]
        for row in rows[len(rows) // 2 :]:
            # A real sample-to-clock gap affects RAW too.
            for column in (1, 2, 3, 4):
                row[column] += 60_000_000
        with self.assertRaisesRegex(ValueError, "raw audio capture continuity"):
            audio_clock_report(audio, 601)

    def test_uncertain_stale_wrong_domain_or_nonmonotonic_bridges_rejected(self):
        for column, delta in ((4, 2_000_000), (1, -30_000_000), (2, 100_000_000), (0, -20_000)):
            audio = raw_fixture()
            audio["capture_clock"]["anchors"][100][column] += delta
            with self.subTest(column=column), self.assertRaises(ValueError):
                audio_clock_report(audio, 601)

    def test_tampered_timeline_or_residual_rejected(self):
        for field in ("sample_start_monotonic_ns", "sample_end_monotonic_ns", "max_residual_ns"):
            audio = raw_fixture()
            audio["capture_clock"][field] += 1_000_000
            with self.subTest(field=field), self.assertRaises(ValueError):
                audio_clock_report(audio, 601)

    def test_missing_observations_and_wrong_timestamp_source_rejected(self):
        for mutation in ("missing", "source", "boolean", "wrong_width"):
            audio = raw_fixture()
            clock = audio["capture_clock"]
            if mutation == "missing":
                del clock["anchors"][100:110]
            elif mutation == "source":
                clock["timestamp_source"] = "alsa_htimestamp_dma"
            elif mutation == "boolean":
                clock["anchors"][100][3] = True
            else:
                clock["anchors"][100].pop()
            with self.subTest(mutation=mutation), self.assertRaises(ValueError):
                audio_clock_report(audio, 601)

    def test_legacy_v1_report_is_unchanged(self):
        audio = audio_fixture()
        before = copy.deepcopy(audio)
        report = audio_clock_report(audio, 3)
        self.assertEqual(audio, before)
        self.assertNotIn("mapping", report)
        self.assertNotIn("sample_timeline", report)


if __name__ == "__main__":
    unittest.main()

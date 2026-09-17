import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from rp_ylx.camera.exposure import configure_exposure
from rp_ylx.recording.audit import capture_audit


class CaptureAuditTests(unittest.TestCase):
    def test_wrap_matching_clock_gap_and_unavailable_adc_are_explicit(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            frames, imu = [], []
            for index, (counter, host) in enumerate(
                ((2**24 - 1, 1_000_000_000), (0, 1_040_000_000))
            ):
                frames.append(
                    {
                        "host_monotonic_ns": host,
                        "source_sequence": index * 2,
                        "timestamp_audit": {
                            "camera_counter_raw": counter,
                            "timestamp_clock": "v4l2_monotonic",
                            "host_dequeue_monotonic_ns": host + 100,
                        },
                    }
                )
                for slot in range(2):
                    imu.append(
                        {
                            "host_monotonic_ns": host - 10_000_000,
                            "host_read_start_ns": host - 12_000_000,
                            "host_read_end_ns": host - 8_000_000,
                            "packet_sequence": index,
                            "sample_index": slot,
                            "device_timestamp_raw": counter,
                        }
                    )
            for name, rows in (("frames", frames), ("imu", imu)):
                (root / f"{name}.ndjson").write_text("".join(json.dumps(r) + "\n" for r in rows))
            result = capture_audit(root, 30, 2)
            self.assertEqual(result["camera"]["counter_matched_frames"], 2)
            self.assertEqual(result["camera"]["counter_wraps"], 1)
            self.assertEqual(result["camera"]["actual_fps"], 25)
            self.assertEqual(result["camera"]["source_missing_frames"], 0)
            self.assertFalse(result["imu"]["independent_adc_timestamps"])
            self.assertEqual(result["alignment"]["applied_offset_ns"], 0)

    def test_manual_exposure_is_checked_and_null_preserves_controls(self):
        with patch("rp_ylx.camera.exposure.os.open") as opened:
            self.assertIsNone(configure_exposure("/dev/video0", None))
            for invalid in (True, 0, 101, "100"):
                with self.assertRaises(ValueError):
                    configure_exposure("/dev/video0", invalid)
            opened.assert_not_called()
        with (
            patch("rp_ylx.camera.exposure.os.open", return_value=9),
            patch("rp_ylx.camera.exposure.os.close") as closed,
            patch("rp_ylx.camera.exposure.fcntl.ioctl"),
            self.assertRaisesRegex(RuntimeError, "exposure_readback_failed"),
        ):
            configure_exposure("/dev/video0", 100)
        closed.assert_called_once_with(9)

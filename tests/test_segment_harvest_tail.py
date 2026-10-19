import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from rp_ylx.recording import device_session as d


class _PreviouslySealed(dict):
    def get(self, *args, **kwargs):
        raise AssertionError("already sealed records must not be reconverted every poll")


class SegmentHarvestTailTests(unittest.TestCase):
    def recorder(self, root, segments, indexed):
        recorder = object.__new__(d.DeviceSessionRecorder)
        recorder._native_transaction = SimpleNamespace(
            segments=lambda: segments,
            snapshot=lambda: {"sink": {"frames_indexed": indexed[0]}},
        )
        recorder._harvested_segments = 1000
        recorder._segment_records = [{"index": i} for i in range(1000)]
        recorder._segment_record = lambda segment: {"index": segment.index}
        recorder._capture_diagnostics = lambda: {}
        recorder._partial = root
        recorder._files = {}
        recorder._plan = SimpleNamespace(session_id="test-harvest-tail")
        return recorder

    def segment(self, index):
        return {
            "index": index,
            "start_frame": index * 3,
            "end_frame": (index + 1) * 3,
            "left_path": f"video/left_{index:05d}.mp4",
            "right_path": f"video/right_{index:05d}.mp4",
            "left_bytes": 1,
            "right_bytes": 1,
        }

    def test_idle_poll_does_not_revisit_sealed_record_fields(self):
        recorder = self.recorder(Path("/not-used"), [_PreviouslySealed()] * 1000, [3000])
        with patch.object(d, "write_json_atomic") as checkpoint:
            recorder._harvest_segments()
        checkpoint.assert_not_called()
        self.assertEqual(recorder._harvested_segments, 1000)

    def test_new_tail_still_waits_for_index_and_publishes_contiguous_checkpoint(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            for name in ("frames.ndjson", "imu.ndjson"):
                (root / name).write_bytes(b"")
            segments = [_PreviouslySealed()] * 1000 + [self.segment(1000), self.segment(1001)]
            indexed = [3003]
            recorder = self.recorder(root, segments, indexed)
            with (
                patch.object(d, "write_json_atomic") as checkpoint,
                patch.object(d.os, "fsync") as sync,
            ):
                recorder._harvest_segments()
                self.assertEqual(recorder._harvested_segments, 1001)
                self.assertEqual(checkpoint.call_count, 1)
                self.assertEqual(sync.call_count, 2)
                self.assertEqual(len(checkpoint.call_args.args[1]["segments"]), 1001)
                segments[1000] = _PreviouslySealed()
                indexed[0] = 3006
                recorder._harvest_segments()
                self.assertEqual(recorder._harvested_segments, 1002)
                self.assertEqual(checkpoint.call_count, 2)
                self.assertEqual(sync.call_count, 4)
                self.assertEqual(recorder._segment_records[-2:], [{"index": 1000}, {"index": 1001}])

    def test_regressed_native_inventory_fails_without_publication(self):
        recorder = self.recorder(Path("/not-used"), [self.segment(i) for i in range(999)], [3000])
        with (
            patch.object(d, "write_json_atomic") as checkpoint,
            self.assertRaises(d.DeviceRecordingError) as error,
        ):
            recorder._harvest_segments()
        self.assertEqual(error.exception.code, "segment_invalid")
        checkpoint.assert_not_called()
        self.assertEqual(recorder._harvested_segments, 1000)

    def test_new_invalid_path_is_not_skipped(self):
        invalid = self.segment(1000)
        invalid["left_path"] = "video/../outside.mp4"
        recorder = self.recorder(
            Path("/not-used"), [self.segment(i) for i in range(1000)] + [invalid], [3003]
        )
        with (
            patch.object(d, "write_json_atomic") as checkpoint,
            self.assertRaises(d.DeviceRecordingError),
        ):
            recorder._harvest_segments()
        checkpoint.assert_not_called()
        self.assertEqual(recorder._harvested_segments, 1000)


if __name__ == "__main__":
    unittest.main()

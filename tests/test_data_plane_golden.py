from __future__ import annotations

import base64
import hashlib
import json
import struct
import unittest
from pathlib import Path
from unittest.mock import patch

from rp_ylx.camera import CameraError, CameraMode, V4L2CameraStream, split_sbs_mjpeg
from rp_ylx.camera.v4l2 import _jpeg_dimensions, _jpeg_markers
from rp_ylx.contracts.frame_stream import MAGIC, encode_frame
from tests.test_camera import _FakeV4L2Device

CORPUS = json.loads((Path(__file__).parents[1] / "contracts/golden/data-plane-v0.json").read_text())


class DataPlaneGoldenTest(unittest.TestCase):
    def test_jpeg_markers_dimensions_and_eye_direction(self) -> None:
        left = base64.b64decode(CORPUS["jpeg"]["left_base64"])
        right = base64.b64decode(CORPUS["jpeg"]["right_base64"])
        sbs = left + right

        self.assertEqual(_jpeg_markers(sbs), [(0, len(left)), (len(left), len(sbs))])
        self.assertEqual(_jpeg_dimensions(left), tuple(CORPUS["jpeg"]["dimensions"]))
        self.assertEqual(_jpeg_dimensions(right), tuple(CORPUS["jpeg"]["dimensions"]))
        self.assertEqual(split_sbs_mjpeg(sbs, 4, 2), (left, right))

    def test_frame_stream_encoding_is_exact(self) -> None:
        left = base64.b64decode(CORPUS["jpeg"]["left_base64"])
        encoded = MAGIC + encode_frame(left)
        self.assertEqual(len(encoded), CORPUS["frame_stream"]["encoded_left_bytes"])
        self.assertEqual(
            hashlib.sha256(encoded).hexdigest(),
            CORPUS["frame_stream"]["encoded_left_sha256"],
        )
        self.assertEqual(encoded[:8], MAGIC)
        self.assertEqual(struct.unpack(">I", encoded[8:12])[0], len(left))

    def test_sequence_timestamp_and_lifecycle_cases_are_frozen(self) -> None:
        self.assertEqual(
            CORPUS["sequence"]["wrap_output"],
            [4294967294, 4294967295, 4294967296, 4294967297],
        )
        self.assertEqual(CORPUS["sequence"]["regression_error"], "sequence_regression")
        self.assertEqual(CORPUS["timestamp"]["regression_error"], "timestamp_regression")
        self.assertTrue(CORPUS["lifecycle"]["close_is_idempotent"])
        self.assertTrue(CORPUS["lifecycle"]["stop_after_close_is_noop"])

    def test_v4l2_sequence_wrap_matches_corpus(self) -> None:
        stream = object.__new__(V4L2CameraStream)
        stream._last_sequence = None
        stream._sequence_epoch = 0
        actual = [stream._source_sequence(value) for value in CORPUS["sequence"]["wrap_input"]]
        self.assertEqual(actual, CORPUS["sequence"]["wrap_output"])

    def test_v4l2_regressions_and_repeated_close_match_corpus(self) -> None:
        stream = object.__new__(V4L2CameraStream)
        stream._last_sequence = None
        stream._sequence_epoch = 0
        stream._source_sequence(CORPUS["sequence"]["regression_input"][0])
        with self.assertRaises(CameraError) as sequence:
            stream._source_sequence(CORPUS["sequence"]["regression_input"][1])
        self.assertEqual(sequence.exception.code, CORPUS["sequence"]["regression_error"])

        left = base64.b64decode(CORPUS["jpeg"]["left_base64"])
        right = base64.b64decode(CORPUS["jpeg"]["right_base64"])
        fake = _FakeV4L2Device(
            [left + right, left + right],
            timestamps=tuple(
                value // 1_000_000_000 for value in CORPUS["timestamp"]["regression_input_ns"]
            ),
        )
        stream = V4L2CameraStream(
            "/dev/video-test",
            CameraMode(3840, 1080, 30.0, "mjpg"),
            ioctl=fake.ioctl,
            mmap_factory=fake.mmap,
            open_file=lambda path, flags: 7,
            close_file=fake.closed_fds.append,
            wait_readable=lambda fd, timeout: True,
        )
        stream.start()
        stream.read(1.0)
        with self.assertRaises(CameraError) as timestamp:
            stream.read(1.0)
        self.assertEqual(timestamp.exception.code, CORPUS["timestamp"]["regression_error"])
        with patch.object(fake.buffers[0], "close", wraps=fake.buffers[0].close) as close:
            stream.close()
            stream.close()
        close.assert_not_called()
        stream.stop()


if __name__ == "__main__":
    unittest.main()

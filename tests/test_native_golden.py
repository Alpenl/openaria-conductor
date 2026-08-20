from __future__ import annotations

import base64
import hashlib
import importlib
import json
import unittest
from pathlib import Path

from rp_ylx.camera.v4l2 import _jpeg_dimensions, _jpeg_markers
from rp_ylx.contracts.frame_stream import encode_frame

CORPUS = json.loads((Path(__file__).parents[1] / "contracts/golden/data-plane-v0.json").read_text())


class NativeGoldenTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        try:
            cls.native = importlib.import_module("rp_ylx._native")
        except ModuleNotFoundError as error:
            raise unittest.SkipTest("native wheel is not installed") from error

    def test_jpeg_metadata_matches_python_and_golden_corpus(self) -> None:
        left = base64.b64decode(CORPUS["jpeg"]["left_base64"])
        right = base64.b64decode(CORPUS["jpeg"]["right_base64"])
        payload = b"padding" + left + right + b"\0"

        native = self.native.jpeg_metadata(payload)
        self.assertEqual(native["ranges"], _jpeg_markers(payload))
        self.assertEqual(native["dimensions"], _jpeg_dimensions(payload))
        self.assertEqual(
            self.native.jpeg_metadata(left)["dimensions"],
            tuple(CORPUS["jpeg"]["dimensions"]),
        )

    def test_frame_stream_encoding_matches_python_and_golden_digest(self) -> None:
        left = base64.b64decode(CORPUS["jpeg"]["left_base64"])
        native = self.native.encode_frame(left)
        self.assertEqual(native, encode_frame(left))
        self.assertEqual(
            hashlib.sha256(b"YLXFRM0\n" + native).hexdigest(),
            CORPUS["frame_stream"]["encoded_left_sha256"],
        )
        for payload in (b"", b"x" * (64 * 1024 * 1024 + 1)):
            with self.assertRaisesRegex(ValueError, "invalid_frame_length"):
                self.native.encode_frame(payload)


if __name__ == "__main__":
    unittest.main()

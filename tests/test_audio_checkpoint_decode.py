import importlib
import json
import unittest
from types import SimpleNamespace
from unittest.mock import patch


class AudioCheckpointDecodeTests(unittest.TestCase):
    def native_parse(self, payload):
        module = importlib.import_module("rp_ylx._native")
        return module.audio_checkpoint_segments(payload)

    def test_native_selects_segments_preserving_order_and_u64_indices(self):
        segments = [
            {"index": 2**64 - 1, "path": "audio/audio_18446744073709551615.wav"},
            {"index": 0, "path": "audio/audio_00000.wav"},
        ]
        payload = json.dumps(
            {"anchors": [[i, 1000000 + i] for i in range(10000)], "segments": segments}
        ).encode()
        self.assertEqual(self.native_parse(payload), segments)

    def test_native_rejects_malformed_ignored_content_and_trailing_bytes(self):
        for payload in (
            b'{"anchors":[[1,]],"segments":[]}',
            b'{"segments":[],"anchors":{"a":}}',
            b'{"segments":[]}garbage',
            b'{"segments":[],"ignored":"\xff"}',
            b'{"segments":[],"ignored":"\\q"}',
        ):
            with self.subTest(payload=payload), self.assertRaises(ValueError):
                self.native_parse(payload)

    def test_native_rejects_invalid_segment_fields(self):
        for value in (
            {},
            {"segments": None},
            {"segments": [{"index": True, "path": "audio/x.wav"}]},
            {"segments": [{"index": -1, "path": "audio/x.wav"}]},
            {"segments": [{"index": 2**64, "path": "audio/x.wav"}]},
            {"segments": [{"index": 1.0, "path": "audio/x.wav"}]},
            {"segments": [{"index": 0, "path": 5}]},
        ):
            with self.subTest(value=value), self.assertRaises(ValueError):
                self.native_parse(json.dumps(value).encode())

    def test_native_rejects_duplicate_segment_keys(self):
        for payload in (
            b'{"segments":[],"segments":[]}',
            b'{"segments":[{"index":0,"index":1,"path":"audio/x.wav"}]}',
        ):
            with self.subTest(payload=payload), self.assertRaises(ValueError):
                self.native_parse(payload)

    def test_wrapper_uses_native_without_python_anchor_allocation(self):
        from rp_ylx.native import audio_checkpoint_segments

        with patch("json.loads", side_effect=AssertionError("whole JSON decoded")):
            self.assertEqual(audio_checkpoint_segments(b'{"anchors":[[1,2]],"segments":[]}'), [])

    def test_wrapper_keeps_older_native_and_source_checkout_fallback(self):
        from rp_ylx.native import audio_checkpoint_segments

        payload = b'{"segments":[{"index":0,"path":"audio/audio_00000.wav"}]}'
        expected = json.loads(payload)["segments"]
        with patch("rp_ylx.native.importlib.import_module", return_value=SimpleNamespace()):
            self.assertEqual(audio_checkpoint_segments(payload), expected)
        with patch("rp_ylx.native.importlib.import_module", side_effect=ImportError("absent")):
            self.assertEqual(audio_checkpoint_segments(payload), expected)

    def test_wrapper_never_hides_native_parse_errors_with_fallback(self):
        from rp_ylx.native import audio_checkpoint_segments

        with (
            patch("json.loads", side_effect=AssertionError("unexpected fallback")),
            self.assertRaises(ValueError),
        ):
            audio_checkpoint_segments(b'{"anchors":[[1,]],"segments":[]}')


if __name__ == "__main__":
    unittest.main()

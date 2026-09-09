import unittest

from rp_ylx.recording.encoding import RecordingEncoding


class RecordingEncodingTest(unittest.TestCase):
    def test_high_quality_has_higher_budget_and_bounded_quantization(self):
        value = RecordingEncoding.from_mapping({"preset": "high"})
        self.assertEqual(value.bitrate_kbps, 16384)
        self.assertEqual((value.min_qp, value.max_qp), (18, 32))
        self.assertEqual(value.manifest()["b_frames"], 0)
        self.assertEqual(RecordingEncoding.from_mapping({}), value)
        self.assertEqual(RecordingEncoding.from_mapping({"codec": "hevc"}).bitrate_kbps, 16384)

    def test_rejects_ignored_typos_and_invalid_hardware_ranges(self):
        for settings in (
            {"bitrte": 1000},
            {"bitrate_kbps": True},
            {"max_qp": 52},
            {"min_qp": 30, "max_qp": 20},
            {"gop_frames": 0},
            {"codec": "h265"},
            {"rate_control": "crf"},
            {"vbv_ms": 3001},
        ):
            with self.subTest(settings=settings), self.assertRaises(ValueError):
                RecordingEncoding.from_mapping(settings)

    def test_manifest_does_not_claim_unused_vbr_or_fixed_qp_bitrate(self):
        for mode in ("vbr", "fixqp"):
            value = RecordingEncoding(rate_control=mode).manifest()
            self.assertIsNone(value["bitrate_kbps"])
            self.assertIsNone(value["max_qp"])
            self.assertIsNone(value["vbv_ms"])
        self.assertIsNone(RecordingEncoding(rate_control="vbr").manifest()["initial_qp"])

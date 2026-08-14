from __future__ import annotations

import io
import json
import unittest
from pathlib import Path

from rp_ylx.contracts.frame_stream import FrameStreamError, iter_frames, write_frame, write_header
from rp_ylx.contracts.session import SessionValidationError, validate_session

ROOT = Path(__file__).resolve().parents[1]
SESSION_ID = "0198c9a8-7a3c-7000-8000-000000000001"
EXAMPLES = ROOT / "contracts" / "examples"


class FrameStreamTest(unittest.TestCase):
    def test_round_trip(self) -> None:
        stream = io.BytesIO()
        write_header(stream)
        write_frame(stream, b"left")
        write_frame(stream, b"right")
        stream.seek(0)
        self.assertEqual(list(iter_frames(stream)), [b"left", b"right"])

    def test_truncated_frame_is_rejected(self) -> None:
        stream = io.BytesIO(b"YLXFRM0\n\x00\x00\x00\x05abc")
        with self.assertRaises(FrameStreamError):
            list(iter_frames(stream))


class SessionContractTest(unittest.TestCase):
    def test_schema_declares_v0(self) -> None:
        schema_path = ROOT / "src" / "rp_ylx" / "contracts" / "recording-session-v0.schema.json"
        schema = json.loads(schema_path.read_text(encoding="utf-8"))
        self.assertEqual(schema["properties"]["format"]["const"], "ylx.recording-session.v0")
        self.assertFalse(schema["additionalProperties"])
        imu_schema_path = ROOT / "src" / "rp_ylx" / "contracts" / "imu-sample-v0.schema.json"
        imu_schema = json.loads(imu_schema_path.read_text(encoding="utf-8"))
        self.assertEqual(imu_schema["properties"]["format"]["const"], "ylx.imu.v0")
        self.assertIn("packet_sequence", imu_schema["required"])
        self.assertIn("device_timestamp_raw", imu_schema["required"])

    def test_valid_fixture(self) -> None:
        manifest = validate_session(EXAMPLES / "valid" / SESSION_ID)
        self.assertEqual(manifest["counts"]["frames"], 2)

    def test_invalid_fixtures(self) -> None:
        cases = {
            "digest-mismatch": "digest_mismatch",
            "path-traversal": "unsafe_path",
            "duplicate-role": "duplicate_role",
            "future-version": "unsupported_version",
            "count-mismatch": "count_mismatch",
            "missing-file": "missing_file",
        }
        for fixture, expected_code in cases.items():
            with self.subTest(fixture=fixture):
                with self.assertRaises(SessionValidationError) as raised:
                    validate_session(EXAMPLES / "invalid" / fixture / SESSION_ID)
                self.assertEqual(raised.exception.code, expected_code)

    def test_interrupted_fixture_is_not_complete(self) -> None:
        path = EXAMPLES / "invalid" / "interrupted" / f"{SESSION_ID}.partial"
        with self.assertRaises(SessionValidationError) as raised:
            validate_session(path)
        self.assertEqual(raised.exception.code, "not_sealed")


if __name__ == "__main__":
    unittest.main()

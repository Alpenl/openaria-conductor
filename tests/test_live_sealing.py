import hashlib
import json
import os
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from rp_ylx.recording.audit import capture_audit
from rp_ylx.recording.device_session import DeviceRecordingError, validate_device_session_directory
from rp_ylx.recording.live_seal import LiveSealing, _Journal
from rp_ylx.recording.storage import BLOCK_BYTES, open_metadata
from tests import test_split_eye_recording as fixtures


class LiveSealingTests(unittest.TestCase):
    def test_incremental_blocks_tail_and_final_native_digest_agree(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            path = root / "frames.ndjson"
            path.touch()
            journal = _Journal(path)
            payload = b"a" * (BLOCK_BYTES + 127)
            with path.open("ab") as producer:
                producer.write(payload)
                producer.flush()
                journal.pump()
                self.assertGreater(journal.temporary.stat().st_size, 0)
                self.assertEqual(journal.size, BLOCK_BYTES)
                producer.write(b"tail")
            prepared = journal.finish()
            descriptor = {
                "path": path.name,
                "bytes": len(payload) + 4,
                "sha256": hashlib.sha256(payload + b"tail").hexdigest(),
            }
            prepared.use(root, descriptor)
            with open_metadata(prepared.target) as decoded:
                self.assertEqual(decoded.read(), payload + b"tail")
            self.assertTrue(path.exists())
            with self.assertRaisesRegex(ValueError, "source changed"):
                prepared.use(root, {**descriptor, "sha256": "0" * 64})
            prepared.target.write_bytes(b"bad")
            with self.assertRaisesRegex(ValueError, "output changed"):
                prepared.use(root, descriptor)

    def test_live_audit_waits_for_partial_lines_and_matches_offline(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for name in ("frames", "imu"):
                (root / f"{name}.ndjson").touch()
            sealing = LiveSealing(root, 30, 2)
            frames = root / "frames.ndjson"
            imu = root / "imu.ndjson"
            for index in range(10):
                host = 1_000_000_000 + index * 33_333_333
                row = (
                    json.dumps({"host_monotonic_ns": host, "source_sequence": index * 2}).encode()
                    + b"\n"
                )
                with frames.open("ab") as stream:
                    stream.write(row[:15])
                    stream.flush()
                    time.sleep(0.003)
                    stream.write(row[15:])
                with imu.open("ab") as stream:
                    stream.write(
                        json.dumps(
                            {
                                "host_monotonic_ns": host,
                                "host_read_start_ns": host - 1,
                                "host_read_end_ns": host + 1,
                                "packet_sequence": index,
                                "device_timestamp_raw": index,
                            }
                        ).encode()
                        + b"\n"
                    )
            result = sealing.finish()
            self.assertEqual(result, capture_audit(root, 30, 2))
            self.assertEqual(set(sealing.prepared), {"frames.ndjson", "imu.ndjson"})

    def test_cancel_without_imu_joins_and_keeps_recovery_journals(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "frames.ndjson").write_text('{"host_monotonic_ns":1,"source_sequence":0}\n')
            (root / "imu.ndjson").touch()
            sealing = LiveSealing(root, 30, 2)
            thread = threading.Thread(target=sealing.close)
            thread.start()
            thread.join(timeout=2)
            self.assertFalse(thread.is_alive())
            self.assertTrue((root / "frames.ndjson").exists())
            self.assertTrue((root / "imu.ndjson").exists())

    def test_verified_files_avoid_second_read_but_detect_restored_mtime_tamper(self):
        with tempfile.TemporaryDirectory() as directory:
            fixture = fixtures.SplitEyeRecordingTest()
            recorder, _, _ = fixture.build(Path(directory))
            recorder.start()
            fixture.feed(recorder, 3)
            sealed = recorder.stop()
            video = next(
                relative for relative in sealed.verified_artifacts if relative.endswith(".mp4")
            )
            from rp_ylx.recording import device_session

            original = device_session._verify_artifact_fd
            with patch.object(device_session, "_verify_artifact_fd", wraps=original) as read:
                validate_device_session_directory(
                    sealed.path, verified_artifacts=sealed.verified_artifacts
                )
                cached_reads = read.call_count
            with patch.object(device_session, "_verify_artifact_fd", wraps=original) as read:
                validate_device_session_directory(sealed.path)
                self.assertGreater(read.call_count, cached_reads)
            path = sealed.path / video
            metadata = path.stat()
            payload = bytearray(path.read_bytes())
            payload[-1] ^= 1
            path.write_bytes(payload)
            os.utime(path, ns=(metadata.st_atime_ns, metadata.st_mtime_ns))
            with self.assertRaises(DeviceRecordingError):
                validate_device_session_directory(
                    sealed.path, verified_artifacts=sealed.verified_artifacts
                )

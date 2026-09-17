import hashlib
import shutil
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch

import zstandard

from rp_ylx.api.downloads import DirectorySessionStore
from rp_ylx.recording.device_session import validate_device_session_directory
from rp_ylx.recording.encoding import RecordingEncoding
from rp_ylx.recording.recovery import recover_device_session
from rp_ylx.recording.storage import BLOCK_BYTES, compact_metadata, open_metadata
from tests import test_recording_recovery as recovery_fixtures
from tests import test_split_eye_recording as fixtures


class LosslessStorageTests(unittest.TestCase):
    def test_multiframe_zstd_round_trip_checksum_and_empty_stream(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "imu.ndjson"
            for original in (b"", b'{"imu":[1,2,3],"time":123456789}\n' * 80_000):
                path.write_bytes(original)
                target, encoding = compact_metadata(path)
                self.assertEqual(
                    encoding["uncompressed_sha256"], hashlib.sha256(original).hexdigest()
                )
                self.assertEqual(path.read_bytes(), original)
                with open_metadata(target) as decoded:
                    self.assertEqual(decoded.read(), original)
                with open_metadata(target) as decoded:
                    self.assertEqual(b"".join(decoded), original)
                corrupted = bytearray(target.read_bytes())
                corrupted[-1] ^= 1
                target.write_bytes(corrupted)
                with self.assertRaises(zstandard.ZstdError), open_metadata(target) as decoded:
                    decoded.read()
                target.unlink()

    @unittest.skipUnless(shutil.which("ffmpeg"), "FLAC integration requires ffmpeg")
    def test_sealed_v4_downloads_compressed_bytes_and_preserves_pcm_clock(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            fixture = fixtures.SplitEyeRecordingTest()
            recorder, _, _ = fixture.build(
                root,
                audio_enabled=True,
                recording_encoding=RecordingEncoding.from_mapping({"gop_frames": 3}),
            )
            recorder._config = replace(recorder._config, lossless_storage=True)
            recorder.start()
            fixture.feed(recorder, 3)
            sealed = recorder.stop()
            manifest = validate_device_session_directory(sealed.path)
            from rp_ylx.api.downloads import iter_device_session_v1_artifacts

            self.assertEqual(
                set(recorder._artifact_identities),
                {artifact["path"] for artifact in iter_device_session_v1_artifacts(manifest)},
            )
            self.assertEqual(manifest["schema"], "ylx.device-session.v4")
            self.assertEqual(manifest["audio"]["sample_count"], 4800)
            self.assertEqual(
                manifest["audio"]["segments"][0]["artifact"]["media_type"], "audio/flac"
            )
            self.assertFalse((sealed.path / "imu.ndjson").exists())
            self.assertFalse((sealed.path / "audio/audio_00000.wav").exists())
            frame = manifest["frames"]["artifact"]
            with open_metadata(sealed.path / frame["path"]) as decoded:
                self.assertEqual(len(decoded.readlines()), 3)
            store = DirectorySessionStore(
                root, verified_manifests={manifest["session_id"]: sealed.manifest_sha256}
            )
            with store.open_verified_artifact(
                manifest["session_id"], frame["artifact_id"], "v4"
            ) as artifact:
                self.assertEqual(artifact.read(), (sealed.path / frame["path"]).read_bytes())

    def test_crash_during_compaction_keeps_raw_prefix_recoverable(self):
        with tempfile.TemporaryDirectory() as directory:
            recorder = recovery_fixtures.RecordingRecoveryTests().recording(Path(directory))
            original = (recorder.partial_path / "frames.ndjson").read_bytes()
            compact_metadata(recorder.partial_path / "frames.ndjson")
            (recorder.partial_path / "imu.ndjson.zst.tmp").write_bytes(b"interrupted")
            recovered = recover_device_session(recorder.partial_path)
            self.assertEqual(recovered.manifest["frames"]["count"], 30)
            self.assertEqual((recovered.path / "frames.ndjson").read_bytes(), original)
            validate_device_session_directory(recovered.path)

    def test_write_failure_keeps_original_metadata(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "imu.ndjson"
            original = b"evidence\n" * (BLOCK_BYTES // 4)
            path.write_bytes(original)
            with (
                patch("rp_ylx.recording.storage.os.fsync", side_effect=OSError("disk full")),
                self.assertRaises(OSError),
            ):
                compact_metadata(path)
            self.assertEqual(path.read_bytes(), original)
            self.assertFalse(path.with_suffix(".ndjson.zst").exists())

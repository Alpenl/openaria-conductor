"""Integrity must not depend on sub-jiffy ctime changes being observable."""

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from rp_ylx.recording import device_session, recovery, verified
from rp_ylx.recording.live_seal import PreparedArtifact, _read_back
from tests import test_recording_recovery as recovery_fixtures
from tests import test_split_eye_recording as fixtures


class ArtifactContentChecks(unittest.TestCase):
    def test_unchanged_checks_bytes_even_when_all_identity_fields_collide(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "artifact"
            path.write_bytes(b"original")
            proof = _read_back(path)
            path.write_bytes(b"modified")
            with patch.object(verified, "identity", return_value=proof.identity):
                self.assertFalse(proof.unchanged(path))

    def test_prepared_source_rejects_same_identity_corruption(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source, target = root / "source", root / "target"
            source.write_bytes(b"original")
            target.write_bytes(b"encoded")
            original = _read_back(source)
            artifact = PreparedArtifact(original, target, _read_back(target), {})
            source.write_bytes(b"modified")
            identity = verified.identity

            def collide(metadata):
                if metadata.st_ino == original.identity[1]:
                    return original.identity
                return identity(metadata)

            with (
                patch.object(verified, "identity", side_effect=collide),
                self.assertRaisesRegex(ValueError, "source changed"),
            ):
                artifact.use(root, {"path": source.name, "bytes": 8, "sha256": original.sha256})

    def test_seal_does_not_return_old_digest_from_colliding_stat(self):
        with tempfile.TemporaryDirectory() as temporary:
            recorder, _, _ = fixtures.SplitEyeRecordingTest().build(Path(temporary))
            recorder.start()
            try:
                path = recorder.partial_path / "fixture.bin"
                path.write_bytes(b"original")
                proof = _read_back(path)
                recorder._verified_artifacts[path.name] = proof
                path.write_bytes(b"modified")
                with patch.object(device_session, "verified_identity", return_value=proof.identity):
                    actual = recorder._finalize_for_seal(path, 8, code="artifact_invalid")
                self.assertNotEqual(actual.sha256, proof.sha256)
            finally:
                recorder.abort()

    def test_publication_checks_bytes_despite_matching_proof_metadata(self):
        with tempfile.TemporaryDirectory() as temporary:
            fixture = fixtures.SplitEyeRecordingTest()
            recorder, _, _ = fixture.build(Path(temporary))
            recorder.start()
            fixture.feed(recorder, 3)
            sealed = recorder.stop()
            relative = next(name for name in sealed.verified_artifacts if name.endswith(".mp4"))
            path = sealed.path / relative
            proof = sealed.verified_artifacts[relative]
            payload = bytearray(path.read_bytes())
            payload[-1] ^= 1
            path.write_bytes(payload)
            identity = verified.identity

            def collide(metadata):
                if metadata.st_ino == proof.identity[1]:
                    return proof.identity
                return identity(metadata)

            with (
                patch.object(verified, "identity", side_effect=collide),
                self.assertRaises(device_session.DeviceRecordingError),
            ):
                device_session.validate_device_session_directory(
                    sealed.path, verified_artifacts=sealed.verified_artifacts
                )

    def test_recovery_does_not_repeat_full_readback_and_explicit_diagnostics_still_work(self):
        with tempfile.TemporaryDirectory() as temporary:
            recorder = recovery_fixtures.RecordingRecoveryTests().recording(Path(temporary))
            partial = recorder.partial_path
            target = partial / "video/left_00000.mp4"
            original_identity = verified.identity(target.stat())
            identity = verified.identity
            finalize = recovery._finalize_artifact

            def collide(metadata):
                if metadata.st_ino == original_identity[1]:
                    return original_identity
                return identity(metadata)

            def mutate_after_read(path, *args, **kwargs):
                result = finalize(path, *args, **kwargs)
                if path == target:
                    payload = bytearray(target.read_bytes())
                    payload[-1] ^= 1
                    target.write_bytes(payload)
                return result

            with (
                patch.object(recovery, "verified_identity", side_effect=collide),
                patch.object(verified, "identity", side_effect=collide),
                patch.object(recovery, "_finalize_artifact", side_effect=mutate_after_read),
                patch.object(
                    device_session,
                    "_verify_artifact_fd",
                    side_effect=AssertionError("automatic scan"),
                ),
            ):
                saved = recovery.recover_device_session(partial)
            self.assertIsNotNone(saved)
            self.assertFalse(partial.exists())
            with self.assertRaises(device_session.DeviceRecordingError):
                device_session.validate_device_session_directory(saved.path)


if __name__ == "__main__":
    unittest.main()

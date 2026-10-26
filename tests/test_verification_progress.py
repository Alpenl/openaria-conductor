import os
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch

from rp_ylx.recording import device_session
from rp_ylx.recording.encoding import RecordingEncoding
from tests import test_capture_coordinator as coordinator_fixtures
from tests import test_split_eye_recording as recorder_fixtures


class VerificationProgressTests(unittest.TestCase):
    def test_stop_has_no_verification_progress_for_raw_or_lossless_recordings(self):
        for lossless in (False, True):
            with self.subTest(lossless=lossless):
                fixture = coordinator_fixtures.CaptureCoordinatorTest()
                fixture.setUp()
                fixture.session_config = replace(
                    fixture.session_config,
                    lossless_storage=lossless,
                    recording_encoding=RecordingEncoding.from_mapping({"gop_frames": 1}),
                )
                coordinator = fixture.coordinator()
                observed = []
                try:
                    coordinator.start_capture(coordinator_fixtures.start_command("progress-start"))
                    self.assertTrue(coordinator.submit_frame(coordinator_fixtures.frame()))
                    recorder = coordinator._active
                    original = recorder._verification_progress

                    def report(
                        completed,
                        total,
                        original=original,
                        coordinator=coordinator,
                        observed=observed,
                    ):
                        original(completed, total)
                        status = coordinator.capture_status()
                        coordinator_fixtures.validate_capture_status(status)
                        self.assertEqual(status["snapshot"]["device_state"], "verifying")
                        observed.append((completed, total))

                    with patch.object(recorder, "_verification_progress", side_effect=report):
                        stopped = coordinator.stop_capture(
                            coordinator_fixtures.stop_command("progress-stop")
                        )
                    self.assertEqual(stopped.body["snapshot"]["device_state"], "idle")
                    self.assertEqual(observed, [])
                finally:
                    coordinator.close()
                    fixture.tearDown()

    def test_directory_progress_follows_actual_content_reads(self):
        with tempfile.TemporaryDirectory() as directory:
            fixture = recorder_fixtures.SplitEyeRecordingTest()
            recorder, _, _ = fixture.build(Path(directory))
            recorder.start()
            fixture.feed(recorder, 3)
            sealed = recorder.stop()
            for interruptible in (False, True):
                with self.subTest(interruptible=interruptible):
                    completed_reads = []
                    progress = []
                    original = device_session._verify_artifact_fd

                    def read(*args, original=original, completed_reads=completed_reads):
                        result = original(*args)
                        completed_reads.append(True)
                        return result

                    def report(done, total, completed_reads=completed_reads, progress=progress):
                        self.assertLessEqual(done, len(completed_reads))
                        progress.append((done, total))

                    with patch.object(device_session, "_verify_artifact_fd", side_effect=read):
                        device_session.validate_device_session_directory(
                            sealed.path,
                            verified_artifacts=sealed.verified_artifacts,
                            interrupt_check=(lambda: None) if interruptible else None,
                            progress=report,
                        )
                    count = len(completed_reads)
                    self.assertGreater(count, 2)
                    self.assertEqual(progress, [(i, count) for i in range(count + 1)])

    def test_corrupt_content_never_reports_complete_even_with_cached_identities(self):
        with tempfile.TemporaryDirectory() as directory:
            fixture = recorder_fixtures.SplitEyeRecordingTest()
            recorder, _, _ = fixture.build(Path(directory))
            recorder.start()
            fixture.feed(recorder, 3)
            sealed = recorder.stop()
            descriptor = device_session._manifest_artifacts(sealed.manifest)[0]
            path = sealed.path / descriptor["path"]
            metadata = path.stat()
            data = bytearray(path.read_bytes())
            data[-1] ^= 1
            path.write_bytes(data)
            os.utime(path, ns=(metadata.st_atime_ns, metadata.st_mtime_ns))
            progress = []
            with self.assertRaises(device_session.DeviceRecordingError) as caught:
                device_session.validate_device_session_directory(
                    sealed.path,
                    verified_artifacts=sealed.verified_artifacts,
                    progress=lambda done, total: progress.append((done, total)),
                )
            self.assertEqual(caught.exception.code, "digest_mismatch")
            self.assertTrue(progress)
            self.assertTrue(all(done < total for done, total in progress))

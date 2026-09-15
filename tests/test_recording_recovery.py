"""Independent prefix recovery, including crash and media corruption boundaries."""

import hashlib
import json
import tempfile
import unittest
import wave
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import patch

from rp_ylx.audio_clock import audio_clock_report
from rp_ylx.camera import FrameObservation, StereoFrame
from rp_ylx.recording.coordinator import (
    CaptureCoordinator,
    CoordinatorConfig,
    initialize_capture_volume,
)
from rp_ylx.recording.device_session import validate_device_session_directory, write_json_atomic
from rp_ylx.recording.recovery import recover_device_session
from tests import test_split_eye_recording as fixtures


class RecordingRecoveryTests(unittest.TestCase):
    def recording(self, root, frames=32):
        recorder, _, _ = fixtures.SplitEyeRecordingTest().build(root)
        with patch.object(recorder, "_now", return_value=datetime.now(UTC) - timedelta(seconds=10)):
            recorder.start()
        origin = recorder._started_monotonic_ns
        for index in range(frames):
            recorder.submit_frame(
                FrameObservation(
                    StereoFrame(
                        source_sequence=100 + index * 2,
                        host_monotonic_ns=origin + 10_000_000 + round(index * 1e9 / 30),
                        left=b"",
                        right=b"",
                        raw_side_by_side=fixtures.FRAME,
                    ),
                    dropped_before=0,
                )
            )
        recorder._stop_writer()
        recorder._abandon_encoder()
        recorder._close_files(ignore_errors=False)
        return recorder

    def test_ten_committed_pairs_survive_failed_eleventh_and_are_downloadable(self):
        with tempfile.TemporaryDirectory() as temporary:
            recorder = self.recording(Path(temporary))
            partial = recorder.partial_path
            original = (partial / "frames.ndjson").read_bytes()
            (partial / "video/left_00010.mp4").write_bytes(b"broken-tail")
            inode = (partial / "video/left_00000.mp4").stat().st_ino
            recorder.fail("source_sequence_gap", "gap of 71")
            result = recorder.recovered_session
            self.assertIsNotNone(result)
            manifest = validate_device_session_directory(result.path)
            self.assertEqual(manifest["frames"]["count"], 30)
            self.assertEqual(len(manifest["video"]["segments"]), 10)
            self.assertEqual((result.path / "frames.ndjson").read_bytes(), original)
            self.assertEqual((result.path / "video/left_00010.mp4").read_bytes(), b"broken-tail")
            self.assertEqual((result.path / "video/left_00000.mp4").stat().st_ino, inode)
            receipt = json.loads((result.path / "recovery/interruption.json").read_bytes())
            self.assertEqual(receipt["diagnostics"][0]["code"], "source_sequence_gap")
            self.assertFalse(partial.exists())
            self.assertIsNone(recover_device_session(partial))

    def test_restart_recovers_without_in_memory_segment_list(self):
        with tempfile.TemporaryDirectory() as temporary:
            recorder = self.recording(Path(temporary))
            recorder._segment_records.clear()
            recovered = recover_device_session(recorder.partial_path)
            self.assertEqual(recovered.manifest["frames"]["count"], 30)
            receipt = json.loads((recovered.path / "recovery/interruption.json").read_bytes())
            self.assertEqual(receipt["diagnostics"][0]["code"], "process_interrupted")

    def test_coordinator_restart_publishes_recovery_through_normal_catalog(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            volume = root / "volume"
            volume.mkdir()
            volume_id = initialize_capture_volume(volume)
            sessions = volume / "recordings"
            sessions.mkdir(exist_ok=True)
            recorder = self.recording(sessions)
            capture_path = recorder.partial_path / "capture.json"
            capture = json.loads(capture_path.read_bytes())
            capture["plan"]["volume_id"] = volume_id
            write_json_atomic(capture_path, capture)
            config = CoordinatorConfig(
                volume,
                root / "state",
                recorder._config,
                minimum_available_bytes=0,
                minimum_available_inodes=0,
            )
            for _ in range(2):
                coordinator = CaptureCoordinator(config, mount_checker=lambda path: path == volume)
                try:
                    retained = coordinator.capture_status()["snapshot"]["retained_unsuccessful"]
                    saved = retained["recording_state"]
                    self.assertIn(
                        "recording_prefix_saved", [d["code"] for d in saved["diagnostics"]]
                    )
                    revision = saved["state_revision"]
                    listed = coordinator.list_sessions(
                        cursor=None, limit=50, take_id=None, api_version="v4"
                    )
                    self.assertEqual(len(listed["items"]), 1)
                    session = listed["items"][0]
                    self.assertEqual(session["session_id"], recorder._plan.session_id)
                    coordinator._catalog_sessions(verify_session=recorder._plan.session_id)
                    # Recovered data uses the ordinary artifact access contract.
                    representation = coordinator.open_manifest(recorder._plan.session_id, "v4")
                    representation.close()
                    self.assertEqual(
                        coordinator.capture_status()["snapshot"]["retained_unsuccessful"][
                            "recording_state"
                        ]["state_revision"],
                        revision,
                    )
                finally:
                    coordinator.close()

    def test_missing_eye_or_bad_digest_keeps_only_contiguous_prefix(self):
        for mutation in ("missing", "changed", "symlink"):
            with self.subTest(mutation=mutation), tempfile.TemporaryDirectory() as temporary:
                recorder = self.recording(Path(temporary))
                path = recorder.partial_path / "video/right_00009.mp4"
                if mutation == "changed":
                    path.write_bytes(b"invalid")
                else:
                    path.unlink()
                    if mutation == "symlink":
                        path.symlink_to("right_00008.mp4")
                recovered = recover_device_session(recorder.partial_path)
                self.assertEqual(recovered.manifest["frames"]["count"], 27)
                self.assertEqual(len(recovered.manifest["video"]["segments"]), 9)

    def test_torn_frame_index_excludes_the_whole_unindexed_pair(self):
        with tempfile.TemporaryDirectory() as temporary:
            recorder = self.recording(Path(temporary))
            path = recorder.partial_path / "frames.ndjson"
            lines = path.read_bytes().splitlines(keepends=True)
            path.write_bytes(b"".join(lines[:28]) + lines[28][:25])
            recovered = recover_device_session(recorder.partial_path)
            self.assertEqual(recovered.manifest["frames"]["count"], 27)

    def test_first_segment_failure_does_not_claim_a_usable_session(self):
        with tempfile.TemporaryDirectory() as temporary:
            recorder = self.recording(Path(temporary), frames=2)
            recorder.fail("encoder_failed", "first pair never closed")
            self.assertIsNone(recorder.recovered_session)
            self.assertTrue(recorder.partial_path.is_dir())
            self.assertFalse((recorder.partial_path / "manifest.json").exists())

    def test_storage_failure_keeps_raw_data_and_retry_is_possible(self):
        with tempfile.TemporaryDirectory() as temporary:
            recorder = self.recording(Path(temporary))
            partial = recorder.partial_path
            digest = hashlib.sha256((partial / "frames.ndjson").read_bytes()).hexdigest()
            with patch("rp_ylx.recording.recovery._write_records", side_effect=OSError("no space")):
                recorder.fail("source_sequence_gap", "gap")
            self.assertTrue(partial.exists())
            self.assertEqual(
                hashlib.sha256((partial / "frames.ndjson").read_bytes()).hexdigest(), digest
            )
            self.assertIsNotNone(recover_device_session(partial))

    def test_missing_audio_clock_is_explicit_not_user_disabled(self):
        with tempfile.TemporaryDirectory() as temporary:
            recorder = self.recording(Path(temporary))
            capture_path = recorder.partial_path / "capture.json"
            capture = json.loads(capture_path.read_bytes())
            capture["config"]["audio_enabled"] = True
            write_json_atomic(capture_path, capture)
            recovered = recover_device_session(recorder.partial_path)
            self.assertEqual(recovered.manifest["audio"]["reason"], "recording_interrupted")
            validate_device_session_directory(recovered.path)

    def test_audio_prefix_keeps_real_pcm_samples_and_verified_clock(self):
        with tempfile.TemporaryDirectory() as temporary:
            recorder = self.recording(Path(temporary))
            root = recorder.partial_path
            capture = json.loads((root / "capture.json").read_bytes())
            capture["config"]["audio_enabled"] = True
            write_json_atomic(root / "capture.json", capture)
            (root / "audio").mkdir()
            rate = 48000
            pcm = b"\x01\x00\xfe\xff" * (2 * rate)
            with wave.open(str(root / "audio/audio_00000.wav"), "wb") as output:
                output.setparams((2, 2, rate, 0, "NONE", "not compressed"))
                output.writeframes(pcm)
            origin = recorder._started_monotonic_ns
            start = origin + 20_000_000
            write_json_atomic(
                root / "audio/checkpoint.json",
                {
                    "sample_count": 2 * rate,
                    "thread_started_monotonic_ns": start,
                    "period_frames": 1024,
                    "buffer_frames": 8192,
                    "anchors": [
                        [n, start + round(n * 1e9 / rate)]
                        for n in range(1024, 2 * rate + 1024, 4800)
                    ],
                    "queue_capacity_frames": 786432,
                    "queue_peak_frames": 1024,
                    "max_write_ns": 400000,
                    "segments": [
                        {"path": "audio/audio_00000.wav", "start_sample": 0, "end_sample": 2 * rate}
                    ],
                },
            )
            recovered = recover_device_session(root)
            audio = recovered.manifest["audio"]
            report = audio_clock_report(audio, recovered.manifest["time"]["duration_seconds"])
            self.assertEqual(report["continuity"], "verified")
            self.assertLess(audio["sample_count"], 2 * rate)
            with wave.open(
                str(recovered.path / audio["segments"][0]["artifact"]["path"]), "rb"
            ) as stream:
                self.assertEqual(
                    stream.readframes(audio["sample_count"]), pcm[: audio["sample_count"] * 4]
                )
            self.assertEqual((recovered.path / "audio/audio_00000.wav").read_bytes()[44:], pcm)

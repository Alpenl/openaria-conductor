"""边录边出左右眼 H.264 的录制器行为（issue #46）。"""

from __future__ import annotations

import hashlib
import json
import tempfile
import threading
import time
import unittest
import uuid
from copy import deepcopy
from dataclasses import replace
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

from rp_ylx.api.downloads import (
    ArtifactAccessError,
    iter_device_session_v1_artifacts,
    validate_device_session_manifest,
)
from rp_ylx.camera import FrameObservation, StereoFrame
from rp_ylx.performance.metrics import PerformanceMetrics
from rp_ylx.recording import (
    DeviceSessionConfig,
    DeviceSessionRecorder,
    SessionPlan,
    StorageStatus,
    uuid7,
)
from rp_ylx.recording.device_session import (
    DeviceRecordingError,
    _finalize_artifact,
    inspect_device_session_directory,
    manifest_artifact_bytes_total,
    validate_device_session_directory,
)
from rp_ylx.recording.encoding import RecordingEncoding
from tests.testkit import (
    FRAME,
    FakeAudioRecorder,
    FakeNativeSessionStoreIo,
    FakeSessionStore,
    FakeStereoEncoder,
    imu_observation,
)
from tests.testkit import split_eye_frame as frame


class SplitEyeRecordingTest(unittest.TestCase):
    segment_seconds = 0.1  # 30fps * 0.1s = 3 frames per segment

    def test_finalize_artifact_uses_native_metadata(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "artifact.bin"
            path.write_bytes(b"abc")
            native = SimpleNamespace(
                finalize_artifact=Mock(
                    return_value={
                        "sha256": "a" * 64,
                        "identity": {
                            "device": 1,
                            "inode": 2,
                            "size": 3,
                            "modified_ns": 4,
                            "nlink": 1,
                        },
                    }
                )
            )

            with patch(
                "rp_ylx.recording.device_session._session_store_or_none",
                return_value=native,
            ):
                finalized = _finalize_artifact(path, 3, code="segment_invalid")

            native.finalize_artifact.assert_called_once_with(str(path), 3)
            self.assertEqual(finalized.sha256, "a" * 64)
            self.assertEqual(finalized.bytes, 3)
            self.assertEqual(finalized.identity, (1, 2, 3, 4))

    def test_finalize_artifact_maps_native_errors_to_context_code(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "artifact.bin"
            path.write_bytes(b"abc")
            native = SimpleNamespace(
                finalize_artifact=Mock(side_effect=RuntimeError("artifact_invalid: size changed"))
            )

            with (
                patch(
                    "rp_ylx.recording.device_session._session_store_or_none",
                    return_value=native,
                ),
                self.assertRaises(DeviceRecordingError) as raised,
            ):
                _finalize_artifact(path, 3, code="segment_invalid")

            self.assertEqual(raised.exception.code, "segment_invalid")
            self.assertEqual(raised.exception.message, "size changed")

    def build(
        self,
        root: Path,
        *,
        queue_capacity: int = 128,
        enqueue_timeout: float = 0.05,
        before_write: object | None = None,
        audio_enabled: bool = False,
        audio_fail_stop: bool = False,
        native_data_plane: bool = False,
        recording_encoding: RecordingEncoding | None = None,
        **encoder_options: object,
    ) -> tuple[DeviceSessionRecorder, list[FakeStereoEncoder], list[FakeAudioRecorder]]:
        revision = 0
        encoders: list[FakeStereoEncoder] = []
        audios: list[FakeAudioRecorder] = []

        def allocate_revision() -> int:
            nonlocal revision
            revision += 1
            return revision

        config = DeviceSessionConfig(
            device_id=str(uuid.uuid4()),
            device_label="YLX-12AB34CD",
            hardware_fingerprint="sha256:" + "a" * 64,
            platform="D-Robotics RDK X5 V1.0 + YLX 2UQ2",
            software_version="0.5.0",
            commit="b" * 40,
            width=3840,
            height=1080,
            sensor_fps=60.0,
            frame_decimation=2,
            video_layout="split-eyes",
            segment_seconds=self.segment_seconds,
            audio_enabled=audio_enabled,
            recording_encoding=recording_encoding,
        )

        def factory(partial: Path) -> FakeStereoEncoder:
            encoder = FakeStereoEncoder(
                partial / "video",
                segment_frames=round(
                    config.segment_seconds * config.sensor_fps / config.frame_decimation
                ),
                **encoder_options,  # type: ignore[arg-type]
            )
            encoders.append(encoder)
            return encoder

        def audio_factory(partial: Path) -> FakeAudioRecorder:
            audio = FakeAudioRecorder(partial, fail_stop=audio_fail_stop)
            audios.append(audio)
            return audio

        recorder = DeviceSessionRecorder(
            root,
            config,
            SessionPlan(
                session_id=uuid7(),
                volume_id=str(uuid.uuid4()),
                generation_id=str(uuid.uuid4()),
                capture_mode="production",
                display_name="split-eye fixture",
                take_id=uuid7(),
                take_sequence=1,
                continuation_of=None,
            ),
            authority_epoch=str(uuid.uuid4()),
            allocate_revision=allocate_revision,
            storage_status=lambda: StorageStatus(1024 * 1024 * 1024, True),
            encoder_factory=None if native_data_plane else factory,  # type: ignore[arg-type]
            checkpoint_interval=0.0,
            queue_capacity=queue_capacity,
            enqueue_timeout=enqueue_timeout,
            before_write=None if native_data_plane else before_write,  # type: ignore[arg-type]
            audio_recorder_factory=(None if native_data_plane else audio_factory),
            native_data_plane=native_data_plane,
        )
        return recorder, encoders, audios

    @staticmethod
    def feed(recorder: DeviceSessionRecorder, count: int) -> None:
        for index in range(count):
            recorder.submit_frame(
                FrameObservation(
                    StereoFrame(
                        source_sequence=index,
                        host_monotonic_ns=index * 33_000_000,
                        left=b"",
                        right=b"",
                        raw_side_by_side=FRAME,
                    ),
                    dropped_before=0,
                )
            )

    def test_native_checkpoint_waits_for_indexed_frames_and_coalesces_ready_pairs(self) -> None:
        from types import SimpleNamespace

        for indexed, expected in [(0, 0), (3, 1), (6, 2)]:
            with self.subTest(indexed=indexed), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                recorder, _, _ = self.build(root)
                recorder._partial = root / "partial"
                recorder._partial.mkdir()
                for name in ("frames.ndjson", "imu.ndjson"):
                    (recorder._partial / name).write_bytes(b"")
                segments = [
                    {
                        "index": i,
                        "start_frame": i * 3,
                        "end_frame": (i + 1) * 3,
                        "left_path": f"video/left_{i:05d}.mp4",
                        "left_bytes": 1,
                        "right_path": f"video/right_{i:05d}.mp4",
                        "right_bytes": 1,
                    }
                    for i in range(2)
                ]
                recorder._native_transaction = SimpleNamespace(
                    segments=lambda segments=segments: segments,
                    snapshot=lambda indexed=indexed: {
                        "sink": {"frames_written": 6, "frames_indexed": indexed}
                    },
                )
                with (
                    patch.object(
                        recorder, "_segment_record", side_effect=lambda item: {"index": item.index}
                    ),
                    patch.object(recorder, "_capture_diagnostics", return_value={}),
                    patch("rp_ylx.recording.device_session.write_json_atomic") as checkpoint,
                    patch("rp_ylx.recording.device_session.os.fsync") as sync,
                ):
                    recorder._harvest_segments()
                self.assertEqual(recorder._harvested_segments, expected)
                self.assertEqual(checkpoint.call_count, int(expected > 0))
                self.assertEqual(sync.call_count, 2 if expected else 0)
                if expected:
                    self.assertEqual(len(checkpoint.call_args.args[1]["segments"]), expected)

    def test_capture_diagnostics_identify_shared_metrics_lifetime(self) -> None:
        metrics = PerformanceMetrics()
        metrics.record_loss("source_gap", 2)
        with tempfile.TemporaryDirectory() as directory:
            first, _, _ = self.build(Path(directory))
            second, _, _ = self.build(Path(directory))
            first._metrics = second._metrics = metrics
            for recorder in (first, second):
                result = recorder._capture_diagnostics()
                self.assertEqual(result["capture_scope"], "metrics_lifetime")
                self.assertEqual(result["capture"]["loss"]["source_gap"], 2)
            self.assertEqual(metrics.snapshot().loss["source_gap"], 2)

    def test_metrics_snapshot_failure_preserves_failure_file_and_encoder_diagnostics(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            recorder, _, _ = self.build(Path(directory))
            recorder.start()
            recorder._metrics = Mock(snapshot=Mock(side_effect=RuntimeError("metrics unavailable")))
            recorder._encoder.stats = {"frames_submitted": 7}
            recorder.fail("source_sequence_gap", "source sequence 10 -> 13", media_lost=True)
            diagnostic = json.loads(
                (recorder.partial_path / "failure-diagnostics.json").read_text()
            )
            self.assertEqual(diagnostic["code"], "source_sequence_gap")
            self.assertEqual(diagnostic["message"], "source sequence 10 -> 13")
            self.assertEqual(diagnostic["capture_snapshot_error"], "metrics unavailable")
            self.assertEqual(diagnostic["encoder"], {"frames_submitted": 7})
            self.assertNotIn("capture", diagnostic)

    def test_failure_file_includes_audio_error_discovered_during_final_drain(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            recorder, _, _ = self.build(Path(directory))
            recorder.start()
            recorder._stop_writer()
            drained = False

            def abort(reason):
                nonlocal drained
                drained = True

            transaction = Mock()
            transaction.abort.side_effect = abort
            transaction.diagnostic_snapshot.side_effect = lambda: {
                "audio_failure": {"code": "audio_failed", "message": "late commit failure"}
                if drained
                else None,
                "sink": {},
                "encoder_stats": {},
            }
            recorder._native_transaction = transaction
            with patch.object(recorder, "_harvest_segments"):
                recorder.fail("encoder_failed", "backpressure", media_lost=True)
            diagnostic = json.loads(
                (recorder.partial_path / "failure-diagnostics.json").read_text()
            )
            self.assertEqual(diagnostic["audio_failure"]["message"], "late commit failure")
            self.assertIsNone(recorder._native_transaction)

    def test_audio_failure_does_not_hide_encoder_and_index_diagnostics(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            recorder, _, _ = self.build(Path(directory))
            error = {"code": "audio_failed", "message": "audio durable commit backlog exhausted"}
            encoder_error = {"code": "encoder_failed", "message": "dequeue input failed: -16"}
            transaction = SimpleNamespace(
                snapshot=Mock(side_effect=RuntimeError(error["message"])),
                abort=Mock(),
                diagnostic_snapshot=Mock(
                    return_value={
                        "audio_failure": error,
                        "encoder_failure": encoder_error,
                        "encoder_stats": {"frames_submitted": 90},
                        "sink": {
                            "frames_written": 90,
                            "frames_indexed": 83,
                            "index_pending_bytes": 4096,
                            "bytes_written": 12345,
                        },
                    }
                ),
            )
            recorder._native_transaction = transaction
            result = recorder._capture_diagnostics()
            self.assertEqual(result["audio_failure"], error)
            self.assertEqual(result["encoder_failure"], encoder_error)
            self.assertEqual(result["encoder"]["frames_submitted"], 90)
            self.assertEqual(result["index_writer"]["frames_indexed"], 83)
            self.assertEqual(result["index_writer"]["index_pending_bytes"], 4096)
            self.assertNotIn("snapshot_error", result)
            transaction.snapshot.assert_not_called()
            transaction.diagnostic_snapshot.assert_called_once_with()
            recorder._native_direct_recording = True
            live = recorder._live_recording_state_locked({"progress": {"captured_frames": 0}})
            self.assertEqual(live["progress"]["captured_frames"], 90)
            self.assertEqual(live["progress"]["bytes_written"], 12345)
            transaction.snapshot.assert_not_called()
            recorder._abandon_encoder()
            self.assertIsNone(recorder._native_transaction)
            transaction.abort.assert_called_once_with("recording abandoned")
            final_state = recorder._state_document("failed", diagnostics=[])
            self.assertEqual(final_state["progress"]["captured_frames"], 90)
            self.assertEqual(final_state["progress"]["bytes_written"], 12345)

    def test_sealed_session_declares_contiguous_split_eye_segments(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            recorder, _, _ = self.build(root)
            recorder.start()
            self.feed(recorder, 7)
            sealed = recorder.stop()

            manifest = sealed.manifest
            video = manifest["video"]
            self.assertEqual(video["layout"], "split-eyes")
            self.assertEqual(video["codec"], "h264")
            self.assertEqual(video["container"], "mp4")
            segments = video["segments"]
            self.assertEqual([segment["index"] for segment in segments], [0, 1, 2])
            self.assertEqual(segments[0]["start_frame"], 0)
            self.assertEqual(segments[-1]["end_frame"], 7)
            for previous, following in zip(segments, segments[1:], strict=False):
                self.assertEqual(previous["end_frame"], following["start_frame"])
                self.assertEqual(previous["end_time_seconds"], following["start_time_seconds"])
            self.assertNotIn(
                "raw-sbs.mjpeg", {item.name for item in (sealed.path / "video").iterdir()}
            )
            validate_device_session_manifest(manifest)

    def test_ntp_jump_preserves_elapsed_and_repairs_sealed_recording_time(self) -> None:
        for start_text in ("2000-01-01T08:01:29+08:00", "2026-09-14T18:28:00+08:00"):
            with self.subTest(start=start_text), tempfile.TemporaryDirectory() as directory:
                recorder, _, _ = self.build(Path(directory))
                start = datetime.fromisoformat(start_text)
                end = datetime.fromisoformat("2026-09-13T18:28:14+08:00")
                recorder._plan = replace(
                    recorder._plan, display_name=start.strftime("录制 %Y-%m-%d %H:%M:%S")
                )
                with patch.object(recorder, "_now", return_value=start):
                    recorder.start()
                    self.feed(recorder, 7)
                with patch.object(recorder, "_now", return_value=end):
                    self.assertLess(recorder._elapsed(), 5)
                    sealed = recorder.stop()
                timing = sealed.manifest["time"]
                corrected = datetime.fromisoformat(timing["started_at"])
                self.assertAlmostEqual(
                    (end - corrected).total_seconds(), timing["duration_seconds"], places=5
                )
                self.assertEqual(
                    sealed.manifest["display_name"], corrected.strftime("录制 %Y-%m-%d %H:%M:%S")
                )
                validate_device_session_manifest(sealed.manifest)

    def test_v3_encoding_survives_native_arguments_seal_and_validation(self) -> None:
        for codec in ("h264", "hevc"):
            with self.subTest(codec=codec), tempfile.TemporaryDirectory() as directory:
                encoding = RecordingEncoding.from_mapping(
                    {"preset": "high", "codec": codec, "gop_frames": 3}
                )
                store = FakeSessionStore()
                with (
                    patch(
                        "rp_ylx.recording.device_session.native_session_store", return_value=store
                    ),
                    patch(
                        "rp_ylx.recording.device_session.resolve_executable",
                        return_value=Path("/bin/true"),
                    ),
                ):
                    recorder, _, _ = self.build(
                        Path(directory), native_data_plane=True, recording_encoding=encoding
                    )
                    recorder.start()
                    self.assertEqual(store.transaction.plan.encoder_arguments, encoding.arguments())
                    store.transaction.advance(frames=4, imu_samples=2)
                    sealed = recorder.stop()
                self.assertEqual(sealed.manifest["schema"], "ylx.device-session.v3")
                self.assertEqual(sealed.manifest["video"]["codec"], codec)
                self.assertEqual(sealed.manifest["video"]["encoding"], encoding.manifest())
                validate_device_session_manifest(sealed.manifest)
                validate_device_session_directory(sealed.path)
                bad = deepcopy(sealed.manifest)
                bad["video"]["encoding"]["codec"] = "hevc" if codec == "h264" else "h264"
                with self.assertRaises(ArtifactAccessError):
                    validate_device_session_manifest(bad)

    def test_session_transaction_owns_live_progress_finish_and_seal(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            store = FakeSessionStore()
            with (
                patch(
                    "rp_ylx.recording.device_session.native_session_store",
                    return_value=store,
                ),
                patch(
                    "rp_ylx.recording.device_session.resolve_executable",
                    return_value=Path("/bin/true"),
                ),
            ):
                recorder, _, _ = self.build(root, native_data_plane=True)
                recorder.start()
                transaction = recorder.native_recording_transaction()
                self.assertIs(transaction, store.transaction)
                assert store.transaction is not None
                initial = recorder.current_recording_state
                assert initial is not None
                store.transaction.advance(frames=4, imu_samples=2)
                live = recorder.current_recording_state
                assert live is not None
                self.assertEqual(live["state_revision"], initial["state_revision"])
                self.assertEqual(live["progress"]["captured_frames"], 4)
                self.assertGreater(live["progress"]["bytes_written"], 0)
                native_finish = store.transaction.finish
                finished_at_ns = []

                def delayed_finish(duration_seconds, timeout_seconds):
                    result = native_finish(duration_seconds, timeout_seconds)
                    time.sleep(0.03)
                    finished_at_ns.append(time.monotonic_ns())
                    return result

                with patch.object(store.transaction, "finish", side_effect=delayed_finish):
                    sealed = recorder.stop()
                self.assertEqual(
                    sealed.manifest["time"]["duration_seconds"],
                    recorder._stopped_elapsed,
                )
                self.assertLess(
                    sealed.manifest["time"]["duration_seconds"],
                    (finished_at_ns[0] - recorder._started_monotonic_ns) / 1e9,
                )

            self.assertEqual(len(store.begin_calls), 1)
            self.assertTrue(store.transaction.finished)
            self.assertTrue(store.transaction.sealed)
            self.assertFalse(store.transaction.aborted)
            self.assertEqual(store.transaction.open_handle_count(), 0)
            self.assertEqual(len(store.transaction.seal_calls), 1)
            partial_path, final_path, session_id, paths = store.transaction.seal_calls[0]
            self.assertEqual(Path(final_path), sealed.path)
            self.assertFalse(Path(partial_path).exists())
            self.assertTrue(Path(final_path).is_dir())
            self.assertEqual(session_id, sealed.manifest["session_id"])
            self.assertEqual(
                paths,
                [
                    "video/left_00000.mp4",
                    "video/right_00000.mp4",
                    "video/left_00001.mp4",
                    "video/right_00001.mp4",
                    "frames.ndjson",
                    "imu.ndjson",
                ],
            )
            self.assertTrue((sealed.path / "manifest.json").is_file())
            self.assertFalse((sealed.path / "recording.json").exists())
            self.assertFalse((sealed.path / "capture.json").exists())
            self.assertEqual(sealed.manifest["frames"]["count"], 4)
            self.assertEqual(sealed.manifest["imu"]["sample_count"], 2)
            self.assertEqual(
                [ordinal for ordinal, _ in store.transaction.boundary_calls],
                [0, 3, 3, 4],
            )
            validate_device_session_manifest(sealed.manifest)

    def test_device_session_config_rejects_raw_sbs_writer(self) -> None:
        with self.assertRaisesRegex(ValueError, "Device Session"):
            DeviceSessionConfig(
                device_id=str(uuid.uuid4()),
                device_label="YLX-12AB34CD",
                hardware_fingerprint="sha256:" + "a" * 64,
                platform="D-Robotics RDK X5 V1.0 + YLX 2UQ2",
                software_version="0.5.0",
                commit="b" * 40,
                width=3840,
                height=1080,
                sensor_fps=60.0,
                video_layout="raw-side-by-side",
            )

    def test_native_data_plane_rejects_python_frame_and_imu_callbacks(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            store = FakeSessionStore()
            with (
                patch(
                    "rp_ylx.recording.device_session.native_session_store",
                    return_value=store,
                ),
                patch(
                    "rp_ylx.recording.device_session.resolve_executable",
                    return_value=Path("/bin/true"),
                ),
            ):
                recorder, _, _ = self.build(root, native_data_plane=True)
                recorder.start()
                with self.assertRaises(DeviceRecordingError) as frame_error:
                    recorder.submit_frame(frame(0))
                with self.assertRaises(DeviceRecordingError) as imu_error:
                    recorder.submit_imu(imu_observation())
                recorder.fail("test_cleanup", "cleanup", recoverable=False)
            self.assertEqual(frame_error.exception.code, "invalid_state")
            self.assertEqual(imu_error.exception.code, "invalid_state")
            assert store.transaction is not None
            self.assertTrue(store.transaction.aborted)

    def test_native_failure_preserves_recorded_frame_count(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            store = FakeSessionStore()
            with (
                patch("rp_ylx.recording.device_session.native_session_store", return_value=store),
                patch(
                    "rp_ylx.recording.device_session.resolve_executable",
                    return_value=Path("/bin/true"),
                ),
            ):
                recorder, _, _ = self.build(root, native_data_plane=True)
                recorder.start()
                assert store.transaction is not None
                store.transaction.advance(frames=3, imu_samples=2)
                partial = recorder.fail(
                    "source_sequence_gap", "source frame sequence has a gap of 8"
                )
            state = json.loads((partial / "recording.json").read_text())
            self.assertEqual(state["progress"]["captured_frames"], 3)
            self.assertGreater(state["progress"]["bytes_written"], 0)
            self.assertEqual(state["diagnostics"][0]["code"], "source_sequence_gap")
            self.assertFalse((partial / "manifest.json").exists())
            self.assertTrue(store.transaction.aborted)

    def test_session_transaction_failure_aborts_without_publishing(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            store = FakeSessionStore(fail_finish=True)
            with (
                patch(
                    "rp_ylx.recording.device_session.native_session_store",
                    return_value=store,
                ),
                patch(
                    "rp_ylx.recording.device_session.resolve_executable",
                    return_value=Path("/bin/true"),
                ),
            ):
                recorder, _, _ = self.build(root, native_data_plane=True)
                recorder.start()
                assert store.transaction is not None
                store.transaction.advance(frames=3)
                with self.assertRaises(DeviceRecordingError) as raised:
                    recorder.stop()
            self.assertEqual(raised.exception.code, "encoder_failed")
            self.assertTrue(store.transaction.aborted)
            self.assertFalse((root / recorder._plan.session_id).exists())

    def test_live_progress_includes_audio_snapshot_bytes_before_stop(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            recorder, _, audios = self.build(root, audio_enabled=True)
            recorder.start()
            audios[0].advance_live(sample_count=4_800, bytes_written=47)

            live = recorder.current_recording_state

            assert live is not None
            self.assertEqual(live["progress"]["bytes_written"], 47)
            recorder.fail("test_cleanup", "cleanup", recoverable=False)

    def test_active_take_pending_allows_writer_inflight_plus_queue_capacity(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            recorder, _, _ = self.build(root, queue_capacity=2)

            snapshot = {
                "session_id": recorder._plan.session_id,
                "frame_domain": 3,
                "frames_written": 0,
                "pending_frames": 3,
                "drop_events": [],
            }
            recorder._apply_active_take_snapshot(snapshot, expected_frames_written=0)

            self.assertEqual(recorder._frame_domain, 3)

            snapshot["pending_frames"] = 4
            with self.assertRaises(DeviceRecordingError) as raised:
                recorder._apply_active_take_snapshot(snapshot, expected_frames_written=0)
            self.assertEqual(raised.exception.code, "active_take_writer_failed")

    def test_native_recorder_exposes_only_one_session_transaction(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            store = FakeSessionStore()
            with (
                patch(
                    "rp_ylx.recording.device_session.native_session_store",
                    return_value=store,
                ),
                patch(
                    "rp_ylx.recording.device_session.resolve_executable",
                    return_value=Path("/bin/true"),
                ),
            ):
                recorder, _, _ = self.build(root, native_data_plane=True)
                recorder.start()
                transaction = recorder.native_recording_transaction()
                self.assertIs(transaction, store.transaction)
                self.assertFalse(hasattr(recorder, "native_split_sink_targets"))
                recorder.fail("test_cleanup", "cleanup", recoverable=False)

    def test_explicit_source_test_adapter_uses_python_files(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            recorder, _, _ = self.build(root)
            recorder.start()
            self.feed(recorder, 3)
            sealed = recorder.stop()

            self.assertEqual(sealed.manifest["frames"]["count"], 3)
            validate_device_session_manifest(sealed.manifest)

    def test_take_ending_on_a_segment_boundary_seals_without_an_empty_tail(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            recorder, _, _ = self.build(root)
            recorder.start()
            self.feed(recorder, 6)  # 恰好两段，末尾不留空段
            sealed = recorder.stop()

            segments = sealed.manifest["video"]["segments"]
            self.assertEqual([segment["index"] for segment in segments], [0, 1])
            self.assertEqual(segments[-1]["end_frame"], 6)
            self.assertEqual(
                sorted(item.name for item in (sealed.path / "video").iterdir()),
                ["left_00000.mp4", "left_00001.mp4", "right_00000.mp4", "right_00001.mp4"],
            )
            validate_device_session_manifest(sealed.manifest)

    def test_segment_artifacts_carry_verified_digests(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            recorder, _, _ = self.build(root)
            recorder.start()
            self.feed(recorder, 6)
            sealed = recorder.stop()

            for segment in sealed.manifest["video"]["segments"]:
                for eye in ("left", "right"):
                    artifact = segment["artifacts"][eye]
                    self.assertEqual(artifact["role"], f"video.{eye}")
                    self.assertEqual(artifact["media_type"], "video/mp4")
                    body = (sealed.path / artifact["path"]).read_bytes()
                    self.assertEqual(artifact["bytes"], len(body))
                    self.assertEqual(artifact["sha256"], hashlib.sha256(body).hexdigest())
                    self.assertEqual(artifact["artifact_id"], artifact["sha256"])

    def test_frame_index_records_segment_placement(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            recorder, _, _ = self.build(root)
            recorder.start()
            self.feed(recorder, 4)
            sealed = recorder.stop()

            records = [
                json.loads(line)
                for line in (sealed.path / "frames.ndjson").read_text().splitlines()
            ]
            self.assertEqual([record["segment_index"] for record in records], [0, 0, 0, 1])
            self.assertEqual([record["segment_frame"] for record in records], [0, 1, 2, 0])
            self.assertNotIn("video_offset", records[0])

    def test_open_handles_cover_the_encoder_until_the_take_ends(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            recorder, _, _ = self.build(root)
            recorder.start()
            self.feed(recorder, 4)
            # 助手在卷上持有分段文件，安全换盘不能在它退出前放行。
            self.assertGreater(recorder.open_handle_count, 0)
            recorder.stop()
            self.assertEqual(recorder.open_handle_count, 0)

    def test_encoder_failure_fails_the_take_instead_of_sealing(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            recorder, encoders, _ = self.build(root, fail_after=3)
            recorder.start()
            self.feed(recorder, 3)
            with self.assertRaises(DeviceRecordingError):
                self.feed(recorder, 1)
                recorder.stop()
            self.assertNotEqual(recorder.state, "sealed")
            self.assertFalse((root / recorder.partial_path.name.removesuffix(".partial")).exists())
            self.assertTrue(encoders[0].aborted)

    def test_missing_tail_segment_blocks_sealing(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            recorder, _, _ = self.build(root, drop_tail_segment=True)
            recorder.start()
            self.feed(recorder, 5)
            with self.assertRaises(DeviceRecordingError) as raised:
                recorder.stop()
            self.assertEqual(raised.exception.code, "segment_invalid")
            self.assertNotEqual(recorder.state, "sealed")

    def test_downstream_rejects_manifest_with_a_frame_domain_hole(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            recorder, _, _ = self.build(root)
            recorder.start()
            self.feed(recorder, 6)
            manifest = json.loads(json.dumps(recorder.stop().manifest))
            manifest["video"]["segments"][1]["start_frame"] += 1
            with self.assertRaises(ArtifactAccessError):
                validate_device_session_manifest(manifest)

    def test_split_eye_artifact_iterator_exposes_all_video_segments(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            recorder, _, _ = self.build(root)
            recorder.start()
            self.feed(recorder, 7)
            manifest = recorder.stop().manifest

            artifacts = list(iter_device_session_v1_artifacts(manifest))
            self.assertEqual(
                [artifact["path"] for artifact in artifacts],
                [
                    "video/left_00000.mp4",
                    "video/right_00000.mp4",
                    "video/left_00001.mp4",
                    "video/right_00001.mp4",
                    "video/left_00002.mp4",
                    "video/right_00002.mp4",
                    "frames.ndjson",
                    "imu.ndjson",
                ],
            )

    def test_session_store_owns_safe_manifest_and_artifact_io(self) -> None:
        native = FakeNativeSessionStoreIo()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with patch(
                "rp_ylx.recording.device_session._session_store_or_none",
                return_value=native,
            ):
                recorder, _, _ = self.build(root)
                recorder.start()
                self.feed(recorder, 4)
                sealed = recorder.stop()
                inspected, _ = inspect_device_session_directory(sealed.path)
                validated = validate_device_session_directory(sealed.path)

        self.assertEqual(inspected["session_id"], sealed.manifest["session_id"])
        self.assertEqual(validated["session_id"], sealed.manifest["session_id"])
        open_paths = [relative for _, relative in native.open_calls]
        self.assertGreaterEqual(open_paths.count("manifest.json"), 2)
        for relative in (
            "video/left_00000.mp4",
            "video/right_00000.mp4",
            "frames.ndjson",
            "imu.ndjson",
        ):
            self.assertIn(relative, open_paths)
        self.assertGreaterEqual(len(native.read_calls), 2)
        self.assertEqual(len(native.verify_calls), 6)

    def test_audio_segments_are_declared_and_downloadable(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            recorder, _, audios = self.build(root, audio_enabled=True)
            recorder.start()
            self.feed(recorder, 3)
            sealed = recorder.stop()

            self.assertTrue(audios[0].started)
            audio = sealed.manifest["audio"]
            self.assertEqual(sealed.manifest["schema"], "ylx.device-session.v2")
            self.assertEqual(sealed.manifest["imu"]["coordinate_frame"], "raw_device_axes")
            self.assertEqual(audio["state"], "recorded")
            self.assertEqual(audio["requested_mode"], "enabled")
            self.assertEqual(audio["resolved_mode"], "enabled")
            self.assertEqual(audio["codec"], "pcm_s16le")
            self.assertEqual(audio["container"], "wav")
            self.assertEqual(audio["sample_rate"], 48_000)
            self.assertEqual(audio["sample_count"], 4_800)
            self.assertEqual(audio["sync"]["time_base"], "host_monotonic")
            self.assertEqual(audio["sync"]["video_time_reference"], "session_time_seconds")
            self.assertGreaterEqual(audio["sync"]["start_time_seconds"], 0)
            self.assertGreater(
                audio["sync"]["end_time_seconds"],
                audio["sync"]["start_time_seconds"],
            )
            self.assertEqual(audio["segments"][0]["pcm_payload_bytes"], 4_800 * 2 * 2)
            self.assertEqual(audio["segments"][0]["wav_header_bytes"], 44)
            artifact = audio["segments"][0]["artifact"]
            self.assertEqual(artifact["role"], "audio.wav")
            self.assertEqual(artifact["media_type"], "audio/wav")
            self.assertEqual((sealed.path / artifact["path"]).read_bytes()[:4], b"RIFF")
            self.assertIn(
                "audio/audio_00000.wav",
                [item["path"] for item in iter_device_session_v1_artifacts(sealed.manifest)],
            )
            validate_device_session_manifest(sealed.manifest)

    def test_audio_disabled_is_declared_as_not_recorded(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            recorder, _, _ = self.build(root, audio_enabled=False)
            recorder.start()
            self.feed(recorder, 3)
            sealed = recorder.stop()

        self.assertEqual(sealed.manifest["schema"], "ylx.device-session.v2")
        self.assertEqual(
            sealed.manifest["audio"],
            {
                "state": "not_recorded",
                "requested_mode": "disabled",
                "resolved_mode": "disabled",
                "reason": "user_disabled",
            },
        )
        self.assertNotIn(
            "audio/audio_00000.wav",
            [item["path"] for item in iter_device_session_v1_artifacts(sealed.manifest)],
        )
        validate_device_session_manifest(sealed.manifest)

    def test_final_progress_bytes_match_manifest_artifact_bytes_including_audio(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            recorder, _, audios = self.build(root, audio_enabled=True)
            recorder.start()
            self.feed(recorder, 3)
            audios[0].advance_live(sample_count=4_800, bytes_written=47)
            verifying_states: list[dict[str, object]] = []

            sealed = recorder.stop(
                before_publish=lambda: verifying_states.append(
                    deepcopy(recorder.current_recording_state)
                )
            )

            self.assertEqual(len(verifying_states), 1)
            final_state = verifying_states[0]
            self.assertEqual(final_state["state"], "finalizing")
            expected_bytes = manifest_artifact_bytes_total(sealed.manifest, code="artifact_invalid")
            self.assertEqual(final_state["progress"]["bytes_written"], expected_bytes)

    def test_audio_failure_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            recorder, _, audios = self.build(root, audio_enabled=True, audio_fail_stop=True)
            recorder.start()
            self.feed(recorder, 3)
            with self.assertRaises(DeviceRecordingError) as raised:
                recorder.stop()
            self.assertEqual(raised.exception.code, "audio_failed")
            self.assertNotEqual(recorder.state, "sealed")
            self.assertTrue(audios[0].aborted)

    def test_imu_backpressure_fails_closed(self) -> None:
        block_writer = threading.Event()
        writer_blocked = threading.Event()

        def before_write(role: str, content: bytes) -> None:
            del content
            if role == "frames.index":
                writer_blocked.set()
                if not block_writer.wait(timeout=2):
                    raise TimeoutError("测试没有释放 writer")

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            recorder, _, _ = self.build(
                root,
                queue_capacity=1,
                enqueue_timeout=0.0,
                before_write=before_write,
            )
            recorder.start()
            recorder.submit_frame(frame(0))
            self.assertTrue(writer_blocked.wait(timeout=1))
            self.assertTrue(recorder.submit_frame(frame(1)))
            with self.assertRaises(DeviceRecordingError) as raised:
                recorder.submit_imu(imu_observation())
            self.assertEqual(raised.exception.code, "imu_backpressure")
            block_writer.set()
            with self.assertRaises(DeviceRecordingError) as stopped:
                recorder.stop()
            self.assertEqual(stopped.exception.code, "imu_backpressure")


if __name__ == "__main__":
    unittest.main()

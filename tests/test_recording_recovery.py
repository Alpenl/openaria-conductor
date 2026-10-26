"""Independent prefix recovery, including crash and media corruption boundaries."""

import hashlib
import json
import os
import tempfile
import unittest
import wave
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import patch

import rp_ylx.recording.device_session as session_module
import rp_ylx.recording.recovery as recovery_module
from rp_ylx.audio_clock import audio_clock_report
from rp_ylx.camera import FrameObservation, StereoFrame
from rp_ylx.recording.coordinator import (
    CaptureCoordinator,
    CoordinatorConfig,
    initialize_capture_volume,
)
from rp_ylx.recording.device_session import (
    json_bytes,
    validate_device_session_directory,
    write_json_atomic,
)
from rp_ylx.recording.recovery import _frames, _write_records, recover_device_session
from tests import test_split_eye_recording as fixtures


class RecordingRecoveryTests(unittest.TestCase):
    def test_recovery_manifest_uses_lossless_compact_serialization(self):
        with tempfile.TemporaryDirectory() as temporary:
            recorder = self.recording(Path(temporary))
            recovered = recover_device_session(recorder.partial_path)
            self.assertIsNotNone(recovered)
            raw = (recovered.path / "manifest.json").read_bytes()
            self.assertEqual(raw, json_bytes(json.loads(raw)))
            validate_device_session_directory(recovered.path)

    def test_recovery_readback_proof_rejects_change_during_hash(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            path = root / "artifact.bin"
            path.write_bytes(b"original")
            metadata = path.stat()
            finalize = recovery_module._finalize_artifact
            verified = {}

            def mutate_after_read(*args, **kwargs):
                result = finalize(*args, **kwargs)
                # Visible size changes must be rejected at the readback
                # boundary. Equal-stat corruption is checked at publication
                # by test_artifact_content_checks, without timing assumptions.
                path.write_bytes(b"modified length")
                os.utime(path, ns=(metadata.st_atime_ns, metadata.st_mtime_ns))
                return result

            with (
                patch.object(recovery_module, "_finalize_artifact", side_effect=mutate_after_read),
                self.assertRaisesRegex(ValueError, "changed during independent readback"),
            ):
                recovery_module._artifact(
                    root, path.name, "test", "application/octet-stream", verified=verified
                )
            self.assertEqual(verified, {})

    def test_recovery_avoids_rereads_and_explicit_validation_reads_current_content(self):
        with tempfile.TemporaryDirectory() as temporary:
            recorder = self.recording(Path(temporary))
            with (
                patch.object(
                    recovery_module, "_finalize_artifact", wraps=recovery_module._finalize_artifact
                ) as reads,
                patch.object(
                    session_module, "_verify_artifact_fd", wraps=session_module._verify_artifact_fd
                ) as rereads,
            ):
                recovered = recover_device_session(recorder.partial_path)
            self.assertIsNotNone(recovered)
            paths = [call.args[0] for call in reads.call_args_list]
            self.assertIn(recorder.partial_path / "video/left_00000.mp4", paths)
            self.assertIn(recorder.partial_path / "recovery/imu.ndjson", paths)
            self.assertEqual(rereads.call_count, 0)
            # A later standalone validation also checks current bytes.
            with patch.object(
                session_module, "_verify_artifact_fd", wraps=session_module._verify_artifact_fd
            ) as later:
                validate_device_session_directory(recovered.path)
            self.assertGreater(later.call_count, 0)

    def test_recovery_rejects_changes_after_independent_readback(self):
        for mutation in ("same_size_restored_mtime", "replacement", "symlink"):
            with self.subTest(mutation=mutation), tempfile.TemporaryDirectory() as temporary:
                recorder = self.recording(Path(temporary))
                partial = recorder.partial_path
                target = partial / "video/left_00000.mp4"
                original = target.read_bytes()
                metadata = target.stat()
                validate = recovery_module.inspect_device_session_directory

                def mutate_then_validate(
                    *args,
                    mutation=mutation,
                    target=target,
                    original=original,
                    metadata=metadata,
                    validate=validate,
                    **kwargs,
                ):
                    if mutation == "same_size_restored_mtime":
                        target.write_bytes(b"x" * len(original))
                        os.utime(target, ns=(metadata.st_atime_ns, metadata.st_mtime_ns))
                    elif mutation == "replacement":
                        alternate = target.with_suffix(".replaced")
                        alternate.write_bytes(b"x" * len(original))
                        os.replace(alternate, target)
                        os.utime(target, ns=(metadata.st_atime_ns, metadata.st_mtime_ns))
                    else:
                        alternate = target.with_suffix(".original")
                        target.rename(alternate)
                        target.symlink_to(alternate.name)
                    return validate(*args, **kwargs)

                with (
                    patch.object(
                        recovery_module,
                        "inspect_device_session_directory",
                        side_effect=mutate_then_validate,
                    ),
                    self.assertRaises((session_module.DeviceRecordingError, ValueError)),
                ):
                    recover_device_session(partial)
                self.assertTrue(partial.exists())
                self.assertFalse((partial.parent / recorder._plan.session_id).exists())

    def test_recovery_hardlinked_artifact_is_not_published(self):
        with tempfile.TemporaryDirectory() as temporary:
            recorder = self.recording(Path(temporary))
            target = recorder.partial_path / "video/left_00000.mp4"
            os.link(target, target.with_suffix(".link"))
            try:
                recovered = recover_device_session(recorder.partial_path)
            except (session_module.DeviceRecordingError, ValueError):
                recovered = None
            self.assertIsNone(recovered)
            self.assertTrue(recorder.partial_path.exists())
            self.assertFalse((recorder.partial_path.parent / recorder._plan.session_id).exists())

    def test_failed_imu_spool_preserves_prior_output_and_releases_temporary_space(self):
        for failure in ("iterator", "interrupt", "short_write", "fsync", "replace"):
            with self.subTest(failure=failure), tempfile.TemporaryDirectory() as temporary:
                path = Path(temporary) / "imu.ndjson"
                prior = b"prior verified derived index\n"
                path.write_bytes(prior)
                rows = [{"sample": index} for index in range(8)]
                fdopen = os.fdopen

                class ShortOutput:
                    def __init__(self, descriptor, mode, opener=fdopen):
                        self.stream = opener(descriptor, mode)

                    def __enter__(self):
                        return self

                    def __exit__(self, *args):
                        self.stream.close()

                    def __getattr__(self, name):
                        return getattr(self.stream, name)

                    def write(self, payload):
                        return self.stream.write(payload[:-1])

                def interrupted_rows(items=rows, fault=failure):
                    yield from items[:3]
                    if fault == "interrupt":
                        raise KeyboardInterrupt("cancelled")
                    raise OSError("no space")

                if failure in {"iterator", "interrupt"}:
                    error_type = KeyboardInterrupt if failure == "interrupt" else OSError
                    with self.assertRaises(error_type):
                        _write_records(path, interrupted_rows())
                else:
                    target = {
                        "short_write": "os.fdopen",
                        "fsync": "os.fsync",
                        "replace": "os.replace",
                    }[failure]
                    options = (
                        {"new": ShortOutput}
                        if failure == "short_write"
                        else {"side_effect": OSError("storage failure")}
                    )
                    with (
                        patch("rp_ylx.recording.recovery." + target, **options),
                        self.assertRaises(OSError),
                    ):
                        _write_records(path, rows)
                self.assertEqual(path.read_bytes(), prior)
                self.assertEqual(list(path.parent.glob(".imu.ndjson.*.tmp")), [])
                self.assertEqual(_write_records(path, iter(rows)), len(rows))
                self.assertEqual(path.read_bytes(), b"".join(json_bytes(row) for row in rows))

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
        self.assertIsNone(recorder._writer_error, "recovery fixture writer failed")
        self.assertEqual(recorder._frames_written, frames)
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
            recording_path = recorder.partial_path / "recording.json"
            recording = json.loads(recording_path.read_bytes())
            recording["storage"]["volume_id"] = volume_id
            write_json_atomic(recording_path, recording)
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
                    manifest = json.loads(representation.read())
                    representation.close()
                    receipt = manifest["logs"][0]
                    representation = coordinator.open_verified_artifact(
                        recorder._plan.session_id, receipt["artifact_id"], "v4"
                    )
                    self.assertEqual(json.loads(representation.read())["saved_frames"], 30)
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

    def test_streamed_recovery_preserves_canonical_audited_frames_and_boundaries(self):
        with tempfile.TemporaryDirectory() as temporary:
            recorder = self.recording(Path(temporary))
            root = recorder.partial_path
            rows = [json.loads(line) for line in (root / "frames.ndjson").read_bytes().splitlines()]
            for index, row in enumerate(rows):
                row["schema"] = "ylx.frame-index.v2"
                row["timestamp_audit"] = {
                    "schema": "openaria.frame-timestamp-audit.v1",
                    "camera_counter_raw": index,
                    "timestamp_clock": "v4l2_monotonic",
                    "host_dequeue_monotonic_ns": row["host_monotonic_ns"] + 3000,
                }
                # Non-uniform capture times make each recovered boundary observable.
                row["host_monotonic_ns"] += index * 1000
            original = b"".join((json.dumps(row) + "\n").encode() for row in rows)
            (root / "frames.ndjson").write_bytes(original)
            expected = b"".join(json_bytes(row) for row in rows[:30])

            recovered = recover_device_session(root)

            self.assertEqual((recovered.path / "recovery/frames.ndjson").read_bytes(), expected)
            self.assertEqual((recovered.path / "frames.ndjson").read_bytes(), original)
            for index, segment in enumerate(recovered.manifest["video"]["segments"]):
                expected_start = (
                    rows[index * 3]["host_monotonic_ns"] - recorder._started_monotonic_ns
                ) / 1e9
                self.assertEqual(segment["start_time_seconds"], expected_start)
            validate_device_session_directory(recovered.path)

    def test_late_malformed_index_rolls_back_the_entire_unfinished_segment(self):
        for failure in ("json", "sequence", "timestamp", "segment_frame", "unterminated"):
            with self.subTest(failure=failure), tempfile.TemporaryDirectory() as temporary:
                recorder = self.recording(Path(temporary))
                path = recorder.partial_path / "frames.ndjson"
                lines = path.read_bytes().splitlines(keepends=True)
                expected = b"".join(json_bytes(json.loads(line)) for line in lines[:27])
                bad = json.loads(lines[28])
                if failure == "json":
                    lines[28] = b'{"broken":\n'
                elif failure == "unterminated":
                    lines = lines[:28] + [lines[28].rstrip(b"\n")]
                else:
                    if failure == "sequence":
                        bad["source_sequence"] += 2
                    elif failure == "timestamp":
                        bad["host_monotonic_ns"] = json.loads(lines[27])["host_monotonic_ns"]
                    else:
                        bad["segment_frame"] += 1
                    lines[28] = json_bytes(bad)
                original = b"".join(lines)
                path.write_bytes(original)

                recovered = recover_device_session(recorder.partial_path)

                self.assertEqual(recovered.manifest["frames"]["count"], 27)
                self.assertEqual((recovered.path / "recovery/frames.ndjson").read_bytes(), expected)
                self.assertEqual((recovered.path / "frames.ndjson").read_bytes(), original)
                self.assertEqual(list(recovered.path.glob(".recovery-frames.*.tmp")), [])

    def test_unencodable_frame_is_rejected_only_if_its_segment_is_complete(self):
        for complete in (False, True):
            with self.subTest(complete=complete), tempfile.TemporaryDirectory() as temporary:
                recorder = self.recording(Path(temporary))
                root = recorder.partial_path
                path = root / "frames.ndjson"
                lines = path.read_bytes().splitlines(keepends=True)
                expected = b"".join(json_bytes(json.loads(line)) for line in lines[:27])
                bad = json.loads(lines[27])
                bad["extension"] = "\ud800"
                lines[27] = (json.dumps(bad) + "\n").encode()
                original = b"".join(lines if complete else lines[:29])
                path.write_bytes(original)
                recovery = root / "recovery"
                recovery.mkdir()
                prior = b"previous derived index\n"
                (recovery / "frames.ndjson").write_bytes(prior)

                if complete:
                    with self.assertRaises(UnicodeEncodeError):
                        recover_device_session(root)
                    self.assertEqual((recovery / "frames.ndjson").read_bytes(), prior)
                    self.assertFalse((root / "manifest.json").exists())
                else:
                    recovered = recover_device_session(root)
                    root = recovered.path
                    self.assertEqual(recovered.manifest["frames"]["count"], 27)
                    self.assertEqual((root / "recovery/frames.ndjson").read_bytes(), expected)
                self.assertEqual((root / "frames.ndjson").read_bytes(), original)
                self.assertEqual(list(root.glob(".recovery-frames.*.tmp")), [])

    def test_failed_frame_spool_keeps_prior_index_and_cleans_temporary_output(self):
        for failure in ("short_write", "write", "fsync"):
            with self.subTest(failure=failure), tempfile.TemporaryDirectory() as temporary:
                recorder = self.recording(Path(temporary))
                root = recorder.partial_path
                original = (root / "frames.ndjson").read_bytes()
                recovery = root / "recovery"
                recovery.mkdir()
                prior = b"prior derived index remains until replacement succeeds\n"
                (recovery / "frames.ndjson").write_bytes(prior)
                segments = json.loads((root / "segments.json").read_bytes())["segments"]
                fdopen = os.fdopen

                class FailingOutput:
                    def __init__(self, descriptor, mode, opener=fdopen, fault=failure):
                        self.stream = opener(descriptor, mode)
                        self.fault = fault
                        self.writes = 0

                    def __enter__(self):
                        return self

                    def __exit__(self, *args):
                        self.stream.close()

                    def __getattr__(self, name):
                        return getattr(self.stream, name)

                    def write(self, payload):
                        self.writes += 1
                        if self.writes == 5:
                            if self.fault == "write":
                                raise OSError("no space")
                            if self.fault == "short_write":
                                return self.stream.write(payload[:-1])
                        return self.stream.write(payload)

                with patch("rp_ylx.recording.recovery.os.fdopen", FailingOutput):
                    if failure == "fsync":
                        with (
                            patch(
                                "rp_ylx.recording.recovery.os.fsync", side_effect=OSError("sync")
                            ),
                            self.assertRaises(OSError),
                        ):
                            _frames(
                                root, recorder._plan, segments, 2, recorder._started_monotonic_ns
                            )
                    else:
                        with self.assertRaises(OSError):
                            _frames(
                                root, recorder._plan, segments, 2, recorder._started_monotonic_ns
                            )
                self.assertEqual((root / "frames.ndjson").read_bytes(), original)
                self.assertEqual((recovery / "frames.ndjson").read_bytes(), prior)
                self.assertEqual(list(root.glob(".recovery-frames.*.tmp")), [])
                self.assertIsNotNone(recover_device_session(root))

    def test_first_segment_failure_does_not_claim_a_usable_session(self):
        with tempfile.TemporaryDirectory() as temporary:
            recorder = self.recording(Path(temporary), frames=2)
            recorder.fail("encoder_failed", "first pair never closed")
            self.assertIsNone(recorder.recovered_session)
            self.assertTrue(recorder.partial_path.is_dir())
            self.assertFalse((recorder.partial_path / "manifest.json").exists())
            self.assertFalse((recorder.partial_path / "recovery/frames.ndjson").exists())
            self.assertEqual(list(recorder.partial_path.glob(".recovery-frames.*.tmp")), [])

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
        self._check_audio_prefix(False)

    def test_raw_audio_prefix_keeps_pcm_and_piecewise_clock_through_slew(self):
        self._check_audio_prefix(True)

    def test_journal_audio_prefix_keeps_pcm_and_piecewise_clock(self):
        self._check_audio_prefix(True, journal=True)

    def _check_audio_prefix(self, raw_clock, journal=False):
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
            anchors = [
                [n, start + round(n * 1e9 / rate)] for n in range(1024, 2 * rate + 1024, 4800)
            ]
            if raw_clock:
                anchors = [
                    [
                        n,
                        t + 1_000_000_000,
                        t - round(min(max(n / rate - 0.3, 0), 0.4) * 500_000),
                        t + 1_000_000_000,
                        t + 1_000_000_000,
                    ]
                    for n, t in anchors
                ]
            write_json_atomic(
                root / "audio/checkpoint.json",
                {
                    "sample_count": 2 * rate,
                    "thread_started_monotonic_ns": start,
                    "period_frames": 1024,
                    "buffer_frames": 8192,
                    "schema": "openaria.audio-checkpoint.v2"
                    if raw_clock
                    else "openaria.audio-checkpoint.v1",
                    "anchors": anchors,
                    "queue_capacity_frames": 786432,
                    "queue_peak_frames": 1024,
                    "max_write_ns": 400000,
                    "segments": [
                        {"path": "audio/audio_00000.wav", "start_sample": 0, "end_sample": 2 * rate}
                    ],
                },
            )
            if journal:
                from tests.test_audio_journal import publish

                head = json.loads((root / "audio/checkpoint.json").read_bytes())
                head["segments"][0]["index"] = 0
                published = publish(
                    root, [{"sequence": 0, "anchors": anchors, "segments": head["segments"]}]
                )
                published["thread_started_monotonic_ns"] = head["thread_started_monotonic_ns"]
                write_json_atomic(root / "audio/checkpoint.json", published)
                # Uncommitted bytes after a simulated crash must not be read.
                with (root / "audio/checkpoint.ndjson").open("ab") as output:
                    output.write(b'{"partial":')
            recovered = recover_device_session(root)
            audio = recovered.manifest["audio"]
            report = audio_clock_report(audio, recovered.manifest["time"]["duration_seconds"])
            self.assertEqual(report["continuity"], "verified")
            if raw_clock:
                self.assertEqual(audio["capture_clock"]["schema"], "openaria.audio-clock.v2")
                self.assertEqual(report["mapping"], "piecewise-linear")
            self.assertLess(audio["sample_count"], 2 * rate)
            with wave.open(
                str(recovered.path / audio["segments"][0]["artifact"]["path"]), "rb"
            ) as stream:
                self.assertEqual(
                    stream.readframes(audio["sample_count"]), pcm[: audio["sample_count"] * 4]
                )
            self.assertEqual((recovered.path / "audio/audio_00000.wav").read_bytes()[44:], pcm)

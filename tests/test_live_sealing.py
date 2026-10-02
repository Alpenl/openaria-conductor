import hashlib
import json
import os
import tempfile
import threading
import time
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

from rp_ylx.recording.audit import capture_audit
from rp_ylx.recording.device_session import DeviceRecordingError, validate_device_session_directory
from rp_ylx.recording.live_seal import LiveSealing, _Journal
from rp_ylx.recording.storage import BLOCK_BYTES, open_metadata
from tests import test_split_eye_recording as fixtures


class LiveSealingTests(unittest.TestCase):
    def audio_sealing(self, root):
        # Exercise checkpoint polling without starting audit/compaction workers.
        sealing = object.__new__(LiveSealing)
        sealing.root = root
        sealing.prepared = {}
        sealing._audio_checkpoint_identity = None
        sealing.finished = threading.Event()
        sealing.cancelled = threading.Event()
        return sealing

    def publish_checkpoint(self, root, indices):
        directory = root / "audio"
        directory.mkdir(exist_ok=True)
        segments = []
        for index in indices:
            relative = f"audio/audio_{index:05d}.wav"
            path = root / relative
            if not path.exists():
                path.write_bytes(f"closed audio segment {index}".encode())
            segments.append({"path": relative, "index": index})
        temporary = directory / "checkpoint.tmp"
        temporary.write_text(json.dumps({"segments": segments}))
        temporary.replace(directory / "checkpoint.json")

    def compact_audio(self, source):
        target = source.with_suffix(".flac")
        target.write_bytes(b"compacted audio")
        return target, {"codec": "flac"}

    def test_audio_skips_unchanged_checkpoint_but_forces_final_read(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            sealing = self.audio_sealing(root)
            sealing._audio()  # The recorder may not have published audio yet.
            self.publish_checkpoint(root, [0])
            with (
                patch("json.loads", wraps=json.loads) as decode,
                patch(
                    "rp_ylx.recording.live_seal.compact_audio", side_effect=self.compact_audio
                ) as compact,
            ):
                for _ in range(20):
                    sealing._audio()
                self.assertEqual(decode.call_count, 1)
                self.assertEqual(compact.call_count, 1)
                sealing._audio(force=True)
                self.assertEqual(decode.call_count, 2)
                self.assertEqual(compact.call_count, 1)

    def test_audio_reads_replacement_with_same_size_and_restored_mtime(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            sealing = self.audio_sealing(root)
            self.publish_checkpoint(root, [0])
            checkpoint = root / "audio/checkpoint.json"
            metadata = checkpoint.stat()
            with patch("rp_ylx.recording.live_seal.compact_audio", side_effect=self.compact_audio):
                sealing._audio()
                self.publish_checkpoint(root, [1])
                os.utime(checkpoint, ns=(metadata.st_atime_ns, metadata.st_mtime_ns))
                self.assertEqual(checkpoint.stat().st_size, metadata.st_size)
                self.assertNotEqual(checkpoint.stat().st_ino, metadata.st_ino)
                sealing._audio()
            self.assertEqual(
                set(sealing.prepared), {"audio/audio_00000.wav", "audio/audio_00001.wav"}
            )

    def test_audio_reads_checkpoint_when_only_ctime_changes(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            sealing = self.audio_sealing(root)
            self.publish_checkpoint(root, [])
            checkpoint = root / "audio/checkpoint.json"
            with patch("json.loads", wraps=json.loads) as decode:
                sealing._audio()
                metadata = checkpoint.stat()
                # Changing the same inode and restoring mtime must invalidate
                # the cache. Model ctime explicitly so this does not depend on
                # the filesystem's timestamp resolution during a fast test.
                changed = SimpleNamespace(
                    st_dev=metadata.st_dev,
                    st_ino=metadata.st_ino,
                    st_mode=metadata.st_mode,
                    st_nlink=metadata.st_nlink,
                    st_size=metadata.st_size,
                    st_mtime_ns=metadata.st_mtime_ns,
                    st_ctime_ns=metadata.st_ctime_ns + 1,
                )
                with patch.object(Path, "stat", return_value=changed):
                    sealing._audio()
                self.assertEqual(decode.call_count, 2)

    def test_audio_publication_during_read_is_seen_on_next_poll(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            sealing = self.audio_sealing(root)
            self.publish_checkpoint(root, [0])
            read_bytes = Path.read_bytes

            def publish_after_read(path):
                payload = read_bytes(path)
                self.publish_checkpoint(root, [0, 1])
                return payload

            with patch("rp_ylx.recording.live_seal.compact_audio", side_effect=self.compact_audio):
                with patch.object(Path, "read_bytes", publish_after_read):
                    sealing._audio()
                self.assertEqual(set(sealing.prepared), {"audio/audio_00000.wav"})
                sealing._audio()
                self.assertEqual(
                    set(sealing.prepared), {"audio/audio_00000.wav", "audio/audio_00001.wav"}
                )

    def test_audio_invalid_checkpoint_is_not_cached(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            sealing = self.audio_sealing(root)
            self.publish_checkpoint(root, [])
            checkpoint = root / "audio/checkpoint.json"
            checkpoint.write_text('{"segments": [{"index": 0, "path": "../outside.wav"}]}')
            for _ in range(2):
                with self.assertRaisesRegex(
                    ValueError, "invalid incremental audio checkpoint path"
                ):
                    sealing._audio()
            self.assertIsNone(sealing._audio_checkpoint_identity)

    def test_compaction_includes_tail_published_before_finish(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for name in ("frames.ndjson", "imu.ndjson"):
                (root / name).write_bytes(b"journal tail\n")
            sealing = self.audio_sealing(root)
            self.publish_checkpoint(root, [0])
            audio = sealing._audio

            def stop_after_poll(*, force=False):
                audio(force=force)
                if not force:
                    self.publish_checkpoint(root, [0, 1])
                    sealing.finished.set()

            with (
                patch.object(sealing, "_audio", side_effect=stop_after_poll) as poll,
                patch("rp_ylx.recording.live_seal.compact_audio", side_effect=self.compact_audio),
            ):
                sealing._compact()
            self.assertEqual(poll.call_args_list[-1].kwargs, {"force": True})
            self.assertEqual(
                set(sealing.prepared),
                {"frames.ndjson", "imu.ndjson", "audio/audio_00000.wav", "audio/audio_00001.wav"},
            )

    def test_progress_does_not_sync_coordinator_for_each_artifact(self):
        with tempfile.TemporaryDirectory() as directory:
            recorder, _, _ = fixtures.SplitEyeRecordingTest().build(Path(directory))
            recorder.start()
            sink = recorder._state_sink = Mock()
            try:
                with patch(
                    "rp_ylx.recording.device_session.write_json_atomic",
                    side_effect=AssertionError("progress must not add disk barriers"),
                ):
                    for completed in range(1, 123):
                        recorder._verification_progress(completed, 123)
                sink.assert_not_called()
                self.assertEqual(
                    recorder.current_recording_state["progress"]["verification"],
                    {"completed": 122, "total": 123},
                )
                recorder._persist_state("verifying")
                sink.assert_called_once()
            finally:
                recorder.abort()

    def test_manifest_cache_reuses_only_exact_bytes_and_isolates_mutation(self):
        from rp_ylx.api import downloads

        with tempfile.TemporaryDirectory() as directory:
            fixture = fixtures.SplitEyeRecordingTest()
            recorder, _, _ = fixture.build(Path(directory))
            recorder.start()
            fixture.feed(recorder, 3)
            sealed = recorder.stop()
            downloads._clear_validated_manifest_cache_for_tests()
            validate = downloads._decode_and_validate_manifest
            with patch.object(downloads, "_decode_and_validate_manifest", wraps=validate) as run:
                one = downloads.validated_device_session_payload(
                    sealed.manifest_bytes, sealed.path.name
                )
                one["sealed"] = False
                two = downloads.validated_device_session_payload(
                    sealed.manifest_bytes, sealed.path.name
                )
                self.assertTrue(two["sealed"])
                self.assertEqual(run.call_count, 1)
                with self.assertRaises(downloads.ArtifactAccessError):
                    downloads.validated_device_session_payload(
                        json.dumps(one).encode(), sealed.path.name
                    )
                self.assertEqual(run.call_count, 2)

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

    def test_incremental_journal_handles_many_small_appends_across_blocks(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            path = root / "frames.ndjson"
            path.touch()
            journal = _Journal(path)
            payload = bytes(range(256)) * (2 * BLOCK_BYTES // 256 + 1)
            try:
                with path.open("ab") as producer:
                    for offset in range(0, len(payload), 4093):
                        producer.write(payload[offset : offset + 4093])
                        producer.flush()
                        journal.pump()
                        self.assertLess(len(journal.pending), BLOCK_BYTES)
                prepared = journal.finish()
                prepared.use(
                    root,
                    {
                        "path": path.name,
                        "bytes": len(payload),
                        "sha256": hashlib.sha256(payload).hexdigest(),
                    },
                )
                with open_metadata(prepared.target) as decoded:
                    self.assertEqual(decoded.read(), payload)
            finally:
                journal.close()

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

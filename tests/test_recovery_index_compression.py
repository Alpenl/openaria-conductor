"""Compressed interrupted-take indexes must preserve exact committed prefixes."""

import hashlib
import json
import os
import tempfile
import unittest
from dataclasses import asdict, replace
from pathlib import Path
from unittest.mock import patch

import rp_ylx.recording.recovery as recovery
from rp_ylx.recording.device_session import json_bytes, validate_device_session_directory
from rp_ylx.recording.encoding import RecordingEncoding
from rp_ylx.recording.recovery_index import RecoveryIndex
from rp_ylx.recording.storage import BLOCK_BYTES, open_metadata
from tests import test_recording_recovery as fixtures
from tests import test_recovery_publish_crash as crash_fixtures


class RecoveryIndexCompressionTests(unittest.TestCase):
    def recording(self, root):
        recorder = fixtures.RecordingRecoveryTests().recording(root)
        capture = recorder.partial_path / "capture.json"
        value = json.loads(capture.read_bytes())
        value["config"]["lossless_storage"] = True
        value["config"]["recording_encoding"] = asdict(RecordingEncoding(gop_frames=3))
        capture.write_bytes(json_bytes(value))
        return recorder

    def decoded(self, path):
        with open_metadata(path) as stream:
            return stream.read()

    def test_complete_recovery_with_empty_imu(self):
        with tempfile.TemporaryDirectory() as temporary:
            recorder = self.recording(Path(temporary))
            root = recorder.partial_path
            original = (root / "frames.ndjson").read_bytes()
            expected = b"".join(json_bytes(json.loads(row)) for row in original.splitlines()[:30])
            result = recovery.recover_device_session(root)
            self.assertEqual(result.manifest["schema"], "ylx.device-session.v4")
            self.assertEqual(result.manifest["frames"]["count"], 30)
            self.assertEqual((result.path / "frames.ndjson").read_bytes(), original)
            for key, content in (("frames", expected), ("imu", b"")):
                artifact = result.manifest[key]["artifact"]
                self.assertEqual(artifact["media_type"], "application/zstd")
                self.assertEqual(self.decoded(result.path / artifact["path"]), content)
                encoding = artifact["storage_encoding"]
                self.assertEqual(encoding["uncompressed_bytes"], len(content))
                self.assertEqual(
                    encoding["uncompressed_sha256"], hashlib.sha256(content).hexdigest()
                )
            validate_device_session_directory(result.path)

    def test_torn_frame_segment_and_corrupt_imu_preserve_only_contiguous_prefix(self):
        with tempfile.TemporaryDirectory() as temporary:
            recorder = self.recording(Path(temporary))
            root = recorder.partial_path
            rows = (root / "frames.ndjson").read_bytes().splitlines(keepends=True)
            original = b"".join(rows[:28]) + rows[28][:25]
            (root / "frames.ndjson").write_bytes(original)
            sample = json_bytes(
                {
                    "session_id": recorder._plan.session_id,
                    "host_monotonic_ns": recorder._started_monotonic_ns + 20_000_000,
                }
            )
            (root / "imu.ndjson").write_bytes(sample + b"{broken}\n" + sample)
            result = recovery.recover_device_session(root)
            self.assertEqual(result.manifest["frames"]["count"], 27)
            self.assertEqual(result.manifest["imu"]["sample_count"], 1)
            self.assertEqual((result.path / "frames.ndjson").read_bytes(), original)
            expected = b"".join(json_bytes(json.loads(row)) for row in rows[:27])
            self.assertEqual(self.decoded(result.path / "recovery/frames.ndjson.zst"), expected)
            self.assertEqual(self.decoded(result.path / "recovery/imu.ndjson.zst"), sample)
            validate_device_session_directory(result.path)

    def test_rollback_discards_already_flushed_tail_and_restores_logical_digest(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "frames.zst"
            writer = RecoveryIndex(True)
            prefix = b"committed\n" * (BLOCK_BYTES // 5)
            with path.open("wb") as output:
                writer.write(output, prefix)
                checkpoint = writer.checkpoint(output)
                writer.write(output, b"discarded\n" * BLOCK_BYTES)
                self.assertLess(len(writer.pending), BLOCK_BYTES)
                writer.rollback(output, checkpoint)
                writer.finish(output)
            encoding = writer.verify(path)
            self.assertEqual(self.decoded(path), prefix)
            self.assertEqual(encoding["uncompressed_sha256"], hashlib.sha256(prefix).hexdigest())
            self.assertEqual(encoding["uncompressed_bytes"], len(prefix))

    def test_empty_and_multiblock_record_roundtrip(self):
        for rows in ([], [{"large": "abc" * BLOCK_BYTES}, {"unicode": "汉字"}]):
            with self.subTest(empty=not rows), tempfile.TemporaryDirectory() as temporary:
                path = Path(temporary) / "imu.ndjson.zst"
                encoding = {}
                count = recovery._write_records(path, rows, storage_encoding=encoding)
                expected = b"".join(json_bytes(row) for row in rows)
                self.assertEqual(count, len(rows))
                self.assertEqual(self.decoded(path), expected)
                self.assertEqual(encoding["uncompressed_bytes"], len(expected))
                self.assertLess(path.stat().st_size, max(100, len(expected) // 10))

    def test_failed_compressed_spool_preserves_previous_result_and_is_retryable(self):
        for failure in ("iterator", "interrupt", "short_write", "fsync", "replace", "fdopen"):
            with self.subTest(failure=failure), tempfile.TemporaryDirectory() as temporary:
                path = Path(temporary) / "imu.ndjson.zst"
                prior = b"prior verified artifact"
                path.write_bytes(prior)
                rows = [{"sample": index} for index in range(8)]
                original_write = RecoveryIndex._write
                descriptor_count = len(os.listdir("/proc/self/fd"))
                retained = []
                encoding = {}

                def failing_rows(items=rows, fault=failure):
                    yield from items[:3]
                    if fault == "interrupt":
                        raise KeyboardInterrupt("cancelled")
                    raise OSError("ENOSPC")

                def short_write(writer, output, payload, original_write=original_write):
                    class ShortOutput:
                        def write(self, value):
                            return output.write(value[:-1])

                    return original_write(writer, ShortOutput(), payload)

                if failure in ("iterator", "interrupt"):
                    try:
                        recovery._write_records(path, failing_rows(), storage_encoding=encoding)
                    except (OSError, KeyboardInterrupt) as error:
                        retained.append(error)
                else:
                    target = "RecoveryIndex._write" if failure == "short_write" else "os." + failure
                    options = (
                        {"new": short_write}
                        if failure == "short_write"
                        else {"side_effect": OSError("storage failure")}
                    )
                    with patch("rp_ylx.recording.recovery." + target, **options):
                        try:
                            recovery._write_records(path, rows, storage_encoding=encoding)
                        except OSError as error:
                            retained.append(error)
                self.assertEqual(len(retained), 1)
                self.assertEqual(len(os.listdir("/proc/self/fd")), descriptor_count)
                self.assertEqual(path.read_bytes(), prior)
                self.assertEqual(encoding, {})
                self.assertEqual(list(path.parent.glob(".*.tmp")), [])
                self.assertEqual(
                    recovery._write_records(path, rows, storage_encoding={}), len(rows)
                )
                self.assertEqual(self.decoded(path), b"".join(json_bytes(row) for row in rows))

    def test_missing_checksum_or_mutated_encoded_bytes_cannot_replace_previous_index(self):
        for corruption in ("truncate_checksum", "flip", "empty"):
            with self.subTest(corruption=corruption), tempfile.TemporaryDirectory() as temporary:
                path = Path(temporary) / "imu.ndjson.zst"
                path.write_bytes(b"prior")
                verify = RecoveryIndex.verify

                def damage(writer, temporary, corruption=corruption, verify=verify):
                    data = temporary.read_bytes()
                    if corruption == "truncate_checksum":
                        data = data[:-1]
                    elif corruption == "flip":
                        data = data[:-1] + bytes([data[-1] ^ 1])
                    else:
                        data = b""
                    temporary.write_bytes(data)
                    return verify(writer, temporary)

                with patch.object(RecoveryIndex, "verify", damage), self.assertRaises(ValueError):
                    recovery._write_records(path, [], storage_encoding={})
                self.assertEqual(path.read_bytes(), b"prior")
                self.assertEqual(list(path.parent.glob(".*.tmp")), [])

    def test_corruption_after_derived_index_readback_blocks_manifest_publication(self):
        with tempfile.TemporaryDirectory() as temporary:
            recorder = self.recording(Path(temporary))
            root = recorder.partial_path
            validate = recovery.inspect_device_session_directory

            def damage_then_validate(*args, **kwargs):
                path = root / "recovery/frames.ndjson.zst"
                state = path.stat()
                content = path.read_bytes()
                path.write_bytes(content[:-1] + bytes([content[-1] ^ 1]))
                os.utime(path, ns=(state.st_atime_ns, state.st_mtime_ns))
                return validate(*args, **kwargs)

            with (
                patch.object(recovery, "inspect_device_session_directory", damage_then_validate),
                self.assertRaises((ValueError, recovery.DeviceRecordingError)),
            ):
                recovery.recover_device_session(root)
            self.assertTrue(root.exists())
            self.assertFalse((root.parent / recorder._plan.session_id).exists())
            result = recovery.recover_device_session(root)
            validate_device_session_directory(result.path)

    def test_legacy_capture_without_encoding_keeps_original_manifest_contract(self):
        with tempfile.TemporaryDirectory() as temporary:
            recorder = self.recording(Path(temporary))
            capture = recorder.partial_path / "capture.json"
            value = json.loads(capture.read_bytes())
            value["config"]["recording_encoding"] = None
            capture.write_bytes(json_bytes(value))
            result = recovery.recover_device_session(recorder.partial_path)
            self.assertEqual(result.manifest["schema"], "ylx.device-session.v2")
            for name in ("frames", "imu"):
                self.assertEqual(
                    result.manifest[name]["artifact"]["media_type"], "application/x-ndjson"
                )
                self.assertNotIn("storage_encoding", result.manifest[name]["artifact"])
            validate_device_session_directory(result.path)

    def test_audio_clock_versions_and_original_pcm_survive_compressed_recovery(self):
        fixture = fixtures.RecordingRecoveryTests()
        fixture.recording = self.recording
        for raw, journal in ((False, False), (True, False), (True, True)):
            with self.subTest(raw=raw, journal=journal):
                fixture._check_audio_prefix(raw, journal=journal)

    def test_recovered_compressed_artifacts_remain_downloadable_after_coordinator_restart(self):
        fixture = fixtures.RecordingRecoveryTests()
        fixture.recording = self.recording
        fixture.test_coordinator_restart_publishes_recovery_through_normal_catalog()

    def test_compressed_recovery_survives_publication_crash_boundaries(self):
        cases = (
            "test_archived_controls_survive_crash_before_publication_rename",
            "test_published_prefix_fault_survives_crash_before_coordinator_commit",
            "test_fault_survives_crash_with_all_failure_database_writes_unavailable",
        )
        for case in cases:
            with self.subTest(case=case):
                fixture = crash_fixtures.RecoveryPublishCrashTest()
                fixture.setUp()
                try:
                    fixture.fixture.session_config = replace(
                        fixture.fixture.session_config,
                        lossless_storage=True,
                        recording_encoding=RecordingEncoding(gop_frames=3),
                    )
                    getattr(fixture, case)()
                    manifests = list(
                        (fixture.fixture.mountpoint / "recordings").glob("*/manifest.json")
                    )
                    self.assertEqual(len(manifests), 1)
                    manifest = json.loads(manifests[0].read_bytes())
                    self.assertEqual(manifest["schema"], "ylx.device-session.v4")
                    self.assertEqual(
                        manifest["frames"]["artifact"]["media_type"], "application/zstd"
                    )
                    validate_device_session_directory(manifests[0].parent)
                finally:
                    fixture.doCleanups()

    def test_wrong_valid_compression_payload_fails_independent_logical_readback(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "imu.ndjson.zst"
            path.write_bytes(b"prior")
            import zstandard

            compressor = zstandard.ZstdCompressor(level=1, write_checksum=True)

            class WrongCompressor:
                def compress(self, payload):
                    return compressor.compress(b"wrong\n")

            with (
                patch(
                    "rp_ylx.recording.recovery_index.zstandard.ZstdCompressor",
                    return_value=WrongCompressor(),
                ),
                self.assertRaisesRegex(ValueError, "round-trip mismatch"),
            ):
                recovery._write_records(path, [{"correct": True}], storage_encoding={})
            self.assertEqual(path.read_bytes(), b"prior")
            self.assertEqual(list(path.parent.glob(".*.tmp")), [])

    def test_compressor_allocation_failure_does_not_acquire_output_descriptor(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "imu.ndjson.zst"
            path.write_bytes(b"prior")
            before = len(os.listdir("/proc/self/fd"))
            retained = []
            with patch(
                "rp_ylx.recording.recovery_index.zstandard.ZstdCompressor", side_effect=MemoryError
            ):
                try:
                    recovery._write_records(path, [], storage_encoding={})
                except MemoryError as error:
                    retained.append(error)
            self.assertEqual(len(retained), 1)
            self.assertEqual(len(os.listdir("/proc/self/fd")), before)
            self.assertEqual(path.read_bytes(), b"prior")
            self.assertEqual(list(path.parent.glob(".*.tmp")), [])

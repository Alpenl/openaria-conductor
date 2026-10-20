"""Recovery must retain complete prefixes without unbounded journal reads."""

import hashlib
import io
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from rp_ylx.recording.device_session import (
    json_bytes,
    validate_device_session_directory,
    write_json_atomic,
)
from rp_ylx.recording.recovery import recover_device_session
from tests import test_recording_recovery as fixtures
from tests.test_audio_journal import publish

LIMIT = 1024 * 1024


class RecoveryJournalBoundsTests(unittest.TestCase):
    def recording(self, root):
        return fixtures.RecordingRecoveryTests().recording(root)

    def test_nested_frame_tail_retains_complete_pairs(self):
        with tempfile.TemporaryDirectory() as temporary:
            recorder = self.recording(Path(temporary))
            path = recorder.partial_path / "frames.ndjson"
            rows = path.read_bytes().splitlines(keepends=True)
            original = b"".join(rows[:28]) + b"[" * 10000 + b"0" + b"]" * 10000 + b"\n"
            path.write_bytes(original)
            recovered = recover_device_session(recorder.partial_path)
            self.assertEqual(recovered.manifest["frames"]["count"], 27)
            self.assertEqual((recovered.path / "frames.ndjson").read_bytes(), original)
            validate_device_session_directory(recovered.path)

    def test_oversized_valid_frame_line_is_not_included(self):
        with tempfile.TemporaryDirectory() as temporary:
            recorder = self.recording(Path(temporary))
            path = recorder.partial_path / "frames.ndjson"
            rows = path.read_bytes().splitlines(keepends=True)
            rows[28] = rows[28][:-1] + b" " * LIMIT + b"\n"
            original = b"".join(rows)
            path.write_bytes(original)
            recovered = recover_device_session(recorder.partial_path)
            self.assertEqual(recovered.manifest["frames"]["count"], 27)
            self.assertEqual((recovered.path / "frames.ndjson").read_bytes(), original)
            validate_device_session_directory(recovered.path)

    def test_bad_imu_tail_retains_prior_samples_without_skipping_corruption(self):
        tails = {
            "nested": b"[" * 10000 + b"0" + b"]" * 10000 + b"\n",
            "oversized": b" " * (LIMIT + 1) + b"{}\n",
            "truncated": b'{"session_id":',
            "invalid": b"{broken}\n",
        }
        for name, tail in tails.items():
            with self.subTest(tail=name), tempfile.TemporaryDirectory() as temporary:
                recorder = self.recording(Path(temporary))
                root = recorder.partial_path
                row = {
                    "session_id": recorder._plan.session_id,
                    "host_monotonic_ns": recorder._started_monotonic_ns + 20_000_000,
                }
                prefix = json_bytes(row)
                if name == "oversized":
                    tail = prefix[:-1] + b" " * LIMIT + b"\n"
                if name == "nested":
                    with self.assertRaises(RecursionError):
                        json.loads(tail)
                original = prefix + tail + prefix
                (root / "imu.ndjson").write_bytes(original)
                recovered = recover_device_session(root)
                self.assertEqual(recovered.manifest["frames"]["count"], 30)
                self.assertEqual(recovered.manifest["imu"]["sample_count"], 1)
                self.assertEqual((recovered.path / "recovery/imu.ndjson").read_bytes(), prefix)
                self.assertEqual((recovered.path / "imu.ndjson").read_bytes(), original)
                validate_device_session_directory(recovered.path)

    def test_frame_and_imu_reads_are_bounded_and_stop_before_uncommitted_tail(self):
        with tempfile.TemporaryDirectory() as temporary:
            recorder = self.recording(Path(temporary))
            root = recorder.partial_path
            frames = (root / "frames.ndjson").read_bytes().splitlines(keepends=True)
            streams = {}

            class Guarded(io.BytesIO):
                def __iter__(self):
                    raise AssertionError("unbounded journal iteration")

                def readline(self, size=-1):
                    if not 0 < size <= LIMIT + 1:
                        raise AssertionError("unbounded journal line read")
                    if self.tell() >= len(self.getvalue()):
                        raise AssertionError("read beyond independently committed frame prefix")
                    return super().readline(size)

            opener = Path.open

            def guarded(path, *args, **kwargs):
                if path == root / "frames.ndjson" and args == ("rb",):
                    stream = Guarded(b"".join(frames[:30]))
                elif path == root / "imu.ndjson" and args == ("rb",):
                    stream = Guarded(b" " * (LIMIT + 1))
                else:
                    return opener(path, *args, **kwargs)
                streams[path.name] = stream
                return stream

            with patch.object(Path, "open", guarded):
                recovered = recover_device_session(root)
            self.assertEqual(recovered.manifest["frames"]["count"], 30)
            self.assertEqual(recovered.manifest["imu"]["sample_count"], 0)
            self.assertEqual(set(streams), {"frames.ndjson", "imu.ndjson"})
            validate_device_session_directory(recovered.path)

    def test_complete_line_at_limit_is_accepted(self):
        with tempfile.TemporaryDirectory() as temporary:
            recorder = self.recording(Path(temporary))
            path = recorder.partial_path / "frames.ndjson"
            rows = path.read_bytes().splitlines(keepends=True)
            row = json.loads(rows[0])
            rows[0] = rows[0][:-1] + b" " * (LIMIT - len(rows[0])) + b"\n"
            self.assertEqual(len(rows[0]), LIMIT)
            path.write_bytes(b"".join(rows))
            recovered = recover_device_session(recorder.partial_path)
            self.assertEqual(recovered.manifest["frames"]["count"], 30)
            with (recovered.path / "recovery/frames.ndjson").open("rb") as output:
                self.assertEqual(output.readline(), json_bytes(row))
            validate_device_session_directory(recovered.path)

    def test_optional_metadata_depth_failure_preserves_verified_video(self):
        damaged = b"[" * 10000 + b"0" + b"]" * 10000 + b"\n"
        for name in ("recording.json", "audio/checkpoint.json", "audio/checkpoint.ndjson"):
            with self.subTest(metadata=name), tempfile.TemporaryDirectory() as temporary:
                recorder = self.recording(Path(temporary))
                root = recorder.partial_path
                if name.startswith("audio/"):
                    capture = json.loads((root / "capture.json").read_bytes())
                    capture["config"]["audio_enabled"] = True
                    write_json_atomic(root / "capture.json", capture)
                    (root / "audio").mkdir()
                if name.endswith(".ndjson"):
                    head = publish(root, [{"sequence": 0, "anchors": [], "segments": []}])
                    head["journal_bytes"] = len(damaged)
                    head["journal_sha256"] = hashlib.sha256(damaged).hexdigest()
                    write_json_atomic(root / "audio/checkpoint.json", head)
                (root / name).write_bytes(damaged)
                recovered = recover_device_session(root)
                self.assertEqual(recovered.manifest["frames"]["count"], 30)
                if name.startswith("audio/"):
                    self.assertEqual(recovered.manifest["audio"]["reason"], "recording_interrupted")
                    preserved = recovered.path / name
                else:
                    preserved = recovered.path / "recovery/recording.json"
                self.assertEqual(preserved.read_bytes(), damaged)
                validate_device_session_directory(recovered.path)

    def test_required_metadata_depth_failure_never_publishes(self):
        for name in ("capture.json", "segments.json"):
            with self.subTest(metadata=name), tempfile.TemporaryDirectory() as temporary:
                recorder = self.recording(Path(temporary))
                root = recorder.partial_path
                damaged = b"[" * 10000 + b"0" + b"]" * 10000 + b"\n"
                (root / name).write_bytes(damaged)
                with self.assertRaisesRegex(ValueError, "nesting"):
                    recover_device_session(root)
                self.assertEqual((root / name).read_bytes(), damaged)
                self.assertFalse((root.parent / recorder._plan.session_id).exists())
                self.assertFalse((root / "manifest.json").exists())

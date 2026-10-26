"""Audio inventory must use producer-assigned paths before touching files."""

import tempfile
import threading
import unittest
import wave
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest.mock import patch

from rp_ylx.recording import device_session as d


class AudioInventoryTests(unittest.TestCase):
    def recorder(self, root):
        recorder = object.__new__(d.DeviceSessionRecorder)
        recorder._partial = root
        recorder._artifact_identities = {}
        recorder._verified_artifacts = {}
        return recorder

    def audio(self, root, relative="audio/audio_00000.wav"):
        path = root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        with wave.open(str(path), "wb") as wav:
            wav.setnchannels(2)
            wav.setsampwidth(2)
            wav.setframerate(48000)
            wav.writeframes(b"\x01\x02\x03\x04" * 4)
        return {
            "sample_rate_hz": 48000,
            "channels": 2,
            "sample_format": "S16_LE",
            "sample_count": 4,
            "segments": [
                {
                    "index": 0,
                    "start_sample": 0,
                    "end_sample": 4,
                    "start_time_seconds": 0.0,
                    "end_time_seconds": 4 / 48000,
                    "path": relative,
                }
            ],
        }

    def test_noncanonical_segment_paths_are_rejected_before_read(self):
        for relative in (
            "audio/../other.wav",
            "audio/../../outside.wav",
            "audio/audio_00001.wav",
            "audio/sub/nested.wav",
            "audio//audio_00000.wav",
            "audio/./audio_00000.wav",
        ):
            with self.subTest(path=relative), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary) / "session"
                (root / "audio").mkdir(parents=True)
                result = self.audio(root, relative)
                recorder = self.recorder(root)
                with patch.object(d, "_finalize_artifact", wraps=d._finalize_artifact) as read:
                    with self.assertRaises(d.DeviceRecordingError):
                        recorder._apply_audio_result(result)
                    read.assert_not_called()

    def test_canonical_audio_is_fully_read_and_inventoried(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            result = self.audio(root)
            recorder = self.recorder(root)
            with patch.object(d, "_finalize_artifact", wraps=d._finalize_artifact) as read:
                recorder._apply_audio_result(result)
            self.assertEqual(read.call_count, 1)
            self.assertEqual(recorder._audio_bytes, 60)
            self.assertEqual(recorder._audio_segment_records[0]["pcm_payload_bytes"], 16)
            self.assertEqual(set(recorder._verified_artifacts), {"audio/audio_00000.wav"})

    def multiple_audio(self, root, count=5):
        segments = []
        for index in range(count):
            result = self.audio(root, f"audio/audio_{index:05d}.wav")
            segment = result["segments"][0]
            segment.update(
                index=index,
                start_sample=index * 4,
                end_sample=(index + 1) * 4,
                start_time_seconds=index * 4 / 48000,
                end_time_seconds=(index + 1) * 4 / 48000,
            )
            segments.append(segment)
        result.update(segments=segments, sample_count=count * 4)
        return result

    def test_late_invalid_inventory_is_rejected_before_any_read(self):
        for corruption in ("path", "index", "start_sample", "sample_count"):
            with self.subTest(field=corruption), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                result = self.multiple_audio(root)
                if corruption == "sample_count":
                    result[corruption] += 1
                else:
                    result["segments"][-1][corruption] = (
                        "audio/../other.wav" if corruption == "path" else 999
                    )
                recorder = self.recorder(root)
                with patch.object(d, "_finalize_artifact", wraps=d._finalize_artifact) as read:
                    with self.assertRaises(d.DeviceRecordingError):
                        recorder._apply_audio_result(result)
                    read.assert_not_called()

    def test_reads_overlap_and_out_of_order_completion_preserves_inventory_order(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            result = self.multiple_audio(root)
            recorder = self.recorder(root)
            owner = threading.get_ident()
            case = self

            class OwnerDict(dict):
                def update(self, values):
                    case.assertEqual(threading.get_ident(), owner)
                    return super().update(values)

            recorder._verified_artifacts = OwnerDict()
            recorder._artifact_identities = OwnerDict()
            barrier = threading.Barrier(2, timeout=5)
            second_done = threading.Event()
            finished = []
            original = d._verified_finalization

            def verify(path, expected_bytes, *, code):
                index = int(path.stem.split("_")[-1])
                if index < 2:
                    barrier.wait()
                    if index == 0:
                        self.assertTrue(second_done.wait(5))
                value = original(path, expected_bytes, code=code)
                finished.append(index)
                if index == 1:
                    second_done.set()
                return value

            with patch.object(d, "_verified_finalization", side_effect=verify):
                recorder._apply_audio_result(result)
            self.assertEqual(finished[:2], [1, 0])
            self.assertEqual([s["index"] for s in recorder._audio_segment_records], list(range(5)))
            self.assertEqual(
                list(recorder._verified_artifacts), [f"audio/audio_{i:05d}.wav" for i in range(5)]
            )
            self.assertEqual(recorder._audio_bytes, 300)

    def test_failure_joins_inflight_read_without_late_state_mutation_or_more_submissions(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            result = self.multiple_audio(root)
            recorder = self.recorder(root)
            recorder._audio_result = {"previous": True}
            recorder._audio_segment_records = ["previous"]
            recorder._audio_bytes = 123
            release = threading.Event()
            failed = threading.Event()
            returned = threading.Event()
            barrier = threading.Barrier(2, timeout=5)
            submissions = []
            errors = []
            original = d._verified_finalization

            class TrackedPool(ThreadPoolExecutor):
                def submit(self, function, *args, **kwargs):
                    submissions.append(args[0].name)
                    return super().submit(function, *args, **kwargs)

            def verify(path, expected_bytes, *, code):
                barrier.wait()
                if path.name == "audio_00000.wav":
                    failed.set()
                    raise d.DeviceRecordingError("audio_invalid", "injected read failure")
                self.assertTrue(release.wait(5))
                return original(path, expected_bytes, code=code)

            def apply():
                try:
                    recorder._apply_audio_result(result)
                except BaseException as error:
                    errors.append(error)
                finally:
                    returned.set()

            with (
                patch.object(d, "_verified_finalization", side_effect=verify),
                patch.object(d, "ThreadPoolExecutor", TrackedPool),
            ):
                caller = threading.Thread(target=apply)
                caller.start()
                try:
                    self.assertTrue(failed.wait(5))
                    self.assertFalse(returned.wait(0.05))
                    self.assertEqual(len(submissions), 2)
                finally:
                    release.set()
                    caller.join(5)
            self.assertFalse(caller.is_alive())
            self.assertEqual(len(errors), 1)
            self.assertIsInstance(errors[0], d.DeviceRecordingError)
            self.assertEqual(recorder._verified_artifacts, {})
            self.assertEqual(recorder._artifact_identities, {})
            self.assertEqual(recorder._audio_result, {"previous": True})
            self.assertEqual(recorder._audio_segment_records, ["previous"])
            self.assertEqual(recorder._audio_bytes, 123)
            self.assertEqual(len(submissions), 2)

    def test_changed_content_is_read_again_despite_matching_old_proof(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            result = self.multiple_audio(root, 2)
            recorder = self.recorder(root)
            recorder._apply_audio_result(result)
            relative = "audio/audio_00000.wav"
            old_digest = recorder._verified_artifacts[relative].sha256
            path = root / relative
            payload = bytearray(path.read_bytes())
            payload[-1] ^= 0xFF
            path.write_bytes(payload)
            with patch.object(d, "_finalize_artifact", wraps=d._finalize_artifact) as read:
                recorder._apply_audio_result(result)
            self.assertEqual(read.call_count, 2)
            self.assertNotEqual(recorder._verified_artifacts[relative].sha256, old_digest)


if __name__ == "__main__":
    unittest.main()

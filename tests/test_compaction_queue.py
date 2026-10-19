"""Stop-time file work stays bounded even when workers or admission fail."""

import hashlib
import tempfile
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest.mock import patch

from rp_ylx.recording import storage


class CompactionQueueTests(unittest.TestCase):
    def fixture(self, root, count=8):
        descriptors = []
        for index in range(count):
            path = root / f"journal{index}.ndjson"
            payload = f'{{"index":{index}}}\n'.encode()
            path.write_bytes(payload)
            descriptors.append(
                {
                    "path": path.name,
                    "bytes": len(payload),
                    "sha256": hashlib.sha256(payload).hexdigest(),
                    "media_type": "application/x-ndjson",
                }
            )
        return {
            "schema": "old",
            "frames": {"artifact": descriptors[0]},
            "imu": {"artifact": descriptors[1]},
            "audio": {"segments": [{"artifact": d} for d in descriptors[2:]]},
        }

    def test_failure_limits_submissions_and_joins_active_compaction(self):
        for failure in ("worker", "finalize", "progress"):
            with self.subTest(failure=failure):
                self.assert_compaction_failure(failure)

    def assert_compaction_failure(self, failure):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            manifest = self.fixture(root)
            originals = {p.name: p.read_bytes() for p in root.iterdir()}
            release = threading.Event()
            failed = threading.Event()
            returned = threading.Event()
            barrier = threading.Barrier(2, timeout=5)
            submissions = []
            errors = []
            real_compact = storage.compact_metadata

            class Pool(ThreadPoolExecutor):
                def submit(self, function, *args, **kwargs):
                    submissions.append(args[0]["path"])
                    return super().submit(function, *args, **kwargs)

            def compact(path):
                index = int(path.stem.removeprefix("journal"))
                if index < 2:
                    barrier.wait()
                    if index == 1:
                        self.assertTrue(release.wait(5))
                    elif failure == "worker":
                        failed.set()
                        raise ValueError("injected compaction failure")
                return real_compact(path)

            def finalize(path):
                if failure == "finalize":
                    failed.set()
                    raise ValueError("injected finalize failure")
                return {"path": path.name}

            def progress(completed, total):
                if failure == "progress":
                    failed.set()
                    raise ValueError("injected progress failure")

            def run():
                try:
                    storage.compact_manifest(root, manifest, finalize, progress=progress)
                except BaseException as error:
                    errors.append(error)
                finally:
                    returned.set()

            with (
                patch.object(storage, "compact_metadata", side_effect=compact),
                patch.object(storage, "ThreadPoolExecutor", Pool),
            ):
                caller = threading.Thread(target=run)
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
            self.assertRegex(str(errors[0]), "injected")
            self.assertEqual(len(submissions), 2)
            self.assertEqual(manifest["schema"], "old")
            self.assertEqual({n: (root / n).read_bytes() for n in originals}, originals)
            self.assertLessEqual(len(list(root.glob("*.zst"))), 2)

    def test_overlapping_work_keeps_ordered_caller_finalization_and_progress(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            manifest = self.fixture(root, 5)
            barrier = threading.Barrier(2, timeout=5)
            second_done = threading.Event()
            finished = []
            admitted = []
            progress = []
            owner = threading.get_ident()
            real_compact = storage.compact_metadata

            def compact(path):
                index = int(path.stem.removeprefix("journal"))
                if index < 2:
                    barrier.wait()
                    if index == 0:
                        self.assertTrue(second_done.wait(5))
                result = real_compact(path)
                finished.append(index)
                if index == 1:
                    second_done.set()
                return result

            def finalize(path):
                self.assertEqual(threading.get_ident(), owner)
                admitted.append(path.name)
                with storage.open_metadata(path) as stream:
                    self.assertEqual(stream.read(), (root / path.stem).read_bytes())
                return {"path": path.name}

            def report(completed, total):
                self.assertEqual(threading.get_ident(), owner)
                progress.append((completed, total))

            with patch.object(storage, "compact_metadata", side_effect=compact):
                result, originals = storage.compact_manifest(
                    root, manifest, finalize, progress=report
                )
            self.assertIs(result, manifest)
            self.assertEqual(result["schema"], "ylx.device-session.v4")
            self.assertEqual(finished[:2], [1, 0])
            self.assertEqual(originals, [f"journal{i}.ndjson" for i in range(5)])
            self.assertEqual(admitted, [name + ".zst" for name in originals])
            self.assertEqual(progress, [(i, 5) for i in range(1, 6)])

    def test_slow_second_file_keeps_other_worker_busy_with_bounded_pending_results(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            manifest = self.fixture(root, 12)
            third_started = threading.Event()
            admitted = []
            submissions = []
            real_compact = storage.compact_metadata
            case = self

            class Pool(ThreadPoolExecutor):
                def submit(self, function, *args, **kwargs):
                    submissions.append(args[0]["path"])
                    case.assertLessEqual(len(submissions) - len(admitted), 2)
                    return super().submit(function, *args, **kwargs)

            def compact(path):
                index = int(path.stem.removeprefix("journal"))
                if index == 1:
                    self.assertTrue(third_started.wait(5))
                if index == 2:
                    third_started.set()
                return real_compact(path)

            def finalize(path):
                admitted.append(path.name)
                return {"path": path.name}

            with (
                patch.object(storage, "compact_metadata", side_effect=compact),
                patch.object(storage, "ThreadPoolExecutor", Pool),
            ):
                storage.compact_manifest(root, manifest, finalize)
            self.assertEqual(len(admitted), 12)


class VerificationQueueTests(unittest.TestCase):
    def test_failure_joins_active_verifier_before_closing_root_and_limits_submissions(self):
        from rp_ylx.recording import device_session as d
        from tests import test_split_eye_recording as fixtures

        with tempfile.TemporaryDirectory() as temporary:
            fixture = fixtures.SplitEyeRecordingTest()
            recorder, _, _ = fixture.build(Path(temporary))
            recorder.start()
            fixture.feed(recorder, 3)
            sealed = recorder.stop()
            self.assertGreater(len(d._manifest_artifacts(sealed.manifest)), 2)
            for failure in ("worker", "progress"):
                with self.subTest(failure=failure):
                    self.assert_verification_failure(sealed, failure)

    def assert_verification_failure(self, sealed, failure):
        from rp_ylx.recording import device_session as d

        barrier = threading.Barrier(2, timeout=5)
        release = threading.Event()
        failed = threading.Event()
        returned = threading.Event()
        submissions = []
        verified = []
        errors = []

        class Pool(ThreadPoolExecutor):
            def submit(self, function, *args, **kwargs):
                index = len(submissions)
                submissions.append(args[0]["path"])

                def run():
                    if index < 2:
                        barrier.wait()
                        if index == 1:
                            if not release.wait(5):
                                raise RuntimeError("test release timeout")
                        elif failure == "worker":
                            failed.set()
                            raise d.DeviceRecordingError("injected", "read failed")
                    value = function(*args, **kwargs)
                    verified.append(index)
                    return value

                return super().submit(run)

        def report(done, total):
            if done and failure == "progress":
                failed.set()
                raise RuntimeError("progress failed")

        def validate():
            try:
                d.validate_device_session_directory(sealed.path, progress=report)
            except BaseException as error:
                errors.append(error)
            finally:
                returned.set()

        with patch.object(d, "ThreadPoolExecutor", Pool):
            caller = threading.Thread(target=validate)
            caller.start()
            try:
                self.assertTrue(failed.wait(5))
                self.assertFalse(returned.wait(0.05))
                self.assertEqual(len(submissions), 2)
            finally:
                release.set()
                caller.join(5)
        self.assertFalse(caller.is_alive())
        self.assertEqual(len(submissions), 2)
        self.assertIn(1, verified)
        self.assertEqual(len(errors), 1)
        self.assertEqual(
            str(errors[0]), "injected: read failed" if failure == "worker" else "progress failed"
        )

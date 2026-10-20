"""Partial live-sealing startup must cancel queued work and join started workers."""

import tempfile
import threading
import unittest
from contextlib import ExitStack
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch

import rp_ylx.recording.device_session as session
import rp_ylx.recording.live_seal as live
from tests import test_split_eye_recording as fixtures


class SeededStore(fixtures.FakeSessionStore):
    def begin_recording(self, plan):
        transaction = super().begin_recording(plan)
        for name in ("frames.ndjson", "imu.ndjson"):
            (Path(plan.session_root) / name).write_bytes(b"")
        return transaction


class LiveSealStartTests(unittest.TestCase):
    def check_failure(self, fail_at, *, interrupt=False, recorder_mode=False):
        pools, instances = [], []
        audit_started = threading.Event()
        pool_factory, start_thread = live.ThreadPoolExecutor, threading.Thread.start
        audit = live.capture_audit
        starts = 0
        injected = KeyboardInterrupt("cancel startup") if interrupt else RuntimeError("no worker")

        def start(thread):
            nonlocal starts
            if thread.name.startswith("live-seal"):
                starts += 1
                if not interrupt and starts == fail_at:
                    raise injected
            return start_thread(thread)

        def observed_audit(*args, **kwargs):
            audit_started.set()
            return audit(*args, **kwargs)

        with tempfile.TemporaryDirectory() as directory, ExitStack() as patches:
            root = Path(directory)
            for name in ("frames.ndjson", "imu.ndjson"):
                (root / name).write_bytes(b"")

            def make_pool(**kwargs):
                pool = pool_factory(**kwargs)
                pools.append(pool)
                submit = pool.submit
                calls = 0

                def observed_submit(*args, **kwargs):
                    nonlocal calls
                    calls += 1
                    if calls == 2:
                        if not audit_started.wait(3):
                            raise TimeoutError("audit did not start")
                        if interrupt:
                            raise injected
                    return submit(*args, **kwargs)

                patches.enter_context(patch.object(pool, "submit", side_effect=observed_submit))
                return pool

            def make_sealing(*args):
                sealing = object.__new__(live.LiveSealing)
                instances.append(sealing)
                sealing.__init__(*args)
                return sealing

            patches.enter_context(patch.object(live, "ThreadPoolExecutor", side_effect=make_pool))
            patches.enter_context(patch.object(threading.Thread, "start", start))
            patches.enter_context(patch.object(live, "capture_audit", side_effect=observed_audit))
            recorder = None
            try:
                if recorder_mode:
                    store = SeededStore()
                    recorder, _, _ = fixtures.SplitEyeRecordingTest().build(
                        root, native_data_plane=True
                    )
                    recorder._config = replace(recorder._config, lossless_storage=True)
                    with (
                        patch.object(session, "native_session_store", return_value=store),
                        patch.object(session, "resolve_executable", return_value=Path("/bin/true")),
                        patch.object(session, "LiveSealing", side_effect=make_sealing),
                        self.assertRaises(session.DeviceRecordingError) as raised,
                    ):
                        recorder.start()
                    self.assertIs(raised.exception.__cause__, injected)
                    self.assertEqual(recorder.state, "failed")
                    self.assertTrue(store.transaction.aborted)
                    self.assertIsNone(recorder._native_transaction)
                    self.assertIsNone(recorder._live_sealing)
                else:
                    with self.assertRaises(type(injected)) as raised:
                        make_sealing(root, 30, 2)
                    self.assertIs(raised.exception, injected)
                self.assertEqual(len(instances), 1)
                self.assertTrue(
                    instances[0].cancelled.is_set(), "partial startup was not cancelled"
                )
                self.assertTrue(instances[0].finished.is_set(), "audit was not released")
                self.assertTrue(pools[0]._shutdown)
                self.assertTrue(all(not thread.is_alive() for thread in pools[0]._threads))
                # submit() enqueues before attempting to create a worker. Failed
                # startup must cancel those entries, including an unreturned future.
                while not pools[0]._work_queue.empty():
                    self.assertIsNone(pools[0]._work_queue.get_nowait())
                actual_root = recorder.partial_path if recorder_mode else root
                for name in ("frames.ndjson", "imu.ndjson"):
                    self.assertEqual((actual_root / name).read_bytes(), b"")
            finally:
                # Old-code reproduction must not strand non-daemon executor workers.
                for sealing in instances:
                    sealing.cancelled.set()
                    sealing.finished.set()
                for pool in pools:
                    pool.shutdown(wait=True, cancel_futures=True)
                if recorder is not None:
                    recorder.abort()

    def test_first_worker_failure_cancels_enqueued_audit(self):
        self.check_failure(1)

    def test_second_worker_failure_joins_audit_and_cancels_compaction(self):
        self.check_failure(2)

    def test_interruption_during_second_submission_joins_audit(self):
        self.check_failure(2, interrupt=True)

    def test_native_recorder_start_failure_closes_unreturned_sealer(self):
        self.check_failure(2, recorder_mode=True)

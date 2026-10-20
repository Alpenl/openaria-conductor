"""Stopping must not republish recording checkpoints while accepted frames drain."""

import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import patch

import rp_ylx.recording.device_session as session
from tests import test_split_eye_recording as fixtures


class StopCheckpointTests(unittest.TestCase):
    def build(self, root, **kwargs):
        return fixtures.SplitEyeRecordingTest().build(root, **kwargs)[0]

    def test_draining_frames_does_not_republish_recording_after_finalizing(self):
        entered, release, finalizing = threading.Event(), threading.Event(), threading.Event()
        errors, states = [], []

        def block(role, payload):
            if role == "frames.index":
                entered.set()
                if not release.wait(5):
                    raise TimeoutError("frame gate not released")

        with tempfile.TemporaryDirectory() as temporary:
            recorder = self.build(Path(temporary), before_write=block)
            recorder.start()

            def sink(document):
                states.append(document["state"])
                if document["state"] == "finalizing":
                    finalizing.set()

            recorder._state_sink = sink
            fixtures.SplitEyeRecordingTest.feed(recorder, 32)
            self.assertTrue(entered.wait(2))

            def stop():
                try:
                    recorder.stop()
                except BaseException as error:
                    errors.append(error)

            worker = threading.Thread(target=stop)
            try:
                worker.start()
                self.assertTrue(finalizing.wait(3))
            finally:
                release.set()
                worker.join(15)
                recorder.abort()
            self.assertFalse(worker.is_alive())
            self.assertEqual(errors, [])
            self.assertEqual(recorder._frames_written, 32)
            self.assertNotIn("recording", states[states.index("finalizing") + 1 :])

    def test_inflight_checkpoint_cannot_overtake_finalizing_publication(self):
        entered, release, stop_entered, finalizing_write = (threading.Event() for _ in range(4))
        errors = []
        with tempfile.TemporaryDirectory() as temporary:
            recorder = self.build(Path(temporary))
            recorder.start()
            write = session.write_json_atomic
            persist = recorder._persist_state

            def delayed(path, document, **kwargs):
                if path.name == "recording.json":
                    if document["state"] == "recording":
                        entered.set()
                        if not release.wait(5):
                            raise TimeoutError("checkpoint gate not released")
                    elif document["state"] == "finalizing":
                        finalizing_write.set()
                return write(path, document, **kwargs)

            def observed(state, **kwargs):
                if state == "finalizing":
                    stop_entered.set()
                return persist(state, **kwargs)

            def stop():
                try:
                    recorder.stop()
                except BaseException as error:
                    errors.append(error)

            worker = threading.Thread(target=stop)
            with (
                patch.object(session, "write_json_atomic", side_effect=delayed),
                patch.object(recorder, "_persist_state", side_effect=observed),
            ):
                try:
                    fixtures.SplitEyeRecordingTest.feed(recorder, 3)
                    self.assertTrue(entered.wait(2))
                    worker.start()
                    self.assertTrue(stop_entered.wait(2))
                    self.assertFalse(
                        finalizing_write.wait(0.1),
                        "finalizing raced an older unfinished checkpoint",
                    )
                finally:
                    release.set()
                    if worker.ident is not None:
                        worker.join(15)
                    recorder.abort()
            self.assertFalse(worker.is_alive())
            self.assertEqual(errors, [])

    def test_internal_writer_drain_avoids_intermediate_checkpoints(self):
        entered, release, joining = (threading.Event() for _ in range(3))
        states = []

        def block(role, payload):
            if role == "frames.index":
                entered.set()
                if not release.wait(5):
                    raise TimeoutError("frame gate not released")

        with tempfile.TemporaryDirectory() as temporary:
            recorder = self.build(Path(temporary), before_write=block)
            recorder.start()
            recorder._state_sink = lambda document: states.append(document["state"])
            fixtures.SplitEyeRecordingTest.feed(recorder, 32)
            self.assertTrue(entered.wait(2))
            writer = recorder._writer
            join = writer.join

            def observed_join(timeout=None):
                joining.set()
                return join(timeout)

            worker = threading.Thread(target=recorder._stop_writer)
            try:
                with patch.object(writer, "join", side_effect=observed_join):
                    worker.start()
                    self.assertTrue(joining.wait(2))
                    release.set()
                    worker.join(15)
                self.assertFalse(worker.is_alive())
                self.assertIsNone(recorder._writer_error)
                self.assertEqual(recorder._frames_written, 32)
                self.assertEqual(states, [])
            finally:
                release.set()
                if worker.ident is not None:
                    worker.join(15)
                recorder.abort()

    def test_next_checkpoint_interval_starts_after_durable_write_finishes(self):
        with tempfile.TemporaryDirectory() as temporary:
            recorder = self.build(Path(temporary))
            recorder.start()
            recorder._checkpoint_interval = 1.0
            recorder._last_checkpoint = 100.0
            clock = [101.0]
            states = []
            recorder._state_sink = lambda document: states.append(document["state"])
            write = session.write_json_atomic

            def slow_write(*args, **kwargs):
                result = write(*args, **kwargs)
                clock[0] += 2.0
                return result

            try:
                with (
                    patch.object(session.time, "monotonic", side_effect=lambda: clock[0]),
                    patch.object(session, "write_json_atomic", side_effect=slow_write),
                ):
                    recorder._checkpoint_if_due()
                    recorder._checkpoint_if_due()
                    self.assertEqual(
                        states,
                        ["recording"],
                        "slow storage triggered an immediate extra checkpoint",
                    )
                    clock[0] += 1.0
                    recorder._checkpoint_if_due()
                    self.assertEqual(states, ["recording", "recording"])
            finally:
                recorder.abort()

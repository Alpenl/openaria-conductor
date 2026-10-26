"""Failed recorder startup must release every acquired resource and admission."""

import json
import tempfile
import threading
import unittest
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import patch

import rp_ylx.recording.device_session as session
from rp_ylx.api import ProviderError
from tests import test_capture_coordinator as capture
from tests import test_split_eye_recording as fixtures


@contextmanager
def fail_worker(role, stage, created, error):
    original_thread, original_start = threading.Thread, threading.Thread.start
    prefix = "rp-ylx-session-writer-" if role == "writer" else "rp-ylx-v1-segments-"

    def construct(*args, **kwargs):
        if stage == "construct" and kwargs.get("name", "").startswith(prefix):
            raise error
        thread = original_thread(*args, **kwargs)
        created.append(thread)
        return thread

    def start(thread):
        if stage == "start" and thread.name.startswith(prefix):
            raise error
        return original_start(thread)

    with (
        patch.object(threading, "Thread", side_effect=construct),
        patch.object(original_thread, "start", start),
    ):
        yield


def cleanup_fixture(recorder):
    # Repair only old-version test leftovers, without hiding the asserted error.
    for name in ("_writer", "_harvester"):
        thread = getattr(recorder, name)
        if thread is not None and thread.ident is None:
            setattr(recorder, name, None)
    recorder._stop_writer()
    recorder._abandon_audio()
    recorder._abandon_encoder()
    recorder._close_files(ignore_errors=True)


class SessionWorkerStartTests(unittest.TestCase):
    def check_worker(self, role, native=False):
        for stage in ("construct", "start"):
            with self.subTest(stage=stage), tempfile.TemporaryDirectory() as directory:
                store = fixtures.FakeSessionStore()
                recorder, encoders, audios = fixtures.SplitEyeRecordingTest().build(
                    Path(directory), native_data_plane=native, audio_enabled=not native
                )
                threads = []
                injected = RuntimeError("injected worker unavailable")
                try:
                    with (
                        patch.object(session, "native_session_store", return_value=store),
                        patch.object(session, "resolve_executable", return_value=Path("/bin/true")),
                        fail_worker(role, stage, threads, injected),
                        self.assertRaises(session.DeviceRecordingError) as failure,
                    ):
                        recorder.start()
                    self.assertEqual(failure.exception.code, "start_failed")
                    self.assertIs(failure.exception.__cause__, injected)
                    self.assertEqual(recorder.state, "failed")
                    self.assertIsNone(recorder._writer)
                    self.assertIsNone(recorder._harvester)
                    self.assertIsNone(recorder._native_transaction)
                    self.assertEqual(recorder._files, {})
                    self.assertTrue(all(not thread.is_alive() for thread in threads))
                    self.assertTrue(all(encoder.aborted for encoder in encoders))
                    self.assertTrue(all(audio.aborted for audio in audios))
                    if native:
                        self.assertTrue(store.transaction.aborted)
                    document = json.loads((recorder.partial_path / "recording.json").read_text())
                    self.assertEqual(document["state"], "failed")
                    self.assertEqual(document["diagnostics"][0]["code"], "start_failed")
                    recorder.abort()
                    with self.assertRaises(session.DeviceRecordingError):
                        recorder.submit_frame(fixtures.frame(0))
                finally:
                    cleanup_fixture(recorder)

    def test_writer_creation_failure_releases_resources(self):
        self.check_worker("writer")

    def test_harvester_creation_failure_joins_started_writer(self):
        self.check_worker("harvester")

    def test_native_harvester_creation_failure_aborts_transaction(self):
        self.check_worker("harvester", native=True)

    def check_adapter(self, kind):
        with tempfile.TemporaryDirectory() as directory:
            recorder, encoders, audios = fixtures.SplitEyeRecordingTest().build(
                Path(directory), audio_enabled=True
            )
            cls = fixtures.FakeStereoEncoder if kind == "encoder" else fixtures.FakeAudioRecorder
            original = cls.start
            injected = RuntimeError("injected partially started adapter")

            def partial_start(adapter):
                original(adapter)
                raise injected

            try:
                with (
                    patch.object(cls, "start", partial_start),
                    self.assertRaises(session.DeviceRecordingError) as failure,
                ):
                    recorder.start()
                self.assertIs(failure.exception.__cause__, injected)
                self.assertTrue(encoders[0].aborted)
                if kind == "audio":
                    self.assertTrue(audios[0].aborted)
                self.assertEqual(recorder._files, {})
                self.assertEqual(recorder.state, "failed")
            finally:
                cleanup_fixture(recorder)
                for adapter in encoders + audios:
                    adapter.abort()

    def test_partially_started_encoder_is_aborted(self):
        self.check_adapter("encoder")

    def test_partially_started_audio_is_aborted(self):
        self.check_adapter("audio")


class CoordinatorWorkerStartTests(unittest.TestCase):
    def test_worker_start_failure_releases_capture_and_allows_new_start(self):
        for role in ("writer", "harvester"):
            with self.subTest(role=role):
                fixture = capture.CaptureCoordinatorTest()
                fixture.setUp()
                coordinator = fixture.coordinator()
                threads = []
                try:
                    with (
                        fail_worker(role, "start", threads, RuntimeError("no worker")),
                        self.assertRaises(ProviderError) as failure,
                    ):
                        coordinator.start_capture(capture.start_command("failed-" + role))
                    self.assertEqual(failure.exception.code, "start_failed")
                    self.assertIsNone(coordinator._active)
                    self.assertIsNone(coordinator._active_plan)
                    fixture.assert_network_operation_lock_available()
                    self.assertTrue(all(not thread.is_alive() for thread in threads))
                    self.assertEqual(
                        coordinator.capture_status()["snapshot"]["device_state"], "idle"
                    )
                    accepted = coordinator.start_capture(capture.start_command("retry-" + role))
                    self.assertEqual(accepted.status, 202)
                    self.assertIsNotNone(coordinator._active)
                finally:
                    if coordinator._active is not None:
                        cleanup_fixture(coordinator._active)
                    coordinator.close()
                    fixture.tearDown()

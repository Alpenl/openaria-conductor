import unittest
from unittest.mock import patch

from rp_ylx.api import ProviderError
from tests import test_capture_coordinator as fixtures


class StartReceiptRecoveryTests(unittest.TestCase):
    def setUp(self):
        self.fixture = fixtures.CaptureCoordinatorTest()
        self.fixture.setUp()
        self.addCleanup(self.fixture.tearDown)
        self.sources = fixtures.FakeSources()
        self.coordinator = self.fixture.coordinator(sources=self.sources)
        self.addCleanup(self.coordinator.close)
        self.command = fixtures.start_command("retry-start")
        self.real_write = self.coordinator._state_db.put_capture_state

    def fail_after_sources_started(self, *args, **kwargs):
        if self.sources.open_handle_count:
            raise OSError("injected state database write failure after sources started")
        return self.real_write(*args, **kwargs)

    def start_with_failed_receipt(self):
        with (
            patch.object(
                self.coordinator._state_db, "put_capture_state", self.fail_after_sources_started
            ),
            self.assertRaises(OSError),
        ):
            self.coordinator.start_capture(self.command)
        active = self.coordinator.capture_status()["snapshot"]["active_recording"]
        self.assertIsNotNone(active)
        self.assertEqual(active["recording_state"]["state"], "recording")
        self.assertTrue(self.coordinator.submit_frame(fixtures.frame()))
        return active["recording_state"]["session_id"]

    def test_retry_persists_original_start_without_starting_sources_again(self):
        with patch.object(self.sources, "start", wraps=self.sources.start) as start:
            session_id = self.start_with_failed_receipt()
            replay = self.coordinator.start_capture(self.command)
            self.assertTrue(replay.replayed)
            self.assertEqual(replay.status, 202)
            self.assertEqual(
                replay.body["snapshot"]["active_recording"]["recording_state"]["session_id"],
                session_id,
            )
            start.assert_called_once()
        self.coordinator.stop_capture(fixtures.stop_command("finish"))
        self.coordinator.close()
        restarted = self.fixture.coordinator()
        self.addCleanup(restarted.close)
        persisted = restarted.start_capture(self.command)
        self.assertTrue(persisted.replayed)
        self.assertEqual(persisted.body, replay.body)
        self.assertEqual(restarted.capture_status()["snapshot"]["device_state"], "idle")

    def test_failed_receipt_retry_still_requires_durable_write(self):
        session_id = self.start_with_failed_receipt()
        with patch.object(
            self.coordinator._state_db, "put_capture_state", side_effect=OSError("still full")
        ):
            for _ in range(2):
                with self.assertRaises(OSError):
                    self.coordinator.start_capture(self.command)
        current = self.coordinator.capture_status()["snapshot"]["active_recording"]
        self.assertEqual(current["recording_state"]["session_id"], session_id)
        self.assertTrue(self.coordinator.start_capture(self.command).replayed)

    def test_failed_receipt_retains_conflict_detection(self):
        session_id = self.start_with_failed_receipt()
        with self.assertRaises(ProviderError) as raised:
            self.coordinator.start_capture(
                fixtures.start_command("retry-start", display_name="changed")
            )
        self.assertEqual(raised.exception.code, "idempotency_conflict")
        current = self.coordinator.capture_status()["snapshot"]["active_recording"]
        self.assertEqual(current["recording_state"]["session_id"], session_id)

    def test_source_failure_does_not_become_successful_start_receipt(self):
        with (
            patch.object(self.sources, "start", side_effect=RuntimeError("source failed")),
            self.assertRaises(ProviderError),
        ):
            self.coordinator.start_capture(self.command)
        self.assertFalse(
            any(
                scope[1:] == ("start", self.command.idempotency_key)
                for scope in self.coordinator._commands
            )
        )
        result = self.coordinator.start_capture(self.command)
        self.assertEqual(result.status, 202)
        self.assertFalse(result.replayed)
        self.assertTrue(self.coordinator.submit_frame(fixtures.frame()))


if __name__ == "__main__":
    unittest.main()

"""Deleting a recovered take also retires its retained failed outcome."""

import hashlib
import json
import unittest
from copy import deepcopy
from unittest.mock import patch

from rp_ylx.api import CaptureCommand, ProviderError
from rp_ylx.recording import deletion
from tests import test_capture_coordinator as fixtures


class DeletedRecordingOutcomesTest(unittest.TestCase):
    def setUp(self):
        self.fixture = fixtures.CaptureCoordinatorTest()
        self.fixture.setUp()
        self.addCleanup(self.fixture.tearDown)

    def recovered(self, prefix):
        coordinator, sid = self.fixture._retained_catalog_entry(recovered=True, prefix=prefix)
        manifest = json.loads(
            (self.fixture.mountpoint / "recordings" / sid / "manifest.json").read_bytes()
        )
        coordinator._record_saved_prefix(sid, manifest)
        coordinator._persist_local_state()
        self.addCleanup(coordinator.close)
        self.assertEqual(
            coordinator.capture_status()["snapshot"]["retained_unsuccessful"]["recording_state"][
                "session_id"
            ],
            sid,
        )
        return coordinator, sid

    def command(self, coordinator, session_ids, *, reverse=False):
        body = {
            "schema": "ylx.session-delete-request.v1",
            "sessions": [
                {
                    "session_id": sid,
                    "manifest_sha256": hashlib.sha256(
                        (
                            self.fixture.mountpoint / "recordings" / sid / "manifest.json"
                        ).read_bytes()
                    ).hexdigest(),
                }
                for sid in sorted(session_ids, reverse=reverse)
            ],
        }
        return CaptureCommand(
            "bridge", "delete-selected", body, json.dumps(body, sort_keys=True).encode()
        )

    def test_deleted_recovered_take_disappears_from_status_and_survives_restart(self):
        coordinator, sid = self.recovered("deleted")
        command = self.command(coordinator, {sid})
        revision = coordinator.capture_recording_progress()[1]
        result = coordinator.delete_sessions(command)
        self.assertEqual(result.body["deleted_session_ids"], [sid])
        self.assertIsNone(coordinator.retained_unsuccessful_outcome(sid))
        self.assertIsNone(coordinator.capture_status()["snapshot"]["retained_unsuccessful"])
        self.assertGreater(coordinator.capture_recording_progress()[1], revision)
        revision = coordinator.capture_recording_progress()[1]
        self.assertTrue(coordinator.delete_sessions(command).replayed)
        self.assertEqual(coordinator.capture_recording_progress()[1], revision)
        coordinator.close()
        restarted = self.fixture.coordinator()
        self.addCleanup(restarted.close)
        self.assertIsNone(restarted.retained_unsuccessful_outcome(sid))
        self.assertTrue(restarted.delete_sessions(command).replayed)

    def test_rejected_deletion_preserves_failure_and_revision(self):
        coordinator, sid = self.recovered("rejected")
        command = self.command(coordinator, {sid})
        body = deepcopy(command.body)
        body["sessions"][0]["manifest_sha256"] = "0" * 64
        command = CaptureCommand("bridge", "bad-digest", body, json.dumps(body).encode())
        before = coordinator.retained_unsuccessful_outcome(sid)
        revision = coordinator.capture_recording_progress()[1]
        with self.assertRaises(ProviderError) as error:
            coordinator.delete_sessions(command)
        self.assertEqual(error.exception.code, "manifest_changed")
        self.assertEqual(coordinator.retained_unsuccessful_outcome(sid), before)
        self.assertEqual(coordinator.capture_recording_progress()[1], revision)

    def test_partial_deletion_clears_only_successful_outcomes(self):
        self._check_partial_deletion(False)

    def test_partial_deletion_of_latest_does_not_restore_older_failure(self):
        self._check_partial_deletion(True)

    def _check_partial_deletion(self, latest_first):
        first, first_id = self.recovered("partial-first")
        first.close()
        coordinator, latest_id = self.recovered("partial-latest")
        command = self.command(coordinator, {first_id, latest_id}, reverse=latest_first)
        remove = deletion.shutil.rmtree
        calls = []

        def fail_second(*args, **kwargs):
            calls.append(args[0])
            if len(calls) == 2:
                raise PermissionError("storage failure")
            return remove(*args, **kwargs)

        with patch.object(deletion.shutil, "rmtree", side_effect=fail_second):
            result = coordinator.delete_sessions(command)
        self.assertEqual(len(result.body["deleted_session_ids"]), 1)
        deleted = result.body["deleted_session_ids"][0]
        self.assertEqual(deleted, latest_id if latest_first else first_id)
        remaining = result.body["failed_sessions"][0]["session_id"]
        self.assertIsNone(coordinator.retained_unsuccessful_outcome(deleted))
        self.assertIsNotNone(coordinator.retained_unsuccessful_outcome(remaining))
        status = coordinator.capture_status()["snapshot"]["retained_unsuccessful"]
        if remaining == latest_id:
            self.assertEqual(status["recording_state"]["session_id"], latest_id)
        else:
            self.assertIsNone(status)
        coordinator.close()
        restarted = self.fixture.coordinator()
        self.addCleanup(restarted.close)
        self.assertIsNone(restarted.retained_unsuccessful_outcome(deleted))
        self.assertIsNotNone(restarted.retained_unsuccessful_outcome(remaining))

    def test_startup_reconciles_old_firmware_stale_outcome_from_durable_delete_receipt(self):
        coordinator, sid = self.recovered("legacy-delete")
        saved = deepcopy(coordinator._retained[sid])
        command = self.command(coordinator, {sid})
        coordinator.delete_sessions(command)
        # Model the old firmware: receipt and stale failure both persisted.
        coordinator._retained[sid] = saved
        coordinator._revision = saved["recording_state"]["state_revision"]
        coordinator._persist_local_state()
        coordinator.close()
        restarted = self.fixture.coordinator()
        self.addCleanup(restarted.close)
        self.assertIsNone(restarted.retained_unsuccessful_outcome(sid))
        self.assertIsNone(restarted.capture_status()["snapshot"]["retained_unsuccessful"])
        self.assertTrue(restarted.delete_sessions(command).replayed)
        restarted.close()
        again = self.fixture.coordinator()
        self.addCleanup(again.close)
        self.assertIsNone(again.retained_unsuccessful_outcome(sid))

    def test_delete_receipt_persistence_retry_does_not_repeat_removal(self):
        coordinator, sid = self.recovered("receipt-retry")
        command = self.command(coordinator, {sid})
        with (
            patch.object(
                coordinator._state_db, "put_capture_state", side_effect=OSError("disk full")
            ),
            self.assertRaises(OSError),
        ):
            coordinator.delete_sessions(command)
        self.assertFalse((self.fixture.mountpoint / "recordings" / sid).exists())
        self.assertIsNone(coordinator.retained_unsuccessful_outcome(sid))
        with patch.object(
            deletion, "delete_recordings", side_effect=AssertionError("duplicate delete")
        ):
            self.assertTrue(coordinator.delete_sessions(command).replayed)
        coordinator.close()
        restarted = self.fixture.coordinator()
        self.addCleanup(restarted.close)
        self.assertIsNone(restarted.retained_unsuccessful_outcome(sid))

    def test_missing_media_without_delete_receipt_keeps_failure_diagnostics(self):
        coordinator, sid = self.recovered("missing-media")
        (self.fixture.mountpoint / "recordings" / sid).rename(
            self.fixture.root / "disconnected-media"
        )
        coordinator.close()
        restarted = self.fixture.coordinator()
        self.addCleanup(restarted.close)
        self.assertIsNotNone(restarted.retained_unsuccessful_outcome(sid))

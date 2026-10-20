"""Damaged receipts must not crash startup or replay an operation as new."""

import copy
import json
import unittest
from unittest.mock import patch

from rp_ylx.api import CaptureCommand, ProviderError
from rp_ylx.api.events import validate_capture_status
from rp_ylx.persistence import PersistenceError
from rp_ylx.recording.device_session import uuid7
from tests import test_capture_coordinator as fixtures


class CommandReceiptAdmissionTest(unittest.TestCase):
    def setUp(self):
        self.fixture = fixtures.CaptureCoordinatorTest()
        self.fixture.setUp()
        self.addCleanup(self.fixture.tearDown)
        coordinator = self.fixture.coordinator()
        self.command = fixtures.start_command("original-start")
        self.original = coordinator.start_capture(self.command)
        coordinator.submit_frame(fixtures.frame())
        self.stop = fixtures.stop_command("original-stop")
        coordinator.stop_capture(self.stop)
        coordinator.close()
        self.database = coordinator._state_db
        self.durable = self.database.get("capture")
        with self.database.connection() as db:
            self.rows = db.execute(
                "SELECT principal,operation,idempotency_key,body FROM capture_commands"
            ).fetchall()
        self.row = next(row for row in self.rows if row[1] == "start")
        self.receipt = json.loads(self.row[3])

    def reset(self):
        self.database.put("capture", copy.deepcopy(self.durable))
        with self.database.connection() as db, db:
            db.execute("DELETE FROM capture_commands")
            db.executemany("INSERT INTO capture_commands VALUES (?,?,?,?)", self.rows)

    def corrupt(self, raw, operation="start"):
        with self.database.connection() as db, db:
            db.execute("UPDATE capture_commands SET body=? WHERE operation=?", (raw, operation))

    def assert_blocked(self, coordinator, command=None, method="start_capture"):
        with self.assertRaises(ProviderError) as raised:
            getattr(coordinator, method)(command or self.command)
        self.assertEqual(raised.exception.code, "command_receipt_invalid")
        self.assertEqual(raised.exception.status, 500)
        self.assertFalse(raised.exception.retryable)

    def test_malformed_sqlite_receipt_is_preserved_and_blocked_across_restarts(self):
        for raw in (b"invalid blob", "{", "[]", '"not an object"', "[" * 2000 + "]" * 2000):
            with self.subTest(raw=raw[:20]):
                self.reset()
                self.corrupt(raw)
                for _ in range(2):
                    coordinator = self.fixture.coordinator()
                    try:
                        validate_capture_status(coordinator.capture_status())
                        with patch.object(coordinator, "_start_capture") as execute:
                            self.assert_blocked(coordinator)
                            execute.assert_not_called()
                        with self.database.connection() as db:
                            self.assertEqual(
                                db.execute(
                                    "SELECT body FROM capture_commands WHERE operation='start'"
                                ).fetchone()[0],
                                raw,
                            )
                    finally:
                        coordinator.close()

    def test_bad_envelope_or_response_cannot_be_replayed_or_reexecuted(self):
        variants = [
            ("infinite-status", lambda v: v.update(status=float("inf"))),
            ("boolean-status", lambda v: v.update(status=True)),
            ("string-status", lambda v: v.update(status="202")),
            ("invalid-status", lambda v: v.update(status=999)),
            ("wrong-status", lambda v: v.update(status=204, body=None)),
            ("missing-status", lambda v: v.pop("status")),
            ("bad-canonical-type", lambda v: v.update(canonical_body={})),
            ("bad-canonical-json", lambda v: v.update(canonical_body="{")),
            ("invalid-unicode", lambda v: v.update(canonical_body="\ud800")),
            ("bad-status-body", lambda v: v.update(body={"snapshot": []})),
            ("nonfinite-body", lambda v: v["body"].update(source_revision=float("nan"))),
        ]
        for name, change in variants:
            with self.subTest(case=name):
                self.reset()
                value = copy.deepcopy(self.receipt)
                change(value)
                raw = json.dumps(value)
                self.corrupt(raw)
                coordinator = self.fixture.coordinator()
                try:
                    with patch.object(coordinator, "_start_capture") as execute:
                        self.assert_blocked(coordinator)
                        execute.assert_not_called()
                    coordinator.start_capture(fixtures.start_command("fresh-" + name))
                    coordinator.submit_frame(fixtures.frame())
                    coordinator.stop_capture(fixtures.stop_command("fresh-stop-" + name))
                finally:
                    coordinator.close()

    def test_sqlite_identity_mismatch_cannot_forge_another_receipt(self):
        value = copy.deepcopy(self.receipt)
        forged = fixtures.start_command("forged-start")
        value["idempotency_key"] = forged.idempotency_key
        self.corrupt(json.dumps(value))
        coordinator = self.fixture.coordinator()
        try:
            self.assert_blocked(coordinator)
            with patch.object(coordinator, "_start_capture", return_value=self.original) as execute:
                result = coordinator.start_capture(forged)
                execute.assert_called_once()
                self.assertFalse(result.replayed)
        finally:
            coordinator.close()

    def test_invalid_legacy_embedded_receipt_stays_blocked_after_normalization(self):
        value = copy.deepcopy(self.durable)
        malformed = copy.deepcopy(self.receipt)
        malformed.pop("status")
        value["commands"] = [malformed]
        self.database.put("capture", value)
        with self.database.connection() as db, db:
            db.execute("DELETE FROM capture_commands")
        for _ in range(2):
            coordinator = self.fixture.coordinator()
            try:
                with patch.object(coordinator, "_start_capture") as execute:
                    self.assert_blocked(coordinator)
                    execute.assert_not_called()
            finally:
                coordinator.close()

    def test_bad_stop_receipt_blocks_both_stop_entrypoints_without_stopping_new_take(self):
        self.corrupt("{", operation="stop")
        coordinator = self.fixture.coordinator()
        try:
            coordinator.start_capture(fixtures.start_command("new-take"))
            active = coordinator._active
            for method in ("request_stop_capture", "stop_capture"):
                with self.subTest(method=method):
                    self.assert_blocked(coordinator, self.stop, method)
                    self.assertIs(coordinator._active, active)
                    self.assertIsNone(coordinator._async_stop_worker)
            coordinator.submit_frame(fixtures.frame())
            coordinator.stop_capture(fixtures.stop_command("new-stop"))
        finally:
            coordinator.close()

    def test_valid_start_and_stop_receipts_keep_original_replay_and_conflict(self):
        for _ in range(2):
            coordinator = self.fixture.coordinator()
            try:
                with patch.object(coordinator, "_start_capture") as execute:
                    result = coordinator.start_capture(self.command)
                    self.assertTrue(result.replayed)
                    self.assertEqual(result.body, self.original.body)
                    execute.assert_not_called()
                changed = fixtures.start_command(
                    self.command.idempotency_key, display_name="changed"
                )
                with self.assertRaises(ProviderError) as raised:
                    coordinator.start_capture(changed)
                self.assertEqual(raised.exception.code, "idempotency_conflict")
                self.assertTrue(coordinator.stop_capture(self.stop).replayed)
                self.assertTrue(coordinator.request_stop_capture(self.stop).replayed)
            finally:
                coordinator.close()

    def test_delete_receipt_cannot_hide_a_session_absent_from_its_request(self):
        coordinator, sid = self.fixture._retained_catalog_entry(
            recovered=True, prefix="unrelated-delete"
        )
        manifest = json.loads(
            (self.fixture.mountpoint / "recordings" / sid / "manifest.json").read_bytes()
        )
        coordinator._record_saved_prefix(sid, manifest)
        coordinator._persist_local_state()
        coordinator.close()
        durable = self.database.get("capture")
        retained = copy.deepcopy(durable["retained"][sid])
        requested = uuid7()
        body = {
            "schema": "ylx.session-delete-request.v1",
            "sessions": [{"session_id": requested, "manifest_sha256": "a" * 64}],
        }
        canonical = json.dumps(body)
        receipt = {
            "principal_id": "operator",
            "operation": "delete",
            "idempotency_key": "bad-delete",
            "canonical_body": canonical,
            "status": 200,
            "body": {
                "schema": "ylx.session-delete-result.v1",
                "deleted_session_ids": [sid],
                "failed_sessions": [],
            },
        }
        self.database.put_capture_state(durable, [receipt], set())
        command = CaptureCommand("operator", "bad-delete", body, canonical.encode())
        for _ in range(2):
            restarted = self.fixture.coordinator()
            try:
                self.assertEqual(restarted._retained[sid], retained)
                self.assert_blocked(restarted, command, "delete_sessions")
                self.assertEqual(self.database.get("capture")["retained"][sid], retained)
            finally:
                restarted.close()

    def test_unidentifiable_legacy_command_fails_closed_instead_of_disappearing(self):
        for bad in ([], {"operation": "start", "idempotency_key": "unknown-owner"}):
            with self.subTest(value=bad):
                value = copy.deepcopy(self.durable)
                value["commands"] = [bad]
                self.database.put("capture", value)
                with self.assertRaises(PersistenceError):
                    self.fixture.coordinator()
                self.assertEqual(self.database.get("capture")["commands"], [bad])

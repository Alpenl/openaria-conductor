"""Quarantine evidence must not be rewritten in every capture checkpoint."""

import copy
import json
import sqlite3
import unittest

from rp_ylx.api import ProviderError
from rp_ylx.persistence import PersistenceError
from tests import test_capture_coordinator as fixtures


class QuarantineCheckpointTest(unittest.TestCase):
    def setUp(self):
        self.fixture = fixtures.CaptureCoordinatorTest()
        self.fixture.setUp()
        self.addCleanup(self.fixture.tearDown)
        coordinator = self.fixture.coordinator()
        coordinator.close()
        self.database = coordinator._state_db
        self.state = self.database.get("capture")
        self.scope = ["operator", "start", "quarantined"]
        self.raw = "{" + " " * (512 * 1024)

    def assert_blocked(self, coordinator):
        with self.assertRaises(ProviderError) as raised:
            coordinator.start_capture(fixtures.start_command(self.scope[2]))
        self.assertEqual(raised.exception.code, "command_receipt_invalid")

    def raw_row(self):
        with self.database.connection() as db:
            return db.execute(
                "SELECT body FROM capture_commands WHERE principal=? "
                "AND operation=? AND idempotency_key=?",
                self.scope,
            ).fetchone()

    def assert_compact(self, coordinator):
        for _ in range(3):
            coordinator._next_revision()
            coordinator._persist_local_state()
            with self.database.connection() as db:
                size = db.execute(
                    "SELECT length(CAST(body AS BLOB)) FROM documents WHERE name='capture'"
                ).fetchone()[0]
            self.assertLess(size, 8192, "large evidence is being rewritten into capture state")
            self.assertNotIn(self.raw, json.dumps(coordinator._local_state_write()[1]))
            self.assert_blocked(coordinator)

    def test_large_sqlite_evidence_stays_out_of_repeated_checkpoints(self):
        with self.database.connection() as db, db:
            db.execute("INSERT INTO capture_commands VALUES (?,?,?,?)", (*self.scope, self.raw))
        for _ in range(2):
            coordinator = self.fixture.coordinator()
            try:
                self.assert_compact(coordinator)
                self.assertEqual(self.raw_row(), (self.raw,))
            finally:
                coordinator.close()

    def test_old_inline_evidence_moves_atomically_without_overwriting_existing_row(self):
        for existing in (None, "original SQL evidence"):
            with self.subTest(existing=existing):
                value = copy.deepcopy(self.state)
                value["invalid_commands"] = [{"scope": self.scope, "receipt": self.raw}]
                self.database.put("capture", value)
                with self.database.connection() as db, db:
                    db.execute("DELETE FROM capture_commands")
                    if existing is not None:
                        db.execute(
                            "INSERT INTO capture_commands VALUES (?,?,?,?)", (*self.scope, existing)
                        )
                for _ in range(2):
                    coordinator = self.fixture.coordinator()
                    try:
                        self.assert_compact(coordinator)
                        self.assertEqual(
                            self.raw_row(), (self.raw if existing is None else existing,)
                        )
                    finally:
                        coordinator.close()

    def test_failed_evidence_migration_preserves_inline_source_for_retry(self):
        for boundary in ("evidence", "document"):
            with self.subTest(boundary=boundary):
                value = copy.deepcopy(self.state)
                value["invalid_commands"] = [{"scope": self.scope, "receipt": self.raw}]
                self.database.put("capture", value)
                with self.database.connection() as db, db:
                    db.execute("DELETE FROM capture_commands")
                    original = db.execute(
                        "SELECT body FROM documents WHERE name='capture'"
                    ).fetchone()[0]
                    if boundary == "evidence":
                        db.execute(
                            "CREATE TRIGGER fail_quarantine BEFORE INSERT ON capture_commands "
                            "BEGIN SELECT RAISE(ABORT, 'injected migration failure'); END"
                        )
                    else:
                        db.execute(
                            "CREATE TRIGGER fail_quarantine BEFORE UPDATE ON documents "
                            "WHEN NEW.name='capture' BEGIN SELECT "
                            "RAISE(ABORT, 'injected document failure'); END"
                        )
                with self.assertRaises(sqlite3.IntegrityError):
                    self.fixture.coordinator()
                with self.database.connection() as db, db:
                    self.assertEqual(
                        db.execute("SELECT body FROM documents WHERE name='capture'").fetchone()[0],
                        original,
                    )
                    self.assertIsNone(self.raw_row())
                    db.execute("DROP TRIGGER fail_quarantine")
                coordinator = self.fixture.coordinator()
                try:
                    self.assert_compact(coordinator)
                    self.assertEqual(self.raw_row(), (self.raw,))
                finally:
                    coordinator.close()

    def test_quarantined_sqlite_row_cannot_be_replaced_or_evicted_as_normal_receipt(self):
        with self.database.connection() as db, db:
            db.execute("INSERT INTO capture_commands VALUES (?,?,?,?)", (*self.scope, self.raw))
        coordinator = self.fixture.coordinator()
        try:
            state = self.database.get("capture")
            command = fixtures.start_command(self.scope[2])
            replacement = {
                "principal_id": self.scope[0],
                "operation": self.scope[1],
                "idempotency_key": self.scope[2],
                "canonical_body": command.canonical_body.decode(),
                "status": 202,
                "body": coordinator.capture_status(),
            }
            with self.assertRaises(PersistenceError):
                self.database.put_capture_state(state, [replacement], {tuple(self.scope)})
            self.assertEqual(self.raw_row(), (self.raw,))
            self.database.put_capture_state(state, [], {tuple(self.scope)})
            self.assertEqual(self.raw_row(), (self.raw,))
            self.assert_blocked(coordinator)
        finally:
            coordinator.close()

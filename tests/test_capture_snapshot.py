import json
import tempfile
import unittest
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import patch

from rp_ylx.persistence import PersistenceError, StateDatabase


def command(revision):
    return {
        "principal_id": "operator",
        "operation": "start",
        "idempotency_key": f"request-{revision}",
        "canonical_body": "{}",
        "status": 202,
        "body": {"source_revision": revision},
    }


def scope(revision):
    return ("operator", "start", f"request-{revision}")


class CaptureSnapshotTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.store = StateDatabase(self.root / "state.sqlite3")
        self.store.initialize_document("capture", {"source_revision": 1}, "bootstrap")
        self.store.put_capture_state({"source_revision": 1}, [command(1)], set())
        self.writer = StateDatabase(self.store.path)

    def test_writer_committing_between_reads_cannot_mix_state_and_receipts(self):
        original = self.store.connection
        committed = []
        errors = []

        def trace(sql):
            if "FROM capture_commands" in sql and sql.startswith("SELECT") and not committed:
                committed.append(True)
                try:
                    self.writer.put_capture_state({"source_revision": 2}, [command(2)], {scope(1)})
                except Exception as error:
                    errors.append(error)

        @contextmanager
        def traced_connection():
            with original() as db:
                db.set_trace_callback(trace)
                yield db

        with patch.object(self.store, "connection", traced_connection):
            result = self.store.capture_state()
        self.assertEqual(committed, [True])
        self.assertEqual(errors, [], "a read snapshot must allow the WAL writer to commit")
        self.assertEqual(result, {"source_revision": 1, "commands": [command(1)]})
        self.assertEqual(
            self.store.capture_state(), {"source_revision": 2, "commands": [command(2)]}
        )

    def test_uncommitted_writer_is_not_visible_and_does_not_block_reader(self):
        with self.writer.connection() as db:
            db.execute("BEGIN IMMEDIATE")
            db.execute(
                "UPDATE documents SET body=? WHERE name='capture'", ('{"source_revision":2}',)
            )
            db.execute("DELETE FROM capture_commands")
            db.execute(
                "INSERT INTO capture_commands VALUES (?,?,?,?)", (*scope(2), json.dumps(command(2)))
            )
            self.assertEqual(
                self.store.capture_state(), {"source_revision": 1, "commands": [command(1)]}
            )
            db.commit()
        self.assertEqual(
            self.store.capture_state(), {"source_revision": 2, "commands": [command(2)]}
        )

    def test_rejected_receipt_releases_snapshot_for_checkpoint_and_future_reads(self):
        with self.writer.connection() as db, db:
            db.execute("UPDATE capture_commands SET body='{' ")
        result = self.store.capture_state()
        self.assertEqual(result["commands"], [])
        self.assertEqual(result["invalid_commands"], [{"scope": list(scope(1)), "receipt": "{"}])
        with self.writer.connection() as db:
            self.assertEqual(db.execute("SELECT body FROM capture_commands").fetchone()[0], "{")
        self.writer.put_capture_state({"source_revision": 2}, [command(2)], {scope(1)})
        with self.writer.connection() as db:
            db.execute("PRAGMA busy_timeout=50")
            self.assertEqual(db.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()[0], 0)
        self.assertEqual(
            self.store.capture_state(), {"source_revision": 2, "commands": [command(2)]}
        )

    def test_legacy_embedded_commands_remain_authoritative_until_migrated(self):
        legacy = {"source_revision": 7, "commands": [command(7)]}
        self.store.put("capture", legacy)
        with self.writer.connection() as db, db:
            db.execute("UPDATE capture_commands SET body='{' ")
        self.assertEqual(self.store.capture_state(), legacy)
        self.assertEqual(self.store.get("capture"), legacy)

    def test_receipt_order_and_persisted_revision_are_unchanged_by_reads(self):
        self.store.put_capture_state({"source_revision": 3}, [command(2), command(3)], set())
        with self.store.connection() as db:
            before = tuple(db.iterdump())
        for _ in range(3):
            self.assertEqual(
                self.store.capture_state(),
                {"source_revision": 3, "commands": [command(1), command(2), command(3)]},
            )
        with self.store.connection() as db:
            self.assertEqual(tuple(db.iterdump()), before)

    def test_missing_and_non_object_state_still_fail_closed(self):
        for value in (None, "[]", "null", "7"):
            with self.subTest(value=value):
                with self.store.connection() as db, db:
                    if value is None:
                        db.execute("DELETE FROM documents WHERE name='capture'")
                    else:
                        db.execute(
                            "INSERT OR REPLACE INTO documents VALUES ('capture',?,1,'test')",
                            (value,),
                        )
                with self.assertRaises(PersistenceError):
                    self.store.capture_state()

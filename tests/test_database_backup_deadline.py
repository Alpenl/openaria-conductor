import sqlite3
import tempfile
import threading
import time
import unittest
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import patch

from rp_ylx import persistence


class Cancelled(BaseException):
    pass


class DatabaseBackupDeadlineTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.store = persistence.StateDatabase(self.root / "state.sqlite3")
        self.store.put("capture", {"revision": 42})
        self.destination = self.root / "copy.sqlite3"

    @contextmanager
    def source_locked(self, seconds):
        with self.store.connection() as db:
            db.execute("PRAGMA journal_mode=DELETE")
        original = self.store.connection

        @contextmanager
        def connection():
            with original() as source:
                holder = sqlite3.connect(self.store.path, check_same_thread=False)
                holder.execute("BEGIN EXCLUSIVE")
                release = threading.Timer(seconds, holder.close)
                release.start()
                try:
                    yield source
                finally:
                    release.cancel()
                    release.join(timeout=3)
                    holder.close()

        with patch.object(self.store, "connection", connection):
            yield

    def test_locked_source_times_out_and_removes_incomplete_target(self):
        start = time.monotonic()
        with (
            self.source_locked(1),
            patch.object(persistence, "BACKUP_TIMEOUT_SECONDS", 0.15, create=True),
            self.assertRaisesRegex(persistence.PersistenceError, "backup.*deadline"),
        ):
            self.store.backup(self.destination)
        self.assertLess(time.monotonic() - start, 0.9)
        self.assertFalse(self.destination.exists())
        self.assertEqual(self.store.get("capture"), {"revision": 42})
        self.store.backup(self.destination)
        self.assertEqual(
            persistence.StateDatabase(self.destination).get("capture"), {"revision": 42}
        )

    def test_temporary_lock_can_clear_before_deadline(self):
        with (
            self.source_locked(0.1),
            patch.object(persistence, "BACKUP_TIMEOUT_SECONDS", 2, create=True),
        ):
            self.store.backup(self.destination)
        self.assertEqual(
            persistence.StateDatabase(self.destination).get("capture"), {"revision": 42}
        )

    def test_copying_pages_does_not_reset_overall_deadline(self):
        self.store.put("capture", {"padding": "x" * (2 * 1024**2)})
        with (
            patch.object(persistence, "BACKUP_TIMEOUT_SECONDS", 0.25, create=True),
            patch.object(persistence.time, "monotonic", side_effect=[0, 0, 1]),
            self.assertRaisesRegex(persistence.PersistenceError, "backup.*deadline"),
        ):
            self.store.backup(self.destination)
        self.assertFalse(self.destination.exists())
        self.assertEqual(len(self.store.get("capture")["padding"]), 2 * 1024**2)

    def test_progress_cancellation_closes_connections_and_removes_target(self):
        with (
            patch.object(persistence.time, "monotonic", side_effect=[0, Cancelled()]),
            self.assertRaises(Cancelled),
        ):
            self.store.backup(self.destination)
        self.assertFalse(self.destination.exists())
        with self.store.connection() as db:
            db.execute("PRAGMA busy_timeout=20")
            self.assertEqual(db.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()[0], 0)
        self.store.backup(self.destination)
        self.assertEqual(
            persistence.StateDatabase(self.destination).get("capture"), {"revision": 42}
        )


if __name__ == "__main__":
    unittest.main()

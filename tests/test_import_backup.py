"""A migration receipt requires an exact durable legacy recovery copy."""

import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import rp_ylx.persistence as persistence
from rp_ylx.persistence import PersistenceError, StateDatabase


class ImportBackupTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.source = self.root / "legacy.json"
        self.raw = b'{"answer":42,"padding":"original recovery source"}\n'
        self.source.write_bytes(self.raw)
        self.backup = self.root / "legacy.json.pre-sqlite"
        self.store = StateDatabase(self.root / "state.sqlite3")

    def assert_uncommitted(self):
        self.assertIsNone(self.store.get("state"))
        with self.store.connection() as db:
            self.assertIsNone(db.execute("SELECT 1 FROM imports WHERE name='state'").fetchone())
        self.assertEqual(self.source.read_bytes(), self.raw)

    def retry(self):
        self.assertEqual(self.store.import_json("state", self.source)["answer"], 42)
        self.assertEqual(self.backup.read_bytes(), self.raw)
        self.assertEqual(self.backup.stat().st_mode & 0o777, 0o600)

    def test_short_backup_write_cannot_commit_and_retry_succeeds(self):
        opener = os.fdopen

        class ShortWrite:
            def __init__(self, fd, mode):
                self.stream = opener(fd, mode)

            def __enter__(self):
                return self

            def __exit__(self, *args):
                self.stream.close()

            def __getattr__(self, name):
                return getattr(self.stream, name)

            def write(self, data):
                return self.stream.write(data[: len(data) // 2])

        with patch("rp_ylx.persistence.os.fdopen", ShortWrite), self.assertRaises(OSError):
            self.store.import_json("state", self.source)
        self.assert_uncommitted()
        self.assertFalse(self.backup.exists())
        self.retry()

    def test_backup_sync_failure_leaves_no_published_partial_copy(self):
        with (
            patch("rp_ylx.persistence.os.fsync", side_effect=OSError("sync failed")),
            self.assertRaises(OSError),
        ):
            self.store.import_json("state", self.source)
        self.assert_uncommitted()
        self.assertFalse(self.backup.exists())
        self.retry()

    def test_parent_sync_failure_is_not_bypassed_on_retry(self):
        with patch("rp_ylx.persistence._sync_parent", side_effect=OSError("directory sync failed")):
            for attempt in range(2):
                with self.subTest(attempt=attempt), self.assertRaises(OSError):
                    self.store.import_json("state", self.source)
                self.assert_uncommitted()
        self.retry()

    def test_incomplete_preexisting_backup_is_not_trusted_or_overwritten(self):
        self.backup.write_bytes(self.raw[:8])
        with self.assertRaises(PersistenceError):
            self.store.import_json("state", self.source)
        self.assert_uncommitted()
        self.assertEqual(self.backup.read_bytes(), self.raw[:8])

    def test_exact_existing_copy_can_resume_interrupted_import(self):
        self.backup.write_bytes(self.raw)
        self.backup.chmod(0o600)
        self.retry()

    def test_unsafe_existing_backup_never_blocks_or_commits(self):
        for kind in ("symlink", "fifo", "directory"):
            with self.subTest(kind=kind):
                descriptors_before = len(list(Path("/proc/self/fd").iterdir()))
                if kind == "symlink":
                    self.backup.symlink_to(self.source)
                elif kind == "fifo":
                    os.mkfifo(self.backup)
                else:
                    self.backup.mkdir()
                try:
                    with self.assertRaises((OSError, PersistenceError)):
                        self.store.import_json("state", self.source)
                    self.assert_uncommitted()
                finally:
                    if kind == "directory":
                        self.backup.rmdir()
                    else:
                        self.backup.unlink()
                self.assertEqual(len(list(Path("/proc/self/fd").iterdir())), descriptors_before)
        self.retry()

    def test_backup_publication_failure_leaves_source_retryable(self):
        with (
            patch("rp_ylx.persistence.os.link", side_effect=OSError("publication failed")),
            self.assertRaises(OSError),
        ):
            self.store.import_json("state", self.source)
        self.assert_uncommitted()
        self.assertFalse(self.backup.exists())
        self.assertEqual(list(self.root.glob(".*.tmp")), [])
        self.retry()

    def test_competing_copy_is_verified_and_never_overwritten(self):
        original_link = os.link
        for matches in (False, True):
            with self.subTest(matches=matches):
                winning = self.raw if matches else b"other recovery evidence"

                def competing_link(source, destination, expected=winning, **kwargs):
                    self.backup.write_bytes(expected)
                    self.backup.chmod(0o600)
                    return original_link(source, destination, **kwargs)

                with patch("rp_ylx.persistence.os.link", side_effect=competing_link):
                    if matches:
                        self.retry()
                    else:
                        with self.assertRaises(PersistenceError):
                            self.store.import_json("state", self.source)
                        self.assert_uncommitted()
                self.assertEqual(self.backup.read_bytes(), winning)
                self.assertEqual(list(self.root.glob(".*.tmp")), [])
                self.backup.unlink()

    def test_process_exit_at_backup_publication_leaves_retryable_import(self):
        code = """
import os,runpy,sys
from pathlib import Path
module=runpy.run_path(sys.argv[1])
root=Path(sys.argv[2]);stage=sys.argv[3]
store=module['StateDatabase'](root/'state.sqlite3')
link=os.link
def die(source,destination,**kwargs):
    if stage=='after':link(source,destination,**kwargs)
    os._exit(77)
os.link=die
store.import_json('state',root/'legacy.json')
raise RuntimeError('exit injection was not reached')
"""
        for stage in ("before", "after"):
            with self.subTest(stage=stage):
                root = self.root / stage
                root.mkdir()
                source = root / "legacy.json"
                source.write_bytes(self.raw)
                result = subprocess.run(
                    [sys.executable, "-c", code, persistence.__file__, str(root), stage],
                    capture_output=True,
                    text=True,
                    timeout=15,
                )
                self.assertEqual(result.returncode, 77, result.stderr)
                store = StateDatabase(root / "state.sqlite3")
                self.assertIsNone(store.get("state"))
                with store.connection() as db:
                    self.assertIsNone(db.execute("SELECT 1 FROM imports").fetchone())
                backup = root / "legacy.json.pre-sqlite"
                self.assertEqual(backup.exists(), stage == "after")
                if backup.exists():
                    self.assertEqual(backup.read_bytes(), self.raw)
                self.assertEqual(store.import_json("state", source)["answer"], 42)
                self.assertEqual(backup.read_bytes(), self.raw)
                self.assertEqual(source.read_bytes(), self.raw)

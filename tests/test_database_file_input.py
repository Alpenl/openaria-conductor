"""Reject nonregular database inputs before SQLite can block or follow them."""

import multiprocessing
import os
import socket
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from contextlib import closing
from pathlib import Path
from unittest.mock import patch

from rp_ylx import database_admin
from rp_ylx.persistence import PersistenceError, StateDatabase


class DatabaseFileInputTest(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name)
        self.real = self.root / "real.sqlite3"
        self.database = StateDatabase(self.real)
        self.database.put("capture", {"revision": 7})
        self.source = self.root / "input.sqlite3"
        self.output = self.root / "output.sqlite3"
        self.destination = self.root / "state.sqlite3"
        StateDatabase(self.destination).put("capture", {"revision": 42})

    def attempt(self, action):
        if action == "check":
            database_admin.inspect(self.source)
        elif action == "backup":
            StateDatabase.backup_readonly(self.source, self.output)
        elif action == "restore":
            with patch.object(database_admin, "require_offline"):
                database_admin.restore(self.source, self.destination)
        else:
            self.database.get("capture")

    def assert_rejected(self, action):
        # A real FIFO open is uninterruptible by SQLite busy_timeout. Isolate
        # the call and reap it even when testing the unfixed implementation.
        context = multiprocessing.get_context("fork")
        receiver, sender = context.Pipe(duplex=False)

        def run():
            receiver.close()
            try:
                self.attempt(action)
            except PersistenceError:
                sender.send("rejected")
            except Exception as error:
                sender.send(type(error).__name__)
            else:
                sender.send("accepted")
            finally:
                sender.close()

        process = context.Process(target=run)
        process.start()
        sender.close()
        try:
            process.join(5)
            self.assertFalse(process.is_alive(), "database open blocked on nonregular input")
            self.assertEqual(process.exitcode, 0)
            self.assertTrue(receiver.poll(1), "child exited without a result")
            self.assertEqual(receiver.recv(), "rejected")
        finally:
            if process.is_alive():
                process.kill()
            process.join(5)
            process.close()
            receiver.close()

    def assert_target_preserved(self):
        self.assertFalse(self.output.exists())
        self.assertEqual(StateDatabase(self.destination).get("capture"), {"revision": 42})
        self.assertEqual(list(self.root.glob("*.before-restore-*")), [])
        self.assertEqual(list(self.root.glob("*.restore-*")), [])
        self.assertEqual(list(self.root.glob(".*.backup-*.tmp")), [])

    def test_fifo_is_rejected_by_check_backup_and_restore(self):
        os.mkfifo(self.source)
        for action in ("check", "backup", "restore"):
            with self.subTest(action=action):
                self.assert_rejected(action)
                self.assert_target_preserved()

    def test_symlink_is_rejected_without_following_valid_database(self):
        self.source.symlink_to(self.real)
        before = self.real.read_bytes()
        for action in ("check", "backup", "restore"):
            with self.subTest(action=action):
                self.assert_rejected(action)
                self.assert_target_preserved()
                self.assertEqual(self.real.read_bytes(), before)

    def test_directory_socket_and_character_device_are_rejected(self):
        self.source.mkdir()
        sock = socket.socket(socket.AF_UNIX)
        self.addCleanup(sock.close)
        socket_path = self.root / "socket"
        sock.bind(str(socket_path))
        for path in (self.source, socket_path, Path("/dev/null")):
            for action in ("check", "backup"):
                with self.subTest(path=path, action=action):
                    with patch.object(self, "source", path):
                        self.assert_rejected(action)
                    self.assert_target_preserved()

    def test_missing_and_dangling_sources_are_never_initialized(self):
        for action in ("check", "backup", "restore"):
            with self.subTest(action=action):
                with self.assertRaises(FileNotFoundError):
                    self.attempt(action)
                self.assertFalse(self.source.exists())
                self.assert_target_preserved()
        missing = self.root / "absent.sqlite3"
        self.source.symlink_to(missing)
        self.assert_rejected("backup")
        self.assertFalse(missing.exists())
        self.assert_target_preserved()

    def test_restore_rechecks_source_replaced_after_inspection(self):
        self.database.backup(self.source)
        inspect = database_admin.inspect

        def replace_after_inspect(path):
            result = inspect(path)
            if path == self.source:
                self.source.unlink()
                os.mkfifo(self.source)
            return result

        with patch.object(database_admin, "inspect", replace_after_inspect):
            self.assert_rejected("restore")
        self.assert_target_preserved()

    def test_existing_authority_rejects_replacement_fifo_and_symlink(self):
        saved = self.real.with_name("saved.sqlite3")
        self.real.rename(saved)
        os.mkfifo(self.real)
        self.assert_rejected("read")
        self.real.unlink()
        self.real.symlink_to(saved)
        self.assert_rejected("read")
        self.assertEqual(StateDatabase(saved).get("capture"), {"revision": 7})

    def test_readonly_maintenance_preserves_another_connections_write_lock(self):
        self.database.backup(self.source)
        with closing(sqlite3.connect(self.source)) as writer:
            writer.execute("BEGIN IMMEDIATE")
            database_admin.inspect(self.source)
            StateDatabase.backup_readonly(self.source, self.output)
            # An unrelated raw open/close of the source can release all of this
            # process's POSIX locks. Verify with a genuinely separate process.
            code = """import sqlite3,sys
connection=sqlite3.connect(sys.argv[1], timeout=0)
try:
    connection.execute('BEGIN IMMEDIATE')
except sqlite3.OperationalError as error:
    if 'locked' not in str(error): raise
    print('locked')
else:
    raise AssertionError('existing writer lock was released')
finally:
    connection.close()
"""
            result = subprocess.run(
                [sys.executable, "-c", code, str(self.source)],
                capture_output=True,
                text=True,
                timeout=10,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(result.stdout.strip(), "locked")
            writer.rollback()
        self.assertEqual(StateDatabase(self.output).get("capture"), {"revision": 7})

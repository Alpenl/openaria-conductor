"""Focus preferences and their successful command receipts commit together."""

import copy
import multiprocessing
import os
import sqlite3
import threading
import unittest
from unittest.mock import patch

from tests import test_capture_coordinator as fixtures
from tests.test_focus_restore import FocusSources, focus_command


class FocusAtomicTest(unittest.TestCase):
    def setUp(self):
        self.fixture = fixtures.CaptureCoordinatorTest()
        self.fixture.setUp()
        self.addCleanup(self.fixture.tearDown)
        initial = self.fixture.coordinator()
        initial.close()
        self.database = initial._state_db
        self.database.put("camera_focus", {"value": 42, "auto_enabled": False}, history=True)

    def coordinator(self):
        sources = FocusSources()
        coordinator = self.fixture.coordinator(sources=sources)
        self.addCleanup(coordinator.close)
        sources.calls.clear()
        return coordinator, sources

    def trigger(self, table, condition=""):
        with self.database.connection() as db, db:
            db.execute(
                f"CREATE TRIGGER fail_focus_commit BEFORE INSERT ON {table} "
                f"{condition} BEGIN SELECT RAISE(ABORT, 'injected commit failure'); END"
            )

    def clear_trigger(self):
        with self.database.connection() as db, db:
            db.execute("DROP TRIGGER fail_focus_commit")

    def durable(self):
        with self.database.connection() as db:
            return {
                "documents": db.execute("SELECT * FROM documents ORDER BY name").fetchall(),
                "history": db.execute("SELECT * FROM changes ORDER BY rowid").fetchall(),
                "commands": db.execute("SELECT * FROM capture_commands ORDER BY rowid").fetchall(),
            }

    def test_receipt_insert_failure_rolls_back_focus_and_retry_does_not_touch_hardware(self):
        coordinator, sources = self.coordinator()
        before = self.durable()
        command = focus_command("receipt-fails", 77)
        self.trigger("capture_commands")
        try:
            for _ in range(2):
                with self.assertRaises(sqlite3.IntegrityError):
                    coordinator.set_camera_focus(command)
                self.assertEqual(self.durable(), before)
            self.assertEqual(sources.calls, [(77, None)])
        finally:
            self.clear_trigger()
        response = coordinator.set_camera_focus(command)
        self.assertTrue(response.replayed)
        self.assertEqual(sources.calls, [(77, None)])
        self.assertEqual(self.database.get("camera_focus")["value"], 77)
        coordinator.close()
        restarted, sources = self.coordinator()
        self.assertTrue(restarted.set_camera_focus(command).replayed)
        self.assertEqual(sources.calls, [])

    def test_focus_insert_failure_rolls_back_receipt_and_preserves_retry_without_reapply(self):
        coordinator, sources = self.coordinator()
        before = self.durable()
        command = focus_command("focus-row-fails", 88)
        self.trigger("documents", "WHEN NEW.name='camera_focus'")
        try:
            for _ in range(2):
                with self.assertRaises(sqlite3.IntegrityError):
                    coordinator.set_camera_focus(command)
                self.assertEqual(self.durable(), before)
            self.assertEqual(sources.calls, [(88, None)])
        finally:
            self.clear_trigger()
        self.assertTrue(coordinator.set_camera_focus(command).replayed)
        self.assertEqual(sources.calls, [(88, None)])
        self.assertEqual(self.database.get("camera_focus")["value"], 88)

    def test_later_success_commits_latest_focus_and_both_ordered_receipts(self):
        coordinator, sources = self.coordinator()
        first, second = focus_command("first", 77), focus_command("second", 88)
        self.trigger("capture_commands")
        try:
            with self.assertRaises(sqlite3.IntegrityError):
                coordinator.set_camera_focus(first)
        finally:
            self.clear_trigger()
        coordinator.set_camera_focus(second)
        self.assertEqual(sources.calls, [(77, None), (88, None)])
        self.assertEqual(self.database.get("camera_focus")["value"], 88)
        coordinator.close()
        restarted, sources = self.coordinator()
        self.assertEqual(restarted.set_camera_focus(first).body["value"], 77)
        self.assertEqual(restarted.set_camera_focus(second).body["value"], 88)
        self.assertEqual(sources.calls, [])
        self.assertEqual(self.database.get("camera_focus")["value"], 88)

    def test_startup_does_not_derive_preference_from_historical_receipts(self):
        coordinator, _ = self.coordinator()
        coordinator.set_camera_focus(focus_command("old-focus", 77))
        coordinator.close()
        self.database.put("camera_focus", {"value": 99, "auto_enabled": False}, history=True)
        restarted, sources = self.coordinator()
        self.assertEqual(restarted.camera_focus_status()["value"], 99)
        self.assertEqual(self.database.get("camera_focus")["value"], 99)
        self.assertTrue(restarted.set_camera_focus(focus_command("old-focus", 77)).replayed)
        self.assertEqual(sources.calls, [])
        self.assertEqual(self.database.get("camera_focus")["value"], 99)

    def test_delayed_state_snapshot_cannot_restore_older_focus(self):
        coordinator, sources = self.coordinator()
        coordinator.set_camera_focus(focus_command("earlier", 55))
        entered, release = threading.Event(), threading.Event()
        real_lock = threading.Lock()
        errors = []

        class DelayedLock:
            def __enter__(self):
                if threading.current_thread().name == "old-focus-snapshot":
                    entered.set()
                    if not release.wait(5):
                        raise TimeoutError("delayed focus snapshot")
                real_lock.acquire()

            def __exit__(self, *args):
                real_lock.release()

        def save():
            try:
                coordinator._persist_local_state()
            except BaseException as error:
                errors.append(error)

        thread = threading.Thread(target=save, name="old-focus-snapshot")
        try:
            with patch.object(coordinator, "_state_persist_lock", DelayedLock()):
                thread.start()
                self.assertTrue(entered.wait(2))
                coordinator.set_camera_focus(focus_command("later", 99))
                after = copy.deepcopy(self.durable())
                release.set()
                thread.join(3)
                self.assertFalse(thread.is_alive())
                self.assertEqual(errors, [])
                self.assertEqual(self.durable(), after)
                self.assertEqual(self.database.get("camera_focus")["value"], 99)
        finally:
            release.set()
            thread.join(6)
        self.assertEqual(sources.calls, [(55, None), (99, None)])

    def test_process_exit_after_commit_keeps_preference_and_replay_together(self):
        command = focus_command("committed-before-exit", 77)

        def child():
            coordinator = self.fixture.coordinator(sources=FocusSources())
            original = coordinator._state_db.put_capture_state

            def commit_then_exit(*args, **kwargs):
                original(*args, **kwargs)
                os._exit(75)

            coordinator._state_db.put_capture_state = commit_then_exit
            coordinator.set_camera_focus(command)
            os._exit(76)

        process = multiprocessing.get_context("fork").Process(target=child)
        process.start()
        try:
            process.join(10)
            self.assertEqual(process.exitcode, 75)
        finally:
            if process.is_alive():
                process.kill()
            process.join(5)
            process.close()
        self.assertEqual(self.database.get("camera_focus")["value"], 77)
        restarted, sources = self.coordinator()
        result = restarted.set_camera_focus(command)
        self.assertTrue(result.replayed)
        self.assertEqual(result.body["value"], 77)
        self.assertEqual(sources.calls, [])

    def test_history_failure_rolls_back_all_rows_and_checkpoints_do_not_rewrite_focus(self):
        coordinator, sources = self.coordinator()
        before = self.durable()
        command = focus_command("history-fails", 66)
        self.trigger("changes", "WHEN NEW.name='camera_focus'")
        try:
            with self.assertRaises(sqlite3.IntegrityError):
                coordinator.set_camera_focus(command)
            self.assertEqual(self.durable(), before)
        finally:
            self.clear_trigger()
        self.assertTrue(coordinator.set_camera_focus(command).replayed)
        self.assertEqual(sources.calls, [(66, None)])
        after = self.durable()
        for _ in range(3):
            coordinator._next_revision()
            coordinator._persist_local_state()
        current = self.durable()
        self.assertEqual(current["history"], after["history"])
        self.assertEqual(current["commands"], after["commands"])
        self.assertEqual(
            [row for row in current["documents"] if row[0] == "camera_focus"],
            [row for row in after["documents"] if row[0] == "camera_focus"],
        )

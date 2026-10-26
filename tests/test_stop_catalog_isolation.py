import fcntl
import os
import sqlite3
import threading
import unittest
from contextlib import ExitStack
from unittest.mock import patch

from rp_ylx.api import ProviderError
from tests import test_capture_coordinator as fixtures


class StopCatalogIsolationTest(unittest.TestCase):
    def exercise(self, stage, reason="user", late_failure=False):
        fixture = fixtures.CaptureCoordinatorTest()
        fixture.setUp()
        self.addCleanup(fixture.tearDown)
        first = fixture.coordinator()
        try:
            old_sid = fixture.seal_one(first, prefix="old")
        finally:
            first.close()
        coordinator = fixture.coordinator()
        self.addCleanup(coordinator.close)
        coordinator.start_capture(fixtures.start_command("active"))
        self.assertTrue(coordinator.submit_frame(fixtures.frame()))
        recorder, plan = coordinator._active, coordinator._active_plan
        waiting, entered, release = threading.Event(), threading.Event(), threading.Event()
        outcomes, errors, workers = {}, {}, []

        def launch(name, call):
            done = threading.Event()

            def target():
                try:
                    outcomes[name] = call()
                except BaseException as error:
                    errors[name] = error
                finally:
                    done.set()

            thread = threading.Thread(target=target, name=name + "-worker")
            workers.append(thread)
            thread.start()
            return done

        original_lock = coordinator._catalog_lock

        class ObservedLock:
            def __enter__(self):
                if threading.current_thread().name == "stop-worker":
                    waiting.set()
                original_lock.acquire()
                return self

            def __exit__(self, *args):
                original_lock.release()

        signature = coordinator._catalog_db.signature

        def delayed_signature(directory):
            if directory.name == old_sid and threading.current_thread().name == "list-worker":
                entered.set()
                if not release.wait(10):
                    raise TimeoutError("test directory gate")
            return signature(directory)

        remember = coordinator._catalog_db.remember

        def observed_remember(*args, **kwargs):
            entered.set()
            if stage == "cache-error":
                if not release.wait(10):
                    raise TimeoutError("test catalog gate")
                raise sqlite3.OperationalError("cache unavailable")
            return remember(*args, **kwargs)

        database = None
        with ExitStack() as cleanup:
            try:
                if stage == "list":
                    coordinator._catalog_lock = ObservedLock()
                    cleanup.enter_context(
                        patch.object(
                            coordinator._catalog_db,
                            "signature",
                            side_effect=delayed_signature,
                        )
                    )
                    launch(
                        "list",
                        lambda: coordinator.list_sessions(
                            cursor=None,
                            limit=50,
                            take_id=None,
                            api_version="v4",
                        ),
                    )
                    self.assertTrue(entered.wait(3))
                else:
                    cleanup.enter_context(
                        patch.object(
                            coordinator._catalog_db,
                            "remember",
                            side_effect=observed_remember,
                        )
                    )
                    if stage == "sqlite":
                        database = sqlite3.connect(fixture.state_root / "catalog.sqlite3")
                        database.execute("BEGIN IMMEDIATE")
                command = fixtures.stop_command("stop", reason=reason)
                stop_done = launch("stop", lambda: coordinator.stop_capture(command))
                self.assertTrue((waiting if stage == "list" else entered).wait(3))
                status_done = launch("status", coordinator.capture_status)
                self.assertTrue(status_done.wait(0.25), "catalog wait held the state lock")
                self.assertNotIn("status", errors)
                fixtures.validate_capture_status(outcomes["status"])
                self.assertEqual(outcomes["status"]["snapshot"]["device_state"], "finalizing")
                self.assertIs(coordinator._active, recorder)
                self.assertFalse(stop_done.is_set())
                self.assertIsNone(coordinator.current_safe_swap_receipt())
                peer = os.open(coordinator._network_operation_lock_path(), os.O_RDONLY)
                try:
                    with self.assertRaises(BlockingIOError):
                        fcntl.flock(peer, fcntl.LOCK_EX | fcntl.LOCK_NB)
                finally:
                    os.close(peer)
                start_done = launch(
                    "start",
                    lambda: coordinator.start_capture(
                        fixtures.start_command("forbidden-new-take"),
                    ),
                )
                self.assertTrue(start_done.wait(0.25))
                self.assertIsInstance(errors.get("start"), ProviderError)
                self.assertEqual(errors.pop("start").code, "capture_busy")
                if late_failure:
                    failed = launch(
                        "source-failure",
                        lambda: coordinator._source_failure(
                            plan.generation_id,
                            "source_late_error",
                            "late failure",
                            expected_plan=plan,
                        ),
                    )
                    self.assertTrue(failed.wait(0.25))
                else:
                    duplicate = launch("duplicate", lambda: coordinator.stop_capture(command))
                    self.assertFalse(duplicate.wait(0.05))
            finally:
                release.set()
                if database is not None:
                    database.rollback()
                    database.close()
                for thread in workers:
                    thread.join(5)
                self.assertTrue(all(not thread.is_alive() for thread in workers))
        if late_failure:
            self.assertIsInstance(errors.pop("stop"), ProviderError)
            self.assertEqual(coordinator.capture_status()["snapshot"]["device_state"], "idle")
            retained = coordinator.retained_unsuccessful_outcome(plan.session_id)
            self.assertIsNotNone(retained)
        else:
            expected = 204 if reason == "safe_swap" else 202
            self.assertEqual(outcomes["stop"].status, expected)
            self.assertEqual(outcomes["duplicate"].status, expected)
            self.assertTrue(outcomes["duplicate"].replayed)
            if reason == "safe_swap":
                self.assertIsNotNone(coordinator.current_safe_swap_receipt())
            else:
                page = coordinator.list_sessions(
                    cursor=None,
                    limit=50,
                    take_id=None,
                    api_version="v4",
                )
                self.assertIn(plan.session_id, [item["session_id"] for item in page["items"]])
        self.assertFalse(errors, errors)
        self.assertIsNone(coordinator._network_operation_lease)
        self.assertIsNone(coordinator._stop_inflight)

    def test_list_wait_keeps_status_available_and_stop_ownership(self):
        self.exercise("list")

    def test_sqlite_writer_wait_keeps_status_available_and_stop_ownership(self):
        self.exercise("sqlite")

    def test_safe_swap_receipt_waits_for_catalog_and_stop_commit(self):
        self.exercise("list", reason="safe_swap")

    def test_catalog_io_failure_defers_cache_without_losing_sealed_session(self):
        self.exercise("cache-error")

    def test_source_failure_during_catalog_wait_cannot_publish_stop_success(self):
        self.exercise("list", late_failure=True)

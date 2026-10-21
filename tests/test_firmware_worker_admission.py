from __future__ import annotations

import json
import os
import threading
import unittest
import uuid
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import Mock, patch

import tests.test_firmware_control as fixtures
from rp_ylx import firmware_control as control


class FirmwareWorkerAdmissionTest(unittest.TestCase):
    def setUp(self):
        self.fixture = fixtures.FirmwareControlTest()
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        self.s = self.fixture.supervisor
        self.s.runner = Mock(side_effect=self.install)
        self.workers = []

    def install(self, task):
        self.fixture.make_release("c", "0.2.2", "current")

    def start(self, key=None):
        return self.s.start("update", key or str(uuid.uuid4()), "c" * 40)

    def join_worker(self):
        worker = self.s._worker
        if worker is not None:
            worker.join(3)
            self.assertFalse(worker.is_alive())
        self.assertIsNone(self.s._worker)

    def assert_saved(self, status):
        task = self.s.task
        for path in [
            self.s.state / "task.json",
            self.s.state / "receipts" / (task["id"] + ".json"),
        ]:
            self.assertEqual(json.loads(path.read_bytes())["status"], status)

    def assert_recovered_start(self):
        self.assertEqual(self.start()["status"], "accepted")
        self.join_worker()
        self.assertEqual(self.s.task["status"], "succeeded")
        self.assert_saved("succeeded")

    def test_constructor_failure_finishes_task_and_allows_new_request(self):
        key = str(uuid.uuid4())
        with (
            patch.object(control.threading, "Thread", side_effect=RuntimeError("cannot create")),
            self.assertRaises(control.FirmwareError) as raised,
        ):
            self.start(key)
        self.assertEqual(
            (raised.exception.code, raised.exception.status), ("update_start_failed", 503)
        )
        self.s.runner.assert_not_called()
        self.assertEqual(self.start(key)["status"], "failed")
        self.assert_saved("failed")
        self.assert_recovered_start()

    def test_start_failure_does_not_leave_accepted_receipt_or_busy_slot(self):
        key = str(uuid.uuid4())
        with (
            patch.object(threading.Thread, "start", side_effect=RuntimeError("cannot start")),
            self.assertRaises(control.FirmwareError),
        ):
            self.start(key)
        self.assertIsNone(self.s._worker)
        self.assertEqual(self.start(key)["status"], "failed")
        self.assert_saved("failed")
        self.s.runner.assert_not_called()
        self.assert_recovered_start()

    def test_exception_after_real_thread_start_cancels_before_runner_admission(self):
        real_start = threading.Thread.start
        for error in [RuntimeError("after start"), KeyboardInterrupt(), SystemExit(7)]:
            with self.subTest(error=type(error).__name__):
                workers = []

                def start_then_fail(worker, workers=workers, error=error):
                    real_start(worker)
                    workers.append(worker)
                    raise error

                key = str(uuid.uuid4())
                expected = control.FirmwareError if isinstance(error, Exception) else type(error)
                with (
                    patch.object(threading.Thread, "start", start_then_fail),
                    self.assertRaises(expected),
                ):
                    self.start(key)
                for worker in workers:
                    worker.join(3)
                    self.assertFalse(worker.is_alive())
                self.assertIsNone(self.s._worker)
                self.assertEqual(self.start(key)["status"], "failed")
                self.assert_saved("failed")
                self.s.runner.assert_not_called()

    def test_constructor_cancellation_is_preserved_and_persisted_as_failed(self):
        error = KeyboardInterrupt()
        with (
            patch.object(control.threading, "Thread", side_effect=error),
            self.assertRaises(KeyboardInterrupt) as raised,
        ):
            self.start()
        self.assertIs(raised.exception, error)
        self.assert_saved("failed")
        self.s.runner.assert_not_called()

    def test_initial_persistence_failure_never_starts_installer(self):
        real_save = control.atomic_json

        def save(path, value):
            if value["status"] == "accepted":
                raise OSError("accepted write failed")
            return real_save(path, value)

        with (
            patch.object(control, "atomic_json", side_effect=save),
            self.assertRaises(control.FirmwareError),
        ):
            self.start()
        self.assert_saved("failed")
        self.s.runner.assert_not_called()
        self.assert_recovered_start()

    def test_storage_outage_keeps_failed_memory_and_blocks_new_work_until_repaired(self):
        key = str(uuid.uuid4())
        with patch.object(control, "atomic_json", side_effect=OSError("disk full")):
            with self.assertRaises(control.FirmwareError):
                self.start(key)
            self.assertEqual(self.start(key)["status"], "failed")
            self.assertIsNotNone(self.s.status()["warning"])
            with self.assertRaises(control.FirmwareError) as raised:
                self.start()
            self.assertEqual(raised.exception.code, "update_state_unavailable")
        self.s.runner.assert_not_called()
        self.assertIsNone(self.s.status()["warning"])
        self.assert_saved("failed")
        self.assert_recovered_start()

    def test_receipt_write_failure_is_repaired_before_another_task_can_replace_it(self):
        key = str(uuid.uuid4())
        real_save = control.atomic_json

        def save(path, value):
            if path.parent.name == "receipts":
                raise OSError("receipt write failed")
            return real_save(path, value)

        with patch.object(control, "atomic_json", side_effect=save):
            with self.assertRaises(control.FirmwareError):
                self.start(key)
            self.assertEqual(self.s.task["status"], "failed")
            self.assertEqual(self.start(key)["status"], "failed")
            with self.assertRaises(control.FirmwareError) as raised:
                self.start()
            self.assertEqual(raised.exception.code, "update_state_unavailable")
        self.s.status()
        self.assert_saved("failed")
        self.assert_recovered_start()

    def test_running_persistence_failure_finishes_without_invoking_runner(self):
        real_save = control.atomic_json

        def save(path, value):
            if value["status"] == "running":
                raise OSError("running write failed")
            return real_save(path, value)

        with patch.object(control, "atomic_json", side_effect=save):
            self.start()
            self.join_worker()
        self.s.runner.assert_not_called()
        self.assert_saved("failed")
        self.assert_recovered_start()

    def test_failed_result_survives_terminal_persistence_outage(self):
        self.s.runner.side_effect = RuntimeError("installer failed")
        real_save = control.atomic_json

        def save(path, value):
            if value["status"] == "failed":
                raise OSError("terminal write failed")
            return real_save(path, value)

        key = str(uuid.uuid4())
        with patch.object(control, "atomic_json", side_effect=save):
            self.start(key)
            self.join_worker()
            self.assertEqual(self.start(key)["status"], "failed")
            self.assertIsNotNone(self.s.status()["warning"])
        self.s.status()
        self.assert_saved("failed")
        self.s.runner.assert_called_once()

    def test_success_is_not_lost_and_callback_waits_for_durable_receipt(self):
        self.s.on_success = Mock()
        real_save = control.atomic_json

        def save(path, value):
            if value["status"] == "succeeded":
                raise OSError("terminal write failed")
            return real_save(path, value)

        key = str(uuid.uuid4())
        with patch.object(control, "atomic_json", side_effect=save):
            self.start(key)
            self.join_worker()
            self.assertEqual(self.start(key)["status"], "succeeded")
            self.assertIsNotNone(self.s.status()["warning"])
            self.s.on_success.assert_not_called()
        self.s.status()
        self.assert_saved("succeeded")
        self.s.on_success.assert_called_once()
        self.start(key)
        self.s.status()
        self.s.on_success.assert_called_once()
        self.s.runner.assert_called_once()

    def test_reload_callback_failure_is_observable_and_retried_without_reinstalling(self):
        self.s.on_success = Mock(side_effect=[OSError("reload failed"), None])
        self.start()
        self.join_worker()
        self.assertEqual(self.s.task["status"], "succeeded")
        self.assertTrue(self.s._success_notification_pending)
        self.s.status()
        self.assertFalse(self.s._success_notification_pending)
        self.assertEqual(self.s.on_success.call_count, 2)
        self.s.status()
        self.assertEqual(self.s.on_success.call_count, 2)
        self.s.runner.assert_called_once()

    def test_running_callback_keeps_slot_owned_and_is_not_duplicated(self):
        entered, release = threading.Event(), threading.Event()

        def callback():
            entered.set()
            self.assertTrue(release.wait(3))

        self.s.on_success = Mock(side_effect=callback)
        try:
            self.start()
            self.assertTrue(entered.wait(3))
            self.s.status()
            with self.assertRaises(control.FirmwareError) as raised:
                self.start()
            self.assertEqual(raised.exception.code, "update_busy")
            self.s.on_success.assert_called_once()
        finally:
            release.set()
            self.join_worker()

    def test_restart_repairs_stale_active_receipt_for_terminal_task(self):
        key = str(uuid.uuid4())
        task = {
            "id": key,
            "action": "update",
            "target_commit": "c" * 40,
            "status": "failed",
            "message": "failed",
            "started_at": 1,
            "finished_at": 2,
        }
        control.atomic_json(self.s.state / "task.json", task)
        control.atomic_json(
            self.s.state / "receipts" / (key + ".json"),
            {**task, "status": "accepted", "finished_at": None},
        )
        restored = control.FirmwareSupervisor(self.s.state, self.s.install, runner=Mock())
        self.assertEqual(restored.start("update", key, "c" * 40)["status"], "failed")
        restored.runner.assert_not_called()

    def test_historical_active_receipt_without_owner_is_replayed_as_failed(self):
        key = str(uuid.uuid4())
        task = {
            "id": key,
            "action": "update",
            "target_commit": "c" * 40,
            "status": "accepted",
            "message": "accepted",
            "started_at": 1,
            "finished_at": None,
        }
        receipt = self.s.state / "receipts" / (key + ".json")
        control.atomic_json(receipt, task)
        self.assertEqual(self.start(key)["status"], "failed")
        self.assertEqual(json.loads(receipt.read_bytes())["status"], "failed")
        self.s.runner.assert_not_called()
        self.assertIsNone(self.s.task)

    def test_receipt_pruning_failure_does_not_reject_durable_task(self):
        old = self.s.state / "receipts" / (str(uuid.uuid4()) + ".json")
        control.atomic_json(old, {"old": True})
        os.utime(old, (1, 1))
        original = type(old).unlink

        def unlink(path, *args, **kwargs):
            if path == old:
                raise OSError("cannot prune history")
            return original(path, *args, **kwargs)

        with patch.object(type(old), "unlink", unlink):
            self.assert_recovered_start()
        self.assertTrue(old.exists())

    def test_concurrent_duplicate_requests_execute_installer_once(self):
        entered, release = threading.Event(), threading.Event()

        def install(task):
            entered.set()
            self.assertTrue(release.wait(3))
            self.install(task)

        self.s.runner.side_effect = install
        key = str(uuid.uuid4())
        try:
            with ThreadPoolExecutor(max_workers=8) as pool:
                responses = list(pool.map(lambda _: self.start(key), range(20)))
            self.assertTrue(entered.wait(3))
            self.assertTrue(all(r["id"] == key for r in responses))
            self.s.runner.assert_called_once()
        finally:
            release.set()
            self.join_worker()
        self.assert_saved("succeeded")

    def test_failed_in_memory_request_still_rejects_identity_conflict(self):
        key = str(uuid.uuid4())
        with patch.object(control, "atomic_json", side_effect=OSError("full")):
            with self.assertRaises(control.FirmwareError):
                self.start(key)
            with self.assertRaises(control.FirmwareError) as raised:
                self.s.start("rollback", key, "b" * 40)
        self.assertEqual(raised.exception.code, "idempotency_conflict")
        self.s.runner.assert_not_called()

    def test_restart_remains_queryable_when_failed_receipt_repair_is_temporarily_unwritable(self):
        task = {
            "id": str(uuid.uuid4()),
            "action": "update",
            "target_commit": "c" * 40,
            "status": "running",
            "message": "running",
            "started_at": 1,
            "finished_at": None,
        }
        control.atomic_json(self.s.state / "task.json", task)
        with patch.object(control, "atomic_json", side_effect=OSError("full")):
            restored = control.FirmwareSupervisor(self.s.state, self.s.install, runner=Mock())
            status = restored.status()
            self.assertEqual(status["task"]["status"], "failed")
            self.assertIsNotNone(status["warning"])
        self.assertIsNone(restored.status()["warning"])
        restored.runner.assert_not_called()

    def test_runner_control_cancellation_records_failure_and_releases_ownership(self):
        errors = []
        self.s.runner.side_effect = SystemExit(3)
        with patch.object(
            threading, "excepthook", side_effect=lambda args: errors.append(args.exc_value)
        ):
            self.start()
            self.join_worker()
        self.assertEqual(len(errors), 1)
        self.assertIsInstance(errors[0], SystemExit)
        self.assert_saved("failed")

    def test_real_control_socket_reports_start_failure_and_recovers_without_reinstall_replay(self):
        path = self.fixture.root / "control.sock"
        server = control.ControlServer(str(path), control.ControlHandler)
        server.supervisor = self.s
        thread = threading.Thread(
            target=lambda: server.serve_forever(poll_interval=0.01), daemon=True
        )
        thread.start()
        real_start = threading.Thread.start

        def fail_installer_thread(worker):
            if getattr(worker._target, "__name__", "") == "run_admitted":
                raise RuntimeError("injected installer worker start failure")
            return real_start(worker)

        try:
            key = str(uuid.uuid4())
            with patch.object(control, "SOCKET", str(path)):
                with patch.object(threading.Thread, "start", fail_installer_thread):
                    with self.assertRaises(control.FirmwareError) as raised:
                        control.request_firmware("update", key=key, commit="c" * 40)
                    self.assertEqual(
                        (raised.exception.code, raised.exception.status),
                        ("update_start_failed", 503),
                    )
                    replay = control.request_firmware("update", key=key, commit="c" * 40)
                    self.assertEqual(replay["status"], "failed")
                    self.s.runner.assert_not_called()
                accepted = control.request_firmware(
                    "update", key=str(uuid.uuid4()), commit="c" * 40
                )
                self.assertEqual(accepted["status"], "accepted")
                self.join_worker()
                self.assertEqual(control.request_firmware("status")["task"]["status"], "succeeded")
                self.s.runner.assert_called_once()
        finally:
            server.shutdown()
            server.server_close()
            thread.join(3)
            self.assertFalse(thread.is_alive())

    def test_persistence_cancellation_is_not_masked_by_terminal_save_failure(self):
        cancellation = KeyboardInterrupt()
        with (
            patch.object(
                control, "atomic_json", side_effect=[cancellation, OSError("cleanup full")]
            ),
            self.assertRaises(KeyboardInterrupt) as raised,
        ):
            self.start()
        self.assertIs(raised.exception, cancellation)
        self.assertEqual(self.s.task["status"], "failed")
        self.assertTrue(self.s._task_save_pending)
        self.s.runner.assert_not_called()
        self.s.status()
        self.assert_saved("failed")

    def test_unrepaired_reload_failure_blocks_new_work_without_losing_success(self):
        self.s.on_success = Mock(side_effect=OSError("reload failed"))
        self.start()
        self.join_worker()
        with self.assertRaises(control.FirmwareError) as raised:
            self.start()
        self.assertEqual(raised.exception.code, "update_state_unavailable")
        self.assertEqual(self.s.task["status"], "succeeded")
        self.s.runner.assert_called_once()
        self.s.on_success = Mock()
        self.assertIsNone(self.s.status()["warning"])
        self.s.on_success.assert_called_once()

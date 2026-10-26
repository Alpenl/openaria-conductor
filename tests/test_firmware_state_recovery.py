from __future__ import annotations

import json
import os
import socket
import threading
import unittest
import uuid
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import Mock, patch

import tests.test_firmware_control as fixtures
from rp_ylx import firmware_control as control


class FirmwareStateRecoveryTest(unittest.TestCase):
    def setUp(self):
        self.fixture = fixtures.FirmwareControlTest()
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        self.state = self.fixture.state
        self.state.mkdir(exist_ok=True)
        self.path = self.state / "task.json"
        self.runner = Mock()
        self.callback = Mock()

    def task(self, **changes):
        task = {
            "id": str(uuid.uuid4()),
            "action": "update",
            "target_commit": "c" * 40,
            "status": "failed",
            "message": "interrupted",
            "started_at": 10,
            "finished_at": 20,
        }
        task.update(changes)
        return task

    def restore(self):
        return control.FirmwareSupervisor(
            self.state, self.fixture.install, runner=self.runner, on_success=self.callback
        )

    def write(self, task):
        control.atomic_json(self.path, task)

    def assert_blocked(self, supervisor):
        status = supervisor.status()
        self.assertIsNone(status["task"])
        self.assertIsNotNone(status["warning"])
        self.assertEqual(status["current"]["commit"], "a" * 40)
        self.assertFalse(supervisor.check(force=True)["has_update"])
        self.fixture.fetch.reset_mock()
        for action, commit in [("update", "c" * 40), ("rollback", "b" * 40)]:
            with self.assertRaises(control.FirmwareError) as raised:
                supervisor.start(action, str(uuid.uuid4()), commit)
            self.assertEqual(
                (raised.exception.code, raised.exception.status), ("update_state_unavailable", 503)
            )
        self.fixture.fetch.assert_not_called()
        self.fixture.idle.assert_not_called()
        self.runner.assert_not_called()
        self.callback.assert_not_called()

    def test_corrupt_json_stays_queryable_preserves_bytes_and_blocks_mutation(self):
        payloads = [
            b'{"id":',
            b"[]",
            b"null",
            b"{}",
            b"\xff",
            b'{"status":"running","status":"failed"}',
            b'{"number":NaN}',
            b"[" * 1000 + b"]" * 1000,
        ]
        for raw in payloads:
            with self.subTest(raw=raw[:30]):
                self.path.write_bytes(raw)
                self.assert_blocked(self.restore())
                self.assertEqual(self.path.read_bytes(), raw)
                self.assertFalse((self.state / "receipts").exists())

    def test_invalid_task_fields_are_not_interpreted_as_installed_or_executable(self):
        bad = [
            {"id": "../../outside"},
            {"id": 1},
            {"action": "shell"},
            {"action": []},
            {"target_commit": "../c"},
            {"target_commit": None},
            {"status": "done"},
            {"status": []},
            {"message": ""},
            {"message": "x" * 4097},
            {"message": "\ud800"},
            {"started_at": True},
            {"started_at": -1},
            {"started_at": 2**64},
            {"finished_at": None},
            {"finished_at": True},
            {"finished_at": float("inf")},
            {"status": "running", "finished_at": 2},
        ]
        records = [self.task(**changes) for changes in bad]
        incomplete = self.task()
        incomplete.pop("action")
        records.append(incomplete)
        records.append({**self.task(), "unexpected": "field"})
        for task in records:
            with self.subTest(task=repr(task)[:100]):
                raw = json.dumps(task).encode()
                self.path.write_bytes(raw)
                self.assert_blocked(self.restore())
                self.assertEqual(self.path.read_bytes(), raw)
        self.assertFalse((self.fixture.root / "outside.json").exists())

    def test_missing_initial_task_is_supported_without_warning(self):
        supervisor = self.restore()
        self.assertIsNone(supervisor.status()["warning"])
        self.assertFalse(supervisor._task_load_pending)
        self.assertIsNone(supervisor.task)

    def test_removing_bad_file_does_not_silently_clear_uncertain_state(self):
        self.path.write_bytes(b"{")
        supervisor = self.restore()
        self.path.unlink()
        self.assert_blocked(supervisor)

    def test_valid_replacement_recovers_without_restarting_or_reexecuting(self):
        self.path.write_bytes(b"{")
        supervisor = self.restore()
        task = self.task()
        self.write(task)
        status = supervisor.status()
        self.assertEqual(status["task"], task)
        self.assertIsNone(status["warning"])
        self.assertEqual(supervisor.start("update", task["id"], "c" * 40), task)
        self.runner.assert_not_called()
        self.callback.assert_not_called()
        receipt = self.state / "receipts" / (task["id"] + ".json")
        self.assertEqual(json.loads(receipt.read_bytes()), task)

    def test_restored_active_task_becomes_failed_and_never_runs(self):
        for state in ["accepted", "running"]:
            with self.subTest(state=state):
                task = self.task(status=state, finished_at=None)
                self.write(task)
                supervisor = self.restore()
                restored = supervisor.status()["task"]
                self.assertEqual(restored["status"], "failed")
                self.assertIsInstance(restored["finished_at"], int)
                self.assertEqual(supervisor.start("update", task["id"], "c" * 40), restored)
                self.runner.assert_not_called()
                self.callback.assert_not_called()

    def test_successful_receipt_does_not_trigger_success_callback_after_reload(self):
        task = self.task(status="succeeded")
        self.write(task)
        supervisor = self.restore()
        self.assertEqual(supervisor.status()["task"], task)
        self.callback.assert_not_called()
        self.runner.assert_not_called()

    def test_wall_clock_reversal_does_not_invalidate_terminal_task(self):
        task = self.task(started_at=30, finished_at=10)
        self.write(task)
        self.assertEqual(self.restore().status()["task"], task)

    def test_storage_read_error_recovers_on_later_query(self):
        task = self.task()
        self.write(task)
        original = control.os.open

        def fail(path, *args, **kwargs):
            if path == self.path:
                raise OSError("private disk failure")
            return original(path, *args, **kwargs)

        with patch.object(control.os, "open", side_effect=fail):
            supervisor = self.restore()
            self.assert_blocked(supervisor)
            self.assertNotIn("private", supervisor.status()["warning"])
        self.assertEqual(supervisor.status()["task"], task)
        self.assertIsNone(supervisor.status()["warning"])

    def test_repair_write_failure_remains_queryable_and_retries_after_storage_recovers(self):
        self.path.write_bytes(b"{")
        supervisor = self.restore()
        task = self.task(status="running", finished_at=None)
        self.write(task)
        with patch.object(control, "atomic_json", side_effect=OSError("full")):
            status = supervisor.status()
            self.assertEqual(status["task"]["status"], "failed")
            self.assertIsNotNone(status["warning"])
            with self.assertRaises(control.FirmwareError):
                supervisor.start("update", str(uuid.uuid4()), "c" * 40)
        self.assertIsNone(supervisor.status()["warning"])
        self.runner.assert_not_called()

    def test_oversize_file_is_rejected_before_open(self):
        self.path.write_bytes(b" " * (control.LIMIT + 1))
        with patch.object(control.os, "open", side_effect=AssertionError("must not open")):
            supervisor = self.restore()
        self.assertTrue(supervisor._task_load_pending)

    def test_fifo_directory_socket_symlink_and_hardlink_are_not_opened(self):
        target = self.fixture.root / "target.json"
        target.write_text(json.dumps(self.task()))
        for kind in ["fifo", "directory", "socket", "symlink", "hardlink"]:
            with self.subTest(kind=kind):
                peer = None
                if kind == "fifo":
                    os.mkfifo(self.path)
                elif kind == "directory":
                    self.path.mkdir()
                elif kind == "socket":
                    peer = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
                    peer.bind(str(self.path))
                elif kind == "symlink":
                    self.path.symlink_to(target)
                else:
                    os.link(target, self.path)
                before = target.read_bytes()
                try:
                    with patch.object(
                        control.os, "open", side_effect=AssertionError("must not open")
                    ):
                        supervisor = self.restore()
                    self.assertTrue(supervisor._task_load_pending)
                    self.assertEqual(target.read_bytes(), before)
                finally:
                    if peer:
                        peer.close()
                    if kind == "directory":
                        self.path.rmdir()
                    else:
                        self.path.unlink()

    def test_file_replacement_before_open_is_detected_and_new_bytes_preserved(self):
        self.write(self.task())
        replacement = self.task(message="replacement")
        next_path = self.state / "replacement"
        next_path.write_text(json.dumps(replacement))
        original = control.os.open
        replaced = []

        def replace(path, *args, **kwargs):
            if path == self.path and not replaced:
                next_path.replace(self.path)
                replaced.append(True)
            return original(path, *args, **kwargs)

        with patch.object(control.os, "open", side_effect=replace):
            supervisor = self.restore()
        self.assertTrue(supervisor._task_load_pending)
        self.assertEqual(json.loads(self.path.read_bytes()), replacement)
        self.assertEqual(supervisor.status()["task"], replacement)

    def test_file_replacement_during_read_is_detected_without_overwriting(self):
        self.write(self.task())
        replacement = self.task(message="replacement")
        next_path = self.state / "replacement"
        next_path.write_text(json.dumps(replacement))
        original = control.os.read
        replaced = []

        def replace(fd, count):
            chunk = original(fd, count)
            if not replaced:
                next_path.replace(self.path)
                replaced.append(True)
            return chunk

        with patch.object(control.os, "read", side_effect=replace):
            supervisor = self.restore()
        self.assertTrue(supervisor._task_load_pending)
        self.assertEqual(json.loads(self.path.read_bytes()), replacement)
        self.assertEqual(supervisor.status()["task"], replacement)

    def test_malformed_or_mismatched_receipt_rejects_replay_without_mutation(self):
        supervisor = self.restore()
        key = str(uuid.uuid4())
        receipt = self.state / "receipts" / (key + ".json")
        receipt.parent.mkdir()
        for raw in [b"[]", b"{", json.dumps(self.task()).encode()]:
            with self.subTest(raw=raw[:30]):
                receipt.write_bytes(raw)
                with self.assertRaises(control.FirmwareError) as raised:
                    supervisor.start("update", key, "c" * 40)
                self.assertEqual(raised.exception.code, "update_state_unavailable")
                self.assertEqual(receipt.read_bytes(), raw)
        self.runner.assert_not_called()
        self.fixture.fetch.assert_not_called()
        self.fixture.idle.assert_not_called()

    def test_receipt_repair_allows_idempotent_replay_without_restart(self):
        supervisor = self.restore()
        task = self.task()
        receipt = self.state / "receipts" / (task["id"] + ".json")
        receipt.parent.mkdir()
        receipt.write_bytes(b"{")
        with self.assertRaises(control.FirmwareError):
            supervisor.start("update", task["id"], "c" * 40)
        control.atomic_json(receipt, task)
        self.assertEqual(supervisor.start("update", task["id"], "c" * 40), task)
        self.runner.assert_not_called()

    def test_historical_active_receipt_write_failure_is_typed_and_does_not_run(self):
        supervisor = self.restore()
        task = self.task(status="accepted", finished_at=None)
        receipt = self.state / "receipts" / (task["id"] + ".json")
        control.atomic_json(receipt, task)
        with (
            patch.object(control, "atomic_json", side_effect=OSError("full")),
            self.assertRaises(control.FirmwareError) as raised,
        ):
            supervisor.start("update", task["id"], "c" * 40)
        self.assertEqual(raised.exception.code, "update_state_unavailable")
        self.assertEqual(json.loads(receipt.read_bytes()), task)
        self.assertEqual(supervisor.start("update", task["id"], "c" * 40)["status"], "failed")
        self.runner.assert_not_called()

    def test_process_control_cancellation_is_not_swallowed_by_loader(self):
        self.write(self.task())
        for error in [KeyboardInterrupt(), SystemExit(3)]:
            with (
                patch.object(control.os, "open", side_effect=error),
                self.assertRaises(type(error)),
            ):
                self.restore()

    def test_real_control_socket_serves_degraded_status_and_typed_update_error(self):
        self.path.write_bytes(b"{")
        supervisor = self.restore()
        path = str(self.fixture.root / "control.sock")
        server = control.ControlServer(path, control.ControlHandler)
        server.supervisor = supervisor
        thread = threading.Thread(
            target=lambda: server.serve_forever(poll_interval=0.01), daemon=True
        )
        thread.start()
        try:
            with patch.object(control, "SOCKET", path):
                status = control.request_firmware("status")
                self.assertIsNotNone(status["warning"])
                self.assertIsNone(status["task"])
                with self.assertRaises(control.FirmwareError) as raised:
                    control.request_firmware("update", key=str(uuid.uuid4()), commit="c" * 40)
                self.assertEqual(raised.exception.code, "update_state_unavailable")
                task = self.task()
                self.write(task)
                self.assertEqual(control.request_firmware("status")["task"], task)
        finally:
            server.shutdown()
            server.server_close()
            thread.join(3)
        self.assertFalse(thread.is_alive())
        self.runner.assert_not_called()

    def test_repaired_state_allows_new_task_and_executes_only_new_request(self):
        self.path.write_bytes(b"{")
        supervisor = self.restore()
        self.write(self.task())
        supervisor.status()
        self.runner.side_effect = lambda task: self.fixture.make_release("c", "0.2.2", "current")
        key = str(uuid.uuid4())
        accepted = supervisor.start("update", key, "c" * 40)
        self.assertEqual(accepted["status"], "accepted")
        worker = supervisor._worker
        if worker is not None:
            worker.join(3)
            self.assertFalse(worker.is_alive())
        self.assertEqual(supervisor.status()["task"]["status"], "succeeded")
        self.runner.assert_called_once()
        self.assertEqual(self.runner.call_args.args[0]["id"], key)
        self.callback.assert_called_once()

    def test_unsafe_receipt_is_rejected_without_following_alias_or_opening_fifo(self):
        supervisor = self.restore()
        task = self.task()
        receipt = self.state / "receipts" / (task["id"] + ".json")
        receipt.parent.mkdir()
        target = self.fixture.root / "receipt-target"
        target.write_text(json.dumps(task))
        original = target.read_bytes()
        for kind in ["symlink", "hardlink", "fifo"]:
            with self.subTest(kind=kind):
                if kind == "symlink":
                    receipt.symlink_to(target)
                elif kind == "hardlink":
                    os.link(target, receipt)
                else:
                    os.mkfifo(receipt)
                try:
                    with (
                        patch.object(
                            control.os, "open", side_effect=AssertionError("must not open")
                        ),
                        self.assertRaises(control.FirmwareError) as raised,
                    ):
                        supervisor.start("update", task["id"], "c" * 40)
                    self.assertEqual(raised.exception.code, "update_state_unavailable")
                    self.assertEqual(target.read_bytes(), original)
                finally:
                    receipt.unlink()
        self.runner.assert_not_called()

    def test_concurrent_queries_recover_same_valid_task_without_execution(self):
        self.path.write_bytes(b"{")
        supervisor = self.restore()
        task = self.task()
        self.write(task)
        with ThreadPoolExecutor(max_workers=8) as pool:
            values = list(pool.map(lambda _: supervisor.status(), range(40)))
        self.assertTrue(all(value["task"] == task and value["warning"] is None for value in values))
        self.runner.assert_not_called()
        self.callback.assert_not_called()
        self.assertEqual(json.loads(self.path.read_bytes()), task)

    def test_inplace_mutation_during_read_is_detected_and_preserved(self):
        self.write(self.task())
        replacement = b'{"incomplete":'
        original = control.os.read
        changed = []

        def mutate(fd, count):
            chunk = original(fd, count)
            if not changed:
                self.path.write_bytes(replacement)
                changed.append(True)
            return chunk

        with patch.object(control.os, "read", side_effect=mutate):
            supervisor = self.restore()
        self.assertTrue(supervisor._task_load_pending)
        self.assertEqual(self.path.read_bytes(), replacement)
        self.runner.assert_not_called()

    def test_read_cancellation_closes_file_descriptor_and_preserves_exception(self):
        self.write(self.task())
        before = len(os.listdir("/proc/self/fd"))
        for error in [KeyboardInterrupt(), SystemExit(7)]:
            with (
                patch.object(control.os, "read", side_effect=error),
                self.assertRaises(type(error)) as raised,
            ):
                self.restore()
            self.assertIs(raised.exception, error)
            self.assertEqual(len(os.listdir("/proc/self/fd")), before)

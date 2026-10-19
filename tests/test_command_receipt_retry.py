import threading
import unittest
from unittest.mock import Mock, patch

from rp_ylx.api.gateway import CaptureCommandResult
from tests import test_capture_coordinator as fixtures


class CommandReceiptRetryTests(unittest.TestCase):
    def setUp(self):
        self.fixture = fixtures.CaptureCoordinatorTest(
            "test_stop_without_active_recording_is_idempotent_no_op"
        )
        self.fixture.setUp()
        self.addCleanup(self.fixture.tearDown)
        self.coordinator = self.fixture.coordinator()
        self.addCleanup(self.coordinator.close)

    def rows(self):
        return self.coordinator._read_local_state()["commands"]

    def invoke(self, operation, command, execute):
        if operation == "stop":
            return self.coordinator.stop_capture(command)
        with self.coordinator._lock:
            return self.coordinator._idempotent(operation, command, execute)

    def fill_receipts(self, operation):
        c = self.coordinator
        for index in range(1024):
            cmd = fixtures.stop_command(f"cached-{index}")
            c._commands[(cmd.principal_id, operation, cmd.idempotency_key)] = (
                cmd.canonical_body,
                CaptureCommandResult(204, {"receipt": index}),
            )
        c._persist_local_state()
        self.assertEqual(len(self.rows()), 1024)

    def test_retry_after_failed_write_commits_receipt_without_reexecuting(self):
        for operation in ("stop", "receipt-test"):
            with self.subTest(operation=operation):
                cmd = fixtures.stop_command("retry-" + operation)
                execute = Mock(return_value=CaptureCommandResult(200, {"value": "first"}))
                with (
                    patch.object(
                        self.coordinator._state_db,
                        "put_capture_state",
                        side_effect=OSError("injected full disk"),
                    ),
                    self.assertRaises(OSError),
                ):
                    self.invoke(operation, cmd, execute)
                self.assertFalse(
                    any(r["idempotency_key"] == cmd.idempotency_key for r in self.rows())
                )
                replay = self.invoke(operation, cmd, execute)
                self.assertTrue(replay.replayed)
                self.assertTrue(
                    any(r["idempotency_key"] == cmd.idempotency_key for r in self.rows())
                )
                if operation != "stop":
                    execute.assert_called_once()

    def test_repeated_write_failure_is_not_reported_as_durable_success(self):
        for operation in ("stop", "receipt-test"):
            with self.subTest(operation=operation):
                cmd = fixtures.stop_command("still-failed-" + operation)
                execute = Mock(return_value=CaptureCommandResult(200, None))
                with patch.object(
                    self.coordinator._state_db,
                    "put_capture_state",
                    side_effect=OSError("injected busy database"),
                ):
                    for _ in range(2):
                        with self.assertRaises(OSError):
                            self.invoke(operation, cmd, execute)
                self.assertFalse(
                    any(r["idempotency_key"] == cmd.idempotency_key for r in self.rows())
                )
                if operation != "stop":
                    execute.assert_called_once()

    def test_evicted_key_reused_after_failed_write_survives_restart(self):
        self.fill_receipts("stop")
        c = self.coordinator
        with (
            patch.object(
                c._state_db, "put_capture_state", side_effect=OSError("injected disk failure")
            ),
            self.assertRaises(OSError),
        ):
            c.stop_capture(fixtures.stop_command("overflow"))
        reused = fixtures.stop_command("cached-0", reason="changed-after-eviction")
        self.assertFalse(c.stop_capture(reused).replayed)
        c.close()
        restarted = self.fixture.coordinator()
        self.addCleanup(restarted.close)
        replay = restarted.stop_capture(reused)
        self.assertTrue(replay.replayed)
        self.assertEqual(replay.status, 204)
        self.assertEqual(len(restarted._read_local_state()["commands"]), 1024)

    def test_same_body_reused_key_persists_the_new_result(self):
        self.fill_receipts("receipt-test")
        c = self.coordinator
        with (
            patch.object(
                c._state_db, "put_capture_state", side_effect=OSError("injected disk failure")
            ),
            self.assertRaises(OSError),
        ):
            self.invoke(
                "receipt-test",
                fixtures.stop_command("overflow"),
                lambda: CaptureCommandResult(200, None),
            )
        reused = fixtures.stop_command("cached-0")
        result = self.invoke(
            "receipt-test", reused, lambda: CaptureCommandResult(200, {"receipt": "replacement"})
        )
        self.assertFalse(result.replayed)
        c.close()
        restarted = self.fixture.coordinator()
        self.addCleanup(restarted.close)
        execute = Mock(side_effect=AssertionError("must replay without side effect"))
        with restarted._lock:
            replay = restarted._idempotent("receipt-test", reused, execute)
        self.assertEqual(replay.body, {"receipt": "replacement"})
        self.assertTrue(replay.replayed)
        execute.assert_not_called()

    def test_durable_replay_needs_no_write_and_keeps_cached_body_private(self):
        cmd = fixtures.stop_command("durable-private")
        execute = Mock(return_value=CaptureCommandResult(200, {"nested": {"value": 1}}))
        first = self.invoke("receipt-test", cmd, execute)
        first.body["nested"]["value"] = 2
        with patch.object(
            self.coordinator._state_db,
            "put_capture_state",
            side_effect=AssertionError("unneeded database write"),
        ):
            replay = self.invoke("receipt-test", cmd, execute)
            self.assertEqual(replay.body, {"nested": {"value": 1}})
            replay.body["nested"]["value"] = 3
            self.assertEqual(
                self.invoke("receipt-test", cmd, execute).body, {"nested": {"value": 1}}
            )
        execute.assert_called_once()

    def test_delayed_snapshot_cannot_restore_an_evicted_receipt(self):
        self.fill_receipts("stop")
        c = self.coordinator
        entered, release = threading.Event(), threading.Event()
        real_lock = threading.Lock()
        errors = []

        class DelayedLock:
            def __enter__(self):
                if threading.current_thread().name == "delayed-command-snapshot":
                    entered.set()
                    if not release.wait(3):
                        raise TimeoutError("snapshot not released")
                real_lock.acquire()

            def __exit__(self, *args):
                real_lock.release()

        def delayed():
            try:
                c._persist_local_state()
            except BaseException as error:
                errors.append(error)

        thread = threading.Thread(target=delayed, name="delayed-command-snapshot")
        reused = fixtures.stop_command("cached-0", reason="new-after-eviction")
        try:
            with patch.object(c, "_state_persist_lock", DelayedLock()):
                thread.start()
                self.assertTrue(entered.wait(1))
                c.stop_capture(fixtures.stop_command("overflow"))
                c.stop_capture(reused)
                release.set()
                thread.join(2)
                self.assertFalse(thread.is_alive())
                self.assertEqual(errors, [])
        finally:
            release.set()
            thread.join(4)
        c.close()
        restarted = self.fixture.coordinator()
        self.addCleanup(restarted.close)
        self.assertTrue(restarted.stop_capture(reused).replayed)


if __name__ == "__main__":
    unittest.main(verbosity=2)

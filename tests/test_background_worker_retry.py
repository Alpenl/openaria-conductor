from __future__ import annotations

import threading
import unittest
from unittest.mock import Mock, patch

from rp_ylx.daemon import CaptureEventPump, StorageReserveMonitor


class ScheduledWait:
    def __init__(self, cycles):
        self.cycles = cycles
        self.intervals = []
        self.now = 100.0

    def wait(self, interval):
        self.intervals.append(interval)
        self.now = round(self.now + interval, 6)
        return len(self.intervals) > self.cycles


class BackgroundWorkerRetryTest(unittest.TestCase):
    def pump(self):
        coordinator = Mock()
        coordinator.capture_recording_progress.return_value = ("epoch", 1, None)
        events = Mock()
        with patch("rp_ylx.daemon.threading.Thread"):
            pump = CaptureEventPump(coordinator, events, interval=0.2, progress_interval=0.2)
        return pump, coordinator, events

    def monitor(self):
        coordinator = Mock()
        with patch("rp_ylx.daemon.threading.Thread"):
            monitor = StorageReserveMonitor(coordinator)
        return monitor, coordinator

    @staticmethod
    def snapshot(revision=2):
        return {
            "authority_epoch": "epoch",
            "source_revision": revision,
            "data": {"active_recording": None},
        }

    @staticmethod
    def state(frames):
        return {
            "state": "recording",
            "session_id": "session",
            "progress": {
                "elapsed_seconds": frames / 30,
                "captured_frames": frames,
                "bytes_written": frames * 10,
            },
        }

    def run_cycles(self, worker, cycles, *, storage=False):
        clock = ScheduledWait(cycles)
        if storage:
            worker.stop = clock
        else:
            worker._stop = clock
        with patch("rp_ylx.daemon.time.monotonic", side_effect=lambda: clock.now):
            worker._run()
        return clock

    def test_progress_read_error_retries_and_publishes_next_revision(self):
        pump, coordinator, events = self.pump()
        coordinator.capture_recording_progress.side_effect = [
            OSError("private read detail"),
            ("epoch", 2, None),
        ]
        coordinator.capture_snapshot_event.return_value = self.snapshot()
        with patch("rp_ylx.daemon.operational_logger") as logger:
            self.run_cycles(pump, 2)
        events.publish.assert_called_once_with(self.snapshot())
        self.assertEqual(pump._last_source, ("epoch", 2))
        logger.return_value.event.assert_called_once()
        self.assertNotIn("private read detail", str(logger.mock_calls))

    def test_snapshot_error_retries_without_advancing_revision(self):
        pump, coordinator, events = self.pump()
        coordinator.capture_recording_progress.return_value = ("epoch", 2, None)
        coordinator.capture_snapshot_event.side_effect = [
            OSError("transient snapshot"),
            self.snapshot(),
        ]
        self.run_cycles(pump, 2)
        self.assertEqual(coordinator.capture_snapshot_event.call_count, 2)
        events.publish.assert_called_once_with(self.snapshot())
        self.assertEqual(pump._last_source, ("epoch", 2))

    def test_snapshot_publish_error_does_not_commit_source_or_skip_retry(self):
        for error_type in (OSError, MemoryError, ValueError):
            with self.subTest(error=error_type.__name__):
                pump, coordinator, events = self.pump()
                coordinator.capture_recording_progress.return_value = ("epoch", 2, None)
                coordinator.capture_snapshot_event.return_value = self.snapshot()
                observed_sources = []

                def publish(
                    event, observed_sources=observed_sources, pump=pump, error_type=error_type
                ):
                    observed_sources.append(pump._last_source)
                    if len(observed_sources) == 1:
                        raise error_type("publish failed")

                events.publish.side_effect = publish
                self.run_cycles(pump, 2)
                self.assertEqual(observed_sources, [("epoch", 1)] * 2)
                self.assertEqual(pump._last_source, ("epoch", 2))
                self.assertEqual(events.publish.call_count, 2)

    def test_progress_publish_error_retries_without_advancing_progress(self):
        pump, coordinator, events = self.pump()
        old_state = self.state(0)
        pump._last_progress_key = pump._progress_key(("epoch", 1), old_state)
        old_key = pump._last_progress_key
        coordinator.capture_recording_progress.return_value = ("epoch", 1, self.state(60))
        observed = []

        def publish(event):
            observed.append((pump._last_progress_key, pump._last_progress_at))
            if len(observed) == 1:
                raise OSError("publish failed")

        events.publish.side_effect = publish
        self.run_cycles(pump, 3)
        self.assertEqual(observed, [(old_key, 0.0)] * 2)
        self.assertEqual(events.publish.call_count, 2)
        self.assertNotEqual(pump._last_progress_key, old_key)
        self.assertGreater(pump._last_progress_at, 0)
        coordinator.capture_snapshot_event.assert_not_called()

    def test_malformed_snapshot_does_not_kill_later_valid_updates(self):
        pump, coordinator, events = self.pump()
        coordinator.capture_recording_progress.return_value = ("epoch", 2, None)
        coordinator.capture_snapshot_event.side_effect = [{}, self.snapshot()]
        self.run_cycles(pump, 2)
        events.publish.assert_called_once_with(self.snapshot())
        self.assertEqual(pump._last_source, ("epoch", 2))

    def test_event_error_reporting_failure_does_not_stop_retry(self):
        for stage in ("factory", "event"):
            with self.subTest(stage=stage):
                pump, coordinator, events = self.pump()
                coordinator.capture_recording_progress.side_effect = [
                    OSError("read"),
                    ("epoch", 2, None),
                ]
                coordinator.capture_snapshot_event.return_value = self.snapshot()
                with patch("rp_ylx.daemon.operational_logger") as logger:
                    if stage == "factory":
                        logger.side_effect = OSError("logger unavailable")
                    else:
                        logger.return_value.event.side_effect = OSError("sink unavailable")
                    self.run_cycles(pump, 2)
                events.publish.assert_called_once_with(self.snapshot())

    def test_storage_error_and_broken_log_sink_still_retry_protection(self):
        monitor, coordinator = self.monitor()
        coordinator.enforce_storage_reserve.side_effect = [OSError("probe"), None]
        with patch("rp_ylx.daemon.operational_logger") as logger:
            logger.return_value.event.side_effect = OSError("sink")
            self.run_cycles(monitor, 2, storage=True)
        self.assertEqual(coordinator.enforce_storage_reserve.call_count, 2)

    def test_persistent_failures_wait_each_time_and_rate_limit_diagnostics(self):
        for storage in (False, True):
            for broken_log in (False, True):
                with self.subTest(storage=storage, broken_log=broken_log):
                    if storage:
                        worker, coordinator = self.monitor()
                        poll = coordinator.enforce_storage_reserve
                        cycles = 120
                    else:
                        worker, coordinator, _ = self.pump()
                        poll = coordinator.capture_recording_progress
                        poll.reset_mock()
                        cycles = 600
                    poll.side_effect = OSError("persistent failure")
                    with patch("rp_ylx.daemon.operational_logger") as logger:
                        if broken_log:
                            logger.return_value.event.side_effect = OSError("sink")
                        clock = self.run_cycles(worker, cycles, storage=storage)
                    self.assertEqual(poll.call_count, cycles)
                    self.assertEqual(len(clock.intervals), cycles + 1)
                    self.assertTrue(
                        all(interval >= (1.0 if storage else 0.2) for interval in clock.intervals)
                    )
                    self.assertEqual(logger.return_value.event.call_count, 4)

    def test_success_between_failures_does_not_reset_log_rate_limit(self):
        for storage in (False, True):
            with self.subTest(storage=storage):
                if storage:
                    worker, coordinator = self.monitor()
                    poll = coordinator.enforce_storage_reserve
                    success = None
                else:
                    worker, coordinator, _ = self.pump()
                    poll = coordinator.capture_recording_progress
                    success = ("epoch", 1, None)
                    coordinator.capture_snapshot_event.return_value = self.snapshot(1)
                poll.side_effect = [OSError("flapping"), success] * 5
                with patch("rp_ylx.daemon.operational_logger") as logger:
                    self.run_cycles(worker, 10, storage=storage)
                logger.return_value.event.assert_called_once()

    def test_process_control_exceptions_are_not_silently_retried(self):
        for storage in (False, True):
            for error_type in (KeyboardInterrupt, SystemExit):
                with self.subTest(storage=storage, error=error_type.__name__):
                    if storage:
                        worker, coordinator = self.monitor()
                        poll = coordinator.enforce_storage_reserve
                    else:
                        worker, coordinator, _ = self.pump()
                        poll = coordinator.capture_recording_progress
                    poll.side_effect = error_type()
                    with self.assertRaises(error_type):
                        self.run_cycles(worker, 2, storage=storage)

    def test_real_workers_recover_after_fault_and_close(self):
        recovered = threading.Event()
        coordinator = Mock()
        coordinator.capture_recording_progress.return_value = ("epoch", 1, None)
        coordinator.capture_snapshot_event.side_effect = [OSError("snapshot"), self.snapshot()]
        events = Mock()
        events.publish.side_effect = lambda event: recovered.set()
        errors = []
        with patch(
            "threading.excepthook",
            side_effect=lambda args, errors=errors: errors.append(args.exc_type),
        ):
            pump = CaptureEventPump(coordinator, events, interval=0.01)
            try:
                self.assertTrue(recovered.wait(2), "event worker did not recover")
                self.assertTrue(pump._thread.is_alive())
            finally:
                pump.close()
            self.assertFalse(pump._thread.is_alive())
        self.assertEqual(errors, [])
        recovered.clear()
        calls = []

        def enforce():
            calls.append(1)
            if len(calls) == 1:
                raise OSError("probe")
            recovered.set()

        coordinator.enforce_storage_reserve.side_effect = enforce
        with (
            patch(
                "threading.excepthook",
                side_effect=lambda args, errors=errors: errors.append(args.exc_type),
            ),
            patch("rp_ylx.daemon.operational_logger") as logger,
        ):
            logger.return_value.event.side_effect = OSError("sink")
            monitor = StorageReserveMonitor(coordinator)
            try:
                self.assertTrue(recovered.wait(3), "storage worker did not recover")
                self.assertTrue(monitor.thread.is_alive())
            finally:
                monitor.close()
            self.assertFalse(monitor.thread.is_alive())
        self.assertEqual(errors, [])

    def test_real_workers_close_while_errors_persist(self):
        for storage in (False, True):
            with self.subTest(storage=storage):
                attempted = threading.Event()
                coordinator = Mock()
                coordinator.capture_recording_progress.return_value = ("epoch", 1, None)

                def fail(attempted=attempted):
                    attempted.set()
                    raise OSError("persistent error")

                coordinator.capture_snapshot_event.side_effect = fail
                coordinator.enforce_storage_reserve.side_effect = fail
                errors = []
                with (
                    patch("rp_ylx.daemon.operational_logger") as logger,
                    patch(
                        "threading.excepthook",
                        side_effect=lambda args, errors=errors: errors.append(args.exc_type),
                    ),
                ):
                    logger.return_value.event.side_effect = OSError("persistent sink error")
                    worker = (
                        StorageReserveMonitor(coordinator)
                        if storage
                        else CaptureEventPump(coordinator, Mock(), interval=0.01)
                    )
                    try:
                        self.assertTrue(attempted.wait(2))
                    finally:
                        worker.close()
                    thread = worker.thread if storage else worker._thread
                    self.assertFalse(thread.is_alive())
                self.assertEqual(errors, [])


if __name__ == "__main__":
    unittest.main()

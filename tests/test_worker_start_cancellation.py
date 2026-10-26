from __future__ import annotations

import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from rp_ylx.daemon import CaptureEventPump, StorageReserveMonitor, build_production_service
from tests import test_daemon as fixture

_REAL_START = threading.Thread.start
_REAL_JOIN = threading.Thread.join


class WorkerStartCancellationTest(unittest.TestCase):
    def partial(self, kind):
        cls = StorageReserveMonitor if kind == "storage" else CaptureEventPump
        owner = object.__new__(cls)
        coordinator = Mock()
        coordinator.capture_recording_progress.return_value = ("epoch", 1, None)
        coordinator.capture_snapshot_event.return_value = {
            "authority_epoch": "epoch",
            "source_revision": 1,
            "data": {"active_recording": None},
        }

        def construct():
            if kind == "storage":
                cls.__init__(owner, coordinator)
            else:
                cls.__init__(owner, coordinator, Mock(), interval=0.01)

        return owner, cls, construct

    @staticmethod
    def stop(owner):
        return owner.stop if isinstance(owner, StorageReserveMonitor) else owner._stop

    def cleanup(self, owner, threads):
        self.stop(owner).set()
        for thread in threads:
            if thread.ident is not None:
                _REAL_JOIN(thread, 2)
                self.assertFalse(thread.is_alive(), "test worker remained alive")

    def test_start_failure_before_os_thread_requests_stop_without_join(self):
        for kind in ("event", "storage"):
            for error_type in (RuntimeError, MemoryError, KeyboardInterrupt):
                with self.subTest(kind=kind, error=error_type.__name__):
                    owner, _, construct = self.partial(kind)
                    original = error_type("private startup failure")
                    with (
                        patch("rp_ylx.daemon.threading.Thread.start", side_effect=original),
                        patch("rp_ylx.daemon.threading.Thread.join") as join,
                        self.assertRaises(error_type) as raised,
                    ):
                        construct()
                    self.assertIs(raised.exception, original)
                    self.assertTrue(self.stop(owner).is_set())
                    join.assert_not_called()

    def test_thread_constructor_failure_requests_stop_and_preserves_error(self):
        for kind in ("event", "storage"):
            with self.subTest(kind=kind):
                owner, _, construct = self.partial(kind)
                original = MemoryError("thread construction")
                with (
                    patch("rp_ylx.daemon.threading.Thread", side_effect=original),
                    self.assertRaises(MemoryError) as raised,
                ):
                    construct()
                self.assertIs(raised.exception, original)
                self.assertTrue(self.stop(owner).is_set())

    def test_error_after_real_start_stops_and_joins_worker(self):
        for kind in ("event", "storage"):
            for error_type in (RuntimeError, MemoryError, KeyboardInterrupt):
                with self.subTest(kind=kind, error=error_type.__name__):
                    owner, _, construct = self.partial(kind)
                    original = error_type("after real start")
                    threads = []

                    def start(thread, threads=threads, original=original):
                        threads.append(thread)
                        _REAL_START(thread)
                        raise original

                    try:
                        with (
                            patch("rp_ylx.daemon.threading.Thread.start", start),
                            self.assertRaises(error_type) as raised,
                        ):
                            construct()
                        self.assertIs(raised.exception, original)
                        self.assertTrue(self.stop(owner).is_set())
                        self.assertFalse(threads[0].is_alive())
                    finally:
                        self.cleanup(owner, threads)

    def test_target_cannot_run_before_start_admission_completes(self):
        for kind in ("event", "storage"):
            with self.subTest(kind=kind):
                owner, cls, construct = self.partial(kind)
                entered = threading.Event()
                threads = []
                original = KeyboardInterrupt("cancelled admission")
                observed = []

                def start(
                    thread, threads=threads, entered=entered, observed=observed, original=original
                ):
                    threads.append(thread)
                    _REAL_START(thread)
                    observed.append(entered.wait(0.03))
                    raise original

                try:
                    with (
                        patch.object(cls, "_run", side_effect=entered.set),
                        patch("rp_ylx.daemon.threading.Thread.start", start),
                        self.assertRaises(KeyboardInterrupt),
                    ):
                        construct()
                    self.assertEqual(observed, [False])
                    self.assertFalse(entered.is_set())
                finally:
                    self.cleanup(owner, threads)

    def test_delayed_target_after_rejection_never_polls(self):
        for kind in ("event", "storage"):
            with self.subTest(kind=kind):
                owner, cls, construct = self.partial(kind)
                threads = []

                def reject(thread, threads=threads):
                    threads.append(thread)
                    raise RuntimeError("rejected before OS start")

                try:
                    with patch.object(cls, "_run") as run:
                        with (
                            patch("rp_ylx.daemon.threading.Thread.start", reject),
                            self.assertRaises(RuntimeError),
                        ):
                            construct()
                        _REAL_START(threads[0])
                        _REAL_JOIN(threads[0], 2)
                        self.assertFalse(threads[0].is_alive())
                        run.assert_not_called()
                finally:
                    self.cleanup(owner, threads)

    def test_join_and_logging_failures_do_not_mask_start_error(self):
        for kind in ("event", "storage"):
            with self.subTest(kind=kind):
                owner, _, construct = self.partial(kind)
                original = KeyboardInterrupt("private primary")
                threads = []

                def start(thread, threads=threads, original=original):
                    threads.append(thread)
                    _REAL_START(thread)
                    raise original

                try:
                    with (
                        patch("rp_ylx.daemon.threading.Thread.start", start),
                        patch(
                            "rp_ylx.daemon.threading.Thread.join",
                            side_effect=OSError("private join"),
                        ),
                        patch(
                            "rp_ylx.daemon._OPERATIONAL_LOG.event",
                            side_effect=OSError("private sink"),
                        ) as log,
                        self.assertRaises(KeyboardInterrupt) as raised,
                    ):
                        construct()
                    self.assertIs(raised.exception, original)
                    self.assertTrue(self.stop(owner).is_set())
                    self.assertNotIn("private", str(log.call_args_list))
                finally:
                    self.cleanup(owner, threads)

    def test_join_timeout_is_bounded_and_reported_without_false_stopped_claim(self):
        for kind in ("event", "storage"):
            with self.subTest(kind=kind):
                owner, _, construct = self.partial(kind)
                original = RuntimeError("start failure")
                thread = Mock(ident=123)
                thread.start.side_effect = original
                thread.is_alive.return_value = True
                with (
                    patch("rp_ylx.daemon.threading.Thread", return_value=thread),
                    patch("rp_ylx.daemon._OPERATIONAL_LOG") as log,
                    self.assertRaises(RuntimeError) as raised,
                ):
                    construct()
                self.assertIs(raised.exception, original)
                self.assertTrue(self.stop(owner).is_set())
                thread.join.assert_called_once()
                timeout = thread.join.call_args.kwargs["timeout"]
                self.assertGreater(timeout, 0)
                self.assertLessEqual(timeout, 2.0)
                outcomes = [call.kwargs.get("outcome") for call in log.event.call_args_list]
                self.assertIn("stop_requested", outcomes)
                self.assertNotIn("stopped", outcomes)

    def test_successful_admission_runs_worker_and_preserves_thread_contract(self):
        for kind in ("event", "storage"):
            with self.subTest(kind=kind):
                owner, cls, construct = self.partial(kind)
                entered = threading.Event()
                threads = []
                observed = []

                def start(thread, threads=threads, entered=entered, observed=observed):
                    threads.append(thread)
                    _REAL_START(thread)
                    observed.append(entered.wait(0.03))

                try:
                    with (
                        patch.object(cls, "_run", side_effect=entered.set),
                        patch("rp_ylx.daemon.threading.Thread.start", start),
                    ):
                        construct()
                        self.assertTrue(entered.wait(1))
                    self.assertEqual(observed, [False])
                    self.assertEqual(threads[0].daemon, kind == "storage")
                    owner.close()
                    self.assertFalse(threads[0].is_alive())
                finally:
                    self.cleanup(owner, threads)

    def test_service_builder_unwinds_other_resources_after_real_worker_cancellation(self):
        for failing_name in ("rp-ylx-capture-events", "storage-reserve"):
            with self.subTest(thread=failing_name), tempfile.TemporaryDirectory() as directory:
                config = fixture.ProductionDaemonTest().config(Path(directory))
                coordinator = Mock()
                coordinator.capture_recording_progress.return_value = ("epoch", 1, None)
                coordinator.capture_snapshot_event.return_value = {
                    "authority_epoch": "epoch",
                    "source_revision": 1,
                    "data": {"active_recording": None},
                }
                server = Mock()
                publisher = Mock()
                threads = []
                original = KeyboardInterrupt("service startup cancelled")

                def start(thread, threads=threads, original=original, failing_name=failing_name):
                    threads.append(thread)
                    _REAL_START(thread)
                    if thread.name == failing_name:
                        raise original

                # Retain test-only stop handles even if a constructor never returns.
                stops = []
                real_event = threading.Event

                def event(stops=stops, real_event=real_event):
                    result = real_event()
                    stops.append(result)
                    return result

                try:
                    with (
                        patch("rp_ylx.daemon.__commit__", "a" * 40),
                        patch("rp_ylx.daemon.stable_id_for_device", return_value="camera"),
                        patch("rp_ylx.daemon.ContinuousCaptureSources"),
                        patch("rp_ylx.daemon.CaptureCoordinator", return_value=coordinator),
                        patch("rp_ylx.daemon.create_gateway_server", return_value=server),
                        patch("rp_ylx.daemon.threading.Thread.start", start),
                        patch("rp_ylx.daemon.threading.Event", event),
                        self.assertRaises(KeyboardInterrupt) as raised,
                    ):
                        build_production_service(
                            config,
                            camera_backend_factory=Mock(),
                            imu_source_factory=Mock(),
                            mount_checker=lambda path: False,
                            mdns_publisher_factory=Mock(return_value=publisher),
                        )
                    self.assertIs(raised.exception, original)
                    server.server_close.assert_called_once_with()
                    coordinator.close.assert_called_once_with()
                    self.assertTrue(all(not thread.is_alive() for thread in threads))
                    if failing_name == "storage-reserve":
                        publisher.close.assert_called_once_with()
                finally:
                    for stop in stops:
                        stop.set()
                    for thread in threads:
                        _REAL_JOIN(thread, 2)
                        self.assertFalse(thread.is_alive())


if __name__ == "__main__":
    unittest.main()

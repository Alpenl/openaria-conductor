from __future__ import annotations

import os
import signal
import socketserver
import tempfile
import unittest
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

from rp_ylx.daemon import (
    CaptureEventPump,
    ProductionService,
    StorageReserveMonitor,
    build_production_service,
    run_production_service,
)
from tests import test_daemon as fixture

_STAGES = (
    ("storage_monitor", "close"),
    ("server", "server_close"),
    ("mdns_publisher", "close"),
    ("event_pump", "close"),
    ("coordinator", "close"),
)


class ServiceCleanupIsolationTest(unittest.TestCase):
    def components(self, failures=None, monitor=True):
        failures = failures or {}
        calls = []
        components = {name: Mock() for name, _ in _STAGES}
        for name, method in _STAGES:

            def close(name=name):
                calls.append(name)
                error = failures.get(name)
                if error is not None:
                    raise error

            getattr(components[name], method).side_effect = close
        if not monitor:
            components["storage_monitor"] = None
        return (ProductionService(**components), calls, components)

    def test_every_stage_is_attempted_after_any_close_failure(self):
        for stage, _ in _STAGES:
            for error_type in (RuntimeError, MemoryError, KeyboardInterrupt):
                with self.subTest(stage=stage, error=error_type.__name__):
                    error = error_type("injected component failure")
                    service, calls, _ = self.components({stage: error})
                    with self.assertRaises(error_type) as raised:
                        service.close()
                    self.assertIs(raised.exception, error)
                    self.assertEqual(calls, [name for name, _ in _STAGES])

    def test_multiple_close_failures_preserve_first_and_report_each_stage(self):
        first = OSError("private-primary-detail")
        failures = {
            "storage_monitor": first,
            "mdns_publisher": RuntimeError("private-secondary"),
            "coordinator": MemoryError("private-third"),
        }
        service, calls, _ = self.components(failures)
        with (
            patch("rp_ylx.daemon._OPERATIONAL_LOG") as logger,
            self.assertRaises(OSError) as raised,
        ):
            service.close()
        self.assertIs(raised.exception, first)
        self.assertEqual(calls, [name for name, _ in _STAGES])
        stages = [call.kwargs.get("stage") for call in logger.event.call_args_list]
        self.assertEqual(stages, list(failures))
        self.assertNotIn("private-", str(logger.event.call_args_list))

    def test_cleanup_logging_failure_does_not_abort_or_mask_primary(self):
        original = RuntimeError("component failure")
        service, calls, _ = self.components({"server": original})
        with (
            patch("rp_ylx.daemon._OPERATIONAL_LOG.event", side_effect=OSError("log sink")),
            self.assertRaises(RuntimeError) as raised,
        ):
            service.close()
        self.assertIs(raised.exception, original)
        self.assertEqual(calls, [name for name, _ in _STAGES])

    def test_service_without_optional_monitor_closes_in_existing_order(self):
        service, calls, _ = self.components(monitor=False)
        service.close()
        self.assertEqual(calls, [name for name, _ in _STAGES if name != "storage_monitor"])

    def test_retry_after_transient_close_failure_is_not_disabled(self):
        failures = {"mdns_publisher": RuntimeError("temporary")}
        service, calls, _ = self.components(failures)
        with self.assertRaises(RuntimeError):
            service.close()
        self.assertEqual(calls, [name for name, _ in _STAGES])
        failures.clear()
        service.close()
        self.assertEqual(calls, [name for name, _ in _STAGES] * 2)

    def test_real_listener_workers_and_descriptors_close_after_mdns_error(self):

        class Coordinator:
            def __init__(self):
                self.descriptors = list(os.pipe())

            def capture_recording_progress(self):
                return ("epoch", 1, None)

            def capture_snapshot_event(self):
                return {
                    "authority_epoch": "epoch",
                    "source_revision": 1,
                    "data": {"active_recording": None},
                }

            def enforce_storage_reserve(self):
                pass

            def close(self):
                while self.descriptors:
                    os.close(self.descriptors.pop())

        coordinator = Coordinator()
        server = socketserver.TCPServer(("127.0.0.1", 0), socketserver.BaseRequestHandler)
        pump = CaptureEventPump(coordinator, Mock(), interval=0.01)
        monitor = StorageReserveMonitor(coordinator)
        publisher = Mock()
        error = RuntimeError("mDNS close failure")
        publisher.close.side_effect = error
        service = ProductionService(coordinator, server, pump, publisher, monitor)
        descriptors = tuple(coordinator.descriptors)
        try:
            with self.assertRaises(RuntimeError) as raised:
                service.close()
            self.assertIs(raised.exception, error)
            self.assertEqual(server.socket.fileno(), -1)
            self.assertFalse(monitor.thread.is_alive())
            self.assertFalse(pump._thread.is_alive())
            self.assertFalse(coordinator.descriptors)
            for descriptor in descriptors:
                with self.assertRaises(OSError):
                    os.fstat(descriptor)
        finally:
            monitor.close()
            server.server_close()
            pump.close()
            coordinator.close()

    def test_late_construction_failure_unwinds_every_constructed_component(self):
        for failing in ("StorageReserveMonitor", "ProductionService"):
            with self.subTest(failing=failing), tempfile.TemporaryDirectory() as directory:
                config = fixture.ProductionDaemonTest().config(Path(directory))
                components = {name: Mock() for name, _ in _STAGES}
                error = MemoryError("late construction")
                monitor_factory = Mock(return_value=components["storage_monitor"])
                service_factory = Mock()
                (
                    monitor_factory if failing == "StorageReserveMonitor" else service_factory
                ).side_effect = error
                with (
                    patch("rp_ylx.daemon.__commit__", "a" * 40),
                    patch("rp_ylx.daemon.stable_id_for_device", return_value="camera-stable"),
                    patch("rp_ylx.daemon.ContinuousCaptureSources"),
                    patch(
                        "rp_ylx.daemon.CaptureCoordinator", return_value=components["coordinator"]
                    ),
                    patch("rp_ylx.daemon.create_gateway_server", return_value=components["server"]),
                    patch("rp_ylx.daemon.CaptureEventPump", return_value=components["event_pump"]),
                    patch("rp_ylx.daemon.StorageReserveMonitor", monitor_factory),
                    patch("rp_ylx.daemon.ProductionService", service_factory),
                    self.assertRaises(MemoryError) as raised,
                ):
                    build_production_service(
                        config,
                        camera_backend_factory=Mock(return_value=Mock()),
                        imu_source_factory=Mock(),
                        mount_checker=lambda path: False,
                        mdns_publisher_factory=Mock(return_value=components["mdns_publisher"]),
                    )
                self.assertIs(raised.exception, error)
                for stage, method in _STAGES:
                    if stage == "storage_monitor" and failing == "StorageReserveMonitor":
                        getattr(components[stage], method).assert_not_called()
                    else:
                        getattr(components[stage], method).assert_called_once_with()

    @contextmanager
    def runner(
        self,
        *,
        serve_error=None,
        close_error=None,
        log_failure=None,
        install_failure=None,
        restore_failure=None,
        build_error=None,
    ):
        service = Mock()
        service.server.serve_forever.side_effect = serve_error
        service.close.side_effect = close_error
        config = SimpleNamespace(security_profile="lab", data_plane="rust", port=8080)
        saved = {signal.SIGINT: object(), signal.SIGTERM: object()}
        signals = []

        def set_signal(signum, handler):
            restoring = handler is saved[signum]
            signals.append(("restore" if restoring else "install", signum))
            injection = restore_failure if restoring else install_failure
            if injection is not None and signum == injection[0]:
                raise injection[1]
            return saved[signum]

        def event(name, **kwargs):
            if log_failure is not None and name == log_failure[0]:
                raise log_failure[1]

        with (
            patch("rp_ylx.settings.initialize"),
            patch("rp_ylx.daemon.load_production_config", return_value=config),
            patch(
                "rp_ylx.daemon.build_production_service",
                return_value=service,
                side_effect=build_error,
            ),
            patch("rp_ylx.daemon.signal.signal", side_effect=set_signal),
            patch("rp_ylx.daemon._OPERATIONAL_LOG.event", side_effect=event),
        ):
            yield (service, signals)

    def test_serving_error_survives_close_and_signal_restoration_failures(self):
        original = ValueError("serve failure")
        with (
            self.runner(
                serve_error=original,
                close_error=RuntimeError("close failure"),
                restore_failure=(signal.SIGINT, OSError("restore failure")),
            ) as (service, signals),
            self.assertRaises(ValueError) as raised,
        ):
            run_production_service("unused.json")
        self.assertIs(raised.exception, original)
        service.close.assert_called_once_with()
        self.assertEqual(
            [sig for action, sig in signals if action == "restore"], [signal.SIGINT, signal.SIGTERM]
        )

    def test_lifecycle_log_failures_still_close_resources_and_restore_signals(self):
        for name in ("production_service_ready", "production_service_stopping"):
            with self.subTest(event=name):
                original = OSError("log sink")
                with (
                    self.runner(log_failure=(name, original)) as (service, signals),
                    self.assertRaises(OSError) as raised,
                ):
                    run_production_service("unused.json")
                self.assertIs(raised.exception, original)
                service.close.assert_called_once_with()
                installs = [sig for action, sig in signals if action == "install"]
                restores = [sig for action, sig in signals if action == "restore"]
                self.assertEqual(restores, installs)

    def test_partial_signal_install_failure_closes_service_and_restores_first(self):
        original = RuntimeError("signal registration")
        with (
            self.runner(install_failure=(signal.SIGTERM, original)) as (service, signals),
            self.assertRaises(RuntimeError) as raised,
        ):
            run_production_service("unused.json")
        self.assertIs(raised.exception, original)
        service.close.assert_called_once_with()
        self.assertEqual([sig for action, sig in signals if action == "restore"], [signal.SIGINT])

    def test_signal_restoration_failure_does_not_skip_remaining_handlers(self):
        original = OSError("restore failure")
        with (
            self.runner(restore_failure=(signal.SIGINT, original)) as (service, signals),
            self.assertRaises(OSError) as raised,
        ):
            run_production_service("unused.json")
        self.assertIs(raised.exception, original)
        service.close.assert_called_once_with()
        self.assertEqual(
            [sig for action, sig in signals if action == "restore"], [signal.SIGINT, signal.SIGTERM]
        )

    def test_keyboard_interrupt_remains_a_normal_shutdown_request(self):
        with self.runner(serve_error=KeyboardInterrupt()) as (service, signals):
            run_production_service("unused.json")
        service.close.assert_called_once_with()
        self.assertEqual(
            [sig for action, sig in signals if action == "restore"], [signal.SIGINT, signal.SIGTERM]
        )

    def test_startup_failure_reporting_cannot_mask_original_error(self):
        original = MemoryError("build failure")
        with (
            self.runner(
                build_error=original,
                log_failure=("production_service_start_failed", OSError("log sink")),
            ) as (service, _),
            self.assertRaises(MemoryError) as raised,
        ):
            run_production_service("unused.json")
        self.assertIs(raised.exception, original)
        service.close.assert_not_called()


if __name__ == "__main__":
    unittest.main()

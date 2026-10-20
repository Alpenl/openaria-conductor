from __future__ import annotations

import time
import unittest
from unittest.mock import Mock, patch

from rp_ylx.mdns import MDNS_SERVICE_TYPES, MdnsPublisher
from tests.test_mdns import FakeResponder


class MdnsDiagnosticIsolationTest(unittest.TestCase):
    def publisher(self, *, logger=None, provider=None):
        responders = []

        def factory(addresses):
            responder = FakeResponder(addresses)
            responders.append(responder)
            return responder

        publisher = MdnsPublisher(
            8080,
            interval=0.01,
            logger=logger,
            address_provider=provider or (lambda: ("192.0.2.2",)),
            responder_factory=factory,
        )
        self.addCleanup(publisher.close)
        return publisher, responders

    def test_failure_sink_error_preserves_episode_and_duplicate_suppression(self):
        publisher, _ = self.publisher()
        with patch("rp_ylx.mdns._OPERATIONAL_LOG.event", side_effect=OSError("sink")) as log:
            publisher._record_failure(OSError("private source detail"))
            for _ in range(4):
                publisher._record_failure(OSError("private repeated detail"))
            self.assertEqual(publisher._failure_type, "OSError")
            self.assertEqual(publisher._suppressed_failures, 4)
            log.assert_called_once()
            self.assertNotIn("private", str(log.call_args_list))
            publisher._record_recovered()
            self.assertIsNone(publisher._failure_type)
            self.assertEqual(publisher._suppressed_failures, 0)
            publisher._record_failure(OSError("new episode"))
            self.assertEqual(log.call_count, 3)

    def test_optional_failure_logger_cannot_skip_structured_diagnostic(self):
        optional = Mock()
        optional.error.side_effect = OSError("custom sink")
        publisher, _ = self.publisher(logger=optional)
        with patch("rp_ylx.mdns._OPERATIONAL_LOG.event") as log:
            publisher._record_failure(RuntimeError("private diagnostic"))
        self.assertEqual(publisher._failure_type, "RuntimeError")
        self.assertEqual(log.call_args.args[0], "mdns_publication_failed")
        self.assertEqual(log.call_args.kwargs["exception_type"], "RuntimeError")
        self.assertNotIn("private", str(optional.mock_calls) + str(log.mock_calls))

    def test_recovery_updates_state_even_when_either_or_both_sinks_fail(self):
        for sink in ("optional", "structured", "both"):
            with self.subTest(sink=sink):
                optional = Mock()
                publisher, _ = self.publisher(logger=optional)
                publisher._record_failure(OSError("first failure"))
                publisher._record_failure(OSError("same failure"))
                if sink in ("optional", "both"):
                    optional.info.side_effect = OSError("custom sink")
                with patch("rp_ylx.mdns._OPERATIONAL_LOG.event") as log:
                    if sink in ("structured", "both"):
                        log.side_effect = OSError("structured sink")
                    publisher._record_recovered()
                    publisher._record_recovered()
                    self.assertIsNone(publisher._failure_type)
                    self.assertEqual(publisher._suppressed_failures, 0)
                    log.assert_called_once()
                    self.assertEqual(log.call_args.kwargs["suppressed_count"], 1)
                    optional.info.assert_called_once()

    def test_unprintable_error_and_broken_code_property_do_not_break_diagnostics(self):
        class BrokenError(Exception):
            @property
            def code(self):
                raise OSError("private property")

            def __str__(self):
                raise OSError("private formatting")

        optional = Mock()
        publisher, _ = self.publisher(logger=optional)
        with patch("rp_ylx.mdns._OPERATIONAL_LOG.event") as log:
            publisher._record_failure(BrokenError("private payload"))
        self.assertEqual(log.call_args.kwargs["error_code"], "mdns_publication_failed")
        self.assertEqual(log.call_args.kwargs["exception_type"], "BrokenError")
        self.assertEqual(optional.error.call_args.args[-1], "BrokenError")
        self.assertNotIn("private", str(optional.mock_calls) + str(log.mock_calls))

    def test_ready_log_error_does_not_turn_published_services_into_failure(self):
        publisher, responders = self.publisher()
        with patch("rp_ylx.mdns._OPERATIONAL_LOG.event", side_effect=OSError("sink")):
            publisher._reconcile()
            publisher._reconcile()
        self.assertEqual(len(responders), 1)
        self.assertEqual(len(responders[0].registered), len(MDNS_SERVICE_TYPES))
        self.assertFalse(responders[0].closed)
        self.assertIs(publisher._responder, responders[0])
        self.assertIsNone(publisher._failure_type)
        publisher.close()
        self.assertTrue(responders[0].closed)

    def test_real_worker_survives_diagnostic_failures_and_refreshes_addresses(self):
        for failing in (
            "mdns_publication_failed",
            "mdns_publication_recovered",
            "mdns_publication_ready",
            "optional",
        ):
            with self.subTest(log=failing):
                optional = Mock()
                if failing == "optional":
                    optional.error.side_effect = OSError("optional error sink")
                    optional.info.side_effect = OSError("optional info sink")
                current = ["192.0.2.2"]
                samples = []

                def provider(samples=samples, current=current):
                    samples.append(1)
                    if len(samples) == 1:
                        raise OSError("transient address error")
                    return (current[0],)

                publisher, responders = self.publisher(logger=optional, provider=provider)

                def event(name, failing=failing, **kwargs):
                    if name == failing:
                        raise OSError("structured sink")

                errors = []
                with (
                    patch("rp_ylx.mdns._OPERATIONAL_LOG.event", side_effect=event),
                    patch(
                        "threading.excepthook",
                        side_effect=lambda args, errors=errors: errors.append(args.exc_type),
                    ),
                ):
                    try:
                        publisher.start()
                        self.wait_for(lambda samples=samples: len(samples) >= 3)
                        self.assertTrue(publisher._thread.is_alive())
                        self.assertIsNone(publisher._failure_type)
                        self.assertEqual(len(responders), 1)
                        current[0] = "192.0.2.3"
                        self.wait_for(
                            lambda responders=responders: (
                                len(responders) == 2
                                and len(responders[1].registered) == len(MDNS_SERVICE_TYPES)
                            )
                        )
                        self.assertTrue(responders[0].closed)
                        self.assertTrue(publisher._thread.is_alive())
                    finally:
                        publisher.close()
                self.assertEqual(errors, [])
                self.assertTrue(all(responder.closed for responder in responders))

    def test_persistent_address_and_log_errors_preserve_wait_and_suppression(self):
        class Stop:
            def __init__(self):
                self.waits = []

            def is_set(self):
                return len(self.waits) >= 1000

            def wait(self, interval):
                self.waits.append(interval)
                return self.is_set()

        optional = Mock()
        optional.error.side_effect = OSError("sink")

        def unavailable():
            raise OSError("address unavailable")

        provider = Mock(side_effect=unavailable)
        publisher, _ = self.publisher(logger=optional, provider=provider)
        stop = Stop()
        with patch("rp_ylx.mdns._OPERATIONAL_LOG.event", side_effect=OSError("sink")) as log:
            publisher._run(stop)
        self.assertEqual(provider.call_count, 1000)
        self.assertEqual(stop.waits, [0.01] * 1000)
        self.assertEqual(publisher._suppressed_failures, 999)
        optional.error.assert_called_once()
        log.assert_called_once()

    def test_different_failure_types_keep_distinct_episodes_despite_sink_errors(self):
        publisher, _ = self.publisher()
        with patch("rp_ylx.mdns._OPERATIONAL_LOG.event", side_effect=OSError("sink")) as log:
            publisher._record_failure(OSError("one"))
            publisher._record_failure(OSError("two"))
            publisher._record_failure(ValueError("three"))
        self.assertEqual(publisher._failure_type, "ValueError")
        self.assertEqual(publisher._suppressed_failures, 0)
        self.assertEqual(
            [call.kwargs["exception_type"] for call in log.call_args_list],
            ["OSError", "ValueError"],
        )
        self.assertEqual(log.call_args_list[-1].kwargs["suppressed_count"], 1)

    def test_process_control_exceptions_from_diagnostics_are_not_hidden(self):
        for error_type in (KeyboardInterrupt, SystemExit):
            with self.subTest(error=error_type.__name__):
                publisher, _ = self.publisher()
                with (
                    patch("rp_ylx.mdns._OPERATIONAL_LOG.event", side_effect=error_type()),
                    self.assertRaises(error_type),
                ):
                    publisher._record_failure(OSError("source"))

    def wait_for(self, predicate):
        deadline = time.monotonic() + 1
        while not predicate() and time.monotonic() < deadline:
            time.sleep(0.005)
        self.assertTrue(predicate(), "worker did not recover before deadline")


if __name__ == "__main__":
    unittest.main()

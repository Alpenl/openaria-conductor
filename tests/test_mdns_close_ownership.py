from __future__ import annotations

import os
import time
import unittest
from contextlib import suppress
from unittest.mock import Mock, patch

from rp_ylx.mdns import MDNS_SERVICE_TYPES, MdnsPublisher
from tests.test_mdns import FakeResponder


class ResourceResponder(FakeResponder):
    def __init__(self, addresses, *, close_failures=0, registration_error=None):
        super().__init__(addresses)
        self.fds = list(os.pipe())
        self.close_failures = close_failures
        self.close_error = OSError("private close failure")
        self.close_calls = 0
        self.registration_error = registration_error
        self.registration_calls = 0
        self.unregister_failures = 0
        self.unregister_error = OSError("private unregister failure")
        self.unregister_calls = []

    def register_service(self, info, **kwargs):
        self.registration_calls += 1
        super().register_service(info, **kwargs)
        if self.registration_error is not None and self.registration_calls == 2:
            raise self.registration_error

    def unregister_service(self, info):
        self.unregister_calls.append(info)
        if self.unregister_failures:
            self.unregister_failures -= 1
            raise self.unregister_error
        super().unregister_service(info)

    def close(self):
        self.close_calls += 1
        if self.close_failures:
            if self.close_failures > 0:
                self.close_failures -= 1
            raise self.close_error
        while self.fds:
            os.close(self.fds.pop())
        super().close()


class MdnsCloseOwnershipTest(unittest.TestCase):
    def publisher(self, *, failures=1, registration_error=None):
        responders = []
        live_counts = []
        provider = Mock(return_value=("192.0.2.2",))

        def factory(addresses):
            live_counts.append(sum(not item.closed for item in responders))
            first = not responders
            item = ResourceResponder(
                addresses,
                close_failures=failures if first else 0,
                registration_error=registration_error if first else None,
            )
            responders.append(item)
            return item

        publisher = MdnsPublisher(
            8080, interval=0.01, address_provider=provider, responder_factory=factory
        )

        def cleanup():
            publisher._stop.set()
            for item in responders:
                item.close_failures = 0
                item.unregister_failures = 0
            publisher.close()
            for item in responders:
                if item.fds:
                    item.close_failures = 0
                    item.close()

        self.addCleanup(cleanup)
        return publisher, responders, provider, live_counts

    def test_close_failure_retains_owner_and_real_fds_until_successful_retry(self):
        publisher, responders, _, _ = self.publisher()
        publisher._reconcile()
        item = responders[0]
        descriptors = tuple(item.fds)
        with self.assertRaises(OSError) as raised:
            publisher.close()
        self.assertIs(raised.exception, item.close_error)
        self.assertIs(publisher._responder, item)
        self.assertEqual(publisher._addresses, ())
        for descriptor in descriptors:
            os.fstat(descriptor)
        publisher.close()
        self.assertIsNone(publisher._responder)
        self.assertTrue(item.closed)
        self.assertEqual(item.close_calls, 2)
        for descriptor in descriptors:
            with self.assertRaises(OSError):
                os.fstat(descriptor)

    def test_repeated_close_failure_never_reports_closed_or_drops_owner(self):
        publisher, responders, _, live_counts = self.publisher(failures=-1)
        publisher._reconcile()
        item = responders[0]
        with patch("rp_ylx.mdns._OPERATIONAL_LOG.event") as log:
            for _ in range(6):
                with self.assertRaises(OSError) as raised:
                    publisher.close()
                self.assertIs(raised.exception, item.close_error)
                self.assertIs(publisher._responder, item)
            self.assertNotIn(
                "mdns_publisher_stopped", [call.args[0] for call in log.call_args_list]
            )
        self.assertEqual(len(item.fds), 2)
        self.assertEqual(live_counts, [0])
        item.close_failures = 0
        publisher.close()
        self.assertTrue(item.closed)

    def test_address_switch_waits_for_previous_responder_cleanup(self):
        publisher, responders, provider, live_counts = self.publisher(failures=2)
        publisher._reconcile()
        item = responders[0]
        provider.return_value = ("192.0.2.3",)
        for _ in range(2):
            with self.assertRaises(OSError):
                publisher._reconcile()
            self.assertEqual(len(responders), 1)
            self.assertIs(publisher._responder, item)
        publisher._reconcile()
        self.assertEqual(len(responders), 2)
        self.assertTrue(item.closed)
        self.assertEqual(responders[1].addresses, ("192.0.2.3",))
        self.assertEqual(live_counts, [0, 0])

    def test_empty_addresses_do_not_skip_pending_cleanup(self):
        publisher, responders, provider, _ = self.publisher()
        publisher._reconcile()
        with self.assertRaises(OSError):
            publisher.close()
        provider.return_value = ()
        publisher._reconcile()
        self.assertTrue(responders[0].closed)
        self.assertIsNone(publisher._responder)
        self.assertEqual(len(responders), 1)

    def test_pending_cleanup_runs_before_unavailable_address_lookup(self):
        publisher, responders, provider, _ = self.publisher()
        publisher._reconcile()
        with self.assertRaises(OSError):
            publisher.close()
        provider.side_effect = OSError("address unavailable")
        with self.assertRaisesRegex(OSError, "address unavailable"):
            publisher._reconcile()
        self.assertTrue(responders[0].closed)
        self.assertIsNone(publisher._responder)

    def test_partial_registration_preserves_primary_error_and_pending_responder(self):
        original = ValueError("private registration failure")
        publisher, responders, _, live_counts = self.publisher(registration_error=original)
        with self.assertRaises(ValueError) as raised:
            publisher._reconcile()
        self.assertIs(raised.exception, original)
        item = responders[0]
        self.assertIs(publisher._responder, item)
        self.assertEqual(len(item.unregister_calls), 2)
        self.assertEqual(publisher._addresses, ())
        publisher._reconcile()
        self.assertTrue(item.closed)
        self.assertEqual(len(responders), 2)
        self.assertEqual(live_counts, [0, 0])

    def test_registration_control_or_memory_error_still_retains_cleanup_owner(self):
        for error_type in (KeyboardInterrupt, MemoryError):
            with self.subTest(error=error_type.__name__):
                original = error_type("registration interrupted")
                publisher, responders, _, _ = self.publisher(registration_error=original)
                with self.assertRaises(error_type) as raised:
                    publisher._reconcile()
                self.assertIs(raised.exception, original)
                self.assertIs(publisher._responder, responders[0])
                publisher.close()
                self.assertTrue(responders[0].closed)

    def test_unregister_errors_still_attempt_all_services_and_close(self):
        publisher, responders, _, _ = self.publisher(failures=0)
        publisher._reconcile()
        item = responders[0]
        item.unregister_failures = 2
        # A stopped lifecycle log may fail, but cleanup must already be complete.
        with (
            patch("rp_ylx.mdns._OPERATIONAL_LOG.event", side_effect=OSError("sink")),
            suppress(OSError),
        ):
            publisher.close()
        self.assertEqual(len(item.unregister_calls), len(MDNS_SERVICE_TYPES))
        self.assertTrue(item.closed)
        self.assertIsNone(publisher._responder)

    def test_retry_repeats_only_failed_unregister_and_preserves_close_error(self):
        publisher, responders, _, _ = self.publisher()
        publisher._reconcile()
        item = responders[0]
        item.unregister_failures = 1
        with (
            patch("rp_ylx.mdns._OPERATIONAL_LOG.event", side_effect=OSError("sink")),
            self.assertRaises(OSError) as raised,
        ):
            publisher.close()
        self.assertIs(raised.exception, item.close_error)
        self.assertIs(publisher._responder, item)
        failed_service = item.unregister_calls[0]
        publisher.close()
        self.assertTrue(item.closed)
        self.assertEqual(item.unregister_calls.count(failed_service), 2)
        self.assertEqual(len(item.unregister_calls), 3)

    def test_unregister_control_exception_is_preserved_after_best_effort_close(self):
        publisher, responders, _, _ = self.publisher(failures=0)
        publisher._reconcile()
        item = responders[0]
        original = KeyboardInterrupt("unregister interrupted")
        item.unregister_failures = 1
        item.unregister_error = original
        with self.assertRaises(KeyboardInterrupt) as raised:
            publisher.close()
        self.assertIs(raised.exception, original)
        self.assertEqual(len(item.unregister_calls), 2)
        self.assertTrue(item.closed)
        self.assertIsNone(publisher._responder)
        publisher.close()

    def test_unregister_interruption_and_close_failure_preserve_primary_and_owner(self):
        for error_type in (KeyboardInterrupt, SystemExit):
            with self.subTest(error=error_type.__name__):
                publisher, responders, _, _ = self.publisher()
                publisher._reconcile()
                item = responders[0]
                original = error_type("unregister interruption")
                item.unregister_failures = 1
                item.unregister_error = original
                with (
                    patch("rp_ylx.mdns._OPERATIONAL_LOG.event", side_effect=OSError("sink")),
                    self.assertRaises(error_type) as raised,
                ):
                    publisher.close()
                self.assertIs(raised.exception, original)
                self.assertIs(publisher._responder, item)
                self.assertEqual(len(item.fds), 2)
                publisher.close()
                self.assertTrue(item.closed)
                self.assertIsNone(publisher._responder)

    def test_worker_shutdown_cleanup_failure_can_be_retried_without_new_worker(self):
        publisher, responders, _, _ = self.publisher(failures=-1)
        uncaught = []
        with patch("threading.excepthook", side_effect=lambda args: uncaught.append(args.exc_type)):
            publisher.start()
            self.wait_for(lambda: responders and len(responders[0].registered) == 2)
            item = responders[0]
            with self.assertRaises(OSError) as raised:
                publisher.close()
            self.assertIs(raised.exception, item.close_error)
            self.assertIsNone(publisher._thread)
            self.assertIs(publisher._responder, item)
            self.assertGreaterEqual(item.close_calls, 2)
            with self.assertRaises(OSError):
                publisher.start()
            self.assertIsNone(publisher._thread)
            self.assertEqual(len(responders), 1)
            item.close_failures = 0
            publisher.close()
            publisher.start()
            self.wait_for(lambda: len(responders) == 2 and len(responders[1].registered) == 2)
            publisher.close()
        self.assertEqual(uncaught, [])
        self.assertTrue(all(item.closed for item in responders))

    def test_worker_recovers_address_change_after_transient_cleanup_failures(self):
        publisher, responders, provider, live_counts = self.publisher(failures=3)
        uncaught = []
        with patch("threading.excepthook", side_effect=lambda args: uncaught.append(args.exc_type)):
            publisher.start()
            self.wait_for(lambda: responders and len(responders[0].registered) == 2)
            provider.return_value = ("192.0.2.3",)
            self.wait_for(lambda: len(responders) == 2 and len(responders[1].registered) == 2)
            self.assertTrue(responders[0].closed)
            self.assertEqual(responders[0].close_calls, 4)
            self.assertEqual(live_counts, [0, 0])
            self.assertTrue(publisher._thread.is_alive())
            publisher.close()
        self.assertEqual(uncaught, [])

    def wait_for(self, predicate):
        deadline = time.monotonic() + 2
        while not predicate() and time.monotonic() < deadline:
            time.sleep(0.005)
        self.assertTrue(predicate(), "worker did not reach expected state")


if __name__ == "__main__":
    unittest.main()

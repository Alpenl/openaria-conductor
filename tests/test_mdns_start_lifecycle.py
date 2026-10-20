from __future__ import annotations

import threading
import unittest
from unittest.mock import patch

from rp_ylx.mdns import MDNS_SERVICE_TYPES, MdnsPublisher
from tests.test_mdns import FakeResponder

_REAL_THREAD = threading.Thread
_REAL_START = threading.Thread.start


class MdnsStartLifecycleTest(unittest.TestCase):
    def publisher(self):
        ready = threading.Event()
        responders = []

        class Responder(FakeResponder):
            def register_service(self, info, **kwargs):
                super().register_service(info, **kwargs)
                if len(self.registered) == len(MDNS_SERVICE_TYPES):
                    ready.set()

        def factory(addresses):
            responder = Responder(addresses)
            responders.append(responder)
            return responder

        publisher = MdnsPublisher(
            8080, address_provider=lambda: ("192.0.2.2",), responder_factory=factory, interval=0.01
        )

        def cleanup():
            publisher._stop.set()
            thread = publisher._thread
            if thread is not None and thread.ident is not None:
                thread.join(3)
            publisher._unpublish()

        self.addCleanup(cleanup)
        return (publisher, ready, responders)

    def assert_retry_publishes_and_closes(self, publisher, ready, responders):
        ready.clear()
        publisher.start()
        self.assertTrue(ready.wait(2), "retry did not publish")
        publisher.close()
        publisher.close()
        self.assertTrue(responders[-1].closed)
        self.assertEqual(len(responders[-1].unregistered), len(MDNS_SERVICE_TYPES))

    def test_thread_admission_failure_can_close_and_retry(self):
        for error in (RuntimeError("thread quota"), MemoryError("thread allocation")):
            with self.subTest(error=type(error).__name__):
                publisher, ready, responders = self.publisher()
                with (
                    patch("rp_ylx.mdns.threading.Thread.start", side_effect=error),
                    self.assertRaises(type(error)) as raised,
                ):
                    publisher.start()
                self.assertIs(raised.exception, error)
                publisher.close()
                self.assertIsNone(publisher._thread)
                self.assert_retry_publishes_and_closes(publisher, ready, responders)

    def test_constructor_failure_leaves_retry_available(self):
        publisher, ready, responders = self.publisher()
        with (
            patch("rp_ylx.mdns.threading.Thread", side_effect=MemoryError("constructor")),
            self.assertRaises(MemoryError),
        ):
            publisher.start()
        publisher.close()
        self.assert_retry_publishes_and_closes(publisher, ready, responders)

    def test_failed_admission_never_logs_publisher_started(self):
        publisher, _, _ = self.publisher()
        with patch("rp_ylx.mdns._OPERATIONAL_LOG") as logger:
            with (
                patch("rp_ylx.mdns.threading.Thread.start", side_effect=RuntimeError("quota")),
                self.assertRaises(RuntimeError),
            ):
                publisher.start()
            self.assertNotIn(
                "mdns_publisher_started", [call.args[0] for call in logger.event.call_args_list]
            )

    def test_failure_logging_cannot_replace_the_original_admission_error(self):
        publisher, ready, responders = self.publisher()
        original = RuntimeError("thread quota")
        with (
            patch("rp_ylx.mdns.threading.Thread.start", side_effect=original),
            patch("rp_ylx.mdns._OPERATIONAL_LOG.event", side_effect=OSError("log sink")),
            self.assertRaises(RuntimeError) as raised,
        ):
            publisher.start()
        self.assertIs(raised.exception, original)
        publisher.close()
        self.assert_retry_publishes_and_closes(publisher, ready, responders)

    def test_exception_after_real_thread_start_retains_joinable_owner(self):
        publisher, ready, responders = self.publisher()
        error = RuntimeError("injected after actual thread start")

        def start_then_fail(thread):
            _REAL_START(thread)
            raise error

        with (
            patch("rp_ylx.mdns.threading.Thread.start", new=start_then_fail),
            self.assertRaises(RuntimeError) as raised,
        ):
            publisher.start()
        self.assertIs(raised.exception, error)
        self.assertIsNotNone(publisher._thread)
        self.assertIsNotNone(publisher._thread.ident)
        self.assertTrue(publisher._stop.is_set())
        with self.assertRaises(RuntimeError):
            publisher.start()
        publisher.close()
        self.assert_retry_publishes_and_closes(publisher, ready, responders)

    def test_started_log_failure_stops_worker_without_masking_original_error(self):
        publisher, ready, responders = self.publisher()
        error = OSError("log sink failure")

        def event(name, **kwargs):
            if name == "mdns_publisher_started":
                raise error

        with patch("rp_ylx.mdns._OPERATIONAL_LOG.event", side_effect=event):
            with self.assertRaises(OSError) as raised:
                publisher.start()
            self.assertIs(raised.exception, error)
            publisher.close()
        self.assertTrue(all(responder.closed for responder in responders))
        self.assert_retry_publishes_and_closes(publisher, ready, responders)

    def test_delayed_failed_target_cannot_use_retry_stop_event_or_unpublish_retry(self):
        publisher, ready, responders = self.publisher()
        captured = {}

        class DelayedThread:
            ident = None

            def __init__(self, *, target, args=(), **kwargs):
                captured.update(target=target, args=args)

            def start(self):
                raise RuntimeError("admission interrupted before worker entry")

        with patch("rp_ylx.mdns.threading.Thread", DelayedThread), self.assertRaises(RuntimeError):
            publisher.start()
        publisher.start()
        self.assertTrue(ready.wait(2))
        active = responders[-1]
        late = _REAL_THREAD(target=captured["target"], args=captured["args"], daemon=True)
        late.start()
        try:
            late.join(1)
            self.assertFalse(late.is_alive(), "failed attempt reused retry stop event")
            self.assertFalse(active.closed, "failed attempt unpublished the new responder")
            self.assertIs(publisher._responder, active)
        finally:
            publisher._stop.set()
            late.join(2)
        publisher.close()

    def test_concurrent_start_admits_only_one_worker(self):
        publisher, ready, responders = self.publisher()
        entered = threading.Event()
        second_constructor = threading.Event()
        release = threading.Event()
        attempting_second = threading.Event()
        workers = []
        errors = []

        def factory(**kwargs):
            worker = _REAL_THREAD(**kwargs)
            workers.append(worker)
            entered.set()
            if len(workers) > 1:
                second_constructor.set()
            if not release.wait(3):
                raise RuntimeError("test constructor barrier timed out")
            return worker

        def start(second=False):
            if second:
                attempting_second.set()
            try:
                publisher.start()
            except Exception as error:
                errors.append(error)

        first = _REAL_THREAD(target=start)
        second = _REAL_THREAD(target=lambda: start(True))
        with patch("rp_ylx.mdns.threading.Thread", side_effect=factory):
            first.start()
            try:
                self.assertTrue(entered.wait(2))
                second.start()
                self.assertTrue(attempting_second.wait(2))
                second_constructor.wait(0.2)
            finally:
                release.set()
                first.join(3)
                if second.ident is not None:
                    second.join(3)
        try:
            self.assertEqual(len(workers), 1)
            self.assertEqual(len(errors), 1)
            self.assertRegex(str(errors[0]), "already been started")
            self.assertTrue(ready.wait(2))
        finally:
            publisher._stop.set()
            for worker in workers:
                if worker.ident is not None:
                    worker.join(3)
        publisher.close()

    def test_close_waits_for_start_admission_then_joins_the_worker(self):
        publisher, _, _ = self.publisher()
        entered = threading.Event()
        release = threading.Event()
        close_called = threading.Event()
        close_done = threading.Event()
        errors = []

        def factory(**kwargs):
            entered.set()
            if not release.wait(3):
                raise RuntimeError("test constructor barrier timed out")
            return _REAL_THREAD(**kwargs)

        def start():
            try:
                publisher.start()
            except Exception as error:
                errors.append(error)

        def close():
            close_called.set()
            try:
                publisher.close()
            except Exception as error:
                errors.append(error)
            finally:
                close_done.set()

        starter = _REAL_THREAD(target=start)
        closer = _REAL_THREAD(target=close)
        with patch("rp_ylx.mdns.threading.Thread", side_effect=factory):
            starter.start()
            try:
                self.assertTrue(entered.wait(2))
                closer.start()
                self.assertTrue(close_called.wait(2))
                self.assertFalse(close_done.wait(0.2), "close returned during startup admission")
            finally:
                release.set()
                starter.join(3)
                if closer.ident is not None:
                    closer.join(3)
        self.assertFalse(errors)
        self.assertIsNone(publisher._thread)


if __name__ == "__main__":
    unittest.main()

from __future__ import annotations

import hashlib
import json
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from unittest.mock import patch

import test_capture_coordinator as fixtures

from rp_ylx.api import ProviderError
from rp_ylx.api.downloads import ArtifactAccessError


class CatalogVerificationConcurrencyTest(unittest.TestCase):
    def setUp(self):
        self.fixture = fixtures.CaptureCoordinatorTest()
        self.fixture.setUp()
        self.addCleanup(self.fixture.tearDown)
        original = self.fixture.coordinator()
        try:
            self.session = self.fixture.seal_one(original)
        finally:
            original.close()
        self.coordinator = self.fixture.coordinator()
        self.addCleanup(self.coordinator.close)
        self.directory = self.fixture.mountpoint / "recordings" / self.session
        payload = (self.directory / "manifest.json").read_bytes()
        self.manifest = json.loads(payload)
        self.artifact = self.manifest["video"]["segments"][0]["artifacts"]["left"]["artifact_id"]
        self.deletion = fixtures.command(
            "delete-during-scan",
            {
                "schema": "ylx.session-delete-request.v1",
                "sessions": [
                    {
                        "session_id": self.session,
                        "manifest_sha256": hashlib.sha256(payload).hexdigest(),
                    }
                ],
            },
        )

    def diagnostic_artifact(self, session, artifact, version, *, cancelled=None):
        self.coordinator._verify_catalog_session(session, cancelled=cancelled)
        return self.coordinator.open_cancellable_artifact(
            session, artifact, version, cancelled=cancelled
        )

    @contextmanager
    def scanning(self, *, cancelled=None):
        entered, release = threading.Event(), threading.Event()
        original = self.coordinator._verify_exact_session_payload

        def slow(*args, **kwargs):
            entered.set()
            for _ in range(500):
                kwargs["interrupt_check"]()
                if release.wait(0.01):
                    return original(*args, **kwargs)
            raise AssertionError("test scan was not released")

        with (
            patch.object(self.coordinator, "_verify_exact_session_payload", side_effect=slow),
            ThreadPoolExecutor(max_workers=3) as pool,
        ):
            future = pool.submit(
                self.diagnostic_artifact,
                self.session,
                self.artifact,
                "v4",
                cancelled=cancelled,
            )
            try:
                self.assertTrue(entered.wait(2))
                yield pool, future, release
            finally:
                release.set()
                try:
                    representation = future.result(timeout=3)
                except (ProviderError, ArtifactAccessError):
                    pass
                else:
                    representation.close()

    def test_listing_and_clock_do_not_wait_for_full_payload_scan(self):
        with (
            self.scanning() as (pool, _, _),
            patch.object(self.coordinator, "_clock_control", return_value={"source": "client"}),
        ):
            listing = pool.submit(
                self.coordinator.list_sessions, cursor=None, limit=50, take_id=None
            ).result(timeout=1)
            clock = pool.submit(self.coordinator.sync_clock, "operator", {}).result(timeout=1)
            self.assertEqual(listing["items"][0]["session_id"], self.session)
            self.assertIsNone(listing["items"][0]["verification"])
            self.assertEqual(clock, {"source": "client"})

    def test_delete_cancels_scan_and_retries_with_same_idempotency_key(self):
        with self.scanning() as (pool, scan, _):
            with self.assertRaises(ProviderError) as busy:
                pool.submit(self.coordinator.delete_sessions, self.deletion).result(timeout=1)
            self.assertEqual(busy.exception.code, "verification_cancelling")
            self.assertTrue(busy.exception.retryable)
            with self.assertRaises(ProviderError) as cancelled:
                scan.result(timeout=1)
            self.assertEqual(cancelled.exception.code, "verification_cancelled")
            self.assertEqual(self.coordinator.open_handle_count, 0)
            result = self.coordinator.delete_sessions(self.deletion)
            self.assertEqual(result.body["deleted_session_ids"], [self.session])
            self.assertFalse(self.directory.exists())
            replay = self.coordinator.delete_sessions(self.deletion)
            self.assertTrue(replay.replayed)

    def test_capture_start_interrupts_historical_scan(self):
        with self.scanning() as (pool, scan, _):
            started = pool.submit(
                self.coordinator.start_capture, fixtures.start_command("start-during-scan")
            ).result(timeout=1)
            self.assertEqual(started.status, 202)
            with self.assertRaises(ProviderError) as cancelled:
                scan.result(timeout=1)
            self.assertEqual(cancelled.exception.code, "verification_cancelled")
            self.assertNotIn(self.session, self.coordinator._verified)

    def test_cancelled_request_releases_scan_and_can_retry(self):
        disconnected = threading.Event()
        with self.scanning(cancelled=disconnected.is_set) as (_, scan, _):
            disconnected.set()
            with self.assertRaises(ProviderError):
                scan.result(timeout=1)
            self.assertIsNone(self.coordinator._catalog_verification_session)
            self.assertEqual(self.coordinator.open_handle_count, 0)
            self.assertNotIn(self.session, self.coordinator._verified)
        with self.coordinator.open_verified_artifact(self.session, self.artifact, "v4"):
            pass

    def test_second_download_waits_without_duplicate_scan_or_control_lock(self):
        with self.scanning() as (pool, _, release):
            second = pool.submit(self.diagnostic_artifact, self.session, self.artifact, "v4")
            with patch.object(self.coordinator, "_clock_control", return_value={}):
                pool.submit(self.coordinator.sync_clock, "operator", {}).result(timeout=1)
            self.assertFalse(second.done())
            release.set()
            second.result(timeout=1).close()

    def test_shutdown_cancels_scan_and_waits_for_its_handles(self):
        with self.scanning() as (pool, scan, _):
            pool.submit(self.coordinator.close).result(timeout=1)
            with self.assertRaises(ProviderError):
                scan.result(timeout=1)
            self.assertIsNone(self.coordinator._catalog_verification_session)
            self.assertEqual(self.coordinator.open_handle_count, 0)

    def test_cancelled_waiter_does_not_cancel_the_active_download(self):
        cancelled = threading.Event()
        with self.scanning() as (pool, first, release):
            second = pool.submit(
                self.diagnostic_artifact,
                self.session,
                self.artifact,
                "v4",
                cancelled=cancelled.is_set,
            )
            cancelled.set()
            with self.assertRaises(ProviderError):
                second.result(timeout=1)
            self.assertFalse(first.done())
            self.assertFalse(self.coordinator._catalog_verification_cancel.is_set())
            release.set()
            first.result(timeout=1).close()

    def test_delete_cancellation_also_rejects_already_queued_download(self):
        queued = threading.Event()
        lock = self.coordinator._catalog_verification_lock

        class ObservedLock:
            def acquire(self, **kwargs):
                queued.set()
                return lock.acquire(**kwargs)

            def release(self):
                lock.release()

        with (
            self.scanning() as (pool, first, _),
            patch.object(self.coordinator, "_catalog_verification_lock", ObservedLock()),
        ):
            second = pool.submit(self.diagnostic_artifact, self.session, self.artifact, "v4")
            self.assertTrue(queued.wait(1))
            with self.assertRaises(ProviderError):
                self.coordinator.delete_sessions(self.deletion)
            for future in (first, second):
                with self.assertRaises(ProviderError):
                    future.result(timeout=1)
            self.assertIsNone(self.coordinator._catalog_verification_session)
            self.assertEqual(self.coordinator.delete_sessions(self.deletion).status, 200)

    def test_file_changed_before_publication_is_never_admitted(self):
        original = self.coordinator._verify_exact_session_payload
        left = self.manifest["video"]["segments"][0]["artifacts"]["left"]
        path = self.directory / left["path"]

        def change_after_scan(*args, **kwargs):
            result = original(*args, **kwargs)
            contents = path.read_bytes()
            path.write_bytes(bytes([contents[0] ^ 1]) + contents[1:])
            return result

        with (
            patch.object(
                self.coordinator, "_verify_exact_session_payload", side_effect=change_after_scan
            ),
            self.assertRaises(ProviderError) as changed,
        ):
            self.diagnostic_artifact(self.session, self.artifact, "v4")
        self.assertEqual(changed.exception.code, "verification_cancelled")
        self.assertNotIn(self.session, self.coordinator._verified)
        self.assertIsNone(self.coordinator._catalog_verification_session)
        self.assertEqual(self.coordinator.open_handle_count, 0)

    def test_delete_never_waits_for_catalog_with_control_lock_held(self):
        with ThreadPoolExecutor(max_workers=2) as pool, self.coordinator._catalog_lock:
            with self.assertRaises(ProviderError) as busy:
                pool.submit(self.coordinator.delete_sessions, self.deletion).result(timeout=1)
            self.assertEqual(busy.exception.code, "catalog_busy")
            with patch.object(self.coordinator, "_clock_control", return_value={}):
                pool.submit(self.coordinator.sync_clock, "operator", {}).result(timeout=1)

    def test_safe_swap_fences_download_scans_before_any_payload_is_opened(self):
        for field, value in (
            ("_pending_safe_swap", {"session_id": self.session}),
            ("_released", True),
            ("_async_stop_reason", "safe_swap"),
        ):
            with (
                self.subTest(field=field),
                patch.object(self.coordinator, field, value),
                patch.object(self.coordinator, "_verify_exact_session_payload") as verify,
                self.assertRaises(ProviderError) as rejected,
            ):
                self.coordinator.open_verified_artifact(self.session, self.artifact, "v4")
            self.assertEqual(rejected.exception.code, "volume_releasing")
            verify.assert_not_called()
            self.assertEqual(self.coordinator.open_handle_count, 0)


if __name__ == "__main__":
    unittest.main()

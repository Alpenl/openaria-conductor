from __future__ import annotations

import unittest
import uuid
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import Mock, patch

import tests.test_firmware_control as fixtures
from rp_ylx import firmware_control as control


class FirmwareCacheClockTest(unittest.TestCase):
    def setUp(self):
        self.fixture = fixtures.FirmwareControlTest()
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        self.s = self.fixture.supervisor
        self.fetch = self.fixture.fetch
        wall = patch.object(control.time, "time", return_value=10000)
        self.wall = wall.start()
        self.addCleanup(wall.stop)
        mono = patch.object(control.time, "monotonic", return_value=10)
        self.mono = mono.start()
        self.addCleanup(mono.stop)

    def next_release(self):
        self.fetch.return_value = {**self.fixture.manifest, "commit": "d" * 40}

    def test_backward_wall_jump_does_not_extend_expired_cache(self):
        self.s.check()
        self.next_release()
        self.wall.return_value = 9000
        self.mono.return_value = 10 + control.TTL + 10
        status = self.s.check()
        self.assertEqual(self.fetch.call_count, 2)
        self.assertEqual(status["available"]["commit"], "d" * 40)
        self.assertEqual(status["checked_at"], 9000)

    def test_forward_wall_jump_does_not_expire_fresh_cache(self):
        self.s.check()
        self.next_release()
        self.wall.return_value = 10000 + 365 * 86400
        self.mono.return_value = 11
        status = self.s.check()
        self.fetch.assert_called_once()
        self.assertEqual(status["available"]["commit"], "c" * 40)
        self.assertEqual(status["checked_at"], 10000)

    def test_exact_ttl_boundary_refreshes_but_just_before_reuses(self):
        self.s.check()
        self.next_release()
        self.mono.return_value = 10 + control.TTL - 0.001
        self.assertEqual(self.s.check()["available"]["commit"], "c" * 40)
        self.fetch.assert_called_once()
        self.mono.return_value = 10 + control.TTL
        self.assertEqual(self.s.check()["available"]["commit"], "d" * 40)
        self.assertEqual(self.fetch.call_count, 2)

    def test_epoch_zero_is_a_valid_checked_date_not_never_checked(self):
        self.assertIsNone(self.s.status()["checked_at"])
        self.wall.return_value = 0
        self.assertEqual(self.s.check()["checked_at"], 0)
        self.mono.return_value = 20
        self.assertEqual(self.s.check()["checked_at"], 0)
        self.fetch.assert_called_once()

    def test_zero_monotonic_origin_can_be_cached(self):
        self.mono.return_value = 0
        self.s.check()
        self.next_release()
        self.mono.return_value = 1
        self.assertEqual(self.s.check()["available"]["commit"], "c" * 40)
        self.fetch.assert_called_once()

    def test_force_always_refreshes_even_when_clocks_do_not_advance(self):
        self.s.check()
        self.next_release()
        self.assertEqual(self.s.check(force=True)["available"]["commit"], "d" * 40)
        self.assertEqual(self.fetch.call_count, 2)

    def test_failed_expired_refresh_does_not_extend_cache_or_change_checked_date(self):
        for error in [OSError("offline"), ValueError("bad manifest")]:
            with self.subTest(error=type(error).__name__):
                self.fetch.side_effect = None
                self.s.check(force=True)
                old_checked_at = self.s.checked_at
                self.wall.return_value += 100
                self.mono.return_value += control.TTL
                self.fetch.side_effect = error
                failed = self.s.check()
                self.assertIsNotNone(failed["warning"])
                self.assertEqual(failed["checked_at"], old_checked_at)
                calls = self.fetch.call_count
                self.fetch.side_effect = None
                self.next_release()
                recovered = self.s.check()
                self.assertEqual(self.fetch.call_count, calls + 1)
                self.assertIsNone(recovered["warning"])
                self.assertEqual(recovered["available"]["commit"], "d" * 40)

    def test_failed_force_does_not_renew_previous_success_lifetime(self):
        self.s.check()
        self.mono.return_value = 110
        self.fetch.side_effect = OSError("offline")
        self.assertIsNotNone(self.s.check(force=True)["warning"])
        self.fetch.side_effect = None
        self.next_release()
        self.mono.return_value = 10 + control.TTL
        self.assertEqual(self.s.check()["available"]["commit"], "d" * 40)
        self.assertEqual(self.fetch.call_count, 3)

    def test_cache_lifetime_starts_after_fetch_completes(self):
        def fetch(*_):
            self.mono.return_value = 210
            self.wall.return_value = 10200
            return self.fixture.manifest

        self.fetch.side_effect = fetch
        self.s.check()
        self.fetch.side_effect = None
        self.next_release()
        self.mono.return_value = 210 + control.TTL - 0.001
        status = self.s.check()
        self.fetch.assert_called_once()
        self.assertEqual(status["checked_at"], 10200)
        self.mono.return_value = 210 + control.TTL
        self.assertEqual(self.s.check()["available"]["commit"], "d" * 40)

    def test_monotonic_reset_fails_closed_to_refresh(self):
        self.s.check()
        self.next_release()
        self.mono.return_value = 9
        self.assertEqual(self.s.check()["available"]["commit"], "d" * 40)
        self.assertEqual(self.fetch.call_count, 2)

    def test_unsupported_channel_discards_old_cache_and_checks_again(self):
        self.s.check()
        self.fetch.return_value = {**self.fixture.manifest, "version": "0.1.0"}
        status = self.s.check(force=True)
        self.assertIsNone(status["available"])
        self.assertIsNotNone(status["warning"])
        self.next_release()
        self.assertEqual(self.s.check()["available"]["commit"], "d" * 40)
        self.assertEqual(self.fetch.call_count, 3)

    def test_parallel_expired_checks_fetch_once_and_share_new_result(self):
        self.s.check()
        self.next_release()
        self.wall.return_value = 9000
        self.mono.return_value = 10 + control.TTL
        with ThreadPoolExecutor(max_workers=8) as pool:
            statuses = list(pool.map(lambda _: self.s.check(), range(40)))
        self.assertEqual(self.fetch.call_count, 2)
        self.assertTrue(all(s["available"]["commit"] == "d" * 40 for s in statuses))
        self.assertTrue(all(s["checked_at"] == 9000 for s in statuses))

    def test_update_admission_forces_check_and_rejects_changed_release(self):
        self.s.check()
        self.next_release()
        self.s.runner = Mock()
        with self.assertRaises(control.FirmwareError) as raised:
            self.s.start("update", str(uuid.uuid4()), "c" * 40)
        self.assertEqual(raised.exception.code, "release_changed")
        self.assertEqual(self.fetch.call_count, 2)
        self.fixture.idle.assert_not_called()
        self.s.runner.assert_not_called()

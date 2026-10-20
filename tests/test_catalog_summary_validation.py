from __future__ import annotations

import json
import sys
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest.mock import Mock, patch

import rp_ylx.recording.coordinator as coordinator_module
from rp_ylx.recording.catalog import SessionCatalog
from tests import test_capture_coordinator as fixtures


class CatalogSummaryValidationTest(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.store = SessionCatalog(self.root / "catalog.sqlite3")
        self.directory = self.root / "session"
        self.directory.mkdir()
        (self.directory / "manifest.json").write_text("{}")
        self.summary = {
            "session_id": "session",
            "started_at": "2026",
            "take_id": "take",
            "verification": None,
            "nested": {"value": [1]},
        }
        self.store.remember("volume", self.directory, "commit", self.summary, {})
        self.signature = self.store.signature(self.directory)
        self.validate = Mock(side_effect=self.validate_value)

    @staticmethod
    def validate_value(value):
        if value["take_id"] != "take":
            raise ValueError("invalid take")

    def cached(self, **kwargs):
        options = {"validate_summary": self.validate, "cache_validation": True}
        options.update(kwargs)
        return self.store.cached("volume", "session", self.signature, "commit", **options)

    def change_row(self, value):
        with self.store.connection() as db, db:
            db.execute("UPDATE sessions SET summary=?", (json.dumps(value),))

    def test_unchanged_row_is_validated_once_but_every_response_is_fresh(self):
        first = self.cached()
        first["nested"]["value"].append(2)
        first["verification"] = {"verdict": "usable"}
        for _ in range(5):
            self.assertEqual(self.cached(), self.summary)
        self.assertEqual(self.validate.call_count, 1)

    def test_external_row_changes_and_invalid_rows_are_not_hidden(self):
        self.cached()
        changed = {**self.summary, "nested": {"value": [2]}}
        self.change_row(changed)
        self.assertEqual(self.cached(), changed)
        self.assertEqual(self.validate.call_count, 2)
        self.change_row({**changed, "take_id": "invalid"})
        self.assertIsNone(self.cached())
        self.assertIsNone(self.cached())
        self.assertEqual(self.validate.call_count, 4)
        self.change_row(self.summary)
        self.assertEqual(self.cached(), self.summary)
        self.assertEqual(self.validate.call_count, 5)

    def test_validator_identity_and_build_changes_require_validation(self):
        self.cached()
        second = Mock(side_effect=self.validate_value)
        self.assertEqual(self.cached(validate_summary=second), self.summary)
        self.assertEqual(second.call_count, 1)
        with self.store.connection() as db, db:
            db.execute("UPDATE sessions SET validator='new-commit'")
        self.assertIsNone(self.cached())
        self.store.cached(
            "volume", "session", self.signature, "new-commit",
            validate_summary=second, cache_validation=True,
        )
        self.assertEqual(second.call_count, 2)

    def test_stateful_validators_remain_uncached_by_default(self):
        self.cached(cache_validation=False)
        self.validate.side_effect = ValueError("now unavailable")
        self.assertIsNone(self.cached(cache_validation=False))
        self.assertEqual(self.validate.call_count, 2)
        self.assertFalse(self.store._validated_summaries)
        self.validate.side_effect = self.validate_value
        self.cached(validate_summary=None)
        self.cached()
        self.assertEqual(self.validate.call_count, 3)

    def test_row_limit_evicts_least_recently_used_validation(self):
        with patch.object(self.store, "_VALIDATION_CACHE_ENTRIES", 3):
            for index in range(4):
                sid = f"session-{index}"
                with self.store.connection() as db, db:
                    db.execute(
                        "INSERT INTO sessions SELECT volume,?,signature,validator,"
                        "started_at,take_id,? FROM sessions WHERE session='session'",
                        (sid, json.dumps({**self.summary, "session_id": sid})),
                    )
                self.store.cached(
                    "volume", sid, self.signature, "commit",
                    validate_summary=self.validate, cache_validation=True,
                )
            self.assertEqual(len(self.store._validated_summaries), 3)
            self.assertNotIn(("volume", "session-0"), self.store._validated_summaries)
            self.store.cached(
                "volume", "session-0", self.signature, "commit",
                validate_summary=self.validate, cache_validation=True,
            )
            self.assertEqual(self.validate.call_count, 5)
            self.assertNotIn(("volume", "session-1"), self.store._validated_summaries)

    def test_large_unicode_rows_do_not_exceed_retained_byte_budget(self):
        with patch.object(self.store, "_VALIDATION_CACHE_BYTES", 400):
            self.cached()
            for size in (1000, 10000):
                self.change_row({**self.summary, "nested": {"value": ["界" * size]}})
                self.cached()
                self.cached()
                self.assertFalse(self.store._validated_summaries)
                self.assertEqual(self.store._validated_summary_bytes, 0)
        self.assertEqual(self.validate.call_count, 5)

    def test_combined_byte_budget_and_recently_read_entries_are_respected(self):
        raw = json.dumps(self.summary)
        size = sys.getsizeof(raw)
        with (
            patch.object(self.store, "_VALIDATION_CACHE_BYTES", 2 * size),
            patch.object(self.store, "_VALIDATION_CACHE_ENTRIES", 10),
        ):
            for volume in ("one", "two", "three"):
                with self.store.connection() as db, db:
                    db.execute(
                        "INSERT INTO sessions SELECT ?,session,signature,validator,"
                        "started_at,take_id,summary FROM sessions WHERE volume='volume'",
                        (volume,),
                    )

            def read(volume):
                return self.store.cached(
                    volume, "session", self.signature, "commit",
                    validate_summary=self.validate, cache_validation=True,
                )

            read("one")
            read("two")
            read("one")
            read("three")
            self.assertEqual(self.validate.call_count, 3)
            self.assertEqual(set(self.store._validated_summaries), {
                ("one", "session"), ("three", "session"),
            })
            self.assertEqual(self.store._validated_summary_bytes, 2 * size)

    def test_unexpected_validator_failure_is_not_cached_or_suppressed(self):
        for failure in (OSError("unavailable"), KeyboardInterrupt()):
            self.validate.side_effect = failure
            with self.subTest(failure=type(failure).__name__), self.assertRaises(type(failure)):
                self.cached()
            self.assertFalse(self.store._validated_summaries)
        self.validate.side_effect = self.validate_value
        self.cached()
        self.cached()
        self.assertEqual(self.validate.call_count, 3)

    def add_rows(self, volume, keys):
        with self.store.connection() as db, db:
            for key in keys:
                db.execute(
                    "INSERT INTO sessions SELECT ?,?,signature,validator,started_at,take_id,? "
                    "FROM sessions WHERE volume='volume' AND session='session'",
                    (volume, key, json.dumps({**self.summary, "session_id": key})),
                )

    def read_rows(self, volume, keys):
        for key in keys:
            self.assertIsNotNone(self.store.cached(
                volume, key, self.signature, "commit",
                validate_summary=self.validate, cache_validation=True,
            ))

    def test_full_scan_larger_than_entry_capacity_keeps_a_reusable_subset(self):
        keys = [f"row-{i}" for i in range(20)]
        self.add_rows("large", keys)
        with patch.object(self.store, "_VALIDATION_CACHE_ENTRIES", 3):
            for expected_calls in (20, 37, 54):
                with self.store.scan():
                    self.read_rows("large", keys)
                    self.assertLessEqual(len(self.store._scan_state.validations_seen), 3)
                self.assertEqual(self.validate.call_count, expected_calls)
                self.assertEqual(len(self.store._validated_summaries), 3)
                self.assertIsNone(self.store._scan_state.validations_seen)

    def test_full_scan_larger_than_byte_capacity_keeps_a_reusable_subset(self):
        keys = ["aaaaaaa", "bbbbbbb", "ccccccc"]
        self.add_rows("large", keys)
        budget = 2 * sys.getsizeof(json.dumps(self.summary))
        with patch.object(self.store, "_VALIDATION_CACHE_BYTES", budget):
            for expected_calls in (3, 4, 5):
                with self.store.scan():
                    self.read_rows("large", keys)
                self.assertEqual(self.validate.call_count, expected_calls)
                self.assertLessEqual(self.store._validated_summary_bytes, budget)
                self.assertEqual(len(self.store._validated_summaries), 2)

    def test_new_volume_can_replace_unused_rows_from_an_earlier_scan(self):
        keys = ["row-a", "row-b", "row-c"]
        for volume in ("old", "new"):
            self.add_rows(volume, keys)
        with patch.object(self.store, "_VALIDATION_CACHE_ENTRIES", 2):
            for volume, calls in (("old", 3), ("new", 6), ("new", 7)):
                with self.store.scan():
                    self.read_rows(volume, keys)
                self.assertEqual(self.validate.call_count, calls)
                self.assertTrue(all(key[0] == volume for key in self.store._validated_summaries))

    def test_row_used_earlier_in_scan_is_revalidated_when_its_content_changes(self):
        with patch.object(self.store, "_VALIDATION_CACHE_ENTRIES", 1), self.store.scan():
            self.cached()
            changed = {**self.summary, "nested": {"value": [2]}}
            with self.store.connection() as db:
                db.execute("UPDATE sessions SET summary=?", (json.dumps(changed),))
            self.assertEqual(self.cached(), changed)
            self.assertEqual(self.cached(), changed)
        self.assertEqual(self.validate.call_count, 2)
        self.assertEqual(self.cached(), changed)
        self.assertEqual(self.validate.call_count, 2)

    def test_failed_scan_does_not_leave_admission_protection_on_later_reads(self):
        with self.assertRaisesRegex(RuntimeError, "rollback"), self.store.scan():
            self.cached()
            raise RuntimeError("rollback")
        self.assertIsNone(self.store._scan_state.validations_seen)
        keys = ["row-a", "row-b", "row-c"]
        self.add_rows("direct", keys)
        with patch.object(self.store, "_VALIDATION_CACHE_ENTRIES", 2):
            self.read_rows("direct", keys)
            self.assertNotIn(("direct", "row-a"), self.store._validated_summaries)

    def test_scan_rollback_and_deletion_release_cached_validation(self):
        with self.assertRaisesRegex(RuntimeError, "rollback"), self.store.scan():
            self.cached()
            raise RuntimeError("rollback")
        self.assertFalse(self.store._validated_summaries)
        self.assertEqual(self.store._validated_summary_bytes, 0)
        self.cached()
        self.store.discard_missing("other-volume", set())
        self.assertTrue(self.store._validated_summaries)
        self.store.discard_missing("volume", set())
        self.assertFalse(self.store._validated_summaries)
        self.assertEqual(self.store._validated_summary_bytes, 0)
        self.assertIsNone(self.cached())
        self.assertEqual(self.validate.call_count, 2)

    def test_schema_cache_never_bypasses_filesystem_or_recovery_role_admission(self):
        self.cached()
        self.assertIsNone(self.cached(required_artifact_role="recovery"))
        (self.directory / "manifest.json").write_text("changed")
        self.assertIsNone(self.cached(directory=self.directory))
        self.assertFalse(self.store._admitted)

    def test_concurrent_reads_preserve_isolation_and_budget_accounting(self):
        self.cached()
        with ThreadPoolExecutor(max_workers=8) as pool:
            values = list(pool.map(lambda _: self.cached(), range(100)))
        self.assertEqual(self.validate.call_count, 1)
        self.assertTrue(all(value == self.summary for value in values))
        values[0]["nested"]["value"].append(2)
        self.assertEqual(values[1], self.summary)
        self.assertEqual(
            self.store._validated_summary_bytes,
            sum(sys.getsizeof(entry[1]) for entry in self.store._validated_summaries.values()),
        )

    def test_coordinator_uses_validation_cache_but_rechecks_manifest_identity(self):
        fixture = fixtures.CaptureCoordinatorTest()
        fixture.setUp()
        self.addCleanup(fixture.tearDown)
        original = fixture.coordinator()
        try:
            sid = fixture.seal_one(original, prefix="summary-cache")
        finally:
            original.close()
        reopened = fixture.coordinator()
        self.addCleanup(reopened.close)
        with (
            patch.object(
                coordinator_module, "validate_session_summary",
                wraps=coordinator_module.validate_session_summary,
            ) as validate,
            patch.object(
                reopened._catalog_db, "signature", wraps=reopened._catalog_db.signature,
            ) as signature,
        ):
            first = reopened.list_sessions(cursor=None, limit=50, take_id=None, api_version="v4")
            count = signature.call_count
            for _ in range(5):
                self.assertEqual(
                    reopened.list_sessions(cursor=None, limit=50, take_id=None, api_version="v4"),
                    first,
                )
            self.assertEqual(validate.call_count, 1)
            self.assertGreaterEqual(signature.call_count - count, 5)
            self.assertEqual(first["items"][0]["session_id"], sid)


if __name__ == "__main__":
    unittest.main()

from __future__ import annotations

import hashlib
import json
import sqlite3
import unittest
from copy import deepcopy
from unittest.mock import patch

from rp_ylx.api.events import validate_session_list
from rp_ylx.recording.device_session import inspect_device_session_directory
from tests import test_capture_coordinator as fixtures


class CatalogRecoveryTest(unittest.TestCase):
    def setUp(self):
        self.fixture = fixtures.CaptureCoordinatorTest()
        self.fixture.setUp()
        self.addCleanup(self.fixture.tearDown)

    def sealed_pair(self):
        first = self.fixture.coordinator()
        try:
            broken = self.fixture.seal_one(first, prefix="broken-cache")
            healthy = self.fixture.seal_one(first, prefix="healthy-cache")
            store = first._catalog_db
            with store.connection() as db:
                row = db.execute(
                    "SELECT summary FROM sessions WHERE session=?", (broken,)
                ).fetchone()
            summary = json.loads(row[0])
            summary["verification"] = None
            return broken, healthy, store, summary
        finally:
            first.close()

    def assert_rebuilt(self, broken, healthy, store, expected):
        directory = self.fixture.mountpoint / "recordings" / broken
        before = hashlib.sha256((directory / "manifest.json").read_bytes()).hexdigest()
        reopened = self.fixture.coordinator()
        try:
            with (
                patch(
                    "rp_ylx.recording.coordinator.inspect_device_session_directory",
                    wraps=inspect_device_session_directory,
                ) as inspect,
                patch(
                    "rp_ylx.recording.coordinator.validate_device_session_directory",
                    side_effect=AssertionError("cache repair must not hash media"),
                ),
            ):
                page = reopened.list_sessions(cursor=None, limit=50, take_id=None, api_version="v4")
                validate_session_list(page, limit=50, take_id=None, api_version="v4")
                self.assertEqual({row["session_id"] for row in page["items"]}, {broken, healthy})
                self.assertEqual(page["diagnostics"], [])
                self.assertEqual(
                    next(row for row in page["items"] if row["session_id"] == broken), expected
                )
                self.assertEqual([call.args[0] for call in inspect.call_args_list], [directory])
                reopened.list_sessions(cursor=None, limit=50, take_id=None, api_version="v4")
                self.assertEqual(inspect.call_count, 1)
            self.assertEqual(
                hashlib.sha256((directory / "manifest.json").read_bytes()).hexdigest(), before
            )
            with store.connection() as db:
                saved = json.loads(
                    db.execute(
                        "SELECT summary FROM sessions WHERE session=?", (broken,)
                    ).fetchone()[0]
                )
            self.assertEqual(saved, expected)
            self.assertTrue(store.marker.exists())
        finally:
            reopened.close()

    def test_malformed_summary_rebuilds_only_affected_entry(self):
        broken, healthy, store, expected = self.sealed_pair()
        for body in ("{", "null", "[]", '"cache"', "42", "[" * 2000 + "]" * 2000, b"\xff"):
            with self.subTest(body=repr(body[:30])):
                with store.connection() as db, db:
                    db.execute("UPDATE sessions SET summary=? WHERE session=?", (body, broken))
                self.assert_rebuilt(broken, healthy, store, expected)

    def test_invalid_summary_fields_are_rebuilt_from_manifest(self):
        broken, healthy, store, expected = self.sealed_pair()
        changes = (
            {"session_id": healthy},
            {"started_at": "bad-time"},
            {"take_id": []},
            {"total_bytes": -1},
            {"duration_seconds": float("nan")},
            {"duration_seconds": float("inf")},
            {"device": {"device_id": []}},
            {"extra": True},
        )
        for change in changes:
            with self.subTest(change=change):
                invalid = deepcopy(expected)
                invalid.update(change)
                with store.connection() as db, db:
                    db.execute(
                        "UPDATE sessions SET summary=? WHERE session=?",
                        (json.dumps(invalid), broken),
                    )
                self.assert_rebuilt(broken, healthy, store, expected)
        with store.connection() as db, db:
            db.execute("UPDATE sessions SET summary='{}' WHERE session=?", (broken,))
        self.assert_rebuilt(broken, healthy, store, expected)

    def test_invalid_indexed_paths_are_rebuilt_without_touching_recording(self):
        broken, healthy, store, expected = self.sealed_pair()
        for path in ("bad\x00path", b"blob-path", "../outside"):
            with self.subTest(path=path):
                with store.connection() as db, db:
                    original = db.execute(
                        "SELECT path FROM artifacts WHERE session=? LIMIT 1", (broken,)
                    ).fetchone()[0]
                    db.execute(
                        "UPDATE artifacts SET path=? WHERE session=? AND path=?",
                        (path, broken, original),
                    )
                self.assert_rebuilt(broken, healthy, store, expected)

    def test_database_io_failure_is_not_treated_as_rebuildable_json(self):
        self.sealed_pair()
        reopened = self.fixture.coordinator()
        try:
            with (
                patch.object(
                    reopened._catalog_db,
                    "cached",
                    side_effect=sqlite3.OperationalError("injected disk I/O"),
                ),
                self.assertRaisesRegex(sqlite3.OperationalError, "injected disk I/O"),
            ):
                reopened.list_sessions(cursor=None, limit=50, take_id=None, api_version="v4")
        finally:
            reopened.close()

    def test_bad_live_cache_forgets_admission_and_quarantines_damaged_source(self):
        broken, healthy, store, expected = self.sealed_pair()
        reopened = self.fixture.coordinator()
        try:
            reopened.list_sessions(cursor=None, limit=50, take_id=None, api_version="v4")
            self.assertTrue(any(key[1] == broken for key in reopened._catalog_db._admitted))
            with store.connection() as db, db:
                relative = db.execute(
                    "SELECT path FROM artifacts WHERE session=? AND path LIKE 'video/%' LIMIT 1",
                    (broken,),
                ).fetchone()[0]
                db.execute("UPDATE sessions SET summary='{' WHERE session=?", (broken,))
            path = self.fixture.mountpoint / "recordings" / broken / relative
            original = path.read_bytes()  # Tiny generated test fixture, never device media.
            path.write_bytes(b"damaged")
            page = reopened.list_sessions(cursor=None, limit=50, take_id=None, api_version="v4")
            validate_session_list(page, limit=50, take_id=None, api_version="v4")
            self.assertEqual([row["session_id"] for row in page["items"]], [healthy])
            self.assertEqual(set(reopened._session_diagnostics), {broken})
            self.assertEqual(len(page["diagnostics"]), 1)
            self.assertEqual(page["diagnostics"][0]["code"], "manifest_invalid")
            self.assertFalse(any(key[1] == broken for key in reopened._catalog_db._admitted))
            path.write_bytes(original)
            repaired = reopened.list_sessions(cursor=None, limit=50, take_id=None, api_version="v4")
            validate_session_list(repaired, limit=50, take_id=None, api_version="v4")
            self.assertEqual(repaired["diagnostics"], [])
            self.assertEqual(
                next(row for row in repaired["items"] if row["session_id"] == broken), expected
            )
        finally:
            reopened.close()

    def test_bad_cache_cannot_publish_an_unrecovered_failed_recording(self):
        coordinator, session_id = self.fixture._retained_catalog_entry(recovered=False)
        try:
            with coordinator._catalog_db.connection() as db, db:
                changed = db.execute(
                    "UPDATE sessions SET summary='[]' WHERE session=?", (session_id,)
                )
                self.assertEqual(changed.rowcount, 1)
            page = coordinator.list_sessions(cursor=None, limit=50, take_id=None, api_version="v4")
            self.assertEqual(page["items"], [])
            self.assertTrue(page["diagnostics"])
            self.assertNotIn(session_id, coordinator._verified)
        finally:
            coordinator.close()


if __name__ == "__main__":
    unittest.main()

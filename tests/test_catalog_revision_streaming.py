from __future__ import annotations

import copy
import hashlib
import json
import unittest
import uuid
from unittest.mock import patch

from rp_ylx.api import ProviderError
from rp_ylx.recording import coordinator as module
from tests import test_capture_coordinator as fixture_module


def legacy_revision(items, diagnostics):
    material = {
        "schema": "ylx.session-catalog-revision-input.v1",
        "entries": [
            *({"kind": "session", "value": item} for item in items),
            *({"kind": "diagnostic", "value": diagnostic} for diagnostic in diagnostics),
        ],
    }
    payload = json.dumps(
        material, ensure_ascii=False, separators=(",", ":"), sort_keys=True
    ).encode("utf-8")
    return "sha256:" + hashlib.sha256(module.SESSION_CATALOG_REVISION_DOMAIN + payload).hexdigest()


class CatalogRevisionStreamingTests(unittest.TestCase):
    def test_exact_legacy_revision_at_entry_boundaries(self):
        for count in (0, 1, 63, 64, 65, 127, 128, 129, 1000):
            for diagnostic_count in (0, 1, 63, 64, 65):
                with self.subTest(items=count, diagnostics=diagnostic_count):
                    items = [
                        {"session_id": str(index), "nested": {"z": None, "a": index}}
                        for index in range(count)
                    ]
                    diagnostics = [
                        {"quarantine_id": str(index), "code": "missing"}
                        for index in range(diagnostic_count)
                    ]
                    self.assertEqual(
                        module._session_catalog_revision(items, diagnostics),
                        legacy_revision(items, diagnostics),
                    )

    def test_unicode_escaping_and_number_serialization_are_unchanged(self):
        values = {
            "汉字": '\n\t"\\🎥',
            "line": "\u2028",
            "numbers": [0, -0.0, 1e-9, 1e30, 2**63 + 1],
            "nested": {"z": [True, False, None], "a": "视频"},
        }
        for items, diagnostics in (([values], []), ([], [values]), ([values] * 65, [values] * 65)):
            self.assertEqual(
                module._session_catalog_revision(items, diagnostics),
                legacy_revision(items, diagnostics),
            )

    def test_order_kind_and_nested_content_still_affect_revision(self):
        items = [{"id": 1, "nested": {"value": "old"}}, {"id": 2}]
        original = module._session_catalog_revision(items, [])
        self.assertNotEqual(original, module._session_catalog_revision(list(reversed(items)), []))
        self.assertNotEqual(original, module._session_catalog_revision([], items))
        items[0]["nested"]["value"] = "new"
        self.assertNotEqual(original, module._session_catalog_revision(items, []))

    def test_input_objects_are_not_mutated(self):
        items = [{"id": i, "nested": [None, {"value": i}]} for i in range(200)]
        diagnostics = [{"code": "missing", "details": {"number": i}} for i in range(100)]
        before = copy.deepcopy((items, diagnostics))
        module._session_catalog_revision(items, diagnostics)
        self.assertEqual((items, diagnostics), before)

    def test_unencodable_input_still_raises(self):
        items = [{"name": "valid"}] * 100 + [{"name": "\ud800"}]
        with self.assertRaises(UnicodeEncodeError):
            legacy_revision(items, [])
        with self.assertRaises(UnicodeEncodeError):
            module._session_catalog_revision(items, [])

    def test_cursor_from_legacy_revision_is_accepted_then_detects_change(self):
        fixture = fixture_module.CaptureCoordinatorTest()
        fixture.setUp()
        self.addCleanup(fixture.tearDown)
        coordinator = fixture.coordinator()
        self.addCleanup(coordinator.close)
        fixture.seal_one(coordinator, prefix="revision-compatibility")
        first = coordinator.list_sessions(cursor=None, limit=50, take_id=None, api_version="v4")
        template = first["items"][0]
        items = [
            dict(template, session_id=str(uuid.UUID(int=(7 << 76) + (2 << 62) + i)))
            for i in range(3)
        ]
        items.sort(key=lambda value: (value["started_at"], value["session_id"]), reverse=True)
        coordinator._session_summaries = {value["session_id"]: value for value in items}
        coordinator._session_diagnostics.clear()
        revision = legacy_revision(items, [])
        cursor = module._encode_session_cursor(catalog_revision=revision, take_id=None, offset=1)
        with patch.object(coordinator, "_catalog_sessions"):
            page = coordinator.list_sessions(cursor=cursor, limit=1, take_id=None, api_version="v4")
            self.assertEqual(page["catalog_revision"], revision)
            self.assertEqual(page["items"], [items[1]])
            items[0]["display_name"] = "changed name"
            with self.assertRaises(ProviderError) as error:
                coordinator.list_sessions(cursor=cursor, limit=1, take_id=None, api_version="v4")
            self.assertEqual(error.exception.code, "catalog_changed")


if __name__ == "__main__":
    unittest.main()

from __future__ import annotations

import unittest

from rp_ylx.web import WEB_ASSETS, read_asset


class EmbeddedWebResourcesTest(unittest.TestCase):
    def test_all_embedded_web_assets_are_readable(self) -> None:
        self.assertEqual(
            WEB_ASSETS,
            (
                "index.html",
                "styles.css",
                "app.js",
                "api-client.js",
                "state.js",
                "event-stream.js",
                "preview.js",
            ),
        )
        for name in WEB_ASSETS:
            with self.subTest(name=name):
                payload = read_asset(name)
                self.assertTrue(payload)

    def test_unknown_or_nested_asset_is_rejected(self) -> None:
        for name in ("missing.js", "../index.html", "/index.html"):
            with self.subTest(name=name), self.assertRaises(ValueError):
                read_asset(name)


if __name__ == "__main__":
    unittest.main()

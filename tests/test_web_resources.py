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

    def test_production_web_device_api_consumer_is_v4_only(self) -> None:
        forbidden_tokens = (
            b"/api/v3",
            b"ylx.capture-event.v3",
            b"ylx.device.v3",
            b'api_version: "3.0"',
        )
        for name in WEB_ASSETS:
            payload = read_asset(name)
            for token in forbidden_tokens:
                with self.subTest(name=name, token=token.decode()):
                    self.assertNotIn(token, payload)

        self.assertIn(b"/api/v4", read_asset("api-client.js"))
        self.assertIn(b"/api/v4/capture/events", read_asset("event-stream.js"))
        self.assertIn(b"ylx.capture-event.v4", read_asset("event-stream.js"))


if __name__ == "__main__":
    unittest.main()

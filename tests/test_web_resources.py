from __future__ import annotations

import hashlib
import json
import tempfile
import unittest
from collections.abc import Callable
from pathlib import Path
from unittest import mock

import rp_ylx.web as web_module
from rp_ylx.web import (
    ECHO_WEB_SOURCE_COMMIT,
    ECHO_WEB_SOURCE_REPOSITORY,
    ENTRY_ASSET,
    WEB_ASSETS,
    EchoWebArtifactError,
    asset_content_type,
    echo_web_release,
    echo_web_source,
    read_asset,
    web_assets,
)


class EmbeddedWebResourcesTest(unittest.TestCase):
    def tearDown(self) -> None:
        web_module._manifest.cache_clear()

    def test_manifest_declares_the_hosted_closed_set(self) -> None:
        self.assertEqual(sorted(WEB_ASSETS), ["app.js", "index.html", "styles.css"])
        self.assertIn(ENTRY_ASSET, WEB_ASSETS)

    def test_release_and_source_identity_are_pinned(self) -> None:
        self.assertEqual(echo_web_release(), ("openaria-echo-web", "0.1.0"))
        self.assertEqual(echo_web_source(), (ECHO_WEB_SOURCE_REPOSITORY, ECHO_WEB_SOURCE_COMMIT))
        self.assertEqual(ECHO_WEB_SOURCE_COMMIT, "cd3248bca296f40654d214eb9b602a474cd615ef")

    def test_every_asset_matches_its_declared_size_digest_and_content_type(self) -> None:
        for name, asset in web_assets().items():
            with self.subTest(name=name):
                payload = read_asset(name)
                self.assertTrue(payload)
                self.assertEqual(len(payload), asset.size)
                self.assertEqual(hashlib.sha256(payload).hexdigest(), asset.sha256)
                self.assertEqual(asset_content_type(name), asset.content_type)

    def test_content_types_come_from_the_manifest(self) -> None:
        self.assertEqual(asset_content_type("index.html"), "text/html; charset=utf-8")
        self.assertEqual(asset_content_type("app.js"), "text/javascript; charset=utf-8")
        self.assertEqual(asset_content_type("styles.css"), "text/css; charset=utf-8")

    def test_unknown_or_nested_asset_is_rejected(self) -> None:
        for name in ("missing.js", "../index.html", "/index.html", "assets.json"):
            with self.subTest(name=name), self.assertRaises(ValueError):
                read_asset(name)
            with self.subTest(name=name, call="content_type"), self.assertRaises(ValueError):
                asset_content_type(name)

    def test_tampered_asset_fails_closed(self) -> None:
        self._with_modified_artifacts({"app.js": b"tampered"}, self._assert_app_js_fails_closed)

    def test_tampering_after_a_successful_read_still_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for name in ("assets.json", "index.html", "app.js", "styles.css"):
                source = web_module.files(web_module.__package__).joinpath(name)
                (root / name).write_bytes(source.read_bytes())
            with mock.patch.object(web_module, "files", return_value=root):
                web_module._manifest.cache_clear()
                self.assertTrue(read_asset("app.js"))
                (root / "app.js").write_bytes(b"tampered-after-first-read")
                with self.assertRaises(EchoWebArtifactError):
                    read_asset("app.js")

    def test_missing_asset_fails_closed(self) -> None:
        def remove_app_js(root: Path) -> None:
            (root / "app.js").unlink()

        self._with_artifact_root(remove_app_js, self._assert_app_js_fails_closed)

    def test_bad_manifest_schema_fails_closed(self) -> None:
        def corrupt_schema(root: Path) -> None:
            manifest = json.loads((root / "assets.json").read_text(encoding="utf-8"))
            manifest["schema"] = "openaria.echo-web-artifacts.v999"
            (root / "assets.json").write_text(json.dumps(manifest), encoding="utf-8")

        self._with_artifact_root(corrupt_schema, self._assert_manifest_fails_closed)

    def _assert_app_js_fails_closed(self) -> None:
        with self.assertRaises(EchoWebArtifactError):
            read_asset("app.js")

    def _assert_manifest_fails_closed(self) -> None:
        with self.assertRaises(EchoWebArtifactError):
            web_assets()

    def _with_modified_artifacts(
        self, replacements: dict[str, bytes], assertion: Callable[[], None]
    ) -> None:
        def modify(root: Path) -> None:
            for name, payload in replacements.items():
                (root / name).write_bytes(payload)

        self._with_artifact_root(modify, assertion)

    def _with_artifact_root(
        self, modify: Callable[[Path], None], assertion: Callable[[], None]
    ) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for name in ("assets.json", "index.html", "app.js", "styles.css"):
                source = web_module.files(web_module.__package__).joinpath(name)
                (root / name).write_bytes(source.read_bytes())
            modify(root)
            with mock.patch.object(web_module, "files", return_value=root):
                web_module._manifest.cache_clear()
                assertion()
                web_module._manifest.cache_clear()

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

        app_js = read_asset("app.js")
        self.assertIn(b"/api/v4", app_js)
        self.assertIn(b"capture/events", app_js)
        self.assertIn(b"ylx.capture-event.v4", app_js)


if __name__ == "__main__":
    unittest.main()

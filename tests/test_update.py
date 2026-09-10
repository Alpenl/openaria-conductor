from __future__ import annotations

import ast
import copy
import io
import json
import os
import subprocess
import sys
import tarfile
import tempfile
import types
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

import tests.test_deployment as fixtures
from rp_ylx import update
from rp_ylx.network import saved_network_candidate
from rp_ylx.network_state import NetworkStateStore
from scripts import publish_rdk_x5_oss as publisher


class OnlineUpdateTest(unittest.TestCase):
    def setUp(self):
        self.fixture = fixtures.ReleaseManagerTest()
        self.fixture.setUp()
        self.addCleanup(self.fixture.tearDown)
        self.root = self.fixture.root
        self.bundle = self.fixture.bundle("a")
        self.config = {
            "public_base_url": "https://downloads.example.test/rdk-x5",
            "prefix": "rdk-x5",
        }
        self.output = self.root / "release"
        self.prepared = publisher.prepare(self.bundle, self.output, self.config)
        self.manifest = self.prepared["manifest"]

    def download(self, url, destination, limit):
        if url.endswith("latest.json"):
            source = self.output / "latest.json"
        elif url.endswith("update.py"):
            source = self.output / "update.py"
        else:
            source = self.output / "bundle.tar.gz"
        payload = source.read_bytes()
        self.assertLessEqual(len(payload), limit)
        destination.write_bytes(payload)

    def test_roundtrip_and_deterministic_archive(self):
        second = publisher.prepare(self.bundle, self.root / "second", self.config)
        self.assertEqual(self.prepared, second)
        self.assertEqual(update.validate_manifest(self.manifest), self.manifest)
        target = self.root / "extracted"
        target.mkdir()
        update.extract_bundle(self.output / "bundle.tar.gz", target)
        update.verify_bundle(target, self.manifest)
        self.assertEqual(
            {p.name: p.read_bytes() for p in target.iterdir()},
            {p.name: p.read_bytes() for p in self.bundle.iterdir()},
        )

    def test_manifest_rejects_unsafe_urls_sizes_and_identity(self):
        for field, value in (
            ("bytes", True),
            ("bytes", 0),
            ("bytes", update.MAX_ARCHIVE + 1),
            ("sha256", "../" + "a" * 61),
            ("url", "http://example.test/file"),
            ("url", "file:///etc/passwd"),
            ("url", "https://user:secret@example.test/file"),
        ):
            with self.subTest(field=field, value=value):
                manifest = copy.deepcopy(self.manifest)
                manifest["bundle"][field] = value
                with self.assertRaises(ValueError):
                    update.validate_manifest(manifest)
        for field in ("schema", "commit", "platform"):
            manifest = {**self.manifest, field: "wrong"}
            with self.assertRaises(ValueError):
                update.validate_manifest(manifest)

    def test_https_redirect_rejects_downgrade(self):
        with self.assertRaises(ValueError):
            update.HTTPSRedirect().redirect_request(None, None, 302, "", {}, "http://example.test")

    def test_tar_rejects_traversal_links_devices_and_duplicates(self):
        for name, kind, repeated in (
            ("../escape", tarfile.REGTYPE, False),
            ("/absolute", tarfile.REGTYPE, False),
            ("dir/child", tarfile.REGTYPE, False),
            ("symlink", tarfile.SYMTYPE, False),
            ("hardlink", tarfile.LNKTYPE, False),
            ("pipe", tarfile.FIFOTYPE, False),
            ("duplicate", tarfile.REGTYPE, True),
        ):
            with self.subTest(name=name), tempfile.TemporaryDirectory(dir=self.root) as temp:
                root = Path(temp)
                archive = root / "bad.tar.gz"
                with tarfile.open(archive, "w:gz") as tar:
                    info = tarfile.TarInfo(name)
                    info.type = kind
                    info.linkname = "/etc/passwd"
                    tar.addfile(info)
                    if repeated:
                        tar.addfile(info)
                target = root / "out"
                target.mkdir()
                with self.assertRaises(ValueError):
                    update.extract_bundle(archive, target)

    def test_tampered_bundle_or_mismatched_commit_never_passes(self):
        with self.assertRaises(ValueError):
            update.verify_bundle(self.bundle, {**self.manifest, "commit": "b" * 40})
        (self.bundle / "rdk_x5_install.py").write_text("bad payload")
        with self.assertRaises(ValueError):
            update.verify_bundle(self.bundle, self.manifest)

    def test_cache_reuses_valid_and_replaces_corrupt_archive(self):
        cache = self.root / "cache"
        cache.mkdir()
        with patch.object(update, "download", side_effect=self.download) as download:
            target = update.fetch_bundle(self.manifest, cache)
            update.fetch_bundle(self.manifest, cache)
            self.assertEqual(download.call_count, 1)
            target.write_bytes(b"corrupt")
            update.fetch_bundle(self.manifest, cache)
            self.assertEqual(download.call_count, 2)
            self.assertEqual(update.sha256(target), self.manifest["bundle"]["sha256"])

    def test_failed_download_never_leaves_verified_cache(self):
        cache = self.root / "cache"
        cache.mkdir()
        with (
            patch.object(update, "download", side_effect=lambda u, p, n: p.write_bytes(b"bad")),
            self.assertRaises(ValueError),
        ):
            update.fetch_bundle(self.manifest, cache)
        self.assertEqual(list(cache.iterdir()), [])

    def test_cache_lock_excludes_other_processes_and_symlinks(self):
        cache = self.root / "cache"
        with update.cache_lock(cache), self.assertRaises(ValueError), update.cache_lock(cache):
            self.fail("concurrent lock allowed")
        link = self.root / "link"
        link.symlink_to(cache)
        with self.assertRaises(ValueError), update.cache_lock(link):
            self.fail("symlink cache allowed")

    def test_download_only_is_full_verification_without_installation(self):
        with (
            patch.object(update, "download", side_effect=self.download),
            patch.object(update, "CURRENT", self.root / "current"),
            patch.object(update.subprocess, "run") as run,
            patch.object(update, "require_target") as target,
            patch.object(update, "install_update_command") as install,
        ):
            result = update.main(["--download-only", "--cache-dir", str(self.root / "cache")])
            self.assertEqual(result, 0)
            run.assert_not_called()
            install.assert_not_called()
            target.assert_not_called()

    def test_check_does_not_download_firmware(self):
        with (
            patch.object(update, "download", side_effect=self.download) as download,
            patch.object(update, "CURRENT", self.root / "current"),
        ):
            self.assertEqual(update.main(["--check", "--cache-dir", str(self.root / "cache")]), 0)
            self.assertEqual(download.call_count, 1)

    def test_first_install_and_failed_activation(self):
        config = self.root / "config.json"
        config.write_text(
            json.dumps({"security": {"profile": "customer"}, "listen": {"port": 8080}})
        )
        for fail in (False, True):
            with (
                self.subTest(fail=fail),
                patch.object(update, "DEFAULT_CACHE", self.root / "install-cache"),
                patch.object(update.Path, "home", return_value=self.root),
                patch.object(update, "require_target"),
                patch.object(update, "require_idle"),
                patch.object(update, "prepare_dependencies"),
                patch.object(update, "configure_data_volume"),
                patch.object(update, "prepare_first_install_network"),
                patch.object(update, "CONFIG", config),
                patch.object(update, "download", side_effect=self.download),
                patch.object(update, "current_commit", side_effect=[None, "a" * 40]),
                patch.object(update, "install_update_command") as install,
                patch.object(update.subprocess, "run") as run,
            ):
                if fail:
                    run.side_effect = subprocess.CalledProcessError(2, "installer")
                result = update.main([])
                self.assertEqual(result, 2 if fail else 0)
                self.assertEqual(install.call_count, 0 if fail else 1)
                self.assertEqual(run.call_args.args[0][2], "install")
                self.assertTrue(run.call_args.kwargs["check"])

    def test_bootstrap_private_umask_does_not_restrict_installed_code(self):
        config = self.root / "config.json"
        config.write_text(
            json.dumps({"security": {"profile": "customer"}, "listen": {"port": 8080}})
        )
        installed = self.root / "installed-code"
        real_run = subprocess.run

        def installer_probe(argv, **kwargs):
            # Exercise the real child process boundary with the bootstrap's 077.
            # A fake payload avoids requiring root/systemd in this regression.
            return real_run(
                [
                    sys.executable,
                    "-c",
                    "from pathlib import Path; import sys; "
                    "p = Path(sys.argv[1]); p.mkdir(); (p / 'module.py').write_text('')",
                    str(installed),
                ],
                **kwargs,
            )

        previous_umask = os.umask(0o077)
        try:
            with (
                patch.object(update, "DEFAULT_CACHE", self.root / "private-cache"),
                patch.object(update.Path, "home", return_value=self.root),
                patch.object(update, "require_target"),
                patch.object(update, "require_idle"),
                patch.object(update, "prepare_dependencies"),
                patch.object(update, "configure_data_volume"),
                patch.object(update, "prepare_first_install_network"),
                patch.object(update, "CONFIG", config),
                patch.object(update, "download", side_effect=self.download),
                patch.object(update, "current_commit", side_effect=[None, "a" * 40]),
                patch.object(update, "install_update_command"),
                patch.object(update.subprocess, "run", side_effect=installer_probe),
            ):
                self.assertEqual(update.main([]), 0)
            self.assertEqual(installed.stat().st_mode & 0o777, 0o755)
            self.assertEqual((installed / "module.py").stat().st_mode & 0o777, 0o644)
            cache = self.root / ("private-cache" if os.geteuid() == 0 else ".cache/openaria")
            self.assertEqual(cache.stat().st_mode & 0o777, 0o700)
            self.assertEqual(os.umask(0o077), 0o077)
        finally:
            os.umask(previous_umask)

    def test_fresh_wifi_is_retained_without_activating_or_changing_original(self):
        state = self.root / "network-state"
        profiles = self.root / "profiles"
        profiles.mkdir()
        original = profiles / "factory.nmconnection"
        original.write_bytes(b"factory-profile-and-private-credentials")
        source_uuid = "ab012345-6789-4567-890a-bcdef0123456"
        fields = {
            "GENERAL.CON-UUID": source_uuid,
            "802-11-wireless.mode": "infrastructure",
            "802-11-wireless.ssid": "Existing Wi-Fi",
            "802-11-wireless-security.key-mgmt": "wpa-psk",
            "802-11-wireless-security.psk-flags": "0 (none)",
            "802-11-wireless-security.psk": "test-only-password",
            "IP4.GATEWAY": "192.168.1.1",
            "connection.stable-id": "",
        }

        def nmcli(argv, **kwargs):
            if "-g" in argv:
                return subprocess.CompletedProcess(
                    argv, 0, fields[argv[argv.index("-g") + 1]] + "\n"
                )
            if "clone" in argv:
                path = profiles / (argv[-1] + ".nmconnection")
                path.write_bytes(original.read_bytes())
                path.chmod(0o600)
            elif "modify" in argv:
                self.assertIn("connection.autoconnect", argv)
                self.assertIn("no", argv)
                self.assertEqual(argv[-1], source_uuid)
            else:
                self.fail(f"unexpected network mutation: {argv}")
            return subprocess.CompletedProcess(argv, 0, "")

        with (
            patch.object(update, "NETWORK_STATE", state),
            patch.object(update, "NETWORK_PROFILES", profiles),
            patch.object(update.subprocess, "run", side_effect=nmcli) as run,
            patch.dict(
                os.environ,
                {"RP_YLX_NETWORK_STATE_DIR": str(state), "RP_YLX_NM_PROFILE_DIR": str(profiles)},
            ),
        ):
            update.prepare_first_install_network()
            record = state / "lkg-wlan0.json"
            self.assertEqual(record.stat().st_mode & 0o777, 0o600)
            self.assertNotIn(fields["802-11-wireless-security.psk"], record.read_text())
            self.assertEqual(original.read_bytes(), b"factory-profile-and-private-credentials")
            # The actual controller must load this as a reconnectable Wi-Fi target.
            snapshot = NetworkStateStore(state).snapshot()
            self.assertEqual(snapshot["desired"]["mode"], "wifi-client")
            candidate = saved_network_candidate("wifi-client")
            self.assertEqual(candidate["config"]["ssid"], "Existing Wi-Fi")
            run.reset_mock()
            update.prepare_first_install_network()
            run.assert_not_called()

    def test_fresh_wifi_unsupported_or_missing_credentials_stop_before_clone(self):
        cases = [("key-mgmt", "wpa-eap"), ("psk-flags", "1 (agent-owned)"), ("psk", "")]
        for suffix, rejected in cases:
            with self.subTest(field=suffix):
                fields = {
                    "GENERAL.CON-UUID": "ab012345-6789-4567-890a-bcdef0123456",
                    "802-11-wireless.mode": "infrastructure",
                    "802-11-wireless.ssid": "Existing Wi-Fi",
                    "802-11-wireless-security.key-mgmt": "wpa-psk",
                    "802-11-wireless-security.psk-flags": "0",
                    "802-11-wireless-security.psk": "test-only-password",
                    "IP4.GATEWAY": "192.168.1.1",
                }
                fields["802-11-wireless-security." + suffix] = rejected

                def nmcli(argv, fields=fields, **kwargs):
                    self.assertIn("-g", argv, "must not clone or activate on invalid input")
                    return subprocess.CompletedProcess(argv, 0, fields[argv[argv.index("-g") + 1]])

                with (
                    patch.object(update, "NETWORK_STATE", self.root / "unused-state"),
                    patch.object(update.subprocess, "run", side_effect=nmcli),
                    self.assertRaises(ValueError),
                ):
                    update.prepare_first_install_network()

    def test_wired_first_install_without_wifi_does_not_create_wifi_state(self):
        state = self.root / "unused-state"
        with (
            patch.object(update, "NETWORK_STATE", state),
            patch.object(
                update.subprocess, "run", return_value=subprocess.CompletedProcess([], 0, "--\n")
            ) as run,
        ):
            update.prepare_first_install_network()
            self.assertEqual(run.call_count, 1)
            self.assertFalse(state.exists())

    def test_bootstrap_preserves_explicit_user_hotspot_choice(self):
        state = self.root / "network-choice"
        state.mkdir()
        path = state / "controller-state.json"
        value = {
            "schema": "ylx.network-controller-state.v1",
            "desired": {"mode": "hotspot"},
            "transaction": {"current": None, "latest": {"operation": "apply"}},
        }
        path.write_text(json.dumps(value))
        before = path.read_bytes()
        with (
            patch.object(update, "NETWORK_STATE", state),
            patch.object(update.subprocess, "run") as run,
        ):
            update.prepare_first_install_network()
        run.assert_not_called()
        self.assertEqual(path.read_bytes(), before)

    def test_installed_updater_is_readable_despite_private_bootstrap_umask(self):
        directory = self.root / "updater"
        launcher = self.root / "openaria-update"
        previous_umask = os.umask(0o077)
        try:
            with (
                patch.object(update, "UPDATER_DIRECTORY", directory),
                patch.object(update, "UPDATE_COMMAND", launcher),
            ):
                update.install_update_command(self.output / "update.py")
            self.assertEqual(directory.stat().st_mode & 0o777, 0o755)
            self.assertEqual((directory / "update.py").stat().st_mode & 0o777, 0o644)
            self.assertEqual(launcher.stat().st_mode & 0o777, 0o755)
        finally:
            os.umask(previous_umask)

    def test_busy_device_and_unknown_status_block_update(self):
        config = self.root / "config.json"
        config.write_text(json.dumps({"security": {"profile": "lab"}, "listen": {"port": 8080}}))
        with (
            patch.object(update, "current_commit", return_value="a" * 40),
            patch.object(update, "CONFIG", config),
        ):
            for state in ("recording", "finalizing", "unknown", "idle"):
                value = {"snapshot": {"device_state": state, "active_recording": None}}
                opener = Mock()
                opener.open.return_value = io.BytesIO(json.dumps(value).encode())
                with patch.object(update.urllib.request, "build_opener", return_value=opener):
                    if state == "idle":
                        update.require_idle()
                    else:
                        with self.assertRaises(ValueError):
                            update.require_idle()

    def test_volume_size_keeps_os_reserve(self):
        for available, expected in ((12, 4), (24, 16), (64, 51), (128, 80)):
            self.assertEqual(update.data_volume_size(available * 1024**3), expected)
        with self.assertRaises(ValueError):
            update.data_volume_size(11 * 1024**3)

    def test_bootstrap_shell_and_stock_python_syntax(self):
        subprocess.run(["sh", "-n", str(self.output / "install.sh")], check=True)
        ast.parse((self.output / "update.py").read_text(), feature_version=(3, 10))
        script = (self.output / "install.sh").read_text()
        self.assertIn(self.manifest["updater"]["sha256"], script)
        self.assertIn("sha256sum --check --status", script)

    def test_publication_switches_channel_only_after_anonymous_verification(self):
        objects = {}
        writes = []

        def put(key, filename, headers):
            objects[key] = Path(filename).read_bytes()
            writes.append((key, headers))

        def get(url, path, limit):
            key = "rdk-x5/" + url.removeprefix(self.config["public_base_url"] + "/")
            path.write_bytes(objects[key])

        bucket = Mock()
        bucket.put_object_from_file.side_effect = put
        fake_oss = types.SimpleNamespace(
            exceptions=types.SimpleNamespace(ObjectAlreadyExists=FileExistsError)
        )
        with (
            patch.dict("sys.modules", {"oss2": fake_oss}),
            patch.object(publisher, "download", side_effect=get),
        ):
            publisher.publish(self.output, self.prepared, self.config, bucket)
        self.assertEqual(writes[-1][0], "rdk-x5/latest.json")
        self.assertEqual(writes[-1][1]["Cache-Control"], "no-cache")
        self.assertEqual(writes[0][1]["x-oss-forbid-overwrite"], "true")
        writes.clear()
        with (
            patch.dict("sys.modules", {"oss2": fake_oss}),
            patch.object(publisher, "download", side_effect=OSError),
            self.assertRaises(OSError),
        ):
            publisher.publish(self.output, self.prepared, self.config, bucket)
        self.assertNotIn("rdk-x5/latest.json", [key for key, _ in writes])

    def test_publication_retries_transient_oss_error_but_stops_before_channel(self):
        class ServerError(Exception):
            status = 503

        bucket = Mock()
        bucket.put_object_from_file.side_effect = [ServerError(), None]
        fake_oss = types.SimpleNamespace(
            exceptions=types.SimpleNamespace(ObjectAlreadyExists=FileExistsError)
        )
        with (
            patch.dict("sys.modules", {"oss2": fake_oss}),
            patch.object(publisher.time, "sleep") as sleep,
            patch.object(publisher, "download", side_effect=OSError),
            self.assertRaises(OSError),
        ):
            publisher.publish(self.output, self.prepared, self.config, bucket)
        self.assertEqual(bucket.put_object_from_file.call_count, 2)
        sleep.assert_called_once_with(2)
        self.assertTrue(
            all(
                "latest.json" not in call.args[0]
                for call in bucket.put_object_from_file.call_args_list
            )
        )

    def test_generic_oss_file_exists_still_requires_anonymous_hash_check(self):
        class FileAlreadyExists(Exception):
            status = 409
            code = "FileAlreadyExists"

        bucket = Mock()
        bucket.put_object_from_file.side_effect = FileAlreadyExists()
        fake_oss = types.SimpleNamespace(
            exceptions=types.SimpleNamespace(ObjectAlreadyExists=FileExistsError)
        )
        with (
            patch.dict("sys.modules", {"oss2": fake_oss}),
            patch.object(publisher, "download", side_effect=OSError) as get,
            self.assertRaises(OSError),
        ):
            publisher.publish(self.output, self.prepared, self.config, bucket)
        self.assertEqual(bucket.put_object_from_file.call_count, 1)
        get.assert_called_once()


if __name__ == "__main__":
    unittest.main()

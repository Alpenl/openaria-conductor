from __future__ import annotations

import io
import json
import os
import subprocess
import sys
import tarfile
import tempfile
import unittest
import zipfile
from pathlib import Path
from unittest.mock import MagicMock, patch

from rp_ylx.deployment import (
    BUNDLE_SCHEMA,
    DEPLOYMENT_ASSETS,
    TARGET_PLATFORM,
    DeploymentError,
    ReleaseManager,
    _asset_bytes,
    _extract_runtime,
    _extract_wheel,
    _launcher,
    _sha256,
    _system_launcher,
    _write_atomic,
    load_bundle,
    normalize_runtime_archive,
    write_bundle_manifest,
)
from rp_ylx.network import MDNS_ASSET_NAME, MDNS_PORT, MDNS_SERVICE, _avahi_service
from scripts import rdk_x5_install
from scripts.rdk_x5_install import _prepare_runtime


class ReleaseManagerTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.install_root = self.root / "opt/rp-ylx"
        self.config_root = self.root / "etc/rp-ylx"
        self.state_root = self.root / "var/lib/rp-ylx"
        self.commands: list[tuple[str, ...]] = []

    def tearDown(self) -> None:
        self.temporary.cleanup()

    @staticmethod
    def commit(character: str) -> str:
        return character * 40

    @staticmethod
    def runtime_descriptor(root: Path) -> dict[str, object]:
        runtime = root / "cpython-3.11.13-aarch64.tar.gz"
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory) / "runtime/bin/python3"
            source.parent.mkdir(parents=True)
            source.write_bytes(b"managed python runtime\n")
            with tarfile.open(runtime, "w:gz") as archive:
                archive.add(source.parents[1], arcname="runtime")
        return {
            "file": runtime.name,
            "bytes": runtime.stat().st_size,
            "sha256": _sha256(runtime),
            "implementation": "cpython",
            "python_version": "3.11.13",
            "platform": "linux_aarch64",
            "executable": "runtime/bin/python3",
        }

    def bundle(
        self,
        character: str,
        *,
        platform_tag: str = "manylinux_2_28_aarch64",
        manifest_commit: str | None = None,
    ) -> Path:
        commit = self.commit(character)
        root = self.root / f"bundle-{character}-{platform_tag}"
        root.mkdir()
        name = f"rp_ylx-0.1.0-cp311-abi3-{platform_tag}.whl"
        wheel = root / name
        with zipfile.ZipFile(wheel, "w") as archive:
            archive.writestr(
                "rp_ylx/_build_info.py",
                f'__commit__ = "{commit}"\n',
            )
            archive.writestr(
                "rp_ylx-0.1.0.dist-info/METADATA",
                "Metadata-Version: 2.1\nName: rp-ylx\nVersion: 0.1.0\n",
            )
            elf = bytearray(64)
            elf[:6] = b"\x7fELF\x02\x01"
            elf[18:20] = (183).to_bytes(2, "little")
            archive.writestr("rp_ylx/_native.abi3.so", elf)
        installer = root / "rdk_x5_install.py"
        installer.write_text("# bootstrap\n", encoding="utf-8")
        manifest = {
            "schema": BUNDLE_SCHEMA,
            "platform": TARGET_PLATFORM,
            "commit": manifest_commit or commit,
            "version": "0.1.0",
            "application_wheel": name,
            "installer": {
                "file": installer.name,
                "bytes": installer.stat().st_size,
                "sha256": _sha256(installer),
            },
            "runtime": self.runtime_descriptor(root),
            "wheels": [{"file": name, "bytes": wheel.stat().st_size, "sha256": _sha256(wheel)}],
        }
        _write_atomic(root / "bundle.json", manifest)
        return root

    @staticmethod
    def stage_installer(bundle: object, stage: Path) -> None:
        (stage / "bin").mkdir()
        (stage / "bin/rp-ylx").write_text("installed", encoding="utf-8")
        _extract_runtime(bundle.root / str(bundle.runtime["file"]), stage)

    def manager(self) -> ReleaseManager:
        return ReleaseManager(
            install_root=self.install_root,
            config_root=self.config_root,
            state_root=self.state_root,
            system_root=self.root,
            target_machine="aarch64",
            target_model="D-Robotics RDK X5 V1.0",
            library_finder=lambda name: f"lib{name}.so",
            executable_finder=lambda name: f"/usr/bin/{name}",
            runner=lambda command: self.commands.append(tuple(command)),
            stage_installer=self.stage_installer,
            encoder_builder=lambda stage: None,
            health_checker=lambda: self.commands.append(("health-check",)),
        )

    def test_clean_and_repeated_install_preserve_identity_and_enable_boot(self) -> None:
        manager = self.manager()
        bundle = self.bundle("a")
        first = manager.install(bundle)
        self.assertEqual(first["current"], self.commit("a"))
        self.assertIsNone(first["previous"])
        config_path = self.config_root / "device.json"
        installed_config = json.loads(config_path.read_bytes())
        identity = installed_config["device"]
        self.assertEqual(installed_config["camera"]["data_plane"], "rust")
        self.assertEqual(
            installed_config["audio"],
            {
                "enabled": True,
                "device": "hw:0,0",
                "sample_rate_hz": 48000,
                "channels": 2,
                "sample_format": "S16_LE",
            },
        )
        self.assertEqual(installed_config["storage"]["mountpoint"], "/data")
        (self.state_root / "business-state.json").write_text("keep", encoding="utf-8")

        repeated = manager.install(bundle)
        self.assertEqual(repeated, first)
        self.assertEqual(json.loads(config_path.read_bytes())["device"], identity)
        self.assertEqual((self.state_root / "business-state.json").read_text(), "keep")
        self.assertIn(("systemctl", "enable", "--now", "rp-ylx-data-volume.service"), self.commands)
        self.assertIn(("systemctl", "enable", "--now", "rp-ylx.service"), self.commands)
        self.assertIn(("systemctl", "enable", "--now", "rp-ylx-wifi-watchdog.timer"), self.commands)
        self.assertNotIn(
            ("systemctl", "enable", "--now", "rp-ylx-network-control.socket"),
            self.commands,
        )
        self.assertIn(("health-check",), self.commands)
        self.assertTrue((self.root / "usr/lib/systemd/system/rp-ylx.service").is_file())
        self.assertTrue((self.root / "usr/lib/systemd/system/rp-ylx-data-volume.service").is_file())
        self.assertTrue(
            (self.root / "usr/lib/systemd/system/rp-ylx-network-control.service").is_file()
        )
        self.assertTrue(
            (self.root / "usr/lib/systemd/system/rp-ylx-network-control.socket").is_file()
        )
        data_volume = self.root / "usr/local/sbin/rp-ylx-data-volume"
        self.assertTrue(data_volume.stat().st_mode & 0o100)
        watchdog = self.root / "usr/local/sbin/rp-ylx-wifi-watchdog"
        self.assertTrue(watchdog.stat().st_mode & 0o100)
        self.assertTrue((self.root / "usr/lib/systemd/system/rp-ylx-wifi-watchdog.timer").is_file())
        self.assertTrue((self.root / "etc/modprobe.d/aic8800-rp-ylx.conf").is_file())
        self.assertTrue(
            (self.root / "etc/NetworkManager/conf.d/90-rp-ylx-wifi-powersave.conf").is_file()
        )
        bootstrap = self.root / "usr/local/lib/rp-ylx/deployment.py"
        self.assertIn("import argparse", bootstrap.read_text(encoding="utf-8"))

    def test_repeated_install_preserves_wifi_watchdog_device_configuration(self) -> None:
        manager = self.manager()
        bundle = self.bundle("a")
        manager.install(bundle, activate=False)
        configuration = self.root / "etc/default/rp-ylx-wifi-watchdog"
        configuration.write_text("WIFI_GATEWAY=192.0.2.1\n", encoding="utf-8")

        manager.install(bundle, activate=False)

        self.assertEqual(configuration.read_text(encoding="utf-8"), "WIFI_GATEWAY=192.0.2.1\n")

    def test_clean_install_advertises_mdns_without_a_network_apply(self) -> None:
        # #118：默认安装必须在受支持设备网络上可被按名发现。此前 avahi service 文件只在
        # network apply 事务中写入，因此从未改过网络模式的设备完全不广播。
        manager = self.manager()
        manager.install(self.bundle("a"))

        advertised = self.root / "etc/avahi/services/rp-ylx.service"
        self.assertTrue(advertised.is_file(), "干净安装后 avahi service 文件必须存在")
        self.assertEqual(advertised.stat().st_mode & 0o777, 0o644)

        document = advertised.read_text(encoding="utf-8")
        self.assertIn(f"<type>{MDNS_SERVICE}</type>", document)
        self.assertIn(f"<port>{MDNS_PORT}</port>", document)
        # %h 通配符让 avahi 自行解析主机与地址，因此设备换地址后无需重写该文件。
        self.assertIn('replace-wildcards="yes"', document)

    def test_installed_mdns_service_matches_the_network_apply_payload(self) -> None:
        # 安装器与 network apply 必须写同一份字节，否则端口或服务类型会出现两份真相。
        manager = self.manager()
        manager.install(self.bundle("a"))

        advertised = self.root / "etc/avahi/services/rp-ylx.service"
        self.assertEqual(advertised.read_bytes(), _avahi_service())
        self.assertEqual(advertised.read_bytes(), _asset_bytes(MDNS_ASSET_NAME))

    def test_bootstrap_root_carries_the_mdns_asset_for_standalone_deploys(self) -> None:
        # deployment.py 会被单独复制到 /usr/local/lib/rp-ylx 运行，其资产也必须一并落地。
        manager = self.manager()
        manager.install(self.bundle("a"))

        standalone = self.root / "usr/local/lib/rp-ylx" / MDNS_ASSET_NAME
        self.assertTrue(standalone.is_file())
        self.assertEqual(standalone.read_bytes(), _avahi_service())

    def test_every_system_asset_is_declared_for_packaging(self) -> None:
        # 新增资产若没同时进打包声明，wheel 里就没有它，安装到真机才会暴露。
        repository = Path(__file__).resolve().parents[1]
        pyproject = (repository / "pyproject.toml").read_text(encoding="utf-8")
        manifest = (repository / "MANIFEST.in").read_text(encoding="utf-8")
        for name in DEPLOYMENT_ASSETS:
            with self.subTest(asset=name):
                self.assertTrue((repository / "src/rp_ylx/deploy" / name).is_file())
                self.assertTrue(_asset_bytes(name))
                suffix = Path(name).suffix
                if suffix:
                    glob = f"*{suffix}"
                    self.assertIn(glob, pyproject, f"pyproject.toml 未声明 {glob}")
                    self.assertIn(glob, manifest, f"MANIFEST.in 未声明 {glob}")
                else:
                    self.assertIn(f'"{name}"', pyproject, f"pyproject.toml 未声明 {name}")
                    self.assertIn(name, manifest, f"MANIFEST.in 未声明 {name}")

    def test_launchers_use_only_release_managed_runtime(self) -> None:
        application = _launcher("rp_ylx").decode()
        deployment = _system_launcher().decode()
        self.assertIn("$RELEASE_ROOT/runtime/bin/python3", application)
        self.assertIn("/opt/rp-ylx/current/runtime/bin/python3", deployment)
        self.assertIn("/opt/rp-ylx/previous/runtime/bin/python3", deployment)
        self.assertNotIn("/usr/bin/python3", application)
        self.assertNotIn("/usr/bin/python3", deployment)

    def test_clean_install_on_system_python_310_uses_packaged_runtime(self) -> None:
        manager = self.manager()
        manager.python_version = (3, 10)
        manager.install(self.bundle("a"), activate=False)
        runtime = manager.releases / self.commit("a") / "runtime/bin/python3"
        self.assertEqual(runtime.read_bytes(), b"managed python runtime\n")
        self.assertTrue(runtime.stat().st_mode & 0o100)

    def test_system_python_bootstrap_safely_extracts_packaged_runtime(self) -> None:
        bundle = self.bundle("a")
        original_extractall = tarfile.TarFile.extractall

        def python_310_extractall(
            archive: tarfile.TarFile,
            path: str | Path = ".",
            members: object = None,
        ) -> None:
            original_extractall(archive, path=path, members=members)

        with patch.object(tarfile.TarFile, "extractall", python_310_extractall):
            runtime = _prepare_runtime(bundle)
        self.assertEqual(runtime, bundle / "runtime/bin/python3")
        self.assertEqual(runtime.read_bytes(), b"managed python runtime\n")
        self.assertTrue(runtime.stat().st_mode & 0o100)

    def test_bootstrap_execs_managed_runtime_even_from_system_python_311(self) -> None:
        bundle = self.bundle("a")
        installer = bundle / "rdk_x5_install.py"
        runtime = bundle / "runtime/bin/python3"
        with (
            patch.object(rdk_x5_install, "__file__", str(installer)),
            patch.object(rdk_x5_install.sys, "executable", "/usr/bin/python3.11"),
            patch.object(rdk_x5_install.sys, "version_info", (3, 11)),
            patch.object(rdk_x5_install.sys, "argv", [str(installer), "status"]),
            patch.object(rdk_x5_install, "_prepare_runtime", return_value=runtime) as prepare,
            patch.object(rdk_x5_install.os, "execv", side_effect=SystemExit) as execute,
            self.assertRaises(SystemExit),
        ):
            rdk_x5_install.main()
        prepare.assert_called_once_with(bundle)
        execute.assert_called_once_with(
            str(runtime), [str(runtime), str(installer.resolve()), "status"]
        )

    def test_system_python_bootstrap_rejects_tampered_and_unsafe_runtime(self) -> None:
        tampered = self.bundle("a")
        archive_path = tampered / "cpython-3.11.13-aarch64.tar.gz"
        archive_path.write_bytes(archive_path.read_bytes() + b"tamper")
        with self.assertRaisesRegex(RuntimeError, "digest check failed"):
            _prepare_runtime(tampered)

        for character, member_type, linkname in (
            ("b", tarfile.REGTYPE, ""),
            ("c", tarfile.SYMTYPE, "../../outside"),
            ("d", tarfile.LNKTYPE, "runtime/bin/python3"),
        ):
            with self.subTest(member_type=member_type):
                bundle = self.bundle(character)
                archive_path = bundle / "cpython-3.11.13-aarch64.tar.gz"
                member_name = (
                    "runtime/../escape" if member_type == tarfile.REGTYPE else "runtime/bin/python3"
                )
                with tarfile.open(archive_path, "w:gz") as archive:
                    member = tarfile.TarInfo(member_name)
                    member.type = member_type
                    member.linkname = linkname
                    if member_type == tarfile.REGTYPE:
                        member.size = len(b"escape")
                        archive.addfile(member, io.BytesIO(b"escape"))
                    else:
                        archive.addfile(member)
                manifest = json.loads((bundle / "bundle.json").read_bytes())
                manifest["runtime"]["bytes"] = archive_path.stat().st_size
                manifest["runtime"]["sha256"] = _sha256(archive_path)
                _write_atomic(bundle / "bundle.json", manifest)
                with self.assertRaisesRegex(RuntimeError, "unsafe package-owned runtime member"):
                    _prepare_runtime(bundle)

    def test_bundle_build_rejects_unsafe_or_incomplete_runtime_archive(self) -> None:
        bundle = self.bundle("a")
        runtime = bundle / "cpython-3.11.13-aarch64.tar.gz"
        (bundle / "bundle.json").unlink()

        with tarfile.open(runtime, "w:gz") as archive:
            member = tarfile.TarInfo("runtime/../escape")
            member.size = len(b"escape")
            archive.addfile(member, io.BytesIO(b"escape"))
        with self.assertRaises(DeploymentError) as unsafe:
            write_bundle_manifest(
                bundle,
                application_wheel=next(bundle.glob("*.whl")).name,
                version="0.1.0",
            )
        self.assertEqual(unsafe.exception.code, "runtime_unsafe")
        self.assertFalse((bundle / "bundle.json").exists())

        with tarfile.open(runtime, "w:gz") as archive:
            member = tarfile.TarInfo("runtime/lib/python3.11/os.py")
            member.size = len(b"pass\n")
            archive.addfile(member, io.BytesIO(b"pass\n"))
        with self.assertRaises(DeploymentError) as incomplete:
            write_bundle_manifest(
                bundle,
                application_wheel=next(bundle.glob("*.whl")).name,
                version="0.1.0",
            )
        self.assertEqual(incomplete.exception.code, "runtime_dependency_missing")
        self.assertFalse((bundle / "bundle.json").exists())

    def test_upstream_runtime_is_normalized_and_internal_symlinks_are_materialized(self) -> None:
        source = self.root / (
            "cpython-3.11.15+20260807-aarch64-unknown-linux-gnu-install_only_stripped.tar.gz"
        )
        output = self.root / "cpython-3.11.15-aarch64.tar.gz"
        with tarfile.open(source, "w:gz") as archive:
            executable = tarfile.TarInfo("python/bin/python3.11")
            executable.mode = 0o755
            executable.size = len(b"managed python runtime\n")
            archive.addfile(executable, io.BytesIO(b"managed python runtime\n"))
            link = tarfile.TarInfo("python/bin/python3")
            link.type = tarfile.SYMTYPE
            link.linkname = "python3.11"
            archive.addfile(link)

        normalize_runtime_archive(source, output)

        with tarfile.open(output, "r:gz") as archive:
            members = {member.name: member for member in archive.getmembers()}
            self.assertEqual(set(members), {"runtime/bin/python3.11", "runtime/bin/python3"})
            self.assertTrue(all(member.isfile() for member in members.values()))
            self.assertEqual(
                archive.extractfile("runtime/bin/python3").read(),
                b"managed python runtime\n",
            )

    def test_runtime_normalization_rejects_links_outside_the_archive_root(self) -> None:
        source = self.root / "unsafe-upstream.tar.gz"
        output = self.root / "normalized.tar.gz"
        with tarfile.open(source, "w:gz") as archive:
            link = tarfile.TarInfo("python/bin/python3")
            link.type = tarfile.SYMTYPE
            link.linkname = "../../../outside"
            archive.addfile(link)
        with self.assertRaises(DeploymentError) as rejected:
            normalize_runtime_archive(source, output)
        self.assertEqual(rejected.exception.code, "runtime_unsafe")
        self.assertFalse(output.exists())

    def test_failed_first_activation_is_not_reported_as_installed(self) -> None:
        manager = self.manager()

        def fail_health() -> None:
            raise DeploymentError("service_unhealthy", "gateway health probe failed")

        manager.health_checker = fail_health
        with self.assertRaises(DeploymentError) as rejected:
            manager.install(self.bundle("a"))
        self.assertEqual(rejected.exception.code, "service_unhealthy")
        self.assertIsNone(manager.status()["current"])
        self.assertEqual(manager.status()["previous"], self.commit("a"))
        diagnostic = json.loads((self.state_root / "activation-failure.json").read_bytes())
        self.assertEqual(diagnostic["code"], "service_unhealthy")
        self.assertEqual(manager.status()["activation_failure"], diagnostic)
        self.assertIn(("systemctl", "disable", "--now", "rp-ylx.service"), self.commands)

    def test_upgrade_restarts_and_health_failure_restores_old_release(self) -> None:
        manager = self.manager()
        manager.install(self.bundle("a"))
        self.commands.clear()
        manager.install(self.bundle("b"))
        self.assertIn(("systemctl", "restart", "rp-ylx.service"), self.commands)

        def fail_health() -> None:
            raise DeploymentError("service_unhealthy", "new release failed health check")

        manager.health_checker = fail_health
        with self.assertRaises(DeploymentError):
            manager.install(self.bundle("c"))
        self.assertEqual(manager.status()["current"], self.commit("b"))
        self.assertEqual(manager.status()["previous"], self.commit("c"))

    def test_health_wait_accepts_large_session_catalog_after_30_seconds(self) -> None:
        manager = self.manager()
        response = MagicMock()
        response.__enter__.return_value.status = 200
        with (
            patch("rp_ylx.deployment.time.monotonic", side_effect=[0.0, 31.0]),
            patch(
                "rp_ylx.deployment.subprocess.run",
                return_value=subprocess.CompletedProcess([], 0),
            ),
            patch("rp_ylx.deployment.urllib.request.urlopen", return_value=response),
        ):
            manager._wait_for_health()

    def test_repeated_install_health_failure_preserves_previous_release(self) -> None:
        manager = self.manager()
        manager.install(self.bundle("a"))
        bundle = self.bundle("b")
        manager.install(bundle)

        def fail_health() -> None:
            raise DeploymentError("service_unhealthy", "repeat failed health check")

        manager.health_checker = fail_health
        with self.assertRaises(DeploymentError):
            manager.install(bundle)
        self.assertEqual(manager.status()["current"], self.commit("b"))
        self.assertEqual(manager.status()["previous"], self.commit("a"))

    def test_rollback_health_failure_restores_the_release_it_replaced(self) -> None:
        manager = self.manager()
        manager.install(self.bundle("a"))
        manager.install(self.bundle("b"))

        def fail_health() -> None:
            raise DeploymentError("service_unhealthy", "rollback target failed health check")

        manager.health_checker = fail_health
        with self.assertRaises(DeploymentError):
            manager.rollback()
        self.assertEqual(manager.status()["current"], self.commit("b"))
        self.assertEqual(manager.status()["previous"], self.commit("a"))

    def test_upgrade_keeps_only_current_and_previous_then_rolls_back(self) -> None:
        manager = self.manager()
        manager.install(self.bundle("a"))
        manager.install(self.bundle("b"))
        upgraded = manager.install(self.bundle("c"))
        self.assertEqual(upgraded["current"], self.commit("c"))
        self.assertEqual(upgraded["previous"], self.commit("b"))
        self.assertFalse((manager.releases / self.commit("a")).exists())
        self.assertEqual(
            sorted(path.name for path in manager.releases.iterdir()),
            [self.commit("b"), self.commit("c")],
        )
        rolled_back = manager.rollback()
        self.assertEqual(rolled_back["current"], self.commit("b"))
        self.assertEqual(rolled_back["previous"], self.commit("c"))
        self.assertIn(("systemctl", "restart", "rp-ylx.service"), self.commands)

    def test_prepared_install_recovers_after_interruption(self) -> None:
        manager = self.manager()
        manager.install(self.bundle("a"))
        bundle = load_bundle(self.bundle("b"))
        manager._prepare_release(bundle)
        transaction = manager._transaction_document(
            "install", self.commit("a"), self.commit("b"), "prepared"
        )
        _write_atomic(manager.transaction, transaction)

        recovered = self.manager().recover()
        self.assertEqual(recovered["current"], self.commit("b"))
        self.assertEqual(recovered["previous"], self.commit("a"))
        self.assertFalse(recovered["transaction_pending"])

    def test_switched_install_recovers_before_previous_update(self) -> None:
        manager = self.manager()
        manager.install(self.bundle("a"))
        bundle = load_bundle(self.bundle("b"))
        manager._prepare_release(bundle)
        manager._switch("current", self.commit("b"))
        transaction = manager._transaction_document(
            "install", self.commit("a"), self.commit("b"), "switched"
        )
        _write_atomic(manager.transaction, transaction)

        recovered = self.manager().recover()
        self.assertEqual(recovered["current"], self.commit("b"))
        self.assertEqual(recovered["previous"], self.commit("a"))
        self.assertFalse(recovered["transaction_pending"])

    def test_incomplete_release_never_becomes_current(self) -> None:
        manager = self.manager()
        manager._ensure_layout()
        transaction = manager._transaction_document("install", None, self.commit("a"), "prepared")
        _write_atomic(manager.transaction, transaction)
        with self.assertRaises(DeploymentError) as rejected:
            manager.recover()
        self.assertEqual(rejected.exception.code, "release_incomplete")
        self.assertIsNone(manager.status()["current"])

    def test_bundle_rejects_digest_commit_and_non_aarch64_wheels(self) -> None:
        digest = self.bundle("a")
        wheel = next(digest.glob("*.whl"))
        wheel.write_bytes(wheel.read_bytes() + b"tamper")
        with self.assertRaises(DeploymentError) as mismatch:
            load_bundle(digest)
        self.assertEqual(mismatch.exception.code, "bundle_digest_mismatch")

        with self.assertRaises(DeploymentError) as commit:
            load_bundle(self.bundle("b", manifest_commit=self.commit("c")))
        self.assertEqual(commit.exception.code, "bundle_invalid")

        with self.assertRaises(DeploymentError) as platform_error:
            load_bundle(self.bundle("d", platform_tag="manylinux_2_17_x86_64"))
        self.assertEqual(platform_error.exception.code, "bundle_platform_mismatch")

        with self.assertRaises(DeploymentError) as musl_error:
            load_bundle(self.bundle("e", platform_tag="musllinux_1_2_aarch64"))
        self.assertEqual(musl_error.exception.code, "bundle_platform_mismatch")

    def test_uninstall_preserves_config_state_and_recording_mount(self) -> None:
        manager = self.manager()
        manager.install(self.bundle("a"))
        config = self.config_root / "device.json"
        business = self.state_root / "business-state.json"
        business.write_text("keep", encoding="utf-8")
        recording = self.root / "mnt/ylx-recording/session.bin"
        recording.parent.mkdir(parents=True)
        recording.write_bytes(b"keep")

        advertised = self.root / "etc/avahi/services/rp-ylx.service"
        self.assertTrue(advertised.is_file())

        result = manager.uninstall()
        self.assertFalse(result["installed"])
        self.assertFalse(self.install_root.exists())
        self.assertTrue(config.is_file())
        self.assertEqual(business.read_text(), "keep")
        self.assertEqual(recording.read_bytes(), b"keep")
        self.assertIn(
            ("systemctl", "disable", "--now", "rp-ylx-wifi-watchdog.timer"),
            self.commands,
        )
        for relative, _ in DEPLOYMENT_ASSETS.values():
            self.assertFalse((self.root / relative).exists())
        # 卸载后设备不得继续广播一个已不存在的采集服务。
        self.assertFalse(advertised.exists())

    def test_x86_install_fails_before_writing_layout(self) -> None:
        manager = ReleaseManager(
            install_root=self.install_root,
            config_root=self.config_root,
            state_root=self.state_root,
            system_root=self.root,
            target_machine="x86_64",
            runner=lambda command: None,
            stage_installer=self.stage_installer,
            encoder_builder=lambda stage: None,
        )
        with self.assertRaises(DeploymentError) as rejected:
            manager.install(self.bundle("a"))
        self.assertEqual(rejected.exception.code, "unsupported_platform")
        self.assertFalse(self.install_root.exists())

    def test_non_x5_aarch64_install_fails_before_writing_layout(self) -> None:
        manager = ReleaseManager(
            install_root=self.install_root,
            config_root=self.config_root,
            state_root=self.state_root,
            system_root=self.root,
            target_machine="aarch64",
            target_model="Raspberry Pi 5 Model B Rev 1.0",
            runner=lambda command: None,
            stage_installer=self.stage_installer,
            encoder_builder=lambda stage: None,
        )
        with self.assertRaises(DeploymentError) as rejected:
            manager.install(self.bundle("a"))
        self.assertEqual(rejected.exception.code, "unsupported_platform")
        self.assertFalse(self.install_root.exists())

    def test_missing_native_runtime_fails_before_writing_layout(self) -> None:
        manager = ReleaseManager(
            install_root=self.install_root,
            config_root=self.config_root,
            state_root=self.state_root,
            system_root=self.root,
            target_machine="aarch64",
            target_model="D-Robotics RDK X5 V1.0",
            library_finder=lambda name: None,
            executable_finder=lambda name: f"/usr/bin/{name}",
            runner=lambda command: None,
            stage_installer=self.stage_installer,
            encoder_builder=lambda stage: None,
        )
        with self.assertRaises(DeploymentError) as rejected:
            manager.install(self.bundle("a"))
        self.assertEqual(rejected.exception.code, "runtime_dependency_missing")
        self.assertFalse(self.install_root.exists())
        self.assertFalse(self.config_root.exists())
        self.assertFalse(self.state_root.exists())

    def test_wheel_data_directory_is_rejected_before_extraction(self) -> None:
        wheel = self.root / "unsafe-py3-none-any.whl"
        with zipfile.ZipFile(wheel, "w") as archive:
            archive.writestr("unsafe-1.0.data/scripts/command", "payload")
        target = self.root / "site-packages"
        target.mkdir()
        with self.assertRaises(DeploymentError) as rejected:
            _extract_wheel(wheel, target, max_bytes=1024)
        self.assertEqual(rejected.exception.code, "wheel_unsafe")
        self.assertEqual(list(target.iterdir()), [])

    def test_uninstall_remains_available_when_native_runtime_is_missing(self) -> None:
        manager = self.manager()
        manager.install(self.bundle("a"))
        manager.library_finder = lambda name: None
        result = manager.uninstall()
        self.assertFalse(result["installed"])
        self.assertFalse(self.install_root.exists())
        self.assertTrue(self.config_root.exists())
        self.assertTrue(self.state_root.exists())

    def test_recover_remains_available_when_native_runtime_is_missing(self) -> None:
        manager = self.manager()
        manager.install(self.bundle("a"))
        bundle = load_bundle(self.bundle("b"))
        manager._prepare_release(bundle)
        transaction = manager._transaction_document(
            "install", self.commit("a"), self.commit("b"), "prepared"
        )
        _write_atomic(manager.transaction, transaction)
        manager.library_finder = lambda name: None
        recovered = manager.recover()
        self.assertEqual(recovered["current"], self.commit("b"))
        self.assertEqual(recovered["previous"], self.commit("a"))

    def test_zip_wheel_bootstrap_can_install_without_source_tree(self) -> None:
        commit = self.commit("a")
        bundle = self.root / "zip-bootstrap-bundle"
        bundle.mkdir()
        wheel = bundle / "rp_ylx-0.1.0-cp311-abi3-manylinux_2_28_aarch64.whl"
        repository = Path(__file__).resolve().parents[1]
        with zipfile.ZipFile(wheel, "w") as archive:
            archive.writestr("rp_ylx/__init__.py", "from ._build_info import __commit__\n")
            archive.writestr("rp_ylx/deploy/__init__.py", "")
            archive.write(repository / "src/rp_ylx/deployment.py", "rp_ylx/deployment.py")
            archive.writestr("rp_ylx/_build_info.py", f'__commit__ = "{commit}"\n')
            for name in DEPLOYMENT_ASSETS:
                archive.write(repository / "src/rp_ylx/deploy" / name, f"rp_ylx/deploy/{name}")
            archive.writestr(
                "rp_ylx-0.1.0.dist-info/METADATA",
                "Metadata-Version: 2.1\nName: rp-ylx\nVersion: 0.1.0\n",
            )
            elf = bytearray(64)
            elf[:6] = b"\x7fELF\x02\x01"
            elf[18:20] = (183).to_bytes(2, "little")
            archive.writestr("rp_ylx/_native.abi3.so", elf)
        installer = bundle / "rdk_x5_install.py"
        installer.write_text("# bootstrap\n", encoding="utf-8")
        manifest = {
            "schema": BUNDLE_SCHEMA,
            "platform": TARGET_PLATFORM,
            "commit": commit,
            "version": "0.1.0",
            "application_wheel": wheel.name,
            "installer": {
                "file": installer.name,
                "bytes": installer.stat().st_size,
                "sha256": _sha256(installer),
            },
            "runtime": self.runtime_descriptor(bundle),
            "wheels": [
                {"file": wheel.name, "bytes": wheel.stat().st_size, "sha256": _sha256(wheel)}
            ],
        }
        _write_atomic(bundle / "bundle.json", manifest)
        isolated_root = self.root / "zip-installed-root"
        code = "\n".join(
            (
                "import json, sys",
                "from pathlib import Path",
                f"sys.path.insert(0, {str(wheel)!r})",
                "from rp_ylx.deployment import ReleaseManager",
                f"root = Path({str(isolated_root)!r})",
                "manager = ReleaseManager(",
                "    install_root=root / 'opt/rp-ylx',",
                "    config_root=root / 'etc/rp-ylx',",
                "    state_root=root / 'var/lib/rp-ylx',",
                "    system_root=root,",
                "    target_machine='aarch64',",
                "    target_model='D-Robotics RDK X5 V1.0',",
                "    library_finder=lambda name: f'lib{name}.so',",
                "    executable_finder=lambda name: f'/usr/bin/{name}',",
                "    encoder_builder=lambda stage: None,",
                "    runner=lambda command: None,",
                "    health_checker=lambda: None,",
                ")",
                f"print(json.dumps(manager.install(Path({str(bundle)!r}), activate=False)))",
            )
        )
        result = subprocess.run(
            [sys.executable, "-I", "-c", code],
            check=False,
            capture_output=True,
            text=True,
            timeout=30,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(json.loads(result.stdout)["current"], commit)
        bootstrap = isolated_root / "usr/local/lib/rp-ylx/deployment.py"
        source_module = repository / "src/rp_ylx/deployment.py"
        self.assertEqual(bootstrap.read_bytes(), source_module.read_bytes())
        self.assertTrue((isolated_root / "usr/lib/systemd/system/rp-ylx.service").is_file())
        self.assertTrue(
            (isolated_root / "usr/lib/systemd/system/rp-ylx-data-volume.service").is_file()
        )
        self.assertTrue(
            (isolated_root / "usr/lib/systemd/system/rp-ylx-network-control.socket").is_file()
        )
        self.assertTrue((isolated_root / "usr/local/sbin/rp-ylx-data-volume").is_file())
        self.assertTrue((isolated_root / "usr/local/sbin/rp-ylx-wifi-watchdog").is_file())

    def test_service_declares_boot_restart_and_privileged_recovery(self) -> None:
        unit = (Path(__file__).resolve().parents[1] / "src/rp_ylx/deploy/rp-ylx.service").read_text(
            encoding="utf-8"
        )
        self.assertIn("WantedBy=multi-user.target", unit)
        self.assertIn("Restart=on-failure", unit)
        self.assertIn("Wants=rp-ylx-data-volume.service network-online.target", unit)
        self.assertIn("After=rp-ylx-data-volume.service", unit)
        self.assertIn("ExecStartPre=+/usr/local/sbin/rp-ylx-deploy recover", unit)
        self.assertIn("ExecStart=/opt/rp-ylx/current/bin/rp-ylx serve", unit)
        self.assertIn("SupplementaryGroups=video vpu jpu vps misc", unit)
        self.assertNotIn("raspberry", unit.casefold())

    def test_data_volume_asset_uses_data_mount_and_reserved_backing_file(self) -> None:
        root = Path(__file__).resolve().parents[1] / "src/rp_ylx/deploy"
        script = root / "rp-ylx-data-volume"
        syntax = subprocess.run(
            ["bash", "-n", str(script)], check=False, capture_output=True, text=True
        )
        self.assertEqual(syntax.returncode, 0, syntax.stderr)
        content = script.read_text(encoding="utf-8")
        self.assertIn("/var/lib/rp-ylx-data-volume/data-volume.img", content)
        self.assertIn("/data", content)
        self.assertIn("mkfs.ext4 -F -E nodiscard", content)
        self.assertIn("mountpoint -q", content)
        service = (root / "rp-ylx-data-volume.service").read_text(encoding="utf-8")
        self.assertIn("ExecStart=/usr/local/sbin/rp-ylx-data-volume ensure", service)
        sysusers = (root / "rp-ylx.sysusers").read_text(encoding="utf-8")
        self.assertIn("m rp-ylx misc", sysusers)
        tmpfiles = (root / "rp-ylx.tmpfiles").read_text(encoding="utf-8")
        self.assertIn("d /var/lib/rp-ylx-data-volume 0755 root root -", tmpfiles)

    def test_wifi_watchdog_assets_are_syntactically_valid_and_safely_wired(self) -> None:
        root = Path(__file__).resolve().parents[1] / "src/rp_ylx/deploy"
        script = root / "rp-ylx-wifi-watchdog"
        syntax = subprocess.run(
            ["sh", "-n", str(script)], check=False, capture_output=True, text=True
        )
        self.assertEqual(syntax.returncode, 0, syntax.stderr)
        service = (root / "rp-ylx-wifi-watchdog.service").read_text(encoding="utf-8")
        timer = (root / "rp-ylx-wifi-watchdog.timer").read_text(encoding="utf-8")
        watchdog = script.read_text(encoding="utf-8")
        self.assertIn("ExecStart=/usr/local/sbin/rp-ylx-wifi-watchdog check", service)
        self.assertIn("OnUnitActiveSec=1min", timer)
        self.assertIn("TimeoutStartSec=5min", service)
        self.assertIn("RuntimeDirectoryPreserve=yes", service)
        self.assertIn('modprobe -r "$DRIVER_MODULE"', watchdog)
        self.assertIn('timeout "$NMCLI_TIMEOUT" nmcli connection up', watchdog)
        self.assertIn("automatic reboot is in cooldown", watchdog)
        self.assertIn("systemctl reboot", watchdog)

    def test_network_control_socket_is_staged_as_root_only_fail_closed_boundary(self) -> None:
        root = Path(__file__).resolve().parents[1] / "src/rp_ylx/deploy"
        socket = (root / "rp-ylx-network-control.socket").read_text(encoding="utf-8")
        service = (root / "rp-ylx-network-control.service").read_text(encoding="utf-8")

        self.assertIn("ListenStream=/run/rp-ylx/network-control.sock", socket)
        self.assertIn("SocketUser=rp-ylx", socket)
        self.assertIn("SocketGroup=rp-ylx", socket)
        self.assertIn("SocketMode=0600", socket)
        self.assertIn("DirectoryMode=0755", socket)
        self.assertIn("User=root", service)
        self.assertIn("StandardInput=socket", service)
        self.assertIn("StandardOutput=socket", service)
        self.assertIn(
            "ExecStart=/opt/rp-ylx/current/bin/rp-ylx network-control serve --stdio",
            service,
        )
        self.assertIn("ProtectSystem=strict", service)
        self.assertNotIn("ReadWritePaths=", service)
        self.assertNotIn("/etc/NetworkManager/system-connections", service)
        self.assertNotIn("/var/lib/rp-ylx/network", service)
        self.assertNotIn("rp-ylx serve", service)

    def test_wifi_watchdog_reboots_once_when_driver_reload_does_not_restore_gateway(self) -> None:
        root = Path(__file__).resolve().parents[1] / "src/rp_ylx/deploy"
        script = root / "rp-ylx-wifi-watchdog"
        fake_bin = self.root / "fake-bin"
        fake_bin.mkdir()
        calls = self.root / "systemctl-calls"
        for command in ("ip", "iw", "journalctl", "logger", "nmcli", "sleep"):
            fake = fake_bin / command
            fake.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
            fake.chmod(0o755)
        ping = fake_bin / "ping"
        ping.write_text("#!/bin/sh\nexit 1\n", encoding="utf-8")
        ping.chmod(0o755)
        modprobe = fake_bin / "modprobe"
        modprobe.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
        modprobe.chmod(0o755)
        systemctl = fake_bin / "systemctl"
        systemctl.write_text(
            f"#!/bin/sh\nprintf '%s\\n' \"$*\" >> {calls}\n",
            encoding="utf-8",
        )
        systemctl.chmod(0o755)
        sys_class_net = self.root / "sys/class/net"
        (sys_class_net / "wlan0").mkdir(parents=True)
        environment = os.environ | {
            "PATH": f"{fake_bin}:{os.environ['PATH']}",
            "STATE_DIRECTORY": str(self.root / "state"),
            "RUNTIME_DIRECTORY": str(self.root / "run"),
            "WIFI_SYS_CLASS_NET": str(sys_class_net),
            "WIFI_CONNECTION_UUID": "00000000-0000-0000-0000-000000000001",
            "WIFI_GATEWAY": "192.0.2.1",
            "WIFI_RELOAD_TIMEOUT_SECONDS": "2",
            "WIFI_REBOOT_ON_RECOVERY_FAILURE": "1",
            "WIFI_REBOOT_COOLDOWN_SECONDS": "21600",
        }

        first = subprocess.run(
            [str(script), "recover"],
            check=False,
            capture_output=True,
            text=True,
            env=environment,
        )
        second = subprocess.run(
            [str(script), "recover"],
            check=False,
            capture_output=True,
            text=True,
            env=environment,
        )
        last_reboot = self.root / "state/last-automatic-reboot-epoch"
        last_reboot.write_text("4102444800\n", encoding="utf-8")
        third = subprocess.run(
            [str(script), "recover"],
            check=False,
            capture_output=True,
            text=True,
            env=environment,
        )

        self.assertEqual(first.returncode, 1, first.stderr)
        self.assertEqual(second.returncode, 1, second.stderr)
        self.assertEqual(third.returncode, 1, third.stderr)
        self.assertEqual(calls.read_text(encoding="utf-8").splitlines(), ["reboot"])
        self.assertIn("automatic reboot is in cooldown", second.stdout)
        self.assertIn("automatic reboot is in cooldown", third.stdout)


if __name__ == "__main__":
    unittest.main()

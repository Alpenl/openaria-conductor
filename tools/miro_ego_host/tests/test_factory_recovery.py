"""Unit tests for MEB1/MER1 crypto, device profiles, native bundles, and FAT32 composition."""

from __future__ import annotations

import hashlib
import io
import json
import lzma
import os
import shlex
import shutil
import subprocess
import sys
import tarfile
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from miro_ego_host import disks
from miro_ego_host.factory_recovery import (
    device,
    partition_layout,
    prepare_image,
    provision,
    recover,
)
from miro_ego_host.tests.fixtures import (
    aarch64_elf_stub,
    factory_profile,
    make_native_bundle,
    vendor_mbr,
)


class FactoryRecoveryTests(unittest.TestCase):
    def test_meb1_board_profile_sealing_tamper_and_wrong_board_rejection(self) -> None:
        from cryptography.exceptions import InvalidTag

        profile = factory_profile(board="a" * 64, voice_volume=37)
        sealed = provision.seal_profile(profile)
        self.assertEqual(sealed[:4], b"MEB1")
        self.assertNotIn(profile["board"].encode(), sealed)
        self.assertNotIn(b"machine-id", sealed)
        self.assertEqual(provision.open_profile(sealed, "a" * 64), profile)

        # Wrong board or tampered ciphertext must fail authentication.
        for raw, board in (
            (sealed, "b" * 64),
            (sealed[:-1] + bytes([sealed[-1] ^ 1]), "a" * 64),
        ):
            with self.assertRaises(InvalidTag):
                provision.open_profile(raw, board)

        # Wrong magic or truncated blob rejected before decryption.
        with self.assertRaises(RuntimeError):
            provision.open_profile(b"BAD1" + sealed[4:], "a" * 64)
        with self.assertRaises(RuntimeError):
            provision.open_profile(b"MEB1short", "a" * 64)

    def test_mer_scrypt_aesgcm_encryption_and_tamper_rejection(self) -> None:
        from cryptography.exceptions import InvalidTag

        raw = json.dumps(factory_profile()).encode("utf-8")
        encrypted = recover.encrypt(raw, "strong-backup-password")
        self.assertEqual(encrypted[:4], b"MER1")
        self.assertNotIn(b"machine-id", encrypted)
        self.assertEqual(recover.decrypt(encrypted, "strong-backup-password"), raw)

        for payload, password in (
            (encrypted, "wrong-password"),
            (encrypted[:-1] + bytes([encrypted[-1] ^ 1]), "strong-backup-password"),
        ):
            with self.assertRaises(InvalidTag):
                recover.decrypt(payload, password)

        with self.assertRaises(RuntimeError):
            recover.decrypt(b"BAD1" + encrypted[4:], "strong-backup-password")

    def test_save_private_mer_no_overwrite_and_mode_600(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "board.mer"
            recover.save_private(path, b"encrypted-mer")
            if os.name == "posix":
                self.assertEqual(path.stat().st_mode & 0o777, 0o600)
            with self.assertRaises(FileExistsError):
                recover.save_private(path, b"replacement")
            self.assertEqual(path.read_bytes(), b"encrypted-mer")

    def test_device_allowlist_identity_and_idle_enforcement(self) -> None:
        for forbidden in (
            "data/recordings/a",
            "var/lib/miro-ego/catalog.bin",
            "etc/../etc/shadow",
            "/etc/netplan/01.yaml",
            "etc/netplan/sub/01.yaml",
            "etc/ssh/ssh_host_ed25519_key",
        ):
            self.assertFalse(device.allowed(forbidden))
            value = factory_profile()
            value["files"][forbidden] = next(iter(value["files"].values()))
            with self.assertRaises(RuntimeError):
                device.validate(value)

        # Valid allowlisted paths accepted.
        for ok_path in (
            "etc/miro-ego/device.json",
            "etc/miro-ego/settings.json",
            "etc/miro-ego/button.json",
            "etc/miro-ego/bearer.token",
            "etc/default/miro-ego-data-volume",
            "etc/NetworkManager/system-connections/lab.nmconnection",
            "var/lib/miro-ego/network-state.json",
            "var/lib/miro-ego-network/state.json",
        ):
            self.assertTrue(device.allowed(ok_path))

        # Voice bounds and enabled preservation checks.
        legacy_vol_only = factory_profile(voice_volume=50, voice_enabled=None)
        device.validate(legacy_vol_only)
        self.assertEqual(
            device.voice_settings_payload(legacy_vol_only),
            {"schema": "miro-ego.voice-settings.v1", "enabled": False, "volume": 50},
        )
        enabled_voice = factory_profile(voice_volume=75, voice_enabled=True)
        device.validate(enabled_voice)
        self.assertEqual(
            device.voice_settings_payload(enabled_voice),
            {"schema": "miro-ego.voice-settings.v1", "enabled": True, "volume": 75},
        )
        for bad_vol in (-1, 101, True, "50"):
            value = factory_profile()
            value["voice"]["volume"] = bad_vol
            with self.assertRaises(RuntimeError):
                device.validate(value)
        for bad_enabled in (1, "true", None):
            value = factory_profile()
            value["voice"]["enabled"] = bad_enabled
            with self.assertRaises(RuntimeError):
                device.validate(value)
        extra_key_voice = factory_profile()
        extra_key_voice["voice"]["unexpected"] = 1
        with self.assertRaises(RuntimeError):
            device.validate(extra_key_voice)

        # Idle checks.
        device.require_idle(None)
        device.require_idle({"phase": "idle", "active_session": None, "pending_safe_swap": None})
        for busy in (
            {"active_session": {"session_id": "abc"}},
            {"pending_safe_swap": {"state": "pending"}},
            {"phase": "recording"},
        ):
            with self.assertRaises(RuntimeError):
                device.require_idle(busy)

    def test_device_restore_refuses_other_board_nonfresh_and_symlinks(self) -> None:
        profile = factory_profile(board="a" * 64)
        with (
            patch.object(device, "hardware", return_value="b" * 64),
            patch.object(Path, "mkdir") as mkdir_mock,
        ):
            with self.assertRaises(RuntimeError):
                device.restore(profile)
            mkdir_mock.assert_not_called()

        with (
            patch.object(device, "hardware", return_value="a" * 64),
            patch.object(Path, "exists", return_value=True),
            patch.object(Path, "mkdir") as mkdir_mock,
        ):
            with self.assertRaises(RuntimeError):
                device.restore(profile)
            mkdir_mock.assert_not_called()

        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            (root / "etc/miro-ego").mkdir(parents=True)
            link_target = root / "etc/machine-id"
            used_mock_symlink = False
            try:
                link_target.symlink_to(root / "secret")
            except OSError as exc:
                if getattr(exc, "winerror", None) != 1314:
                    raise
                link_target.write_bytes(b"placeholder")
                used_mock_symlink = True

            def rooted(*parts: str) -> Path:
                return root / Path(*parts).relative_to("/")

            orig_is_symlink = Path.is_symlink

            def fake_is_symlink(self_path: Path) -> bool:
                if used_mock_symlink and self_path == link_target:
                    return True
                return orig_is_symlink(self_path)

            with (
                patch.object(device, "hardware", return_value="a" * 64),
                patch.object(device, "Path", side_effect=rooted),
                patch.object(Path, "is_symlink", fake_is_symlink),
                self.assertRaises(RuntimeError),
            ):
                device.restore(profile)
            self.assertFalse((root / "secret").exists())

        # Clean fresh restore writes files atomically with tight permissions.
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)

            def rooted(*parts: str) -> Path:
                return root / Path(*parts).relative_to("/")

            with (
                patch.object(device, "hardware", return_value="a" * 64),
                patch.object(device, "Path", side_effect=rooted),
            ):
                result = device.restore(profile)
            self.assertTrue(result["restored"])
            self.assertEqual((root / "etc/machine-id").read_bytes(), b"a" * 32)
            if os.name == "posix":
                self.assertEqual((root / "etc/miro-ego/device.json").stat().st_mode & 0o777, 0o640)
                self.assertEqual((root / "etc/machine-id").stat().st_mode & 0o777, 0o644)

    def test_device_permissions_ownership_and_live_capture_guard(self) -> None:
        profile = factory_profile(
            board="a" * 64,
            extra_files={
                "etc/miro-ego/settings.json": b'{"ok":true}\n',
                "etc/miro-ego/button.json": b'{"enabled":true}\n',
                "etc/miro-ego/bearer.token": b"secret-bearer-token\n",
                "etc/default/miro-ego-data-volume": (
                    b"MIRO_EGO_DATA_VOLUME_MODE=exfat\n"
                    b"MIRO_EGO_DATA_VOLUME_PARTUUID=db2e7205-03\n"
                    b"MIRO_EGO_DATA_VOLUME_UUID=ABCD-1234\n"
                ),
                "var/lib/miro-ego/voice.json": (
                    b'{"schema":"miro-ego.voice-settings.v1","enabled":true,"volume":80}\n'
                ),
                "var/lib/miro-ego-network/state.json": b'{"desired":{"mode":"hotspot"}}\n',
                "var/lib/miro-ego-network/rescue.json": b'{"psk":"secret"}\n',
            },
        )
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            chowned: dict[str, tuple[int, int]] = {}

            def rooted(*parts: str) -> Path:
                return root / Path(*parts).relative_to("/")

            def fake_chown(path: Path, uid: int, gid: int, **_kwargs: object) -> None:
                rel = Path(path).relative_to(root).as_posix()
                chowned[rel] = (uid, gid)

            with (
                patch.object(device, "Path", side_effect=rooted),
                patch.object(device.os, "chown", side_effect=fake_chown, create=True),
            ):
                device.write_files(device.validate(profile), service_ids=(997, 998))

            self.assertEqual(chowned["etc/miro-ego"], (0, 998))
            self.assertEqual(chowned["etc/miro-ego/device.json"], (0, 998))
            self.assertEqual(chowned["etc/miro-ego/settings.json"], (0, 998))
            self.assertEqual(chowned["etc/miro-ego/bearer.token"], (0, 998))
            self.assertEqual(chowned["etc/miro-ego/button.json"], (0, 0))
            self.assertEqual(chowned["var/lib/miro-ego"], (997, 998))
            self.assertEqual(chowned["var/lib/miro-ego/voice.json"], (997, 998))
            self.assertEqual(chowned["var/lib/miro-ego-network"], (0, 0))
            self.assertEqual(chowned["var/lib/miro-ego-network/state.json"], (0, 0))
            self.assertEqual(chowned["var/lib/miro-ego-network/rescue.json"], (0, 0))

            if os.name == "posix":
                self.assertEqual((root / "etc/miro-ego").stat().st_mode & 0o777, 0o750)
                self.assertEqual((root / "etc/miro-ego/device.json").stat().st_mode & 0o777, 0o640)
                self.assertEqual((root / "etc/miro-ego/bearer.token").stat().st_mode & 0o777, 0o640)
                self.assertEqual((root / "etc/miro-ego/button.json").stat().st_mode & 0o777, 0o644)
                self.assertEqual(
                    (root / "etc/default/miro-ego-data-volume").stat().st_mode & 0o777, 0o644
                )
                self.assertEqual((root / "var/lib/miro-ego").stat().st_mode & 0o777, 0o750)
                self.assertEqual(
                    (root / "var/lib/miro-ego/voice.json").stat().st_mode & 0o777, 0o640
                )
                self.assertEqual((root / "var/lib/miro-ego-network").stat().st_mode & 0o777, 0o750)
                self.assertEqual(
                    (root / "var/lib/miro-ego-network/state.json").stat().st_mode & 0o777, 0o600
                )

            # Live staging recording.json blocks backup even without legacy capture-state.json.
            staging = root / "data/.miro-ego/staging/0198c9a8.partial"
            staging.mkdir(parents=True)
            (staging / "recording.json").write_text('{"state":"recording"}\n', encoding="utf-8")
            with self.assertRaisesRegex(RuntimeError, "idle without active capture"):
                device._probe_live_capture_idle(
                    {"storage": {"mountpoint": str(root / "data")}, "listen": {"port": 9}}
                )

    def test_bundle_builder_includes_voice_cues_and_new_deploy_assets(self) -> None:
        import sys as _sys

        repo_root = Path(__file__).resolve().parents[3]
        if str(repo_root) not in _sys.path:
            _sys.path.insert(0, str(repo_root))
        from scripts.build_rdk_x5_bundle import REQUIRED_VOICE_ASSETS, build_bundle

        with tempfile.TemporaryDirectory() as folder:
            tmp = Path(folder)
            miro_bin = tmp / "miro-ego"
            enc_bin = tmp / "miro-ego-encoder"
            miro_bin.write_bytes(aarch64_elf_stub(payload=b"miro-ego"))
            enc_bin.write_bytes(aarch64_elf_stub(payload=b"encoder"))
            out_dir = tmp / "bundle"
            archive = tmp / "bundle.tar"

            build_bundle(
                miro_ego_bin=miro_bin,
                encoder_bin=enc_bin,
                output=out_dir,
                archive=archive,
                version="0.3.0",
                commit="2" * 40,
            )
            provision.require_native_bundle(archive)
            manifest = json.loads((out_dir / "bundle.json").read_text(encoding="utf-8"))
            asset_paths = {item["path"] for item in manifest["assets"]}
            for cue in REQUIRED_VOICE_ASSETS:
                self.assertIn(f"share/voice/{cue}", asset_paths)
                self.assertTrue((out_dir / "share/voice" / cue).is_file())
            for expected_deploy in (
                "deploy/miro-ego/90-miro-ego-leds.rules",
                "deploy/miro-ego/miro-ego-deploy.socket",
                "deploy/miro-ego/miro-ego-deploy.service",
            ):
                self.assertIn(expected_deploy, asset_paths)

    def test_native_bundle_validation_checks_aarch64_elf_and_exfat_support(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            good = make_native_bundle(root / "good.tar")
            provision.require_native_bundle(good)

            # Reject x86_64 binary (e_machine=62 instead of EM_AARCH64=183).
            x86_bundle = make_native_bundle(
                root / "x86.tar",
                miro_ego_bytes=aarch64_elf_stub(elf_machine=62),
            )
            with self.assertRaisesRegex(RuntimeError, "AArch64"):
                provision.require_native_bundle(x86_bundle)

            # Reject non-ELF binary.
            script_bundle = make_native_bundle(
                root / "script.tar",
                miro_ego_bytes=b"#!/bin/sh\necho fake\n" + b"x" * 64,
            )
            with self.assertRaisesRegex(RuntimeError, "ELF"):
                provision.require_native_bundle(script_bundle)

            # Reject missing bin/miro-ego-encoder.
            no_enc = make_native_bundle(root / "no-enc.tar", include_encoder=False)
            with self.assertRaisesRegex(RuntimeError, "miro-ego-encoder"):
                provision.require_native_bundle(no_enc)

            # Reject bad encoder architecture.
            bad_enc = make_native_bundle(
                root / "bad-enc.tar",
                encoder_bytes=aarch64_elf_stub(elf_machine=62),
            )
            with self.assertRaisesRegex(RuntimeError, "AArch64"):
                provision.require_native_bundle(bad_enc)

            # Reject missing bundle.json, wrong platform, wrong schema, or unknown fields.
            no_meta = make_native_bundle(root / "no-meta.tar", bundle_schema=None)
            with self.assertRaisesRegex(RuntimeError, "bundle.json"):
                provision.require_native_bundle(no_meta)

            wrong_platform = make_native_bundle(
                root / "wrong-platform.tar",
                bundle_platform="linux_x86_64",
            )
            with self.assertRaisesRegex(RuntimeError, "platform"):
                provision.require_native_bundle(wrong_platform)

            wrong_schema = make_native_bundle(
                root / "wrong-schema.tar",
                bundle_schema="miro-ego.rdk-x5-bundle.v0",
            )
            with self.assertRaisesRegex(RuntimeError, "schema"):
                provision.require_native_bundle(wrong_schema)

            unknown_field = make_native_bundle(
                root / "unknown-field.tar",
                bundle_extra_fields={"arch": "aarch64"},
            )
            with self.assertRaisesRegex(RuntimeError, "unknown field"):
                provision.require_native_bundle(unknown_field)

            # Reject legacy Python wheel in bundle.
            wheel_bundle = make_native_bundle(
                root / "wheel.tar",
                extra_files={"legacy-0.1.0-py3-none-any.whl": b"PK\x03\x04"},
            )
            with self.assertRaisesRegex(RuntimeError, "wheel"):
                provision.require_native_bundle(wheel_bundle)

            # Reject loop-volume fallback in data-volume script.
            loop_bundle = make_native_bundle(
                root / "loop.tar",
                helper_bytes=(
                    b"#!/bin/bash\nMIRO_EGO_DATA_VOLUME_PARTUUID=x\n"
                    b"MIRO_EGO_DATA_VOLUME_UUID=y\nexfat\nlosetup -f data-volume.img\n"
                ),
            )
            with self.assertRaisesRegex(RuntimeError, "loop-volume"):
                provision.require_native_bundle(loop_bundle)

            # Reject missing data-volume assets.
            missing_assets = make_native_bundle(root / "no-assets.tar", helper_bytes=None)
            with self.assertRaisesRegex(RuntimeError, "data-volume"):
                provision.require_native_bundle(missing_assets)

            # Reject tar symlink member.
            symlink_tar = root / "sym.tar"
            with tarfile.open(symlink_tar, "w") as tar:
                member = tarfile.TarInfo("bin/miro-ego")
                member.type = tarfile.SYMTYPE
                member.linkname = "/etc/shadow"
                tar.addfile(member)
            with self.assertRaisesRegex(RuntimeError, "Unsafe"):
                provision.require_native_bundle(symlink_tar)

    def test_real_fat32_composition_embeds_miro_ego_payload_and_three_partition_mbr(self) -> None:
        from pyfatfs.PyFat import PyFat
        from pyfatfs.PyFatFS import PyFatFS

        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            base, partition = root / "vendor.img", root / "fat"
            partition.touch()
            fat = PyFat()
            fat.mkfs(str(partition), PyFat.FAT_TYPE_FAT32, size=256 * 1024**2, label="CONFIG")
            fat.close()
            with PyFatFS(str(partition)) as fs:
                fs.writetext("/hobot_config.sh", "#!/bin/bash -e\nexit 0\n")
            with base.open("wb") as dst, partition.open("rb") as src:
                dst.write(vendor_mbr())
                dst.seek(8192 * 512)
                shutil.copyfileobj(src, dst)

            original_sha = disks.sha256_file(base)
            bundle = make_native_bundle(root / "bundle.tar")
            profile = factory_profile(board="a" * 64, voice_volume=55)
            output = root / "miro-ego-out.img"

            exfat_deb = root / partition_layout.EXFATPROGS_PACKAGE
            exfat_deb.write_bytes(b"exfatprogs-fixture")
            logrotate_deb = root / partition_layout.LOGROTATE_PACKAGE
            logrotate_deb.write_bytes(b"logrotate-fixture")

            with patch.object(
                partition_layout, "prepare_tools", side_effect=[exfat_deb, logrotate_deb]
            ):
                receipt = provision.compose(
                    base,
                    original_sha,
                    bundle,
                    disks.sha256_file(bundle),
                    profile,
                    output,
                    card_bytes=127_999_672_320,
                )

            self.assertEqual(disks.sha256_file(base), original_sha)
            self.assertEqual(disks.sha256_file(output), receipt["sha256"])
            partition_layout.check_fit(output, 127_999_672_320)

            with output.open("rb") as src:
                entries = partition_layout.partitions(src.read(512))
            self.assertEqual(entries[1], (0x83, 532480, 20 * 1024**3 // 512))
            self.assertEqual(entries[2][0], 0x07)

            with PyFatFS(str(output), offset=8192 * 512, read_only=True) as fs:
                self.assertIn(
                    "/boot/config/miro-ego/firstboot.py --register", fs.readtext("/hobot_config.sh")
                )
                manifest = json.loads(fs.readbytes("/miro-ego/manifest.json"))
                self.assertEqual(manifest["schema"], "miro-ego.autoinstall.v1")
                self.assertEqual(manifest["storage"]["label"], "MIRO-EGO")
                self.assertEqual(manifest["storage"], receipt["storage"])
                sealed = fs.readbytes("/miro-ego/profile.bin")
                self.assertEqual(provision.open_profile(sealed, "a" * 64), profile)
                self.assertEqual(fs.readbytes("/miro-ego/bundle.tar"), bundle.read_bytes())
                for script_name in (
                    "disks.py",
                    "firstboot.py",
                    "device.py",
                    "provision.py",
                    "partition_layout.py",
                    "storage_firstboot.py",
                ):
                    self.assertTrue(fs.exists("/miro-ego/" + script_name), script_name)
                self.assertEqual(
                    fs.readbytes("/miro-ego/" + exfat_deb.name), exfat_deb.read_bytes()
                )
                self.assertEqual(
                    fs.readbytes("/miro-ego/" + logrotate_deb.name), logrotate_deb.read_bytes()
                )

            # Never overwrite an existing composed output image.
            with (
                patch.object(
                    partition_layout, "prepare_tools", side_effect=[exfat_deb, logrotate_deb]
                ),
                self.assertRaises(FileExistsError),
            ):
                provision.compose(
                    base,
                    original_sha,
                    bundle,
                    disks.sha256_file(bundle),
                    profile,
                    output,
                    card_bytes=127_999_672_320,
                )

    def test_cli_parser_and_ssh_framing_and_prepare_image(self) -> None:
        parsed_auto = recover.parser().parse_args(
            [
                "auto-flash",
                "--image",
                "base.img",
                "--sha256",
                "a" * 64,
                "--bundle",
                "bundle.tar",
                "--bundle-sha256",
                "b" * 64,
                "--profile",
                "board.mer",
                "--output",
                "out.img",
                "--disk",
                "/dev/sdb",
            ]
        )
        self.assertFalse(parsed_auto.full_erase)

        parsed_compose = recover.parser().parse_args(
            [
                "compose",
                "--image",
                "base.img",
                "--sha256",
                "a" * 64,
                "--bundle",
                "bundle.tar",
                "--bundle-sha256",
                "b" * 64,
                "--profile",
                "board.mer",
                "--card-bytes",
                "64000000000",
                "--output",
                "out.img",
            ]
        )
        self.assertEqual(parsed_compose.card_bytes, 64_000_000_000)

        compile(recover.INSTALL, "install", "exec")
        compile(recover.ACTIVATE, "activate", "exec")

        # SSH framing test with root and sudo user.
        for user, consume_pw in (("root", False), ("MiroEgo", True)):
            pending = io.BytesIO()
            pending.channel = SimpleNamespace(shutdown_write=lambda: None)
            result: list[subprocess.CompletedProcess[bytes]] = []

            def exec_command(
                command: str,
                timeout: int,
                pending: io.BytesIO = pending,
                result: list[subprocess.CompletedProcess[bytes]] = result,
                consume_pw: bool = consume_pw,
            ):
                self.assertNotIn("secret-pw", command)

                def execute() -> subprocess.CompletedProcess[bytes]:
                    if not result:
                        data = pending.getvalue()
                        if consume_pw:
                            data = data.split(b"\n", 1)[1]
                        args = shlex.split(command)
                        args = [sys.executable, *args[args.index("python3") + 1 :]]
                        result.append(subprocess.run(args, input=data, capture_output=True))
                    return result[0]

                channel = SimpleNamespace(recv_exit_status=lambda: execute().returncode)
                return (
                    pending,
                    SimpleNamespace(read=lambda: execute().stdout, channel=channel),
                    SimpleNamespace(read=lambda: execute().stderr),
                )

            client = SimpleNamespace(exec_command=exec_command)
            out = recover.remote(
                client,
                "secret-pw",
                user,
                "import sys,json; print(json.dumps(json.load(sys.stdin)))",
                payload=b'{"ok":true}',
            )
            self.assertEqual(json.loads(out), {"ok": True})

        # prepare_image MD5 verification check.
        raw = b"rdk-os-fixture" * 1024
        compressed = lzma.compress(raw)
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            (root / prepare_image.NAME).write_bytes(compressed)
            with self.assertRaises(RuntimeError):
                prepare_image.prepare(root)
            with patch.object(prepare_image, "MD5", hashlib.md5(compressed).hexdigest()):
                prepare_image.prepare(root)
            manifest = json.loads((root / "image.json").read_bytes())
            self.assertEqual(manifest["sha256"], hashlib.sha256(raw).hexdigest())


if __name__ == "__main__":
    unittest.main()

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

        # Voice bounds and enabled preservation checks (default enabled=True matches miro-ego-hw).
        legacy_vol_only = factory_profile(voice_volume=50, voice_enabled=None)
        device.validate(legacy_vol_only)
        self.assertEqual(
            device.voice_settings_payload(legacy_vol_only),
            {"schema": "miro-ego.voice-settings.v1", "enabled": True, "volume": 50},
        )
        disabled_voice = factory_profile(voice_volume=50, voice_enabled=False)
        device.validate(disabled_voice)
        self.assertEqual(
            device.voice_settings_payload(disabled_voice),
            {"schema": "miro-ego.voice-settings.v1", "enabled": False, "volume": 50},
        )
        omitted_voice = {k: v for k, v in factory_profile().items() if k != "voice"}
        self.assertEqual(
            device.voice_settings_payload(omitted_voice),
            {"schema": "miro-ego.voice-settings.v1", "enabled": True, "volume": 80},
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

            summary = build_bundle(
                miro_ego_bin=miro_bin,
                encoder_bin=enc_bin,
                output=out_dir,
                archive=archive,
                version="0.3.0",
                commit="2" * 40,
            )
            self.assertEqual(summary["public_key_hex"], provision.default_release_public_key_hex())
            self.assertTrue((out_dir / "bundle.json.sig").is_file())
            sig_text = (out_dir / "bundle.json.sig").read_text(encoding="ascii")
            self.assertTrue(sig_text.endswith("\n"))
            self.assertEqual(len(sig_text.strip()), 128)
            self.assertNotIn(
                "bundle.json.sig", (out_dir / "SHA256SUMS").read_text(encoding="utf-8")
            )
            provision.require_native_bundle(archive)
            manifest = json.loads((out_dir / "bundle.json").read_text(encoding="utf-8"))
            asset_paths = {item["path"] for item in manifest["assets"]}
            self.assertNotIn("bundle.json.sig", asset_paths)
            for cue in REQUIRED_VOICE_ASSETS:
                self.assertIn(f"share/voice/{cue}", asset_paths)
                self.assertTrue((out_dir / "share/voice" / cue).is_file())
            for expected_deploy in (
                "deploy/miro-ego/90-miro-ego-leds.rules",
                "deploy/miro-ego/miro-ego-deploy.socket",
                "deploy/miro-ego/miro-ego-deploy.service",
            ):
                self.assertIn(expected_deploy, asset_paths)

            # Custom signing seed produces distinct public key verified by require_native_bundle.
            custom_seed_hex = "ab" * 32
            custom_dir = tmp / "custom-bundle"
            custom_archive = tmp / "custom-bundle.tar"
            custom_summary = build_bundle(
                miro_ego_bin=miro_bin,
                encoder_bin=enc_bin,
                output=custom_dir,
                archive=custom_archive,
                version="0.3.0",
                commit="3" * 40,
                signing_seed_hex=custom_seed_hex,
            )
            self.assertNotEqual(
                custom_summary["public_key_hex"], provision.default_release_public_key_hex()
            )
            with self.assertRaisesRegex(RuntimeError, "signature"):
                provision.require_native_bundle(custom_archive)
            provision.require_native_bundle(
                custom_archive, trusted_public_key_hex=custom_summary["public_key_hex"]
            )
            with patch.dict(
                os.environ,
                {"MIRO_EGO_RELEASE_PUBLIC_KEY_HEX": custom_summary["public_key_hex"]},
            ):
                provision.require_native_bundle(custom_archive)

            # Reject invalid voice cue stem in build_bundle.
            bad_voice_dir = tmp / "bad-voice"
            shutil.copytree(repo_root / "share" / "voice", bad_voice_dir)
            shutil.copy2(bad_voice_dir / "device_ready.wav", bad_voice_dir / "bad cue.wav")
            with self.assertRaisesRegex(ValueError, "stem"):
                build_bundle(
                    miro_ego_bin=miro_bin,
                    encoder_bin=enc_bin,
                    output=tmp / "bad-voice-bundle",
                    voice_dir=bad_voice_dir,
                    version="0.3.0",
                    commit="4" * 40,
                )

            # Reject mock-SDK encoder binary unless --allow-mock-encoder is explicitly enabled.
            mock_enc_bin = tmp / "miro-ego-encoder-mock"
            mock_enc_bin.write_bytes(aarch64_elf_stub(payload=b"mock-hobot-sdk: initialize\x00"))
            with self.assertRaisesRegex(ValueError, "--allow-mock-encoder"):
                build_bundle(
                    miro_ego_bin=miro_bin,
                    encoder_bin=mock_enc_bin,
                    output=tmp / "rejected-mock-bundle",
                    version="0.3.0",
                    commit="5" * 40,
                )
            mock_archive = tmp / "allowed-mock-bundle.tar"
            build_bundle(
                miro_ego_bin=miro_bin,
                encoder_bin=mock_enc_bin,
                output=tmp / "allowed-mock-bundle",
                archive=mock_archive,
                version="0.3.0",
                commit="5" * 40,
                allow_mock_encoder=True,
            )
            provision.require_native_bundle(mock_archive)

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

            # Reject unsigned bundle (missing bundle.json.sig).
            unsigned = make_native_bundle(root / "unsigned.tar", include_sig=False)
            with self.assertRaisesRegex(RuntimeError, "bundle.json.sig"):
                provision.require_native_bundle(unsigned)

            # Reject malformed signature text.
            malformed_sig = make_native_bundle(root / "malformed-sig.tar", sig_bytes=b"not-hex\n")
            with self.assertRaisesRegex(RuntimeError, "bundle.json.sig"):
                provision.require_native_bundle(malformed_sig)

            # Reject bundle signed with untrusted Ed25519 seed (even with valid SHA256SUMS).
            untrusted = make_native_bundle(root / "untrusted.tar", signing_seed=b"\x99" * 32)
            with self.assertRaisesRegex(RuntimeError, "signature"):
                provision.require_native_bundle(untrusted)

            # Reject tampered member bytes even when byte size matches bundle.json!
            corrupted_bin = aarch64_elf_stub(payload=b"miro-ego-biX")
            tampered_payload = make_native_bundle(
                root / "tampered-bin.tar",
                tamper_after_signing={"bin/miro-ego": corrupted_bin},
            )
            with self.assertRaisesRegex(RuntimeError, "Digest or byte-length mismatch"):
                provision.require_native_bundle(tampered_payload)

            # Reject undeclared extra file in tar archive.
            undeclared_tar = make_native_bundle(
                root / "undeclared.tar",
                extra_files={"deploy/miro-ego/rogue-script": b"#!/bin/sh\nid\n"},
            )
            with self.assertRaisesRegex(RuntimeError, "undeclared"):
                provision.require_native_bundle(undeclared_tar)

            # Reject invalid voice cue stem in bundle.json.
            bad_voice = make_native_bundle(
                root / "bad-voice.tar",
                voice_files={"bad cue.wav": b"RIFF$\x00\x00\x00WAVE"},
            )
            with self.assertRaisesRegex(RuntimeError, "Invalid voice asset"):
                provision.require_native_bundle(bad_voice)

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

    def test_customer_profile_backup_restore_permissions_and_https_live_probe(self) -> None:
        cust_profile = factory_profile(board="c" * 64, security_profile="customer", voice_volume=80)
        decoded = device.validate(cust_profile)
        self.assertIn("etc/miro-ego/tls.crt", decoded)
        self.assertIn("etc/miro-ego/tls.key", decoded)
        self.assertIn("etc/miro-ego/bearer.token", decoded)

        # Missing TLS key in customer profile must fail validation.
        missing_key = json.loads(json.dumps(cust_profile))
        del missing_key["files"]["etc/miro-ego/tls.key"]
        with self.assertRaisesRegex(RuntimeError, "missing required file"):
            device.validate(missing_key)

        # Restore customer profile and verify 0640 root:miro-ego on cert, key, and token.
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
                device.write_files(decoded, service_ids=(997, 998))

                self.assertEqual(chowned["etc/miro-ego/tls.crt"], (0, 998))
                self.assertEqual(chowned["etc/miro-ego/tls.key"], (0, 998))
                self.assertEqual(chowned["etc/miro-ego/bearer.token"], (0, 998))
                if os.name == "posix":
                    self.assertEqual((root / "etc/miro-ego/tls.crt").stat().st_mode & 0o777, 0o640)
                    self.assertEqual((root / "etc/miro-ego/tls.key").stat().st_mode & 0o777, 0o640)

                # Verify customer backup_locked + HTTPS Bearer live idle probe.
                seen_req: list[tuple[str, str | None, object]] = []

                class FakeResp:
                    status = 200

                    def __enter__(self):
                        return self

                    def __exit__(self, *_a: object) -> None:
                        return None

                    def read(self, _n: int = -1) -> bytes:
                        return b'{"snapshot":{"device_state":"idle"}}'

                def fake_urlopen(req: object, timeout: float = 1.0, context: object = None):
                    url = getattr(req, "full_url", "")
                    auth = req.get_header("Authorization")  # type: ignore[attr-defined]
                    seen_req.append((url, auth, context))
                    return FakeResp()

                with (
                    patch.object(device, "hardware", return_value="c" * 64),
                    patch.object(device.urllib.request, "urlopen", side_effect=fake_urlopen),
                ):
                    backed_up = device.backup_locked()

                self.assertEqual(len(seen_req), 1)
                self.assertTrue(seen_req[0][0].startswith("https://127.0.0.1:8080/api/v1/"))
                self.assertEqual(seen_req[0][1], "Bearer customer-secret-token")
                self.assertIsNotNone(seen_req[0][2])
                self.assertIn("etc/miro-ego/tls.crt", backed_up["files"])
                self.assertIn("etc/miro-ego/tls.key", backed_up["files"])

    def test_ssh_restore_uploads_offline_debs_and_cleans_up_factory_staging(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            tmp = Path(folder)
            bundle = make_native_bundle(tmp / "bundle.tar")
            bundle_sha = disks.sha256_file(bundle)
            prof_path = tmp / "device.mer"
            raw_prof = json.dumps(factory_profile()).encode("utf-8")
            prof_path.write_bytes(recover.encrypt(raw_prof, "strong-backup-password"))

            exfat_deb = tmp / partition_layout.EXFATPROGS_PACKAGE
            logrotate_deb = tmp / partition_layout.LOGROTATE_PACKAGE
            exfat_deb.write_bytes(b"exfat-deb")
            logrotate_deb.write_bytes(b"logrotate-deb")

            remote_root = tmp / "var_lib_miro_ego_factory" / "miro-ego-factory-1234"
            remote_root.mkdir(parents=True)
            (remote_root / "bundle.tar").write_bytes(b"staged-tar")
            (remote_root / "bundle").mkdir()
            (remote_root / "bundle" / "bin").write_bytes(b"staged-bin")
            shutil.copy2(Path(device.__file__).resolve(), remote_root / "device.py")
            voice_target = tmp / "voice.json"

            uploaded: list[str] = []
            remote_scripts: list[str] = []

            class FakeSftp:
                def __enter__(self):
                    return self

                def __exit__(self, *_a: object) -> None:
                    return None

                def mkdir(self, _p: str, mode: int = 0o700) -> None:
                    return None

                def put(self, local: str, remote_dest: str) -> None:
                    uploaded.append(Path(remote_dest).name)

            class FakeClient:
                def open_sftp(self) -> FakeSftp:
                    return FakeSftp()

                def close(self) -> None:
                    return None

            def fake_remote(
                _client: object,
                _pw: str,
                _user: str,
                script: str,
                r_args: tuple[str, ...] = (),
                _payload: bytes = b"",
            ) -> bytes:
                remote_scripts.append(script)
                if "bundle.tar" in script and "tarfile.open" in script:
                    return json.dumps({"root": str(remote_root)}).encode("utf-8")
                if "ensure_ssh_data_volume" in script:
                    self.assertIn("tools_dir=root", script)
                    return b'{"uuid":"ABCD-1234"}'
                if "systemd-run" in script:
                    return json.dumps(
                        {"unit": "miro-ego-factory-1234", "log": str(remote_root / "install.log")}
                    ).encode("utf-8")
                if "ActiveState,Result" in script:
                    return b"inactive\nsuccess\n"
                if "print('installed')" in script:
                    patched_script = script.replace(
                        "pathlib.Path('/var/lib/miro-ego/voice.json')",
                        f"pathlib.Path({str(voice_target)!r})",
                    ).replace(
                        "pathlib.Path('/usr/local/sbin/miro-ego-device-login')",
                        f"pathlib.Path({str(tmp / 'nonexistent-login')!r})",
                    )
                    with (
                        patch.object(subprocess, "run"),
                        patch.object(device, "apply_service_permissions"),
                    ):
                        ns = {"__name__": "__finalize__"}
                        with patch.object(sys, "argv", ["finalize", str(remote_root)]):
                            exec(patched_script, ns)
                    return b"installed\n"
                return b'{"restored":true}'

            args = recover.parser().parse_args(
                [
                    "restore",
                    "--host",
                    "192.0.2.10",
                    "--profile",
                    str(prof_path),
                    "--bundle",
                    str(bundle),
                    "--bundle-sha256",
                    bundle_sha,
                    "--tools-directory",
                    str(tmp),
                ]
            )
            with (
                patch.object(
                    partition_layout, "prepare_tools", side_effect=[exfat_deb, logrotate_deb]
                ) as prep_tools_mock,
                patch.object(recover.getpass, "getpass", return_value="strong-backup-password"),
                patch.object(recover, "connection", return_value=(FakeClient(), "pw")),
                patch.object(recover, "remote", side_effect=fake_remote),
                patch("builtins.input", return_value="NO"),
            ):
                recover.restore(args)

            self.assertEqual(prep_tools_mock.call_count, 2)
            self.assertIn(partition_layout.EXFATPROGS_PACKAGE, uploaded)
            self.assertIn(partition_layout.LOGROTATE_PACKAGE, uploaded)
            self.assertFalse((remote_root / "bundle.tar").exists())
            self.assertFalse((remote_root / "bundle").exists())
            self.assertFalse(remote_root.exists())
            self.assertTrue(voice_target.is_file())

    def test_release_version_strips_pgp_and_ssh_tag_signatures_and_deploy_units_hardened(
        self,
    ) -> None:
        repo_root = Path(__file__).resolve().parents[3]
        if str(repo_root) not in sys.path:
            sys.path.insert(0, str(repo_root))
        from scripts.release_version import strip_tag_signature

        pgp_tagged = (
            "Miro-Ego 0.3.0 release\n\nAll hardware checks passed.\n"
            "-----BEGIN PGP SIGNATURE-----\nVersion: GnuPG v2\nabc123==\n"
            "-----END PGP SIGNATURE-----\n"
        )
        ssh_tagged = (
            "Miro-Ego 0.3.0 release\n\nSigned with SSH key.\n"
            "-----BEGIN SSH SIGNATURE-----\nU1NIU0lH\n-----END SSH SIGNATURE-----\n"
        )
        self.assertEqual(
            strip_tag_signature(pgp_tagged),
            "Miro-Ego 0.3.0 release\n\nAll hardware checks passed.",
        )
        self.assertEqual(
            strip_tag_signature(ssh_tagged),
            "Miro-Ego 0.3.0 release\n\nSigned with SSH key.",
        )

        deploy_dir = repo_root / "deploy" / "miro-ego"
        deploy_svc = (deploy_dir / "miro-ego-deploy.service").read_text(encoding="utf-8")
        self.assertIn("Requires=miro-ego-deploy.socket", deploy_svc)
        self.assertIn("Wants=miro-ego-recover.service", deploy_svc)
        self.assertNotIn("Requires=miro-ego-recover.service", deploy_svc)

        main_svc = (deploy_dir / "miro-ego.service").read_text(encoding="utf-8")
        self.assertIn("TimeoutStopSec=60s", main_svc)
        self.assertIn("ReadWritePaths=/run/miro-ego", main_svc)
        self.assertIn("After=data.mount ", main_svc)
        self.assertNotIn("Requires=data.mount", main_svc)

        btn_svc = (deploy_dir / "miro-ego-button.service").read_text(encoding="utf-8")
        self.assertIn("TimeoutStopSec=30s", btn_svc)
        self.assertIn("CapabilityBoundingSet=CAP_DAC_READ_SEARCH", btn_svc)

        tmpfiles = (deploy_dir / "miro-ego.tmpfiles").read_text(encoding="utf-8")
        self.assertIn("d /run/miro-ego 0750 root miro-ego -", tmpfiles)

        recover_svc = (deploy_dir / "miro-ego-recover.service").read_text(encoding="utf-8")
        self.assertNotIn("ProtectSystem=strict", recover_svc)
        self.assertNotIn("ProtectHome=yes", recover_svc)
        self.assertNotIn("NoNewPrivileges=yes", recover_svc)
        self.assertNotIn("CapabilityBoundingSet=", recover_svc)
        self.assertIn("AF_NETLINK", recover_svc)

        net_svc = (deploy_dir / "miro-ego-network.service").read_text(encoding="utf-8")
        self.assertIn("ReadWritePaths=/run/miro-ego", net_svc)

        wd_svc = (deploy_dir / "miro-ego-wifi-watchdog.service").read_text(encoding="utf-8")
        self.assertIn("After=data.mount ", wd_svc)

        fstrim_conf = (deploy_dir / "miro-ego-idle-fstrim.conf").read_text(encoding="utf-8")
        self.assertIn("RuntimeMaxSec=10min", fstrim_conf)

    @unittest.skipUnless(
        os.name == "posix" and shutil.which("flock") is not None,
        "POSIX sh + flock required for live watchdog lock behavior test",
    )
    def test_wifi_watchdog_passive_check_does_not_contend_on_operation_or_maintenance_locks(
        self,
    ) -> None:
        import fcntl

        repo_root = Path(__file__).resolve().parents[3]
        watchdog_script = repo_root / "deploy" / "miro-ego" / "miro-ego-wifi-watchdog"
        with tempfile.TemporaryDirectory() as folder:
            tmp = Path(folder)
            bin_dir = tmp / "bin"
            bin_dir.mkdir()
            net_sys = tmp / "sys_net" / "wlan0"
            net_sys.mkdir(parents=True)
            net_lock = tmp / "network-operation.lock"
            maint_lock = tmp / "maintenance.lock"
            net_lock.write_bytes(b"")
            maint_lock.write_bytes(b"")

            mode_file = tmp / "mode.txt"
            mode_file.write_text("wifi-client\n", encoding="utf-8")
            ping_rc_file = tmp / "ping_rc.txt"
            ping_rc_file.write_text("0\n", encoding="utf-8")

            fake_miro = bin_dir / "miro-ego"
            fake_miro.write_text(
                f"#!/bin/sh\ncat {str(mode_file)!r}\n",
                encoding="utf-8",
            )
            fake_miro.chmod(0o755)

            for helper_name, body in (
                ("logger", "#!/bin/sh\nexit 0\n"),
                ("ping", f"#!/bin/sh\nexit $(cat {str(ping_rc_file)!r})\n"),
            ):
                p = bin_dir / helper_name
                p.write_text(body, encoding="utf-8")
                p.chmod(0o755)

            env = {
                **os.environ,
                "PATH": f"{bin_dir}:{os.environ.get('PATH', '')}",
                "WIFI_INTERFACE": "wlan0",
                "WIFI_GATEWAY": "192.0.2.1",
                "WIFI_FAILURE_THRESHOLD": "2",
                "STATE_DIRECTORY": str(tmp / "state"),
                "RUNTIME_DIRECTORY": str(tmp / "run"),
                "WIFI_SYS_CLASS_NET": str(tmp / "sys_net"),
                "MIRO_EGO_COMMAND": str(fake_miro),
                "MIRO_EGO_NETWORK_OPERATION_LOCK_PATH": str(net_lock),
                "MIRO_EGO_MAINTENANCE_LOCK": str(maint_lock),
            }

            # Hold BOTH network-operation.lock and maintenance.lock exclusively while running
            # passive check: passive probe must STILL run and record failure=1 without deferring!
            with net_lock.open("rb") as nl, maint_lock.open("rb") as ml:
                fcntl.flock(nl.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                fcntl.flock(ml.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                ping_rc_file.write_text("1\n", encoding="utf-8")
                proc1 = subprocess.run(
                    ["/bin/sh", str(watchdog_script), "check"],
                    env=env,
                    capture_output=True,
                    text=True,
                    check=True,
                )
                self.assertIn("gateway probe failed (1/2)", proc1.stdout)
                self.assertEqual(
                    (tmp / "run" / "failures").read_text(encoding="utf-8").strip(), "1"
                )

                # Reaching threshold (2/2) while locks are busy defers disruptive driver reload!
                proc2 = subprocess.run(
                    ["/bin/sh", str(watchdog_script), "check"],
                    env=env,
                    capture_output=True,
                    text=True,
                    check=True,
                )
                self.assertIn("deferring driver recovery", proc2.stdout)

    def test_compose_records_trusted_public_key_and_auto_flash_checks_output_disk(self) -> None:
        import types

        from cryptography.hazmat.primitives import serialization
        from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

        custom_seed = hashlib.sha256(b"custom-compose-seed-030").digest()
        custom_pub_hex = (
            Ed25519PrivateKey.from_private_bytes(custom_seed)
            .public_key()
            .public_bytes(serialization.Encoding.Raw, serialization.PublicFormat.Raw)
            .hex()
        )

        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            base_img = root / "vendor.img"
            with base_img.open("wb") as dst:
                dst.write(vendor_mbr())
                dst.truncate(532480 * 512)
            base_sha = disks.sha256_file(base_img)
            bundle = make_native_bundle(root / "bundle.tar", signing_seed=custom_seed)
            bundle_sha = disks.sha256_file(bundle)
            profile = factory_profile(board="a" * 64)
            output = root / "composed.img"
            exfat_deb = root / partition_layout.EXFATPROGS_PACKAGE
            logrotate_deb = root / partition_layout.LOGROTATE_PACKAGE
            exfat_deb.write_bytes(b"exfat")
            logrotate_deb.write_bytes(b"logrotate")

            fat_files: dict[str, bytes] = {}

            class FakePyFatFS:
                def __init__(self, *_a: object, **_kw: object) -> None:
                    pass

                def __enter__(self):
                    return self

                def __exit__(self, *_a: object) -> None:
                    return None

                def readtext(self, path: str) -> str:
                    if path == "/hobot_config.sh":
                        return "#!/bin/bash -e\nexit 0\n"
                    raise FileNotFoundError(path)

                def exists(self, path: str) -> bool:
                    return path in fat_files

                def makedir(self, _path: str) -> None:
                    return None

                def writebytes(self, path: str, data: bytes) -> None:
                    fat_files[path] = data

                def writetext(self, path: str, text: str) -> None:
                    fat_files[path] = text.encode("utf-8")

                def openbin(self, path: str, _mode: str = "r") -> io.BytesIO:
                    buf = io.BytesIO()
                    orig_close = buf.close

                    def save_on_close() -> None:
                        fat_files[path] = buf.getvalue()
                        orig_close()

                    buf.close = save_on_close  # type: ignore[method-assign]
                    return buf

            fake_pyfatfs = types.ModuleType("pyfatfs")
            fake_pyfatfs_sub = types.ModuleType("pyfatfs.PyFatFS")
            fake_pyfatfs_sub.PyFatFS = FakePyFatFS  # type: ignore[attr-defined]
            fake_pyfatfs.PyFatFS = fake_pyfatfs_sub  # type: ignore[attr-defined]

            with (
                patch.dict(
                    sys.modules,
                    {"pyfatfs": fake_pyfatfs, "pyfatfs.PyFatFS": fake_pyfatfs_sub},
                ),
                patch.dict(
                    os.environ,
                    {"MIRO_EGO_RELEASE_PUBLIC_KEY_HEX": custom_pub_hex.upper()},
                ),
                patch.object(
                    partition_layout, "prepare_tools", side_effect=[exfat_deb, logrotate_deb]
                ),
            ):
                provision.compose(
                    base_img,
                    base_sha,
                    bundle,
                    bundle_sha,
                    profile,
                    output,
                    card_bytes=64 * 1024**3,
                )

            manifest = json.loads(fat_files["/miro-ego/manifest.json"])
            self.assertEqual(manifest["bundle"]["trusted_public_key_hex"], custom_pub_hex)

            # auto-flash must check output path safety before compose() runs.
            fake_disk = disks.Disk("/dev/sdb", 64 * 1024**3, "SER", "SD Card")
            with (
                patch.object(disks, "select", return_value=fake_disk),
                patch.object(
                    disks,
                    "check_path_not_on_disk",
                    side_effect=RuntimeError("Output image cannot be stored on target disk"),
                ) as check_mock,
                patch.object(provision, "compose") as compose_mock,
                self.assertRaisesRegex(RuntimeError, "target disk"),
            ):
                recover.main(
                    [
                        "auto-flash",
                        "--image",
                        str(base_img),
                        "--sha256",
                        base_sha,
                        "--bundle",
                        str(bundle),
                        "--bundle-sha256",
                        bundle_sha,
                        "--profile",
                        str(root / "unused.mer"),
                        "--output",
                        str(root / "never-composed.img"),
                        "--disk",
                        "/dev/sdb",
                    ]
                )
            check_mock.assert_called_once()
            compose_mock.assert_not_called()

    def test_deploy_units_sockets_tmpfiles_and_shell_scripts_r3_hardening(self) -> None:
        repo_root = Path(__file__).resolve().parents[3]
        deploy_dir = repo_root / "deploy" / "miro-ego"

        deploy_svc = (deploy_dir / "miro-ego-deploy.service").read_text(encoding="utf-8")
        self.assertNotIn("ProtectSystem=strict", deploy_svc)
        self.assertNotIn("ProtectHome=yes", deploy_svc)
        self.assertNotIn("ReadWritePaths=-", deploy_svc)
        for line in (
            "PrivateTmp=yes",
            "ProtectKernelTunables=yes",
            "ProtectKernelModules=yes",
            "ProtectControlGroups=yes",
            "ProtectClock=yes",
            "LockPersonality=yes",
            "RestrictAddressFamilies=AF_UNIX AF_NETLINK AF_INET AF_INET6",
        ):
            self.assertIn(line, deploy_svc)

        tmpfiles = (deploy_dir / "miro-ego.tmpfiles").read_text(encoding="utf-8")
        self.assertIn("d /data - - - -", tmpfiles)
        self.assertNotIn("d /data 0755 root root -", tmpfiles)
        self.assertIn("d /var/lib/miro-ego-deploy 0750 root root -", tmpfiles)

        for sock_name in ("miro-ego-deploy.socket", "miro-ego-network.socket"):
            sock_text = (deploy_dir / sock_name).read_text(encoding="utf-8")
            self.assertIn("SocketUser=root", sock_text)
            self.assertIn("SocketGroup=miro-ego", sock_text)
            self.assertIn("SocketMode=0660", sock_text)

        idle_fstrim = (deploy_dir / "miro-ego-idle-fstrim").read_text(encoding="utf-8")
        self.assertNotIn('fstrim --verbose --quiet-unsupported "$DATA"', idle_fstrim)
        self.assertIn("fstrim --listed-in /etc/fstab:/proc/self/mountinfo", idle_fstrim)

        watchdog = (deploy_dir / "miro-ego-wifi-watchdog").read_text(encoding="utf-8")
        self.assertNotIn("logger -t", watchdog)
        self.assertIn("/^default via / {print $3; exit}", watchdog)

        data_vol = (deploy_dir / "miro-ego-data-volume").read_text(encoding="utf-8")
        self.assertIn("/etc/default/miro-ego-data-volume", data_vol)
        for req_opt in (
            '"uid=$capture_uid"',
            '"gid=$capture_gid"',
            '"nosuid"',
            '"nodev"',
            '"noexec"',
        ):
            self.assertIn(req_opt, data_vol)

        # miro-ego-device-login skips usermod when shell is already /bin/bash.
        if os.name == "posix":
            with tempfile.TemporaryDirectory() as folder:
                bin_dir = Path(folder)
                usermod_log = bin_dir / "usermod.log"
                (bin_dir / "getent").write_text(
                    "#!/bin/sh\nprintf 'MiroEgo:x:1001:1001::/home/MiroEgo:/bin/bash\\n'\n",
                    encoding="utf-8",
                )
                (bin_dir / "usermod").write_text(
                    f"#!/bin/sh\nprintf '%s\\n' \"$*\" >> {str(usermod_log)!r}\n",
                    encoding="utf-8",
                )
                (bin_dir / "getent").chmod(0o755)
                (bin_dir / "usermod").chmod(0o755)
                subprocess.run(
                    ["/bin/sh", str(deploy_dir / "miro-ego-device-login")],
                    env={**os.environ, "PATH": f"{bin_dir}:{os.environ.get('PATH', '')}"},
                    check=True,
                )
                self.assertFalse(usermod_log.exists())


if __name__ == "__main__":
    unittest.main()

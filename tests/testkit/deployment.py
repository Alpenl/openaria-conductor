"""Deployment release bundle builders and DeploymentHarness."""

from __future__ import annotations

import base64
import hashlib
import json
import os
import struct
import tarfile
import tempfile
import unittest
import zipfile
from contextlib import ExitStack
from pathlib import Path
from unittest.mock import patch

from rp_ylx.deployment import (
    BUNDLE_SCHEMA,
    TARGET_PLATFORM,
    ReleaseManager,
    _extract_runtime,
    _sha256,
    _write_atomic,
)

from . import ManagedHarness


class DeploymentHarness(ManagedHarness):
    """Temporary install/config/state root harness for ReleaseManager tests."""

    def _configure(self, stack: ExitStack) -> None:
        self.temporary = stack.enter_context(tempfile.TemporaryDirectory())
        self.root = Path(self.temporary)
        self.install_root = self.root / "opt/rp-ylx"
        self.config_root = self.root / "etc/rp-ylx"
        self.state_root = self.root / "var/lib/rp-ylx"
        self.maintenance_lock_path = self.root / "maintenance.lock"
        stack.enter_context(
            patch.dict(
                os.environ,
                {"OPENARIA_MAINTENANCE_LOCK": str(self.maintenance_lock_path)},
            )
        )
        self.commands: list[tuple[str, ...]] = []

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
        """Build a synthetic release bundle directory inside the harness root."""
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
        _extract_runtime(bundle.root / str(bundle.runtime["file"]), stage)  # type: ignore[attr-defined]

    @staticmethod
    def tls_material_generator(
        certificate: Path,
        private_key: Path,
        common_name: str,
    ) -> None:
        certificate.write_bytes(f"certificate:{common_name}".encode())
        private_key.write_bytes(f"private-key:{common_name}".encode())

    def manager(self, **overrides: object) -> ReleaseManager:
        """Instantiate a ReleaseManager with fake system probes and command recording."""
        kwargs: dict[str, object] = {
            "install_root": self.install_root,
            "config_root": self.config_root,
            "state_root": self.state_root,
            "system_root": self.root,
            "target_machine": "aarch64",
            "target_model": "D-Robotics RDK X5 V1.0",
            "library_finder": lambda name: f"lib{name}.so",
            "executable_finder": lambda name: f"/usr/bin/{name}",
            "runner": lambda command: self.commands.append(tuple(command)),
            "stage_installer": self.stage_installer,
            "encoder_builder": lambda stage: None,
            "health_checker": lambda: self.commands.append(("health-check",)),
            "group_resolver": lambda name: os.getgid(),
            "tls_material_generator": self.tls_material_generator,
            "unit_load_state_reader": lambda unit: "loaded",
        }
        kwargs.update(overrides)
        return ReleaseManager(**kwargs)  # type: ignore[arg-type]

    def seed_lab_config(self, manager: ReleaseManager) -> None:
        manager._ensure_layout()
        config = manager._default_device_config()
        config["security"] = {"profile": "lab", "isolated_network": True}
        _write_atomic(self.config_root / "device.json", config)


class DeploymentMixin(unittest.TestCase):
    """unittest.TestCase mixin provisioning a DeploymentHarness as self.harness."""

    harness: DeploymentHarness

    def setUp(self) -> None:
        super().setUp()
        self.harness = DeploymentHarness().setUp()
        self.addCleanup(self.harness.tearDown)


def factory_profile() -> dict[str, object]:
    """Build a valid factory recovery profile fixture dictionary."""
    files: dict[str, dict[str, str]] = {}
    config = {"state_root": "/var/lib/rp-ylx", "security": {"profile": "lab"}}
    for name, raw in {
        "etc/rp-ylx/device.json": json.dumps(config).encode(),
        "etc/machine-id": b"a" * 32,
    }.items():
        files[name] = {
            "data": base64.b64encode(raw).decode(),
            "sha256": hashlib.sha256(raw).hexdigest(),
        }
    return {"schema": "openaria.factory-profile.v1", "board": "test-board", "files": files}


def vendor_mbr() -> bytes:
    """Build a synthetic 512-byte vendor MBR image for partition layout tests."""
    raw = bytearray(512)
    raw[440:444] = bytes.fromhex("05722edb")
    raw[450], raw[466] = 0xC, 0x83
    struct.pack_into("<II", raw, 454, 8192, 524288)
    struct.pack_into("<II", raw, 470, 532480, 19767296)
    raw[510:] = b"\x55\xaa"
    return bytes(raw)


__all__ = [
    "DeploymentHarness",
    "DeploymentMixin",
    "factory_profile",
    "vendor_mbr",
]

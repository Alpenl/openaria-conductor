"""State and storage-schema compatibility guards (SQLite and exFAT)."""

from __future__ import annotations

import os
import shlex
from pathlib import Path

from .schema import DeploymentError

DATA_VOLUME_CONFIG_RELATIVE = Path("etc/default/rp-ylx-data-volume")
FACTORY_STORAGE_STATE_RELATIVE = Path("var/lib/openaria-factory/firstboot/storage.json")


def _parse_data_volume_config(system_root: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    config_path = system_root / DATA_VOLUME_CONFIG_RELATIVE
    if os.path.lexists(config_path):
        if config_path.is_symlink() or not config_path.is_file():
            raise ValueError("录像分区配置必须是普通文件")
        for line in config_path.read_text(encoding="utf-8").splitlines():
            fields = shlex.split(line, comments=True)
            if not fields:
                continue
            if len(fields) != 1 or "=" not in fields[0]:
                raise ValueError("录像分区配置格式无效")
            key, value = fields[0].split("=", 1)
            if key in values:
                raise ValueError("录像分区配置项重复")
            values[key] = value
    return values


def exfat_storage_required(system_root: Path) -> bool:
    if os.environ.get("RP_YLX_DATA_VOLUME_MODE") == "exfat" or os.path.lexists(
        system_root / FACTORY_STORAGE_STATE_RELATIVE
    ):
        return True
    try:
        values = _parse_data_volume_config(system_root)
    except (OSError, ValueError) as error:
        raise DeploymentError(
            "storage_schema_incompatible",
            "录像分区配置不可读或无效，无法确认存储模式",
        ) from error
    return values.get("RP_YLX_DATA_VOLUME_MODE") == "exfat"


def release_supports_exfat(package_root: Path) -> bool:
    """Verify an unpacked rp_ylx release handles exFAT f_files == 0 and PARTUUID."""
    recording_candidates = (
        package_root / "recording/volume.py",
        package_root / "recording/fs_safety.py",
        package_root / "recording/coordinator.py",
    )
    has_recording_exfat = any(
        candidate.is_file() and "f_files" in candidate.read_text(encoding="utf-8", errors="ignore")
        for candidate in recording_candidates
    )
    data_volume_script = package_root / "deploy/rp-ylx-data-volume"
    has_data_volume_exfat = False
    if data_volume_script.is_file():
        script_text = data_volume_script.read_text(encoding="utf-8", errors="ignore")
        has_data_volume_exfat = "exfat" in script_text and "PARTUUID" in script_text
    return has_recording_exfat and has_data_volume_exfat


def require_state_compatible_release(
    install_root: Path,
    state_root: Path,
    system_root: Path,
    commit: str,
) -> None:
    package_root = install_root / "releases" / commit / "site-packages/rp_ylx"
    if (
        any(
            (state_root / (name + ".sqlite3.initialized")).exists()
            for name in ("state", "settings")
        )
        and not (package_root / "persistence.py").is_file()
    ):
        raise DeploymentError(
            "state_schema_incompatible",
            "设备已迁移 SQLite，不能直接运行只支持旧 JSON 的固件",
        )
    if exfat_storage_required(system_root) and not release_supports_exfat(package_root):
        raise DeploymentError(
            "storage_schema_incompatible",
            "设备已启用 exFAT 独立录像分区，目标固件不支持 exFAT（f_files == 0）；禁止安装或回退",
        )

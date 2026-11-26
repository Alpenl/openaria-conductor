"""Shared removable-disk discovery and Linux partition matching helpers."""

from __future__ import annotations

import plistlib
import re
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class Disk:
    path: str
    size: int
    identity: str
    model: str


def linux_source_on_disk(source: str, disk_path: str) -> bool:
    """Return True if *source* is *disk_path* itself or one of its partitions."""
    backing = source.strip().split("[", 1)[0]
    return re.fullmatch(rf"{re.escape(disk_path)}(p?\d+)?", backing) is not None


def linux_disks(
    data: dict[str, Any],
    *,
    require_writable: bool = True,
    require_safe_mounts: bool = True,
    strict_device_names: bool = True,
) -> list[Disk]:
    result: list[Disk] = []
    for d in data.get("blockdevices", []):
        if (
            d.get("type") != "disk"
            or int(d.get("size") or 0) == 0
            or not (d.get("rm") or d.get("tran") == "usb")
        ):
            continue
        if require_writable and d.get("ro"):
            continue
        if require_safe_mounts:
            nodes = [d]
            safe = True
            while nodes:
                n = nodes.pop()
                nodes.extend(n.get("children", []))
                mounts = n.get("mountpoints") or []
                if any(m in ("/", "/boot", "/boot/efi", "[SWAP]") for m in mounts):
                    safe = False
                if n.get("type") not in ("disk", "part"):
                    safe = False
            if not safe:
                continue
        path = d.get("path", "")
        if strict_device_names and not re.fullmatch(r"/dev/(sd[a-z]+|mmcblk[0-9]+)", path):
            continue
        result.append(
            Disk(
                path,
                int(d["size"]),
                str(d.get("serial") or d.get("wwn") or ""),
                str(d.get("model") or "SD card"),
            )
        )
    return result


def windows_disks(
    data: Any,
    *,
    filter_attributes: bool = True,
    require_writable: bool = True,
) -> list[Disk]:
    if isinstance(data, dict):
        data = [data]
    result: list[Disk] = []
    for d in data or []:
        size = int(d.get("Size") or 0)
        if size <= 0:
            continue
        if filter_attributes and not (
            d.get("BusType") in ("USB", "SD")
            and d.get("IsBoot") is False
            and d.get("IsSystem") is False
            and (not require_writable or d.get("IsReadOnly") is False)
        ):
            continue
        result.append(
            Disk(
                r"\\.\PhysicalDrive" + str(int(d["Number"])),
                size,
                str(d.get("UniqueId") or d.get("SerialNumber") or ""),
                str(d.get("FriendlyName") or "SD card"),
            )
        )
    return result


def macos_disks(
    run_fn: Callable[..., bytes],
    *,
    require_writable: bool = True,
    exclude_apfs_boot: bool = True,
) -> list[Disk]:
    devices = plistlib.loads(run_fn("diskutil", "list", "-plist", "external", "physical"))
    protected: set[str | None] = set()
    if exclude_apfs_boot:
        root_info = plistlib.loads(run_fn("diskutil", "info", "-plist", "/"))
        protected.update({root_info.get("ParentWholeDisk"), root_info.get("DeviceIdentifier")})
        # Reject all APFS physical stores, including external macOS boot containers.
        apfs = plistlib.loads(run_fn("diskutil", "apfs", "list", "-plist"))
        for container in apfs.get("Containers", []):
            for store in container.get("PhysicalStores", []):
                info = plistlib.loads(
                    run_fn("diskutil", "info", "-plist", store["DeviceIdentifier"])
                )
                protected.add(info.get("ParentWholeDisk"))
    result: list[Disk] = []
    for name in devices.get("WholeDisks", []):
        if name in protected:
            continue
        if exclude_apfs_boot and not re.fullmatch(r"disk[0-9]+", name):
            continue
        d = plistlib.loads(run_fn("diskutil", "info", "-plist", "/dev/" + name))
        if (
            d.get("Whole") is True
            and d.get("Internal") is False
            and (not require_writable or d.get("Writable") is True)
            and (not exclude_apfs_boot or d.get("VirtualOrPhysical") != "Virtual")
        ):
            result.append(
                Disk(
                    "/dev/" + name,
                    int(d["TotalSize"]),
                    str(d.get("DeviceTreePath") or ""),
                    str(d.get("MediaName") or name),
                )
            )
    return result

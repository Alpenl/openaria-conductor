"""不改变设备状态的 RDK X5 硬件事实探针。"""

from __future__ import annotations

import hashlib
import os
import platform
import shutil
import subprocess
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from rp_ylx.hardware.target import (
    RDK_X5_BOARD_ID,
    YLX_2UQ2_CAMERA_ID,
    is_rdk_x5_model,
    is_ylx_2uq2_usb,
)

PROBE_FORMAT = "ylx.hardware-probe.v0"


def _text(path: Path) -> str | None:
    try:
        return path.read_text(encoding="utf-8", errors="replace").strip("\x00\n ") or None
    except OSError:
        return None


def _os_release(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    content = _text(path)
    if content is None:
        return values
    for line in content.splitlines():
        if "=" not in line or line.startswith("#"):
            continue
        key, value = line.split("=", 1)
        values[key] = value.strip().strip('"')
    return {key: values[key] for key in ("ID", "VERSION_ID", "PRETTY_NAME") if key in values}


def _mem_total_kib(path: Path) -> int | None:
    content = _text(path)
    if content is None:
        return None
    for line in content.splitlines():
        if line.startswith("MemTotal:"):
            fields = line.split()
            if len(fields) >= 2 and fields[1].isdigit():
                return int(fields[1])
    return None


def _hashed(value: str | None) -> str | None:
    return None if not value else "sha256:" + hashlib.sha256(value.encode()).hexdigest()


def _uevent(path: Path) -> dict[str, str]:
    content = _text(path)
    if content is None:
        return {}
    result: dict[str, str] = {}
    for line in content.splitlines():
        if "=" in line:
            key, value = line.split("=", 1)
            result[key] = value
    return result


def _v4l2_modes(device: Path, executable: str | None) -> dict[str, Any]:
    if executable is None:
        return {"status": "tool_unavailable", "formats": ""}
    try:
        result = subprocess.run(
            [executable, "--device", str(device), "--list-formats-ext"],
            capture_output=True,
            text=True,
            timeout=5,
            check=False,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        return {"status": "probe_failed", "formats": "", "error": str(exc)}
    if result.returncode != 0:
        return {
            "status": "probe_failed",
            "formats": "",
            "error": result.stderr.strip()[:1000],
        }
    return {"status": "ok", "formats": result.stdout.strip()}


def _video_devices(sys_root: Path, dev_root: Path, executable: str | None) -> list[dict[str, Any]]:
    devices: list[dict[str, Any]] = []
    base = sys_root / "class" / "video4linux"
    try:
        entries = sorted(base.iterdir(), key=lambda item: item.name)
    except OSError:
        return devices
    for entry in entries:
        node = dev_root / entry.name
        devices.append(
            {
                "node": str(node),
                "name": _text(entry / "name"),
                "index": _text(entry / "index"),
                "readable": os.access(node, os.R_OK),
                "writable": os.access(node, os.W_OK),
                "modes": _v4l2_modes(node, executable),
            }
        )
    return devices


def _usb_devices(sys_root: Path) -> list[dict[str, Any]]:
    devices: list[dict[str, Any]] = []
    base = sys_root / "bus" / "usb" / "devices"
    try:
        entries = sorted(base.iterdir(), key=lambda item: item.name)
    except OSError:
        return devices
    for entry in entries:
        vendor = _text(entry / "idVendor")
        product_id = _text(entry / "idProduct")
        if vendor is None or product_id is None:
            continue
        devices.append(
            {
                "sys_name": entry.name,
                "vendor_id": vendor.lower(),
                "product_id": product_id.lower(),
                "device_release_bcd": _text(entry / "bcdDevice"),
                "manufacturer": _text(entry / "manufacturer"),
                "product": _text(entry / "product"),
                "speed_mbps": _text(entry / "speed"),
                "serial_digest": _hashed(_text(entry / "serial")),
            }
        )
    return devices


def _hidraw_devices(sys_root: Path, dev_root: Path) -> list[dict[str, Any]]:
    devices: list[dict[str, Any]] = []
    base = sys_root / "class" / "hidraw"
    try:
        entries = sorted(base.iterdir(), key=lambda item: item.name)
    except OSError:
        return devices
    for entry in entries:
        values = _uevent(entry / "device" / "uevent")
        node = dev_root / entry.name
        devices.append(
            {
                "node": str(node),
                "hid_id": values.get("HID_ID"),
                "name": values.get("HID_NAME"),
                "unique_digest": _hashed(values.get("HID_UNIQ")),
                "readable": os.access(node, os.R_OK),
                "writable": os.access(node, os.W_OK),
            }
        )
    return devices


def _storage(path: Path) -> dict[str, Any]:
    try:
        stat = os.statvfs(path)
    except OSError as exc:
        return {"path": str(path), "status": "unavailable", "error": str(exc)}
    return {
        "path": str(path),
        "status": "ok",
        "block_size": stat.f_frsize,
        "total_bytes": stat.f_blocks * stat.f_frsize,
        "available_bytes": stat.f_bavail * stat.f_frsize,
        "name_max": stat.f_namemax,
    }


def _target(model: str | None, usb_devices: list[dict[str, Any]]) -> dict[str, Any]:
    board_matches = is_rdk_x5_model(model)
    camera_matches = any(is_ylx_2uq2_usb(device) for device in usb_devices)
    if not board_matches:
        return {
            "board": "unsupported",
            "camera": YLX_2UQ2_CAMERA_ID if camera_matches else "not_found",
            "supported": False,
            "reason": "unsupported_board",
        }
    if not camera_matches:
        return {
            "board": RDK_X5_BOARD_ID,
            "camera": "not_found",
            "supported": False,
            "reason": "camera_not_found",
        }
    return {
        "board": RDK_X5_BOARD_ID,
        "camera": YLX_2UQ2_CAMERA_ID,
        "supported": True,
        "reason": "matched",
    }


def collect_hardware_facts(
    *,
    sys_root: Path = Path("/sys"),
    proc_root: Path = Path("/proc"),
    etc_root: Path = Path("/etc"),
    dev_root: Path = Path("/dev"),
    storage_path: Path = Path("/"),
    v4l2_ctl: str | None = None,
) -> dict[str, Any]:
    model = _text(proc_root / "device-tree" / "model")
    executable = shutil.which("v4l2-ctl") if v4l2_ctl is None else v4l2_ctl
    usb_devices = _usb_devices(sys_root)
    return {
        "format": PROBE_FORMAT,
        "observed_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "platform": {
            "machine": platform.machine(),
            "kernel": platform.release(),
            "model": model,
            "raspberry_pi": bool(model and "raspberry pi" in model.lower()),
            "os_release": _os_release(etc_root / "os-release"),
            "memory_total_kib": _mem_total_kib(proc_root / "meminfo"),
        },
        "target": _target(model, usb_devices),
        "tools": {"v4l2_ctl": executable},
        "video_devices": _video_devices(sys_root, dev_root, executable),
        "usb_devices": usb_devices,
        "hidraw_devices": _hidraw_devices(sys_root, dev_root),
        "storage": _storage(storage_path),
    }

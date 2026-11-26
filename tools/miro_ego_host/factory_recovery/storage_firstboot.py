"""Initialize exFAT partition 3 (MIRO-EGO) on a freshly flashed Miro-Ego factory card."""

from __future__ import annotations

import errno
import hashlib
import json
import os
import re
import shutil
import stat
import struct
import subprocess
import uuid
from collections.abc import Callable
from contextlib import suppress
from pathlib import Path
from typing import Any

if __package__:
    from . import partition_layout
else:
    import partition_layout

CONFIG = Path("/etc/default/miro-ego-data-volume")
DATA_MOUNT = Path("/data")
DATA_VOLUME_HELPER = Path("/usr/local/sbin/miro-ego-data-volume")
MIRO_EGO_BIN = Path("/opt/miro-ego/current/bin/miro-ego")
VOLUME_LABEL = partition_layout.DATA_VOLUME_LABEL

# Linux <linux/msdos_fs.h> FAT/exFAT attribute ioctl constants.
FAT_IOCTL_GET_ATTRIBUTES = 0x80047210
FAT_IOCTL_SET_ATTRIBUTES = 0x40047211
FAT_ATTR_HIDDEN = 0x02

DATA_SUBDIRS = (
    "recordings",
    "logs",
    "logs/wifi-watchdog",
    "updates",
    ".miro-ego/index",
    ".miro-ego/staging",
    ".miro-ego/quarantine",
    ".miro-ego/audit",
    ".miro-ego/receipts",
    ".miro-ego/telemetry",
)


def run(*args: str, timeout: int = 120) -> str:
    """Execute a system storage command and return stripped stdout text."""
    return subprocess.check_output(args, timeout=timeout, text=True).strip()


def target(layout: dict[str, Any], *, allow_mounted: bool = False) -> tuple[Path, Path]:
    """Authenticate the physical RDK X5 SD card and return ``(p2_root, p3_data)``."""
    root = Path(run("findmnt", "-n", "-o", "SOURCE", "/")).resolve()
    if not re.fullmatch(r"/dev/mmcblk[0-9]+p2", str(root)):
        raise RuntimeError("Factory storage requires root on RDK SD partition 2")
    disk = Path(str(root)[:-2])
    data = Path(str(root)[:-1] + "3")
    if not all(stat.S_ISBLK(p.stat().st_mode) for p in (root, disk, data)):
        raise RuntimeError("Missing physical factory partitions")
    with disk.open("rb") as src:
        mbr = src.read(512)
    if hashlib.sha256(mbr).hexdigest() != layout["mbr_sha256"]:
        raise RuntimeError("Card layout changed since composition")
    if int(run("blockdev", "--getsize64", str(disk))) < layout["card_bytes"]:
        raise RuntimeError("Card is smaller than the prepared layout")
    entries = partition_layout.partitions(mbr)
    if entries[2][0] != 0x07 or entries[1][1] + entries[1][2] != entries[2][1]:
        raise RuntimeError("Unexpected recording partition bounds")
    if run("blkid", "-s", "PARTUUID", "-o", "value", str(data)) != layout["partuuid"]:
        raise RuntimeError("Recording partition identity mismatch")
    if not allow_mounted and run("lsblk", "-nr", "-o", "MOUNTPOINTS", str(data)):
        raise RuntimeError("Recording partition is in use")
    holders_dir = Path("/sys/class/block") / data.name / "holders"
    if holders_dir.exists() and any(holders_dir.iterdir()):
        raise RuntimeError("Recording partition has active holders")
    return root, data


def _write_data_volume_config(partuuid: str, fs_uuid: str) -> None:
    CONFIG.parent.mkdir(parents=True, mode=0o755, exist_ok=True)
    temporary = CONFIG.with_name(f".{CONFIG.name}.{uuid.uuid4().hex}.tmp")
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | getattr(os, "O_CLOEXEC", 0)
    fd = os.open(temporary, flags, 0o644)
    replaced = False
    try:
        try:
            stream = os.fdopen(fd, "w", encoding="utf-8")
        except BaseException:
            os.close(fd)
            raise
        with stream as dst:
            os.fchmod(dst.fileno(), 0o644)
            dst.write("MIRO_EGO_DATA_VOLUME_MODE=exfat\n")
            dst.write(f"MIRO_EGO_DATA_VOLUME_PARTUUID={partuuid}\n")
            dst.write(f"MIRO_EGO_DATA_VOLUME_UUID={fs_uuid}\n")
            dst.flush()
            os.fsync(dst.fileno())
        os.replace(temporary, CONFIG)
        replaced = True
        descriptor = os.open(CONFIG.parent, os.O_RDONLY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
    except BaseException:
        if not replaced:
            with suppress(OSError):
                temporary.unlink(missing_ok=True)
        raise


def prepare(
    layout: dict[str, Any],
    boot: Path,
    state_root: Path,
    atomic_json: Callable[[Path, Any], None],
) -> tuple[Path, str]:
    """Install offline exfatprogs, resize p2 to 20 GiB, format p3 as ``MIRO-EGO`` exFAT."""
    if layout.get("schema") != partition_layout.LAYOUT_SCHEMA:
        raise RuntimeError("Unknown factory storage layout")
    if layout.get("label", VOLUME_LABEL) != VOLUME_LABEL:
        raise RuntimeError(f"Expected recording volume label {VOLUME_LABEL}")

    package = boot / partition_layout.EXFATPROGS_PACKAGE
    if layout.get("exfatprogs_sha256") != partition_layout.EXFATPROGS_SHA256:
        raise RuntimeError("Offline exfatprogs manifest digest mismatch")
    partition_layout.verify_deb(package, partition_layout.EXFATPROGS_SHA256)

    journal = state_root / "storage.json"
    previous = json.loads(journal.read_bytes()) if journal.exists() else None

    # A completed journal never authorizes another mkfs; only revalidate existing UUID.
    if previous and previous.get("phase") == "ready":
        _, data = target(layout, allow_mounted=True)
        if previous.get("layout") != layout:
            raise RuntimeError("Unrelated storage transaction")
        if run("blkid", "-s", "UUID", "-o", "value", str(data)) != previous["uuid"]:
            raise RuntimeError("Initialized recording filesystem identity changed")
        identity = str(previous["uuid"])
    else:
        if previous and previous != {"phase": "formatting", "layout": layout}:
            raise RuntimeError("Unrelated storage transaction")
        root, data = target(layout)
        run("dpkg", "-i", str(package))
        run("modprobe", "exfat")
        run("resize2fs", str(root), timeout=300)
        # Durable journal permits retry before ready is recorded; never formats after ready.
        atomic_json(journal, {"phase": "formatting", "layout": layout})
        run("mkfs.exfat", "-L", VOLUME_LABEL, str(data))
        run("fsck.exfat", "-n", str(data))
        identity = run("blkid", "-p", "-s", "UUID", "-o", "value", str(data))
        if not identity:
            raise RuntimeError("New exFAT filesystem has no identity")
        atomic_json(journal, {"phase": "ready", "layout": layout, "uuid": identity})

    run("udevadm", "trigger", "--action=change", str(data))
    run("udevadm", "settle", "--timeout=30")
    _write_data_volume_config(layout["partuuid"], identity)
    return data, identity


def _set_fat_hidden_ioctl(path: Path) -> bool:
    if os.name != "posix":
        return False
    import fcntl

    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_CLOEXEC", 0)
    fd = os.open(path, flags)
    try:
        buf = bytearray(4)
        fcntl.ioctl(fd, FAT_IOCTL_GET_ATTRIBUTES, buf, True)
        attrs = struct.unpack("<I", buf)[0]
        if not (attrs & FAT_ATTR_HIDDEN):
            updated = struct.pack("<I", attrs | FAT_ATTR_HIDDEN)
            fcntl.ioctl(fd, FAT_IOCTL_SET_ATTRIBUTES, bytearray(updated), False)
        return True
    except OSError as exc:
        if exc.errno in (errno.ENOTTY, errno.EOPNOTSUPP, errno.EINVAL, errno.ENOSYS):
            return False
        raise
    finally:
        os.close(fd)


def set_exfat_hidden(path: Path, *, miro_ego_bin: Path | None = None) -> bool:
    """Apply native exFAT ``ATTR_HIDDEN`` to *path* using fatattr, miro-ego, or ioctl."""
    if shutil.which("fatattr"):
        try:
            run("fatattr", "+h", str(path))
            return True
        except subprocess.CalledProcessError:
            pass
    binary = miro_ego_bin or MIRO_EGO_BIN
    if binary.is_file() and os.access(binary, os.X_OK):
        try:
            run(str(binary), "volume", "hide", str(path))
            return True
        except subprocess.CalledProcessError:
            pass
    return _set_fat_hidden_ioctl(path)


def ensure_data_layout(
    mountpoint: Path = DATA_MOUNT,
    *,
    miro_ego_bin: Path | None = None,
) -> bool:
    """Create canonical ``MIRO-EGO`` directories under *mountpoint* and hide ``.miro-ego``."""
    for rel in DATA_SUBDIRS:
        (mountpoint / rel).mkdir(parents=True, exist_ok=True)
    binary = miro_ego_bin or MIRO_EGO_BIN
    volume_meta = mountpoint / ".miro-ego" / "volume.json"
    if not volume_meta.exists() and binary.is_file() and os.access(binary, os.X_OK):
        run(str(binary), "volume", "init", str(mountpoint))
    return set_exfat_hidden(mountpoint / ".miro-ego", miro_ego_bin=binary)


def _is_mounted(mountpoint: Path) -> bool:
    result = subprocess.run(
        ["mountpoint", "-q", str(mountpoint)],
        check=False,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    return result.returncode == 0


def mount_and_initialize(
    data_device: Path,
    *,
    mountpoint: Path = DATA_MOUNT,
    helper: Path = DATA_VOLUME_HELPER,
    miro_ego_bin: Path = MIRO_EGO_BIN,
) -> bool:
    """Mount ``/data`` as exFAT, ensure canonical layout, and apply ``ATTR_HIDDEN``."""
    if helper.is_file() and os.access(helper, os.X_OK):
        run(str(helper), "ensure")
    elif not _is_mounted(mountpoint):
        mountpoint.mkdir(parents=True, mode=0o755, exist_ok=True)
        run(
            "mount",
            "-t",
            "exfat",
            "-o",
            "rw,nosuid,nodev,noexec,noatime,errors=remount-ro",
            str(data_device),
            str(mountpoint),
        )
    return ensure_data_layout(mountpoint, miro_ego_bin=miro_ego_bin)

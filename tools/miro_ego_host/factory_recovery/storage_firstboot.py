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


def _write_data_volume_config(
    partuuid: str,
    fs_uuid: str,
    *,
    config_path: Path | None = None,
) -> None:
    target_cfg = config_path if config_path is not None else CONFIG
    target_cfg.parent.mkdir(parents=True, mode=0o755, exist_ok=True)
    temporary = target_cfg.with_name(f".{target_cfg.name}.{uuid.uuid4().hex}.tmp")
    flags = (
        os.O_WRONLY
        | os.O_CREAT
        | os.O_EXCL
        | getattr(os, "O_NOFOLLOW", 0)
        | getattr(os, "O_CLOEXEC", 0)
    )
    fd = os.open(temporary, flags, 0o644)
    replaced = False
    try:
        try:
            stream = os.fdopen(fd, "w", encoding="utf-8")
        except BaseException:
            os.close(fd)
            raise
        with stream as dst:
            if hasattr(os, "fchmod"):
                os.fchmod(dst.fileno(), 0o644)
            dst.write("MIRO_EGO_DATA_VOLUME_MODE=exfat\n")
            dst.write(f"MIRO_EGO_DATA_VOLUME_PARTUUID={partuuid}\n")
            dst.write(f"MIRO_EGO_DATA_VOLUME_UUID={fs_uuid}\n")
            dst.flush()
            os.fsync(dst.fileno())
        if not hasattr(os, "fchmod"):
            with suppress(OSError):
                temporary.chmod(0o644)
        os.replace(temporary, target_cfg)
        replaced = True
        if os.name == "posix":
            descriptor = os.open(target_cfg.parent, os.O_RDONLY)
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
        if run("blkid", "-s", "UUID", "-o", "value", data.as_posix()) != previous["uuid"]:
            raise RuntimeError("Initialized recording filesystem identity changed")
        identity = str(previous["uuid"])
    else:
        if previous and previous != {"phase": "formatting", "layout": layout}:
            raise RuntimeError("Unrelated storage transaction")
        root, data = target(layout)
        run("dpkg", "-i", str(package))
        run("modprobe", "exfat")
        run("resize2fs", root.as_posix(), timeout=300)
        # Durable journal permits retry before ready is recorded; never formats after ready.
        atomic_json(journal, {"phase": "formatting", "layout": layout})
        run("mkfs.exfat", "-L", VOLUME_LABEL, data.as_posix())
        run("fsck.exfat", "-n", data.as_posix())
        identity = run("blkid", "-p", "-s", "UUID", "-o", "value", data.as_posix())
        if not identity:
            raise RuntimeError("New exFAT filesystem has no identity")
        atomic_json(journal, {"phase": "ready", "layout": layout, "uuid": identity})

    run("udevadm", "trigger", "--action=change", data.as_posix())
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


def _blkid_optional(device: Path, tag: str) -> str:
    try:
        return run("blkid", "-s", tag, "-o", "value", device.as_posix())
    except subprocess.CalledProcessError:
        return ""


def _reread_partition_table(disk: Path) -> None:
    disk_posix = disk.as_posix()
    for cmd in (
        ("partx", "-u", disk_posix),
        ("partprobe", disk_posix),
        ("blockdev", "--rereadpt", disk_posix),
    ):
        if shutil.which(cmd[0]):
            try:
                run(*cmd)
                break
            except subprocess.CalledProcessError:
                continue
    run("udevadm", "settle", "--timeout=30")


def ensure_ssh_data_volume(
    *,
    config_path: Path | None = None,
) -> dict[str, str]:
    """Discover or provision partition 3 (``MIRO-EGO`` exFAT) and write data-volume config.

    Used during SSH ``recover.py restore`` before ``ACTIVATE`` starts data-volume mount:
    - Never reformats an existing exFAT recording partition.
    - Creates MBR partition 3 in unallocated trailing space after partition 2 if absent.
    - Formats partition 3 as ``MIRO-EGO`` exFAT only when partition 3 has no filesystem.
    """
    root_src = Path(run("findmnt", "-n", "-o", "SOURCE", "/")).resolve()
    root_posix = root_src.as_posix()
    if not re.fullmatch(r"/dev/mmcblk[0-9]+p2", root_posix):
        raise RuntimeError("SSH recovery requires root on RDK SD partition 2")
    disk = root_src.with_name(root_src.name[:-2])
    data = root_src.with_name(root_src.name[:-1] + "3")
    if not stat.S_ISBLK(disk.stat().st_mode):
        raise RuntimeError(f"Missing whole-disk block device: {disk.as_posix()}")

    if not (data.exists() and stat.S_ISBLK(data.stat().st_mode)):
        with disk.open("rb") as src:
            mbr = src.read(512)
        if len(mbr) != 512 or mbr[510:512] != b"\x55\xaa":
            raise RuntimeError("Expected valid MBR on RDK SD card")
        entries = partition_layout.partitions(mbr)
        if entries[2] != (0, 0, 0) or entries[3] != (0, 0, 0):
            raise RuntimeError("Unexpected MBR partition entries while p3 block node is absent")
        p2_type, p2_start, p2_sectors = entries[1]
        if p2_type != 0x83 or p2_sectors <= 0:
            raise RuntimeError("Unexpected root partition entry in MBR")
        card_bytes = int(run("blockdev", "--getsize64", disk.as_posix()))
        align = partition_layout.ALIGNMENT_SECTORS
        sector_size = partition_layout.SECTOR_SIZE
        min_data_bytes = partition_layout.MIN_DATA_GIB * partition_layout.GIB
        p3_start = ((p2_start + p2_sectors + align - 1) // align) * align
        p3_end = (card_bytes // sector_size // align) * align
        if p3_end <= p3_start or (p3_end - p3_start) * sector_size < min_data_bytes:
            raise RuntimeError(
                "Insufficient unallocated space after p2 for MIRO-EGO exFAT volume; "
                "use auto-flash to prepare a 20 GiB system + MIRO-EGO recording card"
            )
        updated_mbr = bytearray(mbr)
        updated_mbr[478 + 4] = 0x07
        struct.pack_into("<II", updated_mbr, 478 + 8, p3_start, p3_end - p3_start)
        with disk.open("r+b") as dst:
            dst.write(updated_mbr)
            dst.flush()
            os.fsync(dst.fileno())
        _reread_partition_table(disk)
        if not (data.exists() and stat.S_ISBLK(data.stat().st_mode)):
            raise RuntimeError(f"Kernel did not expose recording partition {data.as_posix()}")

    partuuid = _blkid_optional(data, "PARTUUID")
    if not re.fullmatch(r"[0-9a-fA-F]{8}-03", partuuid):
        raise RuntimeError(
            f"Invalid recording partition PARTUUID on {data.as_posix()}: {partuuid!r}"
        )

    fs_type = _blkid_optional(data, "TYPE")
    if fs_type == "exfat":
        fs_uuid = _blkid_optional(data, "UUID")
        if not re.fullmatch(r"[0-9a-fA-F]{4}-[0-9a-fA-F]{4}", fs_uuid):
            raise RuntimeError(f"Invalid exFAT UUID on existing {data.as_posix()}: {fs_uuid!r}")
    elif fs_type == "":
        if run("lsblk", "-nr", "-o", "MOUNTPOINTS", data.as_posix()):
            raise RuntimeError("Unformatted recording partition is unexpectedly mounted")
        run("modprobe", "exfat")
        run("mkfs.exfat", "-L", VOLUME_LABEL, data.as_posix())
        run("fsck.exfat", "-n", data.as_posix())
        fs_uuid = run("blkid", "-p", "-s", "UUID", "-o", "value", data.as_posix())
        if not re.fullmatch(r"[0-9a-fA-F]{4}-[0-9a-fA-F]{4}", fs_uuid):
            raise RuntimeError(f"Formatted exFAT volume has invalid UUID: {fs_uuid!r}")
        run("udevadm", "trigger", "--action=change", data.as_posix())
        run("udevadm", "settle", "--timeout=30")
    else:
        raise RuntimeError(
            f"Refusing to overwrite non-exFAT filesystem {fs_type!r} on {data.as_posix()}"
        )

    _write_data_volume_config(partuuid, fs_uuid, config_path=config_path)
    return {"device": data.as_posix(), "partuuid": partuuid, "uuid": fs_uuid}

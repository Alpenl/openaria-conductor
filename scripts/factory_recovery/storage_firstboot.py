"""Initialize only partition 3 of an authenticated, newly flashed factory card."""

from __future__ import annotations

import hashlib
import json
import os
import re
import stat
import subprocess
from pathlib import Path

if __package__:
    from . import partition_layout
else:
    import partition_layout

CONFIG = Path("/etc/default/rp-ylx-data-volume")


def run(*args, timeout=120):
    return subprocess.check_output(args, timeout=timeout, text=True).strip()


def target(layout, *, allow_mounted=False):
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
    if entries[2][0] != 7 or entries[1][1] + entries[1][2] != entries[2][1]:
        raise RuntimeError("Unexpected recording partition bounds")
    if run("blkid", "-s", "PARTUUID", "-o", "value", str(data)) != layout["partuuid"]:
        raise RuntimeError("Recording partition identity mismatch")
    # Refuse any mounts, swap or device-mapper holders, not just /data.
    if not allow_mounted and run("lsblk", "-nr", "-o", "MOUNTPOINTS", str(data)):
        raise RuntimeError("Recording partition is in use")
    if any((Path("/sys/class/block") / data.name / "holders").iterdir()):
        raise RuntimeError("Recording partition has active holders")
    return root, data


def prepare(layout, boot, state_root, atomic_json):
    if layout.get("schema") != "openaria.storage.exfat.v1":
        raise RuntimeError("Unknown factory storage layout")
    package = boot / partition_layout.PACKAGE
    if (
        layout["package_sha256"] != partition_layout.PACKAGE_SHA256
        or hashlib.sha256(package.read_bytes()).hexdigest() != partition_layout.PACKAGE_SHA256
    ):
        raise RuntimeError("Offline exFAT tools digest mismatch")
    journal = state_root / "storage.json"
    previous = json.loads(journal.read_bytes()) if journal.exists() else None
    # On a retry after installation, /data may already be mounted. A completed
    # journal never authorizes another mkfs; only validate the existing identity.
    if previous and previous.get("phase") == "ready":
        _, data = target(layout, allow_mounted=True)
        if previous.get("layout") != layout:
            raise RuntimeError("Unrelated storage transaction")
        if run("blkid", "-s", "UUID", "-o", "value", str(data)) != previous["uuid"]:
            raise RuntimeError("Initialized recording filesystem identity changed")
        identity = previous["uuid"]
    else:
        if previous and previous != {"phase": "formatting", "layout": layout}:
            raise RuntimeError("Unrelated storage transaction")
        root, data = target(layout)
        run("dpkg", "-i", str(package))
        run("modprobe", "exfat")
        run("resize2fs", str(root), timeout=300)
        # This durable journal permits retry only before software installation.
        # After ready is persisted, no boot/retry path formats recordings again.
        atomic_json(journal, {"phase": "formatting", "layout": layout})
        run("mkfs.exfat", "-L", "RECORDINGS", str(data))
        run("fsck.exfat", "-n", str(data))
        identity = run("blkid", "-p", "-s", "UUID", "-o", "value", str(data))
        if not identity:
            raise RuntimeError("New exFAT filesystem has no identity")
        atomic_json(journal, {"phase": "ready", "layout": layout, "uuid": identity})
    # Refresh aliases after mkfs; an old card may still have stale udev UUID
    # links when the recording service starts immediately after installation.
    run("udevadm", "trigger", "--action=change", str(data))
    run("udevadm", "settle", "--timeout=30")
    CONFIG.parent.mkdir(parents=True, exist_ok=True)
    temporary = CONFIG.with_suffix(".tmp")
    with temporary.open("w") as dst:
        dst.write("RP_YLX_DATA_VOLUME_MODE=exfat\n")
        dst.write("RP_YLX_DATA_VOLUME_PARTUUID=" + layout["partuuid"] + "\n")
        dst.write("RP_YLX_DATA_VOLUME_UUID=" + identity + "\n")
        dst.flush()
        os.fsync(dst.fileno())
    os.replace(temporary, CONFIG)
    descriptor = os.open(CONFIG.parent, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)

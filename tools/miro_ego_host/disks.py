"""Cross-platform removable-disk discovery, partition safety, and verified raw I/O.

Designed for Miro-Ego host PC utilities (factory recovery flashing and read-only
SD-card export). Skips empty 0-byte card readers, blocks OS/system/mapped disks,
enforces strict Linux whole-disk-to-partition matching, and streams SHA-256 digests.
"""

from __future__ import annotations

import hashlib
import json
import os
import platform
import plistlib
import re
import subprocess
from collections.abc import Callable
from contextlib import contextmanager, nullcontext
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, BinaryIO

BLOCK = 4 * 1024 * 1024
MIN_FLASH_CARD_BYTES = 8 * 1024**3
MAX_FLASH_CARD_BYTES = 512 * 1024**3

_LINUX_PROTECTED_MOUNTS = frozenset(
    {
        "/",
        "/boot",
        "/boot/efi",
        "/boot/firmware",
        "/usr",
        "/var",
        "[SWAP]",
    }
)


@dataclass(frozen=True)
class Disk:
    """Eligible removable external whole disk discovered on the host computer."""

    path: str
    size: int
    identity: str
    model: str


def run(*args: str) -> bytes:
    """Execute a host inspection/unmount command and return raw stdout."""
    return subprocess.check_output(args)


def powershell(script: str) -> bytes:
    """Execute a non-interactive PowerShell snippet with Stop error policy."""
    return run(
        "powershell.exe",
        "-NoProfile",
        "-NonInteractive",
        "-Command",
        "$ErrorActionPreference='Stop'; " + script,
    )


def sha256_file(path: str | Path, *, chunk_size: int = BLOCK) -> str:
    """Compute a streaming lowercase SHA-256 hex digest for a regular file."""
    hasher = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(chunk_size), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


def digest(path: str | Path) -> str:
    """Alias for ``sha256_file`` for compatibility with recovery workflows."""
    return sha256_file(path)


def sha256_stream(
    stream: BinaryIO,
    size: int | None = None,
    *,
    chunk_size: int = BLOCK,
) -> tuple[str, int]:
    """Stream up to *size* bytes (or until EOF) and return ``(sha256_hex, total_bytes)``."""
    hasher = hashlib.sha256()
    total = 0
    while size is None or total < size:
        to_read = chunk_size if size is None else min(chunk_size, size - total)
        chunk = stream.read(to_read)
        if not chunk:
            if size is not None:
                raise RuntimeError("Short stream read while computing SHA-256")
            break
        hasher.update(chunk)
        total += len(chunk)
    return hasher.hexdigest(), total


def linux_source_on_disk(source: str, disk_path: str) -> bool:
    """Return True iff *source* refers to *disk_path* itself or one of its partitions.

    Prevents prefix confusion such as ``/dev/sda`` matching ``/dev/sdab1`` or
    ``/dev/sdap1`` (which belongs to ``/dev/sdap``), and ``/dev/mmcblk0``
    matching ``/dev/mmcblk01`` (a different block device).
    """
    backing = source.strip().split("[", 1)[0]
    disk = disk_path.strip()
    if not backing or not disk:
        return False
    if backing == disk:
        return True
    escaped = re.escape(disk)
    if disk[-1].isdigit():
        # Devices ending with a digit (e.g. /dev/mmcblk0, /dev/nvme0n1) use 'p<N>'.
        return re.fullmatch(rf"{escaped}p\d+", backing) is not None
    # Devices ending with a letter (e.g. /dev/sda, /dev/sdz) use '<N>' directly.
    return re.fullmatch(rf"{escaped}\d+", backing) is not None


def _node_mountpoints(node: dict[str, Any]) -> list[str]:
    mounts: list[str] = []
    raw_list = node.get("mountpoints")
    if isinstance(raw_list, list):
        mounts.extend(str(m) for m in raw_list if m)
    single = node.get("mountpoint")
    if single:
        mounts.append(str(single))
    return mounts


def linux_disks(
    data: dict[str, Any],
    *,
    require_writable: bool = True,
    require_safe_mounts: bool = True,
    strict_device_names: bool = True,
) -> list[Disk]:
    """Filter parsed ``lsblk -Jbp`` output down to eligible removable external disks."""
    result: list[Disk] = []
    for entry in data.get("blockdevices", []):
        if entry.get("type") != "disk":
            continue
        size = int(entry.get("size") or 0)
        if size <= 0:
            # Skip empty card readers with no media inserted.
            continue
        if not (entry.get("rm") or entry.get("tran") == "usb"):
            continue
        if require_writable and entry.get("ro"):
            continue
        if require_safe_mounts:
            nodes = [entry]
            safe = True
            while nodes:
                node = nodes.pop()
                nodes.extend(node.get("children", []) or [])
                if any(m in _LINUX_PROTECTED_MOUNTS for m in _node_mountpoints(node)):
                    safe = False
                    break
                if node.get("type") not in ("disk", "part"):
                    # Reject crypt, lvm, raid, md, dm, loop, or rom holders.
                    safe = False
                    break
            if not safe:
                continue
        path = str(entry.get("path") or "")
        if strict_device_names and not re.fullmatch(r"/dev/(sd[a-z]+|mmcblk[0-9]+)", path):
            continue
        if not path:
            continue
        result.append(
            Disk(
                path=path,
                size=size,
                identity=str(entry.get("serial") or entry.get("wwn") or ""),
                model=str(entry.get("model") or "SD card").strip() or "SD card",
            )
        )
    return result


def windows_disks(
    data: Any,
    *,
    filter_attributes: bool = True,
    require_writable: bool = True,
) -> list[Disk]:
    """Filter parsed PowerShell ``Get-Disk`` JSON down to eligible removable disks."""
    if isinstance(data, dict):
        data = [data]
    result: list[Disk] = []
    for entry in data or []:
        if not isinstance(entry, dict):
            continue
        size = int(entry.get("Size") or 0)
        if size <= 0:
            # Skip empty card readers with no media inserted.
            continue
        if entry.get("IsBoot") is True or entry.get("IsSystem") is True:
            # Never expose OS boot or system disks, even when filter_attributes=False.
            continue
        if filter_attributes and not (
            entry.get("BusType") in ("USB", "SD")
            and entry.get("IsBoot") is False
            and entry.get("IsSystem") is False
            and (not require_writable or entry.get("IsReadOnly") is False)
        ):
            continue
        number = entry.get("Number")
        if number is None or isinstance(number, bool):
            continue
        result.append(
            Disk(
                path=r"\\.\PhysicalDrive" + str(int(number)),
                size=size,
                identity=str(entry.get("UniqueId") or entry.get("SerialNumber") or ""),
                model=str(entry.get("FriendlyName") or "SD card").strip() or "SD card",
            )
        )
    return result


def macos_disks(
    run_fn: Callable[..., bytes],
    *,
    require_writable: bool = True,
    exclude_apfs_boot: bool = True,
) -> list[Disk]:
    """Discover eligible external physical disks via macOS ``diskutil``."""
    devices = plistlib.loads(run_fn("diskutil", "list", "-plist", "external", "physical"))
    protected: set[str | None] = set()
    root_info = plistlib.loads(run_fn("diskutil", "info", "-plist", "/"))
    protected.update({root_info.get("ParentWholeDisk"), root_info.get("DeviceIdentifier")})
    if exclude_apfs_boot:
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
        if not re.fullmatch(r"disk[0-9]+", str(name)):
            continue
        info = plistlib.loads(run_fn("diskutil", "info", "-plist", "/dev/" + name))
        total_size = int(info.get("TotalSize") or 0)
        if total_size <= 0:
            continue
        if (
            info.get("Whole") is True
            and info.get("Internal") is False
            and (not require_writable or info.get("Writable") is True)
            and info.get("VirtualOrPhysical") != "Virtual"
        ):
            result.append(
                Disk(
                    path="/dev/" + name,
                    size=total_size,
                    identity=str(info.get("DeviceTreePath") or ""),
                    model=str(info.get("MediaName") or name).strip() or name,
                )
            )
    return result


def discover(
    *,
    require_writable: bool = True,
    require_safe_mounts: bool = True,
    strict_device_names: bool = True,
    exclude_apfs_boot: bool = True,
) -> list[Disk]:
    """Discover eligible removable external whole disks on Linux, Windows, or macOS."""
    system = platform.system()
    if system == "Linux":
        raw = run(
            "lsblk",
            "-J",
            "-b",
            "-p",
            "-o",
            "NAME,PATH,SIZE,TYPE,RM,RO,TRAN,SERIAL,WWN,MODEL,MOUNTPOINTS",
        )
        return linux_disks(
            json.loads(raw),
            require_writable=require_writable,
            require_safe_mounts=require_safe_mounts,
            strict_device_names=strict_device_names,
        )
    if system == "Windows":
        raw = powershell(
            "@(Get-Disk | Select-Object Number,Size,UniqueId,SerialNumber,FriendlyName,"
            "IsBoot,IsSystem,IsReadOnly,@{n='BusType';e={$_.BusType.ToString()}}) | "
            "ConvertTo-Json -Compress"
        )
        parsed = json.loads(raw) if raw.strip() else []
        return windows_disks(
            parsed,
            filter_attributes=True,
            require_writable=require_writable,
        )
    if system == "Darwin":
        return macos_disks(
            run,
            require_writable=require_writable,
            exclude_apfs_boot=exclude_apfs_boot,
        )
    raise RuntimeError(f"Unsupported host OS: {system}")


def select(path: str) -> Disk:
    """Resolve and validate *path* as an eligible writable external whole disk."""
    found = [d for d in discover() if d.path == path]
    if len(found) != 1:
        raise RuntimeError("Target is not an eligible external whole disk; refusing")
    disk = found[0]
    if not MIN_FLASH_CARD_BYTES <= disk.size <= MAX_FLASH_CARD_BYTES:
        raise RuntimeError("Card capacity must be between 8 and 512 GiB")
    return disk


def unmount(disk: Disk) -> None:
    """Unmount all mounted partitions of *disk* after confirming its identity."""
    if select(disk.path) != disk:
        raise RuntimeError("Disk identity changed")
    system = platform.system()
    if system == "Linux":
        tree = json.loads(run("lsblk", "-J", "-p", "-o", "NAME,PATH,MOUNTPOINTS", disk.path))

        def visit(node: dict[str, Any]) -> None:
            for child in node.get("children", []) or []:
                visit(child)
            if _node_mountpoints(node):
                run("umount", node["path"])

        visit(tree["blockdevices"][0])
    elif system == "Darwin":
        run("diskutil", "unmountDisk", disk.path)
    if select(disk.path) != disk:
        raise RuntimeError("Disk identity changed after unmount")


@contextmanager
def windows_volume_locks(disk: Disk):
    """Lock and dismount all Windows volumes on *disk* for raw sector writes."""
    import ctypes
    from ctypes import wintypes

    number = int(disk.path.removeprefix(r"\\.\PhysicalDrive"))
    raw = powershell(
        "@(Get-Partition -DiskNumber "
        + str(number)
        + " -ErrorAction SilentlyContinue"
        + " | Get-Volume -ErrorAction SilentlyContinue"
        + " | Where-Object { $_.Path }"
        + " | Select-Object -ExpandProperty Path)"
        " | ConvertTo-Json -Compress"
    )
    parsed = json.loads(raw) if raw.strip() else []
    if isinstance(parsed, str):
        parsed = [parsed]
    elif not isinstance(parsed, list):
        parsed = []
    paths = [item for item in parsed if isinstance(item, str) and item.strip()]
    kernel = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel.CreateFileW.argtypes = [
        wintypes.LPCWSTR,
        wintypes.DWORD,
        wintypes.DWORD,
        wintypes.LPVOID,
        wintypes.DWORD,
        wintypes.DWORD,
        wintypes.HANDLE,
    ]
    kernel.CreateFileW.restype = wintypes.HANDLE
    kernel.CloseHandle.argtypes = [wintypes.HANDLE]
    kernel.DeviceIoControl.argtypes = [
        wintypes.HANDLE,
        wintypes.DWORD,
        wintypes.LPVOID,
        wintypes.DWORD,
        wintypes.LPVOID,
        wintypes.DWORD,
        ctypes.POINTER(wintypes.DWORD),
        wintypes.LPVOID,
    ]
    handles = []
    try:
        for volume_path in paths:
            if not volume_path.startswith("\\\\?\\Volume{"):
                raise RuntimeError("Unexpected Windows volume path")
            handle = kernel.CreateFileW(volume_path.rstrip("\\"), 0xC0000000, 3, None, 3, 0, None)
            if handle == ctypes.c_void_p(-1).value:
                raise ctypes.WinError(ctypes.get_last_error())
            handles.append(handle)
            count = wintypes.DWORD()
            for code in (0x00090018, 0x00090020):  # FSCTL_LOCK_VOLUME, FSCTL_DISMOUNT_VOLUME
                if not kernel.DeviceIoControl(
                    handle, code, None, 0, None, 0, ctypes.byref(count), None
                ):
                    raise ctypes.WinError(ctypes.get_last_error())
        yield
    finally:
        for handle in reversed(handles):
            kernel.CloseHandle(handle)


def check_path_not_on_disk(disk_path: str, target_path: Path, *, message: str) -> None:
    """Reject *target_path* if its backing filesystem resides on *disk_path*."""
    system = platform.system()
    if system == "Linux":
        source = run("findmnt", "-n", "-o", "SOURCE", "--target", str(target_path)).decode().strip()
        if linux_source_on_disk(source, disk_path):
            raise RuntimeError(message)
    elif system == "Darwin":
        source = run("df", "-P", str(target_path)).decode().splitlines()[-1].split()[0]
        info = plistlib.loads(run("diskutil", "info", "-plist", source))
        if info.get("ParentWholeDisk") == disk_path.removeprefix("/dev/"):
            raise RuntimeError(message)
    elif system == "Windows":
        drive = target_path.drive
        if not re.fullmatch(r"[A-Za-z]:", drive):
            raise RuntimeError("Choose a local fixed drive")
        raw = powershell("(Get-Partition -DriveLetter " + drive[0] + ").DiskNumber")
        if int(raw.strip()) == int(disk_path.removeprefix(r"\\.\PhysicalDrive")):
            raise RuntimeError(message)


def check_source(disk: Disk, image: Path) -> None:
    """Reject an image stored on the card we are about to erase."""
    check_path_not_on_disk(disk.path, image, message="Image is stored on target disk")


def transfer(
    dst: BinaryIO,
    src: BinaryIO | None,
    size: int,
    progress: Callable[[int, int], None],
) -> None:
    """Write *size* bytes from *src* (or zeros when *src* is None) to *dst* and fsync."""
    done = 0
    while done < size:
        count = min(BLOCK, size - done)
        data = src.read(count) if src is not None else bytes(count)
        if len(data) != count:
            raise RuntimeError("Short input read")
        offset = 0
        while offset < count:
            written = dst.write(data[offset:])
            if not written:
                raise RuntimeError("Short disk write")
            offset += written
        done += count
        progress(done, size)
    dst.flush()
    os.fsync(dst.fileno())


def verify(
    stream: BinaryIO,
    size: int,
    expected: str | None,
    progress: Callable[[int, int], None],
) -> None:
    """Verify *size* bytes read from *stream* are all zero (None) or match *expected* SHA-256."""
    hasher = hashlib.sha256()
    done = 0
    while done < size:
        chunk = stream.read(min(BLOCK, size - done))
        if not chunk:
            raise RuntimeError("Short verification read")
        if expected is None and chunk != bytes(len(chunk)):
            raise RuntimeError("Erase verification failed")
        if expected is not None:
            hasher.update(chunk)
        done += len(chunk)
        progress(done, size)
    if expected is not None and hasher.hexdigest() != expected:
        raise RuntimeError("Image readback digest mismatch")


def write_card(
    path: str,
    flags: int,
    disk: Disk,
    image: Path | None,
    expected: str | None,
    *,
    quick: bool = False,
) -> None:
    """Erase and/or write *image* onto raw device *path* with full readback verification."""
    if quick and image is None:
        raise RuntimeError("Quick mode requires a system image")
    fd = os.open(path, flags)
    last = [-1]

    def progress(done: int, total: int) -> None:
        percent = done * 100 // total
        if percent != last[0]:
            print(f"{percent}%", flush=True)
            last[0] = percent

    with os.fdopen(fd, "r+b", buffering=0) as card:
        if not quick:
            print("Erasing every addressable sector...")
            transfer(card, None, disk.size, progress)
            card.seek(0)
            print("Verifying entire card is zero...")
            verify(card, disk.size, None, progress)
        if image is not None:
            if not expected:
                raise RuntimeError("Image SHA-256 is required for write verification")
            card.seek(0)
            print("Writing system image...")
            with image.open("rb") as src:
                transfer(card, src, image.stat().st_size, progress)
            card.seek(0)
            print("Verifying system image...")
            verify(card, image.stat().st_size, expected.lower(), progress)
    print("Verified. Safely remove the card. Do not accept OS format prompts.")


def operate(
    disk: Disk,
    image: str | Path | None = None,
    expected: str | None = None,
    *,
    quick: bool = False,
    fit_checker: Callable[[Path, int], None] | None = None,
) -> None:
    """Interactive confirmation, unmount/lock, raw erase/flash, and readback verification."""
    if quick and image is None:
        raise RuntimeError("Quick mode requires a system image")
    resolved_image: Path | None = None
    if image is not None:
        candidate = Path(image)
        if candidate.is_symlink():
            raise RuntimeError("Use an extracted regular .img file")
        resolved_image = candidate.resolve()
        check_source(disk, resolved_image)
        if not resolved_image.is_file() or resolved_image.suffix != ".img":
            raise RuntimeError("Use an extracted regular .img file")
        image_size = resolved_image.stat().st_size
        if image_size > disk.size or image_size < 1024**2:
            raise RuntimeError("Image size does not fit card")
        if not expected or sha256_file(resolved_image) != expected.lower():
            raise RuntimeError("Image SHA-256 mismatch")
        if fit_checker is not None:
            fit_checker(resolved_image, disk.size)
    print(json.dumps(asdict(disk), ensure_ascii=False))
    print(
        "Disk will be overwritten. Quick mode may leave old data outside the image."
        if quick
        else "ALL partitions and ALL data on this disk will be erased."
    )
    if input("Type ERASE " + disk.path + " to continue: ") != "ERASE " + disk.path:
        raise RuntimeError("Cancelled")
    unmount(disk)
    raw_path = (
        disk.path.replace("/dev/disk", "/dev/rdisk") if platform.system() == "Darwin" else disk.path
    )
    # Linux O_EXCL rejects a mounted device or another exclusive holder.
    flags = (
        os.O_RDWR | getattr(os, "O_BINARY", 0) | (os.O_EXCL if platform.system() == "Linux" else 0)
    )
    guard = windows_volume_locks(disk) if platform.system() == "Windows" else nullcontext()
    with guard:
        if select(disk.path) != disk:
            raise RuntimeError("Disk identity changed before write")
        write_card(raw_path, flags, disk, resolved_image, expected, quick=quick)

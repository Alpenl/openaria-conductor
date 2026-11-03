"""Conservative removable-disk discovery and verified raw writes."""

from __future__ import annotations

import hashlib
import json
import os
import platform
import plistlib
import re
import subprocess
from contextlib import contextmanager, nullcontext
from dataclasses import asdict, dataclass
from pathlib import Path

BLOCK = 4 * 1024 * 1024


def run(*args):
    return subprocess.check_output(args)


def powershell(script):
    return run("powershell.exe", "-NoProfile", "-NonInteractive", "-Command", script)


@dataclass(frozen=True)
class Disk:
    path: str
    size: int
    identity: str
    model: str


def linux_disks(data):
    result = []
    for d in data["blockdevices"]:
        # Fail closed for internal storage, RAID, unknown transports and partitions.
        if (
            d.get("type") != "disk"
            or int(d.get("size", 0)) == 0
            or not (d.get("rm") or d.get("tran") == "usb")
        ):
            continue
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
        if safe and not d.get("ro") and re.fullmatch(r"/dev/(sd[a-z]+|mmcblk[0-9]+)", d["path"]):
            result.append(
                Disk(
                    d["path"],
                    int(d["size"]),
                    str(d.get("serial") or d.get("wwn") or ""),
                    str(d.get("model") or "SD card"),
                )
            )
    return result


def windows_disks(data):
    if isinstance(data, dict):
        data = [data]
    return [
        Disk(
            r"\\.\PhysicalDrive" + str(int(d["Number"])),
            int(d["Size"]),
            str(d.get("UniqueId") or d.get("SerialNumber") or ""),
            str(d.get("FriendlyName") or "SD card"),
        )
        for d in (data or [])
        if d.get("BusType") in ("USB", "SD")
        and d.get("IsBoot") is False
        and d.get("IsSystem") is False
        and d.get("IsReadOnly") is False
    ]


def discover():
    system = platform.system()
    if system == "Linux":
        return linux_disks(
            json.loads(
                run(
                    "lsblk",
                    "-J",
                    "-b",
                    "-p",
                    "-o",
                    "NAME,PATH,SIZE,TYPE,RM,RO,TRAN,SERIAL,WWN,MODEL,MOUNTPOINTS",
                )
            )
        )
    if system == "Windows":
        return windows_disks(
            json.loads(
                powershell(
                    "Get-Disk | Select-Object Number,Size,UniqueId,SerialNumber,FriendlyName,"
                    "IsBoot,IsSystem,IsReadOnly,@{n='BusType';e={$_.BusType.ToString()}} | "
                    "ConvertTo-Json -Compress"
                )
            )
        )
    if system == "Darwin":
        devices = plistlib.loads(run("diskutil", "list", "-plist", "external", "physical"))
        result = []
        root_info = plistlib.loads(run("diskutil", "info", "-plist", "/"))
        protected = {root_info.get("ParentWholeDisk"), root_info.get("DeviceIdentifier")}
        # Reject all APFS physical stores, including external macOS boot containers.
        apfs = plistlib.loads(run("diskutil", "apfs", "list", "-plist"))
        for container in apfs.get("Containers", []):
            for store in container.get("PhysicalStores", []):
                info = plistlib.loads(run("diskutil", "info", "-plist", store["DeviceIdentifier"]))
                protected.add(info.get("ParentWholeDisk"))
        for name in devices["WholeDisks"]:
            if name in protected:
                continue
            if not re.fullmatch(r"disk[0-9]+", name):
                continue
            d = plistlib.loads(run("diskutil", "info", "-plist", "/dev/" + name))
            if (
                d.get("Whole") is True
                and d.get("Internal") is False
                and d.get("Writable") is True
                and d.get("VirtualOrPhysical") != "Virtual"
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
    raise RuntimeError("Only Windows, Linux and macOS are supported")


def select(path):
    found = [d for d in discover() if d.path == path]
    if len(found) != 1:
        raise RuntimeError("Target is not an eligible external whole disk; refusing")
    d = found[0]
    if not 8 * 1024**3 <= d.size <= 512 * 1024**3:
        raise RuntimeError("Card capacity must be between 8 and 512 GiB")
    return d


def unmount(disk):
    if select(disk.path) != disk:
        raise RuntimeError("Disk identity changed")
    system = platform.system()
    if system == "Linux":
        tree = json.loads(run("lsblk", "-J", "-p", "-o", "NAME,PATH,MOUNTPOINTS", disk.path))

        def visit(node):
            for child in node.get("children", []):
                visit(child)
            if any(node.get("mountpoints") or []):
                run("umount", node["path"])

        visit(tree["blockdevices"][0])
    elif system == "Darwin":
        run("diskutil", "unmountDisk", disk.path)
    if select(disk.path) != disk:
        raise RuntimeError("Disk identity changed after unmount")


@contextmanager
def windows_volume_locks(disk):
    import ctypes
    from ctypes import wintypes

    number = int(disk.path.removeprefix(r"\\.\PhysicalDrive"))
    raw = powershell(
        "$ErrorActionPreference='Stop'; @(Get-Partition -DiskNumber "
        + str(number)
        + " | Get-Volume | Select-Object -ExpandProperty Path)"
        " | ConvertTo-Json -Compress"
    )
    paths = json.loads(raw) if raw.strip() else []
    if isinstance(paths, str):
        paths = [paths]
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
        for path in paths:
            if not path.startswith("\\\\?\\Volume{"):
                raise RuntimeError("Unexpected Windows volume path")
            handle = kernel.CreateFileW(path.rstrip("\\"), 0xC0000000, 3, None, 3, 0, None)
            if handle == ctypes.c_void_p(-1).value:
                raise ctypes.WinError(ctypes.get_last_error())
            handles.append(handle)
            count = wintypes.DWORD()
            for code in (0x00090018, 0x00090020):  # LOCK_VOLUME, DISMOUNT_VOLUME
                if not kernel.DeviceIoControl(
                    handle, code, None, 0, None, 0, ctypes.byref(count), None
                ):
                    raise ctypes.WinError(ctypes.get_last_error())
        yield
    finally:
        for handle in reversed(handles):
            kernel.CloseHandle(handle)


def digest(path):
    h = hashlib.sha256()
    with Path(path).open("rb") as f:
        for b in iter(lambda: f.read(BLOCK), b""):
            h.update(b)
    return h.hexdigest()


def transfer(dst, src, size, progress):
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


def verify(stream, size, expected, progress):
    h = hashlib.sha256()
    done = 0
    while done < size:
        b = stream.read(min(BLOCK, size - done))
        if not b:
            raise RuntimeError("Short verification read")
        if expected is None and b != bytes(len(b)):
            raise RuntimeError("Erase verification failed")
        if expected is not None:
            h.update(b)
        done += len(b)
        progress(done, size)
    if expected is not None and h.hexdigest() != expected:
        raise RuntimeError("Image readback digest mismatch")


def check_source(disk, image):
    """Reject an image stored on the card we are about to erase."""
    system = platform.system()
    if system == "Linux":
        source = run("findmnt", "-n", "-o", "SOURCE", "--target", str(image)).decode().strip()
        if source.startswith(disk.path):
            raise RuntimeError("Image is stored on target disk")
    elif system == "Darwin":
        source = run("df", "-P", str(image)).decode().splitlines()[-1].split()[0]
        info = plistlib.loads(run("diskutil", "info", "-plist", source))
        if info.get("ParentWholeDisk") == disk.path.removeprefix("/dev/"):
            raise RuntimeError("Image is stored on target disk")
    elif system == "Windows":
        drive = image.drive
        if not re.fullmatch(r"[A-Za-z]:", drive):
            raise RuntimeError("Use an image on a local fixed drive")
        raw = powershell(
            "$ErrorActionPreference='Stop'; (Get-Partition -DriveLetter "
            + drive[0]
            + ").DiskNumber"
        )
        if int(raw.strip()) == int(disk.path.removeprefix(r"\\.\PhysicalDrive")):
            raise RuntimeError("Image is stored on target disk")


def operate(disk, image=None, expected=None, *, quick=False):
    if quick and image is None:
        raise RuntimeError("Quick mode requires a system image")
    if image is not None:
        image = Path(image).resolve()
        check_source(disk, image)
        if not image.is_file() or image.is_symlink() or image.suffix != ".img":
            raise RuntimeError("Use an extracted regular .img file")
        if image.stat().st_size > disk.size or image.stat().st_size < 1024**2:
            raise RuntimeError("Image size does not fit card")
        if not expected or digest(image) != expected.lower():
            raise RuntimeError("Image SHA-256 mismatch")
    print(json.dumps(asdict(disk), ensure_ascii=False))
    print(
        "Disk will be overwritten. Quick mode may leave old data outside the image."
        if quick
        else "ALL partitions and ALL data on this disk will be erased."
    )
    if input("Type ERASE " + disk.path + " to continue: ") != "ERASE " + disk.path:
        raise RuntimeError("Cancelled")
    unmount(disk)
    path = (
        disk.path.replace("/dev/disk", "/dev/rdisk") if platform.system() == "Darwin" else disk.path
    )
    # Linux exclusive block-device open rejects a mounted device or another holder.
    flags = (
        os.O_RDWR | getattr(os, "O_BINARY", 0) | (os.O_EXCL if platform.system() == "Linux" else 0)
    )
    guard = windows_volume_locks(disk) if platform.system() == "Windows" else nullcontext()
    with guard:
        if select(disk.path) != disk:
            raise RuntimeError("Disk identity changed before write")
        write_card(path, flags, disk, image, expected, quick=quick)


def write_card(path, flags, disk, image, expected, *, quick=False):
    if quick and image is None:
        raise RuntimeError("Quick mode requires a system image")
    fd = os.open(path, flags)
    last = [-1]

    def progress(done, total):
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
            card.seek(0)
            print("Writing system image...")
            with image.open("rb") as src:
                transfer(card, src, image.stat().st_size, progress)
            card.seek(0)
            print("Verifying system image...")
            verify(card, image.stat().st_size, expected.lower(), progress)
    print("Verified. Safely remove the card. Do not accept OS format prompts.")

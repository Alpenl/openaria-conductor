#!/usr/bin/env python3
"""Read-only OpenAria card export. No filesystem drivers or card writes."""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import platform
import plistlib
import re
import shutil
import stat
import struct
import subprocess
import sys
import uuid
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path

from dissect.extfs.extfs import ExtFS
from dissect.util.stream import BufferedStream
from dissect.volume.disk import Disk

try:
    if __package__:
        from .. import _host_disks
    else:
        for _candidate in (
            Path(__file__).resolve().parent,
            Path(__file__).resolve().parents[1],
        ):
            if (_candidate / "_host_disks.py").is_file() and str(_candidate) not in sys.path:
                sys.path.insert(0, str(_candidate))
        import _host_disks
except ImportError:

    class _FallbackHostDisks:
        """Standalone fallback when card_export is distributed without scripts/_host_disks.py."""

        @staticmethod
        def linux_source_on_disk(source: str, disk_path: str) -> bool:
            backing = source.strip().split("[", 1)[0]
            return re.fullmatch(rf"{re.escape(disk_path)}(p?\d+)?", backing) is not None

        @staticmethod
        def linux_disks(data, **_kwargs):
            from types import SimpleNamespace

            disks = []
            for d in data.get("blockdevices", []):
                if (
                    d.get("type") != "disk"
                    or int(d.get("size") or 0) <= 0
                    or not (d.get("rm") or d.get("tran") == "usb")
                ):
                    continue
                disks.append(
                    SimpleNamespace(
                        path=d.get("path", ""),
                        size=int(d["size"]),
                        model=str(d.get("model") or "SD card"),
                    )
                )
            return disks

        @staticmethod
        def windows_disks(data, **_kwargs):
            from types import SimpleNamespace

            if isinstance(data, dict):
                data = [data]
            disks = []
            for d in data or []:
                size = int(d.get("Size") or 0)
                if size <= 0:
                    continue
                disks.append(
                    SimpleNamespace(
                        path=r"\\.\PhysicalDrive" + str(int(d["Number"])),
                        size=size,
                        model=str(d.get("FriendlyName") or "SD card"),
                    )
                )
            return disks

        @staticmethod
        def macos_disks(run_fn, **_kwargs):
            from types import SimpleNamespace

            devices = plistlib.loads(run_fn("diskutil", "list", "-plist", "external", "physical"))
            disks = []
            for name in devices.get("WholeDisks", []):
                d = plistlib.loads(run_fn("diskutil", "info", "-plist", "/dev/" + name))
                if d.get("Whole") is True and d.get("Internal") is False:
                    disks.append(
                        SimpleNamespace(
                            path="/dev/" + name,
                            size=int(d["TotalSize"]),
                            model=str(d.get("MediaName") or name),
                        )
                    )
            return disks

    _host_disks = _FallbackHostDisks()  # type: ignore[assignment]

BLOCK = 4 * 1024**2
MAX_EXFAT_DIR_BYTES = 64 * 1024 * 1024
LOOP = "/var/lib/rp-ylx-data-volume/data-volume.img"
DAY_DIR_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def command(*args):
    return subprocess.check_output(args)


def ps(script):
    return command(
        "powershell.exe",
        "-NoProfile",
        "-NonInteractive",
        "-Command",
        "$ErrorActionPreference='Stop'; " + script,
    )


def discover():
    system = platform.system()
    if system == "Linux":
        data = json.loads(command("lsblk", "-Jbp", "-o", "PATH,SIZE,TYPE,TRAN,RM,MODEL"))
        disks = _host_disks.linux_disks(
            data,
            require_writable=False,
            require_safe_mounts=False,
            strict_device_names=False,
        )
        return [{"path": d.path, "size": d.size, "model": d.model} for d in disks]
    if system == "Windows":
        data = json.loads(
            ps(
                "@(Get-Disk | Where-Object {($_.BusType -in 'USB','SD') -and "
                "!$_.IsBoot -and !$_.IsSystem} | Select-Object Number,Size,"
                "FriendlyName) | ConvertTo-Json -Compress"
            )
        )
        disks = _host_disks.windows_disks(
            data,
            filter_attributes=False,
            require_writable=False,
        )
        return [{"path": d.path, "size": d.size, "model": d.model} for d in disks]
    if system == "Darwin":
        disks = _host_disks.macos_disks(
            command,
            require_writable=False,
            exclude_apfs_boot=False,
        )
        return [{"path": d.path, "size": d.size, "model": d.model} for d in disks]
    raise RuntimeError("Unsupported host OS")


def safe_name(name):
    if (
        not name
        or name in {".", ".."}
        or name.endswith((".", " "))
        or re.search(r'[<>:"/\\|?*\x00-\x1f\ud800-\udfff]', name)
        or name.split(".")[0].upper()
        in {
            "CON",
            "PRN",
            "AUX",
            "NUL",
            *(f"COM{i}" for i in range(1, 10)),
            *(f"LPT{i}" for i in range(1, 10)),
        }
    ):
        raise RuntimeError("Filename cannot be safely exported: " + repr(name))
    return name


def get_optional(fs, path):
    try:
        return fs.get(path)
    except FileNotFoundError:
        return None


def is_ext(stream):
    stream.seek(1080)
    return stream.read(2) == b"\x53\xef"


def is_exfat(stream):
    stream.seek(0)
    boot = stream.read(512)
    return len(boot) == 512 and boot[3:11] == b"EXFAT   " and boot[510:512] == b"\x55\xaa"


def dirty(fs):
    return not (fs.sb.s_state & 1) or bool(fs.sb.s_feature_incompat & 4)


class _ExfatFileStream:
    """Bounded read-only stream over an exFAT cluster chain."""

    def __init__(self, stream, cluster_offsets, cluster_size, size, valid_size=None):
        self._stream = stream
        self._cluster_offsets = cluster_offsets
        self._cluster_size = cluster_size
        self._size = size
        self._valid_size = size if valid_size is None else min(valid_size, size)
        self._pos = 0

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return None

    def read(self, count=-1):
        if self._pos >= self._size:
            return b""
        remaining = (
            self._size - self._pos
            if count is None or count < 0
            else min(count, self._size - self._pos)
        )
        chunks = []
        while remaining > 0:
            if self._pos < self._valid_size:
                cluster_index, intra = divmod(self._pos, self._cluster_size)
                if cluster_index >= len(self._cluster_offsets):
                    break
                take = min(remaining, self._valid_size - self._pos, self._cluster_size - intra)
                self._stream.seek(self._cluster_offsets[cluster_index] + intra)
                chunk = self._stream.read(take)
                if not chunk:
                    break
                chunks.append(chunk)
                self._pos += len(chunk)
                remaining -= len(chunk)
                if len(chunk) < take:
                    break
            else:
                zero_len = min(remaining, self._size - self._pos)
                chunks.append(b"\x00" * zero_len)
                self._pos += zero_len
                remaining -= zero_len
        return b"".join(chunks)


class _ExfatNode:
    def __init__(
        self,
        fs,
        filename,
        *,
        filetype,
        first_cluster,
        size,
        valid_size=None,
        no_fat_chain,
        inum,
    ):
        self._fs = fs
        self.filename = filename
        self.filetype = filetype
        self._first_cluster = first_cluster
        self.size = size
        self._valid_size = size if valid_size is None else min(valid_size, size)
        self._no_fat_chain = no_fat_chain
        self.inum = inum

    def iterdir(self):
        if self.filetype != stat.S_IFDIR:
            return []
        return self._fs._read_directory(
            self._first_cluster,
            self.size if self._no_fat_chain else None,
            self._no_fat_chain,
        )

    def open(self):
        if self.filetype != stat.S_IFREG:
            raise RuntimeError("Cannot open non-regular exFAT entry")
        offsets = self._fs._cluster_offsets(
            self._first_cluster, self._valid_size, self._no_fat_chain
        )
        return _ExfatFileStream(
            self._fs._stream, offsets, self._fs._cluster_size, self.size, self._valid_size
        )


class _ExfatFS:
    """Minimal read-only pure-Python exFAT reader for card export."""

    def __init__(self, stream):
        self._stream = stream
        stream.seek(0)
        vbr = stream.read(512)
        if len(vbr) != 512 or vbr[3:11] != b"EXFAT   " or vbr[510:512] != b"\x55\xaa":
            raise RuntimeError("Invalid exFAT boot sector")
        self._fat_offset = struct.unpack_from("<I", vbr, 80)[0]
        self._fat_length = struct.unpack_from("<I", vbr, 84)[0]
        self._cluster_heap_offset = struct.unpack_from("<I", vbr, 88)[0]
        self._cluster_count = struct.unpack_from("<I", vbr, 92)[0]
        self._root_cluster = struct.unpack_from("<I", vbr, 96)[0]
        volume_flags = struct.unpack_from("<H", vbr, 106)[0]
        bps_shift = vbr[108]
        spc_shift = vbr[109]
        number_of_fats = vbr[110]
        if not (9 <= bps_shift <= 12) or bps_shift + spc_shift > 25:
            raise RuntimeError("Unsupported exFAT geometry")
        if (
            self._cluster_count == 0
            or self._fat_length == 0
            or self._root_cluster < 2
            or self._root_cluster >= self._cluster_count + 2
        ):
            raise RuntimeError("Invalid exFAT geometry")
        if number_of_fats not in (1, 2):
            raise RuntimeError("Unsupported exFAT FAT count")
        active_fat = (volume_flags & 0x0001) if number_of_fats == 2 else 0
        self._active_fat_offset = self._fat_offset + active_fat * self._fat_length
        self._sector_size = 1 << bps_shift
        self._cluster_size = self._sector_size << spc_shift
        self.dirty = bool(volume_flags & 0x0002)
        self._root = _ExfatNode(
            self,
            "",
            filetype=stat.S_IFDIR,
            first_cluster=self._root_cluster,
            size=0,
            no_fat_chain=False,
            inum=(self._root_cluster, 0),
        )

    def _cluster_offset(self, cluster: int) -> int:
        if cluster < 2 or cluster >= self._cluster_count + 2:
            raise RuntimeError(f"exFAT cluster out of bounds: {cluster}")
        return (self._cluster_heap_offset * self._sector_size) + (
            (cluster - 2) * self._cluster_size
        )

    def _cluster_offsets(
        self, first_cluster: int, data_length: int | None, no_fat_chain: bool
    ) -> list[int]:
        if first_cluster < 2:
            return []
        max_clusters = (
            (data_length + self._cluster_size - 1) // self._cluster_size
            if data_length is not None
            else self._cluster_count
        )
        if max_clusters == 0:
            return []
        if no_fat_chain:
            return [self._cluster_offset(first_cluster + i) for i in range(max_clusters)]
        fat_base = self._active_fat_offset * self._sector_size
        fat_bytes = self._fat_length * self._sector_size
        offsets: list[int] = []
        seen: set[int] = set()
        cluster = first_cluster
        while 2 <= cluster < 0xFFFFFFF8 and len(offsets) < max_clusters:
            if cluster in seen:
                raise RuntimeError("Cyclic exFAT cluster chain")
            seen.add(cluster)
            offsets.append(self._cluster_offset(cluster))
            fat_pos = cluster * 4
            if fat_pos + 4 > fat_bytes:
                raise RuntimeError("exFAT FAT lookup out of bounds")
            self._stream.seek(fat_base + fat_pos)
            entry_raw = self._stream.read(4)
            if len(entry_raw) != 4:
                raise RuntimeError("Truncated exFAT FAT entry")
            cluster = struct.unpack_from("<I", entry_raw, 0)[0]
        return offsets

    def _read_directory(
        self, first_cluster: int, data_length: int | None, no_fat_chain: bool
    ) -> list[_ExfatNode]:
        if data_length is not None and data_length > MAX_EXFAT_DIR_BYTES:
            raise RuntimeError("exFAT directory exceeds memory limit")
        offsets = self._cluster_offsets(first_cluster, data_length, no_fat_chain)
        if len(offsets) * self._cluster_size > MAX_EXFAT_DIR_BYTES:
            raise RuntimeError("exFAT directory exceeds memory limit")
        raw_chunks = []
        for offset in offsets:
            self._stream.seek(offset)
            raw_chunks.append(self._stream.read(self._cluster_size))
        raw = b"".join(raw_chunks)
        entries: list[_ExfatNode] = []
        index = 0
        total_slots = len(raw) // 32
        while index < total_slots:
            entry_offset = index * 32
            entry_type = raw[entry_offset]
            if entry_type == 0x00:
                break
            if entry_type != 0x85:
                index += 1
                continue
            secondary_count = raw[entry_offset + 1]
            if secondary_count < 2 or index + secondary_count >= total_slots:
                raise RuntimeError("Corrupt exFAT file directory entry")
            attrs = struct.unpack_from("<H", raw, entry_offset + 4)[0]
            stream_offset = (index + 1) * 32
            if raw[stream_offset] != 0xC0:
                raise RuntimeError("Missing exFAT stream extension entry")
            stream_flags = raw[stream_offset + 1]
            name_len = raw[stream_offset + 3]
            valid_data_length = struct.unpack_from("<Q", raw, stream_offset + 8)[0]
            child_cluster = struct.unpack_from("<I", raw, stream_offset + 20)[0]
            child_size = struct.unpack_from("<Q", raw, stream_offset + 24)[0]
            name_bytes = bytearray()
            for slot in range(index + 2, index + 1 + secondary_count):
                slot_offset = slot * 32
                slot_type = raw[slot_offset]
                if slot_type == 0xC1:
                    name_bytes.extend(raw[slot_offset + 2 : slot_offset + 32])
                elif 0xE0 <= slot_type <= 0xFF:
                    continue
                else:
                    raise RuntimeError("Unexpected exFAT secondary entry")
            if name_len == 0 or len(name_bytes) < name_len * 2:
                raise RuntimeError("Corrupt exFAT filename length")
            filename = name_bytes[: name_len * 2].decode("utf-16-le", errors="strict")
            filetype = stat.S_IFDIR if attrs & 0x10 else stat.S_IFREG
            valid_size = (
                child_size if filetype == stat.S_IFDIR else min(valid_data_length, child_size)
            )
            entries.append(
                _ExfatNode(
                    self,
                    filename,
                    filetype=filetype,
                    first_cluster=child_cluster,
                    size=child_size,
                    valid_size=valid_size,
                    no_fat_chain=bool(stream_flags & 0x02),
                    inum=(first_cluster, index, child_cluster),
                )
            )
            index += 1 + secondary_count
        return entries

    def get(self, path: str) -> _ExfatNode:
        parts = [part for part in path.split("/") if part]
        node = self._root
        for part in parts:
            if node.filetype != stat.S_IFDIR:
                raise FileNotFoundError(path)
            children = node.iterdir()
            match = next((c for c in children if c.filename == part), None)
            if match is None:
                folded = part.casefold()
                match = next((c for c in children if c.filename.casefold() == folded), None)
            if match is None:
                raise FileNotFoundError(path)
            node = match
        return node


class CorruptionError(logging.Handler):
    def emit(self, record):
        raise RuntimeError("Filesystem parser reported damaged directory metadata")


@contextmanager
def volumes(source):
    handler = CorruptionError(level=logging.ERROR)
    logger = logging.getLogger("dissect.extfs.extfs")
    logger.addHandler(handler)
    try:
        with read_volumes(source) as found:
            yield found
    finally:
        logger.removeHandler(handler)


@contextmanager
def read_volumes(source):
    # Always rb, including raw physical disks. No mount, repair, journal replay or writes.
    with open(source, "rb", buffering=0) as handle:
        raw = BufferedStream(handle, align=4096)
        standalone = is_ext(raw) or is_exfat(raw)
        raw.seek(0)
        streams = (
            [("filesystem", raw)]
            if standalone
            else [(f"partition-{p.number}", p.open()) for p in Disk(raw).partitions]
        )
        found = []
        for label, stream in streams:
            if is_exfat(stream):
                fs = _ExfatFS(stream)
                for path in ("/recordings", "/sessions", "/data/recordings", "/data/sessions"):
                    entry = get_optional(fs, path)
                    if entry is not None and entry.filetype == stat.S_IFDIR:
                        found.append((label, fs, fs.dirty))
                        break
                continue
            if not is_ext(stream):
                continue
            fs = ExtFS(stream)
            nested = get_optional(fs, LOOP)
            if nested is not None:
                if nested.filetype != stat.S_IFREG:
                    raise RuntimeError("Recording volume image is not a regular file")
                # RunlistStream unwraps another RunlistStream; retain the outer
                # filesystem mapping through a distinct buffered stream layer.
                inner = ExtFS(BufferedStream(nested.open(), size=nested.size, align=4096))
                found.append((label + "-recording-volume", inner, dirty(fs) or dirty(inner)))
            for path in ("/recordings", "/sessions", "/data/recordings", "/data/sessions"):
                entry = get_optional(fs, path)
                if entry is not None and entry.filetype == stat.S_IFDIR:
                    found.append((label, fs, dirty(fs)))
                    break
        yield found


def _append_recording(result, label, fs, unclean, root, parts, node):
    manifest = get_optional(fs, "/".join([root, *parts, "manifest.json"]))
    result.append(
        {
            "id": f"{label}{root}/{'/'.join(parts)}",
            "node": node,
            "dirty": unclean,
            "sealed": manifest is not None,
            "prefix": Path(label, root.strip("/"), *parts),
        }
    )


def recordings(found):
    result = []
    for label, fs, unclean in found:
        for root in ("/recordings", "/sessions", "/data/recordings", "/data/sessions"):
            node = get_optional(fs, root)
            if node is None or node.filetype != stat.S_IFDIR:
                continue
            for child in node.iterdir():
                if child.filename in {".", ".."} or child.filetype != stat.S_IFDIR:
                    continue
                name = safe_name(child.filename)
                if (
                    DAY_DIR_RE.fullmatch(name) is not None or name == "录制时间未知"
                ) and get_optional(fs, f"{root}/{name}/manifest.json") is None:
                    for grandchild in child.iterdir():
                        if (
                            grandchild.filename in {".", ".."}
                            or grandchild.filetype != stat.S_IFDIR
                        ):
                            continue
                        subname = safe_name(grandchild.filename)
                        _append_recording(
                            result, label, fs, unclean, root, (name, subname), grandchild
                        )
                    continue
                _append_recording(result, label, fs, unclean, root, (name,), child)
    return result


def walk(node, relative=Path(), ancestors=frozenset()):
    key = node.inum
    if key in ancestors or len(ancestors) > 32:
        raise RuntimeError("Cyclic or excessively deep recording directory")
    for child in node.iterdir():
        if child.filename in {".", ".."}:
            continue
        path = relative / safe_name(child.filename)
        if child.filetype == stat.S_IFDIR:
            yield from walk(child, path, ancestors | {key})
        elif child.filetype == stat.S_IFREG:
            yield path, child
        else:
            raise RuntimeError("Symlink or special file in recording: " + str(path))


def destination_check(source, destination):
    destination = Path(destination).resolve(strict=True)
    if not destination.is_dir():
        raise RuntimeError("Destination must be an existing directory on the computer")
    if Path(source).is_file():
        return destination
    system = platform.system()
    if system == "Linux":
        backing = (
            command("findmnt", "-n", "-o", "SOURCE", "--target", str(destination)).decode().strip()
        )
        if _host_disks.linux_source_on_disk(backing, source):
            raise RuntimeError("Do not export onto the source card")
    elif system == "Darwin":
        backing = command("df", "-P", str(destination)).decode().splitlines()[-1].split()[0]
        info = plistlib.loads(command("diskutil", "info", "-plist", backing))
        if "/dev/" + info.get("ParentWholeDisk", "") == source:
            raise RuntimeError("Do not export onto the source card")
    elif system == "Windows":
        drive = destination.drive
        if not re.fullmatch(r"[A-Za-z]:", drive):
            raise RuntimeError("Choose a local computer drive")
        number = ps("(Get-Partition -DriveLetter " + drive[0] + ").DiskNumber").strip()
        if source == r"\\.\PhysicalDrive" + number.decode():
            raise RuntimeError("Do not export onto the source card")
    return destination


def _invoking_sudo_ids() -> tuple[int, int] | None:
    if os.name != "posix" or os.geteuid() != 0:
        return None
    uid_raw = os.environ.get("SUDO_UID")
    gid_raw = os.environ.get("SUDO_GID")
    if not uid_raw or not gid_raw:
        return None
    try:
        uid = int(uid_raw)
        gid = int(gid_raw)
    except ValueError:
        return None
    return (uid, gid) if uid >= 0 and gid >= 0 else None


def _apply_export_permissions(
    path: Path, sudo_ids: tuple[int, int] | None, *, is_dir: bool
) -> None:
    if os.name != "posix":
        return
    path.chmod(0o755 if is_dir else 0o644)
    if sudo_ids is not None:
        os.chown(path, sudo_ids[0], sudo_ids[1])


def _ensure_export_dir(directory: Path, root: Path, sudo_ids: tuple[int, int] | None) -> None:
    missing: list[Path] = []
    cursor = directory
    while cursor != root and not cursor.exists():
        missing.append(cursor)
        cursor = cursor.parent
    directory.mkdir(parents=True, exist_ok=True)
    for created in reversed(missing):
        _apply_export_permissions(created, sudo_ids, is_dir=True)


def export_selected(selected, destination, *, allow_dirty=False):
    if any(r["dirty"] for r in selected) and not allow_dirty:
        raise RuntimeError(
            "Filesystem was not cleanly shut down. Power down normally first, "
            "or explicitly use --allow-dirty for best-effort read-only export."
        )
    files, names = [], set()
    for record in selected:
        for path, node in walk(record["node"]):
            path = record["prefix"] / path
            normalized = str(path).casefold()
            if normalized in names:
                raise RuntimeError("Case-insensitive filename collision")
            names.add(normalized)
            files.append((path, node))
            if len(files) > 1000000:
                raise RuntimeError("Too many files")
    total = sum(n.size for _, n in files)
    if not files:
        raise RuntimeError("No recording files selected")
    if shutil.disk_usage(destination).free < total + 64 * 1024**2:
        raise RuntimeError("Insufficient destination space")
    output = Path(destination) / (
        "OpenAria-export-" + datetime.now().strftime("%Y%m%d-%H%M%S-") + uuid.uuid4().hex[:8]
    )
    sudo_ids = _invoking_sudo_ids()
    output.mkdir(mode=0o755)
    _apply_export_permissions(output, sudo_ids, is_dir=True)
    report = {
        "status": "incomplete",
        "source_dirty": any(r["dirty"] for r in selected),
        "recordings": [r["id"] for r in selected],
        "files": [],
    }
    try:
        done = 0
        for index, (path, node) in enumerate(files, 1):
            target = output / path
            _ensure_export_dir(target.parent, output, sudo_ids)
            temporary = target.with_name(target.name + ".export-partial")
            digest = hashlib.sha256()
            remaining = node.size
            with node.open() as src, temporary.open("xb") as dst:
                while remaining:
                    chunk = src.read(min(BLOCK, remaining))
                    if not chunk:
                        raise RuntimeError("Short source read: " + str(path))
                    dst.write(chunk)
                    digest.update(chunk)
                    remaining -= len(chunk)
                dst.flush()
                os.fsync(dst.fileno())
            copied = hashlib.sha256()
            with temporary.open("rb") as src:
                for chunk in iter(lambda: src.read(BLOCK), b""):
                    copied.update(chunk)
            if copied.digest() != digest.digest():
                raise RuntimeError("Destination verification failed: " + str(path))
            os.replace(temporary, target)
            _apply_export_permissions(target, sudo_ids, is_dir=False)
            report["files"].append(
                {"path": path.as_posix(), "bytes": node.size, "sha256": digest.hexdigest()}
            )
            done += node.size
            print(f"已导出并校验 {index}/{len(files)} 文件，{done}/{total} 字节", flush=True)
        report["status"] = "complete"
    finally:
        report_path = output / "export-report.json"
        with report_path.open("x", encoding="utf-8") as dst:
            json.dump(report, dst, ensure_ascii=True, indent=2)
            dst.flush()
            os.fsync(dst.fileno())
        _apply_export_permissions(report_path, sudo_ids, is_dir=False)
        print("导出目录：" + str(output), flush=True)
    return output


def main():
    parser = argparse.ArgumentParser(description="OpenAria 读卡器录像导出（全程只读源卡）")
    parser.add_argument("--source", help="整盘设备路径或离线.img镜像")
    parser.add_argument("--destination", type=Path, help="电脑上已存在的目标目录")
    parser.add_argument("--list", action="store_true", help="只列出录像")
    parser.add_argument("--recording", action="append", help="录像ID，可重复；默认全部")
    parser.add_argument(
        "--allow-dirty", action="store_true", help="允许未正常卸载文件系统的尽力读取"
    )
    args = parser.parse_args()
    interactive = args.source is None
    if interactive:
        devices = discover()
        if not devices:
            raise RuntimeError("未发现内存卡，请插入读卡器后重试")
        for i, d in enumerate(devices, 1):
            print(f"{i}. {d['path']}  {d['size'] / 1024**3:.1f} GiB  {d['model']}")
        index = int(input("选择内存卡编号："))
        if not 1 <= index <= len(devices):
            raise RuntimeError("编号无效")
        args.source = devices[index - 1]["path"]
    if not Path(args.source).is_file() and args.source not in {d["path"] for d in discover()}:
        raise RuntimeError("Source is not an eligible external card or regular image")
    with volumes(args.source) as found:
        records = recordings(found)
        for i, item in enumerate(records, 1):
            print(
                f"{i}. {item['id']}  "
                + ("有清单" if item["sealed"] else "未封存/无清单")
                + ("  [文件系统未正常卸载]" if item["dirty"] else "")
            )
        if not records:
            raise RuntimeError("未发现录像。可能是空白系统卡、存储损坏或未支持的存储布局")
        if args.list:
            return
        if interactive:
            selection = input("导出编号（空格分隔，回车导出全部）：").strip()
            if selection:
                indices = [int(x) for x in selection.split()]
                if any(i < 1 or i > len(records) for i in indices):
                    raise RuntimeError("录像编号无效")
                records = [records[i - 1] for i in dict.fromkeys(indices)]
        elif args.recording:
            unknown = set(args.recording) - {r["id"] for r in records}
            if unknown:
                raise RuntimeError("Unknown recording ID")
            records = [r for r in records if r["id"] in args.recording]
        if args.destination is None:
            if not interactive:
                parser.error("export requires --destination")
            args.destination = Path(input("电脑上的导出目录：").strip().strip('"'))
        destination = destination_check(args.source, args.destination)
        export_selected(records, destination, allow_dirty=args.allow_dirty)
        print("导出完成，复制内容已校验；源卡未修改。")


if __name__ == "__main__":
    try:
        main()
    except (Exception, KeyboardInterrupt) as error:
        print("导出未完成：" + str(error), file=sys.stderr)
        raise SystemExit(2) from None

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
import subprocess
import sys
import uuid
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path

from dissect.extfs.extfs import ExtFS
from dissect.util.stream import BufferedStream
from dissect.volume.disk import Disk

BLOCK = 4 * 1024**2
LOOP = "/var/lib/rp-ylx-data-volume/data-volume.img"


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
        return [
            {"path": d["path"], "size": int(d["size"]), "model": d.get("model", "")}
            for d in data["blockdevices"]
            if d["type"] == "disk"
            and int(d["size"]) > 0
            and (d.get("tran") == "usb" or d.get("rm"))
        ]
    if system == "Windows":
        data = json.loads(
            ps(
                "@(Get-Disk | Where-Object {($_.BusType -in 'USB','SD') -and "
                "!$_.IsBoot -and !$_.IsSystem} | Select-Object Number,Size,"
                "FriendlyName) | ConvertTo-Json -Compress"
            )
        )
        if isinstance(data, dict):
            data = [data]
        return [
            {
                "path": r"\\.\PhysicalDrive" + str(d["Number"]),
                "size": int(d["Size"]),
                "model": d["FriendlyName"],
            }
            for d in (data or [])
            if int(d["Size"]) > 0
        ]
    if system == "Darwin":
        data = plistlib.loads(command("diskutil", "list", "-plist", "external", "physical"))
        result = []
        for name in data["WholeDisks"]:
            info = plistlib.loads(command("diskutil", "info", "-plist", "/dev/" + name))
            if info.get("Whole") and not info.get("Internal"):
                result.append(
                    {
                        "path": "/dev/" + name,
                        "size": info["TotalSize"],
                        "model": info.get("MediaName", "SD card"),
                    }
                )
        return result
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


def dirty(fs):
    return not (fs.sb.s_state & 1) or bool(fs.sb.s_feature_incompat & 4)


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
        standalone = is_ext(raw)
        raw.seek(0)
        streams = (
            [("filesystem", raw)]
            if standalone
            else [(f"partition-{p.number}", p.open()) for p in Disk(raw).partitions]
        )
        found = []
        for label, stream in streams:
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
                manifest = get_optional(fs, root + "/" + name + "/manifest.json")
                result.append(
                    {
                        "id": f"{label}{root}/{name}",
                        "node": child,
                        "dirty": unclean,
                        "sealed": manifest is not None,
                        "prefix": Path(label) / root.strip("/") / name,
                    }
                )
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
        if backing.startswith(source):
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
    output.mkdir(mode=0o700)
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
            target.parent.mkdir(parents=True, exist_ok=True)
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
            report["files"].append(
                {"path": path.as_posix(), "bytes": node.size, "sha256": digest.hexdigest()}
            )
            done += node.size
            print(f"已导出并校验 {index}/{len(files)} 文件，{done}/{total} 字节", flush=True)
        report["status"] = "complete"
    finally:
        with (output / "export-report.json").open("x", encoding="utf-8") as dst:
            json.dump(report, dst, ensure_ascii=True, indent=2)
            dst.flush()
            os.fsync(dst.fileno())
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

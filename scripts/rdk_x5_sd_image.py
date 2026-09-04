"""Build an offline RDK X5 SD image with a dedicated Open Aria /data partition.

The tool intentionally writes only regular image files. It never mutates a block
device, mounted filesystem, or the running rootfs on an RDK X5.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import tempfile
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import TextIO

SECTOR_SIZE = 512
ALIGN_BYTES = 1024 * 1024
DATA_LABEL = "OPENARIA-DATA"
PLAN_SCHEMA = "ylx.rdk-x5-sd-image-plan.v1"
VOLUME_SCHEMA = "ylx.capture-volume.v1"
VOLUME_MARKER = ".ylx-volume.json"


class ImageBuildError(RuntimeError):
    def __init__(self, code: str, message: str) -> None:
        self.code = code
        self.message = message
        super().__init__(f"{code}: {message}")


@dataclass(frozen=True)
class PartitionPlan:
    number: int
    name: str
    start_bytes: int
    size_bytes: int
    type_code: str
    bootable: bool = False
    label: str | None = None
    mountpoint: str | None = None

    @property
    def start_sector(self) -> int:
        return self.start_bytes // SECTOR_SIZE

    @property
    def size_sectors(self) -> int:
        return self.size_bytes // SECTOR_SIZE

    def to_json(self) -> dict[str, object]:
        value: dict[str, object] = {
            "number": self.number,
            "name": self.name,
            "start_bytes": self.start_bytes,
            "start_sector": self.start_sector,
            "size_bytes": self.size_bytes,
            "size_sectors": self.size_sectors,
            "type": self.type_code,
            "bootable": self.bootable,
        }
        if self.label is not None:
            value["label"] = self.label
        if self.mountpoint is not None:
            value["mountpoint"] = self.mountpoint
        return value


@dataclass(frozen=True)
class ImagePlan:
    output: Path
    image_bytes: int
    boot_source: Path | None
    rootfs_source: Path | None
    volume_id: str
    initialized_at: str
    partitions: tuple[PartitionPlan, PartitionPlan, PartitionPlan]

    @property
    def data_partition(self) -> PartitionPlan:
        return self.partitions[2]

    def to_json(self) -> dict[str, object]:
        return {
            "schema": PLAN_SCHEMA,
            "output": str(self.output),
            "image_bytes": self.image_bytes,
            "sector_size": SECTOR_SIZE,
            "alignment_bytes": ALIGN_BYTES,
            "boot_source": str(self.boot_source) if self.boot_source else None,
            "rootfs_source": str(self.rootfs_source) if self.rootfs_source else None,
            "data": {
                "label": DATA_LABEL,
                "mountpoint": "/data",
                "volume_id": self.volume_id,
                "initialized_at": self.initialized_at,
                "marker": VOLUME_MARKER,
                "directories": ["recordings", "sessions"],
            },
            "partitions": [partition.to_json() for partition in self.partitions],
            "sfdisk": sfdisk_script(self),
        }


def parse_size(value: str) -> int:
    raw = value.strip()
    if not raw:
        raise ValueError("size is empty")
    suffix = raw[-1].upper()
    multipliers = {"K": 1024, "M": 1024**2, "G": 1024**3, "T": 1024**4}
    if suffix in multipliers:
        number = raw[:-1]
        multiplier = multipliers[suffix]
    else:
        number = raw
        multiplier = 1
    if not number.isdigit():
        raise ValueError(f"invalid size: {value}")
    parsed = int(number) * multiplier
    if parsed <= 0:
        raise ValueError("size must be positive")
    return parsed


def align_up(value: int, alignment: int = ALIGN_BYTES) -> int:
    return ((value + alignment - 1) // alignment) * alignment


def canonical_uuid(value: str | None) -> str:
    selected = value or str(uuid.uuid4())
    parsed = uuid.UUID(selected)
    if parsed.version != 4 or str(parsed) != selected:
        raise ValueError("volume id must be canonical UUIDv4")
    return selected


def timestamp(value: str | None) -> str:
    if value is not None:
        if not value.endswith("Z"):
            raise ValueError("initialized_at must be UTC ISO-8601 ending in Z")
        return value
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def source_size(path: Path | None) -> int:
    if path is None:
        return 0
    if not path.is_file():
        raise ImageBuildError("source_missing", f"{path} is not a regular file")
    return path.stat().st_size


def build_plan(
    *,
    output: Path,
    boot_source: Path | None,
    rootfs_source: Path | None,
    boot_size: int,
    rootfs_size: int,
    data_size: int,
    volume_id: str | None = None,
    initialized_at: str | None = None,
) -> ImagePlan:
    boot_size = align_up(max(boot_size, source_size(boot_source)))
    rootfs_size = align_up(max(rootfs_size, source_size(rootfs_source)))
    data_size = align_up(data_size)
    if min(boot_size, rootfs_size, data_size) < 16 * 1024 * 1024:
        raise ImageBuildError("partition_too_small", "all partitions must be at least 16 MiB")

    boot_start = ALIGN_BYTES
    rootfs_start = align_up(boot_start + boot_size)
    data_start = align_up(rootfs_start + rootfs_size)
    image_bytes = align_up(data_start + data_size)
    return ImagePlan(
        output=output,
        image_bytes=image_bytes,
        boot_source=boot_source,
        rootfs_source=rootfs_source,
        volume_id=canonical_uuid(volume_id),
        initialized_at=timestamp(initialized_at),
        partitions=(
            PartitionPlan(
                number=1,
                name="boot_config",
                start_bytes=boot_start,
                size_bytes=boot_size,
                type_code="c",
                bootable=True,
                mountpoint="/boot/config",
            ),
            PartitionPlan(
                number=2,
                name="rootfs",
                start_bytes=rootfs_start,
                size_bytes=rootfs_size,
                type_code="83",
                mountpoint="/",
            ),
            PartitionPlan(
                number=3,
                name="rp_ylx_data",
                start_bytes=data_start,
                size_bytes=data_size,
                type_code="83",
                label=DATA_LABEL,
                mountpoint="/data",
            ),
        ),
    )


def sfdisk_script(plan: ImagePlan) -> str:
    lines = ["label: dos", f"sector-size: {SECTOR_SIZE}", "unit: sectors", ""]
    for partition in plan.partitions:
        fields = [
            f"start={partition.start_sector}",
            f"size={partition.size_sectors}",
            f"type={partition.type_code}",
        ]
        if partition.bootable:
            fields.append("bootable")
        lines.append(", ".join(fields))
    return "\n".join(lines) + "\n"


def require_tool(name: str) -> str:
    path = shutil.which(name)
    if path is None:
        raise ImageBuildError("tool_missing", f"required tool is missing: {name}")
    return path


def run_command(command: list[str], *, input_text: str | None = None) -> None:
    subprocess.run(command, input=input_text, text=True, check=True)


def write_partition_payload(image: Path, source: Path | None, partition: PartitionPlan) -> None:
    if source is None:
        return
    size = source.stat().st_size
    if size > partition.size_bytes:
        raise ImageBuildError(
            "source_too_large",
            f"{source} has {size} bytes but partition {partition.number} has "
            f"{partition.size_bytes} bytes",
        )
    with source.open("rb") as src, image.open("r+b") as dst:
        dst.seek(partition.start_bytes)
        shutil.copyfileobj(src, dst, length=1024 * 1024)
        dst.flush()
        os.fsync(dst.fileno())


def prepare_data_seed(root: Path, *, volume_id: str, initialized_at: str) -> None:
    marker = {
        "schema": VOLUME_SCHEMA,
        "volume_id": volume_id,
        "initialized_at": initialized_at,
    }
    (root / VOLUME_MARKER).write_text(
        json.dumps(marker, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    (root / "recordings").mkdir(mode=0o750)
    (root / "sessions").mkdir(mode=0o750)


def validate_output_path(path: Path, *, force: bool) -> None:
    if path.exists():
        if path.is_block_device():
            raise ImageBuildError("unsafe_output", "output must be a regular image file")
        if path.is_dir():
            raise ImageBuildError("unsafe_output", "output path is a directory")
        if not force:
            raise ImageBuildError("output_exists", f"{path} already exists; pass --force")
        path.unlink()
    path.parent.mkdir(parents=True, exist_ok=True)


def build_image(plan: ImagePlan, *, force: bool = False) -> None:
    validate_output_path(plan.output, force=force)
    require_tool("sfdisk")
    require_tool("mkfs.ext4")

    run_command(["truncate", "-s", str(plan.image_bytes), str(plan.output)])
    run_command(["sfdisk", str(plan.output)], input_text=sfdisk_script(plan))
    write_partition_payload(plan.output, plan.boot_source, plan.partitions[0])
    write_partition_payload(plan.output, plan.rootfs_source, plan.partitions[1])

    with tempfile.TemporaryDirectory(prefix="rp-ylx-data-seed-") as directory:
        seed = Path(directory)
        prepare_data_seed(seed, volume_id=plan.volume_id, initialized_at=plan.initialized_at)
        run_command(
            [
                "mkfs.ext4",
                "-F",
                "-b",
                "4096",
                "-E",
                f"offset={plan.data_partition.start_bytes},nodiscard",
                "-L",
                DATA_LABEL,
                "-U",
                plan.volume_id,
                "-d",
                str(seed),
                str(plan.output),
                str(plan.data_partition.size_bytes // 4096),
            ]
        )


def render_error(error: ImageBuildError, stream: TextIO) -> None:
    stream.write(
        json.dumps(
            {"ok": False, "error": {"code": error.code, "message": error.message}},
            ensure_ascii=False,
            sort_keys=True,
        )
        + "\n"
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True, type=Path, help="regular image file to create")
    parser.add_argument("--boot-image", type=Path, help="raw partition image for /boot/config")
    parser.add_argument("--rootfs-image", type=Path, help="raw partition image for /")
    parser.add_argument("--boot-size", default="256M", help="minimum p1 size")
    parser.add_argument("--rootfs-size", default="32G", help="minimum p2 size")
    parser.add_argument("--data-size", default="80G", help="p3 /data size")
    parser.add_argument("--volume-id", help="canonical UUIDv4 for .ylx-volume.json and ext4 UUID")
    parser.add_argument(
        "--initialized-at",
        help="UTC timestamp ending in Z for deterministic builds",
    )
    parser.add_argument("--manifest", type=Path, help="write the build plan JSON to this path")
    parser.add_argument("--dry-run", action="store_true", help="render plan without writing image")
    parser.add_argument(
        "--force",
        action="store_true",
        help="replace an existing regular image file",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        plan = build_plan(
            output=args.output,
            boot_source=args.boot_image,
            rootfs_source=args.rootfs_image,
            boot_size=parse_size(args.boot_size),
            rootfs_size=parse_size(args.rootfs_size),
            data_size=parse_size(args.data_size),
            volume_id=args.volume_id,
            initialized_at=args.initialized_at,
        )
        if not args.dry_run:
            build_image(plan, force=args.force)
        rendered = json.dumps(plan.to_json(), ensure_ascii=False, indent=2, sort_keys=True) + "\n"
        if args.manifest is not None:
            args.manifest.parent.mkdir(parents=True, exist_ok=True)
            args.manifest.write_text(rendered, encoding="utf-8")
        sys.stdout.write(rendered)
        return 0
    except (ImageBuildError, OSError, subprocess.CalledProcessError, ValueError) as error:
        if isinstance(error, ImageBuildError):
            render_error(error, sys.stderr)
        else:
            render_error(ImageBuildError("image_build_failed", str(error)), sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())

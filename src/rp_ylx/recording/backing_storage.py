"""Reserve checks for the filesystems hosting regular-file loop volumes."""

from __future__ import annotations

import os
import stat
from pathlib import Path

from rp_ylx.recording.device_session import DeviceRecordingError

SYS_DEV_BLOCK = Path("/sys/dev/block")


def backing_capacity(mountpoint: Path) -> tuple[int, int] | None:
    """Return the tightest host byte/inode budget for direct loop filesystems.

    This is a separate reserve, not a replacement for logical volume capacity:
    already allocated image blocks can be reused without allocating host space.
    Do not add logical and backing free bytes or estimate credits from st_blocks.
    The normal appliance mounts its whole loop device, not a loop partition.
    """
    current = mountpoint
    seen: set[int] = set()
    result: tuple[int, int] | None = None
    try:
        for _ in range(8):
            device = current.stat().st_dev
            if device in seen:
                raise ValueError("录制卷宿主设备引用循环")
            seen.add(device)
            node = SYS_DEV_BLOCK / f"{os.major(device)}:{os.minor(device)}" / "loop/backing_file"
            try:
                value = node.read_text().removesuffix("\n")
            except FileNotFoundError:
                # Major 7 is the Linux loop block-device major. An attached
                # loop losing its backing identity must not become unguarded.
                if os.major(device) == 7:
                    raise ValueError("无法读取录制卷镜像身份") from None
                return result
            backing = Path(value)
            if not value or not backing.is_absolute() or "\n" in value:
                raise ValueError("录制卷镜像路径无效")
            mode = backing.stat().st_mode
            if stat.S_ISBLK(mode):
                # A block-backed loop does not allocate blocks in a host file.
                return result
            if not stat.S_ISREG(mode):
                raise ValueError("录制卷镜像不是普通文件")
            capacity = os.statvfs(backing)
            available = (capacity.f_bavail * capacity.f_frsize, capacity.f_favail)
            result = (
                available
                if result is None
                else (min(result[0], available[0]), min(result[1], available[1]))
            )
            current = backing
        raise ValueError("录制卷镜像嵌套过深")
    except (OSError, ValueError) as error:
        raise DeviceRecordingError("storage_unavailable", "无法读取录制卷宿主容量") from error


def require_backing_reserve(mountpoint: Path, minimum_bytes: int, minimum_inodes: int) -> None:
    capacity = backing_capacity(mountpoint)
    if capacity is None:
        return
    available, inodes = capacity
    if available < minimum_bytes:
        raise DeviceRecordingError("insufficient_space", "录制卷宿主文件系统可用空间不足")
    if inodes < minimum_inodes:
        raise DeviceRecordingError("insufficient_inodes", "录制卷宿主文件系统可用 inode 不足")

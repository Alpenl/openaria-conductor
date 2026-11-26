"""Volume admission, capacity caching, reserve enforcement, and readiness checks."""

from __future__ import annotations

import os
import re
import stat
import threading
import time
import uuid
from collections.abc import Callable, Mapping
from contextlib import suppress
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING

from rp_ylx.capture_readiness import SYSTEM_RESERVE_BYTES, SYSTEM_RESERVE_INODES, system_capacity
from rp_ylx.operational_logging import operational_logger
from rp_ylx.recording.backing_storage import backing_capacity, require_backing_reserve
from rp_ylx.recording.device_session import (
    DeviceRecordingError,
    DeviceSessionConfig,
    StorageStatus,
)
from rp_ylx.recording.fs_safety import fsync_directory, write_json_atomic

if TYPE_CHECKING:
    from rp_ylx.recording.device_session import SessionPlan

VOLUME_MARKER = ".ylx-volume.json"
SESSIONS_DIRECTORY = "recordings"
LEGACY_SESSIONS_DIRECTORY = "sessions"


@dataclass(frozen=True, slots=True)
class VolumeAdmission:
    """Admitted recording volume topology and initial capacity snapshot."""

    mountpoint: Path
    sessions_root: Path
    catalog_roots: tuple[Path, ...]
    volume_id: str
    generation_id: str
    device: int
    mount_identity: str
    total_bytes: int
    available_bytes: int
    available_inodes: int


@dataclass(frozen=True, slots=True)
class CoordinatorConfig:
    """Configuration for single-volume bounded capture coordination."""

    mountpoint: Path
    state_root: Path
    session: DeviceSessionConfig
    minimum_available_bytes: int = 2 * 1024 * 1024 * 1024
    minimum_available_inodes: int = 1024
    queue_capacity: int = 128
    enqueue_timeout: float = 0.05
    checkpoint_interval: float = 1.0

    def __post_init__(self) -> None:
        if (
            not self.mountpoint.is_absolute()
            or not self.state_root.is_absolute()
            or self.minimum_available_bytes < 0
            or self.minimum_available_inodes < 0
            or self.queue_capacity <= 0
            or self.enqueue_timeout < 0
            or self.checkpoint_interval < 0
        ):
            raise ValueError("录制协调器配置无效")


def initialize_capture_volume(mountpoint: str | Path, *, volume_id: str | None = None) -> str:
    """显式初始化录制卷；不会覆盖已有标记或创建挂载点。"""

    root = Path(mountpoint).resolve()
    if not root.is_dir():
        raise DeviceRecordingError("storage_unavailable", "录制卷挂载点不存在")
    selected = volume_id or str(uuid.uuid4())
    try:
        parsed = uuid.UUID(selected)
    except ValueError as error:
        raise ValueError("volume_id 必须是 UUIDv4") from error
    if parsed.version != 4 or str(parsed) != selected:
        raise ValueError("volume_id 必须是规范 UUIDv4")
    marker = root / VOLUME_MARKER
    if marker.exists():
        raise DeviceRecordingError("volume_already_initialized", "录制卷已经包含身份标记")
    write_json_atomic(
        marker,
        {
            "schema": "ylx.capture-volume.v1",
            "volume_id": selected,
            "initialized_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        },
    )
    return selected


def read_volume_id(mountpoint: Path) -> str:
    """Read and validate the UUIDv4 volume marker file on `mountpoint`."""
    import json

    marker = mountpoint / VOLUME_MARKER
    try:
        raw = marker.read_bytes()
        if len(raw) > 4096:
            raise ValueError("卷标记过大")
        value = json.loads(raw)
        if not isinstance(value, dict) or set(value) != {
            "schema",
            "volume_id",
            "initialized_at",
        }:
            raise ValueError("卷标记字段无效")
        selected = value["volume_id"]
        parsed = uuid.UUID(selected)
        if (
            value["schema"] != "ylx.capture-volume.v1"
            or parsed.version != 4
            or str(parsed) != selected
            or not isinstance(value["initialized_at"], str)
        ):
            raise ValueError("卷标记值无效")
        return selected
    except (OSError, ValueError, TypeError, json.JSONDecodeError) as error:
        raise DeviceRecordingError("volume_not_admitted", "录制卷缺少有效身份标记") from error


def stat_capacity(mountpoint: Path) -> tuple[int, int, int]:
    """Return `(total_bytes, available_bytes, available_inodes)` handling exFAT `f_files=0`."""
    try:
        info = os.statvfs(mountpoint)
    except OSError as error:
        raise DeviceRecordingError("storage_unavailable", "无法读取录制卷容量") from error
    return (
        info.f_frsize * info.f_blocks,
        info.f_frsize * info.f_bavail,
        # exFAT has no fixed inode table and reports f_files=f_favail=0.
        # Use free allocation units as a conservative creation budget there;
        # preserve the real exhausted-inode signal on ext4.
        info.f_bavail if info.f_files == 0 else info.f_favail,
    )


def mount_writable(mountpoint: Path) -> bool:
    """Return whether `mountpoint` is mounted read-write and accessible for writing."""
    try:
        flags = os.statvfs(mountpoint).f_flag
    except OSError:
        return False
    read_only = bool(flags & getattr(os, "ST_RDONLY", 1))
    return not read_only and os.access(mountpoint, os.W_OK)


def default_mount_identity(mountpoint: Path) -> str:
    """Resolve a mount identity string from `/proc/self/mountinfo` or `(st_dev, st_ino)`."""
    try:
        lines = Path("/proc/self/mountinfo").read_text(encoding="utf-8").splitlines()
    except OSError:
        lines = []
    encoded_mountpoint = str(mountpoint).replace(" ", "\\040").replace("\t", "\\011")
    matched: str | None = None
    for line in lines:
        fields = line.split()
        if len(fields) >= 6 and fields[4] == encoded_mountpoint:
            matched = f"mount:{fields[0]}:{fields[2]}"
    if matched is not None:
        return matched
    current = mountpoint.stat()
    return f"path:{current.st_dev}:{current.st_ino}"


class VolumeManager:
    """Owns volume admission, capacity cache, and storage reserve checks."""

    def __init__(
        self,
        config: CoordinatorConfig,
        *,
        mount_checker: Callable[[Path], bool] | None = None,
        mount_identity: Callable[[Path], str] | None = None,
        stat_capacity_fn: Callable[[Path], tuple[int, int, int]] | None = None,
        system_capacity_fn: Callable[[Path], tuple[int, int]] | None = None,
        backing_capacity_fn: Callable[[Path], tuple[int, int] | None] | None = None,
        monotonic_fn: Callable[[], float] | None = None,
    ) -> None:
        self._config = config
        self._mount_checker = mount_checker or (lambda path: path.is_mount())
        self._mount_identity = mount_identity or default_mount_identity
        self._stat_capacity = stat_capacity_fn or stat_capacity
        self._system_capacity = system_capacity_fn or system_capacity
        self._backing_capacity = backing_capacity_fn or backing_capacity
        self._monotonic = monotonic_fn or time.monotonic
        self._storage_lock = threading.Lock()
        self._storage_checked_at = 0.0
        self._storage_cache: StorageStatus | None = None
        self._admission: VolumeAdmission | None = None
        self._media_lost = False

    def admit(self, persisted: Mapping[str, object]) -> VolumeAdmission:
        """Validate mountpoint, exFAT UUID binding, marker, and session directories."""
        mountpoint = self._config.mountpoint.resolve()
        if not self._mount_checker(mountpoint):
            raise DeviceRecordingError("volume_not_mounted", "录制卷不是当前活动挂载点")
        try:
            mount_stat = mountpoint.stat()
        except OSError as error:
            raise DeviceRecordingError("storage_unavailable", "录制卷不可读取") from error
        if os.environ.get("RP_YLX_DATA_VOLUME_MODE") == "exfat":
            identity = os.environ.get("RP_YLX_DATA_VOLUME_UUID", "")
            if not re.fullmatch(r"[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}", identity):
                raise DeviceRecordingError("volume_not_admitted", "录制卷配置缺少有效身份")
            try:
                expected = Path("/dev/disk/by-uuid", identity).stat()
            except OSError as error:
                raise DeviceRecordingError("volume_not_mounted", "指定的录制卷不在线") from error
            if not stat.S_ISBLK(expected.st_mode) or expected.st_rdev != mount_stat.st_dev:
                raise DeviceRecordingError("volume_not_admitted", "挂载的录制卷与设备配置不匹配")
        volume_id = read_volume_id(mountpoint)
        mount_identity = self._mount_identity(mountpoint)
        total_bytes, available_bytes, available_inodes = self._stat_capacity(mountpoint)
        # Admission proves volume identity and topology. Recording reserves must
        # not prevent reading or deleting existing sessions after a full-disk boot.
        if not mount_writable(mountpoint):
            raise DeviceRecordingError("volume_read_only", "录制卷不可写")
        previous_binding = persisted.get("volume_binding")
        generation_id = str(uuid.uuid4())
        if isinstance(previous_binding, Mapping):
            previous_generation = previous_binding.get("generation_id")
            if (
                previous_binding.get("volume_id") == volume_id
                and previous_binding.get("device") == mount_stat.st_dev
                and previous_binding.get("mount_identity") == mount_identity
                and isinstance(previous_generation, str)
            ):
                with suppress(ValueError):
                    parsed = uuid.UUID(previous_generation)
                    if parsed.version == 4 and str(parsed) == previous_generation:
                        generation_id = previous_generation
        sessions_root = mountpoint / SESSIONS_DIRECTORY
        sessions_root_existed = sessions_root.exists()
        try:
            sessions_root.mkdir(mode=0o750, exist_ok=True)
        except OSError as error:
            raise DeviceRecordingError("storage_unavailable", "无法访问会话目录") from error
        if sessions_root.is_symlink():
            raise DeviceRecordingError("unsafe_sessions_root", "会话目录不能是符号链接")
        if sessions_root.stat().st_dev != mount_stat.st_dev:
            raise DeviceRecordingError("cross_device_sessions", "会话目录不在录制卷文件系统内")
        catalog_roots = [sessions_root]
        legacy_sessions_root = mountpoint / LEGACY_SESSIONS_DIRECTORY
        if legacy_sessions_root != sessions_root and legacy_sessions_root.exists():
            if legacy_sessions_root.is_symlink() or not legacy_sessions_root.is_dir():
                raise DeviceRecordingError("unsafe_sessions_root", "旧会话目录不能是符号链接或文件")
            if legacy_sessions_root.stat().st_dev != mount_stat.st_dev:
                raise DeviceRecordingError(
                    "cross_device_sessions",
                    "旧会话目录不在录制卷文件系统内",
                )
            catalog_roots.append(legacy_sessions_root)
        if not sessions_root_existed:
            fsync_directory(mountpoint)
        return VolumeAdmission(
            mountpoint,
            sessions_root,
            tuple(catalog_roots),
            volume_id,
            generation_id,
            mount_stat.st_dev,
            mount_identity,
            total_bytes,
            available_bytes,
            available_inodes,
        )

    def probe_storage_capacity(self, admission: VolumeAdmission) -> tuple[StorageStatus, int]:
        """Read one admitted volume without changing coordinator or cache state."""
        mountpoint = admission.mountpoint
        try:
            if (
                not self._mount_checker(mountpoint)
                or mountpoint.stat().st_dev != admission.device
                or self._mount_identity(mountpoint) != admission.mount_identity
                or read_volume_id(mountpoint) != admission.volume_id
            ):
                raise DeviceRecordingError("media_lost", "录制介质已移除或被替换")
            _, available_bytes, inodes = self._stat_capacity(mountpoint)
            if not mount_writable(mountpoint):
                raise DeviceRecordingError("media_lost", "录制介质变为不可写")
            return StorageStatus(available_bytes, True), inodes
        except OSError as error:
            raise DeviceRecordingError("media_lost", "录制介质不可访问") from error

    def check_generation(
        self,
        admission: VolumeAdmission,
        generation_id: str | None = None,
        *,
        released: bool,
        force: bool = False,
    ) -> StorageStatus:
        """Verify generation, release/media-loss state, and return cached or fresh capacity."""
        if generation_id is not None and generation_id != admission.generation_id:
            raise DeviceRecordingError("stale_generation", "录制命令来自过期挂载代次")
        if released:
            raise DeviceRecordingError("volume_released", "录制卷已经进入安全移除状态")
        if self._media_lost:
            raise DeviceRecordingError("media_lost", "录制介质已移除或被替换")
        mountpoint = admission.mountpoint
        try:
            if not self._mount_checker(mountpoint) or mountpoint.stat().st_dev != admission.device:
                self._media_lost = True
                raise DeviceRecordingError("media_lost", "录制介质已移除或被替换")
        except DeviceRecordingError:
            raise
        except OSError as error:
            raise DeviceRecordingError("media_lost", "录制介质不可访问") from error
        now = self._monotonic()
        with self._storage_lock:
            if (
                not force
                and self._storage_cache is not None
                and now - self._storage_checked_at < 0.25
            ):
                return self._storage_cache
        try:
            status, _ = self.probe_storage_capacity(admission)
            self.update_storage_cache(status, now)
            return status
        except DeviceRecordingError as error:
            if error.code == "media_lost":
                self._media_lost = True
            raise
        except OSError as error:
            raise DeviceRecordingError("media_lost", "录制介质不可访问") from error

    def require_system_reserve(self) -> None:
        """Enforce minimum free space and inodes on the system state filesystem."""
        try:
            available, inodes = self._system_capacity(self._config.state_root)
        except OSError as error:
            raise DeviceRecordingError("storage_unavailable", "无法读取系统状态盘容量") from error
        if available < SYSTEM_RESERVE_BYTES:
            raise DeviceRecordingError(
                "insufficient_space", "系统状态盘可用空间不足，至少需要 256 MiB"
            )
        if inodes < SYSTEM_RESERVE_INODES:
            raise DeviceRecordingError("insufficient_inodes", "系统状态盘可用 inode 不足")

    def require_capture_reserve(
        self,
        admission: VolumeAdmission,
        capacity: tuple[int, int, int] | None = None,
    ) -> None:
        """Enforce system, loop backing host, and recording volume space/inode reserves."""
        self.require_system_reserve()
        _, available_bytes, available_inodes = (
            self._stat_capacity(admission.mountpoint) if capacity is None else capacity
        )
        require_backing_reserve(
            admission.mountpoint,
            self._config.minimum_available_bytes,
            self._config.minimum_available_inodes,
        )
        if available_bytes < self._config.minimum_available_bytes:
            raise DeviceRecordingError("insufficient_space", "录制卷可用空间不足")
        if available_inodes < self._config.minimum_available_inodes:
            raise DeviceRecordingError("insufficient_inodes", "录制卷可用 inode 不足")

    def cached_storage(self) -> StorageStatus | None:
        """Return the latest cached `StorageStatus` without disk I/O."""
        with self._storage_lock:
            return self._storage_cache

    def update_storage_cache(self, status: StorageStatus, observed_at: float) -> None:
        """Update cached storage capacity monotonically by observation timestamp."""
        with self._storage_lock:
            if observed_at >= self._storage_checked_at:
                self._storage_cache = status
                self._storage_checked_at = observed_at

    def invalidate_storage_cache(self) -> None:
        """Force the next capacity check to re-stat the recording volume."""
        with self._storage_lock:
            self._storage_checked_at = 0.0

    def capture_readiness(
        self,
        *,
        require_admission: Callable[[], VolumeAdmission],
        check_generation: Callable[..., StorageStatus],
        recovery_pending: bool,
        capture_busy: bool,
        closing: bool,
        camera_connected: bool,
    ) -> dict[str, object]:
        """Build `ylx.capture-readiness.v1` snapshot without holding lifecycle locks across I/O."""
        budgets: dict[str, object | None] = {"recording": None, "system": None, "backing": None}
        code, message = "ready", "可以开始新录制"
        try:
            available, _ = self._system_capacity(self._config.state_root)
            budgets["system"] = {
                "available_bytes": max(0, available),
                "required_bytes": SYSTEM_RESERVE_BYTES,
            }
            admission = require_admission()
            status = check_generation(force=True)
            budgets["recording"] = {
                "available_bytes": max(0, status.remaining_bytes or 0),
                "required_bytes": self._config.minimum_available_bytes,
            }
            backing = self._backing_capacity(admission.mountpoint)
            if backing is not None:
                budgets["backing"] = {
                    "available_bytes": max(0, backing[0]),
                    "required_bytes": self._config.minimum_available_bytes,
                }
            self.require_capture_reserve(admission)
            if not status.writable:
                raise DeviceRecordingError("storage_unavailable", "录像盘不可写")
        except (DeviceRecordingError, OSError) as error:
            code = getattr(error, "code", "storage_unavailable")
            message = getattr(error, "message", "无法读取存储状态")
        if recovery_pending:
            code, message = (
                "recovery_pending",
                "正在保存上次录制并释放资源，完成后自动恢复；请勿断电",
            )
        elif capture_busy:
            code, message = "capture_busy", "录制或保存正在进行"
        elif closing:
            code, message = "capture_busy", "录制服务正在关闭"
        elif code == "ready" and not camera_connected:
            code, message = "hardware_unavailable", "相机未连接"
        return {
            "schema": "ylx.capture-readiness.v1",
            "allowed": code == "ready",
            "code": code,
            "message": message,
            "storage": budgets,
        }

    def check_reserve_violation(
        self,
        admission: VolumeAdmission,
        plan: SessionPlan,
    ) -> tuple[StorageStatus, float, tuple[str, str] | None]:
        """Probe volume, system, and backing reserves outside any lifecycle lock."""
        storage, inodes = self.probe_storage_capacity(admission)
        observed_at = self._monotonic()
        available = storage.remaining_bytes
        backing_error = None
        system_error = None
        try:
            self.require_system_reserve()
        except DeviceRecordingError as error:
            system_error = error.code
        try:
            backing = self._backing_capacity(admission.mountpoint)
        except DeviceRecordingError as error:
            # An unreadable host budget must not leave the recorder writing
            # indefinitely while the monitor only logs/retries the error.
            backing, backing_error = (0, 0), error.code
        if (
            backing_error is None
            and system_error is None
            and available >= self._config.minimum_available_bytes
            and inodes >= self._config.minimum_available_inodes
            and (
                backing is None
                or (
                    backing[0] >= self._config.minimum_available_bytes
                    and backing[1] >= self._config.minimum_available_inodes
                )
            )
        ):
            return storage, observed_at, None
        if system_error is not None:
            violation = ("system_reserve_low", system_error)
        elif backing_error is not None:
            violation = ("backing_capacity_unavailable", backing_error)
        elif available < self._config.minimum_available_bytes:
            violation = ("volume_bytes_low", "insufficient_space")
        elif inodes < self._config.minimum_available_inodes:
            violation = ("volume_inodes_low", "insufficient_inodes")
        elif backing is not None and backing[0] < self._config.minimum_available_bytes:
            violation = ("backing_bytes_low", "insufficient_space")
        else:
            violation = ("backing_inodes_low", "insufficient_inodes")
        operational_logger("recording").event(
            "storage_reserve_stop",
            transaction_id=plan.session_id,
            reason=violation[0],
            error_code=violation[1],
        )
        return storage, observed_at, violation

"""Transactional ReleaseManager for RDK X5 install, recovery, rollback, and uninstall."""

from __future__ import annotations

import fcntl
import grp
import inspect
import json
import os
import platform
import re
import shutil
import ssl
import stat
import subprocess
import sys
import time
import urllib.error
import urllib.request
import uuid
from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager, suppress
from pathlib import Path, PurePosixPath

from . import config as cfg
from . import network_migration as netmig
from . import storage, systemd
from .bundle import Bundle, _build_stereo_encoder, _install_stage, load_bundle
from .schema import (
    ACTIVATION_HEALTH_TIMEOUT_SECONDS,
    DEPLOYMENT_ASSETS,
    MAX_ACTIVATION_DIAGNOSTIC_BYTES,
    MAX_INSTALL_TRANSACTION_BYTES,
    NETWORK_CONTROL_PROTOCOL,
    PRESERVED_DEPLOYMENT_ASSETS,
    RELEASE_SCHEMA,
    TRANSACTION_SCHEMA,
    DeploymentError,
    _canonical_commit,
    _fsync_directory,
    _fsync_tree,
    _json_bytes,
    _read_deployment_json,
    _read_release_metadata,
    _require_regular_file,
    _sha256,
    _write_atomic,
    facade_attr,
)


def _ancestor_pids() -> set[int]:
    ancestors: set[int] = set()
    pid = os.getpid()
    while pid > 1 and pid not in ancestors:
        ancestors.add(pid)
        try:
            stat_text = Path(f"/proc/{pid}/stat").read_text(encoding="ascii", errors="ignore")
            rparen = stat_text.rfind(")")
            if rparen < 0:
                break
            fields = stat_text[rparen + 2 :].split()
            if len(fields) < 2:
                break
            pid = int(fields[1])
        except (OSError, ValueError):
            break
    return ancestors


def _is_openaria_updater_process(pid: int) -> bool:
    try:
        raw = Path(f"/proc/{pid}/cmdline").read_bytes()
    except OSError:
        return False
    args = [part.decode("utf-8", errors="ignore") for part in raw.split(b"\0") if part]
    return any(Path(arg).name in {"update.py", "openaria-update"} for arg in args)


def _ancestor_holds_exclusive_flock(st_dev: int, st_ino: int) -> bool:
    """Return True if exclusive flock on (st_dev, st_ino) is held solely by ancestor updater."""
    target_major = os.major(st_dev)
    target_minor = os.minor(st_dev)
    try:
        lines = Path("/proc/locks").read_text(encoding="ascii", errors="ignore").splitlines()
    except OSError:
        return False
    ancestors = _ancestor_pids()
    matched = False
    for line in lines:
        parts = line.split()
        if len(parts) < 6 or parts[1] != "FLOCK":
            continue
        dev_ino = parts[5].split(":")
        if len(dev_ino) != 3:
            continue
        try:
            major = int(dev_ino[0], 16)
            minor = int(dev_ino[1], 16)
            inode = int(dev_ino[2], 10)
            holder_pid = int(parts[4], 10)
        except ValueError:
            continue
        if (major, minor, inode) != (target_major, target_minor, st_ino):
            continue
        matched = True
        if (
            parts[3] != "WRITE"
            or holder_pid not in ancestors
            or not _is_openaria_updater_process(holder_pid)
        ):
            return False
    return matched


class ReleaseManager:
    """Atomic two-version release manager for Open Aria on RDK X5."""

    def __init__(
        self,
        *,
        install_root: Path = Path("/opt/rp-ylx"),
        config_root: Path = Path("/etc/rp-ylx"),
        state_root: Path = Path("/var/lib/rp-ylx"),
        system_root: Path = Path("/"),
        target_machine: str | None = None,
        target_model: str | None = None,
        target_model_path: Path = Path("/proc/device-tree/model"),
        python_version: tuple[int, int] | None = None,
        library_finder: Callable[[str], str | None] | None = None,
        executable_finder: Callable[[str], str | None] = shutil.which,
        runner: systemd.Runner = systemd._run,
        stage_installer: systemd.StageInstaller = _install_stage,
        encoder_builder: Callable[[Path], None] = _build_stereo_encoder,
        health_checker: Callable[[], None] | None = None,
        tls_material_generator: systemd.TlsMaterialGenerator = cfg._default_tls_material_generator,
        group_resolver: systemd.GroupResolver = systemd._group_gid,
        unit_load_state_reader: systemd.UnitLoadStateReader = systemd._systemd_unit_load_state,
    ) -> None:
        if library_finder is None:
            import ctypes.util

            library_finder = ctypes.util.find_library
        self.install_root = install_root
        self.config_root = config_root
        self.state_root = state_root
        self.system_root = system_root
        self.releases = install_root / "releases"
        self.transaction = state_root / "install-transaction.json"
        self.target_machine = (target_machine or platform.machine()).casefold()
        self.target_model = target_model
        self.target_model_path = target_model_path
        self.python_version = python_version or sys.version_info[:2]
        self.library_finder = library_finder
        self.executable_finder = executable_finder
        self.runner = runner
        self.stage_installer = stage_installer
        self.encoder_builder = encoder_builder
        self.health_checker = health_checker or self._wait_for_health
        self.tls_material_generator = tls_material_generator
        self.group_resolver = group_resolver
        self.unit_load_state_reader = unit_load_state_reader
        self._recovery_descriptor: int | None = None
        self._recovery_exclusive = False
        self._active_release_assets: Path | None = None

    @contextmanager
    def _deployment_lock(self, name: str, *, shared: bool = False) -> Iterator[int]:
        # This root-owned directory is shared by every install/state root on the
        # same system. Never unlink lock files: waiters must use the same inode.
        root = self.system_root / "var/lib/openaria-update"
        directory = descriptor = None
        try:
            try:
                root.mkdir(parents=True, exist_ok=True, mode=0o700)
                directory = os.open(
                    root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC
                )
                owner = self._deployment_owner()[0]
                parent = os.fstat(directory)
                if parent.st_uid != owner or parent.st_mode & 0o022:
                    raise ValueError("untrusted deployment lock directory")
                filename = f"deployment-{name}.lock"
                descriptor = os.open(
                    filename,
                    os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW | os.O_NONBLOCK | os.O_CLOEXEC,
                    0o600,
                    dir_fd=directory,
                )
                metadata = os.fstat(descriptor)
                if (
                    not stat.S_ISREG(metadata.st_mode)
                    or metadata.st_nlink != 1
                    or metadata.st_uid != owner
                    or metadata.st_mode & 0o077
                ):
                    raise ValueError("untrusted deployment lock file")
                try:
                    fcntl.flock(
                        descriptor, (fcntl.LOCK_SH if shared else fcntl.LOCK_EX) | fcntl.LOCK_NB
                    )
                except BlockingIOError as error:
                    raise DeploymentError(
                        "deployment_busy", "其他维护操作正在执行，请稍后重试"
                    ) from error
                named = os.stat(filename, dir_fd=directory, follow_symlinks=False)
                current_parent = root.lstat()
                if (named.st_dev, named.st_ino, named.st_mode, named.st_nlink, named.st_uid) != (
                    metadata.st_dev,
                    metadata.st_ino,
                    metadata.st_mode,
                    1,
                    owner,
                ) or (current_parent.st_dev, current_parent.st_ino) != (
                    parent.st_dev,
                    parent.st_ino,
                ):
                    raise ValueError("deployment lock identity changed")
            except (OSError, ValueError) as error:
                raise DeploymentError("deployment_lock_invalid", "无法安全取得维护锁") from error
            yield descriptor
        finally:
            primary = sys.exc_info()[1]
            cleanup_error = None
            for fd in (descriptor, directory):
                if fd is not None:
                    try:
                        os.close(fd)
                    except BaseException as error:
                        if cleanup_error is None:
                            cleanup_error = error
            if primary is None and cleanup_error is not None:
                raise cleanup_error

    @contextmanager
    def _maintenance_lease(self) -> Iterator[None]:
        """Acquire exclusive maintenance.lock matching update.py, unless already held by parent."""
        if "OPENARIA_MAINTENANCE_LOCK" in os.environ:
            path = Path(os.environ["OPENARIA_MAINTENANCE_LOCK"])
        else:
            path = self.system_root / "run/rp-ylx/maintenance.lock"
            if self.system_root != Path("/") and not os.path.lexists(path):
                yield
                return
        existed = path.parent.exists()
        path.parent.mkdir(parents=True, exist_ok=True, mode=0o750)
        if os.geteuid() == 0:
            if not existed or path.parent == (self.system_root / "run/rp-ylx"):
                if stat.S_IMODE(path.parent.stat().st_mode) != 0o750:
                    os.chmod(path.parent, 0o750)
                with suppress(KeyError):
                    gid = grp.getgrnam("rp-ylx").gr_gid
                    if path.parent.stat().st_gid != gid:
                        os.chown(path.parent, 0, gid)
            elif (path.parent.stat().st_mode & 0o050) != 0o050:
                os.chmod(path.parent, 0o750)
        flags = os.O_RDONLY | os.O_CREAT | os.O_NOFOLLOW | getattr(os, "O_CLOEXEC", 0)
        try:
            fd = os.open(path, flags, 0o660)
        except OSError as error:
            raise DeploymentError("deployment_lock_invalid", "无法打开维护锁") from error
        try:
            info = os.fstat(fd)
            if not stat.S_ISREG(info.st_mode):
                raise DeploymentError("deployment_lock_invalid", "维护锁不是普通文件")
            if os.geteuid() == 0 or info.st_uid == os.geteuid():
                os.fchmod(fd, 0o660)
            if os.geteuid() == 0:
                with suppress(KeyError):
                    gid = grp.getgrnam("rp-ylx").gr_gid
                    if info.st_gid != gid:
                        os.fchown(fd, -1, gid)
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as error:
                if not _ancestor_holds_exclusive_flock(info.st_dev, info.st_ino):
                    raise DeploymentError(
                        "deployment_busy", "设备正在录制或更新，请等待当前任务完成"
                    ) from error
            except OSError as error:
                raise DeploymentError("deployment_lock_invalid", "无法取得维护锁") from error
            yield
        finally:
            os.close(fd)

    @contextmanager
    def _maintenance_operation(self) -> Iterator[None]:
        with self._deployment_lock("operation"), self._deployment_lock("recovery") as descriptor:
            self._recovery_descriptor = descriptor
            self._recovery_exclusive = True
            try:
                yield
            finally:
                self._recovery_descriptor = None
                self._recovery_exclusive = False

    @contextmanager
    def _service_recovery_readers(self) -> Iterator[None]:
        descriptor = self._recovery_descriptor
        if descriptor is None:
            yield
            return
        fcntl.flock(descriptor, fcntl.LOCK_SH)
        self._recovery_exclusive = False
        try:
            yield
        finally:
            primary = sys.exc_info()[1]
            try:
                # Only read-only service checks can coexist here. Other writers
                # remain excluded by the operation lock throughout activation.
                fcntl.flock(descriptor, fcntl.LOCK_EX)
                self._recovery_exclusive = True
            except BaseException as error:
                if primary is not None:
                    raise primary from error
                if isinstance(error, OSError):
                    raise DeploymentError("deployment_lock_invalid", "无法恢复维护写锁") from error
                raise

    def _require_target(self) -> None:
        self._require_platform()
        if self.library_finder("turbojpeg") is None:
            raise DeploymentError("runtime_dependency_missing", "正式 60 FPS 路径要求 libturbojpeg")
        if self.library_finder("multimedia") is None:
            raise DeploymentError(
                "runtime_dependency_missing", "边录边出 H.264 要求 hobot-multimedia"
            )
        required_commands = (
            "systemctl",
            "systemd-sysusers",
            "systemd-tmpfiles",
            "nmcli",
            "openssl",
            "make",
            "cc",
            "getent",
            "useradd",
            "usermod",
            "chage",
        )
        missing = [
            command for command in required_commands if self.executable_finder(command) is None
        ]
        if missing:
            raise DeploymentError(
                "runtime_dependency_missing", f"缺少系统命令：{', '.join(missing)}"
            )

    def _require_platform(self) -> None:
        if self.target_machine not in {"aarch64", "arm64"}:
            raise DeploymentError("unsupported_platform", "安装包只支持 RDK X5 aarch64")
        model = self.target_model
        if model is None:
            try:
                model = self.target_model_path.read_text(encoding="utf-8", errors="replace")
            except OSError as error:
                raise DeploymentError(
                    "unsupported_platform", "无法确认 RDK X5 V1.0 板卡型号"
                ) from error
        normalized = " ".join(model.strip("\x00\n ").split()).casefold()
        if normalized != "d-robotics rdk x5 v1.0":
            raise DeploymentError("unsupported_platform", "安装包只支持 D-Robotics RDK X5 V1.0")

    def _ensure_layout(self) -> None:
        self.install_root.mkdir(parents=True, exist_ok=True, mode=0o755)
        self.install_root.chmod(0o755)
        self.releases.mkdir(parents=True, exist_ok=True, mode=0o755)
        self.releases.chmod(0o755)
        self.config_root.mkdir(parents=True, exist_ok=True, mode=0o750)
        self.state_root.mkdir(parents=True, exist_ok=True, mode=0o750)

    def _link_target(self, name: str) -> str | None:
        link = self.install_root / name
        try:
            # Read the link itself once. A separate is_symlink check can race
            # with uninstall; other I/O errors must not masquerade as absence.
            raw = os.readlink(link)
        except FileNotFoundError:
            return None
        except OSError as error:
            raise DeploymentError("install_state_invalid", f"无法读取 {name} 链接") from error
        target = PurePosixPath(raw)
        if len(target.parts) != 2 or target.parts[0] != "releases":
            raise DeploymentError("install_state_invalid", f"{name} 链接越界")
        try:
            return _canonical_commit(target.parts[1])
        except DeploymentError as error:
            raise DeploymentError("install_state_invalid", f"{name} 链接版本无效") from error

    def _switch(self, name: str, release: str | None) -> None:
        link = self.install_root / name
        temporary = self.install_root / f".{name}.{uuid.uuid4().hex}.tmp"
        if release is None:
            link.unlink(missing_ok=True)
            _fsync_directory(self.install_root)
            return
        os.symlink(f"releases/{release}", temporary)
        os.replace(temporary, link)
        _fsync_directory(self.install_root)

    def _transaction_document(
        self, action: str, old_current: str | None, new_current: str, state: str
    ) -> Mapping[str, object]:
        return {
            "schema": TRANSACTION_SCHEMA,
            "transaction_id": str(uuid.uuid4()),
            "action": action,
            "old_current": old_current,
            "new_current": new_current,
            "state": state,
            "config_snapshot_required": True,
        }

    def _read_transaction(self) -> Mapping[str, object] | None:
        reader = facade_attr("_read_deployment_json", _read_deployment_json)
        try:
            try:
                self.transaction.lstat()
            except FileNotFoundError:
                return None
            value = reader(self.transaction, maximum=MAX_INSTALL_TRANSACTION_BYTES)
            required = {"schema", "transaction_id", "action", "old_current", "new_current", "state"}
            fields = frozenset(value) if isinstance(value, dict) else frozenset()
            allowed_fields = {
                frozenset(required),
                frozenset(required | {"config_snapshot_required"}),
            }
            if (
                not isinstance(value, dict)
                or fields not in allowed_fields
                or value["schema"] != TRANSACTION_SCHEMA
                or value["action"] not in ("install", "rollback")
                or value["state"] not in ("prepared", "switched")
                or not isinstance(value["transaction_id"], str)
                or not re.fullmatch(
                    r"[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}",
                    value["transaction_id"],
                )
                or (
                    "config_snapshot_required" in value
                    and value["config_snapshot_required"] is not True
                )
            ):
                raise ValueError("invalid install transaction fields")
            _canonical_commit(value["new_current"])
            if value["old_current"] is not None:
                _canonical_commit(value["old_current"])
            return value
        except (OSError, ValueError, TypeError, RecursionError, DeploymentError) as error:
            raise DeploymentError("install_state_invalid", "安装事务不可读或无效") from error

    def _write_json_atomic(self, path: Path, value: object, *, mode: int = 0o644) -> None:
        writer = facade_attr("_write_atomic", _write_atomic)
        if mode == 0o644:
            writer(path, value)
            return
        if writer is _write_atomic or "mode" in inspect.signature(writer).parameters:
            writer(path, value, mode=mode)
            return
        writer(path, value)
        if path.exists():
            path.chmod(mode)

    def _settle(self, transaction: Mapping[str, object]) -> None:
        old = transaction["old_current"]
        new = str(transaction["new_current"])
        self._release_metadata(self.releases / new)
        if transaction["state"] == "prepared":
            self._switch("current", new)
            switched = dict(transaction)
            switched["state"] = "switched"
            self._write_json_atomic(self.transaction, switched, mode=0o600)
        self._switch("previous", None if old is None or old == new else str(old))
        keep = {new}
        if old is not None and old != new:
            keep.add(str(old))
        self._prune(keep)
        self.transaction.unlink(missing_ok=True)
        _fsync_directory(self.state_root)

    def recover(self, *, service_start: bool = False) -> Mapping[str, object]:
        self._require_platform()
        try:
            with self._maintenance_operation():
                return self._recover_locked()
        except DeploymentError as error:
            if not service_start or error.code != "deployment_busy":
                raise
        # systemd starts recovery as a dependency of capture/network services.
        # During activation only, the installer permits this non-mutating check.
        with self._deployment_lock("recovery", shared=True):
            if self._read_transaction() is not None:
                raise DeploymentError("deployment_busy", "版本事务尚未提交，不能启动服务")
            current = self._link_target("current")
            if current is None:
                raise DeploymentError("deployment_busy", "没有可启动的已提交版本")
            self._release_metadata(self.releases / current)
            return {**self.status(), "recovery_delegated": True}

    def _recover_locked(self) -> Mapping[str, object]:
        self._ensure_layout()
        transaction = self._read_transaction()
        assets_restored = False
        if transaction is not None:
            new = str(transaction["new_current"])
            self._release_metadata(self.releases / new)
            if transaction.get("config_snapshot_required") is True:
                snapshot = self._device_config_snapshot(new)
                if os.path.lexists(snapshot):
                    self._require_state_compatible_release(new)
                    self._restore_device_config_snapshot(new)
                    if (self.releases / new / "site-packages/rp_ylx/deploy").is_dir():
                        self._install_assets(release=self.releases / new)
                        assets_restored = True
                    self._settle(transaction)
                else:
                    old = transaction["old_current"]
                    if old is not None:
                        old_commit = str(old)
                        if (self.releases / old_commit / "site-packages/rp_ylx/deploy").is_dir():
                            self._install_assets(release=self.releases / old_commit)
                            assets_restored = True
                        self._restore_device_config_snapshot(old_commit)
                    self._revert_release_switch(
                        None if old is None else str(old),
                        new,
                    )
            else:
                self._settle(transaction)
        if assets_restored and not (self.runner is systemd._run and self.system_root != Path("/")):
            with suppress(DeploymentError):
                self.runner(["systemctl", "daemon-reload"])
        for staging in self.releases.glob(".*.staging-*"):
            if staging.is_dir() and not staging.is_symlink():
                shutil.rmtree(staging)
        return self.status()

    def _prune(self, keep: set[str]) -> None:
        for candidate in self.releases.iterdir():
            if candidate.name in keep or not candidate.is_dir() or candidate.is_symlink():
                continue
            with suppress(DeploymentError):
                _canonical_commit(candidate.name)
                shutil.rmtree(candidate)
        _fsync_directory(self.releases)

    def _release_metadata(self, release: Path) -> Mapping[str, object]:
        return _read_release_metadata(release)

    def _prepare_release(self, bundle: Bundle) -> Path:
        final = self.releases / bundle.commit
        if final.exists():
            metadata = self._release_metadata(final)
            bundle_root = getattr(bundle, "root", None)
            if bundle_root is None or not (bundle_root / "bundle.json").is_file():
                return final
            bundle_sha = _sha256(bundle_root / "bundle.json")
            expected_sha = metadata.get("bundle_sha256")
            artifacts_ok = (final / "bin/rp-ylx").is_file() and (
                final / "runtime/bin/python3"
            ).is_file()
            if expected_sha in (None, bundle_sha) and artifacts_ok:
                return final
        stage = self.releases / f".{bundle.commit}.staging-{uuid.uuid4().hex}"
        stage.mkdir(mode=0o755)
        stage.chmod(0o755)
        backup: Path | None = None
        try:
            self.stage_installer(bundle, stage)
            self.encoder_builder(stage)
            self._write_json_atomic(
                stage / "release.json",
                {
                    "schema": RELEASE_SCHEMA,
                    "commit": bundle.commit,
                    "version": bundle.version,
                    "bundle_sha256": _sha256(bundle.root / "bundle.json"),
                    "network_control_protocol": NETWORK_CONTROL_PROTOCOL,
                },
            )
            _fsync_tree(stage)
            if final.exists():
                if self._link_target("current") == bundle.commit:
                    self._quiesce_release_services()
                backup = self.releases / f".{bundle.commit}.backup-{uuid.uuid4().hex}"
                os.rename(final, backup)
            os.rename(stage, final)
            _fsync_directory(self.releases)
            if backup is not None:
                with suppress(OSError):
                    shutil.rmtree(backup)
        except BaseException:
            if backup is not None and backup.exists() and not final.exists():
                with suppress(OSError):
                    os.rename(backup, final)
            with suppress(OSError):
                shutil.rmtree(stage)
            raise
        return final

    def _deployment_owner(self) -> tuple[int, int]:
        if self.system_root == Path("/"):
            return 0, 0
        return os.getuid(), os.getgid()

    @staticmethod
    def _read_network_state_payload(path: Path) -> bytes:
        return netmig.read_network_state_payload(path)

    @staticmethod
    def _network_state_contains_secret(value: object) -> bool:
        return netmig.network_state_contains_secret(value)

    def _network_state_payloads(
        self,
        root: Path,
        *,
        migration_target: bool,
    ) -> dict[str, bytes]:
        return netmig.network_state_payloads(root, migration_target=migration_target)

    def _migrate_legacy_network_state(self) -> None:
        netmig.migrate_legacy_network_state(self.system_root, self._deployment_owner())

    @staticmethod
    def _disable_profile_autoconnect(payload: bytes) -> bytes:
        return netmig.disable_profile_autoconnect(payload)

    def _disable_managed_wifi_autoconnect(self) -> None:
        netmig.disable_managed_wifi_autoconnect(
            self.system_root,
            self._deployment_owner(),
            self.runner,
        )

    def _managed_identity_path(self, path: Path) -> bool:
        return cfg.managed_identity_path(self.config_root, path)

    def _prepare_identity_parent(self, path: Path, group_gid: int) -> None:
        uid, _ = self._deployment_owner()
        cfg.prepare_identity_parent(self.config_root, path, uid, group_gid)

    def _secure_identity_file(self, path: Path, mode: int, group_gid: int) -> bytes:
        uid, _ = self._deployment_owner()
        return cfg.secure_identity_file(path, mode, uid, group_gid)

    @staticmethod
    def _valid_bearer_token(payload: bytes) -> bool:
        return cfg.valid_bearer_token(payload)

    def _ensure_customer_identity(self, config: Mapping[str, object]) -> dict[str, object]:
        uid, _ = self._deployment_owner()
        group_gid = self.group_resolver("rp-ylx")
        return cfg.ensure_customer_identity(
            config,
            config_root=self.config_root,
            uid=uid,
            group_gid=group_gid,
            tls_material_generator=self.tls_material_generator,
        )

    def _default_device_config(self) -> dict[str, object]:
        return cfg.default_device_config(self.state_root)

    def _ensure_device_config(self) -> Mapping[str, object]:
        path = self.config_root / "device.json"
        if path.exists():
            _require_regular_file(path, code="production_config_invalid")
            try:
                value = json.loads(path.read_bytes())
            except (OSError, json.JSONDecodeError) as error:
                raise DeploymentError("production_config_invalid", "device.json 不可读") from error
            if not isinstance(value, dict):
                raise DeploymentError("production_config_invalid", "device.json 必须是 object")
            config: dict[str, object] = value
        else:
            config = self._default_device_config()
        return self._write_active_device_config(_json_bytes(config))

    @staticmethod
    def _device_config_profile(payload: bytes) -> tuple[str, bool]:
        return cfg.device_config_profile(payload)

    def _install_mdns_asset(
        self,
        *,
        tls_enabled: bool,
        release_package_root: Path | None = None,
    ) -> None:
        pkg_root = release_package_root or self._active_release_assets
        cfg.install_mdns_asset(
            self.system_root,
            tls_enabled=tls_enabled,
            release_package_root=pkg_root,
        )

    def _write_active_device_config(self, payload: bytes) -> Mapping[str, object]:
        uid, _ = self._deployment_owner()
        group_gid = self.group_resolver("rp-ylx")
        return cfg.write_active_device_config(
            payload,
            config_root=self.config_root,
            system_root=self.system_root,
            uid=uid,
            group_gid=group_gid,
            mdns_installer=lambda tls: self._install_mdns_asset(tls_enabled=tls),
        )

    def _device_config_snapshot(self, commit: str) -> Path:
        return cfg.device_config_snapshot(self.config_root, commit)

    def _snapshot_device_config(self, commit: str) -> None:
        cfg.snapshot_device_config(self.config_root, commit, self._deployment_owner())

    def _restore_device_config_snapshot(self, commit: str) -> None:
        cfg.restore_device_config_snapshot(
            self.config_root,
            commit,
            self._write_active_device_config,
        )

    def _install_assets(self, *, release: Path | None = None) -> None:
        if release is not None:
            candidate = release / "site-packages/rp_ylx"
            self._active_release_assets = (
                candidate
                if (candidate / "deploy").is_dir() or (candidate / "deployment.py").is_file()
                else None
            )
        else:
            self._active_release_assets = None
        systemd.install_assets(self.system_root, release=release)

    def _wait_for_health(self) -> None:
        request, context = self._health_request()
        deadline = time.monotonic() + ACTIVATION_HEALTH_TIMEOUT_SECONDS
        last_error = "service is not active"
        while time.monotonic() < deadline:
            try:
                active = subprocess.run(
                    ["systemctl", "is-active", "--quiet", "rp-ylx.service"],
                    check=False,
                    timeout=5,
                )
            except (OSError, subprocess.SubprocessError) as error:
                active = None
                last_error = str(error)
            if active is not None and active.returncode == 0:
                try:
                    with urllib.request.urlopen(request, timeout=2, context=context) as response:
                        if response.status == 200:
                            return
                        last_error = f"Device API HTTP {response.status}"
                except (OSError, urllib.error.URLError) as error:
                    last_error = str(error)
            time.sleep(0.25)
        raise DeploymentError(
            "service_unhealthy", f"Conductor activation health check timed out: {last_error}"
        )

    def _health_request(self) -> tuple[urllib.request.Request, ssl.SSLContext | None]:
        path = self.config_root / "device.json"
        _require_regular_file(path, code="production_config_invalid")
        try:
            config = json.loads(path.read_bytes())
        except (OSError, json.JSONDecodeError) as error:
            raise DeploymentError("production_config_invalid", "device.json 不可读") from error
        listen = config.get("listen") if isinstance(config, dict) else None
        security = config.get("security") if isinstance(config, dict) else None
        port = listen.get("port") if isinstance(listen, Mapping) else None
        profile = security.get("profile") if isinstance(security, Mapping) else None
        if type(port) is not int or not 1 <= port <= 65535 or profile not in {"lab", "customer"}:
            raise DeploymentError("production_config_invalid", "健康检查配置无效")

        scheme = "https" if profile == "customer" else "http"
        headers: dict[str, str] = {}
        context: ssl.SSLContext | None = None
        if profile == "customer":
            certificate_raw = security.get("tls_certificate_file")
            token_raw = security.get("bearer_token_file")
            if not isinstance(certificate_raw, str) or not isinstance(token_raw, str):
                raise DeploymentError("production_config_invalid", "Customer 健康检查身份缺失")
            certificate = Path(certificate_raw)
            token_path = Path(token_raw)
            _require_regular_file(certificate, code="customer_identity_invalid")
            _require_regular_file(token_path, code="customer_identity_invalid")
            try:
                token_payload = token_path.read_bytes()
            except OSError as error:
                raise DeploymentError(
                    "customer_identity_invalid",
                    "Customer Bearer token 不可读",
                ) from error
            if not self._valid_bearer_token(token_payload):
                raise DeploymentError("customer_identity_invalid", "Bearer token 格式无效")
            token = token_payload.rstrip(b"\r\n").decode("ascii")
            headers["Authorization"] = f"Bearer {token}"
            try:
                context = ssl.create_default_context(cafile=str(certificate))
            except (OSError, ssl.SSLError) as error:
                raise DeploymentError(
                    "customer_identity_invalid",
                    "无法以设备证书建立 Customer 健康检查信任",
                ) from error
            context.minimum_version = ssl.TLSVersion.TLSv1_2
            context.verify_mode = ssl.CERT_REQUIRED
            context.check_hostname = True
            partial_chain = getattr(ssl, "VERIFY_X509_PARTIAL_CHAIN", 0)
            if partial_chain:
                context.verify_flags |= partial_chain

        return (
            urllib.request.Request(
                f"{scheme}://127.0.0.1:{port}/api/v4/device",
                headers=headers,
            ),
            context,
        )

    def _record_activation_failure(self, error: DeploymentError, commit: str) -> None:
        self._write_json_atomic(
            self.state_root / "activation-failure.json",
            {
                "schema": "ylx.activation-failure.v1",
                "release_commit": commit,
                "code": error.code,
                "message": error.message,
            },
            mode=0o600,
        )

    def _run_unit_commands(
        self,
        commands: tuple[list[str], ...] | list[list[str]],
        *,
        code: str,
        message: str,
    ) -> None:
        systemd.run_unit_commands(self.runner, commands, code=code, message=message)

    def _quiesce_release_services(self) -> None:
        systemd.quiesce_release_services(self.runner, self.unit_load_state_reader)

    def _start_release_services(self, *, check_health: bool) -> None:
        with self._service_recovery_readers():
            systemd.start_release_services(
                self.runner,
                self.config_root,
                self.system_root,
                check_health=check_health,
                health_checker=self.health_checker,
            )

    def _deactivate_systemd_units(self) -> None:
        systemd.deactivate_systemd_units(self.runner, self.system_root)

    def _restore_release(self, old_current: str, failed_release: str) -> None:
        self._quiesce_release_services()
        transaction = self._transaction_document(
            "rollback",
            failed_release,
            old_current,
            "prepared",
        )
        self._write_json_atomic(self.transaction, transaction, mode=0o600)
        self._install_assets(release=self.releases / old_current)
        self._restore_device_config_snapshot(old_current)
        self._settle(transaction)
        self.runner(["systemctl", "daemon-reload"])
        self._start_release_services(check_health=False)

    def _revert_release_switch(
        self,
        old_current: str | None,
        new_current: str,
    ) -> None:
        current = self._link_target("current")
        if current not in {old_current, new_current}:
            raise DeploymentError(
                "install_state_invalid",
                "release 切换失败后 current 指向未知目标",
            )
        self._switch("current", old_current)
        self._switch("previous", new_current)
        self.transaction.unlink(missing_ok=True)
        _fsync_directory(self.state_root)

    def _activate(self, old_current: str | None, new_current: str) -> Mapping[str, str] | None:
        release_changed = old_current != new_current
        try:
            self._start_release_services(check_health=True)
        except DeploymentError as error:
            if self._recovery_descriptor is not None and not self._recovery_exclusive:
                # Reacquiring the write guard failed: do not mutate state while
                # a service recovery reader may still be active.
                raise
            diagnostic_error: BaseException | None = None
            try:
                self._record_activation_failure(error, new_current)
            except BaseException as failure:
                # Recording why activation failed must not prevent the required
                # stop/rollback, even when the diagnostic write is cancelled.
                diagnostic_error = failure
            diagnostic_note = (
                ""
                if diagnostic_error is None
                else f"；激活失败诊断未能保存（{type(diagnostic_error).__name__}）"
            )
            try:
                if old_current is None:
                    self._deactivate_systemd_units()
                    self._switch("current", None)
                    self._switch("previous", new_current)
                elif release_changed:
                    self._restore_release(old_current, new_current)
                else:
                    self._deactivate_systemd_units()
            except BaseException as cleanup_error:
                detail = getattr(cleanup_error, "message", str(cleanup_error))
                failure = DeploymentError(
                    "activation_cleanup_failed",
                    f"release {new_current} 激活失败，且无法安全停止或恢复服务：{detail}"
                    f"{diagnostic_note}",
                )
                if not isinstance(cleanup_error, Exception):
                    cleanup_error.add_note(str(failure))
                    raise cleanup_error from error
                if diagnostic_error is not None and not isinstance(diagnostic_error, Exception):
                    diagnostic_error.add_note(str(failure))
                    raise diagnostic_error from cleanup_error
                raise failure from cleanup_error
            if diagnostic_error is not None:
                if not isinstance(diagnostic_error, Exception):
                    raise diagnostic_error from error
                raise DeploymentError(error.code, f"{error.message}{diagnostic_note}") from error
            raise
        # Health and release activation have succeeded. Optional diagnostic
        # cleanup must not send that success through the rollback/error path.
        try:
            (self.state_root / "activation-failure.json").unlink(missing_ok=True)
        except OSError as error:
            return {
                "code": "activation_diagnostic_cleanup_failed",
                "message": "固件已激活，但旧激活失败诊断未能清理"
                f"（{type(error).__name__}）；保留文件供检查",
            }
        return None

    def install(self, bundle_path: str | Path, *, activate: bool = True) -> Mapping[str, object]:
        self._require_target()
        with self._maintenance_operation():
            return self._install_locked(bundle_path, activate=activate)

    def _install_locked(
        self, bundle_path: str | Path, *, activate: bool = True
    ) -> Mapping[str, object]:
        self._ensure_layout()
        self._recover_locked()
        with self._maintenance_lease():
            bundle = load_bundle(bundle_path)
            release_dir = self._prepare_release(bundle)
            self._require_state_compatible_release(bundle.commit)
            if self.executable_finder("logrotate") is None:
                # Older boot media and updaters do not provision this dependency.
                name = "logrotate_3.19.0-1ubuntu1.1_arm64.deb"
                package = release_dir / "site-packages/rp_ylx/deploy" / name
                if (
                    package.is_symlink()
                    or not package.is_file()
                    or _sha256(package)
                    != "bd11fab1ae9c442f188fa06abfbc400365babbe1778afa53dd922968d31da106"
                ):
                    raise DeploymentError("runtime_dependency_missing", "离线日志轮换工具校验失败")
                self.runner(["dpkg", "-i", str(package)])
            current = self._link_target("current")
            old_current = current
            quiesced = old_current is not None and (activate or old_current != bundle.commit)
            if quiesced:
                self._quiesce_release_services()
            transaction_started = False
            old_snapshot_written = False
            try:
                self._install_assets(release=release_dir)
                self.runner(["systemd-sysusers"])
                self.runner(["systemd-tmpfiles", "--create", "rp-ylx.conf"])
                if not (self.runner is systemd._run and self.system_root != Path("/")):
                    self.runner(["/usr/local/sbin/rp-ylx-device-login"])
                if old_current is not None:
                    self._snapshot_device_config(old_current)
                    old_snapshot_written = True
                self._migrate_legacy_network_state()
                self._disable_managed_wifi_autoconnect()
                if current != bundle.commit:
                    transaction = self._transaction_document(
                        "install",
                        current,
                        bundle.commit,
                        "prepared",
                    )
                    self._write_json_atomic(self.transaction, transaction, mode=0o600)
                    transaction_started = True
                self._ensure_device_config()
                self._snapshot_device_config(bundle.commit)
                self.runner(["systemctl", "daemon-reload"])
                if current != bundle.commit:
                    self._settle(transaction)
            except BaseException:
                try:
                    if old_current is not None:
                        self._install_assets(release=self.releases / old_current)
                        if old_snapshot_written:
                            self._restore_device_config_snapshot(old_current)
                    if transaction_started:
                        self._revert_release_switch(old_current, bundle.commit)
                    if quiesced:
                        self.runner(["systemctl", "daemon-reload"])
                        self._start_release_services(check_health=False)
                except BaseException as recovery_error:
                    detail = getattr(recovery_error, "message", str(recovery_error))
                    raise DeploymentError(
                        "install_recovery_failed",
                        f"升级失败，且无法恢复原 release 服务或配置：{detail}",
                    ) from recovery_error
                raise
            warning = self._activate(old_current, bundle.commit) if activate else None
            result = dict(self.status())
            if warning is not None:
                result["warnings"] = [warning]
            return result

    def _require_state_compatible_release(self, commit: str) -> None:
        storage.require_state_compatible_release(
            self.install_root,
            self.state_root,
            self.system_root,
            commit,
        )

    def rollback(self, *, activate: bool = True) -> Mapping[str, object]:
        self._require_target()
        with self._maintenance_operation():
            return self._rollback_locked(activate=activate)

    def _rollback_locked(self, *, activate: bool = True) -> Mapping[str, object]:
        self._ensure_layout()
        self._recover_locked()
        with self._maintenance_lease():
            current = self._link_target("current")
            previous = self._link_target("previous")
            if current is None or previous is None:
                raise DeploymentError("rollback_unavailable", "没有可回滚的上一版本")
            self._release_metadata(self.releases / previous)
            self._require_state_compatible_release(previous)
            self._quiesce_release_services()
            transaction_started = False
            current_snapshot_written = False
            try:
                self._snapshot_device_config(current)
                current_snapshot_written = True
                transaction = self._transaction_document("rollback", current, previous, "prepared")
                self._write_json_atomic(self.transaction, transaction, mode=0o600)
                transaction_started = True
                self._install_assets(release=self.releases / previous)
                self._restore_device_config_snapshot(previous)
                self._settle(transaction)
                self.runner(["systemctl", "daemon-reload"])
            except BaseException:
                try:
                    self._install_assets(release=self.releases / current)
                    if current_snapshot_written:
                        self._restore_device_config_snapshot(current)
                    if transaction_started:
                        self._revert_release_switch(current, previous)
                    self.runner(["systemctl", "daemon-reload"])
                    self._start_release_services(check_health=False)
                except BaseException as recovery_error:
                    detail = getattr(recovery_error, "message", str(recovery_error))
                    raise DeploymentError(
                        "rollback_recovery_failed",
                        f"回滚切换失败，且无法恢复原 release 服务或配置：{detail}",
                    ) from recovery_error
                raise
            warning = self._activate(current, previous) if activate else None
            result = dict(self.status())
            if warning is not None:
                result["warnings"] = [warning]
            return result

    def uninstall(self) -> Mapping[str, object]:
        self._require_platform()
        with self._maintenance_operation():
            return self._uninstall_locked()

    def _uninstall_locked(self) -> Mapping[str, object]:
        with self._maintenance_lease():
            self.runner(["systemctl", "disable", "--now", "openaria-update.service"])
            self._deactivate_systemd_units()
            if self.install_root.exists() and not self.install_root.is_symlink():
                shutil.rmtree(self.install_root)
            self.transaction.unlink(missing_ok=True)
            for name, (relative, _) in DEPLOYMENT_ASSETS.items():
                if name in PRESERVED_DEPLOYMENT_ASSETS:
                    continue
                target = self.system_root / relative
                target.unlink(missing_ok=True)
            (self.system_root / "usr/local/sbin/rp-ylx-deploy").unlink(missing_ok=True)
            bootstrap_root = self.system_root / "usr/local/lib/rp-ylx"
            if bootstrap_root.exists() and not bootstrap_root.is_symlink():
                shutil.rmtree(bootstrap_root)
            (self.system_root / "usr/local/sbin/openaria-update").unlink(missing_ok=True)
            update_root = self.system_root / "usr/local/lib/openaria"
            if update_root.is_dir() and not update_root.is_symlink():
                shutil.rmtree(update_root)
            self.runner(["systemctl", "daemon-reload"])
            return {"installed": False, "config_preserved": True, "state_preserved": True}

    def status(self) -> Mapping[str, object]:
        activation_failure: Mapping[str, object] | None = None
        diagnostic_path = self.state_root / "activation-failure.json"
        reader = facade_attr("_read_deployment_json", _read_deployment_json)
        try:
            try:
                diagnostic_path.lstat()
            except FileNotFoundError:
                pass
            else:
                diagnostic = reader(diagnostic_path, maximum=MAX_ACTIVATION_DIAGNOSTIC_BYTES)
                if (
                    not isinstance(diagnostic, dict)
                    or diagnostic.get("schema") != "ylx.activation-failure.v1"
                    or not isinstance(diagnostic.get("code"), str)
                    or not isinstance(diagnostic.get("message"), str)
                ):
                    raise ValueError("invalid activation diagnostic")
                if "release_commit" in diagnostic:
                    _canonical_commit(diagnostic["release_commit"])
                activation_failure = diagnostic
        except (OSError, ValueError, TypeError, RecursionError, DeploymentError):
            # Diagnostics are optional: preserve the basic status and the original
            # file, while making the read failure visible. Retry on every query.
            activation_failure = {
                "schema": "ylx.activation-failure.v1",
                "code": "diagnostic_unreadable",
                "message": "activation failure diagnostic is unreadable",
            }
        current = self._link_target("current")
        return {
            "installed": current is not None,
            "current": current,
            "previous": self._link_target("previous"),
            "transaction_pending": os.path.lexists(self.transaction),
            "activation_failure": activation_failure,
        }

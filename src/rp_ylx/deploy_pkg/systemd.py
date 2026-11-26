"""Systemd unit control, asset staging from target release, and readiness signaling."""

from __future__ import annotations

import grp
import json
import os
import socket
import subprocess
from collections.abc import Callable, Sequence
from importlib.resources import files
from pathlib import Path

from .bundle import Bundle
from .schema import (
    CORE_SYSTEMD_UNITS,
    DEPLOYMENT_ASSETS,
    NETWORK_CONTROL_SERVICE,
    NETWORK_CONTROL_SOCKET,
    PRESERVED_DEPLOYMENT_ASSETS,
    RECOVER_SERVICE,
    SUPPORTING_DEPLOYMENT_ASSETS,
    WATCHDOG_SERVICE,
    WATCHDOG_TIMER,
    DeploymentError,
    _json_bytes,
    _require_regular_file,
    _write_bytes_atomic,
)

Runner = Callable[[Sequence[str]], None]
StageInstaller = Callable[[Bundle, Path], None]
TlsMaterialGenerator = Callable[[Path, Path, str], None]
GroupResolver = Callable[[str], int]
UnitLoadStateReader = Callable[[str], str]


def _local_package_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _group_gid(name: str) -> int:
    try:
        return grp.getgrnam(name).gr_gid
    except KeyError as error:
        raise DeploymentError("service_identity_missing", f"系统组不存在：{name}") from error


def _run(arguments: Sequence[str]) -> None:
    try:
        subprocess.run(list(arguments), check=True, timeout=300)
    except (OSError, subprocess.SubprocessError) as error:
        raise DeploymentError("command_failed", f"命令失败：{' '.join(arguments)}") from error


def _systemd_unit_load_state(unit: str) -> str:
    try:
        completed = subprocess.run(
            ["systemctl", "show", "--property=LoadState", "--value", unit],
            check=False,
            capture_output=True,
            text=True,
            timeout=10,
        )
    except (OSError, subprocess.SubprocessError) as error:
        raise DeploymentError(
            "unit_state_query_failed",
            f"无法查询 systemd unit 状态：{unit}",
        ) from error
    state = completed.stdout.strip()
    if completed.returncode != 0 or not state or "\n" in state:
        raise DeploymentError(
            "unit_state_query_failed",
            f"systemd unit 状态无效：{unit}",
        )
    return state


def _system_launcher() -> bytes:
    return b"""#!/bin/sh
set -eu
RUNTIME=/opt/rp-ylx/current/runtime/bin/python3
if [ ! -x "$RUNTIME" ]; then
    RUNTIME=/opt/rp-ylx/previous/runtime/bin/python3
    if [ ! -x "$RUNTIME" ]; then
        echo '{"code":"runtime_dependency_missing",'\
'"message":"package-owned runtime unavailable"}' >&2
        exit 2
    fi
fi
exec "$RUNTIME" /usr/local/lib/rp-ylx/deployment.py "$@"
"""


def _asset_bytes(name: str, *, release_package_root: Path | None = None) -> bytes:
    if release_package_root is not None:
        from_release = release_package_root / "deploy" / name
        if from_release.is_file():
            return from_release.read_bytes()
    pkg_root = _local_package_root()
    standalone = pkg_root / name
    if standalone.is_file():
        return standalone.read_bytes()
    packaged = pkg_root / "deploy" / name
    if packaged.is_file():
        return packaged.read_bytes()
    try:
        return files("rp_ylx.deploy").joinpath(name).read_bytes()
    except (ModuleNotFoundError, OSError, TypeError):
        pass
    raise DeploymentError("install_resource_missing", f"缺少部署资源：{name}")


def _module_bytes(*, release_package_root: Path | None = None) -> bytes:
    if release_package_root is not None:
        from_release = release_package_root / "deployment.py"
        if from_release.is_file():
            return from_release.read_bytes()
    module = _local_package_root() / "deployment.py"
    if module.is_file():
        return module.read_bytes()
    try:
        return files("rp_ylx").joinpath("deployment.py").read_bytes()
    except (ModuleNotFoundError, OSError, TypeError):
        pass
    raise DeploymentError("install_resource_missing", "无法读取部署模块自身")


def _support_module_bytes(name: str, *, release_package_root: Path | None = None) -> bytes:
    if release_package_root is not None:
        from_release = release_package_root / name
        if from_release.is_file():
            return from_release.read_bytes()
    sibling = _local_package_root() / name
    if sibling.is_file():
        return sibling.read_bytes()
    return files("rp_ylx").joinpath(name).read_bytes()


def _stage_package_tree(source_dir: Path | None, destination_dir: Path, resource_pkg: str) -> None:
    files_to_stage: dict[str, bytes] = {}
    if source_dir is not None and source_dir.is_dir():
        for entry in sorted(source_dir.iterdir(), key=lambda p: p.name):
            if entry.is_file() and not entry.is_symlink() and entry.suffix == ".py":
                files_to_stage[entry.name] = entry.read_bytes()
    else:
        try:
            tree = files(resource_pkg)
            for entry in sorted(tree.iterdir(), key=lambda p: p.name):
                if entry.is_file() and entry.name.endswith(".py"):
                    files_to_stage[entry.name] = entry.read_bytes()
        except (ModuleNotFoundError, OSError, TypeError):
            return
    if not files_to_stage:
        return
    destination_dir.mkdir(parents=True, exist_ok=True, mode=0o755)
    destination_dir.chmod(0o755)
    for existing in list(destination_dir.iterdir()):
        if existing.is_file() and existing.suffix == ".py" and existing.name not in files_to_stage:
            existing.unlink(missing_ok=True)
    for name, payload in files_to_stage.items():
        _write_bytes_atomic(destination_dir / name, payload, 0o644)


def _find_optional_release_signing_key(
    release_package_root: Path | None,
) -> bytes | None:
    roots = [r for r in (release_package_root, _local_package_root()) if r is not None]
    for root in roots:
        for candidate in (root / "release-signing.pub", root / "deploy/release-signing.pub"):
            if candidate.is_file() and not candidate.is_symlink():
                return candidate.read_bytes()
    return None


def _ensure_parent_dirs(directory: Path, root: Path, mode: int = 0o755) -> None:
    pending: list[Path] = []
    current = directory
    while current != root and not current.exists():
        pending.append(current)
        current = current.parent
    for entry in reversed(pending):
        entry.mkdir(exist_ok=True, mode=mode if entry == directory else 0o755)
        entry.chmod(mode if entry == directory else 0o755)
    if directory != root:
        directory.chmod(mode)


def install_assets(system_root: Path, *, release: Path | None = None) -> None:
    release_package_root: Path | None = None
    if release is not None:
        candidate = release / "site-packages/rp_ylx"
        if (candidate / "deploy").is_dir() or (candidate / "deployment.py").is_file():
            release_package_root = candidate

    targets = {
        name: (system_root / relative, mode) for name, (relative, mode) in DEPLOYMENT_ASSETS.items()
    }
    bootstrap_root = system_root / "usr/local/lib/rp-ylx"
    bootstrap_module = bootstrap_root / "deployment.py"
    bootstrap_root.mkdir(parents=True, exist_ok=True, mode=0o755)
    bootstrap_root.chmod(0o755)
    _write_bytes_atomic(
        bootstrap_module,
        _module_bytes(release_package_root=release_package_root),
        0o644,
    )

    local_root = _local_package_root()
    deploy_pkg_source = (
        release_package_root / "deploy_pkg"
        if release_package_root is not None and (release_package_root / "deploy_pkg").is_dir()
        else (local_root / "deploy_pkg" if (local_root / "deploy_pkg").is_dir() else None)
    )
    _stage_package_tree(deploy_pkg_source, bootstrap_root / "deploy_pkg", "rp_ylx.deploy_pkg")

    core_source = (
        release_package_root / "core"
        if release_package_root is not None and (release_package_root / "core").is_dir()
        else (local_root / "core" if (local_root / "core").is_dir() else None)
    )
    _stage_package_tree(core_source, bootstrap_root / "core", "rp_ylx.core")

    update_root = system_root / "usr/local/lib/openaria"
    update_root.mkdir(parents=True, exist_ok=True, mode=0o755)
    update_root.chmod(0o755)
    for name in ("update.py", "firmware_control.py"):
        source = _support_module_bytes(name, release_package_root=release_package_root)
        _write_bytes_atomic(bootstrap_root / name, source, 0o644)
        _write_bytes_atomic(update_root / name, source, 0o644)
    signing_key = _find_optional_release_signing_key(release_package_root)
    if signing_key is not None:
        _write_bytes_atomic(bootstrap_root / "release-signing.pub", signing_key, 0o644)
        _write_bytes_atomic(update_root / "release-signing.pub", signing_key, 0o644)

    update_launcher = system_root / "usr/local/sbin/openaria-update"
    _ensure_parent_dirs(update_launcher.parent, system_root, 0o755)
    _write_bytes_atomic(
        update_launcher,
        b'#!/bin/sh\nexec /usr/bin/python3 /usr/local/lib/openaria/update.py "$@"\n',
        0o755,
    )
    for name, (_, mode) in targets.items():
        bootstrap_asset = bootstrap_root / name
        _write_bytes_atomic(
            bootstrap_asset,
            _asset_bytes(name, release_package_root=release_package_root),
            mode,
        )
    for name, mode in SUPPORTING_DEPLOYMENT_ASSETS.items():
        _write_bytes_atomic(
            bootstrap_root / name,
            _asset_bytes(name, release_package_root=release_package_root),
            mode,
        )

    bootstrap = system_root / "usr/local/sbin/rp-ylx-deploy"
    _ensure_parent_dirs(bootstrap.parent, system_root, 0o755)
    _write_bytes_atomic(bootstrap, _system_launcher(), 0o755)

    for name, (target, mode) in targets.items():
        if name == "rp-ylx.avahi":
            continue
        if name in PRESERVED_DEPLOYMENT_ASSETS and target.exists():
            if name == "rp-ylx-recording-button.json":
                # Older installs shipped enabled=false. Every deployment
                # restores the product default without losing wiring/LED settings.
                _require_regular_file(target, code="production_config_invalid")
                try:
                    config = json.loads(target.read_bytes())
                except (UnicodeDecodeError, json.JSONDecodeError) as error:
                    raise DeploymentError(
                        "production_config_invalid", "recording-button.json 不可读"
                    ) from error
                if not isinstance(config, dict):
                    raise DeploymentError(
                        "production_config_invalid", "recording-button.json 必须是对象"
                    )
                if config.get("enabled") is not True:
                    config["enabled"] = True
                    _write_bytes_atomic(target, _json_bytes(config), mode)
            continue
        parent_mode = 0o750 if target.parent == system_root / "etc/rp-ylx" else 0o755
        _ensure_parent_dirs(target.parent, system_root, parent_mode)
        _write_bytes_atomic(
            target,
            _asset_bytes(name, release_package_root=release_package_root),
            mode,
        )


def _notify_systemd_ready(status: str) -> None:
    address = os.environ.get("NOTIFY_SOCKET")
    if not address:
        return
    if address.startswith("@"):
        address = "\0" + address[1:]
    try:
        with socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM) as notifier:
            notifier.connect(address)
            notifier.sendall(f"READY=1\nSTATUS={status}".encode())
    except OSError as error:
        raise DeploymentError(
            "readiness_notification_failed",
            "无法通知 systemd 网络控制兼容 launcher 已就绪",
        ) from error


def run_unit_commands(
    runner: Runner,
    commands: Sequence[Sequence[str]],
    *,
    code: str,
    message: str,
) -> None:
    failures: list[str] = []
    for command in commands:
        try:
            runner(command)
        except DeploymentError as error:
            failures.append(f"{' '.join(command)} ({error.code})")
    if failures:
        raise DeploymentError(code, f"{message}：{'; '.join(failures)}")


def quiesce_release_services(
    runner: Runner,
    unit_load_state_reader: UnitLoadStateReader,
) -> None:
    try:
        recover_load_state = unit_load_state_reader(RECOVER_SERVICE)
    except DeploymentError as error:
        raise DeploymentError(
            "unit_quiesce_failed",
            f"无法确认部署相关服务状态；拒绝切换 release：{RECOVER_SERVICE} ({error.code})",
        ) from error
    if not recover_load_state or "\n" in recover_load_state:
        raise DeploymentError(
            "unit_quiesce_failed",
            f"无法确认部署相关服务状态；拒绝切换 release：{RECOVER_SERVICE}",
        )
    commands = [
        ["systemctl", "disable", "--now", WATCHDOG_TIMER],
        ["systemctl", "stop", WATCHDOG_SERVICE],
        ["systemctl", "stop", "rp-ylx.service"],
        ["systemctl", "stop", "rp-ylx-network-control.socket"],
        ["systemctl", "stop", NETWORK_CONTROL_SERVICE],
    ]
    if recover_load_state != "not-found":
        commands.append(["systemctl", "stop", RECOVER_SERVICE])
    run_unit_commands(
        runner,
        commands,
        code="unit_quiesce_failed",
        message="无法确认部署相关服务全部停止；拒绝切换 release",
    )


def start_release_services(
    runner: Runner,
    config_root: Path,
    system_root: Path,
    *,
    check_health: bool,
    health_checker: Callable[[], None],
) -> None:
    # Independent of capture/network units: never stop this supervisor while
    # it is executing the transaction that restarts those services.
    runner(["systemctl", "try-restart", "systemd-journald.service"])
    runner(["systemctl", "enable", "--now", "logrotate.timer"])
    runner(["systemctl", "restart", "logrotate.timer"])
    runner(["systemctl", "enable", "--now", "openaria-update.service"])
    for unit in CORE_SYSTEMD_UNITS:
        runner(["systemctl", "enable", "--now", unit])
    if check_health:
        health_checker()
    button_path = config_root / "recording-button.json"
    if not os.path.lexists(button_path):
        fallback = system_root / "etc/rp-ylx/recording-button.json"
        if os.path.lexists(fallback):
            button_path = fallback
    _require_regular_file(button_path, code="production_config_invalid")
    try:
        button_config = json.loads(button_path.read_bytes())
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise DeploymentError(
            "production_config_invalid", "recording-button.json 不可读"
        ) from error
    if not isinstance(button_config, dict):
        raise DeploymentError("production_config_invalid", "recording-button.json 必须是对象")
    button_action = "enable" if button_config.get("enabled", True) is True else "disable"
    runner(["systemctl", button_action, "--now", "rp-ylx-recording-button.service"])
    runner(["systemctl", "enable", "--now", WATCHDOG_TIMER])


def deactivate_systemd_units(runner: Runner, system_root: Path) -> None:
    run_unit_commands(
        runner,
        (
            ["systemctl", "disable", "--now", WATCHDOG_TIMER],
            ["systemctl", "stop", WATCHDOG_SERVICE],
            ["systemctl", "disable", "--now", "rp-ylx-recording-button.service"],
            ["systemctl", "disable", "--now", "rp-ylx.service"],
            ["systemctl", "disable", "--now", "rp-ylx-network-control.socket"],
            ["systemctl", "stop", NETWORK_CONTROL_SERVICE],
            ["systemctl", "stop", RECOVER_SERVICE],
            ["systemctl", "disable", "--now", "rp-ylx-data-volume.service"],
        ),
        code="unit_deactivation_failed",
        message="无法确认部署相关服务全部停用；保留安装内容",
    )
    try:
        (system_root / NETWORK_CONTROL_SOCKET).unlink(missing_ok=True)
    except OSError as error:
        raise DeploymentError(
            "unit_deactivation_failed",
            "服务已停用，但无法清理 network-control socket；保留安装内容",
        ) from error

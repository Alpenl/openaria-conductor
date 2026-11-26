"""Legacy network state and NetworkManager profile migration helpers."""

from __future__ import annotations

import hashlib
import json
import os
import re
import sqlite3
import stat
import tempfile
from collections.abc import Mapping
from contextlib import closing
from functools import cache
from pathlib import Path

from .schema import (
    FORBIDDEN_NETWORK_STATE_KEYS,
    LEGACY_NETWORK_STATE_RELATIVE,
    MANAGED_WIFI_PROFILE,
    MAX_NETWORK_STATE_MIGRATION_BYTES,
    MAX_NETWORK_STATE_MIGRATION_FILES,
    MAX_NETWORKMANAGER_PROFILE_BYTES,
    NETWORK_STATE_MIGRATION_MARKER,
    NETWORK_STATE_RELATIVE,
    NETWORKMANAGER_CONNECTIONS_RELATIVE,
    DeploymentError,
    _fsync_directory,
    _require_regular_file,
    _write_atomic,
    _write_bytes_atomic,
    facade_attr,
)

_SQLITE_APPLICATION_ID = 0x594C5801
_SQLITE_SCHEMA_VERSION = 1
_STATE_TABLES = {
    "documents": "name TEXT PRIMARY KEY, body TEXT NOT NULL, "
    "revision INTEGER NOT NULL, updated_at TEXT NOT NULL",
    "imports": "name TEXT PRIMARY KEY, source TEXT NOT NULL",
    "changes": "id INTEGER PRIMARY KEY, name TEXT NOT NULL, "
    "revision INTEGER NOT NULL, body TEXT NOT NULL, updated_at TEXT NOT NULL",
    "capture_commands": "principal TEXT NOT NULL, operation TEXT NOT NULL, "
    "idempotency_key TEXT NOT NULL, body TEXT NOT NULL, "
    "PRIMARY KEY(principal,operation,idempotency_key)",
    "transfers": "id TEXT PRIMARY KEY, volume TEXT NOT NULL, session TEXT NOT NULL, "
    "artifact TEXT NOT NULL, started_at TEXT NOT NULL, finished_at TEXT, "
    "status TEXT NOT NULL, bytes_read INTEGER NOT NULL",
}


@cache
def _expected_sqlite_columns() -> dict[str, tuple[tuple[object, ...], ...]]:
    with closing(sqlite3.connect(":memory:")) as db:
        for name, columns in _STATE_TABLES.items():
            db.execute(f"CREATE TABLE {name} ({columns})")
        return {name: tuple(db.execute(f"PRAGMA table_xinfo({name})")) for name in _STATE_TABLES}


def _validate_sqlite_schema(db: sqlite3.Connection) -> None:
    if (
        db.execute("PRAGMA application_id").fetchone()[0] != _SQLITE_APPLICATION_ID
        or db.execute("PRAGMA user_version").fetchone()[0] != _SQLITE_SCHEMA_VERSION
    ):
        raise ValueError("unsupported sqlite database identity or schema")
    objects = dict(db.execute("SELECT name, type FROM sqlite_master"))
    for name, columns in _expected_sqlite_columns().items():
        if (
            objects.get(name) != "table"
            or tuple(db.execute(f"PRAGMA table_xinfo({name})")) != columns
        ):
            raise ValueError(f"missing or incompatible sqlite table: {name}")


def network_state_contains_secret(value: object) -> bool:
    if isinstance(value, Mapping):
        return any(
            (isinstance(key, str) and key.casefold() in FORBIDDEN_NETWORK_STATE_KEYS)
            or network_state_contains_secret(item)
            for key, item in value.items()
        )
    if isinstance(value, list):
        return any(network_state_contains_secret(item) for item in value)
    return False


def _backup_controller_sqlite(entry: Path) -> bytes:
    metadata = _require_regular_file(entry, code="network_state_migration_unsafe")
    if metadata.st_mode & 0o077 or metadata.st_size > MAX_NETWORK_STATE_MIGRATION_BYTES:
        raise DeploymentError(
            "network_state_migration_unsafe",
            f"网络状态数据库权限或大小无效：{entry.name}",
        )
    with tempfile.TemporaryDirectory(prefix="ylx-network-backup-") as directory:
        backup = Path(directory) / entry.name
        try:
            with closing(
                sqlite3.connect(entry.absolute().as_uri() + "?mode=ro", uri=True, timeout=5)
            ) as source:
                source.execute("PRAGMA busy_timeout=5000")
                _validate_sqlite_schema(source)
                for (raw_body,) in source.execute("SELECT body FROM documents"):
                    parsed = json.loads(raw_body)
                    if network_state_contains_secret(parsed):
                        raise ValueError("forbidden secret key in controller database")
                with closing(sqlite3.connect(backup)) as target:
                    source.backup(target, pages=128)
                    if target.execute("PRAGMA integrity_check").fetchone()[0] != "ok":
                        raise ValueError("sqlite backup integrity check failed")
                    if target.execute("PRAGMA journal_mode=DELETE").fetchone()[0] != "delete":
                        raise ValueError("sqlite backup is not self-contained")
            return backup.read_bytes()
        except (
            OSError,
            sqlite3.Error,
            ValueError,
            TypeError,
            UnicodeDecodeError,
            json.JSONDecodeError,
        ) as error:
            raise DeploymentError(
                "network_state_migration_unsafe",
                "网络状态数据库不可安全迁移",
            ) from error


def read_network_state_payload(path: Path) -> bytes:
    metadata = _require_regular_file(path, code="network_state_migration_unsafe")
    if metadata.st_mode & 0o077 or metadata.st_size > MAX_NETWORK_STATE_MIGRATION_BYTES:
        raise DeploymentError(
            "network_state_migration_unsafe",
            f"旧网络状态文件权限或大小无效：{path.name}",
        )
    try:
        payload = path.read_bytes()
        value = json.loads(payload)
    except (OSError, json.JSONDecodeError) as error:
        raise DeploymentError(
            "network_state_migration_unsafe",
            f"旧网络状态文件不是有效 JSON：{path.name}",
        ) from error
    if not isinstance(value, dict):
        raise DeploymentError(
            "network_state_migration_unsafe",
            f"旧网络状态文件必须是 JSON object：{path.name}",
        )
    if network_state_contains_secret(value):
        raise DeploymentError(
            "network_state_migration_unsafe",
            f"旧网络状态文件包含禁止迁移的凭据字段：{path.name}",
        )
    return payload


def network_state_payloads(root: Path, *, migration_target: bool) -> dict[str, bytes]:
    try:
        metadata = root.stat(follow_symlinks=False)
    except FileNotFoundError:
        return {}
    except OSError as error:
        raise DeploymentError(
            "network_state_migration_unsafe",
            "网络状态目录不可读",
        ) from error
    if not stat.S_ISDIR(metadata.st_mode) or metadata.st_mode & 0o077:
        raise DeploymentError(
            "network_state_migration_unsafe",
            "网络状态目录必须是禁止组和其他用户访问的普通目录",
        )

    allowed_root_files = {
        "rescue.json",
        "journal.json",
        "lkg-wlan0.json",
        "lkg-eth0.json",
        "controller-state.json",
    }
    transient_files = {".lock", "operation.lock", "controller.sqlite3.init.lock"}
    payloads: dict[str, bytes] = {}
    try:
        entries = sorted(root.iterdir(), key=lambda path: path.name)
    except OSError as error:
        raise DeploymentError(
            "network_state_migration_unsafe",
            "无法枚举网络状态目录",
        ) from error
    for entry in entries:
        if entry.name in {"controller.sqlite3-wal", "controller.sqlite3-shm"}:
            _require_regular_file(entry, code="network_state_migration_unsafe")
            continue
        if entry.name == "controller.sqlite3":
            payloads[entry.name] = _backup_controller_sqlite(entry)
            continue
        if entry.name in {"controller.sqlite3.initialized", "controller-state.json.pre-sqlite"}:
            file_meta = _require_regular_file(entry, code="network_state_migration_unsafe")
            if file_meta.st_mode & 0o077 or file_meta.st_size > MAX_NETWORK_STATE_MIGRATION_BYTES:
                raise DeploymentError(
                    "network_state_migration_unsafe", "数据库迁移证据权限或大小无效"
                )
            payloads[entry.name] = entry.read_bytes()
            continue
        if entry.name == NETWORK_STATE_MIGRATION_MARKER and migration_target:
            continue
        if entry.name in transient_files:
            _require_regular_file(entry, code="network_state_migration_unsafe")
            continue
        if entry.name == ".idempotency-key":
            key_metadata = _require_regular_file(
                entry,
                code="network_state_migration_unsafe",
            )
            if key_metadata.st_mode & 0o077 or key_metadata.st_size != 32:
                raise DeploymentError(
                    "network_state_migration_unsafe",
                    "旧网络幂等密钥权限或大小无效",
                )
            try:
                payloads[entry.name] = entry.read_bytes()
            except OSError as error:
                raise DeploymentError(
                    "network_state_migration_unsafe",
                    "旧网络幂等密钥不可读",
                ) from error
            continue
        if entry.name == "requests":
            request_metadata = entry.stat(follow_symlinks=False)
            if not stat.S_ISDIR(request_metadata.st_mode) or request_metadata.st_mode & 0o077:
                raise DeploymentError(
                    "network_state_migration_unsafe",
                    "旧网络请求状态目录权限无效",
                )
            for receipt in sorted(entry.iterdir(), key=lambda path: path.name):
                if re.fullmatch(r"[0-9a-f]{64}\.json", receipt.name) is None:
                    raise DeploymentError(
                        "network_state_migration_unsafe",
                        f"旧网络请求状态包含未知文件：{receipt.name}",
                    )
                payloads[f"requests/{receipt.name}"] = read_network_state_payload(receipt)
            continue
        if entry.name not in allowed_root_files:
            raise DeploymentError(
                "network_state_migration_unsafe",
                f"网络状态目录包含未知条目：{entry.name}",
            )
        payloads[entry.name] = read_network_state_payload(entry)
    if (
        len(payloads) > MAX_NETWORK_STATE_MIGRATION_FILES
        or sum(len(payload) for payload in payloads.values()) > MAX_NETWORK_STATE_MIGRATION_BYTES
    ):
        raise DeploymentError(
            "network_state_migration_unsafe",
            "旧网络状态超过安全迁移上限",
        )
    return payloads


def migrate_legacy_network_state(system_root: Path, owner: tuple[int, int]) -> None:
    legacy = system_root / LEGACY_NETWORK_STATE_RELATIVE
    target = system_root / NETWORK_STATE_RELATIVE
    marker = target / NETWORK_STATE_MIGRATION_MARKER
    uid, gid = owner

    if os.path.lexists(marker):
        marker_metadata = _require_regular_file(marker, code="network_state_migration_unsafe")
        if marker_metadata.st_mode & 0o077:
            raise DeploymentError("network_state_migration_unsafe", "网络状态迁移标记权限无效")
        try:
            value = json.loads(marker.read_bytes())
        except (OSError, json.JSONDecodeError) as error:
            raise DeploymentError(
                "network_state_migration_unsafe", "网络状态迁移标记无效"
            ) from error
        if not isinstance(value, dict):
            raise DeploymentError("network_state_migration_unsafe", "网络状态迁移标记结构无效")
        digests = value.get("files")
        if (
            set(value) != {"schema", "source", "files"}
            or value.get("schema") != "ylx.network-state-migration.v1"
            or value.get("source") != "/var/lib/rp-ylx/network"
            or not isinstance(digests, dict)
        ):
            raise DeploymentError("network_state_migration_unsafe", "网络状态迁移标记结构无效")
        for relative, digest in digests.items():
            valid_relative = isinstance(relative, str) and (
                relative
                in {
                    ".idempotency-key",
                    "rescue.json",
                    "journal.json",
                    "lkg-wlan0.json",
                    "lkg-eth0.json",
                    "controller-state.json",
                    "controller.sqlite3",
                    "controller.sqlite3.initialized",
                    "controller-state.json.pre-sqlite",
                }
                or re.fullmatch(r"requests/[0-9a-f]{64}\.json", relative) is not None
            )
            if (
                not isinstance(relative, str)
                or not isinstance(digest, str)
                or re.fullmatch(r"[0-9a-f]{64}", digest) is None
                or not valid_relative
            ):
                raise DeploymentError(
                    "network_state_migration_unsafe",
                    "网络状态迁移标记包含无效文件摘要",
                )
        network_state_payloads(target, migration_target=True)
        return

    legacy_payloads = network_state_payloads(legacy, migration_target=False)
    if not legacy_payloads:
        return
    if os.path.lexists(target):
        target_metadata = target.stat(follow_symlinks=False)
        if not stat.S_ISDIR(target_metadata.st_mode):
            raise DeploymentError(
                "network_state_migration_unsafe",
                "新网络状态路径必须是普通目录且不能是符号链接",
            )
    else:
        target.mkdir(parents=True, mode=0o700)
    target.chmod(0o700)
    os.chown(target, uid, gid)
    existing_payloads = network_state_payloads(target, migration_target=True)
    extras = set(existing_payloads) - set(legacy_payloads)
    conflicts = {
        name
        for name in set(existing_payloads) & set(legacy_payloads)
        if existing_payloads[name] != legacy_payloads[name]
    }
    if extras or conflicts:
        raise DeploymentError(
            "network_state_migration_conflict",
            "新旧网络状态同时存在且内容不一致；拒绝选择任一侧",
        )
    for relative, payload in legacy_payloads.items():
        destination = target / relative
        destination.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        destination.parent.chmod(0o700)
        os.chown(destination.parent, uid, gid)
        if not destination.exists():
            _write_bytes_atomic(destination, payload, 0o600)
        destination.chmod(0o600)
        os.chown(destination, uid, gid)
    writer = facade_attr("_write_atomic", _write_atomic)
    writer(
        marker,
        {
            "schema": "ylx.network-state-migration.v1",
            "source": "/var/lib/rp-ylx/network",
            "files": {
                name: hashlib.sha256(payload).hexdigest()
                for name, payload in sorted(legacy_payloads.items())
            },
        },
    )
    marker.chmod(0o600)
    os.chown(marker, uid, gid)
    _fsync_directory(target)


def disable_profile_autoconnect(payload: bytes) -> bytes:
    if not payload or len(payload) > MAX_NETWORKMANAGER_PROFILE_BYTES or b"\0" in payload:
        raise DeploymentError(
            "network_profile_migration_unsafe",
            "受管 Wi-Fi profile 大小或编码无效",
        )
    lines = payload.splitlines(keepends=True)
    section: bytes | None = None
    connection_sections = 0
    autoconnect_lines: list[tuple[int, bytes, bytes]] = []
    for index, line in enumerate(lines):
        content = line.rstrip(b"\r\n")
        stripped = content.strip()
        if stripped.startswith(b"[") and stripped.endswith(b"]"):
            section = stripped[1:-1].strip().lower()
            if section == b"connection":
                connection_sections += 1
            continue
        if section != b"connection" or b"=" not in content:
            continue
        key, value = content.split(b"=", 1)
        if key.strip().lower() == b"autoconnect":
            autoconnect_lines.append((index, key, value))
    if connection_sections != 1 or len(autoconnect_lines) != 1:
        raise DeploymentError(
            "network_profile_migration_unsafe",
            "受管 Wi-Fi profile 必须包含唯一 connection.autoconnect",
        )
    index, key, value = autoconnect_lines[0]
    normalized = value.strip().lower()
    if normalized in {b"false", b"no", b"0"}:
        return payload
    if normalized not in {b"true", b"yes", b"1"}:
        raise DeploymentError(
            "network_profile_migration_unsafe",
            "受管 Wi-Fi profile 的 autoconnect 值无效",
        )
    line = lines[index]
    newline = line[len(line.rstrip(b"\r\n")) :]
    leading = value[: len(value) - len(value.lstrip())]
    trailing = value[len(value.rstrip()) :]
    lines[index] = key + b"=" + leading + b"false" + trailing + newline
    return b"".join(lines)


def disable_managed_wifi_autoconnect(
    system_root: Path,
    owner: tuple[int, int],
    runner: object,
) -> None:
    connections = system_root / NETWORKMANAGER_CONNECTIONS_RELATIVE
    try:
        metadata = connections.stat(follow_symlinks=False)
    except FileNotFoundError:
        return
    except OSError as error:
        raise DeploymentError(
            "network_profile_migration_unsafe",
            "NetworkManager profile 目录不可读",
        ) from error
    if not stat.S_ISDIR(metadata.st_mode) or metadata.st_mode & 0o022:
        raise DeploymentError(
            "network_profile_migration_unsafe",
            "NetworkManager profile 目录必须是不可组写和其他用户写入的普通目录",
        )
    uid, gid = owner
    changed = False
    try:
        profiles = sorted(connections.iterdir(), key=lambda path: path.name)
    except OSError as error:
        raise DeploymentError(
            "network_profile_migration_unsafe",
            "无法枚举 NetworkManager profile",
        ) from error
    for profile in profiles:
        if MANAGED_WIFI_PROFILE.fullmatch(profile.name) is None:
            continue
        profile_metadata = _require_regular_file(
            profile,
            code="network_profile_migration_unsafe",
        )
        if stat.S_IMODE(profile_metadata.st_mode) != 0o600:
            raise DeploymentError(
                "network_profile_migration_unsafe",
                "受管 Wi-Fi profile 权限必须为 0600",
            )
        try:
            payload = profile.read_bytes()
        except OSError as error:
            raise DeploymentError(
                "network_profile_migration_unsafe",
                "受管 Wi-Fi profile 不可读",
            ) from error
        migrated = disable_profile_autoconnect(payload)
        if migrated == payload:
            continue
        _write_bytes_atomic(profile, migrated, 0o600)
        os.chown(profile, uid, gid)
        changed = True
    if changed:
        runner(["nmcli", "connection", "reload"])  # type: ignore[operator]

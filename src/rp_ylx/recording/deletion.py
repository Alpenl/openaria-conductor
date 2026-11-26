"""Preflight and remove explicitly selected sealed recordings."""

from __future__ import annotations

import hashlib
import os
import re
import shutil
import stat
from contextlib import ExitStack
from http import HTTPStatus
from pathlib import Path

from rp_ylx.core.fs import O_CLOEXEC, O_DIRECTORY, O_NOFOLLOW, O_NONBLOCK
from rp_ylx.errors import ProviderError
from rp_ylx.manifest_limits import MAX_MANIFEST_BYTES
from rp_ylx.recording.fs_safety import is_exfat_or_msdos_fd
from rp_ylx.recording.manifest_builder import (
    DeviceRecordingError,
    inspect_device_session_directory,
    read_bounded_fd,
)
from rp_ylx.recording_layout import iter_recordings, open_relative_directory

SESSION_ID = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$")
SHA256 = re.compile(r"^[0-9a-f]{64}$")


def _verify_directory_manifest_digest(
    parent_fd: int,
    directory_name: str,
    *,
    expected_stat: os.stat_result,
    expected_sha256: str,
) -> None:
    """Pin directory_name via parent_fd and re-verify manifest.json SHA-256 on exFAT."""
    dir_fd = os.open(
        directory_name,
        os.O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC,
        dir_fd=parent_fd,
    )
    try:
        opened_dir = os.fstat(dir_fd)
        if (opened_dir.st_dev, opened_dir.st_mode, opened_dir.st_mtime_ns) != (
            expected_stat.st_dev,
            expected_stat.st_mode,
            expected_stat.st_mtime_ns,
        ):
            raise OSError("录制目录在删除前已变化")
        manifest_fd = os.open(
            "manifest.json",
            os.O_RDONLY | O_NOFOLLOW | O_NONBLOCK | O_CLOEXEC,
            dir_fd=dir_fd,
        )
        try:
            info = os.fstat(manifest_fd)
            if not stat.S_ISREG(info.st_mode) or info.st_size > MAX_MANIFEST_BYTES:
                raise OSError("录制清单在删除前已变化")
            payload = read_bounded_fd(manifest_fd, MAX_MANIFEST_BYTES, code="manifest_invalid")
        finally:
            os.close(manifest_fd)
        if hashlib.sha256(payload).hexdigest() != expected_sha256:
            raise OSError("录制清单在删除前已变化")
    except DeviceRecordingError as error:
        raise OSError("录制清单在删除前已变化") from error
    finally:
        os.close(dir_fd)


def valid_delete_request(body: object) -> bool:
    if not isinstance(body, dict) or set(body) != {"schema", "sessions"}:
        return False
    items = body.get("sessions")
    if body.get("schema") != "ylx.session-delete-request.v1" or not isinstance(items, list):
        return False
    if not 1 <= len(items) <= 200:
        return False
    seen = set()
    for item in items:
        if not isinstance(item, dict) or set(item) != {"session_id", "manifest_sha256"}:
            return False
        session_id, digest = item["session_id"], item["manifest_sha256"]
        if (
            not isinstance(session_id, str)
            or SESSION_ID.fullmatch(session_id) is None
            or not isinstance(digest, str)
            or SHA256.fullmatch(digest) is None
            or session_id in seen
        ):
            return False
        seen.add(session_id)
    return True


def delete_recordings(roots: tuple[Path, ...], expected: dict[str, str]) -> dict[str, object]:
    candidates: dict[str, tuple[int, Path, os.stat_result, dict[str, object], bytes, str]] = {}
    root_flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | getattr(os, "O_CLOEXEC", 0)
    with ExitStack() as stack:
        for root in roots:
            descriptor = os.open(root, root_flags)
            stack.callback(os.close, descriptor)
            for path, session_id in iter_recordings(root, root_fd=descriptor):
                if SESSION_ID.fullmatch(session_id) is None:
                    continue
                relative = path.relative_to(root)
                parent = descriptor
                opened_parent: int | None = None
                try:
                    if len(relative.parts) > 1:
                        opened_parent = open_relative_directory(descriptor, relative.parent)
                        parent = opened_parent
                    if path.is_symlink():
                        raise ValueError("symbolic link")
                    identity = os.stat(relative.name, dir_fd=parent, follow_symlinks=False)
                    manifest, payload = inspect_device_session_directory(
                        path, expected_session_id=session_id
                    )
                    if session_id in candidates:
                        raise ValueError("ambiguous recording identity")
                except (OSError, DeviceRecordingError, ValueError, KeyError) as error:
                    if session_id in expected:
                        raise ProviderError(
                            "catalog_unavailable",
                            "录制目录无法安全检查，请先修复或移除异常目录",
                            status=HTTPStatus.CONFLICT,
                        ) from error
                    continue
                finally:
                    if opened_parent is not None:
                        os.close(opened_parent)
                candidates[session_id] = (
                    descriptor,
                    relative.parent,
                    identity,
                    manifest,
                    payload,
                    relative.name,
                )
        for session_id, digest in expected.items():
            item = candidates.get(session_id)
            if item is None:
                raise ProviderError(
                    "not_found", "录制不存在，请刷新列表", status=HTTPStatus.NOT_FOUND
                )
            if hashlib.sha256(item[4]).hexdigest() != digest:
                raise ProviderError(
                    "manifest_changed",
                    "录制内容已变化，请刷新后重新确认删除",
                    status=HTTPStatus.CONFLICT,
                )
        for session_id, item in candidates.items():
            if item[3]["take"]["continuation_of"] in expected and session_id not in expected:
                raise ProviderError(
                    "continuation_required",
                    "请同时选择后续连续录制",
                    status=HTTPStatus.CONFLICT,
                    details={"session_id": session_id},
                )
        ordered = sorted(
            expected, key=lambda name: candidates[name][3]["take"]["sequence"], reverse=True
        )
        deleted, failures = [], []
        for index, name in enumerate(ordered):
            root_fd, relative_parent, original, _, _, directory_name = candidates[name]
            parent = root_fd
            opened_parent = None
            try:
                if relative_parent.parts:
                    opened_parent = open_relative_directory(root_fd, relative_parent)
                    parent = opened_parent
                current = os.stat(directory_name, dir_fd=parent, follow_symlinks=False)
                exfat = is_exfat_or_msdos_fd(parent)
                if exfat:
                    unchanged = (current.st_dev, current.st_mode, current.st_mtime_ns) == (
                        original.st_dev,
                        original.st_mode,
                        original.st_mtime_ns,
                    )
                else:
                    unchanged = (current.st_dev, current.st_ino, current.st_mode) == (
                        original.st_dev,
                        original.st_ino,
                        original.st_mode,
                    )
                if not unchanged:
                    raise OSError("录制目录在删除前已变化")
                if exfat:
                    _verify_directory_manifest_digest(
                        parent,
                        directory_name,
                        expected_stat=original,
                        expected_sha256=expected[name],
                    )
                shutil.rmtree(directory_name, dir_fd=parent)
                os.fsync(parent)
                deleted.append(name)
            except OSError:
                failures.extend(
                    {"session_id": remaining, "error": "删除中断，请刷新列表检查后重试"}
                    for remaining in ordered[index:]
                    if remaining not in deleted
                )
                break
            finally:
                if opened_parent is not None:
                    os.close(opened_parent)
    return {
        "schema": "ylx.session-delete-result.v1",
        "deleted_session_ids": deleted,
        "failed_sessions": failures,
    }

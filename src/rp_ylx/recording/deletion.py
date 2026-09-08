"""Preflight and remove explicitly selected sealed recordings."""

from __future__ import annotations

import hashlib
import os
import re
import shutil
from contextlib import ExitStack
from http import HTTPStatus
from pathlib import Path

from rp_ylx.api.gateway import ProviderError
from rp_ylx.recording.device_session import DeviceRecordingError, inspect_device_session_directory

SESSION_ID = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$")
SHA256 = re.compile(r"^[0-9a-f]{64}$")


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
    candidates = {}
    with ExitStack() as stack:
        for root in roots:
            descriptor = os.open(root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
            stack.callback(os.close, descriptor)
            for name in os.listdir(descriptor):
                if SESSION_ID.fullmatch(name) is None:
                    continue
                path = root / name
                try:
                    if path.is_symlink():
                        raise ValueError("symbolic link")
                    identity = path.stat()
                    manifest, payload = inspect_device_session_directory(path)
                    if manifest["session_id"] != name or name in candidates:
                        raise ValueError("ambiguous recording identity")
                except (OSError, DeviceRecordingError, ValueError, KeyError) as error:
                    raise ProviderError(
                        "catalog_unavailable",
                        "录制目录无法安全检查，请先修复或移除异常目录",
                        status=HTTPStatus.CONFLICT,
                    ) from error
                candidates[name] = (descriptor, identity, manifest, payload)
        for session_id, digest in expected.items():
            item = candidates.get(session_id)
            if item is None:
                raise ProviderError(
                    "not_found", "录制不存在，请刷新列表", status=HTTPStatus.NOT_FOUND
                )
            if hashlib.sha256(item[3]).hexdigest() != digest:
                raise ProviderError(
                    "manifest_changed",
                    "录制内容已变化，请刷新后重新确认删除",
                    status=HTTPStatus.CONFLICT,
                )
        for session_id, item in candidates.items():
            if item[2]["take"]["continuation_of"] in expected and session_id not in expected:
                raise ProviderError(
                    "continuation_required",
                    "请同时选择后续连续录制",
                    status=HTTPStatus.CONFLICT,
                    details={"session_id": session_id},
                )
        ordered = sorted(
            expected, key=lambda name: candidates[name][2]["take"]["sequence"], reverse=True
        )
        deleted, failures = [], []
        for index, name in enumerate(ordered):
            descriptor, original, _, _ = candidates[name]
            try:
                current = os.stat(name, dir_fd=descriptor, follow_symlinks=False)
                if (current.st_dev, current.st_ino, current.st_mode) != (
                    original.st_dev,
                    original.st_ino,
                    original.st_mode,
                ):
                    raise OSError("录制目录在删除前已变化")
                shutil.rmtree(name, dir_fd=descriptor)
                deleted.append(name)
                os.fsync(descriptor)
            except OSError:
                failures.extend(
                    {"session_id": remaining, "error": "删除中断，请刷新列表检查后重试"}
                    for remaining in ordered[index:]
                    if remaining not in deleted
                )
                break
    return {
        "schema": "ylx.session-delete-result.v1",
        "deleted_session_ids": deleted,
        "failed_sessions": failures,
    }

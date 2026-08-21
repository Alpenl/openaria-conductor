"""从已选定的会话 generation 安全读取 immutable manifest 与 artifact。"""

from __future__ import annotations

import errno
import hashlib
import json
import math
import os
import re
import stat
import threading
from collections import OrderedDict
from collections.abc import Iterator, Mapping
from contextlib import suppress
from dataclasses import dataclass
from datetime import datetime
from importlib.resources import files
from pathlib import Path
from typing import Literal

from jsonschema import Draft202012Validator, FormatChecker
from jsonschema.exceptions import ValidationError

from rp_ylx.native import (
    NativeModuleError,
    NativeSessionIo,
    create_native_session_io,
    parse_native_single_range,
)

_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_MEDIA_TYPE = re.compile(r"^[a-z0-9!#$&^_.+-]+/[a-z0-9!#$&^_.+-]+$")
_SESSION_ID = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[47][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$"
)
_TEMP_COMPONENT = re.compile(r"^[^/]*\.tmp(?:[._-][^/]*)?$")
_READ_CHUNK = 1024 * 1024
_DEVICE_SESSION_SCHEMA = json.loads(
    files("rp_ylx.schemas")
    .joinpath("ylx-device-session-v1.schema.json")
    .read_text(encoding="utf-8")
)
_DEVICE_SESSION_VALIDATOR = Draft202012Validator(
    _DEVICE_SESSION_SCHEMA,
    format_checker=FormatChecker(),
)
_RECORDING_SESSION_SCHEMA = json.loads(
    files("rp_ylx.contracts")
    .joinpath("recording-session-v0.schema.json")
    .read_text(encoding="utf-8")
)
_RECORDING_SESSION_VALIDATOR = Draft202012Validator(
    _RECORDING_SESSION_SCHEMA,
    format_checker=FormatChecker(),
)
_SESSION_IO_LOCK = threading.Lock()
_SESSION_IO: NativeSessionIo | None = None
_SESSION_IO_UNAVAILABLE = False
_RANGE_PARSER_UNAVAILABLE = False
_DEVICE_SESSION_SUMMARY_UNAVAILABLE = False
_DEVICE_SESSION_ARTIFACTS_UNAVAILABLE = False
_VALIDATED_MANIFEST_CACHE_LIMIT = 32
_VALIDATED_MANIFEST_CACHE_LOCK = threading.Lock()
_VALIDATED_MANIFEST_CACHE: OrderedDict[tuple[str, str, str], Mapping[str, object]] = OrderedDict()


class ArtifactAccessError(RuntimeError):
    """可稳定映射到 Device API 404 或 409 的读取失败。"""

    def __init__(
        self,
        code: Literal["not_found", "not_verified"],
        message: str,
        *,
        reason: Literal["absent", "stale", "unusable"] | None = None,
    ) -> None:
        if code == "not_found" and reason is not None:
            raise ValueError("not_found 错误不能携带 verification reason")
        self.code = code
        self.message = message
        self.reason = "unusable" if code == "not_verified" and reason is None else reason
        super().__init__(f"{code}: {message}")


class UnsatisfiableRange(ValueError):
    """Range 格式无效、多段或无法满足。"""

    def __init__(self, complete_size: int) -> None:
        self.complete_size = complete_size
        super().__init__(f"无法满足对 {complete_size} 字节表示的 Range 请求")


@dataclass(frozen=True, slots=True)
class ArtifactDescriptor:
    artifact_id: str
    role: str
    path: str
    media_type: str
    bytes: int
    sha256: str


@dataclass(frozen=True, slots=True)
class _FileIdentity:
    device: int
    inode: int
    size: int
    modified_ns: int

    @classmethod
    def read(cls, descriptor: int) -> _FileIdentity:
        current = os.fstat(descriptor)
        if not stat.S_ISREG(current.st_mode):
            raise ArtifactAccessError("not_verified", "会话对象不是普通文件")
        return cls(
            device=current.st_dev,
            inode=current.st_ino,
            size=current.st_size,
            modified_ns=current.st_mtime_ns,
        )


class LockedBytes:
    """持有从 generation 根一路打开的 fd，读取期间不再按路径查找。"""

    def __init__(
        self,
        descriptor: int,
        owned_descriptors: list[int],
        *,
        identity: _FileIdentity,
        etag: str,
        content_type: str,
    ) -> None:
        self._descriptor = descriptor
        self._owned_descriptors = owned_descriptors
        self._identity = identity
        self.etag = etag
        self.content_type = content_type
        self.size = identity.size
        self._closed = False

    def __enter__(self) -> LockedBytes:
        if self._closed:
            raise RuntimeError("读取句柄已经关闭")
        return self

    def __exit__(self, *args: object) -> None:
        self.close()

    def __del__(self) -> None:
        self.close()

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        for descriptor in reversed(self._owned_descriptors):
            with suppress(OSError):
                os.close(descriptor)
        self._owned_descriptors.clear()

    def _assert_unchanged(self) -> None:
        if self._closed:
            raise RuntimeError("读取句柄已经关闭")
        if _FileIdentity.read(self._descriptor) != self._identity:
            raise ArtifactAccessError("not_verified", "读取期间会话对象发生变化")

    def read(self, offset: int = 0, length: int | None = None) -> bytes:
        if offset < 0 or (length is not None and length < 0):
            raise ValueError("offset 和 length 不能为负数")
        self._assert_unchanged()
        available = max(0, self.size - offset)
        remaining = available if length is None else min(length, available)
        chunks: list[bytes] = []
        cursor = offset
        while remaining:
            block = os.pread(self._descriptor, min(_READ_CHUNK, remaining), cursor)
            if not block:
                raise ArtifactAccessError("not_verified", "会话对象在读取期间被截断")
            chunks.append(block)
            cursor += len(block)
            remaining -= len(block)
        self._assert_unchanged()
        return b"".join(chunks)

    def send_to(self, output_descriptor: int, offset: int = 0, length: int | None = None) -> int:
        if output_descriptor < 0 or offset < 0 or (length is not None and length < 0):
            raise ValueError("output_descriptor、offset 或 length 无效")
        self._assert_unchanged()
        available = max(0, self.size - offset)
        selected = available if length is None else min(length, available)
        native = _session_io_or_none()
        if native is not None:
            sent = native.sendfile(output_descriptor, self._descriptor, offset, selected)
            if not isinstance(sent, int) or sent != selected:
                raise ArtifactAccessError("not_verified", "artifact native sendfile 发生短写")
            self._assert_unchanged()
            return sent
        sent = 0
        for chunk in self.iter_chunks(offset, selected):
            view = memoryview(chunk)
            while view:
                written = os.write(output_descriptor, view)
                if written <= 0:
                    raise BrokenPipeError("artifact socket wrote zero bytes")
                sent += written
                view = view[written:]
        self._assert_unchanged()
        return sent

    def iter_chunks(
        self,
        offset: int = 0,
        length: int | None = None,
        *,
        chunk_size: int = _READ_CHUNK,
    ) -> Iterator[bytes]:
        if offset < 0 or (length is not None and length < 0) or chunk_size <= 0:
            raise ValueError("offset、length 或 chunk_size 无效")
        self._assert_unchanged()
        available = max(0, self.size - offset)
        remaining = available if length is None else min(length, available)
        cursor = offset
        while remaining:
            block = os.pread(self._descriptor, min(chunk_size, remaining), cursor)
            if not block:
                raise ArtifactAccessError("not_verified", "会话对象在读取期间被截断")
            yield block
            cursor += len(block)
            remaining -= len(block)
        self._assert_unchanged()


class LockedManifest(LockedBytes):
    """已钉住 session generation 的 exact manifest 字节。"""

    manifest_sha256: str

    def __init__(
        self,
        descriptor: int,
        owned_descriptors: list[int],
        *,
        identity: _FileIdentity,
        manifest_sha256: str,
        payload: bytes,
    ) -> None:
        if len(payload) != identity.size or hashlib.sha256(payload).hexdigest() != manifest_sha256:
            raise ValueError("LockedManifest payload 与 exact manifest 身份不一致")
        super().__init__(
            descriptor,
            owned_descriptors,
            identity=identity,
            etag=f'"{manifest_sha256}"',
            content_type="application/json",
        )
        self.manifest_sha256 = manifest_sha256
        self._payload = payload

    def read(self, offset: int = 0, length: int | None = None) -> bytes:
        if offset < 0 or (length is not None and length < 0):
            raise ValueError("offset 和 length 不能为负数")
        end = None if length is None else offset + length
        return self._payload[offset:end]

    def iter_chunks(
        self,
        offset: int = 0,
        length: int | None = None,
        *,
        chunk_size: int = _READ_CHUNK,
    ) -> Iterator[bytes]:
        if offset < 0 or (length is not None and length < 0) or chunk_size <= 0:
            raise ValueError("offset、length 或 chunk_size 无效")
        available = max(0, self.size - offset)
        remaining = available if length is None else min(length, available)
        cursor = offset
        while remaining:
            selected = min(chunk_size, remaining)
            yield self._payload[cursor : cursor + selected]
            cursor += selected
            remaining -= selected

    def send_to(self, output_descriptor: int, offset: int = 0, length: int | None = None) -> int:
        if output_descriptor < 0 or offset < 0 or (length is not None and length < 0):
            raise ValueError("output_descriptor、offset 或 length 无效")
        available = max(0, self.size - offset)
        selected = available if length is None else min(length, available)
        sent = 0
        for chunk in self.iter_chunks(offset, selected):
            view = memoryview(chunk)
            while view:
                written = os.write(output_descriptor, view)
                if written <= 0:
                    raise BrokenPipeError("manifest socket wrote zero bytes")
                sent += written
                view = view[written:]
        return sent


class LockedArtifact(LockedBytes):
    """已由 exact manifest 固定身份并通过打开的 fd 读取的 artifact。"""

    descriptor: ArtifactDescriptor

    def __init__(
        self,
        descriptor: int,
        owned_descriptors: list[int],
        *,
        identity: _FileIdentity,
        artifact_descriptor: ArtifactDescriptor,
    ) -> None:
        super().__init__(
            descriptor,
            owned_descriptors,
            identity=identity,
            etag=f'"{artifact_descriptor.sha256}"',
            content_type=artifact_descriptor.media_type,
        )
        self.descriptor = artifact_descriptor


def parse_single_range(value: str | None, complete_size: int) -> tuple[int, int] | None:
    """解析一个 RFC 9110 bytes range，返回首尾均包含的区间。"""

    if complete_size < 0:
        raise ValueError("complete_size 不能为负数")
    if value is None:
        return None
    global _RANGE_PARSER_UNAVAILABLE
    if not _RANGE_PARSER_UNAVAILABLE:
        try:
            return parse_native_single_range(value, complete_size)
        except NativeModuleError:
            _RANGE_PARSER_UNAVAILABLE = True
        except ValueError as error:
            raise UnsatisfiableRange(complete_size) from error
    if not value.startswith("bytes=") or "," in value:
        raise UnsatisfiableRange(complete_size)
    selected = value.removeprefix("bytes=")
    if selected.startswith("-"):
        suffix = selected[1:]
        suffix_size = _bounded_range_decimal(suffix, complete_size, complete_size)
        if suffix_size == 0 or complete_size == 0:
            raise UnsatisfiableRange(complete_size)
        return max(0, complete_size - suffix_size), complete_size - 1

    first_text, separator, last_text = selected.partition("-")
    if (
        not separator
        or not first_text.isascii()
        or not first_text.isdigit()
        or (last_text and (not last_text.isascii() or not last_text.isdigit()))
    ):
        raise UnsatisfiableRange(complete_size)
    first = _bounded_range_decimal(first_text, complete_size, complete_size)
    if first >= complete_size:
        raise UnsatisfiableRange(complete_size)
    if not last_text:
        return first, complete_size - 1
    last = _bounded_range_decimal(last_text, complete_size, complete_size)
    if last < first:
        raise UnsatisfiableRange(complete_size)
    return first, min(last, complete_size - 1)


def _session_io_or_none() -> NativeSessionIo | None:
    global _SESSION_IO, _SESSION_IO_UNAVAILABLE
    if _SESSION_IO_UNAVAILABLE:
        return None
    if _SESSION_IO is not None:
        return _SESSION_IO
    with _SESSION_IO_LOCK:
        if _SESSION_IO_UNAVAILABLE:
            return None
        if _SESSION_IO is not None:
            return _SESSION_IO
        try:
            _SESSION_IO = create_native_session_io()
        except NativeModuleError:
            _SESSION_IO_UNAVAILABLE = True
            return None
        return _SESSION_IO


def _native_device_session_v1_artifact_descriptors(
    manifest_bytes: bytes,
    session_id: str,
    manifest: Mapping[str, object],
) -> dict[str, ArtifactDescriptor] | None:
    if manifest.get("schema") != "ylx.device-session.v1":
        return None
    summary = _native_device_session_v1_summary(manifest_bytes, session_id, manifest)
    if summary is not None:
        raw_artifacts = summary["artifacts"]
        if not isinstance(raw_artifacts, list):
            raise ArtifactAccessError("not_verified", "manifest artifact 清单无效")
        return _artifact_descriptors_from_raw(raw_artifacts, legacy=False, path_validated=True)
    global _DEVICE_SESSION_ARTIFACTS_UNAVAILABLE
    if _DEVICE_SESSION_ARTIFACTS_UNAVAILABLE:
        return None
    native = _session_io_or_none()
    if native is None:
        _DEVICE_SESSION_ARTIFACTS_UNAVAILABLE = True
        return None
    read_artifacts = getattr(native, "device_session_v1_artifacts", None)
    if not callable(read_artifacts):
        _DEVICE_SESSION_ARTIFACTS_UNAVAILABLE = True
        return None
    try:
        raw_descriptors = read_artifacts(manifest_bytes, session_id)
    except AttributeError:
        _DEVICE_SESSION_ARTIFACTS_UNAVAILABLE = True
        return None
    except BaseException as error:
        raise ArtifactAccessError("not_verified", "manifest artifact 清单无效") from error
    if not isinstance(raw_descriptors, list):
        raise ArtifactAccessError("not_verified", "manifest artifact 清单无效")
    return _artifact_descriptors_from_raw(raw_descriptors, legacy=False, path_validated=True)


def device_session_v1_summary(
    manifest_bytes: bytes,
    session_id: str,
    manifest: Mapping[str, object] | None = None,
) -> dict[str, object]:
    """Return the sealed Device Session v1 summary, preferring Rust's manifest fast path."""

    native_summary = _native_device_session_v1_summary(manifest_bytes, session_id, manifest)
    if native_summary is not None:
        return native_summary
    if manifest is None:
        manifest = _validated_manifest(manifest_bytes, session_id, "v3")
    return _device_session_v1_summary_python(
        manifest,
        manifest_bytes=manifest_bytes,
        session_id=session_id,
    )


def _native_device_session_v1_summary(
    manifest_bytes: bytes,
    session_id: str,
    manifest: Mapping[str, object] | None,
) -> dict[str, object] | None:
    if manifest is not None and manifest.get("schema") != "ylx.device-session.v1":
        return None
    global _DEVICE_SESSION_SUMMARY_UNAVAILABLE
    if _DEVICE_SESSION_SUMMARY_UNAVAILABLE:
        return None
    native = _session_io_or_none()
    if native is None:
        _DEVICE_SESSION_SUMMARY_UNAVAILABLE = True
        return None
    read_summary = getattr(native, "device_session_v1_summary", None)
    if not callable(read_summary):
        _DEVICE_SESSION_SUMMARY_UNAVAILABLE = True
        return None
    try:
        raw_summary = read_summary(manifest_bytes, session_id)
    except AttributeError:
        _DEVICE_SESSION_SUMMARY_UNAVAILABLE = True
        return None
    except BaseException as error:
        raise ArtifactAccessError("not_verified", "manifest summary 无效") from error
    return _coerce_device_session_v1_summary(raw_summary, session_id)


def _device_session_v1_summary_python(
    manifest: Mapping[str, object],
    *,
    manifest_bytes: bytes,
    session_id: str,
) -> dict[str, object]:
    del manifest_bytes
    try:
        if (
            manifest.get("schema") != "ylx.device-session.v1"
            or manifest.get("sealed") is not True
            or manifest.get("session_id") != session_id
        ):
            raise ArtifactAccessError("not_verified", "manifest 不是 sealed device-session v1")
        time = manifest["time"]
        frames = manifest["frames"]
        imu = manifest["imu"]
        if not all(isinstance(value, Mapping) for value in (time, frames, imu)):
            raise ArtifactAccessError("not_verified", "manifest summary 结构无效")
        audio = manifest.get("audio")
        audio_sample_count = None
        if audio is not None:
            if not isinstance(audio, Mapping):
                raise ArtifactAccessError("not_verified", "manifest audio 结构无效")
            audio_sample_count = _non_negative_int(
                audio.get("sample_count"),
                "manifest audio sample_count 无效",
            )
        descriptors = _artifact_descriptors(manifest)
        artifacts = _artifact_descriptor_payloads(descriptors)
        return {
            "session_id": _string_value(manifest.get("session_id"), "manifest session_id 无效"),
            "display_name": _string_value(
                manifest.get("display_name"),
                "manifest display_name 无效",
            ),
            "started_at": _string_value(time.get("started_at"), "manifest started_at 无效"),
            "ended_at": _string_value(time.get("ended_at"), "manifest ended_at 无效"),
            "duration_seconds": _non_negative_number(
                time.get("duration_seconds"),
                "manifest duration_seconds 无效",
            ),
            "frames_count": _non_negative_int(
                frames.get("count"),
                "manifest frames count 无效",
            ),
            "imu_sample_count": _non_negative_int(
                imu.get("sample_count"),
                "manifest imu sample_count 无效",
            ),
            "audio_sample_count": audio_sample_count,
            "total_bytes": sum(descriptor.bytes for descriptor in descriptors.values()),
            "artifacts": artifacts,
        }
    except ArtifactAccessError:
        raise
    except (KeyError, TypeError, ValueError) as error:
        raise ArtifactAccessError("not_verified", "manifest summary 无效") from error


def _coerce_device_session_v1_summary(
    raw_summary: object,
    session_id: str,
) -> dict[str, object]:
    if not isinstance(raw_summary, Mapping):
        raise ArtifactAccessError("not_verified", "manifest summary 无效")
    required = {
        "session_id",
        "display_name",
        "started_at",
        "ended_at",
        "duration_seconds",
        "frames_count",
        "imu_sample_count",
        "audio_sample_count",
        "total_bytes",
        "artifacts",
    }
    if not required.issubset(raw_summary):
        raise ArtifactAccessError("not_verified", "manifest summary 字段缺失")
    raw_artifacts = raw_summary["artifacts"]
    if not isinstance(raw_artifacts, list):
        raise ArtifactAccessError("not_verified", "manifest artifact 清单无效")
    descriptors = _artifact_descriptors_from_raw(
        raw_artifacts,
        legacy=False,
        path_validated=True,
    )
    artifacts = _artifact_descriptor_payloads(descriptors)
    total_bytes = _non_negative_int(raw_summary["total_bytes"], "manifest total_bytes 无效")
    computed_total = sum(descriptor.bytes for descriptor in descriptors.values())
    if total_bytes != computed_total:
        raise ArtifactAccessError("not_verified", "manifest total_bytes 与 artifact 不一致")
    selected_session_id = _string_value(raw_summary["session_id"], "manifest session_id 无效")
    if selected_session_id != session_id:
        raise ArtifactAccessError("not_verified", "manifest 会话身份不匹配")
    audio_sample_count = raw_summary["audio_sample_count"]
    if audio_sample_count is not None:
        audio_sample_count = _non_negative_int(
            audio_sample_count,
            "manifest audio sample_count 无效",
        )
    return {
        "session_id": selected_session_id,
        "display_name": _string_value(
            raw_summary["display_name"],
            "manifest display_name 无效",
        ),
        "started_at": _string_value(raw_summary["started_at"], "manifest started_at 无效"),
        "ended_at": _string_value(raw_summary["ended_at"], "manifest ended_at 无效"),
        "duration_seconds": _non_negative_number(
            raw_summary["duration_seconds"],
            "manifest duration_seconds 无效",
        ),
        "frames_count": _non_negative_int(
            raw_summary["frames_count"],
            "manifest frames count 无效",
        ),
        "imu_sample_count": _non_negative_int(
            raw_summary["imu_sample_count"],
            "manifest imu sample_count 无效",
        ),
        "audio_sample_count": audio_sample_count,
        "total_bytes": total_bytes,
        "artifacts": artifacts,
    }


def _native_device_session_v1_artifact_descriptor(
    manifest_bytes: bytes,
    session_id: str,
    manifest: Mapping[str, object],
    artifact_id: str,
) -> ArtifactDescriptor | None:
    if manifest.get("schema") != "ylx.device-session.v1":
        return None
    global _DEVICE_SESSION_ARTIFACTS_UNAVAILABLE
    if _DEVICE_SESSION_ARTIFACTS_UNAVAILABLE:
        return None
    native = _session_io_or_none()
    if native is None:
        _DEVICE_SESSION_ARTIFACTS_UNAVAILABLE = True
        return None
    read_artifact = getattr(native, "device_session_v1_artifact", None)
    if not callable(read_artifact):
        return None
    try:
        raw_descriptor = read_artifact(manifest_bytes, session_id, artifact_id)
    except AttributeError:
        return None
    except BaseException as error:
        raise ArtifactAccessError("not_verified", "manifest artifact 清单无效") from error
    if raw_descriptor is None:
        return None
    if not isinstance(raw_descriptor, Mapping):
        raise ArtifactAccessError("not_verified", "manifest artifact 清单无效")
    return _artifact_descriptor(raw_descriptor, legacy=False, path_validated=True)


def _native_open_relative_regular(path: str, session_descriptor: int) -> int | None:
    native = _session_io_or_none()
    if native is None:
        return None
    opener = getattr(native, "open_relative_regular", None)
    if not callable(opener):
        return None
    try:
        descriptor = opener(session_descriptor, path)
    except AttributeError:
        return None
    except BaseException as error:
        raise ArtifactAccessError(
            "not_verified", "manifest 声明的 artifact 不存在或不安全"
        ) from error
    if not isinstance(descriptor, int) or descriptor < 0:
        raise ArtifactAccessError("not_verified", "artifact native open 返回无效 fd")
    return descriptor


def _native_read_bounded_file(descriptor: int, maximum_bytes: int) -> bytes | None:
    native = _session_io_or_none()
    if native is None:
        return None
    reader = getattr(native, "read_bounded_fd", None)
    if not callable(reader):
        return None
    try:
        payload = reader(descriptor, maximum_bytes)
    except AttributeError:
        return None
    except BaseException as error:
        raise ArtifactAccessError("not_verified", "会话对象在校验期间无法读取") from error
    if not isinstance(payload, bytes):
        raise ArtifactAccessError("not_verified", "artifact native read 返回无效 payload")
    if len(payload) > maximum_bytes:
        raise ArtifactAccessError("not_verified", "会话对象在校验期间超过允许大小")
    return payload


class DirectorySessionStore:
    """仅用于 fake/dev：从一个已选定的 generation 根提供安全只读会话。"""

    def __init__(
        self,
        generation_root: str | Path,
        *,
        verified_manifests: Mapping[str, str] | None = None,
        max_manifest_bytes: int = 16 * 1024 * 1024,
    ) -> None:
        if max_manifest_bytes <= 0:
            raise ValueError("max_manifest_bytes 必须大于零")
        self._root_descriptor = _open_absolute_directory(generation_root)
        self._verified_manifests = {} if verified_manifests is None else verified_manifests
        self._max_manifest_bytes = max_manifest_bytes
        self._lock = threading.Lock()

    def __enter__(self) -> DirectorySessionStore:
        return self

    def __exit__(self, *args: object) -> None:
        self.close()

    def __del__(self) -> None:
        self.close()

    def close(self) -> None:
        descriptor = getattr(self, "_root_descriptor", -1)
        if descriptor < 0:
            return
        with self._lock:
            descriptor = self._root_descriptor
            self._root_descriptor = -1
        if descriptor >= 0:
            with suppress(OSError):
                os.close(descriptor)

    def _duplicate_root(self) -> int:
        with self._lock:
            if self._root_descriptor < 0:
                raise RuntimeError("DirectorySessionStore 已关闭")
            return os.dup(self._root_descriptor)

    def _open_manifest_bytes(self, session_id: str) -> tuple[list[int], int, _FileIdentity, bytes]:
        if not _SESSION_ID.fullmatch(session_id):
            raise ArtifactAccessError("not_found", "会话不存在")
        owned = [self._duplicate_root()]
        try:
            session_descriptor = _open_directory_component(session_id, owned[-1])
            owned.append(session_descriptor)
            manifest_descriptor = _open_regular_component("manifest.json", session_descriptor)
            owned.append(manifest_descriptor)
            identity = _FileIdentity.read(manifest_descriptor)
            if identity.size > self._max_manifest_bytes:
                raise ArtifactAccessError("not_found", "会话 manifest 无法读取")
            payload = _read_exact_file(manifest_descriptor, identity)
            return owned, manifest_descriptor, identity, payload
        except ArtifactAccessError:
            _close_all(owned)
            raise
        except OSError as error:
            _close_all(owned)
            raise ArtifactAccessError("not_found", "会话不存在或尚未封存") from error

    def open_manifest(self, session_id: str, api_version: str) -> LockedManifest:
        owned, manifest_descriptor, identity, payload = self._open_manifest_bytes(session_id)
        try:
            _validated_manifest(payload, session_id, api_version)
            manifest_sha256 = hashlib.sha256(payload).hexdigest()
            return LockedManifest(
                manifest_descriptor,
                owned,
                identity=identity,
                manifest_sha256=manifest_sha256,
                payload=payload,
            )
        except ArtifactAccessError:
            _close_all(owned)
            raise

    def open_verified_artifact(
        self, session_id: str, artifact_id: str, api_version: str
    ) -> LockedArtifact:
        if not _SHA256.fullmatch(artifact_id):
            raise ArtifactAccessError("not_found", "artifact 不存在")
        verified_sha256 = self._verified_manifests.get(session_id)
        if verified_sha256 is None:
            raise ArtifactAccessError("not_verified", "会话尚无验证结果", reason="absent")
        try:
            owned, _, _, manifest_bytes = self._open_manifest_bytes(session_id)
        except ArtifactAccessError as error:
            raise ArtifactAccessError(
                "not_verified",
                "已验证的 manifest 不再存在或无法安全读取",
                reason="stale",
            ) from error
        try:
            manifest_sha256 = hashlib.sha256(manifest_bytes).hexdigest()
            if verified_sha256 != manifest_sha256:
                raise ArtifactAccessError(
                    "not_verified", "会话验证结果与当前 manifest 不匹配", reason="stale"
                )
            try:
                manifest = _validated_manifest(manifest_bytes, session_id, api_version)
            except ArtifactAccessError as error:
                raise ArtifactAccessError(
                    "not_verified", "当前 manifest 无法作为 sealed 会话使用"
                ) from error
            selected = _native_device_session_v1_artifact_descriptor(
                manifest_bytes,
                session_id,
                manifest,
                artifact_id,
            )
            if selected is None:
                descriptors = _native_device_session_v1_artifact_descriptors(
                    manifest_bytes,
                    session_id,
                    manifest,
                )
                if descriptors is None:
                    descriptors = _artifact_descriptors(manifest)
                selected = descriptors.get(artifact_id)
            if selected is None:
                raise ArtifactAccessError("not_found", "artifact 不存在")

            try:
                artifact_descriptor = _open_relative_regular(selected.path, owned[1])
            except ArtifactAccessError as error:
                raise ArtifactAccessError(
                    "not_verified", "manifest 声明的 artifact 不存在或不安全"
                ) from error
            owned.append(artifact_descriptor)
            identity = _FileIdentity.read(artifact_descriptor)
            if identity.size != selected.bytes:
                raise ArtifactAccessError("not_verified", "artifact 大小与 manifest 不一致")
            representation = LockedArtifact(
                artifact_descriptor,
                owned,
                identity=identity,
                artifact_descriptor=selected,
            )
            owned = []
            return representation
        except ArtifactAccessError:
            raise
        except OSError as error:
            raise ArtifactAccessError("not_verified", "artifact 无法安全读取") from error
        finally:
            _close_all(owned)


def _required_open_flags(*, directory: bool = False) -> int:
    if not hasattr(os, "O_NOFOLLOW") or not hasattr(os, "O_DIRECTORY"):
        raise RuntimeError("安全会话下载需要 Linux O_NOFOLLOW 与 O_DIRECTORY")
    flags = os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW
    if directory:
        flags |= os.O_DIRECTORY
    else:
        flags |= os.O_NONBLOCK
    return flags


def _bounded_range_decimal(value: str, upper_bound: int, complete_size: int) -> int:
    if not value.isascii() or not value.isdigit():
        raise UnsatisfiableRange(complete_size)
    normalized = value.lstrip("0") or "0"
    bound = str(upper_bound)
    if len(normalized) > len(bound) or (len(normalized) == len(bound) and normalized > bound):
        return upper_bound + 1
    return int(normalized)


def _open_absolute_directory(path: str | Path) -> int:
    raw_path = os.fspath(path)
    if not os.path.isabs(raw_path) or "\x00" in raw_path:
        raise ValueError("generation_root 必须是无 NUL 的绝对路径")
    components = [component for component in raw_path.split("/") if component]
    if any(component in {".", ".."} for component in components):
        raise ValueError("generation_root 不能包含 . 或 ..")
    current = os.open("/", _required_open_flags(directory=True))
    try:
        for component in components:
            following = _open_directory_component(component, current)
            os.close(current)
            current = following
        return current
    except Exception:
        os.close(current)
        raise


def _open_directory_component(component: str, parent_descriptor: int) -> int:
    try:
        return os.open(
            component,
            _required_open_flags(directory=True),
            dir_fd=parent_descriptor,
        )
    except OSError as error:
        if error.errno in {errno.ENOENT, errno.ENOTDIR, errno.ELOOP}:
            raise ArtifactAccessError("not_found", "路径不存在或不安全") from error
        raise


def _open_regular_component(component: str, parent_descriptor: int) -> int:
    try:
        descriptor = os.open(
            component,
            _required_open_flags(),
            dir_fd=parent_descriptor,
        )
    except OSError as error:
        if error.errno in {errno.ENOENT, errno.ENOTDIR, errno.ELOOP}:
            raise ArtifactAccessError("not_found", "文件不存在或不安全") from error
        raise
    try:
        _FileIdentity.read(descriptor)
        return descriptor
    except Exception:
        os.close(descriptor)
        raise


def _open_relative_regular(path: str, session_descriptor: int) -> int:
    native_descriptor = _native_open_relative_regular(path, session_descriptor)
    if native_descriptor is not None:
        return native_descriptor
    components = _safe_relative_components(path)
    current = os.dup(session_descriptor)
    try:
        for component in components[:-1]:
            following = _open_directory_component(component, current)
            os.close(current)
            current = following
        result = _open_regular_component(components[-1], current)
        os.close(current)
        return result
    except Exception:
        os.close(current)
        raise


def _safe_relative_components(path: str) -> list[str]:
    if not isinstance(path, str) or not path or len(path) > 1024 or path.startswith("/"):
        raise ArtifactAccessError("not_verified", "manifest artifact 路径无效")
    if "\\" in path or any(
        ord(character) < 32 or 127 <= ord(character) <= 159 for character in path
    ):
        raise ArtifactAccessError("not_verified", "manifest artifact 路径无效")
    components = path.split("/")
    if any(
        not component or component in {".", ".."} or _TEMP_COMPONENT.fullmatch(component)
        for component in components
    ):
        raise ArtifactAccessError("not_verified", "manifest artifact 路径无效")
    if components[0] in {"manifest.json", "recording.json"}:
        raise ArtifactAccessError("not_verified", "manifest artifact 路径使用保留名称")
    return components


def _read_exact_file(descriptor: int, identity: _FileIdentity) -> bytes:
    native_payload = _native_read_bounded_file(descriptor, identity.size)
    if native_payload is not None:
        if len(native_payload) != identity.size:
            raise ArtifactAccessError("not_verified", "会话对象在校验期间被截断")
        if _FileIdentity.read(descriptor) != identity:
            raise ArtifactAccessError("not_verified", "会话对象在校验期间发生变化")
        return native_payload

    remaining = identity.size
    offset = 0
    blocks: list[bytes] = []
    while remaining:
        block = os.pread(descriptor, min(_READ_CHUNK, remaining), offset)
        if not block:
            raise ArtifactAccessError("not_verified", "会话对象在校验期间被截断")
        blocks.append(block)
        offset += len(block)
        remaining -= len(block)
    if _FileIdentity.read(descriptor) != identity:
        raise ArtifactAccessError("not_verified", "会话对象在校验期间发生变化")
    return b"".join(blocks)


def _validated_manifest(payload: bytes, session_id: str, api_version: str) -> Mapping[str, object]:
    if api_version not in {"v2", "v3", "v4"}:
        raise ValueError("api_version 必须是 v2、v3 或 v4")
    manifest_sha256 = hashlib.sha256(payload).hexdigest()
    cache_key = (api_version, session_id, manifest_sha256)
    with _VALIDATED_MANIFEST_CACHE_LOCK:
        cached = _VALIDATED_MANIFEST_CACHE.get(cache_key)
        if cached is not None:
            _VALIDATED_MANIFEST_CACHE.move_to_end(cache_key)
            return cached
        manifest = _decode_and_validate_manifest(payload, session_id, api_version)
        _VALIDATED_MANIFEST_CACHE[cache_key] = manifest
        _VALIDATED_MANIFEST_CACHE.move_to_end(cache_key)
        while len(_VALIDATED_MANIFEST_CACHE) > _VALIDATED_MANIFEST_CACHE_LIMIT:
            _VALIDATED_MANIFEST_CACHE.popitem(last=False)
        return manifest


def _decode_and_validate_manifest(
    payload: bytes, session_id: str, api_version: str
) -> Mapping[str, object]:
    try:
        decoded = payload.decode("utf-8")
        manifest = json.loads(
            decoded,
            object_pairs_hook=_unique_object,
            parse_constant=lambda value: (_ for _ in ()).throw(ValueError(value)),
        )
    except (UnicodeDecodeError, ValueError, json.JSONDecodeError) as error:
        raise ArtifactAccessError("not_found", "会话 manifest 无效") from error
    if not isinstance(manifest, dict) or manifest.get("session_id") != session_id:
        raise ArtifactAccessError("not_found", "会话 manifest 身份不匹配")
    is_v1 = manifest.get("schema") == "ylx.device-session.v1" and manifest.get("sealed") is True
    is_v0 = (
        manifest.get("format") == "ylx.recording-session.v0" and manifest.get("state") == "sealed"
    )
    if not is_v1 and not (api_version == "v2" and is_v0):
        raise ArtifactAccessError("not_found", "会话不存在或尚未封存")
    if is_v1:
        _validate_device_session_v1(manifest)
    else:
        _validate_recording_session_v0(manifest)
    return manifest


def _clear_validated_manifest_cache_for_tests() -> None:
    with _VALIDATED_MANIFEST_CACHE_LOCK:
        _VALIDATED_MANIFEST_CACHE.clear()


def _unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate key: {key}")
        result[key] = value
    return result


def _artifact_descriptors(manifest: Mapping[str, object]) -> dict[str, ArtifactDescriptor]:
    raw_descriptors: list[object]
    legacy = False
    if manifest.get("schema") == "ylx.device-session.v1":
        raw_descriptors = list(iter_device_session_v1_artifacts(manifest))
    else:
        artifacts = manifest.get("artifacts")
        if not isinstance(artifacts, list):
            raise ArtifactAccessError("not_verified", "manifest artifact 清单无效")
        raw_descriptors = artifacts
        legacy = True

    return _artifact_descriptors_from_raw(raw_descriptors, legacy=legacy)


def _artifact_descriptors_from_raw(
    raw_descriptors: list[object],
    *,
    legacy: bool,
    path_validated: bool = False,
) -> dict[str, ArtifactDescriptor]:
    descriptors: dict[str, ArtifactDescriptor] = {}
    paths: set[str] = set()
    for raw in raw_descriptors:
        descriptor = _artifact_descriptor(raw, legacy=legacy, path_validated=path_validated)
        existing = descriptors.get(descriptor.artifact_id)
        if descriptor.path in paths:
            raise ArtifactAccessError("not_verified", "manifest artifact 路径重复")
        if existing is not None and (
            descriptor.bytes != existing.bytes
            or descriptor.media_type != existing.media_type
            or descriptor.sha256 != existing.sha256
        ):
            raise ArtifactAccessError("not_verified", "同一 artifact 身份的表示元数据不一致")
        if existing is None:
            descriptors[descriptor.artifact_id] = descriptor
        paths.add(descriptor.path)
    if not descriptors:
        raise ArtifactAccessError("not_verified", "manifest 未声明 artifact")
    return descriptors


def _artifact_descriptor_payloads(
    descriptors: Mapping[str, ArtifactDescriptor],
) -> list[dict[str, object]]:
    return [
        {
            "artifact_id": descriptor.artifact_id,
            "role": descriptor.role,
            "path": descriptor.path,
            "media_type": descriptor.media_type,
            "bytes": descriptor.bytes,
            "sha256": descriptor.sha256,
        }
        for descriptor in descriptors.values()
    ]


def _string_value(value: object, message: str) -> str:
    if not isinstance(value, str):
        raise ArtifactAccessError("not_verified", message)
    return value


def _non_negative_int(value: object, message: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ArtifactAccessError("not_verified", message)
    return value


def _non_negative_number(value: object, message: str) -> int | float:
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not math.isfinite(value)
        or value < 0
    ):
        raise ArtifactAccessError("not_verified", message)
    return value


def iter_device_session_v1_artifacts(
    manifest: Mapping[str, object],
) -> Iterator[Mapping[str, object]]:
    """Yield every artifact descriptor declared by a Device Session v1 manifest."""

    try:
        video = manifest["video"]
        frames = manifest["frames"]
        imu = manifest["imu"]
    except KeyError as error:
        raise ArtifactAccessError("not_verified", "manifest artifact 清单无效") from error
    if not all(isinstance(value, Mapping) for value in (video, frames, imu)):
        raise ArtifactAccessError("not_verified", "manifest artifact 清单无效")

    layout = video.get("layout")
    if layout == "raw-side-by-side":
        artifact = video.get("artifact")
        if not isinstance(artifact, Mapping):
            raise ArtifactAccessError("not_verified", "manifest video artifact 无效")
        yield artifact
    elif layout == "split-eyes":
        segments = video.get("segments")
        if not isinstance(segments, list):
            raise ArtifactAccessError("not_verified", "manifest video segments 无效")
        for segment in segments:
            if not isinstance(segment, Mapping):
                raise ArtifactAccessError("not_verified", "manifest video segment 无效")
            artifacts = segment.get("artifacts")
            if not isinstance(artifacts, Mapping):
                raise ArtifactAccessError("not_verified", "manifest video segment artifact 无效")
            for eye in ("left", "right"):
                artifact = artifacts.get(eye)
                if not isinstance(artifact, Mapping):
                    raise ArtifactAccessError(
                        "not_verified", "manifest video segment artifact 无效"
                    )
                yield artifact
    else:
        raise ArtifactAccessError("not_verified", "manifest video layout 无效")

    for section in (frames, imu):
        artifact = section.get("artifact")
        if not isinstance(artifact, Mapping):
            raise ArtifactAccessError("not_verified", "manifest artifact 清单无效")
        yield artifact

    audio = manifest.get("audio")
    if audio is not None:
        if not isinstance(audio, Mapping):
            raise ArtifactAccessError("not_verified", "manifest audio 结构无效")
        segments = audio.get("segments")
        if not isinstance(segments, list):
            raise ArtifactAccessError("not_verified", "manifest audio segments 无效")
        for segment in segments:
            if not isinstance(segment, Mapping):
                raise ArtifactAccessError("not_verified", "manifest audio segment 无效")
            artifact = segment.get("artifact")
            if not isinstance(artifact, Mapping):
                raise ArtifactAccessError("not_verified", "manifest audio artifact 无效")
            yield artifact


def _validate_device_session_v1(manifest: Mapping[str, object]) -> None:
    try:
        _DEVICE_SESSION_VALIDATOR.validate(manifest)
    except ValidationError as error:
        raise ArtifactAccessError(
            "not_verified", "manifest 不符合 device-session v1 契约"
        ) from error

    camera = manifest["camera"]
    time = manifest["time"]
    integrity = manifest["integrity"]
    take = manifest["take"]
    if not all(isinstance(value, Mapping) for value in (camera, time, integrity, take)):
        raise ArtifactAccessError("not_verified", "manifest v1 结构无效")
    if camera["width"] != camera["eye_width"] * 2:
        raise ArtifactAccessError("not_verified", "camera width 与 eye_width 不一致")
    nominal_fps = camera["sensor_fps"] / camera["frame_decimation"]
    if take["continuation_of"] == manifest["session_id"]:
        raise ArtifactAccessError("not_verified", "会话不能继续自身")

    started = _api_datetime(time["started_at"])
    ended = _api_datetime(time["ended_at"])
    verified = _api_datetime(integrity["verified_at"])
    sealed = _api_datetime(manifest["sealed_at"])
    if not started <= ended <= verified <= sealed:
        raise ArtifactAccessError("not_verified", "manifest 时间顺序无效")
    if (
        "duration_clock" not in time
        and abs(time["duration_seconds"] - (ended - started).total_seconds()) > 0.001
    ):
        raise ArtifactAccessError("not_verified", "manifest duration_seconds 与时间戳不一致")
    frames = manifest["frames"]
    if not isinstance(frames, Mapping):
        raise ArtifactAccessError("not_verified", "manifest frames 结构无效")

    drops = integrity["drop_events"]
    dropped_sum = 0
    previous_end: int | None = None
    for drop in drops:
        if (
            drop["end_frame"] <= drop["start_frame"]
            or drop["dropped"] != drop["end_frame"] - drop["start_frame"]
            or (previous_end is not None and drop["start_frame"] <= previous_end)
        ):
            raise ArtifactAccessError("not_verified", "manifest drop event 无效")
        dropped_sum += drop["dropped"]
        previous_end = drop["end_frame"]
    if dropped_sum != integrity["dropped_frames"]:
        raise ArtifactAccessError("not_verified", "dropped_frames 与 drop event 不一致")
    policy = integrity.get("quality_policy")
    has_measured_semantics = "nominal_fps" in camera and policy is not None
    if ("nominal_fps" in camera) != (policy is not None):
        raise ArtifactAccessError("not_verified", "manifest FPS 与质量策略版本不完整")
    if not has_measured_semantics:
        if dropped_sum:
            raise ArtifactAccessError("not_verified", "legacy v1 非零丢帧缺少可验证质量策略")
        if abs(camera["effective_fps"] - nominal_fps) > 1e-9:
            raise ArtifactAccessError(
                "not_verified", "legacy camera effective_fps 与抽帧配置不一致"
            )
        return
    assert isinstance(policy, Mapping)
    if abs(camera["nominal_fps"] - nominal_fps) > 1e-9:
        raise ArtifactAccessError("not_verified", "camera nominal_fps 与抽帧配置不一致")
    measured_fps = (
        0.0 if time["duration_seconds"] == 0 else frames["count"] / time["duration_seconds"]
    )
    if abs(camera["effective_fps"] - measured_fps) > 1e-9:
        raise ArtifactAccessError("not_verified", "camera effective_fps 不是实际保留帧率")
    total = frames["count"] + dropped_sum
    fraction = 0.0 if total == 0 else dropped_sum / total
    contiguous = max((drop["dropped"] for drop in drops), default=0)
    window_drops = max(
        (
            sum(
                other["dropped"]
                for other in drops
                if drop["at_time_seconds"]
                <= other["at_time_seconds"]
                < drop["at_time_seconds"] + policy["window_seconds"]
            )
            for drop in drops
        ),
        default=0,
    )
    if (
        contiguous > policy["max_contiguous_dropped_frames"]
        or dropped_sum > policy["max_total_dropped_frames"]
        or fraction > policy["max_drop_fraction"]
        or window_drops > policy["max_dropped_frames_per_window"]
    ):
        raise ArtifactAccessError("not_verified", "manifest 丢帧超过质量策略")

    video = manifest["video"]
    if isinstance(video, Mapping) and video.get("layout") == "split-eyes":
        _validate_split_eye_segments(manifest, video, drops, dropped_sum)
    audio = manifest.get("audio")
    if audio is not None:
        if not isinstance(audio, Mapping):
            raise ArtifactAccessError("not_verified", "manifest audio 结构无效")
        _validate_audio(audio)


def validate_device_session_manifest(manifest: Mapping[str, object]) -> None:
    """验证生产者与下载路径共享的 Device Session v1 语义。"""

    _validate_device_session_v1(manifest)


def _validate_recording_session_v0(manifest: Mapping[str, object]) -> None:
    try:
        _RECORDING_SESSION_VALIDATOR.validate(manifest)
    except ValidationError as error:
        raise ArtifactAccessError(
            "not_verified", "manifest 不符合 recording-session v0 契约"
        ) from error

    time = manifest["time"]
    if not isinstance(time, Mapping):
        raise ArtifactAccessError("not_verified", "manifest v0 时间结构无效")
    if _api_datetime(time["ended_at"]) < _api_datetime(time["started_at"]):
        raise ArtifactAccessError("not_verified", "manifest v0 时间顺序无效")


def _validate_split_eye_segments(
    manifest: Mapping[str, object],
    video: Mapping[str, object],
    drops: list[Mapping[str, object]],
    dropped_sum: int,
) -> None:
    segments = video["segments"]
    previous_frame_end: int | None = None
    previous_time_end: float | None = None
    for expected_index, segment in enumerate(segments):
        if (
            segment["index"] != expected_index
            or segment["start_frame"] >= segment["end_frame"]
            or segment["start_time_seconds"] >= segment["end_time_seconds"]
            or (previous_frame_end is not None and segment["start_frame"] != previous_frame_end)
            or (
                previous_time_end is not None
                and abs(segment["start_time_seconds"] - previous_time_end) > 1e-9
            )
        ):
            raise ArtifactAccessError("not_verified", "manifest video segment 无效")
        previous_frame_end = segment["end_frame"]
        previous_time_end = float(segment["end_time_seconds"])
    sequence_start = segments[0]["start_frame"]
    sequence_end = segments[-1]["end_frame"]
    if any(
        drop["start_frame"] < sequence_start or drop["end_frame"] > sequence_end for drop in drops
    ):
        raise ArtifactAccessError("not_verified", "drop event 超出视频帧域")
    frames = manifest["frames"]
    if frames["count"] != sequence_end - sequence_start - dropped_sum:
        raise ArtifactAccessError("not_verified", "frames count 与视频帧域不一致")


def _validate_audio(audio: Mapping[str, object]) -> None:
    sample_rate = audio["sample_rate_hz"]
    sample_count = audio["sample_count"]
    if (
        isinstance(sample_rate, bool)
        or not isinstance(sample_rate, int)
        or isinstance(sample_count, bool)
        or not isinstance(sample_count, int)
    ):
        raise ArtifactAccessError("not_verified", "manifest audio 采样字段无效")
    sync = audio["sync"]
    segments = audio["segments"]
    if not isinstance(sync, Mapping) or not isinstance(segments, list):
        raise ArtifactAccessError("not_verified", "manifest audio 结构无效")
    if sync["stopped_monotonic_ns"] < sync["started_monotonic_ns"]:
        raise ArtifactAccessError("not_verified", "manifest audio 单调时间无效")
    if sync.get("timebase") is not None:
        _validate_audio_timeline_sync(sync)
    previous_end = 0
    for expected_index, segment in enumerate(segments):
        if not isinstance(segment, Mapping):
            raise ArtifactAccessError("not_verified", "manifest audio segment 无效")
        if (
            segment["index"] != expected_index
            or segment["start_sample"] != previous_end
            or segment["end_sample"] <= segment["start_sample"]
            or segment["start_time_seconds"] >= segment["end_time_seconds"]
        ):
            raise ArtifactAccessError("not_verified", "manifest audio segment 无效")
        start_time = segment["start_sample"] / sample_rate
        end_time = segment["end_sample"] / sample_rate
        if (
            abs(segment["start_time_seconds"] - start_time) > 1e-9
            or abs(segment["end_time_seconds"] - end_time) > 1e-9
        ):
            raise ArtifactAccessError("not_verified", "manifest audio 时间域无效")
        previous_end = segment["end_sample"]
    if previous_end != sample_count:
        raise ArtifactAccessError("not_verified", "manifest audio sample_count 不一致")


def _validate_audio_timeline_sync(sync: Mapping[str, object]) -> None:
    if sync.get("clock") != "host_monotonic" or sync.get("timebase") != "monotonic_ns":
        raise ArtifactAccessError("not_verified", "manifest audio 时间线无效")
    required_ints = (
        "session_start_monotonic_ns",
        "started_monotonic_ns",
        "stopped_monotonic_ns",
        "session_start_offset_ns",
        "session_stop_offset_ns",
        "sample_duration_ns",
    )
    for field in required_ints:
        value = sync.get(field)
        if isinstance(value, bool) or not isinstance(value, int):
            raise ArtifactAccessError("not_verified", "manifest audio 时间线无效")
    session_start = sync["session_start_monotonic_ns"]
    started = sync["started_monotonic_ns"]
    stopped = sync["stopped_monotonic_ns"]
    start_offset = sync["session_start_offset_ns"]
    stop_offset = sync["session_stop_offset_ns"]
    sample_duration = sync["sample_duration_ns"]
    if (
        session_start <= 0
        or started <= 0
        or stopped <= 0
        or stopped < started
        or start_offset < 0
        or sample_duration <= 0
        or start_offset != started - session_start
        or stop_offset != stopped - session_start
        or stop_offset < start_offset
    ):
        raise ArtifactAccessError("not_verified", "manifest audio 时间线无效")
    start_seconds = sync.get("session_start_offset_seconds")
    stop_seconds = sync.get("session_stop_offset_seconds")
    if (
        isinstance(start_seconds, bool)
        or not isinstance(start_seconds, (int, float))
        or isinstance(stop_seconds, bool)
        or not isinstance(stop_seconds, (int, float))
        or not math.isfinite(float(start_seconds))
        or not math.isfinite(float(stop_seconds))
        or abs(float(start_seconds) - start_offset / 1e9) > 1e-9
        or abs(float(stop_seconds) - stop_offset / 1e9) > 1e-9
    ):
        raise ArtifactAccessError("not_verified", "manifest audio 时间线无效")


def _api_datetime(value: object) -> datetime:
    if not isinstance(value, str):
        raise ArtifactAccessError("not_verified", "manifest 时间戳无效")
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as error:
        raise ArtifactAccessError("not_verified", "manifest 时间戳无效") from error


def _artifact_descriptor(
    raw: object, *, legacy: bool, path_validated: bool = False
) -> ArtifactDescriptor:
    if not isinstance(raw, dict):
        raise ArtifactAccessError("not_verified", "manifest artifact 描述符无效")
    required = {"role", "path", "media_type", "bytes", "sha256"}
    expected = required | ({"records"} if legacy else {"artifact_id"})
    if set(raw) != expected:
        raise ArtifactAccessError("not_verified", "manifest artifact 描述符字段无效")
    sha256 = raw.get("sha256")
    artifact_id = sha256 if legacy else raw.get("artifact_id")
    role = raw.get("role")
    path = raw.get("path")
    media_type = raw.get("media_type")
    size = raw.get("bytes")
    if (
        not isinstance(artifact_id, str)
        or not _SHA256.fullmatch(artifact_id)
        or not isinstance(sha256, str)
        or not _SHA256.fullmatch(sha256)
        or artifact_id != sha256
        or (not legacy and (not isinstance(role, str) or not role))
        or not isinstance(path, str)
        or not isinstance(media_type, str)
        or not _MEDIA_TYPE.fullmatch(media_type)
        or isinstance(size, bool)
        or not isinstance(size, int)
        or size < 0
    ):
        raise ArtifactAccessError("not_verified", "manifest artifact 描述符值无效")
    if legacy:
        records = raw.get("records")
        if isinstance(records, bool) or not isinstance(records, int) or records < 0:
            raise ArtifactAccessError("not_verified", "manifest artifact records 无效")
    if not path_validated:
        _safe_relative_components(path)
    return ArtifactDescriptor(
        artifact_id=artifact_id,
        role=role if isinstance(role, str) else "",
        path=path,
        media_type=media_type,
        bytes=size,
        sha256=sha256,
    )


def _close_all(descriptors: list[int]) -> None:
    for descriptor in reversed(descriptors):
        with suppress(OSError):
            os.close(descriptor)
    descriptors.clear()

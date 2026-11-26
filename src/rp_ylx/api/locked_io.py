"""Descriptor-pinned file opening, identity verification, and locked byte streams."""

from __future__ import annotations

import errno
import hashlib
import os
import re
import stat
from collections.abc import Iterator, Mapping
from contextlib import suppress
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

from rp_ylx.api.compat import facade_attr
from rp_ylx.core.fs import is_exfat_or_msdos_fd
from rp_ylx.native import native_session_store_or_none as _session_store_or_none

_TEMP_COMPONENT = re.compile(r"^[^/]*\.tmp(?:[._-][^/]*)?$")
_READ_CHUNK = 1024 * 1024
_ExclusiveFileIdentity = tuple[int, int, int, int, int, int, int]


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


def _exclusive_regular_identity(descriptor: int) -> _ExclusiveFileIdentity:
    metadata = os.fstat(descriptor)
    if not stat.S_ISREG(metadata.st_mode) or metadata.st_nlink != 1:
        raise ArtifactAccessError("not_verified", "会话对象不是独占普通文件")
    inode = 0 if is_exfat_or_msdos_fd(descriptor) else metadata.st_ino
    return (
        metadata.st_dev,
        inode,
        metadata.st_mode,
        metadata.st_nlink,
        metadata.st_size,
        metadata.st_mtime_ns,
        metadata.st_ctime_ns,
    )


def _native_store() -> object | None:
    return facade_attr("rp_ylx.api.downloads", "_session_store_or_none", _session_store_or_none)()


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
        native = _native_store()
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
        _preverified_sha256: bool = False,
    ) -> None:
        if len(payload) != identity.size or (
            not _preverified_sha256 and hashlib.sha256(payload).hexdigest() != manifest_sha256
        ):
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


def _required_open_flags(*, directory: bool = False) -> int:
    if not hasattr(os, "O_NOFOLLOW") or not hasattr(os, "O_DIRECTORY"):
        raise RuntimeError("安全会话下载需要 Linux O_NOFOLLOW 与 O_DIRECTORY")
    flags = os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW
    if directory:
        flags |= os.O_DIRECTORY
    else:
        flags |= os.O_NONBLOCK
    return flags


def _open_absolute_directory(path: str | Path) -> int:
    raw_path = os.fspath(path)
    if not os.path.isabs(raw_path) or "\x00" in raw_path:
        raise ValueError("generation_root 必须是无 NUL 的绝对路径")
    components = [component for component in raw_path.split("/") if component]
    if any(component in {".", ".."} for component in components):
        raise ValueError("generation_root 不能包含 . 或 ..")
    open_dir = facade_attr(
        "rp_ylx.api.downloads", "_open_directory_component", _open_directory_component
    )
    current = os.open("/", _required_open_flags(directory=True))
    try:
        for component in components:
            following = open_dir(component, current)
            os.close(current)
            current = following
        return current
    except BaseException:
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
    except BaseException:
        os.close(descriptor)
        raise


def _native_open_relative_regular(path: str, session_descriptor: int) -> int | None:
    native = _native_store()
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


def _native_open_verified_artifact(
    selected: ArtifactDescriptor,
    session_descriptor: int,
) -> tuple[int, _FileIdentity] | None:
    native = _native_store()
    if native is None:
        return None
    opener = getattr(native, "open_verified_artifact", None)
    if not callable(opener):
        return None
    try:
        result = opener(
            session_descriptor,
            selected.path,
            selected.bytes,
            selected.sha256,
        )
    except AttributeError:
        return None
    except BaseException as error:
        raise ArtifactAccessError(
            "not_verified", "manifest 声明的 artifact 不存在、已变化或不安全"
        ) from error
    if not isinstance(result, Mapping):
        raise ArtifactAccessError("not_verified", "artifact native open 返回无效结果")
    descriptor = result.get("descriptor")
    raw_identity = result.get("identity")
    if not isinstance(descriptor, int) or descriptor < 0 or not isinstance(raw_identity, Mapping):
        if isinstance(descriptor, int) and descriptor >= 0:
            os.close(descriptor)
        raise ArtifactAccessError("not_verified", "artifact native open 返回无效结果")
    try:
        exclusive = _exclusive_regular_identity(descriptor)
        raw_inode = os.fstat(descriptor).st_ino
        expected_identity = {
            "device": exclusive[0],
            "inode": raw_inode,
            "size": exclusive[4],
            "modified_ns": exclusive[5],
            "nlink": exclusive[3],
        }
        if dict(raw_identity) != expected_identity or exclusive[4] != selected.bytes:
            raise ArtifactAccessError("not_verified", "artifact native identity 与打开文件不一致")
        return descriptor, _FileIdentity.read(descriptor)
    except BaseException:
        os.close(descriptor)
        raise


def _native_read_bounded_file(descriptor: int, maximum_bytes: int) -> bytes | None:
    native = _native_store()
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


def _open_relative_regular(path: str, session_descriptor: int) -> int:
    native_opener = facade_attr(
        "rp_ylx.api.downloads", "_native_open_relative_regular", _native_open_relative_regular
    )
    native_descriptor = native_opener(path, session_descriptor)
    if native_descriptor is not None:
        return native_descriptor
    components = _safe_relative_components(path)
    open_dir = facade_attr(
        "rp_ylx.api.downloads", "_open_directory_component", _open_directory_component
    )
    open_reg = facade_attr(
        "rp_ylx.api.downloads", "_open_regular_component", _open_regular_component
    )
    session_dev = os.fstat(session_descriptor).st_dev
    current = os.dup(session_descriptor)
    try:
        for component in components[:-1]:
            following = open_dir(component, current)
            os.close(current)
            current = following
            if os.fstat(current).st_dev != session_dev:
                raise ArtifactAccessError("not_verified", "artifact 路径跨文件系统挂载边界")
        result = open_reg(components[-1], current)
        try:
            if os.fstat(result).st_dev != session_dev:
                raise ArtifactAccessError("not_verified", "artifact 文件跨文件系统挂载边界")
        except BaseException:
            os.close(result)
            raise
        os.close(current)
        return result
    except BaseException:
        os.close(current)
        raise


def _verify_open_artifact_python(
    descriptor: int,
    selected: ArtifactDescriptor,
) -> _FileIdentity:
    before = _exclusive_regular_identity(descriptor)
    if before[4] != selected.bytes:
        raise ArtifactAccessError("not_verified", "artifact 大小与 manifest 不一致")
    digest = hashlib.sha256()
    cursor = 0
    while cursor < selected.bytes:
        block = os.pread(descriptor, min(_READ_CHUNK, selected.bytes - cursor), cursor)
        if not block:
            raise ArtifactAccessError("not_verified", "artifact 在校验期间被截断")
        digest.update(block)
        cursor += len(block)
    if os.pread(descriptor, 1, selected.bytes):
        raise ArtifactAccessError("not_verified", "artifact 在校验期间增长")
    if _exclusive_regular_identity(descriptor) != before:
        raise ArtifactAccessError("not_verified", "artifact 在校验期间发生变化")
    if digest.hexdigest() != selected.sha256:
        raise ArtifactAccessError("not_verified", "artifact 摘要与 manifest 不一致")
    return _FileIdentity.read(descriptor)


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


def _close_all(descriptors: list[int]) -> None:
    for descriptor in reversed(descriptors):
        with suppress(OSError):
            os.close(descriptor)
    descriptors.clear()

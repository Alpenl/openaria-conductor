"""从已选定的会话 generation 安全读取 immutable manifest 与 artifact。"""

from __future__ import annotations

import hashlib
import os
import threading
from collections.abc import Mapping
from contextlib import suppress
from pathlib import Path

from rp_ylx.api.locked_io import (
    ArtifactAccessError,
    ArtifactDescriptor,
    LockedArtifact,
    LockedBytes,
    LockedManifest,
    _close_all,
    _exclusive_regular_identity,
    _ExclusiveFileIdentity,
    _FileIdentity,
    _native_open_relative_regular,
    _native_open_verified_artifact,
    _native_read_bounded_file,
    _open_absolute_directory,
    _open_directory_component,
    _open_regular_component,
    _open_relative_regular,
    _read_exact_file,
    _required_open_flags,
    _safe_relative_components,
    _session_store_or_none,
    _verify_open_artifact_python,
)
from rp_ylx.api.manifest_validator import (
    _artifact_descriptors,
    _clear_validated_manifest_cache_for_tests,
    _decode_and_validate_manifest,
    _ManifestValidator,
    _validate_device_session_manifest,
    _validate_device_session_v1,
    _validate_device_session_v2,
    _validated_manifest,
    _validated_manifest_with_digest,
    device_session_v1_summary,
    iter_device_session_v1_artifacts,
    validate_device_session_manifest,
    validated_device_session_payload,
)
from rp_ylx.api.range import UnsatisfiableRange, _bounded_range_decimal, parse_single_range
from rp_ylx.core.ids import valid_sha256, valid_uuid4, valid_uuid7
from rp_ylx.manifest_limits import MAX_MANIFEST_BYTES
from rp_ylx.recording_layout import open_relative_directory, resolve_recording

__all__ = [
    "ArtifactAccessError",
    "ArtifactDescriptor",
    "DirectorySessionStore",
    "LockedArtifact",
    "LockedBytes",
    "LockedManifest",
    "UnsatisfiableRange",
    "_ExclusiveFileIdentity",
    "_FileIdentity",
    "_ManifestValidator",
    "_bounded_range_decimal",
    "_clear_validated_manifest_cache_for_tests",
    "_decode_and_validate_manifest",
    "_exclusive_regular_identity",
    "_native_open_relative_regular",
    "_native_open_verified_artifact",
    "_native_read_bounded_file",
    "_open_absolute_directory",
    "_open_directory_component",
    "_open_regular_component",
    "_open_relative_regular",
    "_read_exact_file",
    "_required_open_flags",
    "_safe_relative_components",
    "_session_store_or_none",
    "_validate_device_session_manifest",
    "_validate_device_session_v1",
    "_validate_device_session_v2",
    "_validated_manifest",
    "_validated_manifest_with_digest",
    "_verify_open_artifact_python",
    "device_session_v1_summary",
    "iter_device_session_v1_artifacts",
    "parse_single_range",
    "validate_device_session_manifest",
    "validated_device_session_payload",
]


_DEFAULT_VALIDATED_MANIFEST = _validated_manifest


def _valid_session_id(session_id: object) -> bool:
    return valid_uuid4(session_id) or valid_uuid7(session_id)


class DirectorySessionStore:
    """仅用于 fake/dev：从一个已选定的 generation 根提供安全只读会话。"""

    def __init__(
        self,
        generation_root: str | Path,
        *,
        verified_manifests: Mapping[str, str] | None = None,
        max_manifest_bytes: int = MAX_MANIFEST_BYTES,
        recording_paths: Mapping[str, Path] | None = None,
    ) -> None:
        if max_manifest_bytes <= 0:
            raise ValueError("max_manifest_bytes 必须大于零")
        self._root_descriptor = _open_absolute_directory(generation_root)
        self._generation_root = Path(generation_root)
        self._recording_paths = recording_paths or {}
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

    def _open_manifest_file(self, session_id: str) -> tuple[list[int], int, _FileIdentity]:
        if not _valid_session_id(session_id):
            raise ArtifactAccessError("not_found", "会话不存在")
        owned = [self._duplicate_root()]
        try:
            relative = self._recording_paths.get(session_id)
            if relative is None:
                relative = resolve_recording(
                    self._generation_root, session_id, root_fd=owned[-1]
                ).relative_to(self._generation_root)
            session_descriptor = open_relative_directory(owned[-1], relative)
            owned.append(session_descriptor)
            manifest_descriptor = _open_regular_component("manifest.json", session_descriptor)
            owned.append(manifest_descriptor)
            identity = _FileIdentity.read(manifest_descriptor)
            if identity.size > self._max_manifest_bytes:
                raise ArtifactAccessError("not_found", "会话 manifest 无法读取")
            return owned, manifest_descriptor, identity
        except ArtifactAccessError:
            _close_all(owned)
            raise
        except OSError as error:
            _close_all(owned)
            raise ArtifactAccessError("not_found", "会话不存在或尚未封存") from error
        except BaseException:
            _close_all(owned)
            raise

    def manifest_identity(
        self, session_id: str, *, expected_sha256: str | None = None
    ) -> _ExclusiveFileIdentity:
        """Read current content when checking a previously validated manifest.

        File identity alone is not content proof on coarse-timestamp filesystems.
        Hashing the small manifest avoids reparsing unchanged JSON or reading
        historical video payloads on a catalog request.
        """
        owned, descriptor, file_identity = self._open_manifest_file(session_id)
        try:
            before = _exclusive_regular_identity(descriptor)
            if expected_sha256 is not None:
                payload = _read_exact_file(descriptor, file_identity)
                if (
                    hashlib.sha256(payload).hexdigest() != expected_sha256
                    or _exclusive_regular_identity(descriptor) != before
                ):
                    raise ArtifactAccessError("not_verified", "manifest 内容已变化", reason="stale")
            return before
        finally:
            _close_all(owned)

    def _open_manifest_bytes(self, session_id: str) -> tuple[list[int], int, _FileIdentity, bytes]:
        owned, manifest_descriptor, identity = self._open_manifest_file(session_id)
        try:
            payload = _read_exact_file(manifest_descriptor, identity)
            return owned, manifest_descriptor, identity, payload
        except ArtifactAccessError:
            _close_all(owned)
            raise
        except OSError as error:
            _close_all(owned)
            raise ArtifactAccessError("not_found", "会话不存在或尚未封存") from error
        except BaseException:
            _close_all(owned)
            raise

    def open_manifest(self, session_id: str, api_version: str) -> LockedManifest:
        owned, manifest_descriptor, identity, payload = self._open_manifest_bytes(session_id)
        try:
            if _validated_manifest is _DEFAULT_VALIDATED_MANIFEST:
                _, manifest_sha256 = _validated_manifest_with_digest(
                    payload, session_id, api_version
                )
            else:
                _validated_manifest(payload, session_id, api_version)
                manifest_sha256 = hashlib.sha256(payload).hexdigest()
            representation = LockedManifest(
                manifest_descriptor,
                owned,
                identity=identity,
                manifest_sha256=manifest_sha256,
                payload=payload,
                _preverified_sha256=True,
            )
            owned = []
            return representation
        except BaseException:
            _close_all(owned)
            raise

    def open_artifact(self, session_id: str, artifact_id: str, api_version: str) -> LockedArtifact:
        """Open one declared artifact WITHOUT checking content digest or verification receipts.

        Intended strictly for fake/dev session stores; production gateways must call
        ``open_verified_artifact`` so SHA-256 content verification is enforced.
        """
        if not valid_sha256(artifact_id):
            raise ArtifactAccessError("not_found", "artifact 不存在")
        owned, _, _, manifest_bytes = self._open_manifest_bytes(session_id)
        try:
            manifest = _validated_manifest(manifest_bytes, session_id, api_version)
            selected = _artifact_descriptors(manifest).get(artifact_id)
            if selected is None:
                raise ArtifactAccessError("not_found", "artifact 不存在")
            descriptor = _open_relative_regular(selected.path, owned[1])
            owned.append(descriptor)
            metadata = _exclusive_regular_identity(descriptor)
            if metadata[4] != selected.bytes:
                raise ArtifactAccessError("not_verified", "录像文件大小异常，无法读取")
            representation = LockedArtifact(
                descriptor,
                owned,
                identity=_FileIdentity.read(descriptor),
                artifact_descriptor=selected,
            )
            owned = []
            return representation
        except OSError as error:
            raise ArtifactAccessError("not_verified", "录像文件无法读取") from error
        finally:
            _close_all(owned)

    def open_verified_artifact(
        self, session_id: str, artifact_id: str, api_version: str
    ) -> LockedArtifact:
        """Open one artifact after matching a verified manifest receipt and full SHA-256 check."""
        if not valid_sha256(artifact_id):
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
                manifest = _validated_manifest(
                    manifest_bytes,
                    session_id,
                    api_version,
                    manifest_sha256=manifest_sha256,
                )
            except ArtifactAccessError as error:
                raise ArtifactAccessError(
                    "not_verified", "当前 manifest 无法作为 sealed 会话使用"
                ) from error
            selected = _artifact_descriptors(manifest).get(artifact_id)
            if selected is None:
                raise ArtifactAccessError("not_found", "artifact 不存在")

            native_artifact = _native_open_verified_artifact(selected, owned[1])
            try:
                if native_artifact is None:
                    artifact_descriptor = _open_relative_regular(selected.path, owned[1])
                    try:
                        identity = _verify_open_artifact_python(artifact_descriptor, selected)
                    except BaseException:
                        os.close(artifact_descriptor)
                        raise
                else:
                    artifact_descriptor, identity = native_artifact
            except ArtifactAccessError as error:
                raise ArtifactAccessError(
                    "not_verified", "manifest 声明的 artifact 不存在、已变化或不安全"
                ) from error
            owned.append(artifact_descriptor)
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

    def snapshot_verified_identities(
        self,
        session_id: str,
        api_version: str,
    ) -> tuple[
        bytes,
        _ExclusiveFileIdentity,
        tuple[tuple[str, _ExclusiveFileIdentity], ...],
    ]:
        """Read one verified manifest and stat every artifact it declares."""
        verified_sha256 = self._verified_manifests.get(session_id)
        if verified_sha256 is None:
            raise ArtifactAccessError("not_verified", "会话尚无验证结果", reason="absent")
        try:
            owned, manifest_descriptor, _, manifest_bytes = self._open_manifest_bytes(session_id)
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
                manifest = _validated_manifest(
                    manifest_bytes,
                    session_id,
                    api_version,
                    manifest_sha256=manifest_sha256,
                )
            except ArtifactAccessError as error:
                raise ArtifactAccessError(
                    "not_verified", "当前 manifest 无法作为 sealed 会话使用"
                ) from error
            descriptors = _artifact_descriptors(manifest)

            artifact_identities: list[tuple[str, _ExclusiveFileIdentity]] = []
            for artifact_id, selected in descriptors.items():
                artifact_descriptor = _open_relative_regular(selected.path, owned[1])
                try:
                    identity = _exclusive_regular_identity(artifact_descriptor)
                finally:
                    os.close(artifact_descriptor)
                if identity[4] != selected.bytes:
                    raise ArtifactAccessError("not_verified", "artifact 大小与 manifest 不一致")
                artifact_identities.append((artifact_id, identity))

            return (
                manifest_bytes,
                _exclusive_regular_identity(manifest_descriptor),
                tuple(artifact_identities),
            )
        except ArtifactAccessError:
            raise
        except OSError as error:
            raise ArtifactAccessError("not_verified", "artifact 无法安全读取") from error
        finally:
            _close_all(owned)

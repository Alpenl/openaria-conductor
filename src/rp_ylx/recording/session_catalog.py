"""Session catalog discovery, path indexing, representation tracking, and deletion."""

from __future__ import annotations

import base64
import binascii
import copy
import hashlib
import hmac
import json
import os
import sqlite3
import threading
import uuid
from collections.abc import Callable, Iterator, Mapping
from contextlib import ExitStack, closing, suppress
from dataclasses import dataclass
from datetime import UTC, datetime
from http import HTTPStatus
from pathlib import Path

from rp_ylx.api.downloads import (
    ArtifactAccessError,
    DirectorySessionStore,
    LockedArtifact,
    LockedBytes,
    LockedManifest,
    iter_device_session_v1_artifacts,
)
from rp_ylx.api.events import validate_session_summary
from rp_ylx.api.gateway import ProviderError
from rp_ylx.operational_logging import operational_logger
from rp_ylx.persistence import PersistenceError
from rp_ylx.recording.catalog import SessionCatalog
from rp_ylx.recording.device_session import (
    DeviceRecordingError,
    inspect_device_session_directory,
    manifest_artifact_bytes_total,
    validate_device_session_directory,
)
from rp_ylx.recording.recovery import RECOVERY_ROLE
from rp_ylx.recording.volume import VolumeAdmission
from rp_ylx.recording_layout import (
    FLAGS as LAYOUT_FLAGS,
)
from rp_ylx.recording_layout import (
    iter_recording_entries,
    publish_readable,
    resolve_recording,
)
from rp_ylx.recording_layout import manifest_id as layout_manifest_id

SESSION_CATALOG_REVISION_DOMAIN = b"ylx.session-list.v3.catalog-revision\x00"
SESSION_CURSOR_DOMAIN = b"ylx.session-list.v3.cursor\x00"
SESSION_CURSOR_SCHEMA = "ylx.session-list.cursor.v1"
SESSION_DIAGNOSTIC_SUMMARIES = {
    "manifest_unreadable": "会话清单无法安全读取",
    "unsupported_schema": "会话清单 schema 不受当前 Device API 支持",
    "manifest_invalid": "会话清单或 artifact 描述不符合 Device Session 合同",
    "manifest_not_sealed": "会话尚未形成可下载的 sealed 清单",
}
GATEWAY_VERIFICATION_DIAGNOSTICS = {
    "digest_mismatch": (
        "artifact_digest_mismatch",
        "artifact 内容 SHA-256 与清单声明不一致",
    ),
    "artifact_invalid": (
        "artifact_invalid",
        "artifact 类型、大小或稳定身份与清单声明不一致",
    ),
    "manifest_invalid": (
        "manifest_invalid",
        "manifest 内容不符合 Device Session 合同",
    ),
}


@dataclass(frozen=True, slots=True)
class _VerifiedSessionSnapshot:
    manifest_sha256: str
    manifest_id: str
    volume_id: str
    sealed_at: str
    manifest_identity: tuple[int, int, int, int, int, int, int]
    artifact_identities: tuple[tuple[str, tuple[int, int, int, int, int, int, int]], ...]


def canonical_json_bytes(value: object) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def stable_quarantine_id(candidate_identity: str, code: str) -> str:
    digest = bytearray(
        hashlib.sha256(
            b"ylx.session-list.v3.quarantine-id\x00"
            + candidate_identity.encode("utf-8", errors="surrogatepass")
            + b"\x00"
            + code.encode("ascii")
        ).digest()[:16]
    )
    digest[6] = (digest[6] & 0x0F) | 0x40
    digest[8] = (digest[8] & 0x3F) | 0x80
    return str(uuid.UUID(bytes=bytes(digest)))


def session_catalog_revision(
    items: list[dict[str, object]],
    diagnostics: list[dict[str, object]],
) -> str:
    # Preserve the exact canonical v1 JSON byte stream and existing cursors,
    # without materializing/encoding the entire catalog in one GIL-held call.
    digest = hashlib.sha256(SESSION_CATALOG_REVISION_DOMAIN)
    digest.update(b'{"entries":[')
    batch: list[dict[str, object]] = []
    separator = b""

    def consume_batch() -> None:
        nonlocal separator
        digest.update(separator)
        digest.update(canonical_json_bytes(batch)[1:-1])
        separator = b","
        batch.clear()

    for kind, values in (("session", items), ("diagnostic", diagnostics)):
        for value in values:
            batch.append({"kind": kind, "value": value})
            if len(batch) == 64:
                consume_batch()
    if batch:
        consume_batch()
    digest.update(b'],"schema":"ylx.session-catalog-revision-input.v1"}')
    return f"sha256:{digest.hexdigest()}"


def encode_session_cursor(
    *,
    catalog_revision: str,
    take_id: str | None,
    offset: int,
) -> str:
    payload = canonical_json_bytes(
        {
            "schema": SESSION_CURSOR_SCHEMA,
            "catalog_revision": catalog_revision,
            "take_id": take_id,
            "offset": offset,
        }
    )
    signature = hashlib.sha256(SESSION_CURSOR_DOMAIN + payload).digest()
    return base64.urlsafe_b64encode(payload + signature).rstrip(b"=").decode("ascii")


def decode_session_cursor(cursor: str) -> Mapping[str, object]:
    if len(cursor) > 2048:
        raise ProviderError("invalid_cursor", "会话游标无效", status=HTTPStatus.BAD_REQUEST)
    try:
        encoded = cursor.encode("ascii")
        padding = b"=" * ((4 - len(encoded) % 4) % 4)
        decoded = base64.b64decode(encoded + padding, altchars=b"-_", validate=True)
        if len(decoded) <= 32:
            raise ValueError("cursor payload is empty")
        payload, signature = decoded[:-32], decoded[-32:]
        expected = hashlib.sha256(SESSION_CURSOR_DOMAIN + payload).digest()
        if not hmac.compare_digest(signature, expected):
            raise ValueError("cursor signature mismatch")
        value = json.loads(payload)
        if canonical_json_bytes(value) != payload:
            raise ValueError("cursor payload is not canonical")
    except (
        UnicodeEncodeError,
        UnicodeDecodeError,
        binascii.Error,
        json.JSONDecodeError,
        TypeError,
        ValueError,
    ) as error:
        raise ProviderError(
            "invalid_cursor",
            "会话游标无效",
            status=HTTPStatus.BAD_REQUEST,
        ) from error
    if (
        not isinstance(value, Mapping)
        or set(value) != {"schema", "catalog_revision", "take_id", "offset"}
        or value.get("schema") != SESSION_CURSOR_SCHEMA
        or not isinstance(value.get("catalog_revision"), str)
        or (value.get("take_id") is not None and not isinstance(value.get("take_id"), str))
        or type(value.get("offset")) is not int
        or int(value["offset"]) <= 0
    ):
        raise ProviderError("invalid_cursor", "会话游标无效", status=HTTPStatus.BAD_REQUEST)
    return value


class _TrackedRepresentation:
    """Tracks handle lifetime, contiguous byte coverage, and transfer completion."""

    def __init__(
        self,
        representation: LockedBytes,
        release: Callable[[], None],
        on_close: Callable[[str, int], None] | None = None,
    ) -> None:
        self._representation = representation
        self._release = release
        self._closed = False
        self._on_close = on_close
        self._bytes_read = 0
        self._covered_ranges: list[tuple[int, int]] = []
        self._full_read = False
        self._failed = False
        self.etag = representation.etag
        self.size = representation.size
        self.content_type = representation.content_type
        descriptor = getattr(representation, "descriptor", None)
        if descriptor is not None:
            self.descriptor = descriptor

    def __enter__(self) -> _TrackedRepresentation:
        if self._closed:
            raise RuntimeError("会话表示已关闭")
        return self

    def __exit__(self, *args: object) -> None:
        self.close()

    def __del__(self) -> None:
        self.close()

    def _record_coverage(self, start: int, count: int) -> None:
        if self.size == 0 and start == 0:
            self._full_read = True
            return
        if count <= 0 or start < 0:
            return
        end = min(self.size, start + count)
        if end <= start:
            return
        ranges = self._covered_ranges
        # Fast sequential path: extending the single contiguous [0, cursor) span.
        if len(ranges) == 1 and ranges[0][0] == 0 and start <= ranges[0][1]:
            if end > ranges[0][1]:
                ranges[0] = (0, end)
            if ranges[0] == (0, self.size):
                self._full_read = True
            return
        ranges.append((start, end))
        ranges.sort()
        merged: list[tuple[int, int]] = [ranges[0]]
        for cur_start, cur_end in ranges[1:]:
            prev_start, prev_end = merged[-1]
            if cur_start <= prev_end:
                merged[-1] = (prev_start, max(prev_end, cur_end))
            else:
                merged.append((cur_start, cur_end))
        self._covered_ranges = merged
        if merged == [(0, self.size)]:
            self._full_read = True

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        try:
            self._representation.close()
        finally:
            try:
                if self._on_close is not None:
                    status = (
                        "failed"
                        if self._failed
                        else "served"
                        if self._full_read
                        else "partial"
                        if self._bytes_read
                        else "metadata_only"
                    )
                    self._on_close(status, self._bytes_read)
            finally:
                self._release()

    def read(self, offset: int = 0, length: int | None = None) -> bytes:
        try:
            value = self._representation.read(offset, length)
            self._bytes_read += len(value)
            self._record_coverage(offset, len(value))
            return value
        except BaseException:
            self._failed = True
            raise

    def iter_chunks(
        self,
        offset: int = 0,
        length: int | None = None,
        *,
        chunk_size: int = 1024 * 1024,
    ) -> Iterator[bytes]:
        cursor = offset
        try:
            for chunk in self._representation.iter_chunks(offset, length, chunk_size=chunk_size):
                chunk_len = len(chunk)
                self._bytes_read += chunk_len
                self._record_coverage(cursor, chunk_len)
                cursor += chunk_len
                yield chunk
            if self.size == 0 and offset == 0:
                self._full_read = True
        except GeneratorExit:
            raise
        except BaseException:
            self._failed = True
            raise

    def send_to(self, output_descriptor: int, offset: int = 0, length: int | None = None) -> int:
        try:
            count = self._representation.send_to(output_descriptor, offset, length)
            self._bytes_read += count
            self._record_coverage(offset, count)
            return count
        except BaseException:
            self._failed = True
            raise


class _MultiRootSessionStore:
    """Read sealed sessions from the current recordings root and legacy roots."""

    def __init__(
        self,
        roots: tuple[Path, ...],
        *,
        verified_manifests: Mapping[str, str],
        recording_paths: Mapping[Path, Mapping[str, Path]] | None = None,
        on_stale_path: Callable[[Path, str], None] | None = None,
    ) -> None:
        self._roots = roots
        self._verified_manifests = dict(verified_manifests)
        self._on_stale_path = on_stale_path
        self._stores: list[tuple[Path, DirectorySessionStore, Mapping[str, Path] | None]] = []
        with ExitStack() as cleanup:
            for root in roots:
                root_paths = (
                    None
                    if recording_paths is None or root not in recording_paths
                    else dict(recording_paths[root])
                )
                store = DirectorySessionStore(
                    root,
                    verified_manifests=self._verified_manifests,
                    recording_paths=root_paths,
                )
                cleanup.callback(store.close)
                self._stores.append((root, store, root_paths))
            cleanup.pop_all()

    def close(self) -> None:
        stores, self._stores = self._stores, []
        with ExitStack() as cleanup:
            for _, store, _ in stores:
                cleanup.callback(store.close)

    def _call_with_fallback(
        self,
        session_id: str,
        fn_name: str,
        *args: object,
        retry_stale: bool = False,
        **kwargs: object,
    ) -> object:
        last_retryable: ArtifactAccessError | None = None
        for root, store, root_paths in self._stores:
            try:
                return getattr(store, fn_name)(session_id, *args, **kwargs)
            except ArtifactAccessError as error:
                if (
                    error.code == "not_found"
                    and root_paths is not None
                    and session_id in root_paths
                ):
                    if self._on_stale_path is not None:
                        self._on_stale_path(root, session_id)
                    with closing(
                        DirectorySessionStore(
                            root,
                            verified_manifests=self._verified_manifests,
                            recording_paths=None,
                        )
                    ) as fallback:
                        try:
                            return getattr(fallback, fn_name)(session_id, *args, **kwargs)
                        except ArtifactAccessError as fallback_error:
                            error = fallback_error
                if error.code == "not_found" or (
                    retry_stale and error.code == "not_verified" and error.reason == "stale"
                ):
                    last_retryable = error
                    continue
                raise
        if last_retryable is not None:
            raise last_retryable
        raise ArtifactAccessError("not_found", "会话不存在")

    def open_manifest(self, session_id: str, api_version: str) -> LockedManifest:
        return self._call_with_fallback(session_id, "open_manifest", api_version)  # type: ignore[return-value]

    def manifest_identity(
        self, session_id: str, *, expected_sha256: str | None = None
    ) -> tuple[int, int, int, int, int, int, int]:
        return self._call_with_fallback(  # type: ignore[return-value]
            session_id, "manifest_identity", expected_sha256=expected_sha256
        )

    def open_artifact(self, session_id: str, artifact_id: str, api_version: str) -> LockedArtifact:
        return self._call_with_fallback(session_id, "open_artifact", artifact_id, api_version)  # type: ignore[return-value]

    def snapshot_verified_identities(
        self,
        session_id: str,
        api_version: str,
    ) -> tuple[
        bytes,
        tuple[int, int, int, int, int, int, int],
        tuple[tuple[str, tuple[int, int, int, int, int, int, int]], ...],
    ]:
        return self._call_with_fallback(  # type: ignore[return-value]
            session_id,
            "snapshot_verified_identities",
            api_version,
            retry_stale=True,
        )


class CatalogService:
    """Owns session catalog indexing, summaries, diagnostics, and artifact handles."""

    def __init__(
        self,
        state_root: Path,
        *,
        commit: str,
        inspect_fn: Callable[..., tuple[Mapping[str, object], bytes]] | None = None,
        validate_fn: Callable[..., Mapping[str, object]] | None = None,
        validate_summary_fn: Callable[[object], None] | None = None,
        store_cls: type[_MultiRootSessionStore] | None = None,
    ) -> None:
        self._commit = commit
        self._inspect_fn = inspect_fn or inspect_device_session_directory
        self._validate_fn = validate_fn or validate_device_session_directory
        self._validate_summary_fn = validate_summary_fn or validate_session_summary
        self._store_cls = store_cls or _MultiRootSessionStore

        self._catalog_lock = threading.RLock()
        self._catalog_db = SessionCatalog(state_root / "catalog.sqlite3")
        self._catalog_verification_lock = threading.Lock()
        self._catalog_verification_cancel = threading.Event()
        self._catalog_verification_epoch = 0
        self._catalog_verification_session: str | None = None

        self._recording_paths: dict[Path, dict[str, Path]] = {}
        self._verified: dict[str, str] = {}
        self._session_snapshots: dict[str, _VerifiedSessionSnapshot] = {}
        self._session_summaries: dict[str, dict[str, object]] = {}
        self._session_diagnostics: dict[str, dict[str, object]] = {}

    def invalidate_session_verification(self, session_id: str) -> None:
        with self._catalog_lock:
            self._verified.pop(session_id, None)
            self._session_snapshots.pop(session_id, None)
            summary = self._session_summaries.get(session_id)
            if summary is not None:
                summary["verification"] = None

    def forget_recording_path(
        self,
        root: Path,
        session_id: str,
        *,
        volume_id: str | None = None,
    ) -> None:
        with self._catalog_lock:
            entries = self._recording_paths.get(root)
            if entries is not None:
                entries.pop(session_id, None)
        if volume_id is not None:
            with suppress(OSError, sqlite3.Error, PersistenceError):
                self._catalog_db.forget_path(volume_id, session_id)

    def publish_recording_directory(self, path: Path, manifest: Mapping[str, object]) -> Path:
        try:
            published = publish_readable(path, manifest)
            session_id = str(manifest["session_id"])
            with self._catalog_lock:
                self._recording_paths.setdefault(path.parent, {})[session_id] = (
                    published.relative_to(path.parent)
                )
            return published
        except (OSError, ValueError, AttributeError):
            operational_logger("recording").event(
                "readable_directory_deferred",
                level="warning",
                transaction_id=str(manifest.get("session_id")),
            )
            return resolve_recording(path.parent, str(manifest["session_id"]))

    def _paths_snapshot(self, volume_id: str) -> dict[Path, dict[str, Path]]:
        cached = self._catalog_db.cached_paths(volume_id)
        with self._catalog_lock:
            for root, entries in self._recording_paths.items():
                merged = cached.setdefault(root, {})
                merged.update(entries)
        return cached

    def open_store(
        self,
        admission: VolumeAdmission,
        *,
        verified_manifests: Mapping[str, str] | None = None,
    ) -> _MultiRootSessionStore:
        with self._catalog_lock:
            verified = (
                dict(self._verified) if verified_manifests is None else dict(verified_manifests)
            )
        paths = self._paths_snapshot(admission.volume_id)
        return self._store_cls(
            admission.catalog_roots,
            verified_manifests=verified,
            recording_paths=paths,
            on_stale_path=lambda root, sid: self.forget_recording_path(
                root, sid, volume_id=admission.volume_id
            ),
        )

    def load_manifest(self, admission: VolumeAdmission, session_id: str) -> Mapping[str, object]:
        paths = self._paths_snapshot(admission.volume_id)
        for sessions_root in admission.catalog_roots:
            relative = paths.get(sessions_root, {}).get(session_id)
            path = (
                sessions_root / relative
                if relative is not None
                else resolve_recording(sessions_root, session_id)
            )
            try:
                manifest, _ = self._inspect_fn(path)
                return manifest
            except (OSError, DeviceRecordingError):
                if relative is not None:
                    self.forget_recording_path(
                        sessions_root, session_id, volume_id=admission.volume_id
                    )
                    try:
                        manifest, _ = self._inspect_fn(resolve_recording(sessions_root, session_id))
                        return manifest
                    except (OSError, DeviceRecordingError):
                        pass
                continue
        raise ArtifactAccessError("not_found", "会话不存在")

    def session_manifest_is_current(
        self,
        admission: VolumeAdmission,
        session_id: str,
        snapshot: _VerifiedSessionSnapshot,
    ) -> bool:
        store = self.open_store(
            admission,
            verified_manifests={session_id: snapshot.manifest_sha256},
        )
        try:
            return (
                store.manifest_identity(session_id, expected_sha256=snapshot.manifest_sha256)
                == snapshot.manifest_identity
            )
        except (ArtifactAccessError, DeviceRecordingError, OSError):
            return False
        finally:
            store.close()

    def session_payload_is_current(
        self,
        admission: VolumeAdmission,
        session_id: str,
        snapshot: _VerifiedSessionSnapshot,
    ) -> bool:
        store = self.open_store(
            admission,
            verified_manifests={session_id: snapshot.manifest_sha256},
        )
        try:
            payload, manifest_identity, artifact_identities = store.snapshot_verified_identities(
                session_id, "v4"
            )
            return (
                hashlib.sha256(payload).hexdigest() == snapshot.manifest_sha256
                and manifest_identity == snapshot.manifest_identity
                and artifact_identities == snapshot.artifact_identities
            )
        except (ArtifactAccessError, DeviceRecordingError, OSError):
            return False
        finally:
            store.close()

    def verify_exact_session_payload(
        self,
        admission: VolumeAdmission,
        session_path: Path,
        session_id: str,
        manifest: Mapping[str, object],
        manifest_payload: bytes,
        *,
        interrupt_check: Callable[[], None] | None = None,
        verified_artifacts=None,
        progress: Callable[[int, int], None] | None = None,
    ) -> tuple[_VerifiedSessionSnapshot, DeviceRecordingError | None]:
        manifest_sha256 = hashlib.sha256(manifest_payload).hexdigest()
        store = self.open_store(
            admission,
            verified_manifests={session_id: manifest_sha256},
        )
        try:
            try:
                descriptors = list(iter_device_session_v1_artifacts(manifest))
            except ArtifactAccessError as error:
                raise DeviceRecordingError("manifest_invalid", error.message) from error
            seen_artifact_ids: set[str] = set()
            for descriptor in descriptors:
                artifact_id = descriptor.get("artifact_id")
                if not isinstance(artifact_id, str) or artifact_id in seen_artifact_ids:
                    raise DeviceRecordingError("manifest_invalid", "manifest artifact 描述符无效")
                seen_artifact_ids.add(artifact_id)

            def snapshot() -> _VerifiedSessionSnapshot:
                try:
                    current_payload, manifest_identity, artifact_identities = (
                        store.snapshot_verified_identities(session_id, "v4")
                    )
                    if current_payload != manifest_payload:
                        raise DeviceRecordingError(
                            "verification_changed",
                            "manifest 在独立校验期间发生变化",
                        )
                except ArtifactAccessError as error:
                    raise DeviceRecordingError(
                        "verification_changed",
                        "会话在独立校验期间发生变化",
                    ) from error
                return _VerifiedSessionSnapshot(
                    manifest_sha256=manifest_sha256,
                    manifest_id=manifest["manifest_id"],
                    volume_id=manifest["volume_id"],
                    sealed_at=manifest["sealed_at"],
                    manifest_identity=manifest_identity,
                    artifact_identities=artifact_identities,
                )

            before = snapshot()
            verification_error: DeviceRecordingError | None = None
            try:
                validated_manifest = self._validate_fn(
                    session_path,
                    expected_session_id=session_id,
                    interrupt_check=interrupt_check,
                    **({"verified_artifacts": verified_artifacts} if verified_artifacts else {}),
                    **({"progress": progress} if progress is not None else {}),
                )
            except DeviceRecordingError as error:
                if error.code != "digest_mismatch":
                    raise
                verification_error = error
                validated_manifest = manifest
            if validated_manifest != manifest:
                raise DeviceRecordingError(
                    "verification_changed",
                    "manifest 在独立校验期间发生变化",
                )
            after = snapshot()
            if before != after:
                raise DeviceRecordingError(
                    "verification_changed",
                    "会话在独立校验期间发生变化",
                )
            return after, verification_error
        finally:
            store.close()

    @staticmethod
    def session_diagnostic(candidate_identity: str, error: object) -> dict[str, object]:
        code = "manifest_invalid"
        if isinstance(error, OSError):
            code = "manifest_unreadable"
        elif isinstance(error, DeviceRecordingError):
            text = f"{error.code} {error.message}".lower()
            cause = error.__cause__
            schema_error = getattr(cause, "__cause__", None)
            schema_path = tuple(getattr(schema_error, "absolute_path", ()))
            if any(
                marker in text
                for marker in (
                    "no such file",
                    "not found",
                    "expecting value",
                    "expecting property",
                    "utf-8",
                    "无法安全打开",
                    "无法读取",
                    "超过允许大小",
                )
            ):
                code = "manifest_unreadable"
            elif any(
                marker in text
                for marker in (
                    "unsupported schema",
                    "unknown schema",
                    "schema unsupported",
                    "schema unknown",
                    "不支持的 schema",
                    "不是支持的 device-session",
                    "未知 schema",
                )
            ):
                code = "unsupported_schema"
            elif schema_path == ("sealed",) or (
                "sealed" in text
                and any(
                    marker in text
                    for marker in ("false", "unsealed", "未封存", "尚未", "必须为 true")
                )
            ):
                code = "manifest_not_sealed"
        return {
            "quarantine_id": stable_quarantine_id(candidate_identity, code),
            "code": code,
            "observed_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
            "message": SESSION_DIAGNOSTIC_SUMMARIES[code],
        }

    def session_summary(
        self,
        session_id: str,
        manifest: Mapping[str, object],
        manifest_payload: bytes,
        *,
        verification_error: DeviceRecordingError | None = None,
        verification_current: bool = True,
    ) -> dict[str, object]:
        del session_id
        take = manifest["take"]
        timing = manifest["time"]
        device = manifest["device"]
        integrity = manifest["integrity"]
        if not all(isinstance(value, Mapping) for value in (take, timing, device, integrity)):
            raise DeviceRecordingError("manifest_invalid", "会话清单结构无效")
        manifest_sha256 = hashlib.sha256(manifest_payload).hexdigest()
        validator_name = "openaria-conductor-device-session-v2-integrity"
        validator_version = "1"
        validator_build_sha256 = hashlib.sha256(
            (f"{validator_name}\x00{validator_version}\x00{self._commit}").encode()
        ).hexdigest()
        diagnostics: list[dict[str, str]] = []
        if any(log.get("role") == RECOVERY_ROLE for log in manifest.get("logs", [])):
            diagnostics.append(
                {
                    "code": "recording_interrupted",
                    "summary": "录制已中断，完整前段已保存，可正常导出",
                }
            )
        if verification_error is not None:
            code, summary = GATEWAY_VERIFICATION_DIAGNOSTICS.get(
                verification_error.code,
                (
                    "verification_failed",
                    "gateway 无法确认 artifact 内容与 sealed 清单一致",
                ),
            )
            diagnostics.append({"code": code, "summary": summary})
        verification: dict[str, object] | None = None
        if verification_current:
            verification = {
                "actor": "gateway",
                "validator": {
                    "name": validator_name,
                    "version": validator_version,
                    "build_sha256": validator_build_sha256,
                },
                "manifest_sha256": manifest_sha256,
                "verified_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
                "verdict": "usable" if verification_error is None else "unusable",
                "diagnostics": diagnostics,
            }
        return {
            "session_id": manifest["session_id"],
            "producer_outcome": "sealed",
            "take_id": take["take_id"],
            "take_sequence": take["sequence"],
            "continuation_of": take["continuation_of"],
            "display_name": manifest["display_name"],
            "device": {
                "device_id": device["device_id"],
                "device_label": device["device_label"],
            },
            "started_at": timing["started_at"],
            "ended_at": timing["ended_at"],
            "duration_seconds": timing["duration_seconds"],
            "total_bytes": manifest_artifact_bytes_total(manifest, code="manifest_invalid"),
            "verification": verification,
        }

    def _iter_catalog_entries(
        self,
        root: Path,
        volume_id: str,
    ) -> Iterator[tuple[Path, str, bool]]:
        """Yield `(directory, session_id_hint, from_cache)` skipping cached manifest parses."""
        return iter_recording_entries(
            root,
            lookup_cached_id=lambda rel: self._catalog_db.cached_session_for_path(
                volume_id, root, rel
            ),
        )

    @staticmethod
    def _disk_manifest_identity(directory: Path) -> str:
        try:
            fd = os.open(directory, LAYOUT_FLAGS)
        except OSError:
            return directory.name
        try:
            return layout_manifest_id(fd)
        except (OSError, ValueError, RecursionError):
            return directory.name
        finally:
            os.close(fd)

    def check_catalog_verification_idle(self, *, is_busy: Callable[[], bool]) -> None:
        if is_busy() or self._catalog_verification_cancel.is_set():
            raise DeviceRecordingError("verification_changed", "历史内容校验已暂停，等待设备空闲")

    def verify_catalog_session(
        self,
        session_id: str,
        *,
        stop_condition: threading.Condition,
        is_busy: Callable[[], bool],
        is_releasing: Callable[[bool], bool],
        require_admission: Callable[[], VolumeAdmission],
        retained_ids: Callable[[], set[str]],
        verify_payload_fn: Callable[
            ..., tuple[_VerifiedSessionSnapshot, DeviceRecordingError | None]
        ]
        | None = None,
        cancelled: Callable[[], bool] | None = None,
        allow_pending_safe_swap: bool = False,
    ) -> None:
        registered = False
        with stop_condition:
            epoch = self._catalog_verification_epoch

        def check() -> None:
            self.check_catalog_verification_idle(is_busy=is_busy)
            with stop_condition:
                current_epoch = self._catalog_verification_epoch
            if epoch != current_epoch:
                raise DeviceRecordingError("verification_changed", "排队的录像校验已取消")
            if cancelled is not None and cancelled():
                raise DeviceRecordingError("verification_changed", "下载请求已取消")

        while not self._catalog_verification_lock.acquire(timeout=0.05):
            try:
                check()
            except DeviceRecordingError as error:
                raise ProviderError(
                    "verification_cancelled",
                    "录像校验已中止，请稍后重试",
                    status=503,
                    retryable=True,
                ) from error
        try:
            with stop_condition:
                if epoch != self._catalog_verification_epoch:
                    raise DeviceRecordingError("verification_changed", "排队的录像校验已取消")
                if is_busy():
                    raise ProviderError(
                        "verification_busy", "采集资源繁忙，请稍后重试", status=503, retryable=True
                    )
                if is_releasing(allow_pending_safe_swap):
                    raise ProviderError(
                        "volume_releasing", "录制卷正在安全释放", status=423, retryable=True
                    )
                self._catalog_verification_cancel.clear()
                self._catalog_verification_session = session_id
                registered = True
            check()
            admission = require_admission()
            with self._catalog_lock:
                snapshot = self._session_snapshots.get(session_id)
            if snapshot is not None and self.session_payload_is_current(
                admission, session_id, snapshot
            ):
                check()
                return
            self.invalidate_session_verification(session_id)
            with stop_condition:
                retained = retained_ids()
            verify_fn = verify_payload_fn or (
                lambda session_path, sid, manifest, payload, **kw: (
                    self.verify_exact_session_payload(
                        admission, session_path, sid, manifest, payload, **kw
                    )
                )
            )
            for root in admission.catalog_roots:
                directory = resolve_recording(root, session_id)
                if not directory.is_dir():
                    continue
                try:
                    signature = self._catalog_db.signature(directory)
                    manifest, payload = self._inspect_fn(directory)
                    recovered = any(log.get("role") == RECOVERY_ROLE for log in manifest["logs"])
                    if session_id in retained and not recovered:
                        raise DeviceRecordingError(
                            "manifest_invalid", "会话发布失败，未进入可下载 catalog"
                        )
                    check()
                    snapshot, error = verify_fn(
                        directory,
                        session_id,
                        manifest,
                        payload,
                        interrupt_check=check,
                    )
                    check()
                    with self._catalog_lock:
                        check()
                        if not self.session_payload_is_current(admission, session_id, snapshot):
                            raise DeviceRecordingError(
                                "verification_changed", "会话在校验结果发布前发生变化"
                            )
                        summary = self.session_summary(
                            session_id,
                            manifest,
                            payload,
                            verification_error=error,
                            verification_current=True,
                        )
                        self._catalog_db.remember(
                            admission.volume_id,
                            directory,
                            self._commit,
                            summary,
                            manifest,
                            expected_signature=signature,
                            root=root,
                        )
                        self._session_summaries[session_id] = summary
                        self._session_diagnostics.pop(session_id, None)
                        self._session_snapshots[session_id] = snapshot
                        if error is None:
                            self._verified[session_id] = snapshot.manifest_sha256
                    return
                except (OSError, DeviceRecordingError, KeyError, TypeError, ValueError) as error:
                    if (
                        isinstance(error, DeviceRecordingError)
                        and error.code == "verification_changed"
                    ):
                        raise
                    with self._catalog_lock:
                        self.invalidate_session_verification(session_id)
                        self._session_summaries.pop(session_id, None)
                        self._session_diagnostics[session_id] = self.session_diagnostic(
                            session_id, error
                        )
                    return
        except DeviceRecordingError as error:
            if error.code != "verification_changed":
                raise
            raise ProviderError(
                "verification_cancelled", "录像校验已中止，请稍后重试", status=503, retryable=True
            ) from error
        finally:
            if registered:
                with stop_condition:
                    self._catalog_verification_session = None
                    stop_condition.notify_all()
            self._catalog_verification_lock.release()

    def catalog_sessions(
        self,
        admission: VolumeAdmission,
        *,
        retained_ids: set[str],
        check_manifest_current: Callable[[str, _VerifiedSessionSnapshot], bool],
        verify_session_fn: Callable[..., None],
        verify_session: str | None = None,
        cancelled: Callable[[], bool] | None = None,
    ) -> None:
        if verify_session is not None and (
            verify_session in {"", ".", ".."}
            or "\x00" in verify_session
            or Path(verify_session).name != verify_session
        ):
            return
        if verify_session is not None:
            verify_session_fn(verify_session, cancelled=cancelled)
            return
        with self._catalog_lock:
            discovered: set[str] = set()
            seen_per_root: dict[Path, set[str]] = {}
            pending_paths: list[tuple[str, Path, Path]] = []
            pending_remembers: list[
                tuple[Path, str, dict[str, object], Mapping[str, object], str, Path]
            ] = []

            for sessions_root in admission.catalog_roots:
                root_seen = seen_per_root.setdefault(sessions_root, set())
                for directory, session_id, from_path_cache in self._iter_catalog_entries(
                    sessions_root, admission.volume_id
                ):
                    relative = directory.relative_to(sessions_root)
                    if session_id in discovered:
                        if not from_path_cache:
                            continue
                        actual_id = self._disk_manifest_identity(directory)
                        if actual_id in discovered:
                            continue
                        session_id = actual_id
                        from_path_cache = False

                    summary = self._session_summaries.get(session_id)
                    snapshot = self._session_snapshots.get(session_id)
                    if summary is not None and snapshot is not None:
                        if check_manifest_current(session_id, snapshot):
                            self._recording_paths.setdefault(sessions_root, {})[session_id] = (
                                relative
                            )
                            discovered.add(session_id)
                            root_seen.add(session_id)
                            continue
                        self.invalidate_session_verification(session_id)
                        summary = None

                    try:
                        signature = self._catalog_db.signature(directory)
                        cached = self._catalog_db.cached(
                            admission.volume_id,
                            session_id,
                            signature,
                            self._commit,
                            directory=directory,
                            validate_summary=self._validate_summary_fn,
                            cache_validation=True,
                            required_artifact_role=(
                                RECOVERY_ROLE
                                if session_id in retained_ids and session_id not in self._verified
                                else None
                            ),
                        )
                    except OSError:
                        cached = None
                    if cached is not None:
                        discovered.add(session_id)
                        root_seen.add(session_id)
                        self._recording_paths.setdefault(sessions_root, {})[session_id] = relative
                        pending_paths.append((session_id, sessions_root, relative))
                        if summary is None or snapshot is None:
                            self._session_summaries[session_id] = cached
                        self._session_diagnostics.pop(session_id, None)
                        continue

                    # If the path cache pointed at a directory whose manifest changed
                    # or failed validation, inspect the manifest once to resolve the real session.
                    del from_path_cache
                    resolved_id = session_id
                    discovered.add(resolved_id)
                    root_seen.add(resolved_id)
                    self._recording_paths.setdefault(sessions_root, {})[resolved_id] = relative
                    self._session_summaries.pop(resolved_id, None)
                    try:
                        inspected_signature = self._catalog_db.signature(directory)
                        manifest, payload = self._inspect_fn(directory)
                        new_id = str(manifest["session_id"])
                        if new_id != resolved_id:
                            discovered.discard(resolved_id)
                            root_seen.discard(resolved_id)
                            self._recording_paths.get(sessions_root, {}).pop(resolved_id, None)
                            if new_id in discovered:
                                continue
                            self._session_summaries.pop(new_id, None)
                            resolved_id = new_id
                            discovered.add(resolved_id)
                            root_seen.add(resolved_id)
                            self._recording_paths.setdefault(sessions_root, {})[resolved_id] = (
                                relative
                            )
                        recovered = any(
                            log.get("role") == RECOVERY_ROLE for log in manifest["logs"]
                        )
                        if (
                            resolved_id in retained_ids
                            and resolved_id not in self._verified
                            and not recovered
                        ):
                            self._session_diagnostics[resolved_id] = self.session_diagnostic(
                                resolved_id, "会话发布失败，未进入可下载 catalog"
                            )
                            continue
                        built_summary = self.session_summary(
                            resolved_id,
                            manifest,
                            payload,
                            verification_current=False,
                        )
                        self._session_summaries[resolved_id] = built_summary
                        self._session_diagnostics.pop(resolved_id, None)
                        self.invalidate_session_verification(resolved_id)
                        pending_remembers.append(
                            (
                                directory,
                                resolved_id,
                                built_summary,
                                manifest,
                                inspected_signature,
                                sessions_root,
                            )
                        )
                    except (
                        OSError,
                        DeviceRecordingError,
                        KeyError,
                        TypeError,
                        ValueError,
                    ) as error:
                        self._session_summaries.pop(resolved_id, None)
                        self._verified.pop(resolved_id, None)
                        self._session_snapshots.pop(resolved_id, None)
                        diagnostic = self.session_diagnostic(resolved_id, error)
                        current_diagnostic = self._session_diagnostics.get(resolved_id)
                        if (
                            current_diagnostic is not None
                            and current_diagnostic.get("code") == diagnostic["code"]
                        ):
                            diagnostic = current_diagnostic
                        self._session_diagnostics[resolved_id] = diagnostic

            for root in list(self._recording_paths):
                if root not in seen_per_root:
                    self._recording_paths.pop(root, None)
                else:
                    valid_ids = seen_per_root[root]
                    entries = self._recording_paths[root]
                    for sid in [sid for sid in entries if sid not in valid_ids]:
                        entries.pop(sid, None)
            for session_id in set(self._session_summaries) - discovered:
                self._session_summaries.pop(session_id, None)
                self._verified.pop(session_id, None)
                self._session_snapshots.pop(session_id, None)
            for session_id in set(self._session_diagnostics) - discovered:
                self._session_diagnostics.pop(session_id, None)

            with self._catalog_db.scan():
                for session_id, sessions_root, relative in pending_paths:
                    self._catalog_db.remember_path(
                        admission.volume_id, session_id, sessions_root, relative
                    )
                for (
                    directory,
                    resolved_id,
                    built_summary,
                    manifest,
                    inspected_signature,
                    sessions_root,
                ) in pending_remembers:
                    try:
                        self._catalog_db.remember(
                            admission.volume_id,
                            directory,
                            self._commit,
                            built_summary,
                            manifest,
                            expected_signature=inspected_signature,
                            root=sessions_root,
                        )
                    except (
                        OSError,
                        DeviceRecordingError,
                        KeyError,
                        TypeError,
                        ValueError,
                    ) as error:
                        self._session_summaries.pop(resolved_id, None)
                        self._verified.pop(resolved_id, None)
                        self._session_snapshots.pop(resolved_id, None)
                        self._session_diagnostics[resolved_id] = self.session_diagnostic(
                            resolved_id, error
                        )
                self._catalog_db.discard_missing(admission.volume_id, discovered)

    def remember_sealed_session(
        self,
        *,
        volume_id: str,
        published_path: Path,
        root: Path,
        session_id: str,
        manifest: Mapping[str, object],
        manifest_bytes: bytes,
        manifest_sha256: str | None = None,
    ) -> None:
        """Update in-memory and SQLite catalog for a newly sealed session."""
        with self._catalog_lock:
            self._session_summaries[session_id] = self.session_summary(
                session_id,
                manifest,
                manifest_bytes,
                verification_current=False,
            )
            try:
                self._catalog_db.remember(
                    volume_id,
                    published_path,
                    self._commit,
                    self._session_summaries[session_id],
                    manifest,
                    root=root,
                )
            except (OSError, sqlite3.Error, PersistenceError):
                operational_logger("recording").event(
                    "catalog_publish_deferred",
                    level="warning",
                    transaction_id=session_id,
                )
            self.invalidate_session_verification(session_id)
            if manifest_sha256 is not None:
                self._verified[session_id] = manifest_sha256
            self._session_diagnostics.pop(session_id, None)

    def list_sessions_page(
        self,
        *,
        cursor: str | None,
        limit: int,
        take_id: str | None,
        api_version: str = "v3",
    ) -> Mapping[str, object]:
        with self._catalog_lock:
            all_items = list(self._session_summaries.values())
            diagnostics = list(self._session_diagnostics.values())
            all_items.sort(
                key=lambda item: (str(item["started_at"]), str(item["session_id"])),
                reverse=True,
            )

            if api_version != "v4":
                items = [
                    item for item in all_items if take_id is None or item["take_id"] == take_id
                ]
                combined: list[tuple[str, dict[str, object]]] = [
                    (str(item["session_id"]), item) for item in items
                ]
                start = 0
                if cursor is not None:
                    positions = [index for index, (key, _) in enumerate(combined) if key == cursor]
                    if not positions:
                        raise ProviderError(
                            "invalid_cursor",
                            "会话游标无效",
                            status=HTTPStatus.BAD_REQUEST,
                        )
                    start = positions[0] + 1
                selected_items = [
                    copy.deepcopy(item) for _, item in combined[start : start + limit]
                ]
                for item in selected_items:
                    verification = item.get("verification")
                    if isinstance(verification, dict):
                        raw_diagnostics = verification.get("diagnostics")
                        if isinstance(raw_diagnostics, list):
                            verification["diagnostics"] = [
                                diagnostic["summary"]
                                for diagnostic in raw_diagnostics
                                if isinstance(diagnostic, Mapping)
                                and isinstance(diagnostic.get("summary"), str)
                            ]
                remaining = limit - len(selected_items)
                selected_diagnostics = copy.deepcopy(diagnostics[:remaining])
                consumed = start + len(selected_items)
                next_cursor = None
                if consumed < len(combined) and selected_items:
                    next_cursor = str(selected_items[-1]["session_id"])
                return {
                    "schema": "ylx.session-list.v2",
                    "items": selected_items,
                    "diagnostics": selected_diagnostics,
                    "next_cursor": next_cursor,
                }

            diagnostics.sort(key=lambda item: str(item["quarantine_id"]))
            catalog_revision = session_catalog_revision(all_items, diagnostics)
            items = [item for item in all_items if take_id is None or item["take_id"] == take_id]
            ordered: list[tuple[str, dict[str, object]]] = [
                *(("item", item) for item in items),
                *(("diagnostic", diagnostic) for diagnostic in diagnostics),
            ]
            start = 0
            if cursor is not None:
                decoded = decode_session_cursor(cursor)
                if decoded["take_id"] != take_id:
                    raise ProviderError(
                        "invalid_cursor",
                        "会话游标与当前筛选条件不匹配",
                        status=HTTPStatus.BAD_REQUEST,
                    )
                if decoded["catalog_revision"] != catalog_revision:
                    raise ProviderError(
                        "catalog_changed",
                        "会话目录已变化，请从第一页重新加载",
                        status=HTTPStatus.CONFLICT,
                        retryable=True,
                        details={"catalog_revision": catalog_revision},
                    )
                start = int(decoded["offset"])
                if start >= len(ordered):
                    raise ProviderError(
                        "invalid_cursor",
                        "会话游标位置无效",
                        status=HTTPStatus.BAD_REQUEST,
                    )

            selected = ordered[start : start + limit]
            selected_items = [copy.deepcopy(value) for kind, value in selected if kind == "item"]
            selected_diagnostics = [
                copy.deepcopy(value) for kind, value in selected if kind == "diagnostic"
            ]
            consumed = start + len(selected)
            next_cursor = (
                encode_session_cursor(
                    catalog_revision=catalog_revision,
                    take_id=take_id,
                    offset=consumed,
                )
                if consumed < len(ordered)
                else None
            )
            return {
                "schema": "ylx.session-list.v3",
                "catalog_revision": catalog_revision,
                "items": selected_items,
                "diagnostics": selected_diagnostics,
                "next_cursor": next_cursor,
            }

    def prune_after_deletion(
        self,
        volume_id: str,
        requested_ids: set[str],
        deleted_ids: list[str],
    ) -> None:
        """Evict catalog caches and cached paths under `_catalog_lock` after deletion."""
        with self._catalog_lock:
            for session_id in requested_ids:
                self._session_summaries.pop(session_id, None)
                self._session_snapshots.pop(session_id, None)
                self._verified.pop(session_id, None)
                self._session_diagnostics.pop(session_id, None)
            deleted_set = set(deleted_ids)
            for entries in self._recording_paths.values():
                for session_id in deleted_set:
                    entries.pop(session_id, None)
            with suppress(OSError, sqlite3.Error, PersistenceError):
                self._catalog_db.discard_sessions(volume_id, deleted_set)

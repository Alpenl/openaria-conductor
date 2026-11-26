"""Session listing, manifest download, artifact range download, deletion, and outcomes."""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping
from http import HTTPStatus
from typing import TYPE_CHECKING

from rp_ylx.api.controllers.types import CaptureCommand, ProviderError
from rp_ylx.api.downloads import ArtifactAccessError, LockedManifest
from rp_ylx.api.range import UnsatisfiableRange, parse_single_range
from rp_ylx.api.validators.commands import valid_delete_request
from rp_ylx.api.validators.events import (
    InvalidSourceEvent,
    validate_retained_unsuccessful_outcome,
    validate_session_list,
)
from rp_ylx.core.ids import valid_uuid7
from rp_ylx.manifest_limits import MAX_MANIFEST_BYTES

if TYPE_CHECKING:
    from rp_ylx.api.http_server import GatewayHandler

QUOTED_SHA256 = re.compile(r'^"([0-9a-f]{64})"$')


def list_sessions(
    handler: GatewayHandler, api_version: str, query: Mapping[str, list[str]]
) -> None:
    if handler._principal("listSessions") is None:
        return
    if set(query) - {"cursor", "limit", "take_id"} or any(
        len(values) != 1 for values in query.values()
    ):
        handler._problem(HTTPStatus.BAD_REQUEST, "invalid_request", "会话查询参数无效")
        return
    cursor = query.get("cursor", [None])[0]
    if cursor == "":
        handler._problem(HTTPStatus.BAD_REQUEST, "invalid_request", "cursor 不能为空")
        return
    limit_value = query.get("limit", ["50"])[0]
    if isinstance(limit_value, str) and limit_value.isascii() and limit_value.isdecimal():
        limit = int(limit_value)
    else:
        limit = 0
    if not 1 <= limit <= 200:
        handler._problem(HTTPStatus.BAD_REQUEST, "invalid_request", "limit 必须介于 1 和 200")
        return
    take_id = query.get("take_id", [None])[0]
    if take_id is not None and not valid_uuid7(take_id):
        handler._problem(HTTPStatus.BAD_REQUEST, "invalid_request", "take_id 必须是 UUIDv7")
        return
    try:
        result = handler.server.provider.list_sessions(
            cursor=cursor,
            limit=limit,
            take_id=take_id,
            api_version=api_version,
        )
        validate_session_list(
            result,
            limit=limit,
            take_id=take_id,
            api_version=api_version,
        )
    except InvalidSourceEvent:
        handler._invalid_source_state("daemon session list 无效")
        return
    except ProviderError as error:
        handler._problem(
            error.status,
            error.code,
            error.message,
            retryable=error.retryable,
            details=error.details,
        )
        return
    except Exception:
        handler._provider_failure()
        return
    handler._send_json(HTTPStatus.OK, result)


def _valid_delete_result(body: object) -> bool:
    if not isinstance(body, Mapping) or set(body) != {
        "schema",
        "deleted_session_ids",
        "failed_sessions",
    }:
        return False
    if body.get("schema") != "ylx.session-delete-result.v1":
        return False
    deleted = body.get("deleted_session_ids")
    failed = body.get("failed_sessions")
    if (
        not isinstance(deleted, list)
        or not all(valid_uuid7(item) for item in deleted)
        or len(set(deleted)) != len(deleted)
    ):
        return False
    if not isinstance(failed, list):
        return False
    for entry in failed:
        if (
            not isinstance(entry, Mapping)
            or set(entry) != {"session_id", "error"}
            or not valid_uuid7(entry.get("session_id"))
            or not isinstance(entry.get("error"), str)
            or not entry["error"]
        ):
            return False
    return True


def delete_sessions(handler: GatewayHandler) -> None:
    identity = handler._command_principal("deleteSessions")
    if identity is None:
        return
    principal, key = identity
    body = handler._read_json()
    if body is None:
        return
    if not valid_delete_request(body):
        handler._problem(HTTPStatus.BAD_REQUEST, "invalid_request", "删除请求无效")
        return
    canonical = json.dumps(body, sort_keys=True, separators=(",", ":")).encode()
    try:
        result = handler.server.provider.delete_sessions(
            CaptureCommand(principal.principal_id, key, body, canonical)
        )
    except ProviderError as error:
        handler._problem(
            error.status,
            error.code,
            error.message,
            retryable=error.retryable,
            details=error.details,
        )
        return
    except Exception:
        handler._provider_failure()
        return
    if result.status != HTTPStatus.OK or not _valid_delete_result(result.body):
        handler._invalid_source_state("daemon session delete 响应无效")
        return
    handler._audit("deleteSessions", None, "completed", principal)
    handler._send_json(
        result.status,
        result.body,
        headers={"Idempotency-Replayed": "true"} if result.replayed else None,
    )


def _admit_session_download(handler: GatewayHandler, *, head: bool = False) -> bool:
    if handler.server.begin_session_download(handler.connection):
        return True
    handler.close_connection = True
    headers = {"Retry-After": "1", "YLX-Error-Code": "download_capacity_exhausted"}
    if head:
        handler._send_empty(HTTPStatus.SERVICE_UNAVAILABLE, headers=headers)
    else:
        handler._problem(
            HTTPStatus.SERVICE_UNAVAILABLE,
            "download_capacity_exhausted",
            "录制下载并发已达上限，请稍后重试",
            retryable=True,
            headers=headers,
        )
    return False


def get_manifest(handler: GatewayHandler, api_version: str, session_id: str) -> None:
    if handler._principal("getSession", session_id) is None:
        return
    if not _admit_session_download(handler):
        return
    try:
        _send_manifest(handler, api_version, session_id)
    finally:
        handler.server.end_session_download(handler.connection)


def _send_manifest_headers(
    handler: GatewayHandler,
    *,
    content_type: str,
    size: int,
    etag: str,
    manifest_sha256: str,
) -> None:
    handler.send_response(HTTPStatus.OK)
    handler.send_header("Content-Type", content_type)
    handler.send_header("Content-Length", str(size))
    handler.send_header("ETag", etag)
    handler.send_header("YLX-Manifest-SHA256", manifest_sha256)
    handler.send_header("Cache-Control", "no-store")
    handler._send_common_tail_headers()
    handler.end_headers()


def _send_manifest(handler: GatewayHandler, api_version: str, session_id: str) -> None:
    try:
        locked = handler.server.provider.open_manifest(session_id, api_version)
        with locked as manifest:
            etag = getattr(manifest, "etag", None)
            content_type = getattr(manifest, "content_type", None)
            size = getattr(manifest, "size", None)
            iter_chunks = getattr(manifest, "iter_chunks", None)
            etag_match = QUOTED_SHA256.fullmatch(etag) if isinstance(etag, str) else None
            if (
                etag_match is None
                or content_type != "application/json"
                or type(size) is not int
                or not 0 < size <= MAX_MANIFEST_BYTES
                or not callable(iter_chunks)
            ):
                handler._invalid_source_state("daemon manifest representation 元数据无效")
                return
            declared_sha256 = etag_match.group(1)

            # LockedManifest already validated exact content and SHA-256 at open time;
            # stream directly without re-hashing or double-buffering chunks.
            if isinstance(manifest, LockedManifest) and manifest.manifest_sha256 == declared_sha256:
                _send_manifest_headers(
                    handler,
                    content_type=content_type,
                    size=size,
                    etag=etag,
                    manifest_sha256=declared_sha256,
                )
                handler.wfile.write(manifest.read())
                return

            chunks: list[bytes] = []
            digest = hashlib.sha256()
            received = 0
            try:
                for chunk in iter_chunks(0, size):
                    if not isinstance(chunk, (bytes, bytearray, memoryview)):
                        handler._invalid_source_state("daemon manifest representation 字节无效")
                        return
                    payload_chunk = bytes(chunk)
                    received += len(payload_chunk)
                    if received > size or received > MAX_MANIFEST_BYTES:
                        handler._invalid_source_state("daemon manifest representation 长度无效")
                        return
                    digest.update(payload_chunk)
                    chunks.append(payload_chunk)
            except Exception:
                handler._invalid_source_state("daemon manifest representation 读取失败")
                return
            if received != size or digest.hexdigest() != declared_sha256:
                handler._invalid_source_state("daemon manifest representation 摘要无效")
                return
            _send_manifest_headers(
                handler,
                content_type=content_type,
                size=size,
                etag=etag,
                manifest_sha256=declared_sha256,
            )
            for payload_chunk in chunks:
                handler.wfile.write(payload_chunk)
    except ArtifactAccessError as error:
        if getattr(handler, "_headers_sent", False):
            handler._log_midstream_abort(error)
            return
        handler._problem(HTTPStatus.NOT_FOUND, "not_found", "会话不存在或尚未封存")
    except ProviderError as error:
        if getattr(handler, "_headers_sent", False):
            handler._log_midstream_abort(error)
            return
        handler._problem(
            error.status,
            error.code,
            error.message,
            retryable=error.retryable,
            details=error.details,
        )
    except Exception as error:
        if getattr(handler, "_headers_sent", False):
            handler._log_midstream_abort(error)
            return
        handler._provider_failure()


def get_retained_outcome(handler: GatewayHandler, session_id: str) -> None:
    if handler._principal("getRetainedUnsuccessfulSessionOutcome", session_id) is None:
        return
    try:
        outcome = handler.server.provider.retained_unsuccessful_outcome(session_id)
    except ProviderError as error:
        handler._problem(
            error.status,
            error.code,
            error.message,
            retryable=error.retryable,
            details=error.details,
        )
        return
    except Exception:
        handler._provider_failure()
        return
    if outcome is None:
        handler._problem(HTTPStatus.NOT_FOUND, "not_found", "没有保留的失败终态")
        return
    try:
        validate_retained_unsuccessful_outcome(outcome, session_id=session_id)
    except InvalidSourceEvent:
        handler._invalid_source_state("daemon retained outcome 无效")
        return
    handler._send_json(HTTPStatus.OK, outcome)


def session_artifact(
    handler: GatewayHandler,
    api_version: str,
    session_id: str,
    artifact_id: str,
    *,
    head: bool,
) -> None:
    operation_id = "headSessionArtifact" if head else "getSessionArtifact"
    if handler._principal(operation_id, session_id) is None:
        return
    try:
        state = handler.server.provider.artifact_io_state()
    except ProviderError as error:
        if head:
            handler._send_empty(error.status)
        else:
            handler._problem(
                error.status,
                error.code,
                error.message,
                retryable=error.retryable,
                details=error.details,
            )
        return
    except Exception:
        if head:
            handler._send_empty(HTTPStatus.INTERNAL_SERVER_ERROR)
        else:
            handler._provider_failure()
        return

    if state is not None:
        headers = {
            "YLX-Error-Code": "capture_busy",
            "YLX-Wait-State": "idle",
            "Retry-After": "1",
        }
        if head:
            handler._send_empty(HTTPStatus.LOCKED, headers=headers)
        else:
            handler._problem(
                HTTPStatus.LOCKED,
                "capture_busy",
                "采集资源繁忙，请等待 idle",
                retryable=True,
                headers=headers,
                details={
                    "wait_for": "idle",
                    "retry_after_seconds": 1,
                    "current_state": state,
                },
            )
        return
    if not _admit_session_download(handler, head=head):
        return
    try:
        _send_session_artifact(handler, api_version, session_id, artifact_id, head=head)
    finally:
        handler.server.end_session_download(handler.connection)


def _send_session_artifact(
    handler: GatewayHandler,
    api_version: str,
    session_id: str,
    artifact_id: str,
    *,
    head: bool,
) -> None:
    try:
        cancellable = getattr(handler.server.provider, "open_cancellable_artifact", None)
        if callable(cancellable):
            locked = cancellable(
                session_id,
                artifact_id,
                api_version,
                cancelled=lambda: handler.server.session_download_cancelled(handler.connection),
            )
        else:
            locked = handler.server.provider.open_verified_artifact(
                session_id, artifact_id, api_version
            )
        with locked as representation:
            selected_range = None
            if not head:
                range_value, range_is_single = handler._single_header("Range")
                if_range, if_range_is_single = handler._single_header("If-Range")
                if not range_is_single:
                    handler._problem(
                        HTTPStatus.REQUESTED_RANGE_NOT_SATISFIABLE,
                        "range_not_satisfiable",
                        "Range 只能出现一次",
                        headers={"Content-Range": f"bytes */{representation.size}"},
                    )
                    return
                if range_value is not None and (
                    if_range_is_single and (if_range is None or if_range == representation.etag)
                ):
                    try:
                        selected_range = parse_single_range(range_value, representation.size)
                    except UnsatisfiableRange:
                        handler._problem(
                            HTTPStatus.REQUESTED_RANGE_NOT_SATISFIABLE,
                            "range_not_satisfiable",
                            "请求的字节范围无法满足",
                            headers={"Content-Range": f"bytes */{representation.size}"},
                        )
                        return
            if selected_range is None:
                handler._send_representation(representation, head=head)
            else:
                first, last = selected_range
                handler._send_representation(
                    representation,
                    status=HTTPStatus.PARTIAL_CONTENT,
                    offset=first,
                    length=last - first + 1,
                    content_range=f"bytes {first}-{last}/{representation.size}",
                )
    except ArtifactAccessError as error:
        if getattr(handler, "_headers_sent", False):
            handler._log_midstream_abort(error)
            return
        if error.code == "not_verified":
            if head:
                handler._send_empty(
                    HTTPStatus.CONFLICT,
                    headers={"YLX-Error-Code": "session_not_verified"},
                )
            else:
                handler._problem(
                    HTTPStatus.CONFLICT,
                    "session_not_verified",
                    "会话没有当前可用的验证结果",
                    headers={"YLX-Error-Code": "session_not_verified"},
                    details={"reason": error.reason},
                )
        elif head:
            handler._send_empty(HTTPStatus.NOT_FOUND)
        else:
            handler._problem(HTTPStatus.NOT_FOUND, "not_found", "artifact 不存在")
    except ProviderError as error:
        if getattr(handler, "_headers_sent", False):
            handler._log_midstream_abort(error)
            return
        headers = {"YLX-Error-Code": error.code} if error.code == "session_not_verified" else None
        if error.retryable and error.status == HTTPStatus.SERVICE_UNAVAILABLE:
            headers = {"YLX-Error-Code": error.code, "Retry-After": "1"}
        if head:
            handler._send_empty(error.status, headers=headers)
        else:
            handler._problem(
                error.status,
                error.code,
                error.message,
                retryable=error.retryable,
                headers=headers,
                details=error.details,
            )
    except (BrokenPipeError, ConnectionResetError):
        handler.close_connection = True
        return
    except Exception as error:
        if getattr(handler, "_headers_sent", False):
            handler._log_midstream_abort(error)
            return
        if head:
            handler._send_empty(HTTPStatus.INTERNAL_SERVER_ERROR)
        else:
            handler._provider_failure()

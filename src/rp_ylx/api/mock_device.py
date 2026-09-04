"""可控制状态和故障的 v0 模拟设备。"""

from __future__ import annotations

import base64
import copy
import json
import threading
import uuid
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

ERROR_STATUS = {
    "invalid_request": 400,
    "not_found": 404,
    "capture_busy": 409,
    "invalid_state": 409,
    "stale_revision": 409,
    "hardware_unavailable": 503,
    "storage_unavailable": 507,
    "preview_unavailable": 503,
    "preview_frame_expired": 410,
    "internal_error": 500,
}

_DEFAULT_LEFT_JPEG = base64.b64decode(
    "/9j/4AAQSkZJRgABAQAAAQABAAD/2wBDABALDA4MChAODQ4SERATGCgaGBYWGDEjJR0oOjM9"
    "PDkzODdASFxOQERXRTc4UG1RV19iZ2hnPk1xeXBkeFxlZ2P/wAALCAACAAIBAREA/8QAFAAB"
    "AAAAAAAAAAAAAAAAAAAABv/EABQQAQAAAAAAAAAAAAAAAAAAAAD/2gAIAQEAAD8APv/Z"
)
_DEFAULT_RIGHT_JPEG = base64.b64decode(
    "/9j/4AAQSkZJRgABAQAAAQABAAD/2wBDABALDA4MChAODQ4SERATGCgaGBYWGDEjJR0oOjM9"
    "PDkzODdASFxOQERXRTc4UG1RV19iZ2hnPk1xeXBkeFxlZ2P/wAALCAACAAIBAREA/8QAFAAB"
    "AAAAAAAAAAAAAAAAAAAABv/EABQQAQAAAAAAAAAAAAAAAAAAAAD/2gAIAQEAAD8AQP/Z"
)


def _utc_now() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


@dataclass(frozen=True, slots=True)
class CommandResult:
    status: int
    body: dict[str, Any]


@dataclass(frozen=True, slots=True)
class _PreviewPair:
    sequence: int
    capture_monotonic_ns: int
    left: bytes
    right: bytes


class ApiError(ValueError):
    def __init__(self, code: str, message: str, *, retryable: bool, revision: int = 0) -> None:
        if code not in ERROR_STATUS:
            raise ValueError(f"未知 API 错误码：{code}")
        self.code = code
        self.message = message
        self.retryable = retryable
        self.revision = revision
        super().__init__(message)

    @property
    def status(self) -> int:
        return ERROR_STATUS[self.code]

    def as_body(self) -> dict[str, Any]:
        return {
            "error": {"code": self.code, "message": self.message, "retryable": self.retryable},
            "revision": self.revision,
        }


class MockDevice:
    """线程安全的内存设备；只用于客户端开发和合同测试。"""

    def __init__(
        self,
        *,
        device_id: str = "rp-ylx-mock",
        label: str = "Open Aria 模拟设备",
        model: str = "mock",
        software_version: str = "0.1.0",
        free_bytes: int = 64 * 1024 * 1024 * 1024,
        session_id_factory: Callable[[], str] | None = None,
        preview_cache_capacity: int = 4,
    ) -> None:
        if preview_cache_capacity <= 0:
            raise ValueError("预览缓存容量必须大于零")
        self._lock = threading.RLock()
        self._condition = threading.Condition(self._lock)
        self._revision = 0
        self._device = {
            "id": device_id,
            "label": label,
            "model": model,
            "software_version": software_version,
        }
        self._recording: dict[str, Any] = {
            "state": "idle",
            "session_id": None,
            "started_at": None,
            "frames": 0,
            "imu_samples": 0,
        }
        self._free_bytes = free_bytes
        self._health = "ok"
        self._issues: list[dict[str, str]] = []
        self._sessions: list[dict[str, Any]] = []
        self._commands: dict[tuple[str, str], tuple[str, CommandResult]] = {}
        self._events: deque[tuple[int, str, dict[str, Any]]] = deque(maxlen=128)
        self._session_id_factory = session_id_factory or (lambda: str(uuid.uuid4()))
        self._preview_sequence = 0
        self._preview_has_source_frame = False
        self._preview_pairs: deque[_PreviewPair] = deque(maxlen=preview_cache_capacity)
        self._preview_pairs.append(
            _PreviewPair(0, 1_000_000_000, _DEFAULT_LEFT_JPEG, _DEFAULT_RIGHT_JPEG)
        )

    @property
    def revision(self) -> int:
        with self._lock:
            return self._revision

    def device(self) -> dict[str, Any]:
        with self._lock:
            return {"api_version": "v0", "device": copy.deepcopy(self._device)}

    def _status_locked(self) -> dict[str, Any]:
        return {
            "revision": self._revision,
            "observed_at": _utc_now(),
            "health": self._health,
            "recording": copy.deepcopy(self._recording),
            "storage": {"free_bytes": self._free_bytes},
            "issues": copy.deepcopy(self._issues),
        }

    def status(self) -> dict[str, Any]:
        with self._lock:
            return self._status_locked()

    def sessions(self) -> dict[str, Any]:
        with self._lock:
            return {"revision": self._revision, "sessions": copy.deepcopy(self._sessions)}

    def _publish_locked(self, event: str) -> None:
        self._revision += 1
        self._events.append((self._revision, event, self._status_locked()))
        self._condition.notify_all()

    def _validate_command(self, command: str, payload: object) -> tuple[str, int | None, str]:
        if not isinstance(payload, dict):
            raise ApiError(
                "invalid_request", "请求体必须是 JSON 对象", retryable=False, revision=self.revision
            )
        allowed = {"request_id", "expected_revision"}
        if command == "stop":
            allowed.add("reason")
        if set(payload) - allowed:
            raise ApiError(
                "invalid_request", "请求体含未知字段", retryable=False, revision=self.revision
            )
        request_id = payload.get("request_id")
        if not isinstance(request_id, str) or not request_id or len(request_id) > 128:
            raise ApiError(
                "invalid_request",
                "request_id 必须是 1 到 128 字符",
                retryable=False,
                revision=self.revision,
            )
        expected = payload.get("expected_revision")
        if expected is not None and (
            isinstance(expected, bool) or not isinstance(expected, int) or expected < 0
        ):
            raise ApiError(
                "invalid_request",
                "expected_revision 必须是非负整数",
                retryable=False,
                revision=self.revision,
            )
        reason = payload.get("reason", "user")
        if command == "stop" and reason not in {"user", "device_shutdown"}:
            raise ApiError(
                "invalid_request", "停止原因无效", retryable=False, revision=self.revision
            )
        return request_id, expected, json.dumps(payload, sort_keys=True, separators=(",", ":"))

    def _replay_locked(self, command: str, request_id: str, canonical: str) -> CommandResult | None:
        cached = self._commands.get((command, request_id))
        if cached is None:
            return None
        old_payload, result = cached
        if old_payload != canonical:
            raise ApiError(
                "invalid_request",
                "同一 request_id 不能用于不同请求体",
                retryable=False,
                revision=self._revision,
            )
        return copy.deepcopy(result)

    def start(self, payload: object) -> CommandResult:
        request_id, expected, canonical = self._validate_command("start", payload)
        with self._lock:
            replay = self._replay_locked("start", request_id, canonical)
            if replay is not None:
                return replay
            if expected is not None and expected != self._revision:
                raise ApiError(
                    "stale_revision", "设备状态版本已变化", retryable=True, revision=self._revision
                )
            if any(issue["code"] == "hardware_unavailable" for issue in self._issues):
                raise ApiError(
                    "hardware_unavailable",
                    "采集硬件不可用",
                    retryable=True,
                    revision=self._revision,
                )
            if any(issue["code"] == "storage_unavailable" for issue in self._issues):
                raise ApiError(
                    "storage_unavailable", "录制存储不可用", retryable=True, revision=self._revision
                )
            if self._recording["state"] != "idle":
                raise ApiError(
                    "capture_busy", "设备已有录制任务", retryable=True, revision=self._revision
                )
            self._recording = {
                "state": "starting",
                "session_id": self._session_id_factory(),
                "started_at": _utc_now(),
                "frames": 0,
                "imu_samples": 0,
            }
            self._publish_locked("status")
            result = CommandResult(
                202,
                {
                    "request_id": request_id,
                    "accepted": True,
                    "revision": self._revision,
                    "recording": copy.deepcopy(self._recording),
                },
            )
            self._commands[("start", request_id)] = (canonical, result)
            return copy.deepcopy(result)

    def complete_start(self) -> None:
        with self._lock:
            if self._recording["state"] == "starting":
                self._recording["state"] = "recording"
                self._publish_locked("status")

    def stop(self, payload: object) -> CommandResult:
        request_id, expected, canonical = self._validate_command("stop", payload)
        with self._lock:
            replay = self._replay_locked("stop", request_id, canonical)
            if replay is not None:
                return replay
            if expected is not None and expected != self._revision:
                raise ApiError(
                    "stale_revision", "设备状态版本已变化", retryable=True, revision=self._revision
                )
            if self._recording["state"] not in {"starting", "recording"}:
                raise ApiError(
                    "invalid_state",
                    "当前没有可停止的录制",
                    retryable=False,
                    revision=self._revision,
                )
            self._recording["state"] = "stopping"
            self._publish_locked("status")
            result = CommandResult(
                202,
                {
                    "request_id": request_id,
                    "accepted": True,
                    "revision": self._revision,
                    "recording": copy.deepcopy(self._recording),
                },
            )
            self._commands[("stop", request_id)] = (canonical, result)
            return copy.deepcopy(result)

    def complete_stop(self) -> None:
        with self._lock:
            if self._recording["state"] != "stopping":
                return
            self._sessions.append(
                {
                    "session_id": self._recording["session_id"],
                    "state": "sealed",
                    "started_at": self._recording["started_at"],
                    "ended_at": _utc_now(),
                    "bytes": 0,
                }
            )
            self._recording = {
                "state": "idle",
                "session_id": None,
                "started_at": None,
                "frames": 0,
                "imu_samples": 0,
            }
            self._publish_locked("status")

    def set_fault(self, code: str, message: str) -> None:
        if code not in {"hardware_unavailable", "storage_unavailable", "preview_unavailable"}:
            raise ValueError("模拟故障码无效")
        with self._lock:
            self._issues = [issue for issue in self._issues if issue["code"] != code]
            self._issues.append({"code": code, "message": message})
            self._health = "error" if code != "preview_unavailable" else "degraded"
            if code != "preview_unavailable" and self._recording["state"] in {
                "starting",
                "recording",
                "stopping",
            }:
                self._recording["state"] = "failed"
            self._publish_locked("diagnostic")

    def clear_faults(self) -> None:
        with self._lock:
            self._issues = []
            self._health = "ok"
            self._publish_locked("diagnostic")

    def publish_preview_pair(
        self,
        left: bytes,
        right: bytes,
        *,
        source_sequence: int | None = None,
        capture_monotonic_ns: int | None = None,
    ) -> int:
        if not isinstance(left, bytes) or not left or not isinstance(right, bytes) or not right:
            raise ValueError("双目预览必须包含非空 bytes")
        with self._lock:
            previous_time = self._preview_pairs[-1].capture_monotonic_ns
            first_source_frame = not self._preview_has_source_frame
            if source_sequence is not None and (
                isinstance(source_sequence, bool)
                or not isinstance(source_sequence, int)
                or source_sequence < 0
            ):
                raise ValueError("双目预览源序列必须是非负整数")
            if source_sequence is None:
                selected_sequence = self._preview_sequence + 1
            else:
                selected_sequence = source_sequence
                if not first_source_frame and selected_sequence <= self._preview_sequence:
                    raise ValueError("双目预览源序列必须严格递增")
            if capture_monotonic_ns is None:
                selected_time = previous_time + 1
            else:
                selected_time = capture_monotonic_ns
            time_invalid = (
                isinstance(selected_time, bool)
                or not isinstance(selected_time, int)
                or selected_time <= previous_time
            )
            if time_invalid and not (
                first_source_frame
                and source_sequence is not None
                and not isinstance(selected_time, bool)
                and isinstance(selected_time, int)
                and selected_time >= 0
            ):
                raise ValueError("双目预览采集时间必须严格递增")
            self._preview_sequence = selected_sequence
            pair = _PreviewPair(selected_sequence, selected_time, left, right)
            if first_source_frame and source_sequence is not None:
                # Replace the built-in placeholder as soon as a real source
                # frame arrives, even when its sequence does not start at zero.
                self._preview_pairs[-1] = pair
            else:
                self._preview_pairs.append(pair)
            self._preview_has_source_frame = True
            return selected_sequence

    def preview(self, eye: str, *, sequence: int | None = None) -> tuple[bytes, int, int]:
        with self._lock:
            if eye not in {"left", "right"}:
                raise ApiError(
                    "invalid_request",
                    "eye 必须是 left 或 right",
                    retryable=False,
                    revision=self._revision,
                )
            if any(issue["code"] == "preview_unavailable" for issue in self._issues):
                raise ApiError(
                    "preview_unavailable", "预览暂不可用", retryable=True, revision=self._revision
                )
            if sequence is not None and (
                isinstance(sequence, bool) or not isinstance(sequence, int) or sequence < 0
            ):
                raise ApiError(
                    "invalid_request",
                    "sequence 必须是非负整数",
                    retryable=False,
                    revision=self._revision,
                )
            pair = self._preview_pairs[-1]
            if sequence is not None:
                pair = next(
                    (
                        candidate
                        for candidate in reversed(self._preview_pairs)
                        if candidate.sequence == sequence
                    ),
                    None,
                )
                if pair is None:
                    raise ApiError(
                        "preview_frame_expired",
                        "请求的双目预览帧已不在缓存中，请重新读取最新帧",
                        retryable=True,
                        revision=self._revision,
                    )
            payload = pair.left if eye == "left" else pair.right
            return payload, pair.sequence, pair.capture_monotonic_ns

    def events_after(self, revision: int) -> list[tuple[int, str, dict[str, Any]]]:
        with self._lock:
            return copy.deepcopy([event for event in self._events if event[0] > revision])

    def wait_events(self, revision: int, timeout: float) -> list[tuple[int, str, dict[str, Any]]]:
        with self._condition:
            events = [event for event in self._events if event[0] > revision]
            if not events:
                self._condition.wait(timeout)
                events = [event for event in self._events if event[0] > revision]
            return copy.deepcopy(events)

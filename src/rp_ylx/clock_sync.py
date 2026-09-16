"""Initialize an offline device's wall clock from an authenticated, connected client.

Only the privileged controller calls clock_settime. Challenges expire on the
monotonic clock, and one accepted client initializes the clock until it changes
externally or the controller restarts. NTP always takes precedence.
"""

from __future__ import annotations

import re
import secrets
import subprocess
import threading
import time
from collections.abc import Mapping

from rp_ylx.operational_logging import operational_logger

MIN_CLIENT_UNIX_MS = 1704067200000  # 2024-01-01 UTC
MAX_CLIENT_UNIX_MS = 4102444800000  # 2100-01-01 UTC (exclusive)
CHALLENGE_TTL_MS = 5000
_CHALLENGE = re.compile(r"^[0-9a-f]{32}$")
_LOG = operational_logger("clock-sync")


class ClockSyncError(RuntimeError):
    def __init__(self, code: str, message: str) -> None:
        self.code = code
        super().__init__(message)


def valid_clock_request(body: object) -> bool:
    return (
        isinstance(body, Mapping)
        and set(body) == {"schema", "challenge", "unix_time_ms"}
        and body.get("schema") == "ylx.clock-sync-request.v1"
        and isinstance(body.get("challenge"), str)
        and _CHALLENGE.fullmatch(body["challenge"]) is not None
        and type(body.get("unix_time_ms")) is int
        and MIN_CLIENT_UNIX_MS <= body["unix_time_ms"] < MAX_CLIENT_UNIX_MS
    )


def valid_clock_response(body: object) -> bool:
    if not isinstance(body, Mapping) or set(body) != {
        "schema",
        "source",
        "unix_time_ms",
        "challenge",
        "expires_in_ms",
        "applied",
    }:
        return False
    challenge = body.get("challenge")
    return (
        body.get("schema") == "ylx.clock-status.v1"
        and isinstance(body.get("source"), str)
        and body.get("source") in {"ntp", "client", "unsynchronized"}
        and type(body.get("unix_time_ms")) is int
        and body["unix_time_ms"] >= 0
        and type(body.get("applied")) is bool
        and (not body["applied"] or body["source"] == "client")
        and type(body.get("expires_in_ms")) is int
        and (
            isinstance(challenge, str)
            and _CHALLENGE.fullmatch(challenge) is not None
            and body["source"] == "unsynchronized"
            and body["expires_in_ms"] == CHALLENGE_TTL_MS
            or challenge is None
            and body["source"] in {"ntp", "client"}
            and body["expires_in_ms"] == 0
        )
    )


def ntp_synchronized() -> bool:
    try:
        result = subprocess.run(
            ["timedatectl", "show", "--property=NTPSynchronized", "--value"],
            check=True,
            capture_output=True,
            text=True,
            timeout=1.0,
        )
    except (OSError, subprocess.SubprocessError) as error:
        raise ClockSyncError("clock_status_unavailable", "暂时无法读取网络校时状态") from error
    value = result.stdout.strip()
    if value not in {"yes", "no"}:
        raise ClockSyncError("clock_status_unavailable", "网络校时状态无效")
    return value == "yes"


class ClockSynchronizer:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._challenges: dict[str, tuple[str, int, int]] = {}
        self._client_reference: tuple[int, int] | None = None

    def _source(self) -> str:
        if ntp_synchronized():
            return "ntp"
        if self._client_reference is not None:
            mono, wall = self._client_reference
            if abs(time.time_ns() - wall - (time.monotonic_ns() - mono)) < 2_000_000_000:
                return "client"
            self._client_reference = None
        return "unsynchronized"

    @staticmethod
    def _response(source: str, *, challenge: str | None = None, applied: bool = False) -> dict:
        return {
            "schema": "ylx.clock-status.v1",
            "source": source,
            "unix_time_ms": time.time_ns() // 1_000_000,
            "challenge": challenge,
            "expires_in_ms": CHALLENGE_TTL_MS if challenge else 0,
            "applied": applied,
        }

    def status(self, principal_id: str) -> dict:
        with self._lock:
            source = self._source()
            if source != "unsynchronized":
                return self._response(source)
            now = time.monotonic_ns()
            self._challenges = {
                key: value
                for key, value in self._challenges.items()
                if 0 <= now - value[1] < CHALLENGE_TTL_MS * 1_000_000
            }
            if len(self._challenges) >= 64:
                del self._challenges[next(iter(self._challenges))]
            challenge = secrets.token_hex(16)
            self._challenges[challenge] = (principal_id, now, time.time_ns())
            return self._response(source, challenge=challenge)

    def sync(self, principal_id: str, body: Mapping[str, object]) -> dict:
        if not valid_clock_request(body):
            raise ClockSyncError("invalid_request", "客户端日期或校时请求无效")
        with self._lock:
            received = time.monotonic_ns()
            reference = self._challenges.pop(str(body["challenge"]), None)
            if reference is None or reference[0] != principal_id:
                raise ClockSyncError(
                    "clock_challenge_expired", "校时请求已失效，请重新取得设备时间"
                )
            _, issued, original_wall = reference
            source = self._source()
            # Another client or NTP may have initialized the clock since GET.
            if source != "unsynchronized":
                return self._response(source)
            now = time.monotonic_ns()
            if not 0 <= now - issued < CHALLENGE_TTL_MS * 1_000_000:
                raise ClockSyncError("clock_challenge_expired", "校时请求已过期，请重试")
            if abs(time.time_ns() - original_wall - (now - issued)) > 1_000_000_000:
                raise ClockSyncError("clock_changed", "设备时间已改变，请重新校时")
            # Account for local processing, without guessing the one-way network delay.
            target_ns = int(body["unix_time_ms"]) * 1_000_000 + now - received
            applied = abs(time.time_ns() - target_ns) > 1_000_000_000
            if applied:
                try:
                    time.clock_settime_ns(time.CLOCK_REALTIME, target_ns)
                except OSError as error:
                    raise ClockSyncError("clock_set_failed", "设备系统日期校准失败") from error
            self._client_reference = (time.monotonic_ns(), time.time_ns())
            self._challenges.clear()
            _LOG.event("client_clock_synchronized", outcome="applied" if applied else "matched")
            return self._response("client", applied=applied)

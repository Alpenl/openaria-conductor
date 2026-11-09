"""Initialize an offline device's wall clock from an authenticated, connected client.

Only the privileged controller calls clock_settime. Challenges expire on the
monotonic clock, and one accepted client initializes the clock until it changes
externally or the device reboots. NTP always takes precedence.
"""

from __future__ import annotations

import json
import os
import re
import secrets
import subprocess
import threading
import time
import uuid
from collections.abc import Mapping
from pathlib import Path

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
        and (
            set(body) == {"schema", "challenge", "unix_time_ms"}
            and body.get("schema") == "ylx.clock-sync-request.v1"
            or set(body) == {"schema", "challenge", "unix_time_ms", "replace"}
            and body.get("schema") == "ylx.clock-sync-request.v2"
            and type(body.get("replace")) is bool
        )
        and isinstance(body.get("challenge"), str)
        and _CHALLENGE.fullmatch(body["challenge"]) is not None
        and type(body.get("unix_time_ms")) is int
        and MIN_CLIENT_UNIX_MS <= body["unix_time_ms"] < MAX_CLIENT_UNIX_MS
    )


def valid_clock_response(body: object) -> bool:
    if isinstance(body, Mapping) and body.get("schema") == "ylx.clock-status.v2":
        return (
            set(body)
            == {
                "schema",
                "source",
                "unix_time_ms",
                "challenge",
                "expires_in_ms",
                "applied",
                "boot_id",
                "synchronized_at_ms",
                "age_seconds",
            }
            and isinstance(body.get("source"), str)
            and body.get("source") in {"ntp", "client", "unsynchronized"}
            and type(body.get("unix_time_ms")) is int
            and body["unix_time_ms"] >= 0
            and type(body.get("applied")) is bool
            and (not body["applied"] or body["source"] == "client")
            and isinstance(body.get("challenge"), str)
            and _CHALLENGE.fullmatch(body["challenge"]) is not None
            and body.get("expires_in_ms") == CHALLENGE_TTL_MS
            and isinstance(body.get("boot_id"), str)
            and (
                body["source"] == "unsynchronized"
                and body.get("synchronized_at_ms") is None
                and body.get("age_seconds") is None
                or body["source"] != "unsynchronized"
                and type(body.get("synchronized_at_ms")) is int
                and MIN_CLIENT_UNIX_MS <= body["synchronized_at_ms"] < MAX_CLIENT_UNIX_MS
                and type(body.get("age_seconds")) is int
                and body["age_seconds"] >= 0
            )
        )
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
            ["timedatectl", "show", "--property=NTP", "--property=NTPSynchronized"],
            check=True,
            capture_output=True,
            text=True,
            timeout=1.0,
        )
    except (OSError, subprocess.SubprocessError) as error:
        raise ClockSyncError("clock_status_unavailable", "暂时无法读取网络校时状态") from error
    values = dict(line.split("=", 1) for line in result.stdout.splitlines() if "=" in line)
    if set(values) != {"NTP", "NTPSynchronized"} or any(
        value not in {"yes", "no"} for value in values.values()
    ):
        raise ClockSyncError("clock_status_unavailable", "网络校时状态无效")
    # The kernel can retain a synchronized flag after the NTP service is disabled.
    # Only an enabled network time service may establish a new NTP reference.
    return values["NTP"] == "yes" and values["NTPSynchronized"] == "yes"


class ClockSynchronizer:
    def __init__(self, state_path: Path | None = None, *, boot_id: str | None = None) -> None:
        self._lock = threading.Lock()
        self._challenges: dict[str, tuple] = {}
        self._client_reference: tuple[int, int] | None = None
        self._reference_source = "client"
        self._revision = uuid.uuid4().hex
        self._state_path = state_path
        self._boot_id = boot_id or Path("/proc/sys/kernel/random/boot_id").read_text().strip()
        if state_path is not None:
            try:
                value = json.loads(state_path.read_text())
                mono, wall = value["monotonic_ns"], value["unix_ns"]
                if (
                    value["boot_id"] == self._boot_id
                    and value["source"] in {"client", "ntp"}
                    and type(mono) is int
                    and 0 <= mono <= time.monotonic_ns()
                    and type(wall) is int
                    and MIN_CLIENT_UNIX_MS * 1_000_000 <= wall < MAX_CLIENT_UNIX_MS * 1_000_000
                ):
                    self._client_reference = (mono, wall)
                    self._reference_source = value["source"]
            except (OSError, ValueError, KeyError, TypeError):
                pass  # Missing/corrupt/previous-boot state never authorizes recording.

    def _remember(self, source: str) -> None:
        mono, wall = time.monotonic_ns(), time.time_ns()
        if not MIN_CLIENT_UNIX_MS * 1_000_000 <= wall < MAX_CLIENT_UNIX_MS * 1_000_000:
            raise ClockSyncError("clock_verify_failed", "设备日期回读异常，请重试")
        if self._state_path is not None:
            temp = self._state_path.with_suffix(".tmp")
            try:
                # Parent is the controller's existing private state directory.
                with temp.open("w", encoding="utf-8") as stream:
                    os.chmod(temp, 0o600)
                    json.dump(
                        {
                            "boot_id": self._boot_id,
                            "monotonic_ns": mono,
                            "unix_ns": wall,
                            "source": source,
                        },
                        stream,
                    )
                    stream.flush()
                    os.fsync(stream.fileno())
                os.replace(temp, self._state_path)
                fd = os.open(self._state_path.parent, os.O_RDONLY | os.O_DIRECTORY)
                try:
                    os.fsync(fd)
                finally:
                    os.close(fd)
            except OSError as error:
                self._client_reference = None
                raise ClockSyncError("clock_save_failed", "校时记录保存失败，请重试") from error
        self._client_reference = (mono, wall)
        self._reference_source = source
        self._revision = uuid.uuid4().hex

    def _source(self) -> str:
        if self._client_reference is not None:
            mono, wall = self._client_reference
            if (
                time.monotonic_ns() < mono
                or abs(time.time_ns() - wall - (time.monotonic_ns() - mono)) >= 2_000_000_000
            ):
                self._client_reference = None
                self._revision = uuid.uuid4().hex
        if ntp_synchronized():
            if self._client_reference is None or self._reference_source != "ntp":
                self._remember("ntp")
            return "ntp"
        if self._client_reference is not None:
            return self._reference_source
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

    def _status(
        self, principal_id: str, source: str, *, extended: bool, applied: bool = False
    ) -> dict:
        if source != "unsynchronized" and not extended:
            return self._response(source, applied=applied)
        now = time.monotonic_ns()
        self._challenges = {
            key: value
            for key, value in self._challenges.items()
            if 0 <= now - value[1] < CHALLENGE_TTL_MS * 1_000_000
        }
        if len(self._challenges) >= 64:
            del self._challenges[next(iter(self._challenges))]
        challenge = secrets.token_hex(16)
        self._challenges[challenge] = (principal_id, now, time.time_ns(), self._revision, extended)
        result = self._response(source, challenge=challenge, applied=applied)
        if extended:
            reference = self._client_reference
            result.update(
                schema="ylx.clock-status.v2",
                boot_id=self._boot_id,
                synchronized_at_ms=None if reference is None else reference[1] // 1_000_000,
                age_seconds=None if reference is None else (now - reference[0]) // 1_000_000_000,
            )
        return result

    def status(self, principal_id: str, *, extended: bool = False) -> dict:
        with self._lock:
            return self._status(principal_id, self._source(), extended=extended)

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
            _, issued, original_wall, revision, extended = reference
            if body["schema"] == "ylx.clock-sync-request.v2" and not extended:
                raise ClockSyncError("clock_challenge_expired", "请重新取得设备时间")
            source = self._source()
            # Another client or NTP may have initialized the clock since GET.
            replace = body.get("replace") is True
            if source != "unsynchronized" and not replace:
                return self._status(principal_id, source, extended=extended)
            if replace and ntp_synchronized():
                raise ClockSyncError("clock_ntp_active", "设备正在使用网络校时，不能用手机覆盖")
            if replace and revision != self._revision:
                raise ClockSyncError("clock_changed", "设备时间已改变，请重新确认")
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
            if abs(time.time_ns() - target_ns - (time.monotonic_ns() - now)) > 1_000_000_000:
                self._client_reference = None
                raise ClockSyncError("clock_verify_failed", "设备日期回读不一致，请重试")
            self._remember("client")
            self._challenges.clear()
            _LOG.event("client_clock_synchronized", outcome="applied" if applied else "matched")
            return self._status(principal_id, "client", extended=extended, applied=applied)

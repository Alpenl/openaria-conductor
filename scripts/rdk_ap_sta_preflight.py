#!/usr/bin/env python3
"""Collect read-only AP+STA preflight evidence on an RDK X5.

The harness is intentionally inventory-only. It samples Device API GET
endpoints, NetworkManager/iw/systemd state, mDNS visibility, route/link state,
and rescue file presence without applying desired network state, restarting
services, changing interfaces, or enabling the root network-control socket.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import platform
import re
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

UTC = timezone.utc  # noqa: UP017 - RDK Ubuntu 22.04 system Python is 3.10.

SCHEMA = "ylx.acceptance.ap-sta-preflight.v1"
DEFAULT_BASE_URL = "http://127.0.0.1:8080"
DEFAULT_SERVICE_NAME = "rp-ylx"
DEFAULT_OUTPUT_DIR = Path("/tmp")
MAX_CAPTURED_TEXT_BYTES = 80_000
MAX_HTTP_BYTES = 32_768

SECRET_KEY_MARKERS = ("password", "passwd", "psk", "secret", "token", "credential", "key")
IDENTIFIER_KEY_MARKERS = (
    "ssid",
    "bssid",
    "peer_or_ssid",
    "profile",
    "connection",
    "connection_id",
    "uuid",
)
MAC_RE = re.compile(r"\b[0-9a-fA-F]{2}(?::[0-9a-fA-F]{2}){5}\b")
BEARER_RE = re.compile(r"Bearer\s+[A-Za-z0-9._~+/=-]+", re.IGNORECASE)
SECRET_LINE_RE = re.compile(
    r"(?im)^([^\n]*(?:password|passwd|psk|secret|token|credential)[^\n:=]*[:=]).*$"
)

FORBIDDEN_COMMAND_PATTERNS = (
    "nmcli connection up/down/add/modify/delete",
    "nmcli device disconnect/connect",
    "iw ... interface add/del",
    "ip link set/add/del",
    "rfkill block/unblock",
    "modprobe/rmmod",
    "systemctl restart/stop/start/enable/disable/reboot",
    "reboot/shutdown/poweroff",
    "rp-ylx network apply/rescue/reconcile",
    "HTTP POST /api/v4/network/apply",
    "HTTP POST /api/v4/network/retry",
    "HTTP POST /api/v4/network/forget",
)


@dataclass(frozen=True)
class CommandResult:
    label: str
    args: list[str]
    returncode: int | None
    stdout: str
    stderr: str
    timeout_s: float
    elapsed_s: float
    error: str | None = None


@dataclass(frozen=True)
class HttpProbe:
    label: str
    method: str
    path: str
    status: int | None
    headers: dict[str, str]
    payload: Any
    body_text: str | None
    latency_s: float
    error: str | None = None


class ReadOnlyRunner:
    def __init__(self) -> None:
        self.commands_run: list[dict[str, Any]] = []
        self.refused_commands: list[dict[str, Any]] = []

    def run(self, label: str, args: list[str], *, timeout: float = 5.0) -> CommandResult:
        if not command_is_allowed(args):
            refused = {"label": label, "args": args, "reason": "not in read-only allowlist"}
            self.refused_commands.append(refused)
            raise ValueError(f"refusing non-read-only command: {args!r}")

        started = time.monotonic()
        try:
            completed = subprocess.run(
                args,
                check=False,
                capture_output=True,
                text=True,
                timeout=timeout,
            )
            result = CommandResult(
                label=label,
                args=args,
                returncode=completed.returncode,
                stdout=redact_text(truncate_text(completed.stdout), label=label),
                stderr=redact_text(truncate_text(completed.stderr), label=label),
                timeout_s=timeout,
                elapsed_s=time.monotonic() - started,
            )
        except subprocess.TimeoutExpired as exc:
            result = CommandResult(
                label=label,
                args=args,
                returncode=None,
                stdout=redact_text(truncate_text(exc.stdout or ""), label=label),
                stderr=redact_text(truncate_text(exc.stderr or ""), label=label),
                timeout_s=timeout,
                elapsed_s=time.monotonic() - started,
                error=f"timeout after {timeout:.1f}s",
            )
        except OSError as exc:
            result = CommandResult(
                label=label,
                args=args,
                returncode=None,
                stdout="",
                stderr="",
                timeout_s=timeout,
                elapsed_s=time.monotonic() - started,
                error=f"{type(exc).__name__}: {exc}",
            )

        self.commands_run.append(command_result_to_json(result))
        return result


def _utc_now() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def _utc_stamp() -> str:
    return datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")


def _stable_hash(value: str) -> str:
    digest = hashlib.sha256(value.encode("utf-8", "replace")).hexdigest()
    return f"sha256:{digest[:16]}"


def _script_sha256() -> str | None:
    try:
        return hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    except OSError:
        return None


def truncate_text(value: str, *, max_bytes: int = MAX_CAPTURED_TEXT_BYTES) -> str:
    payload = value.encode("utf-8", "replace")
    if len(payload) <= max_bytes:
        return value
    truncated = payload[:max_bytes].decode("utf-8", "replace")
    return f"{truncated}\n[TRUNCATED after {max_bytes} bytes]"


def redact_text(value: str, *, label: str | None = None) -> str:
    redacted = BEARER_RE.sub("Bearer [REDACTED]", value)
    redacted = MAC_RE.sub(lambda match: _stable_hash(match.group(0)), redacted)
    redacted = SECRET_LINE_RE.sub(r"\1[REDACTED]", redacted)
    if label and label.startswith("nmcli-"):
        redacted = _redact_nmcli_text(label, redacted)
    return redacted


def _redact_nmcli_text(label: str, value: str) -> str:
    if label == "nmcli-device-status":
        return "\n".join(_hash_colon_fields(line, {3}) for line in value.splitlines())
    if label == "nmcli-active-connections":
        return "\n".join(_hash_colon_fields(line, {0, 1}) for line in value.splitlines())
    if label.startswith("nmcli-device-show-"):
        output: list[str] = []
        for line in value.splitlines():
            key, separator, field = line.partition(":")
            if separator and key in {"GENERAL.CONNECTION", "GENERAL.HWADDR"} and field:
                output.append(f"{key}:{_stable_hash(field)}")
            else:
                output.append(line)
        return "\n".join(output)
    return value


def _hash_colon_fields(line: str, indexes: set[int]) -> str:
    if not line:
        return line
    fields = line.split(":")
    for index in indexes:
        if index < len(fields) and fields[index] and fields[index] != "--":
            fields[index] = _stable_hash(fields[index])
    return ":".join(fields)


def redact_value(value: Any, *, key: str | None = None) -> Any:
    if isinstance(value, dict):
        return {
            str(item_key): redact_value(item_value, key=str(item_key))
            for item_key, item_value in value.items()
        }
    if isinstance(value, list):
        return [redact_value(item) for item in value]
    if isinstance(value, tuple):
        return [redact_value(item) for item in value]
    if not isinstance(value, str):
        return value

    key_lower = key.lower() if key else ""
    if any(marker in key_lower for marker in SECRET_KEY_MARKERS):
        return "[REDACTED]"
    if identifier_key_is_private(key_lower) and value:
        return _stable_hash(value)
    return redact_text(value)


def identifier_key_is_private(key_lower: str) -> bool:
    return key_lower in IDENTIFIER_KEY_MARKERS or key_lower.endswith(("_ssid", "_bssid"))


def command_is_allowed(args: list[str]) -> bool:
    if not args:
        return False
    command = args[0]
    if command == "systemctl":
        return len(args) >= 3 and args[1] == "show"
    if command == "ip":
        return args in (["ip", "-j", "addr"], ["ip", "-j", "route"], ["ip", "-j", "link"])
    if command == "nmcli":
        if args == ["nmcli", "--version"]:
            return True
        return _nmcli_is_read_only(args)
    if command == "iw":
        return _iw_is_read_only(args)
    if command == "avahi-browse":
        return args in (
            ["avahi-browse", "-rt", "_ylx-capture._tcp"],
            ["avahi-browse", "-rt", "_http._tcp"],
        )
    if command == "getent":
        return args == ["getent", "ahostsv4", "rp-ylx.local"]
    return False


def _nmcli_is_read_only(args: list[str]) -> bool:
    allowed = (
        ["nmcli", "-t", "-f", "DEVICE,TYPE,STATE,CONNECTION", "device", "status"],
        [
            "nmcli",
            "-t",
            "-f",
            "NAME,UUID,TYPE,DEVICE",
            "connection",
            "show",
            "--active",
        ],
    )
    if args in allowed:
        return True
    device_show_prefix = [
        "nmcli",
        "-t",
        "-f",
        "GENERAL.DEVICE,GENERAL.TYPE,GENERAL.STATE,GENERAL.CONNECTION,"
        "IP4.ADDRESS,IP4.GATEWAY,IP4.ROUTE,IP6.ADDRESS",
        "device",
        "show",
    ]
    return (
        args[: len(device_show_prefix)] == device_show_prefix
        and len(args) == len(device_show_prefix) + 1
    )


def _iw_is_read_only(args: list[str]) -> bool:
    if args in (["iw", "list"], ["iw", "dev"], ["iw", "reg", "get"]):
        return True
    if len(args) == 4 and args[:2] == ["iw", "dev"] and args[3] in {"info", "link"}:
        return True
    return (
        len(args) == 5
        and args[:2] == ["iw", "dev"]
        and args[3:]
        == [
            "get",
            "power_save",
        ]
    )


def command_result_to_json(result: CommandResult) -> dict[str, Any]:
    return {
        "label": result.label,
        "args": result.args,
        "returncode": result.returncode,
        "stdout": result.stdout,
        "stderr": result.stderr,
        "timeout_s": result.timeout_s,
        "elapsed_s": round(result.elapsed_s, 6),
        "error": result.error,
        "read_only": True,
    }


def _url(base_url: str, path: str) -> str:
    return base_url.rstrip("/") + path


def http_get(
    base_url: str,
    path: str,
    *,
    bearer_token: str | None = None,
    timeout: float = 5.0,
    accept: str = "application/json",
) -> HttpProbe:
    headers = {"Accept": accept}
    if bearer_token:
        headers["Authorization"] = f"Bearer {bearer_token}"
    request = urllib.request.Request(_url(base_url, path), headers=headers, method="GET")
    started = time.monotonic()
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            content_type = response.headers.get("content-type", "")
            if "text/event-stream" in content_type or path.endswith("/events"):
                body = _read_stream_sample(response, timeout=timeout)
            else:
                body = response.read(MAX_HTTP_BYTES)
            return _http_probe_from_body(
                label=f"GET {path}",
                path=path,
                status=response.status,
                headers=dict(response.headers),
                body=body,
                latency_s=time.monotonic() - started,
            )
    except urllib.error.HTTPError as exc:
        body = exc.read(MAX_HTTP_BYTES)
        return _http_probe_from_body(
            label=f"GET {path}",
            path=path,
            status=exc.code,
            headers=dict(exc.headers),
            body=body,
            latency_s=time.monotonic() - started,
        )
    except (OSError, TimeoutError) as exc:
        return HttpProbe(
            label=f"GET {path}",
            method="GET",
            path=path,
            status=None,
            headers={},
            payload=None,
            body_text=None,
            latency_s=time.monotonic() - started,
            error=f"{type(exc).__name__}: {exc}",
        )


def _read_stream_sample(response: Any, *, timeout: float) -> bytes:
    deadline = time.monotonic() + timeout
    lines: list[bytes] = []
    data_seen = False
    while time.monotonic() < deadline and len(lines) < 80:
        try:
            line = response.readline()
        except (OSError, TimeoutError):
            break
        if not line:
            break
        lines.append(line)
        if line.startswith(b"data:"):
            data_seen = True
        if data_seen and line in {b"\n", b"\r\n"}:
            break
    return b"".join(lines)[:MAX_HTTP_BYTES]


def _http_probe_from_body(
    *,
    label: str,
    path: str,
    status: int,
    headers: dict[str, str],
    body: bytes,
    latency_s: float,
) -> HttpProbe:
    parsed = _parse_json_body(body)
    body_text = None
    if not isinstance(parsed, (dict, list)):
        body_text = redact_text(truncate_text(body.decode("utf-8", "replace")))
    return HttpProbe(
        label=label,
        method="GET",
        path=path,
        status=status,
        headers=redact_value(headers),
        payload=redact_value(parsed),
        body_text=body_text,
        latency_s=latency_s,
    )


def _parse_json_body(body: bytes) -> Any:
    if not body:
        return None
    try:
        return json.loads(body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        text = body.decode("utf-8", "replace")
        return parse_sse_data(text) or text


def parse_sse_data(text: str) -> list[Any] | None:
    events: list[Any] = []
    for line in text.splitlines():
        if not line.startswith("data:"):
            continue
        payload = line.removeprefix("data:").strip()
        if not payload:
            continue
        try:
            events.append(json.loads(payload))
        except json.JSONDecodeError:
            events.append(payload)
    return events or None


def http_probe_to_json(probe: HttpProbe) -> dict[str, Any]:
    return {
        "label": probe.label,
        "method": probe.method,
        "path": probe.path,
        "status": probe.status,
        "headers": probe.headers,
        "payload": probe.payload,
        "body_text": probe.body_text,
        "latency_s": round(probe.latency_s, 6),
        "error": probe.error,
    }


def parse_iw_list(text: str) -> dict[str, Any]:
    modes = sorted(set(re.findall(r"^\s*\*\s+([A-Za-z0-9_-]+)\s*$", text, re.MULTILINE)))
    combination_lines = _valid_interface_combination_lines(text)
    combinations: list[dict[str, Any]] = []
    max_managed = 0
    max_ap = 0
    max_total = 0
    max_channels = 0
    advertises_same_phy = False

    for line in combination_lines:
        limits: dict[str, int] = {}
        for iface_group, count in re.findall(r"#\{\s*([^}]+?)\s*\}\s*<=\s*(\d+)", line):
            for iface_type in re.split(r"[,/]\s*|\s+", iface_group.strip()):
                normalized = iface_type.strip()
                if normalized:
                    limits[normalized] = max(limits.get(normalized, 0), int(count))
        total_match = re.search(r"\btotal\s*<=\s*(\d+)", line)
        channels_match = re.search(r"#channels\s*<=\s*(\d+)", line)
        total = int(total_match.group(1)) if total_match else None
        channels = int(channels_match.group(1)) if channels_match else None
        managed = limits.get("managed", 0)
        ap = limits.get("AP", 0)
        if total is not None:
            max_total = max(max_total, total)
        if channels is not None:
            max_channels = max(max_channels, channels)
        max_managed = max(max_managed, managed)
        max_ap = max(max_ap, ap)
        advertises_same_phy = advertises_same_phy or (
            managed >= 1 and ap >= 1 and (total or 0) >= 2
        )
        combinations.append(
            {
                "line": line.strip(),
                "limits": limits,
                "total": total,
                "channels": channels,
                "same_phy_ap_sta": managed >= 1 and ap >= 1 and (total or 0) >= 2,
            }
        )

    if advertises_same_phy:
        state = "driver_advertised"
    elif "managed" in modes and "AP" in modes and combination_lines:
        state = "not_advertised"
    elif "managed" in modes and "AP" in modes:
        state = "unknown"
    elif modes:
        state = "not_advertised"
    else:
        state = "unknown"

    return {
        "supported_interface_modes": modes,
        "valid_interface_combinations": combinations,
        "max_managed_interfaces": max_managed,
        "max_ap_interfaces": max_ap,
        "max_total_interfaces": max_total,
        "max_channels": max_channels,
        "driver_advertises_same_phy_ap_sta": state,
    }


def _valid_interface_combination_lines(text: str) -> list[str]:
    lines = text.splitlines()
    selected: list[str] = []
    in_section = False
    for line in lines:
        stripped = line.strip()
        if stripped.startswith("valid interface combinations:"):
            in_section = True
            continue
        if not in_section:
            continue
        if stripped.startswith("*"):
            selected.append(stripped.removeprefix("*").strip())
            continue
        if selected and stripped and not stripped.endswith(":"):
            selected[-1] = f"{selected[-1]} {stripped}"
            continue
        if stripped.endswith(":") and selected:
            break
    return selected


def parse_nmcli_terse_table(
    text: str, columns: list[str], *, hash_columns: set[str]
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for line in text.splitlines():
        if not line.strip():
            continue
        fields = line.split(":")
        row: dict[str, Any] = {}
        for index, column in enumerate(columns):
            field = fields[index] if index < len(fields) else ""
            row[column] = (
                _stable_hash(field) if column in hash_columns and field and field != "--" else field
            )
        rows.append(row)
    return rows


def parse_systemctl_show(text: str) -> dict[str, str]:
    result: dict[str, str] = {}
    for line in text.splitlines():
        key, separator, value = line.partition("=")
        if separator and key:
            result[key] = value
    return result


def parse_ip_json(result: CommandResult) -> Any:
    if result.returncode != 0 or not result.stdout.strip():
        return None
    try:
        return redact_value(json.loads(result.stdout))
    except json.JSONDecodeError:
        return result.stdout


def filesystem_inventory() -> dict[str, Any]:
    network_dir = Path("/var/lib/rp-ylx/network")
    system_connections = Path("/etc/NetworkManager/system-connections")
    paths = [
        network_dir / "rescue.json",
        network_dir / "last-known-good.json",
        network_dir / "lkg.json",
        Path("/run/rp-ylx/network-control.sock"),
        Path("/etc/systemd/system/rp-ylx-network-control.service"),
        Path("/etc/systemd/system/rp-ylx-network-control.socket"),
        Path("/lib/systemd/system/rp-ylx-network-control.service"),
        Path("/lib/systemd/system/rp-ylx-network-control.socket"),
        Path("/opt/rp-ylx/current/bin/rp-ylx"),
    ]
    return {
        "paths": {str(path): path_metadata(path) for path in paths},
        "network_profiles": list_matching_metadata(system_connections, "rp-ylx-*.nmconnection"),
        "network_state_dir": list_matching_metadata(network_dir, "*"),
    }


def path_metadata(path: Path) -> dict[str, Any]:
    try:
        stat = path.stat()
    except FileNotFoundError:
        return {"exists": False}
    except PermissionError as exc:
        return {"exists": None, "error": f"PermissionError: {exc}"}
    except OSError as exc:
        return {"exists": None, "error": f"{type(exc).__name__}: {exc}"}
    return {
        "exists": True,
        "type": "directory" if path.is_dir() else "socket" if path.is_socket() else "file",
        "mode": oct(stat.st_mode & 0o7777),
        "owner_uid": stat.st_uid,
        "group_gid": stat.st_gid,
        "size_bytes": stat.st_size if path.is_file() else None,
        "mtime": datetime.fromtimestamp(stat.st_mtime, UTC).isoformat().replace("+00:00", "Z"),
    }


def list_matching_metadata(directory: Path, pattern: str) -> dict[str, Any]:
    try:
        items = sorted(directory.glob(pattern))
    except PermissionError as exc:
        return {"available": False, "error": f"PermissionError: {exc}", "items": []}
    except OSError as exc:
        return {"available": False, "error": f"{type(exc).__name__}: {exc}", "items": []}
    return {
        "available": True,
        "items": [
            {"name_hash": _stable_hash(path.name), "metadata": path_metadata(path)}
            for path in items
        ],
    }


def collect_preflight(
    *,
    base_url: str,
    expected_commit: str | None,
    bearer_token: str | None,
    runner: ReadOnlyRunner | None = None,
) -> dict[str, Any]:
    runner = runner or ReadOnlyRunner()
    started_at = _utc_now()

    http = collect_http(base_url, bearer_token=bearer_token)
    command_sections = collect_commands(runner)
    device_payload = http.get("/api/v4/device", {}).get("payload")
    actual_commit = extract_device_commit(device_payload)
    network_payload = http.get("/api/v4/network", {}).get("payload")
    iw_summary = parse_iw_list(command_sections["wifi_phy"]["iw_list"]["stdout"] or "")

    report: dict[str, Any] = {
        "schema": SCHEMA,
        "collected_at": started_at,
        "completed_at": _utc_now(),
        "script": {
            "path": str(Path(__file__).resolve()),
            "sha256": _script_sha256(),
            "argv": sys.argv,
        },
        "target": {
            "hostname": socket.gethostname(),
            "fqdn": socket.getfqdn(),
            "platform": {
                "system": platform.system(),
                "release": platform.release(),
                "version": platform.version(),
                "machine": platform.machine(),
                "python": platform.python_version(),
            },
            "base_url": base_url,
            "expected_commit": expected_commit,
            "actual_commit": actual_commit,
            "expected_commit_match": (
                None
                if expected_commit is None or actual_commit is None
                else expected_commit == actual_commit
            ),
        },
        "api": http,
        "services": command_sections["services"],
        "routes": command_sections["routes"],
        "network_manager": command_sections["network_manager"],
        "wifi_phy": {
            **command_sections["wifi_phy"],
            "iw_list_parse": iw_summary,
        },
        "mdns": command_sections["mdns"],
        "rescue_readiness": {
            "filesystem": filesystem_inventory(),
            "network_control_health": optional_network_control_health(base_url, bearer_token),
        },
        "readiness": summarize_readiness(
            expected_commit=expected_commit,
            actual_commit=actual_commit,
            http=http,
            network_payload=network_payload,
            iw_summary=iw_summary,
            services=command_sections["services"],
        ),
        "collection_ok": True,
        "safety_transcript": {
            "commands_run": runner.commands_run,
            "commands_not_run": [
                {"pattern": pattern, "reason": "mutating"} for pattern in FORBIDDEN_COMMAND_PATTERNS
            ],
            "refused_commands": runner.refused_commands,
            "http_methods_run": sorted({probe["method"] for probe in http.values()}),
            "http_methods_not_run": ["POST", "PUT", "PATCH", "DELETE"],
            "non_coverage": [
                "No AP+STA interface creation attempted.",
                "No NetworkManager profile activation or modification attempted.",
                "No service restart, socket enablement, reboot, or failover drill attempted.",
                "No inline network credential material collected.",
            ],
            "redaction": {
                "field_markers": list(SECRET_KEY_MARKERS),
                "identifier_fields_hashed": list(IDENTIFIER_KEY_MARKERS),
                "mac_addresses_hashed": True,
                "bearer_tokens_redacted": True,
            },
        },
    }
    return redact_value(report)


def collect_http(base_url: str, *, bearer_token: str | None) -> dict[str, dict[str, Any]]:
    probes = [
        http_get(base_url, "/api/v4/device", bearer_token=bearer_token),
        http_get(base_url, "/api/v4/network", bearer_token=bearer_token),
        http_get(
            base_url,
            "/api/v4/network/events",
            bearer_token=bearer_token,
            accept="text/event-stream",
            timeout=5.0,
        ),
        http_get(base_url, "/api/v4/capture/status", bearer_token=bearer_token),
    ]
    return {probe.path: http_probe_to_json(probe) for probe in probes}


def collect_commands(runner: ReadOnlyRunner) -> dict[str, Any]:
    services = collect_services(runner)
    routes = {
        "ip_addr": command_result_to_json(runner.run("ip-addr", ["ip", "-j", "addr"])),
        "ip_route": command_result_to_json(runner.run("ip-route", ["ip", "-j", "route"])),
        "ip_link": command_result_to_json(runner.run("ip-link", ["ip", "-j", "link"])),
        "proc_net_route": read_text_file(Path("/proc/net/route")),
        "sys_class_net": sys_class_net_inventory(),
    }
    routes["ip_addr"]["parsed"] = parse_ip_json(
        command_json_to_result("ip-addr", routes["ip_addr"])
    )
    routes["ip_route"]["parsed"] = parse_ip_json(
        command_json_to_result("ip-route", routes["ip_route"])
    )
    routes["ip_link"]["parsed"] = parse_ip_json(
        command_json_to_result("ip-link", routes["ip_link"])
    )

    nm_version = command_result_to_json(runner.run("nmcli-version", ["nmcli", "--version"]))
    nm_device_status = command_result_to_json(
        runner.run(
            "nmcli-device-status",
            ["nmcli", "-t", "-f", "DEVICE,TYPE,STATE,CONNECTION", "device", "status"],
        )
    )
    nm_active = command_result_to_json(
        runner.run(
            "nmcli-active-connections",
            ["nmcli", "-t", "-f", "NAME,UUID,TYPE,DEVICE", "connection", "show", "--active"],
        )
    )
    network_manager = {
        "version": nm_version,
        "device_status": {
            **nm_device_status,
            "parsed": parse_nmcli_terse_table(
                nm_device_status["stdout"],
                ["device", "type", "state", "connection"],
                hash_columns={"connection"},
            ),
        },
        "active_connections": {
            **nm_active,
            "parsed": parse_nmcli_terse_table(
                nm_active["stdout"],
                ["name", "uuid", "type", "device"],
                hash_columns={"name", "uuid"},
            ),
        },
        "devices": {},
    }
    for device in ("wlan0", "wlan1", "eth0"):
        result = command_result_to_json(
            runner.run(
                f"nmcli-device-show-{device}",
                [
                    "nmcli",
                    "-t",
                    "-f",
                    "GENERAL.DEVICE,GENERAL.TYPE,GENERAL.STATE,GENERAL.CONNECTION,"
                    "IP4.ADDRESS,IP4.GATEWAY,IP4.ROUTE,IP6.ADDRESS",
                    "device",
                    "show",
                    device,
                ],
            )
        )
        network_manager["devices"][device] = result

    wifi_phy = {
        "iw_list": command_result_to_json(runner.run("iw-list", ["iw", "list"], timeout=10.0)),
        "iw_dev": command_result_to_json(runner.run("iw-dev", ["iw", "dev"])),
        "iw_reg_get": command_result_to_json(runner.run("iw-reg-get", ["iw", "reg", "get"])),
        "interfaces": {},
    }
    for device in ("wlan0", "wlan1"):
        wifi_phy["interfaces"][device] = {
            "info": command_result_to_json(
                runner.run(f"iw-dev-{device}-info", ["iw", "dev", device, "info"])
            ),
            "link": command_result_to_json(
                runner.run(f"iw-dev-{device}-link", ["iw", "dev", device, "link"])
            ),
            "power_save": command_result_to_json(
                runner.run(
                    f"iw-dev-{device}-power-save",
                    ["iw", "dev", device, "get", "power_save"],
                )
            ),
        }

    mdns = {
        "ylx_capture": command_result_to_json(
            runner.run(
                "avahi-ylx-capture",
                ["avahi-browse", "-rt", "_ylx-capture._tcp"],
                timeout=10.0,
            )
        ),
        "http": command_result_to_json(
            runner.run("avahi-http", ["avahi-browse", "-rt", "_http._tcp"], timeout=10.0)
        ),
        "rp_ylx_local": command_result_to_json(
            runner.run("getent-rp-ylx-local", ["getent", "ahostsv4", "rp-ylx.local"])
        ),
    }
    return {
        "services": services,
        "routes": routes,
        "network_manager": network_manager,
        "wifi_phy": wifi_phy,
        "mdns": mdns,
    }


def collect_services(runner: ReadOnlyRunner) -> dict[str, Any]:
    units = (
        DEFAULT_SERVICE_NAME,
        "NetworkManager.service",
        "avahi-daemon.service",
        "rp-ylx-wifi-watchdog.service",
        "rp-ylx-network-control.socket",
        "rp-ylx-network-control.service",
    )
    fields = (
        "ActiveState",
        "SubState",
        "LoadState",
        "UnitFileState",
        "MainPID",
        "NRestarts",
        "FragmentPath",
        "ExecMainStartTimestamp",
    )
    services: dict[str, Any] = {}
    for unit in units:
        args = ["systemctl", "show", unit, *[f"--property={field}" for field in fields]]
        result = command_result_to_json(runner.run(f"systemctl-show-{unit}", args))
        result["parsed"] = parse_systemctl_show(result["stdout"])
        services[unit] = result
    return services


def command_json_to_result(label: str, payload: dict[str, Any]) -> CommandResult:
    return CommandResult(
        label=label,
        args=list(payload.get("args") or []),
        returncode=payload.get("returncode"),
        stdout=str(payload.get("stdout") or ""),
        stderr=str(payload.get("stderr") or ""),
        timeout_s=float(payload.get("timeout_s") or 0.0),
        elapsed_s=float(payload.get("elapsed_s") or 0.0),
        error=payload.get("error"),
    )


def read_text_file(path: Path) -> dict[str, Any]:
    try:
        return {"available": True, "content": redact_text(truncate_text(path.read_text()))}
    except FileNotFoundError:
        return {"available": False, "error": "not_found"}
    except PermissionError as exc:
        return {"available": False, "error": f"PermissionError: {exc}"}
    except OSError as exc:
        return {"available": False, "error": f"{type(exc).__name__}: {exc}"}


def sys_class_net_inventory(root: Path = Path("/sys/class/net")) -> dict[str, Any]:
    try:
        interfaces = sorted(item for item in root.iterdir() if item.is_dir())
    except OSError as exc:
        return {"available": False, "error": f"{type(exc).__name__}: {exc}", "interfaces": {}}
    inventory: dict[str, Any] = {}
    for interface in interfaces:
        inventory[interface.name] = {
            "operstate": read_text_file(interface / "operstate"),
            "carrier": read_text_file(interface / "carrier"),
            "address": {
                **read_text_file(interface / "address"),
                "content": _stable_hash(
                    (read_text_file(interface / "address").get("content") or "").strip()
                )
                if read_text_file(interface / "address").get("available")
                else None,
            },
        }
    return {"available": True, "interfaces": inventory}


def optional_network_control_health(base_url: str, bearer_token: str | None) -> dict[str, Any]:
    socket_path = Path("/run/rp-ylx/network-control.sock")
    return {
        "socket": path_metadata(socket_path),
        "note": (
            "No health request sent directly to the root socket. "
            "This preflight keeps the socket disabled/closed unless deployment already enabled it."
        ),
        "http_network_status": http_probe_to_json(
            http_get(base_url, "/api/v4/network", bearer_token=bearer_token)
        ),
    }


def extract_device_commit(payload: Any) -> str | None:
    if not isinstance(payload, dict):
        return None
    build = payload.get("build")
    if not isinstance(build, dict):
        return None
    commit = build.get("commit")
    return commit if isinstance(commit, str) and commit else None


def summarize_readiness(
    *,
    expected_commit: str | None,
    actual_commit: str | None,
    http: dict[str, dict[str, Any]],
    network_payload: Any,
    iw_summary: dict[str, Any],
    services: dict[str, Any],
) -> dict[str, Any]:
    blockers: list[str] = []
    warnings: list[str] = []

    if expected_commit and actual_commit and expected_commit != actual_commit:
        blockers.append("deployed_commit_does_not_match_expected_commit")
    elif expected_commit and actual_commit is None:
        warnings.append("device_commit_unavailable")

    network_status = http.get("/api/v4/network", {})
    if network_status.get("status") != 200:
        blockers.append("v4_network_status_unavailable")
    if not isinstance(network_payload, dict):
        warnings.append("network_payload_unparseable")
    else:
        capabilities = network_payload.get("capabilities")
        if isinstance(capabilities, dict) and capabilities.get("same_phy_ap_sta") != "unverified":
            warnings.append("unexpected_same_phy_ap_sta_contract_value")

    if iw_summary["driver_advertises_same_phy_ap_sta"] != "driver_advertised":
        blockers.append("driver_does_not_advertise_same_phy_ap_sta")

    rp_service = services.get(DEFAULT_SERVICE_NAME, {}).get("parsed")
    if isinstance(rp_service, dict) and rp_service.get("ActiveState") != "active":
        blockers.append("rp_ylx_service_not_active")

    network_control = services.get("rp-ylx-network-control.socket", {}).get("parsed")
    if isinstance(network_control, dict) and network_control.get("LoadState") == "not-found":
        warnings.append("network_control_socket_unit_not_installed")

    return {
        "stage": "read_only_preflight",
        "closeable": False,
        "reason": (
            "Inventory-only evidence; no AP+STA activation, rescue drill, "
            "or rollback drill attempted."
        ),
        "driver_ap_sta": iw_summary["driver_advertises_same_phy_ap_sta"],
        "blockers": sorted(set(blockers)),
        "warnings": sorted(set(warnings)),
    }


def default_output_path(expected_commit: str | None) -> Path:
    commit = (expected_commit or "unknown")[:12]
    return DEFAULT_OUTPUT_DIR / f"openaria-score-3-ap-sta-preflight-{commit}-{_utc_stamp()}.json"


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, allow_nan=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL)
    parser.add_argument("--expected-commit")
    parser.add_argument("--bearer-token")
    parser.add_argument("--output", type=Path)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_arg_parser().parse_args(argv)
    output = args.output or default_output_path(args.expected_commit)
    report = collect_preflight(
        base_url=args.base_url,
        expected_commit=args.expected_commit,
        bearer_token=args.bearer_token,
    )
    write_json(output, report)
    print(output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

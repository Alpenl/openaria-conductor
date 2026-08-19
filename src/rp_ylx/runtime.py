"""从 Linux 标准接口读取 Device API runtime telemetry。"""

from __future__ import annotations

import fcntl
import socket
import struct
from collections.abc import Callable, Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path

_SIOCGIFADDR = 0x8915
_SIOCGIFNETMASK = 0x891B


def _ipv4_addresses(interface: str) -> list[str]:
    request = struct.pack("256s", interface.encode("ascii")[:15])
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as control:
            address = socket.inet_ntoa(fcntl.ioctl(control.fileno(), _SIOCGIFADDR, request)[20:24])
            mask = socket.inet_ntoa(fcntl.ioctl(control.fileno(), _SIOCGIFNETMASK, request)[20:24])
    except (OSError, UnicodeEncodeError):
        return []
    prefix = sum(int(octet).bit_count() for octet in socket.inet_aton(mask))
    return [f"{address}/{prefix}"]


def _default_interface(route_path: Path) -> str | None:
    try:
        rows = route_path.read_text(encoding="ascii").splitlines()[1:]
    except OSError:
        return None
    candidates: list[tuple[int, str]] = []
    for row in rows:
        fields = row.split()
        if len(fields) < 8 or fields[1] != "00000000":
            continue
        try:
            flags = int(fields[3], 16)
            metric = int(fields[6])
        except ValueError:
            continue
        if flags & 0x1:
            candidates.append((metric, fields[0]))
    return min(candidates)[1] if candidates else None


def _temperature(thermal_paths: Sequence[Path]) -> float:
    readings: list[float] = []
    for path in thermal_paths:
        try:
            value = float(path.read_text(encoding="ascii").strip())
        except (OSError, ValueError):
            continue
        if abs(value) >= 1000:
            value /= 1000
        if -40 <= value <= 125:
            readings.append(value)
    return max(readings, default=0.0)


def _interface_status(
    interface: str | None,
    net_root: Path,
    ipv4_lookup: Callable[[str], list[str]],
) -> dict[str, object]:
    if interface is None:
        return {
            "state": "unavailable",
            "interface": None,
            "addresses": [],
            "peer_or_ssid": None,
        }
    try:
        state = (net_root / interface / "operstate").read_text(encoding="ascii").strip()
    except OSError:
        state = "unknown"
    addresses = ipv4_lookup(interface) if state in {"up", "unknown"} else []
    if addresses:
        status = "connected"
    elif state == "up":
        status = "connecting"
    else:
        status = "disconnected"
    return {
        "state": status,
        "interface": interface,
        "addresses": addresses,
        "peer_or_ssid": None,
    }


def collect_linux_runtime(
    *,
    net_root: Path = Path("/sys/class/net"),
    route_path: Path = Path("/proc/net/route"),
    thermal_paths: Sequence[Path] | None = None,
    ipv4_lookup: Callable[[str], list[str]] = _ipv4_addresses,
) -> Mapping[str, object]:
    if thermal_paths is None:
        # X5 exposes hwmon attributes that can block in the kernel; thermal zones are nonblocking.
        thermal_paths = tuple(Path("/sys/class/thermal").glob("thermal_zone*/temp"))
    try:
        interfaces = sorted(path.name for path in net_root.iterdir() if path.name != "lo")
    except OSError:
        interfaces = []
    wifi_interfaces = [
        name
        for name in interfaces
        if name.startswith("wl") or (net_root / name / "wireless").is_dir()
    ]
    wired_interfaces = [name for name in interfaces if name.startswith(("eth", "en", "usb"))]
    default_interface = _default_interface(route_path)

    wifi = default_interface if default_interface in wifi_interfaces else None
    if wifi is None and wifi_interfaces:
        wifi = wifi_interfaces[0]
    wired = default_interface if default_interface in wired_interfaces else None
    if wired is None and wired_interfaces:
        wired = wired_interfaces[0]

    wifi_status = _interface_status(wifi, net_root, ipv4_lookup)
    wired_status = _interface_status(wired, net_root, ipv4_lookup)
    if default_interface in wifi_interfaces:
        default_route = "wifi_client"
        connection_method = "wifi_client" if wifi_status["state"] == "connected" else "offline"
        ap_status = {
            "state": "disabled",
            "interface": wifi,
            "addresses": [],
            "peer_or_ssid": None,
        }
    elif default_interface in wired_interfaces:
        default_route = "wired"
        connection_method = "ethernet_lan" if wired_status["state"] == "connected" else "offline"
        ap_status = _interface_status(None, net_root, ipv4_lookup)
    elif wifi_status["state"] == "connected":
        default_route = "none"
        connection_method = "wifi_ap"
        ap_status = {**wifi_status, "state": "active"}
        wifi_status = {
            "state": "disconnected",
            "interface": wifi,
            "addresses": [],
            "peer_or_ssid": None,
        }
    elif wired_status["state"] == "connected":
        default_route = "none"
        connection_method = "ethernet_direct"
        ap_status = _interface_status(None, net_root, ipv4_lookup)
    else:
        default_route = "none"
        connection_method = "offline"
        ap_status = _interface_status(None, net_root, ipv4_lookup)

    return {
        "observed_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "connection_method": connection_method,
        "temperature_celsius": _temperature(thermal_paths),
        "network": {
            "ap": ap_status,
            "wifi_client": wifi_status,
            "wired": wired_status,
            "default_route": default_route,
        },
        "live_imu": None,
    }

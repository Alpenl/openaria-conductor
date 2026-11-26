"""从 Linux 标准接口读取 Device API runtime telemetry。"""

from __future__ import annotations

import fcntl
import ipaddress
import re
import socket
import struct
from collections.abc import Callable, Mapping, Sequence
from contextlib import suppress
from pathlib import Path

from rp_ylx.core.time import utc_now_iso

_SIOCGIFADDR = 0x8915
_SIOCGIFNETMASK = 0x891B
_RTM_NEWADDR = 20
_RTM_GETADDR = 22
_NLMSG_DONE = 3
_NLM_F_REQUEST = 0x01
_NLM_F_DUMP = 0x300
_IFA_ADDRESS = 1
_IFA_LOCAL = 2
_HOTSPOT_ADDRESS = "10.42.0.1/24"
# Primary Linux interface names fit IFNAMSIZ (16 including NUL). The API
# additionally restricts their alphabet; do not emit unrepresentable names or
# truncate an ioctl query into a different interface.
_INTERFACE_NAME = re.compile(r"[A-Za-z0-9_.:-]{1,15}")
_RTF_UP = 0x0001
_RTF_REJECT = 0x0200


def _netlink_ipv4_addresses(interface: str) -> list[str]:
    if not hasattr(socket, "AF_NETLINK") or not hasattr(socket, "NETLINK_ROUTE"):
        return []
    try:
        target_index = socket.if_nametoindex(interface)
    except OSError:
        return []
    discovered: list[str] = []
    try:
        with socket.socket(socket.AF_NETLINK, socket.SOCK_RAW, socket.NETLINK_ROUTE) as netlink:
            netlink.settimeout(1.0)
            request = struct.pack(
                "=IHHIIBBBBi",
                24,
                _RTM_GETADDR,
                _NLM_F_REQUEST | _NLM_F_DUMP,
                1,
                0,
                socket.AF_INET,
                0,
                0,
                0,
                0,
            )
            netlink.sendall(request)
            while True:
                data = netlink.recv(65536)
                if not data:
                    break
                offset = 0
                done = False
                while offset + 16 <= len(data):
                    nl_len, nl_type, _flags, _seq, _pid = struct.unpack_from("=IHHII", data, offset)
                    if nl_len < 16 or offset + nl_len > len(data):
                        done = True
                        break
                    if nl_type == _NLMSG_DONE:
                        done = True
                        break
                    if nl_type == _RTM_NEWADDR and nl_len >= 24:
                        fam, prefixlen, _iflags, _scope, ifindex = struct.unpack_from(
                            "=BBBBI", data, offset + 16
                        )
                        if fam == socket.AF_INET and ifindex == target_index:
                            local_ip: str | None = None
                            peer_ip: str | None = None
                            attr_pos = offset + 24
                            attr_end = offset + nl_len
                            while attr_pos + 4 <= attr_end:
                                rta_len, rta_type = struct.unpack_from("=HH", data, attr_pos)
                                if rta_len < 4 or attr_pos + rta_len > attr_end:
                                    break
                                if rta_type == _IFA_LOCAL and rta_len >= 8:
                                    local_ip = socket.inet_ntoa(data[attr_pos + 4 : attr_pos + 8])
                                elif rta_type == _IFA_ADDRESS and rta_len >= 8:
                                    peer_ip = socket.inet_ntoa(data[attr_pos + 4 : attr_pos + 8])
                                attr_pos += (rta_len + 3) & ~3
                            chosen = local_ip or peer_ip
                            if chosen is not None:
                                addr_obj = ipaddress.IPv4Address(chosen)
                                cidr = f"{chosen}/{prefixlen}"
                                if not addr_obj.is_loopback and cidr not in discovered:
                                    discovered.append(cidr)
                    offset += (nl_len + 3) & ~3
                if done:
                    break
    except OSError:
        return []
    return discovered


def _ipv4_addresses(interface: str) -> list[str]:
    if _INTERFACE_NAME.fullmatch(interface) is None:
        return []
    request = struct.pack("256s", interface.encode("ascii"))
    addresses: list[str] = []
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as control:
            address = socket.inet_ntoa(fcntl.ioctl(control.fileno(), _SIOCGIFADDR, request)[20:24])
            mask = socket.inet_ntoa(fcntl.ioctl(control.fileno(), _SIOCGIFNETMASK, request)[20:24])
        if not ipaddress.IPv4Address(address).is_loopback:
            prefix = sum(int(octet).bit_count() for octet in socket.inet_aton(mask))
            addresses.append(f"{address}/{prefix}")
    except (OSError, UnicodeEncodeError):
        pass
    with suppress(OSError):
        for candidate in _netlink_ipv4_addresses(interface):
            if candidate not in addresses:
                addresses.append(candidate)
    return addresses


def _default_interface(route_path: Path) -> str | None:
    candidates: list[tuple[int, str]] = []
    try:
        with route_path.open("rb") as rows:
            next(rows, None)
            for row in rows:
                fields = row.split()
                if len(fields) < 8 or fields[1] != b"00000000" or fields[7] != b"00000000":
                    continue
                try:
                    interface = fields[0].decode("ascii")
                    flags = int(fields[3], 16)
                    metric = int(fields[6])
                except ValueError:
                    # Decode and parse per row so one bad entry cannot hide a
                    # healthy default route on another interface.
                    continue
                if (
                    _INTERFACE_NAME.fullmatch(interface) is not None
                    and flags & _RTF_UP
                    and not flags & _RTF_REJECT
                    and metric >= 0
                ):
                    candidates.append((metric, interface))
    except OSError:
        return None
    return min(candidates)[1] if candidates else None


def read_temperature(thermal_paths: Sequence[Path]) -> float | None:
    """Return the hottest valid SoC thermal reading in Celsius, or None if unreadable."""
    readings: list[float] = []
    for path in thermal_paths:
        try:
            value = float(path.read_text(encoding="ascii").strip())
        except (OSError, ValueError):
            continue
        # Linux thermal_zone*/temp always uses millidegrees Celsius, including
        # readings between -1 and +1 degrees. Never infer units from magnitude.
        value /= 1000
        if -40 <= value <= 125:
            readings.append(value)
    return max(readings) if readings else None


def _temperature(thermal_paths: Sequence[Path]) -> float:
    reading = read_temperature(thermal_paths)
    # OpenAPI v2/v3/v4 and api/events.py currently require numeric [-40, 125].
    return 0.0 if reading is None else reading


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
    except (OSError, UnicodeError):
        state = "unknown"
    try:
        addresses = ipv4_lookup(interface) if state in {"up", "unknown"} else []
    except OSError:
        addresses = []
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


def _select_interface(
    statuses: Sequence[dict[str, object]],
    preferred: str | None,
) -> dict[str, object] | None:
    # Preserve an observed default-route owner even while it is disconnected.
    # Otherwise select a usable address before a connecting or down adapter;
    # stable input ordering breaks ties without hiding a later healthy port.
    return min(
        statuses,
        key=lambda status: (
            status["interface"] != preferred,
            status["state"] != "connected",
            status["state"] != "connecting",
        ),
        default=None,
    )


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
        interfaces = sorted(
            path.name
            for path in net_root.iterdir()
            if path.name != "lo" and _INTERFACE_NAME.fullmatch(path.name) is not None
        )
    except OSError:
        interfaces = []
    wifi_interfaces = [
        name
        for name in interfaces
        if name.startswith("wl") or (net_root / name / "wireless").is_dir()
    ]
    wired_interfaces = [
        name
        for name in interfaces
        if name not in wifi_interfaces and name.startswith(("eth", "en", "usb"))
    ]
    default_interface = _default_interface(route_path)

    # Sample each candidate at most once: AP and client can be different adapters, and
    # an alphabetically earlier down port must not hide a usable local link.
    wifi_statuses = [_interface_status(name, net_root, ipv4_lookup) for name in wifi_interfaces]
    # The actual wired default owner is authoritative, so probing other ports
    # cannot change its projection and would only add sysfs/ioctl work.
    wired_candidates = (
        [default_interface] if default_interface in wired_interfaces else wired_interfaces
    )
    wired_statuses = [_interface_status(name, net_root, ipv4_lookup) for name in wired_candidates]
    ap_source = _select_interface(
        [status for status in wifi_statuses if _HOTSPOT_ADDRESS in status["addresses"]],
        default_interface,
    )
    wifi_status = _select_interface(
        [status for status in wifi_statuses if _HOTSPOT_ADDRESS not in status["addresses"]],
        default_interface,
    )
    wired_status = _select_interface(wired_statuses, default_interface)
    if wired_status is None:
        wired_status = _interface_status(None, net_root, ipv4_lookup)
    if wifi_status is None:
        wifi_status = _interface_status(None, net_root, ipv4_lookup)
        if ap_source is not None:
            wifi_status = {
                **wifi_status,
                "state": "disconnected",
                "interface": ap_source["interface"],
            }

    if ap_source is not None:
        ap_status = {**ap_source, "state": "active"}
    else:
        ap_status = {
            "state": "disabled" if wifi_status["interface"] is not None else "unavailable",
            "interface": wifi_status["interface"],
            "addresses": [],
            "peer_or_ssid": None,
        }

    if default_interface is not None and default_interface == wired_status["interface"]:
        default_route = "wired"
        connection_method = "ethernet_lan" if wired_status["state"] == "connected" else "offline"
    elif (
        default_interface is not None
        and default_interface == wifi_status["interface"]
        and (ap_source is None or default_interface != ap_source["interface"])
    ):
        default_route = "wifi_client"
        connection_method = "wifi_client" if wifi_status["state"] == "connected" else "offline"
    else:
        default_route = "none"
        if ap_source is not None:
            connection_method = "wifi_ap"
        elif wifi_status["state"] == "connected":
            connection_method = "wifi_client"
        elif wired_status["state"] == "connected":
            connection_method = "ethernet_direct"
        else:
            connection_method = "offline"

    return {
        "observed_at": utc_now_iso(),
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

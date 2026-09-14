"""Network request validation shared by HTTP and the privileged controller."""

from __future__ import annotations

import ipaddress
import re
from collections.abc import Mapping

NETWORK_MODES = ("hotspot", "wifi-client", "ethernet-dhcp", "ethernet-static")
WIFI_SECURITY = frozenset({"open", "wpa2-personal", "wpa3-personal", "wpa2-wpa3-personal"})
NETWORK_CREDENTIAL_REF = re.compile(r"^cred-[A-Za-z0-9_.:-]+$")
_UUID_V7 = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$")


def _valid_ipv4(value: object) -> bool:
    if not isinstance(value, str):
        return False
    try:
        ipaddress.IPv4Address(value)
    except ValueError:
        return False
    return True


def _valid_network_static_ipv4(value: object) -> bool:
    if not isinstance(value, Mapping) or set(value) != {
        "address",
        "prefix_length",
        "gateway",
        "dns",
    }:
        return False
    dns = value.get("dns")
    return (
        _valid_ipv4(value.get("address"))
        and type(value.get("prefix_length")) is int
        and 1 <= value["prefix_length"] <= 32
        and (value.get("gateway") is None or _valid_ipv4(value.get("gateway")))
        and isinstance(dns, list)
        and len(dns) <= 3
        and all(_valid_ipv4(item) for item in dns)
        and len(set(dns)) == len(dns)
    )


def valid_network_ethernet(value: object) -> bool:
    if not isinstance(value, Mapping) or set(value) != {"addressing", "static_ipv4"}:
        return False
    addressing = value.get("addressing")
    static = value.get("static_ipv4")
    return (addressing == "dhcp" and static is None) or (
        addressing == "static" and _valid_network_static_ipv4(static)
    )


def _valid_network_apply_wifi_client(value: object) -> bool:
    if not isinstance(value, Mapping) or not {"ssid", "security"}.issubset(value):
        return False
    security = value.get("security")
    expected_keys = (
        {"ssid", "security"} if security == "open" else {"ssid", "security", "credential_ref"}
    )
    if set(value) != expected_keys or security not in WIFI_SECURITY:
        return False
    ssid = value.get("ssid")
    credential_ref = value.get("credential_ref")
    return (
        isinstance(ssid, str)
        and 1 <= len(ssid.encode("utf-8")) <= 32
        and (
            security == "open"
            or isinstance(credential_ref, str)
            and 1 <= len(credential_ref) <= 128
            and NETWORK_CREDENTIAL_REF.fullmatch(credential_ref) is not None
        )
    )


def _valid_network_apply_desired_state(value: object) -> bool:
    if not isinstance(value, Mapping) or set(value) != {"mode", "wifi_client", "ethernet"}:
        return False
    mode = value.get("mode")
    wifi = value.get("wifi_client")
    ethernet = value.get("ethernet")
    if mode not in NETWORK_MODES:
        return False
    if mode == "wifi-client":
        if not _valid_network_apply_wifi_client(wifi):
            return False
    elif wifi is not None:
        return False
    if ethernet is not None and not valid_network_ethernet(ethernet):
        return False
    return not (mode == "ethernet-static" and ethernet is None)


def valid_network_request(operation: str, body: object) -> bool:
    if not isinstance(body, Mapping):
        return False
    if operation == "apply":
        return (
            set(body) == {"schema", "desired"}
            and body.get("schema") == "ylx.network-apply-request.v1"
            and _valid_network_apply_desired_state(body.get("desired"))
        )
    if operation == "retry":
        return (
            set(body) == {"schema", "transaction_id"}
            and body.get("schema") == "ylx.network-retry-request.v1"
            and isinstance(body.get("transaction_id"), str)
            and _UUID_V7.fullmatch(str(body["transaction_id"])) is not None
        )
    return set(body) == {"schema"} and body.get("schema") == "ylx.network-forget-request.v1"

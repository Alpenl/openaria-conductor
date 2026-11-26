"""Network request and contract validation shared by HTTP, CLI, and controller."""

from __future__ import annotations

import ipaddress
import re
from collections.abc import Mapping

from rp_ylx.core.ids import parse_rfc3339, valid_sha256, valid_uuid4, valid_uuid7

NETWORK_MODES = ("hotspot", "wifi-client", "ethernet-dhcp", "ethernet-static")
WIFI_SECURITY = frozenset({"open", "wpa2-personal", "wpa3-personal", "wpa2-wpa3-personal"})
NETWORK_CREDENTIAL_REF = re.compile(r"^cred-[A-Za-z0-9_.:-]+$")
HOTSPOT_IPV4_NETWORK = ipaddress.IPv4Network("10.42.0.0/24")

CONTROLLER_STATE_SCHEMA = "ylx.network-controller-state.v1"
TRANSACTION_SCHEMA = "ylx.network-transaction.v1"
RECEIPT_SCHEMA = "ylx.network-transaction-receipt.v1"
TERMINAL_STATUSES = frozenset({"committed", "rescued", "failed"})
TRANSACTION_STATUSES = frozenset({"accepted", "running", *TERMINAL_STATUSES})
TRANSACTION_STAGES = frozenset(
    {
        "accepted",
        "prepared",
        "ap_ready",
        "activating",
        "verifying",
        "committed",
        "falling_back",
        "rescued",
        "failed",
        "forgetting",
        "forgotten",
    }
)
RECOVERY_ACTIONS = frozenset(
    {
        "await_device",
        "reconnect_target_lan",
        "reconnect_rescue_ap",
        "retry",
        "service_required",
        "none",
    }
)
TRANSACTION_ERROR_CODES = frozenset(
    {
        "rescue_ap_unavailable",
        "credential_rejected",
        "dhcp_timeout",
        "route_lost",
        "network_manager_unavailable",
        "concurrency_unsupported",
    }
)
STAGE_STATUS = {
    "accepted": "accepted",
    "prepared": "running",
    "ap_ready": "running",
    "activating": "running",
    "verifying": "running",
    "committed": "committed",
    "falling_back": "running",
    "rescued": "rescued",
    "failed": "failed",
    "forgetting": "running",
    "forgotten": "committed",
}


def usable_ipv4_address(address: ipaddress.IPv4Address) -> bool:
    """Return True if address is a unicast, non-link-local, non-loopback IPv4 host."""
    return not (
        address.is_link_local
        or address.is_loopback
        or address.is_multicast
        or address.is_unspecified
    )


def validate_wifi_ssid(ssid: object) -> str:
    """Validate a Wi-Fi SSID (1..32 UTF-8 bytes, no ASCII control characters)."""
    if (
        not isinstance(ssid, str)
        or not ssid
        or any(ord(character) < 32 for character in ssid)
        or not 1 <= len(ssid.encode("utf-8")) <= 32
    ):
        raise ValueError("SSID 必须为 1 至 32 字节且不能包含控制字符")
    return ssid


def validate_static_ipv4_fields(
    address_cidr: object,
    gateway: object = None,
    dns: object = (),
    *,
    require_unique_dns: bool = False,
) -> tuple[ipaddress.IPv4Interface, ipaddress.IPv4Address | None, list[str]]:
    """Validate static IPv4 interface CIDR, gateway subnet membership, and DNS servers."""
    if (
        not isinstance(address_cidr, str)
        or not address_cidr
        or any(ord(character) < 32 for character in address_cidr)
    ):
        raise ValueError("address 必须是有效 IPv4 CIDR")
    try:
        interface = ipaddress.IPv4Interface(address_cidr)
    except ValueError as exc:
        raise ValueError("address 必须是有效 IPv4 CIDR") from exc
    if not usable_ipv4_address(interface.ip):
        raise ValueError("address 必须是可用的非 link-local IPv4 主机地址")
    if interface.network.overlaps(HOTSPOT_IPV4_NETWORK):
        raise ValueError("有线静态地址不能与热点网段重叠")

    parsed_gateway: ipaddress.IPv4Address | None = None
    if gateway is not None:
        if (
            not isinstance(gateway, str)
            or not gateway
            or any(ord(character) < 32 for character in gateway)
        ):
            raise ValueError("gateway 必须是有效 IPv4 地址")
        try:
            parsed_gateway = ipaddress.IPv4Address(gateway)
        except ValueError as exc:
            raise ValueError("gateway 必须是有效 IPv4 地址") from exc
        if not usable_ipv4_address(parsed_gateway) or parsed_gateway not in interface.network:
            raise ValueError("gateway 必须是同一子网中的可用主机地址")
        if interface.network.prefixlen < 31 and parsed_gateway in {
            interface.network.network_address,
            interface.network.broadcast_address,
        }:
            raise ValueError("gateway 必须是同一子网中的可用主机地址")

    if not isinstance(dns, (list, tuple)) or len(dns) > 3:
        raise ValueError("dns 必须是最多三个 IPv4 地址的数组")
    normalized_dns: list[str] = []
    for item in dns:
        if not isinstance(item, str):
            raise ValueError("dns 必须是最多三个 IPv4 地址的数组")
        try:
            parsed_dns = ipaddress.IPv4Address(item)
        except ValueError as exc:
            raise ValueError("dns 必须是最多三个 IPv4 地址的数组") from exc
        if not usable_ipv4_address(parsed_dns):
            raise ValueError("dns 必须是可用的非 link-local IPv4 地址")
        normalized_dns.append(str(parsed_dns))
    if require_unique_dns and len(set(normalized_dns)) != len(normalized_dns):
        raise ValueError("dns 地址不能重复")
    return interface, parsed_gateway, normalized_dns


def _valid_ipv4(value: object) -> bool:
    if not isinstance(value, str):
        return False
    try:
        ipaddress.IPv4Address(value)
    except ValueError:
        return False
    return True


def _valid_network_static_ipv4_shape(value: object) -> bool:
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


def _valid_network_static_ipv4(value: object) -> bool:
    if not _valid_network_static_ipv4_shape(value):
        return False
    assert isinstance(value, Mapping)
    try:
        validate_static_ipv4_fields(
            f"{value['address']}/{value['prefix_length']}",
            value.get("gateway"),
            value.get("dns"),
            require_unique_dns=True,
        )
    except ValueError:
        return False
    return True


def valid_network_ethernet_shape(value: object) -> bool:
    """Validate structural schema of an ethernet configuration object."""
    if not isinstance(value, Mapping) or set(value) != {"addressing", "static_ipv4"}:
        return False
    addressing = value.get("addressing")
    static = value.get("static_ipv4")
    return (addressing == "dhcp" and static is None) or (
        addressing == "static" and _valid_network_static_ipv4_shape(static)
    )


def valid_network_ethernet(value: object) -> bool:
    """Validate both structural schema and IPv4 address policy of ethernet configuration."""
    if not valid_network_ethernet_shape(value):
        return False
    assert isinstance(value, Mapping)
    if value.get("addressing") == "static":
        return _valid_network_static_ipv4(value.get("static_ipv4"))
    return True


def _valid_network_apply_wifi_client_shape(value: object) -> bool:
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


def _valid_network_apply_desired_shape(value: object) -> bool:
    if not isinstance(value, Mapping) or set(value) != {"mode", "wifi_client", "ethernet"}:
        return False
    mode = value.get("mode")
    wifi = value.get("wifi_client")
    ethernet = value.get("ethernet")
    if mode not in NETWORK_MODES:
        return False
    if mode == "wifi-client":
        if not _valid_network_apply_wifi_client_shape(wifi):
            return False
    elif wifi is not None:
        return False
    if ethernet is not None and not valid_network_ethernet_shape(ethernet):
        return False
    return not (mode == "ethernet-static" and ethernet is None)


def validate_desired_state_policy(value: object) -> tuple[str, str] | None:
    """Check semantic network policy rules aligned with network._validate_config.

    Returns None if valid, or (error_code, message) on policy violation.
    """
    if not isinstance(value, Mapping):
        return ("candidate_invalid", "desired network state is invalid")
    mode = value.get("mode")
    if mode == "wifi-client":
        wifi = value.get("wifi_client")
        if not isinstance(wifi, Mapping):
            return ("candidate_invalid", "wifi_client configuration is invalid")
        try:
            validate_wifi_ssid(wifi.get("ssid"))
        except ValueError:
            return ("candidate_invalid", "Wi-Fi SSID contains invalid characters or length")
    ethernet = value.get("ethernet")
    if isinstance(ethernet, Mapping) and ethernet.get("addressing") == "static":
        static = ethernet.get("static_ipv4")
        if not isinstance(static, Mapping):
            return ("candidate_invalid", "static IPv4 configuration is invalid")
        try:
            validate_static_ipv4_fields(
                f"{static.get('address')}/{static.get('prefix_length')}",
                static.get("gateway"),
                static.get("dns"),
                require_unique_dns=True,
            )
        except ValueError:
            return ("candidate_invalid", "static IPv4 configuration violates addressing policy")
    return None


def valid_network_request_shape(operation: str, body: object) -> bool:
    """Validate structural schema of a network mutation request without semantic policy checks."""
    if not isinstance(body, Mapping):
        return False
    if operation == "apply":
        return (
            set(body) == {"schema", "desired"}
            and body.get("schema") == "ylx.network-apply-request.v1"
            and _valid_network_apply_desired_shape(body.get("desired"))
        )
    if operation == "retry":
        return (
            set(body) == {"schema", "transaction_id"}
            and body.get("schema") == "ylx.network-retry-request.v1"
            and valid_uuid7(body.get("transaction_id"))
        )
    return set(body) == {"schema"} and body.get("schema") == "ylx.network-forget-request.v1"


def valid_network_request(operation: str, body: object) -> bool:
    """Validate both structural schema and semantic policy of a network mutation request."""
    if not valid_network_request_shape(operation, body):
        return False
    if operation == "apply":
        assert isinstance(body, Mapping)
        return validate_desired_state_policy(body.get("desired")) is None
    return True


def valid_desired_state(value: object) -> bool:
    """Validate a persisted or projected secret-free network desired state."""
    if not isinstance(value, Mapping) or set(value) != {"mode", "wifi_client", "ethernet"}:
        return False
    mode = value.get("mode")
    if mode not in NETWORK_MODES:
        return False
    wifi = value.get("wifi_client")
    if mode == "wifi-client":
        if (
            not isinstance(wifi, Mapping)
            or set(wifi) != {"ssid", "security", "credential_state"}
            or not isinstance(wifi.get("ssid"), str)
            or not 1 <= len(str(wifi["ssid"]).encode("utf-8")) <= 32
            or wifi.get("security") not in WIFI_SECURITY
            or wifi.get("credential_state") not in {"absent", "pending_input", "stored"}
        ):
            return False
        if (wifi.get("security") == "open") != (wifi.get("credential_state") == "absent"):
            return False
    elif wifi is not None:
        return False
    ethernet = value.get("ethernet")
    if ethernet is not None and not valid_network_ethernet_shape(ethernet):
        return False
    return not (mode == "ethernet-static" and ethernet is None)


def valid_deadline(value: object) -> bool:
    """Validate a device-monotonic transaction deadline object."""
    if value is None:
        return True
    if not isinstance(value, Mapping) or set(value) != {
        "time_base",
        "deadline_ns",
        "remaining_seconds",
    }:
        return False
    remaining = value.get("remaining_seconds")
    return (
        value.get("time_base") == "device_monotonic"
        and type(value.get("deadline_ns")) is int
        and value["deadline_ns"] >= 0
        and isinstance(remaining, (int, float))
        and not isinstance(remaining, bool)
        and 0 <= remaining <= 10
    )


def _valid_rfc3339(value: object) -> bool:
    try:
        parse_rfc3339(value)
    except ValueError:
        return False
    return True


def valid_transaction(value: object) -> bool:
    """Validate a v1 network mutation transaction record."""
    if not isinstance(value, Mapping) or set(value) != {
        "schema",
        "authority_epoch",
        "source_revision",
        "transaction_id",
        "operation",
        "status",
        "stage",
        "desired",
        "accepted_at",
        "updated_at",
        "deadline",
        "recovery_action",
        "rescue",
        "error",
    }:
        return False
    status = value.get("status")
    stage = value.get("stage")
    deadline = value.get("deadline")
    recovery_action = value.get("recovery_action")
    rescue = value.get("rescue")
    error = value.get("error")
    if status in {"accepted", "running", "committed"} and error is not None:
        return False
    if status in {"rescued", "failed"} and error is None:
        return False
    if status in TERMINAL_STATUSES and deadline is not None:
        return False
    if stage in {"activating", "verifying"} and deadline is None:
        return False
    if status == "rescued" and recovery_action != "reconnect_rescue_ap":
        return False
    return (
        value.get("schema") == TRANSACTION_SCHEMA
        and valid_uuid4(value.get("authority_epoch"))
        and type(value.get("source_revision")) is int
        and value["source_revision"] >= 0
        and valid_uuid7(value.get("transaction_id"))
        and value.get("operation") in {"apply", "retry", "forget"}
        and status in TRANSACTION_STATUSES
        and stage in TRANSACTION_STAGES
        and STAGE_STATUS.get(str(stage)) == status
        and valid_desired_state(value.get("desired"))
        and _valid_rfc3339(value.get("accepted_at"))
        and _valid_rfc3339(value.get("updated_at"))
        and valid_deadline(deadline)
        and recovery_action in RECOVERY_ACTIONS
        and isinstance(rescue, Mapping)
        and set(rescue) == {"ap_validated", "fallback_mode", "failure_trigger_seconds"}
        and type(rescue.get("ap_validated")) is bool
        and rescue.get("fallback_mode") == "hotspot"
        and rescue.get("failure_trigger_seconds") == 10
        and (
            error is None
            or isinstance(error, Mapping)
            and set(error) == {"code", "message", "retryable"}
            and error.get("code") in TRANSACTION_ERROR_CODES
            and isinstance(error.get("message"), str)
            and 1 <= len(error["message"]) <= 512
            and type(error.get("retryable")) is bool
        )
    )


__all__ = [
    "CONTROLLER_STATE_SCHEMA",
    "HOTSPOT_IPV4_NETWORK",
    "NETWORK_CREDENTIAL_REF",
    "NETWORK_MODES",
    "RECEIPT_SCHEMA",
    "RECOVERY_ACTIONS",
    "STAGE_STATUS",
    "TERMINAL_STATUSES",
    "TRANSACTION_ERROR_CODES",
    "TRANSACTION_SCHEMA",
    "TRANSACTION_STAGES",
    "TRANSACTION_STATUSES",
    "WIFI_SECURITY",
    "usable_ipv4_address",
    "valid_deadline",
    "valid_desired_state",
    "valid_network_ethernet",
    "valid_network_ethernet_shape",
    "valid_network_request",
    "valid_network_request_shape",
    "valid_sha256",
    "valid_transaction",
    "valid_uuid4",
    "valid_uuid7",
    "validate_desired_state_policy",
    "validate_static_ipv4_fields",
    "validate_wifi_ssid",
]

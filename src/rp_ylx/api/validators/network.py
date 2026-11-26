"""Network status, scan, transaction, and credential validators."""

from __future__ import annotations

import re
from collections.abc import Mapping
from http import HTTPStatus

from rp_ylx.core.ids import parse_rfc3339, valid_uuid4, valid_uuid7
from rp_ylx.errors import ProviderError
from rp_ylx.network_validation import (
    NETWORK_CREDENTIAL_REF,
    NETWORK_MODES,
    WIFI_SECURITY,
    valid_network_ethernet,
)

NETWORK_MUTATION_OPERATIONS = ("apply", "retry", "forget")
NETWORK_TOKEN = re.compile(r"^[A-Za-z0-9_.:-]{1,64}$")
MDNS_TOKEN = re.compile(r"^[A-Za-z0-9_.-]{1,128}$")
INTERFACE_NAME = re.compile(r"^[A-Za-z0-9_.:-]+$")
NETWORK_INTERFACE_STATES = frozenset(
    {
        "disabled",
        "disconnected",
        "starting",
        "connecting",
        "connected",
        "active",
        "degraded",
        "failed",
        "unavailable",
    }
)
NETWORK_TRANSACTION_STATUSES = frozenset({"accepted", "running", "committed", "rescued", "failed"})
NETWORK_TRANSACTION_STAGES = frozenset(
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
NETWORK_RECOVERY_ACTIONS = frozenset(
    {
        "await_device",
        "reconnect_target_lan",
        "reconnect_rescue_ap",
        "retry",
        "service_required",
        "none",
    }
)
NETWORK_MUTATION_DISABLED_REASONS = frozenset(
    {
        "not_enabled",
        "auth_profile_unavailable",
        "controller_unavailable",
        "network_manager_unavailable",
        "rescue_ap_not_validated",
        "capture_active",
        "recovery_required",
        "maintenance_window_closed",
        "unsupported_concurrency",
    }
)
INVALID_NETWORK_DESIRED_STATE_REASONS = frozenset(
    {
        "missing_credential_ref",
        "credential_ref_expired",
        "credential_ref_already_used",
        "credential_ref_not_allowed",
        "security_mismatch",
        "ssid_too_long",
        "unsupported_mode",
        "invalid_static_ipv4",
        "unsafe_secret_field",
        "active_state",
    }
)
NETWORK_STAGE_STATUSES = {
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
NETWORK_TRANSACTION_ERROR_CODES = frozenset(
    {
        "rescue_ap_unavailable",
        "credential_rejected",
        "dhcp_timeout",
        "route_lost",
        "network_manager_unavailable",
        "concurrency_unsupported",
    }
)


def valid_datetime(value: object) -> bool:
    if not isinstance(value, str) or not value:
        return False
    try:
        parse_rfc3339(value)
    except ValueError:
        return False
    return True


def valid_network_interface(value: object) -> bool:
    """Validate one network interface state object including bounded address strings."""
    if not isinstance(value, Mapping) or set(value) != {
        "state",
        "interface",
        "addresses",
        "peer_or_ssid",
    }:
        return False
    state = value.get("state")
    interface = value.get("interface")
    addresses = value.get("addresses")
    peer_or_ssid = value.get("peer_or_ssid")
    if (
        state not in NETWORK_INTERFACE_STATES
        or (
            interface is not None
            and (
                not isinstance(interface, str)
                or not 1 <= len(interface) <= 64
                or INTERFACE_NAME.fullmatch(interface) is None
            )
        )
        or not isinstance(addresses, list)
        or any(not isinstance(address, str) or not 1 <= len(address) <= 64 for address in addresses)
        or len(set(addresses)) != len(addresses)
        or (
            peer_or_ssid is not None
            and (not isinstance(peer_or_ssid, str) or not 1 <= len(peer_or_ssid) <= 128)
        )
    ):
        return False
    return not (
        state in {"connected", "active", "degraded"}
        and (not isinstance(interface, str) or not addresses)
    )


def valid_network_desired_wifi_client(value: object) -> bool:
    if not isinstance(value, Mapping) or set(value) != {
        "ssid",
        "security",
        "credential_state",
    }:
        return False
    ssid = value.get("ssid")
    security = value.get("security")
    credential_state = value.get("credential_state")
    return (
        isinstance(ssid, str)
        and 1 <= len(ssid.encode("utf-8")) <= 32
        and security in WIFI_SECURITY
        and credential_state in {"absent", "pending_input", "stored"}
        and (security == "open") == (credential_state == "absent")
    )


def valid_network_desired_state(value: object) -> bool:
    if not isinstance(value, Mapping) or set(value) != {"mode", "wifi_client", "ethernet"}:
        return False
    mode = value.get("mode")
    wifi = value.get("wifi_client")
    ethernet = value.get("ethernet")
    if mode not in NETWORK_MODES:
        return False
    if mode == "wifi-client":
        if not valid_network_desired_wifi_client(wifi):
            return False
    elif wifi is not None:
        return False
    if ethernet is not None and not valid_network_ethernet(ethernet):
        return False
    return not (mode == "ethernet-static" and ethernet is None)


def valid_network_mdns(value: object) -> bool:
    if not isinstance(value, Mapping) or set(value) != {
        "hostname",
        "service",
        "aliases",
        "port",
    }:
        return False
    aliases = value.get("aliases")
    return (
        isinstance(value.get("hostname"), str)
        and MDNS_TOKEN.fullmatch(str(value["hostname"])) is not None
        and str(value["hostname"]).endswith(".local")
        and isinstance(value.get("service"), str)
        and MDNS_TOKEN.fullmatch(str(value["service"])) is not None
        and isinstance(aliases, list)
        and len(aliases) <= 16
        and all(
            isinstance(alias, str) and MDNS_TOKEN.fullmatch(alias) is not None for alias in aliases
        )
        and len(set(aliases)) == len(aliases)
        and type(value.get("port")) is int
        and 1 <= value["port"] <= 65535
    )


def valid_network_devices(value: object) -> bool:
    if not isinstance(value, list) or len(value) > 64:
        return False
    interfaces: set[str] = set()
    for device in value:
        if (
            not isinstance(device, Mapping)
            or set(device) != {"interface", "type", "state"}
            or not all(isinstance(device.get(key), str) for key in ("interface", "type", "state"))
            or any(
                NETWORK_TOKEN.fullmatch(str(device[key])) is None
                for key in ("interface", "type", "state")
            )
            or device["interface"] in interfaces
        ):
            return False
        interfaces.add(str(device["interface"]))
    return True


def valid_network_observed_state(value: object) -> bool:
    if not isinstance(value, Mapping) or set(value) != {
        "ap",
        "wifi_client",
        "wired",
        "default_route",
        "mdns",
        "devices",
    }:
        return False
    return (
        valid_network_interface(value.get("ap"))
        and valid_network_interface(value.get("wifi_client"))
        and valid_network_interface(value.get("wired"))
        and value.get("default_route") in {"wifi_client", "wired", "none"}
        and valid_network_mdns(value.get("mdns"))
        and valid_network_devices(value.get("devices"))
    )


def valid_network_mutation_capability(value: object) -> bool:
    if not isinstance(value, Mapping) or set(value) != {
        "enabled",
        "disabled_reason",
        "operations",
        "idempotency_key_required",
        "secret_handling",
        "active_state_policy",
    }:
        return False
    enabled = value.get("enabled")
    disabled_reason = value.get("disabled_reason")
    return (
        type(enabled) is bool
        and (
            disabled_reason is None
            if enabled
            else disabled_reason in NETWORK_MUTATION_DISABLED_REASONS
        )
        and value.get("operations") == list(NETWORK_MUTATION_OPERATIONS)
        and value.get("idempotency_key_required") is True
        and value.get("secret_handling") == "opaque_credential_reference_only"
        and value.get("active_state_policy") == "idle_only"
    )


def valid_network_concurrency_capability(value: object) -> bool:
    return (
        isinstance(value, Mapping)
        and set(value)
        == {
            "rescue_ap_required",
            "same_phy_ap_sta",
            "exclusive_client_failure_timeout_seconds",
            "max_managed_interfaces",
            "max_ap_interfaces",
        }
        and value.get("rescue_ap_required") is True
        and value.get("same_phy_ap_sta") in {"supported", "unsupported", "unverified"}
        and value.get("exclusive_client_failure_timeout_seconds") == 10
        and type(value.get("max_managed_interfaces")) is int
        and 0 <= value["max_managed_interfaces"] <= 8
        and type(value.get("max_ap_interfaces")) is int
        and 0 <= value["max_ap_interfaces"] <= 8
    )


def valid_network_transaction_error(value: object) -> bool:
    return (
        isinstance(value, Mapping)
        and set(value) == {"code", "message", "retryable"}
        and value.get("code") in NETWORK_TRANSACTION_ERROR_CODES
        and isinstance(value.get("message"), str)
        and 1 <= len(value["message"]) <= 512
        and type(value.get("retryable")) is bool
    )


def valid_network_transaction(value: object) -> bool:
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
    rescue = value.get("rescue")
    error = value.get("error")
    deadline = value.get("deadline")
    status = value.get("status")
    stage = value.get("stage")
    valid_deadline = deadline is None or (
        isinstance(deadline, Mapping)
        and set(deadline) == {"time_base", "deadline_ns", "remaining_seconds"}
        and deadline.get("time_base") == "device_monotonic"
        and type(deadline.get("deadline_ns")) is int
        and deadline["deadline_ns"] >= 0
        and isinstance(deadline.get("remaining_seconds"), (int, float))
        and not isinstance(deadline.get("remaining_seconds"), bool)
        and 0 <= deadline["remaining_seconds"] <= 10
    )
    valid = (
        value.get("schema") == "ylx.network-transaction.v1"
        and valid_uuid4(value.get("authority_epoch"))
        and type(value.get("source_revision")) is int
        and value["source_revision"] >= 0
        and valid_uuid7(value.get("transaction_id"))
        and value.get("operation") in NETWORK_MUTATION_OPERATIONS
        and status in NETWORK_TRANSACTION_STATUSES
        and stage in NETWORK_TRANSACTION_STAGES
        and NETWORK_STAGE_STATUSES.get(str(stage)) == status
        and valid_network_desired_state(value.get("desired"))
        and valid_datetime(value.get("accepted_at"))
        and valid_datetime(value.get("updated_at"))
        and valid_deadline
        and value.get("recovery_action") in NETWORK_RECOVERY_ACTIONS
        and isinstance(rescue, Mapping)
        and set(rescue) == {"ap_validated", "fallback_mode", "failure_trigger_seconds"}
        and type(rescue.get("ap_validated")) is bool
        and rescue.get("fallback_mode") == "hotspot"
        and rescue.get("failure_trigger_seconds") == 10
        and (error is None or valid_network_transaction_error(error))
    )
    if not valid:
        return False
    terminal = status in {"committed", "rescued", "failed"}
    return not (
        (status in {"accepted", "running", "committed"} and error is not None)
        or (status in {"rescued", "failed"} and error is None)
        or (terminal and deadline is not None)
        or (stage in {"activating", "verifying"} and deadline is None)
        or (status == "rescued" and value.get("recovery_action") != "reconnect_rescue_ap")
    )


def valid_network_transaction_window(value: object) -> bool:
    return (
        isinstance(value, Mapping)
        and set(value) == {"current", "latest"}
        and (value.get("current") is None or valid_network_transaction(value.get("current")))
        and (value.get("latest") is None or valid_network_transaction(value.get("latest")))
    )


def valid_network_status(value: object) -> bool:
    if not isinstance(value, Mapping) or set(value) != {
        "schema",
        "authority_epoch",
        "source_revision",
        "observed_at",
        "saved",
        "verified",
        "desired",
        "observed",
        "transaction",
        "mutation_capability",
        "concurrency_capability",
    }:
        return False
    transaction = value.get("transaction")
    transactions = (
        []
        if not isinstance(transaction, Mapping)
        else [transaction.get("current"), transaction.get("latest")]
    )
    return (
        value.get("schema") == "ylx.network-status.v1"
        and valid_uuid4(value.get("authority_epoch"))
        and type(value.get("source_revision")) is int
        and value["source_revision"] >= 0
        and valid_datetime(value.get("observed_at"))
        and type(value.get("saved")) is bool
        and type(value.get("verified")) is bool
        and (not value["verified"] or value["saved"])
        and valid_network_desired_state(value.get("desired"))
        and valid_network_observed_state(value.get("observed"))
        and valid_network_transaction_window(transaction)
        and all(
            item is None
            or (
                item["authority_epoch"] == value["authority_epoch"]
                and item["source_revision"] <= value["source_revision"]
            )
            for item in transactions
        )
        and (
            transaction.get("current") is None
            or transaction["current"]["status"] in {"accepted", "running"}
        )
        and valid_network_mutation_capability(value.get("mutation_capability"))
        and valid_network_concurrency_capability(value.get("concurrency_capability"))
    )


def valid_network_receipt(value: object) -> bool:
    return (
        isinstance(value, Mapping)
        and set(value) == {"schema", "accepted_at", "transaction"}
        and value.get("schema") == "ylx.network-transaction-receipt.v1"
        and valid_datetime(value.get("accepted_at"))
        and valid_network_transaction(value.get("transaction"))
        and value["transaction"]["status"] == "accepted"
        and value.get("accepted_at") == value["transaction"]["accepted_at"]
    )


def valid_network_scan(value: object) -> bool:
    if not isinstance(value, Mapping) or set(value) != {
        "schema",
        "authority_epoch",
        "source_revision",
        "scanned_at",
        "networks",
    }:
        return False
    networks = value.get("networks")
    if not isinstance(networks, list) or len(networks) > 256:
        return False
    for entry in networks:
        if not isinstance(entry, Mapping) or set(entry) != {
            "ssid",
            "hidden",
            "security",
            "signal_dbm",
            "credential_required",
        }:
            return False
        ssid = entry.get("ssid")
        hidden = entry.get("hidden")
        security = entry.get("security")
        signal = entry.get("signal_dbm")
        if (
            type(hidden) is not bool
            or security not in WIFI_SECURITY
            or type(signal) is not int
            or not -127 <= signal <= 0
            or type(entry.get("credential_required")) is not bool
            or entry["credential_required"] != (security != "open")
            or (hidden and ssid is not None)
            or (
                not hidden
                and (not isinstance(ssid, str) or not 1 <= len(ssid.encode("utf-8")) <= 32)
            )
        ):
            return False
    return (
        value.get("schema") == "ylx.network-scan.v1"
        and valid_uuid4(value.get("authority_epoch"))
        and type(value.get("source_revision")) is int
        and value["source_revision"] >= 0
        and valid_datetime(value.get("scanned_at"))
    )


def valid_network_credential_receipt(value: object) -> bool:
    if not isinstance(value, Mapping) or set(value) != {
        "schema",
        "credential_ref",
        "issued_at",
        "expires_at",
        "ttl_seconds",
        "single_use",
    }:
        return False
    issued_at = value.get("issued_at")
    expires_at = value.get("expires_at")
    if not (
        value.get("schema") == "ylx.network-credential-receipt.v1"
        and isinstance(value.get("credential_ref"), str)
        and 1 <= len(value["credential_ref"]) <= 128
        and NETWORK_CREDENTIAL_REF.fullmatch(value["credential_ref"]) is not None
        and valid_datetime(issued_at)
        and valid_datetime(expires_at)
        and type(value.get("ttl_seconds")) is int
        and 1 <= value["ttl_seconds"] <= 120
        and value.get("single_use") is True
    ):
        return False
    issued = parse_rfc3339(issued_at)
    expires = parse_rfc3339(expires_at)
    return (expires - issued).total_seconds() == value["ttl_seconds"]


def valid_network_provider_error(error: ProviderError, *, operation: str) -> bool:
    if not isinstance(error.message, str) or not 1 <= len(error.message) <= 1024:
        return False
    details = error.details
    read_errors = {
        "status": ("network_status_unavailable", "网络状态暂时不可用"),
        "scan": ("network_scan_unavailable", "无线网络扫描暂时不可用"),
    }
    expected_read_error = read_errors.get(operation)
    if expected_read_error is not None:
        return (
            (error.code, error.message) == expected_read_error
            and error.status == HTTPStatus.SERVICE_UNAVAILABLE
            and error.retryable is True
            and details is None
        )
    if error.code == "network_mutation_unavailable":
        return (
            error.status == HTTPStatus.SERVICE_UNAVAILABLE
            and error.retryable is True
            and isinstance(details, Mapping)
            and set(details) == {"reason"}
            and details.get("reason") in NETWORK_MUTATION_DISABLED_REASONS
        )
    if error.code == "idempotency_conflict":
        original_transaction_id = (
            details.get("original_transaction_id") if isinstance(details, Mapping) else None
        )
        return (
            operation in NETWORK_MUTATION_OPERATIONS
            and error.status == HTTPStatus.CONFLICT
            and error.retryable is False
            and isinstance(details, Mapping)
            and set(details) == {"idempotency_scope", "original_transaction_id"}
            and details.get("idempotency_scope") == "network-mutation"
            and (original_transaction_id is None or valid_uuid7(original_transaction_id))
        )
    if error.code == "invalid_network_desired_state":
        return (
            operation in {"apply", "retry"}
            and error.status == HTTPStatus.UNPROCESSABLE_ENTITY
            and error.retryable is False
            and isinstance(details, Mapping)
            and set(details) == {"field", "reason"}
            and isinstance(details.get("field"), str)
            and 1 <= len(details["field"]) <= 128
            and details.get("reason") in INVALID_NETWORK_DESIRED_STATE_REASONS
        )
    if error.code == "network_transaction_not_found":
        transaction_id = details.get("transaction_id") if isinstance(details, Mapping) else None
        return (
            operation == "retry"
            and error.status == HTTPStatus.NOT_FOUND
            and error.retryable is False
            and isinstance(details, Mapping)
            and set(details) == {"transaction_id"}
            and valid_uuid7(transaction_id)
        )
    return (
        operation == "credentials"
        and error.code == "invalid_request"
        and error.status == HTTPStatus.BAD_REQUEST
        and error.retryable is False
    )

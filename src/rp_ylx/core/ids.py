"""Identifier, bearer-token, and strict RFC 3339 timestamp validators."""

from __future__ import annotations

import re
from datetime import UTC, datetime, timedelta, timezone

UUID_V4_RE = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$")
UUID_V7_RE = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$")
SHA256_HEX_RE = re.compile(r"^[0-9a-f]{64}$")
RFC3339_RE = re.compile(
    r"^(?P<year>\d{4})-(?P<month>\d{2})-(?P<day>\d{2})"
    r"[Tt]"
    r"(?P<hour>\d{2}):(?P<minute>\d{2}):(?P<second>\d{2})"
    r"(?:\.(?P<fraction>\d{1,9}))?"
    r"(?P<tz>[Zz]|(?P<tz_sign>[+-])(?P<tz_hour>\d{2}):(?P<tz_minute>\d{2}))\Z"
)

CUSTOMER_BEARER_TOKEN_MIN_LENGTH = 8
CUSTOMER_BEARER_TOKEN_MAX_LENGTH = 512

__all__ = [
    "CUSTOMER_BEARER_TOKEN_MAX_LENGTH",
    "CUSTOMER_BEARER_TOKEN_MIN_LENGTH",
    "RFC3339_RE",
    "SHA256_HEX_RE",
    "UUID_V4_RE",
    "UUID_V7_RE",
    "parse_rfc3339",
    "valid_customer_bearer_token",
    "valid_sha256",
    "valid_uuid4",
    "valid_uuid7",
]


def valid_uuid4(value: object) -> bool:
    """Return True if value is a strictly lowercase UUIDv4 string."""
    return isinstance(value, str) and UUID_V4_RE.fullmatch(value) is not None


def valid_uuid7(value: object) -> bool:
    """Return True if value is a strictly lowercase UUIDv7 string."""
    return isinstance(value, str) and UUID_V7_RE.fullmatch(value) is not None


def valid_sha256(value: object) -> bool:
    """Return True if value is a 64-character lowercase hexadecimal SHA-256 string."""
    return isinstance(value, str) and SHA256_HEX_RE.fullmatch(value) is not None


def parse_rfc3339(value: object) -> datetime:
    """Strictly parse an RFC 3339 timestamp string into a timezone-aware UTC datetime."""
    if not isinstance(value, str):
        raise ValueError("RFC 3339 timestamp must be a string")
    match = RFC3339_RE.fullmatch(value)
    if match is None:
        raise ValueError("invalid RFC 3339 timestamp format")

    year = int(match.group("year"))
    month = int(match.group("month"))
    day = int(match.group("day"))
    hour = int(match.group("hour"))
    minute = int(match.group("minute"))
    second = int(match.group("second"))
    fraction = match.group("fraction")
    microsecond = int(fraction.ljust(6, "0")[:6]) if fraction is not None else 0

    tz_raw = match.group("tz")
    if tz_raw in {"Z", "z"}:
        tzinfo = UTC
    else:
        tz_hour = int(match.group("tz_hour"))
        tz_minute = int(match.group("tz_minute"))
        if tz_hour > 23 or tz_minute > 59:
            raise ValueError("invalid RFC 3339 timezone offset")
        delta = timedelta(hours=tz_hour, minutes=tz_minute)
        if match.group("tz_sign") == "-":
            delta = -delta
        tzinfo = timezone(delta)

    try:
        parsed = datetime(year, month, day, hour, minute, second, microsecond, tzinfo=tzinfo)
        return parsed.astimezone(UTC)
    except OverflowError as error:
        raise ValueError("RFC 3339 timestamp out of representable UTC range") from error


def valid_customer_bearer_token(value: object) -> bool:
    """Return True if value is a valid customer bearer token (8..512 printable ASCII)."""
    return (
        isinstance(value, str)
        and CUSTOMER_BEARER_TOKEN_MIN_LENGTH <= len(value) <= CUSTOMER_BEARER_TOKEN_MAX_LENGTH
        and all(0x21 <= ord(character) <= 0x7E for character in value)
    )

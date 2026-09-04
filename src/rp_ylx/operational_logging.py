"""Bounded structured operational events for long-running Open Aria services."""

from __future__ import annotations

import json
import logging
import math
import os
import re
import sys
import threading
from collections.abc import Mapping
from datetime import UTC, datetime
from typing import TextIO

from rp_ylx import __commit__, __version__

OPERATIONAL_EVENT_SCHEMA = "ylx.operational-event.v1"
OPERATIONAL_LOGGER_NAME = "rp_ylx.operational"

_NAME_PATTERN = re.compile(r"^[a-z][a-z0-9_-]{0,63}$")
_VALUE_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:/+@-]{0,127}$")
_LEVELS = {
    "debug": logging.DEBUG,
    "info": logging.INFO,
    "warning": logging.WARNING,
    "error": logging.ERROR,
    "critical": logging.CRITICAL,
}
_STRING_CONTEXT_FIELDS = frozenset(
    {
        "data_plane",
        "desired_mode",
        "error_code",
        "exception_type",
        "fallback_error_code",
        "kind",
        "operation",
        "outcome",
        "reason",
        "recovery_action",
        "rescue_error_code",
        "scheme",
        "security_profile",
        "signal",
        "stage",
        "status",
        "transaction_id",
        "transport",
    }
)
_BOOLEAN_CONTEXT_FIELDS = frozenset(
    {
        "active_transaction",
        "ap_validated",
        "new_boot",
        "replayed",
        "retryable",
        "saved",
        "verified",
        "worker_enabled",
    }
)
_INTEGER_CONTEXT_FIELDS = frozenset(
    {
        "address_count",
        "connections_served",
        "exit_code",
        "frames_published",
        "port",
        "source_revision",
        "suppressed_count",
    }
)
_NUMBER_CONTEXT_FIELDS = frozenset(
    {
        "duration_ms",
        "grace_seconds",
        "poll_seconds",
        "ttl_seconds",
    }
)

_root_logger = logging.getLogger(OPERATIONAL_LOGGER_NAME)
_root_logger.addHandler(logging.NullHandler())
_root_logger.setLevel(logging.DEBUG)
_root_logger.propagate = False
_configuration_lock = threading.Lock()


def _timestamp(created: float) -> str:
    return (
        datetime.fromtimestamp(created, UTC)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z")
    )


def _safe_build_value(value: object) -> str:
    rendered = str(value)
    return rendered if _VALUE_PATTERN.fullmatch(rendered) else "unknown"


def _safe_context(value: object) -> dict[str, object]:
    if not isinstance(value, Mapping):
        return {"redacted_field_count": 1}
    result: dict[str, object] = {}
    redacted = 0
    for key, item in value.items():
        if not isinstance(key, str):
            redacted += 1
            continue
        if key in _STRING_CONTEXT_FIELDS:
            if isinstance(item, str) and _VALUE_PATTERN.fullmatch(item):
                result[key] = item
            else:
                redacted += 1
            continue
        if key in _BOOLEAN_CONTEXT_FIELDS:
            if type(item) is bool:
                result[key] = item
            else:
                redacted += 1
            continue
        if key in _INTEGER_CONTEXT_FIELDS:
            if type(item) is int and item >= 0:
                result[key] = item
            else:
                redacted += 1
            continue
        if key in _NUMBER_CONTEXT_FIELDS:
            if type(item) in {int, float} and math.isfinite(item) and item >= 0:
                result[key] = item
            else:
                redacted += 1
            continue
        redacted += 1
    if redacted:
        result["redacted_field_count"] = redacted
    return result


class OperationalJsonFormatter(logging.Formatter):
    """Render one closed, secret-resistant JSON object per log record."""

    def format(self, record: logging.LogRecord) -> str:
        component = getattr(record, "operational_component", "runtime")
        event = getattr(record, "operational_event", "unstructured_log_rejected")
        if not isinstance(component, str) or _NAME_PATTERN.fullmatch(component) is None:
            component = "runtime"
        if not isinstance(event, str) or _NAME_PATTERN.fullmatch(event) is None:
            event = "operational_event_rejected"
        level = record.levelname.casefold()
        if level not in _LEVELS:
            level = "info"
        payload = {
            "schema": OPERATIONAL_EVENT_SCHEMA,
            "timestamp": _timestamp(record.created),
            "level": level,
            "component": component,
            "event": event,
            "version": _safe_build_value(__version__),
            "commit": _safe_build_value(__commit__),
            "context": _safe_context(getattr(record, "operational_context", None)),
        }
        return json.dumps(
            payload,
            ensure_ascii=True,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        )


class OperationalLogger:
    """Small typed boundary that prevents arbitrary messages from reaching the log."""

    def __init__(self, component: str) -> None:
        if _NAME_PATTERN.fullmatch(component) is None:
            raise ValueError("operational log component is invalid")
        self.component = component
        self._logger = logging.getLogger(f"{OPERATIONAL_LOGGER_NAME}.{component}")

    def event(self, event: str, *, level: str = "info", **context: object) -> None:
        if _NAME_PATTERN.fullmatch(event) is None:
            raise ValueError("operational log event is invalid")
        normalized_level = level.casefold()
        if normalized_level not in _LEVELS:
            raise ValueError("operational log level is invalid")
        self._logger.log(
            _LEVELS[normalized_level],
            event,
            extra={
                "operational_component": self.component,
                "operational_event": event,
                "operational_context": context,
            },
        )


def operational_logger(component: str) -> OperationalLogger:
    return OperationalLogger(component)


def configure_operational_logging(
    *,
    stream: TextIO | None = None,
    level: str | None = None,
) -> str:
    """Configure the process-wide operational stream and return its active level."""

    requested = (
        level
        if level is not None
        else os.environ.get("OPENARIA_LOG_LEVEL", os.environ.get("RP_YLX_LOG_LEVEL", "info"))
    )
    normalized = requested.casefold()
    invalid_level = normalized not in _LEVELS
    if invalid_level:
        normalized = "info"
    handler = logging.StreamHandler(stream if stream is not None else sys.stderr)
    handler.setFormatter(OperationalJsonFormatter())
    handler.setLevel(_LEVELS[normalized])
    with _configuration_lock:
        for existing in tuple(_root_logger.handlers):
            _root_logger.removeHandler(existing)
            existing.close()
        _root_logger.addHandler(handler)
        _root_logger.setLevel(_LEVELS[normalized])
        _root_logger.propagate = False
    if invalid_level:
        operational_logger("runtime").event(
            "log_level_defaulted",
            level="warning",
            error_code="invalid_log_level",
        )
    return normalized


def reset_operational_logging() -> None:
    """Restore the library-safe null handler after an embedded CLI invocation."""

    with _configuration_lock:
        for existing in tuple(_root_logger.handlers):
            _root_logger.removeHandler(existing)
            existing.close()
        _root_logger.addHandler(logging.NullHandler())
        _root_logger.setLevel(logging.DEBUG)
        _root_logger.propagate = False


__all__ = [
    "OPERATIONAL_EVENT_SCHEMA",
    "OperationalJsonFormatter",
    "OperationalLogger",
    "configure_operational_logging",
    "operational_logger",
    "reset_operational_logging",
]

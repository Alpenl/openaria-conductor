"""Device API v2/v3/v4 的共享 HTTP gateway。"""

from __future__ import annotations

import time

from rp_ylx.api.controllers.device import WEB_CONTENT_SECURITY_POLICY
from rp_ylx.api.controllers.sessions import QUOTED_SHA256
from rp_ylx.api.controllers.types import (
    CaptureCommand,
    CaptureCommandResult,
    DeviceProvider,
    LockedRepresentation,
    NetworkCommand,
    NetworkCommandResult,
    NetworkCredentialCommand,
    PreviewResponse,
    ProviderError,
)
from rp_ylx.api.http_server import (
    _RESPONSE_WRITE_CHUNK,
    GatewayHandler,
    GatewayServer,
    _BoundedResponseWriter,
    _IODeadline,
    create_gateway_server,
)
from rp_ylx.api.middleware import HTTP_CONTENT_LENGTH, MAX_BODY_BYTES
from rp_ylx.api.router import (
    ALL_DECLARED_OPENAPI_ROUTES,
    CAMERA_FOCUS_API_VERSIONS,
    NETWORK_API_VERSIONS,
    SUPPORTED_API_VERSIONS,
    WEB_PATHS,
)
from rp_ylx.api.streams import _NetworkEventReplayBuffer, _StreamRegistry, _StreamSession
from rp_ylx.api.validators.commands import valid_session_id as _valid_session_id
from rp_ylx.api.validators.events import UUID_V4, UUID_V7
from rp_ylx.api.validators.network import (
    INVALID_NETWORK_DESIRED_STATE_REASONS,
    MDNS_TOKEN,
    NETWORK_INTERFACE_STATES,
    NETWORK_MUTATION_DISABLED_REASONS,
    NETWORK_MUTATION_OPERATIONS,
    NETWORK_RECOVERY_ACTIONS,
    NETWORK_STAGE_STATUSES,
    NETWORK_TOKEN,
    NETWORK_TRANSACTION_ERROR_CODES,
    NETWORK_TRANSACTION_STAGES,
    NETWORK_TRANSACTION_STATUSES,
)
from rp_ylx.firmware_control import request_firmware
from rp_ylx.operational_logging import operational_logger
from rp_ylx.web import assert_compatible_web_device_api

assert_compatible_web_device_api(SUPPORTED_API_VERSIONS)

__all__ = [
    "ALL_DECLARED_OPENAPI_ROUTES",
    "CAMERA_FOCUS_API_VERSIONS",
    "CaptureCommand",
    "CaptureCommandResult",
    "DeviceProvider",
    "GatewayHandler",
    "GatewayServer",
    "HTTP_CONTENT_LENGTH",
    "INVALID_NETWORK_DESIRED_STATE_REASONS",
    "LockedRepresentation",
    "MAX_BODY_BYTES",
    "MDNS_TOKEN",
    "NETWORK_API_VERSIONS",
    "NETWORK_INTERFACE_STATES",
    "NETWORK_MUTATION_DISABLED_REASONS",
    "NETWORK_MUTATION_OPERATIONS",
    "NETWORK_RECOVERY_ACTIONS",
    "NETWORK_STAGE_STATUSES",
    "NETWORK_TOKEN",
    "NETWORK_TRANSACTION_ERROR_CODES",
    "NETWORK_TRANSACTION_STAGES",
    "NETWORK_TRANSACTION_STATUSES",
    "NetworkCommand",
    "NetworkCommandResult",
    "NetworkCredentialCommand",
    "PreviewResponse",
    "ProviderError",
    "QUOTED_SHA256",
    "SUPPORTED_API_VERSIONS",
    "UUID_V4",
    "UUID_V7",
    "WEB_CONTENT_SECURITY_POLICY",
    "WEB_PATHS",
    "_BoundedResponseWriter",
    "_IODeadline",
    "_NetworkEventReplayBuffer",
    "_RESPONSE_WRITE_CHUNK",
    "_StreamRegistry",
    "_StreamSession",
    "_valid_session_id",
    "create_gateway_server",
    "operational_logger",
    "request_firmware",
    "time",
]

"""Single declarative HTTP route table for Device API v2/v3/v4 and web assets."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING

from rp_ylx.api.controllers import capture, device, network, sessions
from rp_ylx.api.validators.commands import valid_session_id
from rp_ylx.api.validators.events import SUPPORTED_DEVICE_API_VERSIONS
from rp_ylx.api.validators.network import NETWORK_MUTATION_OPERATIONS
from rp_ylx.core.ids import valid_sha256
from rp_ylx.web import WEB_ASSETS

if TYPE_CHECKING:
    from rp_ylx.api.http_server import GatewayHandler

SUPPORTED_API_VERSIONS = SUPPORTED_DEVICE_API_VERSIONS
CAMERA_FOCUS_API_VERSIONS = frozenset({"v4"})
NETWORK_API_VERSIONS = frozenset({"v4"})
WEB_PATHS = {"/": "index.html", **{f"/{name}": name for name in WEB_ASSETS if name != "index.html"}}


@dataclass(frozen=True, slots=True)
class RouteSpec:
    """Declarative route specification driving GET, HEAD, POST, OPTIONS, and 405."""

    api_version: str | None
    path_template: str
    methods: tuple[str, ...]
    operation_ids: Mapping[str, str]
    allowed_headers_by_method: Mapping[str, tuple[str, ...]]
    handler: Callable[[GatewayHandler, str], None]
    not_found_methods: frozenset[str] = frozenset()

    def allowed_headers(self, method: str) -> tuple[str, ...]:
        return self.allowed_headers_by_method.get(method, ("Authorization",))


@dataclass(frozen=True, slots=True)
class RouteMatch:
    spec: RouteSpec
    handler: Callable[[GatewayHandler, str], None]


_SSE_HEADERS = ("Authorization", "Last-Event-ID")
_ARTIFACT_HEADERS = ("Authorization", "If-Range", "Range")
_POST_COMMAND_HEADERS = ("Authorization", "Content-Type", "X-CSRF-Token", "Idempotency-Key")
_POST_NO_IDEMPOTENCY_HEADERS = ("Authorization", "Content-Type", "X-CSRF-Token")
_GET_HEADERS = ("Authorization",)


def _api_routes() -> tuple[RouteSpec, ...]:
    specs: list[RouteSpec] = []
    for version in sorted(SUPPORTED_API_VERSIONS):
        specs.extend(
            (
                RouteSpec(
                    version,
                    "/device",
                    ("GET", "OPTIONS"),
                    {"GET": "getDevice"},
                    {"GET": _GET_HEADERS},
                    lambda h, _m, v=version: device.get_device(h, v),
                ),
                RouteSpec(
                    version,
                    "/preview",
                    ("GET", "OPTIONS"),
                    {"GET": "getPreview"},
                    {"GET": _GET_HEADERS},
                    lambda h, _m: capture.get_preview(h, h.query_params()),
                ),
                RouteSpec(
                    version,
                    "/capture/status",
                    ("GET", "OPTIONS"),
                    {"GET": "getCaptureStatus"},
                    {"GET": _GET_HEADERS},
                    lambda h, _m, v=version: capture.get_capture_status(h, v),
                ),
                RouteSpec(
                    version,
                    "/capture/events",
                    ("GET", "OPTIONS"),
                    {"GET": "streamCaptureEvents"},
                    {"GET": _SSE_HEADERS},
                    lambda h, _m, v=version: capture.capture_events(h, v, h.query_params()),
                ),
                RouteSpec(
                    version,
                    "/capture/safe-swap",
                    ("GET", "OPTIONS"),
                    {"GET": "getCurrentSafeSwapReceipt"},
                    {"GET": _GET_HEADERS},
                    lambda h, _m, v=version: capture.get_safe_swap(h, v),
                ),
                RouteSpec(
                    version,
                    "/capture/start",
                    ("POST", "OPTIONS"),
                    {"POST": "startCapture"},
                    {"POST": _POST_COMMAND_HEADERS},
                    lambda h, _m, v=version: capture.capture_command(h, v, "start"),
                ),
                RouteSpec(
                    version,
                    "/capture/stop",
                    ("POST", "OPTIONS"),
                    {"POST": "stopCapture"},
                    {"POST": _POST_COMMAND_HEADERS},
                    lambda h, _m, v=version: capture.capture_command(h, v, "stop"),
                ),
                RouteSpec(
                    version,
                    "/sessions",
                    ("GET", "OPTIONS"),
                    {"GET": "listSessions"},
                    {"GET": _GET_HEADERS},
                    lambda h, _m, v=version: sessions.list_sessions(h, v, h.query_params()),
                ),
            )
        )
    # v4-only endpoints
    specs.extend(
        (
            RouteSpec(
                "v4",
                "/firmware",
                ("GET", "OPTIONS"),
                {"GET": "firmwareStatus"},
                {"GET": _GET_HEADERS},
                lambda h, _m: device.firmware_request(h, "status"),
            ),
            RouteSpec(
                "v4",
                "/firmware/check",
                ("GET", "OPTIONS"),
                {"GET": "checkFirmware"},
                {"GET": _GET_HEADERS},
                lambda h, _m: device.firmware_request(h, "check"),
            ),
            RouteSpec(
                "v4",
                "/firmware/update",
                ("POST", "OPTIONS"),
                {"POST": "firmwareUpdate"},
                {"POST": _POST_COMMAND_HEADERS},
                lambda h, _m: device.firmware_request(h, "update"),
            ),
            RouteSpec(
                "v4",
                "/firmware/rollback",
                ("POST", "OPTIONS"),
                {"POST": "rollbackFirmware"},
                {"POST": _POST_COMMAND_HEADERS},
                lambda h, _m: device.firmware_request(h, "rollback"),
            ),
            RouteSpec(
                "v4",
                "/voice",
                ("GET", "POST", "OPTIONS"),
                {"GET": "getVoice", "POST": "setVoiceVolume"},
                {"GET": _GET_HEADERS, "POST": _POST_NO_IDEMPOTENCY_HEADERS},
                lambda h, m: device.voice_request(h, write=(m == "POST")),
            ),
            RouteSpec(
                "v4",
                "/capture/readiness",
                ("GET", "OPTIONS"),
                {"GET": "getCaptureReadiness"},
                {"GET": _GET_HEADERS},
                lambda h, _m: capture.capture_readiness_request(h),
            ),
            RouteSpec(
                "v4",
                "/capture/timer",
                ("GET", "POST", "OPTIONS"),
                {"GET": "getCaptureTimer", "POST": "setCaptureTimer"},
                {"GET": _GET_HEADERS, "POST": _POST_NO_IDEMPOTENCY_HEADERS},
                lambda h, m: capture.capture_timer_request(h, write=(m == "POST")),
            ),
            RouteSpec(
                "v4",
                "/clock",
                ("GET", "OPTIONS"),
                {"GET": "getDeviceClock"},
                {"GET": _GET_HEADERS},
                lambda h, _m: device.clock_request(h, sync=False, extended=False),
            ),
            RouteSpec(
                "v4",
                "/clock/check",
                ("GET", "OPTIONS"),
                {"GET": "inspectDeviceClock"},
                {"GET": _GET_HEADERS},
                lambda h, _m: device.clock_request(h, sync=False, extended=True),
            ),
            RouteSpec(
                "v4",
                "/clock/sync",
                ("POST", "OPTIONS"),
                {"POST": "syncDeviceClock"},
                {"POST": _POST_NO_IDEMPOTENCY_HEADERS},
                lambda h, _m: device.clock_request(h, sync=True, extended=False),
            ),
            RouteSpec(
                "v4",
                "/camera/focus",
                ("GET", "POST", "OPTIONS"),
                {"GET": "getCameraFocus", "POST": "setCameraFocus"},
                {"GET": _GET_HEADERS, "POST": _POST_COMMAND_HEADERS},
                lambda h, m: (
                    capture.camera_focus_command(h) if m == "POST" else capture.get_camera_focus(h)
                ),
            ),
            RouteSpec(
                "v4",
                "/sessions/delete",
                ("POST", "OPTIONS"),
                {"POST": "deleteSessions"},
                {"POST": _POST_COMMAND_HEADERS},
                lambda h, _m: sessions.delete_sessions(h),
            ),
            RouteSpec(
                "v4",
                "/network",
                ("GET", "OPTIONS"),
                {"GET": "getNetworkStatus"},
                {"GET": _GET_HEADERS},
                lambda h, _m: network.get_network_status(h),
                # Preserve existing contract test expecting 404 on POST /api/v4/network.
                not_found_methods=frozenset({"POST"}),
            ),
            RouteSpec(
                "v4",
                "/network/scan",
                ("GET", "OPTIONS"),
                {"GET": "scanNetworks"},
                {"GET": _GET_HEADERS},
                lambda h, _m: network.get_network_scan(h),
            ),
            RouteSpec(
                "v4",
                "/network/events",
                ("GET", "OPTIONS"),
                {"GET": "streamNetworkEvents"},
                {"GET": _SSE_HEADERS},
                lambda h, _m: network.network_events(h),
            ),
            RouteSpec(
                "v4",
                "/network/credentials",
                ("POST", "OPTIONS"),
                {"POST": "createNetworkCredentialReference"},
                {"POST": _POST_NO_IDEMPOTENCY_HEADERS},
                lambda h, _m: network.create_network_credential(h),
            ),
        )
    )
    network_op_ids = {
        "apply": "applyNetworkDesiredState",
        "retry": "retryNetworkTransaction",
        "forget": "forgetNetworkClientProfile",
    }
    for op in NETWORK_MUTATION_OPERATIONS:
        specs.append(
            RouteSpec(
                "v4",
                f"/network/{op}",
                ("POST", "OPTIONS"),
                {"POST": network_op_ids[op]},
                {"POST": _POST_COMMAND_HEADERS},
                lambda h, _m, operation=op: network.network_command(h, operation),
            )
        )
    return tuple(specs)


def _parametric_routes() -> tuple[RouteSpec, ...]:
    return (
        RouteSpec(
            None,
            "/sessions/{session_id}",
            ("GET", "OPTIONS"),
            {"GET": "getSession"},
            {"GET": _GET_HEADERS},
            lambda h, _m: None,
        ),
        RouteSpec(
            None,
            "/sessions/{session_id}/unsuccessful-outcome",
            ("GET", "OPTIONS"),
            {"GET": "getRetainedUnsuccessfulSessionOutcome"},
            {"GET": _GET_HEADERS},
            lambda h, _m: None,
        ),
        RouteSpec(
            None,
            "/sessions/{session_id}/artifacts/{artifact_id}",
            ("GET", "HEAD", "OPTIONS"),
            {"GET": "getSessionArtifact", "HEAD": "headSessionArtifact"},
            {"GET": _ARTIFACT_HEADERS, "HEAD": _ARTIFACT_HEADERS},
            lambda h, _m: None,
        ),
    )


STATIC_API_ROUTES: Mapping[str, RouteSpec] = {
    f"/api/{spec.api_version}{spec.path_template}": spec for spec in _api_routes()
}

ALL_DECLARED_OPENAPI_ROUTES: tuple[tuple[str, str, str, str], ...] = tuple(
    (
        version,
        spec.path_template,
        method.lower(),
        op_id,
    )
    for version in sorted(SUPPORTED_API_VERSIONS)
    for spec in (*_api_routes(), *_parametric_routes())
    if spec.api_version in (None, version)
    for method, op_id in spec.operation_ids.items()
)


def match_route(path: str) -> RouteMatch | None:
    """Resolve an HTTP path into a declarative RouteMatch using the shared route table."""
    web_asset = WEB_PATHS.get(path)
    if web_asset is not None:
        spec = RouteSpec(
            None,
            path,
            ("GET", "HEAD", "OPTIONS"),
            {},
            {"GET": _GET_HEADERS, "HEAD": _GET_HEADERS},
            lambda h, m, asset=web_asset: device.send_web_asset(h, asset, head=(m == "HEAD")),
        )
        return RouteMatch(spec=spec, handler=spec.handler)

    static_spec = STATIC_API_ROUTES.get(path)
    if static_spec is not None:
        return RouteMatch(spec=static_spec, handler=static_spec.handler)

    parts = path.split("/")
    if len(parts) < 5 or parts[1] != "api" or parts[2] not in SUPPORTED_API_VERSIONS:
        return None
    version = parts[2]
    if parts[3] != "sessions":
        return None
    session_id = parts[4]
    if not valid_session_id(version, session_id):
        return None

    if len(parts) == 5:
        spec = RouteSpec(
            version,
            "/sessions/{session_id}",
            ("GET", "OPTIONS"),
            {"GET": "getSession"},
            {"GET": _GET_HEADERS},
            lambda h, _m, v=version, sid=session_id: sessions.get_manifest(h, v, sid),
        )
        return RouteMatch(spec=spec, handler=spec.handler)

    if len(parts) == 6 and parts[5] == "unsuccessful-outcome":
        spec = RouteSpec(
            version,
            "/sessions/{session_id}/unsuccessful-outcome",
            ("GET", "OPTIONS"),
            {"GET": "getRetainedUnsuccessfulSessionOutcome"},
            {"GET": _GET_HEADERS},
            lambda h, _m, sid=session_id: sessions.get_retained_outcome(h, sid),
        )
        return RouteMatch(spec=spec, handler=spec.handler)

    if len(parts) == 7 and parts[5] == "artifacts" and valid_sha256(parts[6]):
        artifact_id = parts[6]
        spec = RouteSpec(
            version,
            "/sessions/{session_id}/artifacts/{artifact_id}",
            ("GET", "HEAD", "OPTIONS"),
            {"GET": "getSessionArtifact", "HEAD": "headSessionArtifact"},
            {"GET": _ARTIFACT_HEADERS, "HEAD": _ARTIFACT_HEADERS},
            lambda h, m, v=version, sid=session_id, aid=artifact_id: sessions.session_artifact(
                h, v, sid, aid, head=(m == "HEAD")
            ),
        )
        return RouteMatch(spec=spec, handler=spec.handler)

    return None

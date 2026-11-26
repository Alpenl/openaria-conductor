"""Shared command types, representations, and DeviceProvider Protocol."""

from __future__ import annotations

from collections.abc import Callable, Iterator, Mapping
from dataclasses import dataclass
from typing import Protocol

from rp_ylx.errors import ProviderError


@dataclass(frozen=True, slots=True)
class CaptureCommand:
    principal_id: str
    idempotency_key: str
    body: Mapping[str, object]
    canonical_body: bytes


@dataclass(frozen=True, slots=True)
class CaptureCommandResult:
    status: int
    body: object | None
    replayed: bool = False


@dataclass(frozen=True, slots=True)
class NetworkCommand:
    principal_id: str
    idempotency_key: str
    body: Mapping[str, object]
    canonical_body: bytes


@dataclass(frozen=True, slots=True)
class NetworkCommandResult:
    status: int
    body: object | None
    replayed: bool = False


@dataclass(frozen=True, slots=True)
class NetworkCredentialCommand:
    principal_id: str
    passphrase: str


class LockedRepresentation(Protocol):
    etag: str
    size: int
    content_type: str

    def __enter__(self) -> LockedRepresentation: ...

    def __exit__(self, *args: object) -> None: ...

    def iter_chunks(
        self,
        offset: int = 0,
        length: int | None = None,
        *,
        chunk_size: int = 1024 * 1024,
    ) -> Iterator[bytes]: ...

    def send_to(
        self, output_descriptor: int, offset: int = 0, length: int | None = None
    ) -> int: ...


class PreviewResponse(Protocol):
    content_type: str
    body: bytes | Iterator[bytes]
    content_length: int | None


class DeviceProvider(Protocol):
    """Narrow device daemon boundary for HTTP gateway controllers."""

    def device_descriptor(
        self, api_version: str, security_profile: str
    ) -> Mapping[str, object]: ...

    def capture_status(self) -> Mapping[str, object]: ...

    def start_capture(self, command: CaptureCommand) -> CaptureCommandResult: ...

    def stop_capture(self, command: CaptureCommand) -> CaptureCommandResult: ...

    def request_stop_capture(self, command: CaptureCommand) -> CaptureCommandResult: ...

    def camera_focus_status(self) -> Mapping[str, object] | None: ...

    def set_camera_focus(self, command: CaptureCommand) -> CaptureCommandResult: ...

    def network_status(self) -> Mapping[str, object]: ...

    def clock_status(self, principal_id: str) -> Mapping[str, object]: ...

    def clock_inspect(self, principal_id: str) -> Mapping[str, object]: ...

    def sync_clock(self, principal_id: str, body: Mapping[str, object]) -> Mapping[str, object]: ...

    def voice_status(self) -> Mapping[str, object]: ...

    def set_voice_volume(self, volume: int) -> Mapping[str, object]: ...

    def capture_timer_status(self) -> Mapping[str, object]: ...

    def set_capture_timer(self, settings: Mapping[str, object]) -> Mapping[str, object]: ...

    def capture_readiness(self) -> Mapping[str, object]: ...

    def scan_networks(self) -> Mapping[str, object]: ...

    def create_network_credential(
        self, command: NetworkCredentialCommand
    ) -> Mapping[str, object]: ...

    def apply_network_desired_state(self, command: NetworkCommand) -> NetworkCommandResult: ...

    def retry_network_transaction(self, command: NetworkCommand) -> NetworkCommandResult: ...

    def forget_network_client_profile(self, command: NetworkCommand) -> NetworkCommandResult: ...

    def list_sessions(
        self,
        *,
        cursor: str | None,
        limit: int,
        take_id: str | None,
        api_version: str,
        include_recovery: bool = False,
    ) -> Mapping[str, object]: ...

    def open_manifest(self, session_id: str, api_version: str) -> LockedRepresentation: ...

    def delete_sessions(self, command: CaptureCommand) -> CaptureCommandResult: ...

    def retained_unsuccessful_outcome(self, session_id: str) -> object | None: ...

    def current_safe_swap_receipt(self) -> object | None: ...

    def capture_snapshot_event(self) -> Mapping[str, object]: ...

    def latest_preview(self, *, fps: int | None, accept: str) -> PreviewResponse: ...

    def artifact_io_state(self) -> str | None: ...

    def open_verified_artifact(
        self, session_id: str, artifact_id: str, api_version: str
    ) -> LockedRepresentation: ...

    def open_cancellable_artifact(
        self,
        session_id: str,
        artifact_id: str,
        api_version: str,
        *,
        cancelled: Callable[[], bool],
    ) -> LockedRepresentation: ...

    def firmware_status(self) -> Mapping[str, object]: ...

    def firmware_check(self, *, force: bool = False) -> Mapping[str, object]: ...

    def firmware_update(self, *, key: str, commit: str) -> Mapping[str, object]: ...

    def firmware_rollback(self, *, key: str, commit: str) -> Mapping[str, object]: ...


__all__ = [
    "CaptureCommand",
    "CaptureCommandResult",
    "DeviceProvider",
    "LockedRepresentation",
    "NetworkCommand",
    "NetworkCommandResult",
    "NetworkCredentialCommand",
    "PreviewResponse",
    "ProviderError",
]

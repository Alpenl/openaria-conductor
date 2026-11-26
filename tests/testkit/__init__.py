"""Shared test doubles, fixture builders, and harnesses for Open Aria Conductor tests."""

from __future__ import annotations

import io
import os
import threading
from collections.abc import Callable
from contextlib import ExitStack
from typing import Self


class Cancelled(BaseException):
    """Sentinel BaseException simulating interrupted operations across Exception handlers."""


class DelayedLock:
    """Context-manager lock that pauses one named thread before acquiring."""

    def __init__(
        self,
        thread_name: str,
        *,
        entered: threading.Event | None = None,
        release: threading.Event | None = None,
        timeout: float = 3.0,
        lock: threading.Lock | None = None,
        message: str = "delayed lock was not released",
    ) -> None:
        self.thread_name = thread_name
        self.entered = entered or threading.Event()
        self.release = release or threading.Event()
        self.timeout = timeout
        self._lock = lock or threading.Lock()
        self._message = message

    def __enter__(self) -> Self:
        if threading.current_thread().name == self.thread_name:
            self.entered.set()
            if not self.release.wait(self.timeout):
                raise TimeoutError(self._message)
        self._lock.acquire()
        return self

    def __exit__(self, *args: object) -> None:
        self._lock.release()


class ShortOutput:
    """Stream wrapper (or StringIO fallback) whose write() drops the final byte/character."""

    def __init__(
        self,
        stream: object | None = None,
        *,
        fail_on_write: int | None = None,
        fault: str = "short_write",
    ) -> None:
        self.stream: object = io.StringIO() if stream is None else stream
        self._fail_on_write = fail_on_write
        self._fault = fault
        self.writes = 0

    @classmethod
    def fdopen(
        cls,
        descriptor: int,
        mode: str,
        *,
        opener: Callable[[int, str], object] = os.fdopen,
    ) -> ShortOutput:
        return cls(opener(descriptor, mode))

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *args: object) -> None:
        close = getattr(self.stream, "close", None)
        if callable(close):
            close()

    def __getattr__(self, name: str) -> object:
        return getattr(self.stream, name)

    def write(self, payload: bytes | str) -> int:
        self.writes += 1
        if self._fail_on_write is not None:
            if self.writes == self._fail_on_write:
                if self._fault == "write":
                    raise OSError("no space")
                return self.stream.write(payload[:-1])  # type: ignore[attr-defined,no-any-return]
            return self.stream.write(payload)  # type: ignore[attr-defined,no-any-return]
        return self.stream.write(payload[:-1])  # type: ignore[attr-defined,no-any-return]


class ManagedHarness:
    """Lifecycle base class backed by an ExitStack for setUp/tearDown and context managers."""

    def __init__(self) -> None:
        self._stack: ExitStack | None = None

    def setUp(self) -> Self:
        if self._stack is not None:
            return self
        stack = ExitStack()
        try:
            self._stack = stack
            self._configure(stack)
        except BaseException:
            self._stack = None
            stack.close()
            raise
        return self

    def tearDown(self) -> None:
        stack = self._stack
        self._stack = None
        if stack is not None:
            stack.close()

    def doCleanups(self) -> None:
        self.tearDown()

    def _configure(self, stack: ExitStack) -> None:
        raise NotImplementedError

    def __enter__(self) -> Self:
        return self.setUp()

    def __exit__(self, *args: object) -> None:
        self.tearDown()


from .coordinator import (  # noqa: E402
    COMMIT,
    CoordinatorHarness,
    CoordinatorMixin,
    FocusSources,
    check_started_worker_cancellation,
    check_worker_initialization_failures,
    command,
    deletion_command,
    focus_command,
    idle_network_control,
    network_command,
    start_command,
    stop_command,
)
from .deployment import (  # noqa: E402
    DeploymentHarness,
    DeploymentMixin,
    factory_profile,
    vendor_mbr,
)
from .firmware import FIRMWARE_STATUS, FirmwareHarness, FirmwareMixin, firmware_task  # noqa: E402
from .gateway import (  # noqa: E402
    ARTIFACT_BYTES,
    ARTIFACT_ID,
    AUTHORITY_EPOCH,
    CAPTURE_STATUS,
    DEVICE_V3,
    MANIFEST_BYTES,
    MANIFEST_DIGEST,
    NETWORK_CREDENTIAL_RECEIPT,
    NETWORK_RECEIPT,
    NETWORK_SCAN,
    NETWORK_STATUS,
    NETWORK_TRANSACTION_ID,
    NEXT_SESSION_ID,
    PREVIEW_JPEG,
    PREVIEW_PART,
    RAW_LIVE_IMU,
    RETAINED_OUTCOME,
    SAFE_SWAP_V2,
    SAFE_SWAP_V3,
    SESSION_ID,
    SESSION_LIST,
    SESSION_LIST_V4,
    SNAPSHOT_EVENT,
    ArtifactDescriptor,
    DeviceProvider,
    DirectoryArtifactProvider,
    FaultingManifestRepresentation,
    GatewayHarness,
    GatewayMixin,
    LifecycleProvider,
    MemoryArtifact,
    MemoryArtifactProvider,
    MemoryRepresentation,
    active_capture_status,
)
from .mdns import FakeResponder  # noqa: E402
from .media import (  # noqa: E402
    CAMERA_FOCUS_STATUS,
    FRAME,
    JPEG,
    FakeAudioRecorder,
    FakeMmap,
    FakeSources,
    FakeSourcesWithLatestImu,
    FakeSourcesWithScriptedLatestImu,
    FakeSourcesWithSequentialLatestImu,
    FakeSplitEyeEncoder,
    FakeStereoEncoder,
    FakeV4L2Device,
    audio_fixture,
    frame,
    imu_observation,
    single_imu_observation,
    split_eye_frame,
)
from .network import FakeNmcli, NetworkHarness, NetworkMixin, glib_keyfile_string  # noqa: E402
from .recording import (  # noqa: E402
    FakeNativeSessionStore,
    FakeNativeSessionStoreIo,
    FakeSessionStore,
    FakeSessionTransaction,
    FaultingBinaryStream,
    RecordingFaultInjection,
    RecoveryFixtureBuilder,
    SplitEyeHarness,
    audio_inventory_recorder,
    audio_sealing,
    compact_audio_stub,
    make_audio_result,
    make_multiple_audio_result,
    path_open_fault,
    publish_audio_checkpoint,
)
from .tls import write_ed25519_keypair, write_self_signed_tls  # noqa: E402

__all__ = [
    "ARTIFACT_BYTES",
    "ARTIFACT_ID",
    "AUTHORITY_EPOCH",
    "ArtifactDescriptor",
    "CAMERA_FOCUS_STATUS",
    "CAPTURE_STATUS",
    "COMMIT",
    "Cancelled",
    "CoordinatorHarness",
    "CoordinatorMixin",
    "DEVICE_V3",
    "DelayedLock",
    "DeploymentHarness",
    "DeploymentMixin",
    "DeviceProvider",
    "DirectoryArtifactProvider",
    "FIRMWARE_STATUS",
    "FRAME",
    "FakeAudioRecorder",
    "FakeMmap",
    "FakeNativeSessionStore",
    "FakeNativeSessionStoreIo",
    "FakeNmcli",
    "FakeResponder",
    "FakeSessionStore",
    "FakeSessionTransaction",
    "FakeSources",
    "FakeSourcesWithLatestImu",
    "FakeSourcesWithScriptedLatestImu",
    "FakeSourcesWithSequentialLatestImu",
    "FakeSplitEyeEncoder",
    "FakeStereoEncoder",
    "FakeV4L2Device",
    "FaultingBinaryStream",
    "FaultingManifestRepresentation",
    "FirmwareHarness",
    "FirmwareMixin",
    "FocusSources",
    "GatewayHarness",
    "GatewayMixin",
    "JPEG",
    "LifecycleProvider",
    "MANIFEST_BYTES",
    "MANIFEST_DIGEST",
    "ManagedHarness",
    "MemoryArtifact",
    "MemoryArtifactProvider",
    "MemoryRepresentation",
    "NETWORK_CREDENTIAL_RECEIPT",
    "NETWORK_RECEIPT",
    "NETWORK_SCAN",
    "NETWORK_STATUS",
    "NETWORK_TRANSACTION_ID",
    "NEXT_SESSION_ID",
    "NetworkHarness",
    "NetworkMixin",
    "PREVIEW_JPEG",
    "PREVIEW_PART",
    "RAW_LIVE_IMU",
    "RETAINED_OUTCOME",
    "RecordingFaultInjection",
    "RecoveryFixtureBuilder",
    "SAFE_SWAP_V2",
    "SAFE_SWAP_V3",
    "SESSION_ID",
    "SESSION_LIST",
    "SESSION_LIST_V4",
    "SNAPSHOT_EVENT",
    "ShortOutput",
    "SplitEyeHarness",
    "active_capture_status",
    "audio_fixture",
    "audio_inventory_recorder",
    "audio_sealing",
    "check_started_worker_cancellation",
    "check_worker_initialization_failures",
    "command",
    "compact_audio_stub",
    "deletion_command",
    "factory_profile",
    "firmware_task",
    "focus_command",
    "frame",
    "glib_keyfile_string",
    "idle_network_control",
    "imu_observation",
    "make_audio_result",
    "make_multiple_audio_result",
    "network_command",
    "path_open_fault",
    "publish_audio_checkpoint",
    "single_imu_observation",
    "split_eye_frame",
    "start_command",
    "stop_command",
    "vendor_mbr",
    "write_ed25519_keypair",
    "write_self_signed_tls",
]

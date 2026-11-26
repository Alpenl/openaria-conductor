"""CaptureCoordinator test harness, command builders, and lock/storage helpers."""

from __future__ import annotations

import errno
import fcntl
import hashlib
import json
import os
import tempfile
import threading
import unittest
import uuid
from collections.abc import Iterable
from contextlib import ExitStack
from copy import deepcopy
from pathlib import Path
from unittest.mock import patch

import rp_ylx.recording.coordinator as coordinator_module
from rp_ylx.api import CaptureCommand, NetworkCommand, ProviderError
from rp_ylx.api.events import validate_capture_status, validate_device_descriptor
from rp_ylx.camera import CameraError
from rp_ylx.network_control import NetworkControlClientError
from rp_ylx.recording import (
    CaptureCoordinator,
    CoordinatorConfig,
    DeviceSessionConfig,
    initialize_capture_volume,
    validate_device_session_directory,
)
from rp_ylx.recording_layout import resolve_recording

from . import DelayedLock, ManagedHarness
from .media import (
    CAMERA_FOCUS_STATUS,
    JPEG,
    FakeSources,
    FakeSplitEyeEncoder,
    frame,
    imu_observation,
    single_imu_observation,
)

COMMIT = "a" * 40


def command(key: str, body: dict[str, object]) -> CaptureCommand:
    """Build a CaptureCommand with canonical JSON bytes."""
    canonical = json.dumps(body, sort_keys=True, separators=(",", ":")).encode()
    return CaptureCommand("operator", key, body, canonical)


def network_command(key: str, body: dict[str, object]) -> NetworkCommand:
    """Build a NetworkCommand with canonical JSON bytes."""
    canonical = json.dumps(body, sort_keys=True, separators=(",", ":")).encode()
    return NetworkCommand("operator", key, body, canonical)


def idle_network_control(operation: str, **unused: object) -> dict[str, object]:
    """Stub for rp_ylx.recording.coordinator.request_network_control in coordinator tests."""
    del unused
    if operation == "clock_inspect":
        return {
            "ok": True,
            "operation": operation,
            "status": 200,
            "body": {
                "schema": "ylx.clock-status.v2",
                "source": "client",
                "unix_time_ms": 1794144394000,
                "challenge": "a" * 32,
                "expires_in_ms": 5000,
                "applied": False,
                "boot_id": "a4578322-241e-4b9c-bb54-a860d5a2aa37",
                "synchronized_at_ms": 1794144390000,
                "age_seconds": 4,
            },
        }
    if operation == "status":
        return {
            "ok": True,
            "operation": "status",
            "status": 200,
            "body": {"transaction": {"current": None}},
        }
    raise NetworkControlClientError("controller_unavailable", "network controller is unavailable")


def start_command(
    key: str,
    *,
    mode: str = "production",
    display_name: str = "测试录制",
) -> CaptureCommand:
    """Build a ylx.capture-start.v2 CaptureCommand."""
    return command(
        key,
        {
            "schema": "ylx.capture-start.v2",
            "mode": mode,
            "display_name": display_name,
            "take": {"kind": "new"},
        },
    )


def stop_command(key: str, *, reason: str = "user") -> CaptureCommand:
    """Build a ylx.capture-stop.v2 CaptureCommand."""
    return command(key, {"schema": "ylx.capture-stop.v2", "reason": reason})


def deletion_command(
    coordinator: CaptureCoordinator,
    session_ids: Iterable[str],
    key: str = "delete-one",
) -> CaptureCommand:
    """Build a ylx.session-delete-request.v1 command using live manifest digests."""
    target_ids = set(session_ids)
    items = coordinator.list_sessions(cursor=None, limit=200, take_id=None)["items"]

    def manifest_digest(session_id: str) -> str:
        with coordinator.open_manifest(session_id, "v4") as opened:
            return hashlib.sha256(opened.read()).hexdigest()

    body: dict[str, object] = {
        "schema": "ylx.session-delete-request.v1",
        "sessions": [
            {
                "session_id": item["session_id"],
                "manifest_sha256": manifest_digest(item["session_id"]),
            }
            for item in items
            if item["session_id"] in target_ids
        ],
    }
    return CaptureCommand("bridge", key, body, json.dumps(body, sort_keys=True).encode())


class CoordinatorHarness(ManagedHarness):
    """Temporary volume/state harness with patched split-eye encoder and network control."""

    def _configure(self, stack: ExitStack) -> None:
        self.temporary = stack.enter_context(tempfile.TemporaryDirectory())
        self.root = Path(self.temporary)
        self.mountpoint = self.root / "volume"
        self.state_root = self.root / "state"
        self.network_operation_lock_path = self.root / "network-operation.lock"
        self.maintenance_lock_path = self.root / "maintenance.lock"
        self.mountpoint.mkdir()
        stack.enter_context(
            patch.dict(
                os.environ,
                {
                    "RP_YLX_NETWORK_OPERATION_LOCK_PATH": str(self.network_operation_lock_path),
                    "OPENARIA_MAINTENANCE_LOCK": str(self.maintenance_lock_path),
                },
            )
        )
        self.volume_id = initialize_capture_volume(self.mountpoint)
        self.session_config = DeviceSessionConfig(
            device_id=str(uuid.uuid4()),
            device_label="YLX-12AB34CD",
            hardware_fingerprint="sha256:" + "b" * 64,
            platform="D-Robotics RDK X5 V1.0 + YLX 2UQ2",
            software_version="0.5.0",
            commit=COMMIT,
            width=3840,
            height=1080,
            sensor_fps=60.0,
        )
        stack.enter_context(
            patch(
                "rp_ylx.recording.device_session.StereoEncoderProcess",
                FakeSplitEyeEncoder,
            )
        )
        stack.enter_context(
            patch(
                "rp_ylx.recording.coordinator.request_network_control",
                side_effect=idle_network_control,
            )
        )

    def coordinator(
        self,
        *,
        before_write: object | None = None,
        minimum_available_bytes: int = 0,
        minimum_available_inodes: int = 0,
        checkpoint_interval: float = 1.0,
        queue_capacity: int = 128,
        enqueue_timeout: float = 0.05,
        sources: object | None = None,
    ) -> CaptureCoordinator:
        """Instantiate a CaptureCoordinator bound to this harness's volume and state."""
        return CaptureCoordinator(
            CoordinatorConfig(
                self.mountpoint,
                self.state_root,
                self.session_config,
                minimum_available_bytes=minimum_available_bytes,
                minimum_available_inodes=minimum_available_inodes,
                checkpoint_interval=checkpoint_interval,
                queue_capacity=queue_capacity,
                enqueue_timeout=enqueue_timeout,
            ),
            mount_checker=lambda path: path == self.mountpoint.resolve(),
            sources=sources,  # type: ignore[arg-type]
            before_write=before_write,  # type: ignore[arg-type]
        )

    @staticmethod
    def active_session_id(coordinator: CaptureCoordinator) -> str:
        status = coordinator.capture_status()
        validate_capture_status(status)
        active = status["snapshot"]["active_recording"]
        return str(active["recording_state"]["session_id"])

    def _open_network_lock(self) -> int:
        return os.open(
            self.network_operation_lock_path,
            os.O_RDWR | os.O_CREAT | getattr(os, "O_CLOEXEC", 0),
            0o660,
        )

    def assert_network_operation_lock_available(self) -> None:
        descriptor = self._open_network_lock()
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
            fcntl.flock(descriptor, fcntl.LOCK_UN)
        finally:
            os.close(descriptor)

    def assert_network_operation_lock_held(self) -> None:
        descriptor = self._open_network_lock()
        try:
            try:
                fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except OSError as rejected:
                if rejected.errno not in {errno.EACCES, errno.EAGAIN}:
                    raise AssertionError(
                        f"expected EACCES/EAGAIN for held network lock, got {rejected.errno}"
                    ) from rejected
            else:
                fcntl.flock(descriptor, fcntl.LOCK_UN)
                raise AssertionError("expected network operation lock to be held")
        finally:
            os.close(descriptor)

    def seal_one(
        self,
        coordinator: CaptureCoordinator,
        *,
        prefix: str = "one",
        mode: str = "production",
        reason: str = "user",
    ) -> str:
        """Start, feed one frame, and stop/seal one recording session."""
        coordinator.start_capture(start_command(f"{prefix}-start", mode=mode))
        session_id = self.active_session_id(coordinator)
        if not coordinator.submit_frame(frame()):
            raise AssertionError("coordinator rejected initial frame")
        result = coordinator.stop_capture(stop_command(f"{prefix}-stop", reason=reason))
        expected_status = 204 if reason == "safe_swap" else 202
        if result.status != expected_status:
            raise AssertionError(f"expected stop status {expected_status}, got {result.status}")
        if expected_status == 202 and result.body["snapshot"]["device_state"] != "idle":
            raise AssertionError(
                f"expected idle device_state, got {result.body['snapshot']['device_state']!r}"
            )
        return session_id

    def assert_storage_admission_failure(
        self,
        coordinator: CaptureCoordinator,
        expected_code: str,
        expected_storage: dict[str, object] | None = None,
    ) -> None:
        """Verify storage admission rejects capture with expected_code and closes coordinator."""
        try:
            validate_capture_status(coordinator.capture_status())
            descriptor = coordinator.device_descriptor("v3", "customer")
            validate_device_descriptor(
                descriptor,
                api_version="v3",
                security_profile="customer",
            )
            if descriptor["capabilities"]["capture"] is not False:
                raise AssertionError("expected capture capability to be False")
            target_storage = expected_storage or {
                "volume_id": None,
                "total_bytes": 0,
                "available_bytes": 0,
                "writable": False,
            }
            if descriptor["storage"] != target_storage:
                raise AssertionError(
                    f"expected storage {target_storage!r}, got {descriptor['storage']!r}"
                )
            try:
                coordinator.start_capture(start_command(expected_code))
            except ProviderError as rejected:
                if rejected.code != expected_code:
                    raise AssertionError(
                        f"expected error code {expected_code!r}, got {rejected.code!r}"
                    ) from rejected
                if rejected.status != 409:
                    raise AssertionError(
                        f"expected status 409, got {rejected.status}"
                    ) from rejected
            else:
                raise AssertionError(f"expected ProviderError({expected_code!r})")
        finally:
            coordinator.close()

    def retained_catalog_entry(
        self,
        *,
        recovered: bool,
        prefix: str = "retained-catalog",
    ) -> tuple[CaptureCoordinator, str]:
        """Create a sealed (optionally recovered) session persisted as a retained failed entry."""
        coordinator = self.coordinator()
        coordinator.start_capture(start_command(prefix + "-start"))
        retained = deepcopy(coordinator.capture_status()["snapshot"]["active_recording"])
        session_id = retained["recording_state"]["session_id"]
        coordinator.submit_frame(frame())
        coordinator.stop_capture(stop_command(prefix + "-stop"))
        directory = resolve_recording(self.mountpoint / "recordings", session_id)
        if recovered:
            payload = b'{"outcome":"interrupted"}\n'
            log = directory / "recovery" / "interruption.json"
            log.parent.mkdir()
            log.write_bytes(payload)
            digest = hashlib.sha256(payload).hexdigest()
            manifest_path = directory / "manifest.json"
            manifest = json.loads(manifest_path.read_bytes())
            manifest["logs"].append(
                {
                    "artifact_id": digest,
                    "path": "recovery/interruption.json",
                    "sha256": digest,
                    "bytes": len(payload),
                    "role": coordinator_module.RECOVERY_ROLE,
                    "media_type": "application/json",
                }
            )
            manifest_path.write_text(json.dumps(manifest))
            validate_device_session_directory(directory)
        retained["recording_state"]["state"] = "failed"
        retained["recording_state"]["diagnostics"] = [
            {
                "code": "process_interrupted",
                "severity": "error",
                "message": "test interrupted recording",
                "at": retained["recording_state"]["updated_at"],
                "recoverable": False,
            }
        ]
        coordinator._retained[session_id] = retained
        coordinator._invalidate_session_verification(session_id)
        coordinator._persist_local_state()
        coordinator.list_sessions(cursor=None, limit=50, take_id=None, api_version="v4")
        return coordinator, session_id

    @staticmethod
    def deletion_command(
        coordinator: CaptureCoordinator,
        session_ids: Iterable[str],
        key: str = "delete-one",
    ) -> CaptureCommand:
        return deletion_command(coordinator, session_ids, key=key)


class CoordinatorMixin(unittest.TestCase):
    """unittest.TestCase mixin provisioning a CoordinatorHarness as self.harness."""

    harness: CoordinatorHarness

    def setUp(self) -> None:
        super().setUp()
        self.harness = CoordinatorHarness().setUp()
        self.addCleanup(self.harness.tearDown)


class FocusSources(FakeSources):
    """FakeSources tracking focus calls, scripted errors, and value/auto validation."""

    def __init__(self) -> None:
        super().__init__()
        self.focus = deepcopy(CAMERA_FOCUS_STATUS)
        self.calls: list[tuple[int | None, bool | None]] = []
        self.failures: list[BaseException] = []

    def set_camera_focus(
        self,
        *,
        value: int | None = None,
        auto_enabled: bool | None = None,
    ) -> dict[str, object]:
        self.calls.append((value, auto_enabled))
        if self.failures:
            raise self.failures.pop(0)
        if value is not None and (type(value) is not int or not 0 <= value <= 255):
            raise CameraError("invalid_focus_value", "fixture invalid focus")
        if auto_enabled is not None and type(auto_enabled) is not bool:
            raise CameraError("invalid_camera_focus", "fixture invalid auto mode")
        return super().set_camera_focus(value=value, auto_enabled=auto_enabled)


def focus_command(key: str, value: object) -> CaptureCommand:
    """Build a ylx.camera-focus-set.v1 CaptureCommand."""
    return command(key, {"schema": "ylx.camera-focus-set.v1", "value": value})


def check_worker_initialization_failures(
    case: unittest.TestCase,
    coordinator: CaptureCoordinator,
    stop_cmd: CaptureCommand,
    *,
    recording: bool,
    interrupted: bool,
) -> None:
    """Assert Thread construction/start failures keep stop state and leases clean."""
    kinds = (KeyboardInterrupt, SystemExit) if interrupted else (RuntimeError, MemoryError)
    scope = (stop_cmd.principal_id, "stop", stop_cmd.idempotency_key)
    active = coordinator._active
    for stage in ("construct", "start"):
        for kind in kinds:
            with case.subTest(stage=stage, kind=kind.__name__):
                injected = kind("injected worker initialization failure")
                target = (
                    patch("rp_ylx.recording.coordinator.threading.Thread", side_effect=injected)
                    if stage == "construct"
                    else patch.object(threading.Thread, "start", side_effect=injected)
                )
                caught: BaseException | None = None
                try:
                    with target:
                        try:
                            coordinator.request_stop_capture(stop_cmd)
                        except BaseException as error:
                            caught = error
                    case.assertIsNone(coordinator._async_stop_worker)
                    case.assertIsNone(coordinator._async_stop_reason)
                    case.assertIsNone(coordinator._stop_inflight)
                    case.assertIs(coordinator._active, active)
                    case.assertNotIn(scope, coordinator._commands)
                    case.assertFalse(
                        any(
                            receipt["idempotency_key"] == stop_cmd.idempotency_key
                            for receipt in coordinator._state_db.capture_state().get("commands", [])
                        )
                    )
                    if recording:
                        case.fixture.assert_network_operation_lock_held()  # type: ignore[attr-defined]
                        case.assertEqual(
                            coordinator.capture_status()["snapshot"]["device_state"], "recording"
                        )
                    else:
                        case.fixture.assert_network_operation_lock_available()  # type: ignore[attr-defined]
                        case.assertIsNone(coordinator.current_safe_swap_receipt())
                    if interrupted:
                        case.assertIs(caught, injected)
                    else:
                        case.assertIsInstance(caught, ProviderError)
                        assert isinstance(caught, ProviderError)
                        case.assertEqual(caught.code, "stop_worker_unavailable")
                        case.assertEqual(caught.status, 503)
                        case.assertTrue(caught.retryable)
                        case.assertIs(caught.__cause__, injected)
                finally:
                    with coordinator._stop_condition:
                        worker = coordinator._async_stop_worker
                        if worker is not None and worker.ident is None:
                            coordinator._async_stop_worker = None
                            coordinator._async_stop_reason = None
                            coordinator._stop_inflight = None
                            coordinator._stop_condition.notify_all()
                        if not recording:
                            coordinator._release_network_operation_lease()


def check_started_worker_cancellation(
    case: unittest.TestCase,
    coordinator: CaptureCoordinator,
    stop_cmd: CaptureCommand,
    *,
    recording: bool,
) -> None:
    """Assert KeyboardInterrupt after Thread.start() rolls back stop acceptance cleanly."""
    started: list[threading.Thread] = []
    injected = KeyboardInterrupt("cancelled after OS thread start")
    original_start = threading.Thread.start
    operation = "_finish_stop_capture" if recording else "_check_pending_safe_swap"

    def start(worker: threading.Thread) -> None:
        original_start(worker)
        started.append(worker)
        raise injected

    with patch.object(coordinator, operation, wraps=getattr(coordinator, operation)) as work:
        try:
            caught: BaseException | None = None
            with patch.object(threading.Thread, "start", start):
                try:
                    coordinator.request_stop_capture(stop_cmd)
                except BaseException as error:
                    caught = error
            case.assertIs(caught, injected)
            for worker in started:
                worker.join(3)
                case.assertFalse(worker.is_alive())
            case.assertEqual(len(started), 1)
            work.assert_not_called()
            case.assertIsNone(coordinator._async_stop_worker)
            case.assertIsNone(coordinator._async_stop_reason)
            case.assertIsNone(coordinator._stop_inflight)
            case.assertNotIn(
                (stop_cmd.principal_id, "stop", stop_cmd.idempotency_key), coordinator._commands
            )
            if recording:
                case.fixture.assert_network_operation_lock_held()  # type: ignore[attr-defined]
                case.assertEqual(
                    coordinator.capture_status()["snapshot"]["device_state"], "recording"
                )
            else:
                case.fixture.assert_network_operation_lock_available()  # type: ignore[attr-defined]
                case.assertIsNone(coordinator.current_safe_swap_receipt())
        finally:
            for worker in started:
                worker.join(4)


__all__ = [
    "CAMERA_FOCUS_STATUS",
    "COMMIT",
    "CoordinatorHarness",
    "CoordinatorMixin",
    "DelayedLock",
    "FakeSources",
    "FakeSplitEyeEncoder",
    "FocusSources",
    "JPEG",
    "check_started_worker_cancellation",
    "check_worker_initialization_failures",
    "command",
    "deletion_command",
    "focus_command",
    "frame",
    "idle_network_control",
    "imu_observation",
    "network_command",
    "single_imu_observation",
    "start_command",
    "stop_command",
    "validate_capture_status",
    "validate_device_session_directory",
]

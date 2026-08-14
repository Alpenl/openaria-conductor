from __future__ import annotations

import json
import tempfile
import threading
import time
import unittest
import uuid
from pathlib import Path

from rp_ylx.api import CaptureCommand
from rp_ylx.api.events import EventReplayBuffer
from rp_ylx.camera import (
    CameraController,
    CameraDescriptor,
    CameraError,
    CameraMode,
    FrameObservation,
    StereoFrame,
    SyntheticCameraBackend,
)
from rp_ylx.recording import (
    CaptureCoordinator,
    CoordinatorConfig,
    DeviceSessionConfig,
    ThreadedCaptureSources,
    initialize_capture_volume,
    validate_device_session_directory,
)

JPEG = b"\xff\xd8threaded-sbs\xff\xd9"


class BlockingCamera:
    def __init__(self, *, failure: BaseException | None = None) -> None:
        self.failure = failure
        self.closed = threading.Event()
        self.delivered = threading.Event()

    def open(self, mode: CameraMode, *, stable_id: str | None = None) -> object:
        del mode, stable_id
        return self

    def start(self) -> None:
        return None

    def read(self, *, timeout: float) -> FrameObservation:
        if self.failure is not None:
            self.delivered.set()
            raise self.failure
        if not self.delivered.is_set():
            self.delivered.set()
            return FrameObservation(
                StereoFrame(0, 1_000_000, b"left", b"right", True, JPEG),
                dropped_before=0,
            )
        if not self.closed.wait(timeout):
            raise TimeoutError("fake camera timeout")
        raise CameraError("disconnected", "fake camera closed")

    def stop(self) -> None:
        self.closed.set()

    def close(self) -> None:
        self.closed.set()


class BlockingImu:
    def __init__(self) -> None:
        self.closed = threading.Event()

    def read(self, *, timeout: float) -> object:
        if not self.closed.wait(timeout):
            raise TimeoutError("fake imu timeout")
        raise OSError("fake imu closed")

    def close(self) -> None:
        self.closed.set()


def capture_command(key: str, body: dict[str, object]) -> CaptureCommand:
    canonical = json.dumps(body, sort_keys=True, separators=(",", ":")).encode()
    return CaptureCommand("operator", key, body, canonical)


class ThreadedCaptureSourcesTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.volume = self.root / "volume"
        self.volume.mkdir()
        initialize_capture_volume(self.volume)
        self.config = CoordinatorConfig(
            self.volume,
            self.root / "state",
            DeviceSessionConfig(
                device_id=str(uuid.uuid4()),
                device_label="YLX-89ABCDEF",
                hardware_fingerprint="sha256:" + "c" * 64,
                platform="D-Robotics RDK X5 V1.0 + YLX 2UQ2",
                software_version="0.5.0",
                commit="d" * 40,
                width=3840,
                height=1080,
                sensor_fps=60.0,
            ),
            minimum_available_bytes=0,
            minimum_available_inodes=0,
        )

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def start(self, coordinator: CaptureCoordinator, *, key: str) -> str:
        result = coordinator.start_capture(
            capture_command(
                key,
                {
                    "schema": "ylx.capture-start.v2",
                    "mode": "production",
                    "take": {"kind": "new"},
                },
            )
        )
        active = result.body["snapshot"]["active_recording"]
        return active["recording_state"]["session_id"]

    def stop(self, coordinator: CaptureCoordinator, *, key: str) -> None:
        coordinator.stop_capture(
            capture_command(
                key,
                {"schema": "ylx.capture-stop.v2", "reason": "user"},
            )
        )

    def wait_for_retained_failure(
        self,
        coordinator: CaptureCoordinator,
        session_id: str,
    ) -> dict[str, object]:
        deadline = time.monotonic() + 2
        while time.monotonic() < deadline:
            retained = coordinator.retained_unsuccessful_outcome(session_id)
            if retained is not None:
                return retained
            time.sleep(0.01)
        self.fail(f"session {session_id} did not settle to a retained failure")

    def test_threads_submit_raw_frame_and_release_before_seal(self) -> None:
        camera = BlockingCamera()
        imu = BlockingImu()
        sources = ThreadedCaptureSources(
            lambda: camera,
            lambda: imu,
            CameraMode(3840, 1080, 60.0, "mjpg"),
            read_timeout=0.2,
        )
        coordinator = CaptureCoordinator(
            self.config,
            mount_checker=lambda path: path == self.volume.resolve(),
            sources=sources,
        )
        try:
            session_id = self.start(coordinator, key="threaded-start")
            self.assertTrue(camera.delivered.wait(timeout=1))
            self.stop(coordinator, key="threaded-stop")
            manifest = validate_device_session_directory(self.volume / "sessions" / session_id)
            self.assertEqual(manifest["frames"]["count"], 1)
            self.assertEqual(sources.open_handle_count, 0)
            self.assertTrue(camera.closed.is_set())
            self.assertTrue(imu.closed.is_set())
        finally:
            coordinator.close()

    def test_camera_failure_closes_imu_and_retains_failed_outcome(self) -> None:
        camera = BlockingCamera(failure=CameraError("disconnected", "模拟热拔"))
        imu = BlockingImu()
        sources = ThreadedCaptureSources(
            lambda: camera,
            lambda: imu,
            CameraMode(3840, 1080, 60.0, "mjpg"),
            read_timeout=0.2,
        )
        coordinator = CaptureCoordinator(
            self.config,
            mount_checker=lambda path: path == self.volume.resolve(),
            sources=sources,
        )
        try:
            session_id = self.start(coordinator, key="failure-start")
            deadline = time.monotonic() + 2
            status = coordinator.capture_status()
            while status["snapshot"]["device_state"] != "idle" and time.monotonic() < deadline:
                time.sleep(0.01)
                status = coordinator.capture_status()
            self.assertEqual(status["snapshot"]["device_state"], "idle")
            retained = coordinator.retained_unsuccessful_outcome(session_id)
            state = retained["outcome"]["recording_state"]
            self.assertEqual(state["state"], "failed")
            self.assertEqual(state["diagnostics"][0]["code"], "disconnected")
            self.assertEqual(sources.open_handle_count, 0)
            self.assertTrue(imu.closed.is_set())
            partial = self.volume / "sessions" / f"{session_id}.partial"
            self.assertFalse((partial / "manifest.json").exists())
        finally:
            coordinator.close()

    def test_source_fault_matrix_is_retained_and_survives_restart(self) -> None:
        cases = (
            (
                "source_sequence_gap",
                (
                    StereoFrame(0, 1_000_000, b"left-0", b"right-0", True, JPEG),
                    StereoFrame(2, 2_000_000, b"left-2", b"right-2", True, JPEG),
                ),
            ),
            (
                "sequence_regression",
                (
                    StereoFrame(2, 1_000_000, b"left-2", b"right-2", True, JPEG),
                    StereoFrame(2, 2_000_000, b"left-2", b"right-2", True, JPEG),
                ),
            ),
            (
                "sequence_regression",
                (
                    StereoFrame(2, 1_000_000, b"left-2", b"right-2", True, JPEG),
                    StereoFrame(1, 2_000_000, b"left-1", b"right-1", True, JPEG),
                ),
            ),
            (
                "bad_frame",
                (StereoFrame(0, 1_000_000, b"", b"right", True),),
            ),
        )

        for index, (expected_code, frames) in enumerate(cases):
            with self.subTest(index=index, expected_code=expected_code):
                mode = CameraMode(3840, 1080, 60.0, "mjpg")
                descriptor = CameraDescriptor(
                    f"camera-{index}",
                    f"/dev/camera-{index}",
                    "YLX Stereo",
                    (mode,),
                )
                backend = SyntheticCameraBackend(
                    (descriptor,),
                    frames={descriptor.stable_id: frames},
                )
                imu = BlockingImu()
                sources = ThreadedCaptureSources(
                    lambda backend=backend: CameraController(backend),
                    lambda imu=imu: imu,
                    mode,
                    stable_id=descriptor.stable_id,
                    read_timeout=0.2,
                )
                coordinator = CaptureCoordinator(
                    self.config,
                    mount_checker=lambda path: path == self.volume.resolve(),
                    sources=sources,
                )
                try:
                    session_id = self.start(coordinator, key=f"source-fault-{index}")
                    retained = self.wait_for_retained_failure(coordinator, session_id)
                    state = retained["outcome"]["recording_state"]
                    self.assertEqual(state["state"], "failed")
                    self.assertEqual(state["diagnostics"][0]["code"], expected_code)

                    status = coordinator.capture_status()
                    projected = status["snapshot"]["retained_unsuccessful"]
                    self.assertEqual(projected["recording_state"], state)
                    source_event = coordinator.capture_snapshot_event()
                    self.assertEqual(
                        source_event["data"]["retained_unsuccessful"]["recording_state"],
                        state,
                    )
                    event_buffer = EventReplayBuffer(capacity=1)
                    event_buffer.publish(source_event)
                    event_buffer.publish(source_event)
                    replayed = event_buffer.replay(
                        "1",
                        api_version="v3",
                        snapshot=coordinator.capture_snapshot_event,
                    )
                    replayed_state = replayed[0].source_event["data"]["retained_unsuccessful"][
                        "recording_state"
                    ]
                    self.assertEqual(replayed_state, state)
                    self.assertEqual(
                        coordinator.retained_unsuccessful_outcome(session_id)["outcome"][
                            "recording_state"
                        ],
                        state,
                    )

                    partial = self.volume / "sessions" / f"{session_id}.partial"
                    final = self.volume / "sessions" / session_id
                    self.assertTrue(partial.is_dir())
                    self.assertFalse((partial / "manifest.json").exists())
                    self.assertFalse(final.exists())
                    self.assertEqual(sources.open_handle_count, 0)
                    self.assertTrue(imu.closed.is_set())
                finally:
                    coordinator.close()

                restarted = CaptureCoordinator(
                    self.config,
                    mount_checker=lambda path: path == self.volume.resolve(),
                )
                try:
                    recovered = restarted.retained_unsuccessful_outcome(session_id)
                    recovered_state = recovered["outcome"]["recording_state"]
                    self.assertEqual(recovered_state["state"], "failed")
                    self.assertEqual(recovered_state["diagnostics"][0]["code"], expected_code)
                    recovered_snapshot = restarted.capture_status()["snapshot"]
                    self.assertEqual(
                        recovered_snapshot["retained_unsuccessful"]["recording_state"][
                            "session_id"
                        ],
                        session_id,
                    )
                finally:
                    restarted.close()

        coordinator = CaptureCoordinator(
            self.config,
            mount_checker=lambda path: path == self.volume.resolve(),
        )
        try:
            session_id = self.start(coordinator, key="after-source-fault")
            self.assertTrue(
                coordinator.submit_frame(
                    FrameObservation(
                        StereoFrame(0, 1_000_000, b"left", b"right", True, JPEG),
                        dropped_before=0,
                    )
                )
            )
            self.stop(coordinator, key="after-source-fault-stop")
            manifest = validate_device_session_directory(self.volume / "sessions" / session_id)
            self.assertEqual(manifest["frames"]["count"], 1)
        finally:
            coordinator.close()


if __name__ == "__main__":
    unittest.main()

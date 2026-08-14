from __future__ import annotations

import errno
import hashlib
import json
import os
import tempfile
import threading
import time
import unittest
import uuid
from pathlib import Path
from unittest.mock import patch

from rp_ylx.api import CaptureCommand, ProviderError
from rp_ylx.api.downloads import ArtifactAccessError, DirectorySessionStore
from rp_ylx.api.events import (
    EventReplayBuffer,
    validate_capture_status,
    validate_device_descriptor,
    validate_retained_unsuccessful_outcome,
    validate_safe_swap_v3_receipt,
    validate_session_list,
)
from rp_ylx.api.preview import PreviewFrameUnavailable
from rp_ylx.camera import FrameObservation, StereoFrame
from rp_ylx.recording import (
    CaptureCoordinator,
    CoordinatorConfig,
    DeviceRecordingError,
    DeviceSessionConfig,
    initialize_capture_volume,
    validate_device_session_directory,
)

JPEG = b"\xff\xd8raw-side-by-side\xff\xd9"
COMMIT = "a" * 40


class FakeSources:
    def __init__(self) -> None:
        self.open_handle_count = 0
        self.mode: str | None = None
        self.generation_id: str | None = None
        self.submit_frame: object | None = None
        self.submit_imu: object | None = None

    def start(
        self,
        *,
        mode: str,
        generation_id: str,
        submit_frame: object,
        submit_imu: object,
        on_failure: object,
    ) -> None:
        self.mode = mode
        self.generation_id = generation_id
        self.submit_frame = submit_frame
        self.submit_imu = submit_imu
        self.open_handle_count = 2

    def stop(self) -> None:
        self.open_handle_count = 0


def command(key: str, body: dict[str, object]) -> CaptureCommand:
    canonical = json.dumps(body, sort_keys=True, separators=(",", ":")).encode()
    return CaptureCommand("operator", key, body, canonical)


def start_command(
    key: str,
    *,
    mode: str = "production",
    display_name: str = "测试录制",
) -> CaptureCommand:
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
    return command(key, {"schema": "ylx.capture-stop.v2", "reason": reason})


def frame(
    sequence: int = 0,
    *,
    generation_payload: bytes = JPEG,
    dropped_before: int = 0,
) -> FrameObservation:
    return FrameObservation(
        StereoFrame(
            source_sequence=sequence,
            host_monotonic_ns=1_000_000 + sequence,
            left=b"left-preview",
            right=b"right-preview",
            raw_side_by_side=generation_payload,
        ),
        dropped_before=dropped_before,
    )


class CaptureCoordinatorTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.mountpoint = self.root / "volume"
        self.state_root = self.root / "state"
        self.mountpoint.mkdir()
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

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def coordinator(
        self,
        *,
        before_write: object | None = None,
        minimum_available_bytes: int = 0,
        minimum_available_inodes: int = 0,
        checkpoint_interval: float = 1.0,
        queue_capacity: int = 128,
        enqueue_timeout: float = 0.05,
    ) -> CaptureCoordinator:
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
            before_write=before_write,  # type: ignore[arg-type]
        )

    def active_session_id(self, coordinator: CaptureCoordinator) -> str:
        status = coordinator.capture_status()
        validate_capture_status(status)
        active = status["snapshot"]["active_recording"]
        return active["recording_state"]["session_id"]

    def seal_one(
        self,
        coordinator: CaptureCoordinator,
        *,
        prefix: str = "one",
        mode: str = "production",
        reason: str = "user",
    ) -> str:
        coordinator.start_capture(start_command(f"{prefix}-start", mode=mode))
        session_id = self.active_session_id(coordinator)
        self.assertTrue(coordinator.submit_frame(frame()))
        result = coordinator.stop_capture(stop_command(f"{prefix}-stop", reason=reason))
        expected_status = 204 if reason == "safe_swap" else 202
        self.assertEqual(result.status, expected_status)
        if expected_status == 202:
            self.assertEqual(result.body["snapshot"]["device_state"], "idle")
        return session_id

    def test_volume_requires_explicit_marker_mount_and_capacity(self) -> None:
        missing = self.root / "missing-marker"
        missing.mkdir()
        with self.assertRaises(DeviceRecordingError) as marker:
            CaptureCoordinator(
                CoordinatorConfig(missing, self.state_root, self.session_config),
                mount_checker=lambda path: path == missing.resolve(),
            )
        self.assertEqual(marker.exception.code, "volume_not_admitted")

        with (
            patch(
                "rp_ylx.recording.coordinator._stat_capacity",
                return_value=(10_000, 9, 100),
            ),
            self.assertRaises(DeviceRecordingError) as bytes_error,
        ):
            self.coordinator(minimum_available_bytes=10)
        self.assertEqual(bytes_error.exception.code, "insufficient_space")

        with (
            patch(
                "rp_ylx.recording.coordinator._stat_capacity",
                return_value=(10_000, 100, 9),
            ),
            self.assertRaises(DeviceRecordingError) as inode_error,
        ):
            self.coordinator(minimum_available_inodes=10)
        self.assertEqual(inode_error.exception.code, "insufficient_inodes")

        with (
            patch("rp_ylx.recording.coordinator.os.access", return_value=False),
            self.assertRaises(DeviceRecordingError) as read_only,
        ):
            self.coordinator()
        self.assertEqual(read_only.exception.code, "volume_read_only")

    def test_unmounted_volume_keeps_control_plane_and_recovers_without_restart(self) -> None:
        mounted = False
        coordinator = CaptureCoordinator(
            CoordinatorConfig(
                self.mountpoint,
                self.state_root,
                self.session_config,
                minimum_available_bytes=0,
                minimum_available_inodes=0,
            ),
            mount_checker=lambda path: mounted and path == self.mountpoint.resolve(),
        )
        try:
            validate_capture_status(coordinator.capture_status())
            descriptor = coordinator.device_descriptor("v3", "customer")
            validate_device_descriptor(
                descriptor,
                api_version="v3",
                security_profile="customer",
            )
            self.assertFalse(descriptor["capabilities"]["capture"])
            self.assertEqual(
                descriptor["storage"],
                {
                    "volume_id": None,
                    "total_bytes": 0,
                    "available_bytes": 0,
                    "writable": False,
                },
            )

            with self.assertRaises(ProviderError) as unavailable:
                coordinator.start_capture(start_command("volume-missing"))
            self.assertEqual(unavailable.exception.code, "volume_not_mounted")
            self.assertEqual(unavailable.exception.status, 409)

            mounted = True
            validate_capture_status(coordinator.capture_status())
            automatically_restored = coordinator.device_descriptor("v3", "customer")
            self.assertTrue(automatically_restored["capabilities"]["capture"])
            self.assertEqual(automatically_restored["storage"]["volume_id"], self.volume_id)
            result = coordinator.start_capture(start_command("volume-restored"))
            self.assertEqual(result.status, 202)
            self.assertEqual(result.body["snapshot"]["device_state"], "recording")
            self.assertEqual(coordinator.volume_id, self.volume_id)
            restored = coordinator.device_descriptor("v3", "customer")
            self.assertTrue(restored["capabilities"]["capture"])
            self.assertEqual(restored["storage"]["volume_id"], self.volume_id)
            self.assertTrue(restored["storage"]["writable"])
        finally:
            coordinator.close()

    def test_start_rechecks_thresholds_and_rejects_wrong_volume(self) -> None:
        coordinator = self.coordinator(minimum_available_bytes=10, minimum_available_inodes=10)
        try:
            wrong = start_command("wrong-volume")
            wrong_body = {**wrong.body, "volume_id": str(uuid.uuid4())}
            with self.assertRaises(ProviderError) as mismatch:
                coordinator.start_capture(command("wrong-volume", wrong_body))
            self.assertEqual(mismatch.exception.code, "volume_mismatch")

            cases = (
                ((100, 9, 100), "insufficient_space"),
                ((100, 100, 9), "insufficient_inodes"),
            )
            for capacity, expected in cases:
                with (
                    self.subTest(expected=expected),
                    patch(
                        "rp_ylx.recording.coordinator._stat_capacity",
                        return_value=capacity,
                    ),
                ):
                    with self.assertRaises(ProviderError) as rejected:
                        coordinator.start_capture(start_command(expected))
                    self.assertEqual(rejected.exception.code, expected)
        finally:
            coordinator.close()

    def test_normal_v1_seal_is_manifest_last_and_gateway_readable(self) -> None:
        coordinator = self.coordinator()
        try:
            session_id = self.seal_one(coordinator)
            session = self.mountpoint / "sessions" / session_id
            manifest = validate_device_session_directory(session)
            self.assertEqual(manifest["schema"], "ylx.device-session.v1")
            self.assertEqual(manifest["video"]["layout"], "raw-side-by-side")
            self.assertEqual(manifest["frames"]["count"], 1)
            self.assertEqual(manifest["camera"]["nominal_fps"], 60.0)
            self.assertAlmostEqual(
                manifest["camera"]["effective_fps"],
                manifest["frames"]["count"] / manifest["time"]["duration_seconds"],
            )
            self.assertEqual(
                manifest["integrity"]["quality_policy"]["policy_id"],
                "rdk-x5-lossless-v1",
            )
            self.assertFalse((session / "recording.json").exists())
            self.assertFalse((session / "capture.json").exists())

            listed = coordinator.list_sessions(cursor=None, limit=50, take_id=None)
            validate_session_list(listed, limit=50, take_id=None)
            self.assertEqual([item["session_id"] for item in listed["items"]], [session_id])

            manifest_handle = coordinator.open_manifest(session_id, "v3")
            try:
                manifest_bytes = manifest_handle.read()
                self.assertEqual(json.loads(manifest_bytes)["session_id"], session_id)
            finally:
                manifest_handle.close()
            video = manifest["video"]["artifact"]
            artifact = coordinator.open_verified_artifact(session_id, video["artifact_id"], "v3")
            try:
                self.assertEqual(artifact.read(), JPEG)
            finally:
                artifact.close()
        finally:
            coordinator.close()

    def test_raw_sbs_frame_is_used_for_preview_without_eye_materialization(self) -> None:
        coordinator = self.coordinator()
        try:
            coordinator.start_capture(start_command("raw-sbs-preview"))
            observation = FrameObservation(
                StereoFrame(
                    source_sequence=0,
                    host_monotonic_ns=1_000_000,
                    left=b"",
                    right=b"",
                    raw_side_by_side=JPEG,
                ),
                dropped_before=0,
            )

            self.assertTrue(coordinator.submit_frame(observation))
            preview = coordinator.latest_preview(fps=None, accept="image/jpeg")
            self.assertEqual(preview.body, JPEG)
            self.assertEqual(preview.content_length, len(JPEG))
            result = coordinator.stop_capture(stop_command("raw-sbs-preview-stop"))
            self.assertEqual(result.status, 202)
            with self.assertRaises(PreviewFrameUnavailable):
                coordinator.latest_preview(fps=None, accept="image/jpeg")
        finally:
            coordinator.close()

    def test_source_sequence_gap_fails_and_is_retained_without_manifest(self) -> None:
        coordinator = self.coordinator()
        try:
            coordinator.start_capture(start_command("source-gap-start"))
            session_id = self.active_session_id(coordinator)
            with self.assertRaises(DeviceRecordingError) as rejected:
                coordinator.submit_frame(frame(3, dropped_before=2))
            self.assertEqual(rejected.exception.code, "source_sequence_gap")
            retained = coordinator.retained_unsuccessful_outcome(session_id)
            state = retained["outcome"]["recording_state"]
            self.assertEqual(state["state"], "failed")
            self.assertEqual(state["diagnostics"][0]["code"], "source_sequence_gap")
            partial = self.mountpoint / "sessions" / f"{session_id}.partial"
            self.assertTrue(partial.is_dir())
            self.assertFalse((partial / "manifest.json").exists())
            with self.assertRaises(PreviewFrameUnavailable):
                coordinator.latest_preview(fps=None, accept="image/jpeg")
        finally:
            coordinator.close()

    def test_writer_drop_exceeding_lossless_policy_never_seals(self) -> None:
        writer_blocked = threading.Event()
        release_writer = threading.Event()

        def slow_writer(role: str, payload: bytes) -> None:
            if role == "video.raw-side-by-side" and payload:
                writer_blocked.set()
                if not release_writer.wait(timeout=2):
                    raise TimeoutError("test did not release writer")

        coordinator = self.coordinator(
            before_write=slow_writer,
            queue_capacity=1,
            enqueue_timeout=0,
        )
        try:
            coordinator.start_capture(start_command("drop-gate-start"))
            session_id = self.active_session_id(coordinator)
            self.assertTrue(coordinator.submit_frame(frame(0)))
            self.assertTrue(writer_blocked.wait(timeout=1))
            self.assertTrue(coordinator.submit_frame(frame(1)))
            self.assertFalse(coordinator.submit_frame(frame(2)))
            release_writer.set()
            with self.assertRaises(ProviderError) as rejected:
                coordinator.stop_capture(stop_command("drop-gate-stop"))
            self.assertEqual(rejected.exception.code, "drop_quality_exceeded")
            partial = self.mountpoint / "sessions" / f"{session_id}.partial"
            self.assertTrue(partial.is_dir())
            self.assertFalse((partial / "manifest.json").exists())
            retained = coordinator.retained_unsuccessful_outcome(session_id)
            state = retained["outcome"]["recording_state"]
            self.assertEqual(state["state"], "failed")
            self.assertEqual(state["diagnostics"][0]["code"], "drop_quality_exceeded")
            diagnostic = state["diagnostics"][0]["message"]
            self.assertIn("classification=write_backpressure", diagnostic)
            self.assertIn("contiguous=1", diagnostic)
            self.assertIn("window_drops=1", diagnostic)
            self.assertIn("limits=contiguous:0,total:0,fraction:0,window:0", diagnostic)
        finally:
            release_writer.set()
            coordinator.close()

    def test_production_provider_projections_pass_all_frozen_validators(self) -> None:
        coordinator = self.coordinator()
        try:
            for api_version in ("v2", "v3"):
                descriptor = coordinator.device_descriptor(api_version, "customer")
                validate_device_descriptor(
                    descriptor,
                    api_version=api_version,
                    security_profile="customer",
                )
            validate_capture_status(coordinator.capture_status())
            self.assertEqual(EventReplayBuffer().publish(coordinator.capture_snapshot_event()), "1")

            coordinator.start_capture(start_command("validator-start"))
            session_id = self.active_session_id(coordinator)
            validate_capture_status(coordinator.capture_status())
            self.assertEqual(EventReplayBuffer().publish(coordinator.capture_snapshot_event()), "1")
            coordinator.report_media_loss(generation_id=coordinator.generation_id)

            status = coordinator.capture_status()
            validate_capture_status(status)
            retained = coordinator.retained_unsuccessful_outcome(session_id)
            validate_retained_unsuccessful_outcome(retained, session_id=session_id)
            self.assertEqual(EventReplayBuffer().publish(coordinator.capture_snapshot_event()), "1")
            descriptor = coordinator.device_descriptor("v3", "customer")
            validate_device_descriptor(
                descriptor,
                api_version="v3",
                security_profile="customer",
            )
            self.assertEqual(descriptor["storage"]["available_bytes"], 0)
            self.assertFalse(descriptor["storage"]["writable"])
        finally:
            coordinator.close()

    def test_duplicate_control_and_modes_are_mutually_exclusive(self) -> None:
        coordinator = self.coordinator()
        try:
            start = start_command("same-key")
            first = coordinator.start_capture(start)
            replay = coordinator.start_capture(start)
            self.assertEqual(first.body, replay.body)
            self.assertTrue(replay.replayed)

            changed = start_command("same-key", mode="calibration")
            with self.assertRaises(ProviderError) as conflict:
                coordinator.start_capture(changed)
            self.assertEqual(conflict.exception.code, "idempotency_conflict")

            with self.assertRaises(ProviderError) as busy:
                coordinator.start_capture(start_command("other-key", mode="calibration"))
            self.assertEqual(busy.exception.code, "capture_busy")
            coordinator.submit_frame(frame())
            coordinator.stop_capture(stop_command("stop"))
        finally:
            coordinator.close()

    def test_recording_progress_checkpoints_and_revision_survives_restart(self) -> None:
        coordinator = self.coordinator(checkpoint_interval=0)
        coordinator.start_capture(start_command("checkpoint-start"))
        session_id = self.active_session_id(coordinator)
        coordinator.submit_frame(frame(0))
        deadline = time.monotonic() + 1
        recording_path = self.mountpoint / "sessions" / f"{session_id}.partial/recording.json"
        while time.monotonic() < deadline:
            state = json.loads(recording_path.read_bytes())
            if state["progress"]["captured_frames"] == 1:
                break
            time.sleep(0.01)
            coordinator.submit_frame(frame(1))
        self.assertGreaterEqual(state["progress"]["captured_frames"], 1)
        self.assertGreater(state["progress"]["bytes_written"], 0)
        checkpoint_revision = coordinator.capture_status()["source_revision"]
        revision_deadline = time.monotonic() + 1
        while (
            checkpoint_revision < state["state_revision"] and time.monotonic() < revision_deadline
        ):
            time.sleep(0.01)
            checkpoint_revision = coordinator.capture_status()["source_revision"]
        self.assertGreaterEqual(checkpoint_revision, state["state_revision"])
        coordinator.close()

        restarted = self.coordinator()
        try:
            recovered = restarted.retained_unsuccessful_outcome(session_id)
            recovered_revision = recovered["source_revision"]
            self.assertGreater(recovered_revision, checkpoint_revision)
            validate_retained_unsuccessful_outcome(recovered, session_id=session_id)
        finally:
            restarted.close()

    def test_idempotent_stop_replays_after_process_restart(self) -> None:
        first = self.coordinator()
        stop = stop_command("durable-stop")
        first.start_capture(start_command("durable-start"))
        first.submit_frame(frame())
        first_result = first.stop_capture(stop)
        self.assertEqual(first_result.status, 202)
        self.assertEqual(first_result.body["snapshot"]["device_state"], "idle")
        first.close()

        restarted = self.coordinator()
        try:
            replay = restarted.stop_capture(stop)
            self.assertEqual(replay.status, 202)
            self.assertEqual(replay.body, first_result.body)
            self.assertTrue(replay.replayed)
        finally:
            restarted.close()

    def test_stop_without_active_recording_is_idempotent_no_op(self) -> None:
        coordinator = self.coordinator()
        try:
            stop = stop_command("already-idle")
            result = coordinator.stop_capture(stop)
            self.assertEqual((result.status, result.body, result.replayed), (204, None, False))
            replay = coordinator.stop_capture(stop)
            self.assertEqual((replay.status, replay.body, replay.replayed), (204, None, True))
        finally:
            coordinator.close()

    def test_seal_does_not_rescan_artifact_bytes(self) -> None:
        coordinator = self.coordinator()
        try:
            coordinator.start_capture(start_command("incremental-digest-start"))
            coordinator.submit_frame(frame())
            with (
                patch(
                    "rp_ylx.recording.device_session._digest",
                    side_effect=AssertionError("seal rescanned an artifact"),
                ),
                patch(
                    "rp_ylx.recording.coordinator.validate_device_session_directory",
                    side_effect=AssertionError("coordinator repeated full validation"),
                ),
            ):
                result = coordinator.stop_capture(stop_command("incremental-digest-stop"))
            self.assertEqual(result.status, 202)
        finally:
            coordinator.close()

    def test_catalog_and_list_do_not_read_artifact_contents(self) -> None:
        first = self.coordinator()
        try:
            session_id = self.seal_one(first, prefix="lightweight-catalog")
        finally:
            first.close()

        with patch(
            "rp_ylx.recording.coordinator.validate_device_session_directory",
            side_effect=AssertionError("catalog/list performed full artifact validation"),
        ):
            restarted = self.coordinator()
            try:
                listed = restarted.list_sessions(cursor=None, limit=50, take_id=None)
            finally:
                restarted.close()
        self.assertEqual([item["session_id"] for item in listed["items"]], [session_id])
        manifest = json.loads(
            (self.mountpoint / "sessions" / session_id / "manifest.json").read_bytes()
        )
        expected_bytes = sum(
            int(manifest[section]["artifact"]["bytes"]) for section in ("video", "imu", "frames")
        )
        self.assertEqual(listed["items"][0]["total_bytes"], expected_bytes)

    def test_remount_at_same_path_rotates_generation_and_rejects_stale_submitter(self) -> None:
        mount_identity = "mount-a"
        first = CaptureCoordinator(
            CoordinatorConfig(
                self.mountpoint,
                self.state_root,
                self.session_config,
                minimum_available_bytes=0,
                minimum_available_inodes=0,
            ),
            mount_checker=lambda path: path == self.mountpoint.resolve(),
            mount_identity=lambda path: mount_identity,
        )
        previous_generation = first.generation_id
        first.close()

        mount_identity = "mount-b"
        restarted = CaptureCoordinator(
            CoordinatorConfig(
                self.mountpoint,
                self.state_root,
                self.session_config,
                minimum_available_bytes=0,
                minimum_available_inodes=0,
            ),
            mount_checker=lambda path: path == self.mountpoint.resolve(),
            mount_identity=lambda path: mount_identity,
        )
        try:
            self.assertNotEqual(restarted.generation_id, previous_generation)
            restarted.start_capture(start_command("remount-start"))
            with self.assertRaises(DeviceRecordingError) as stale:
                restarted.submit_frame(frame(), generation_id=previous_generation)
            self.assertEqual(stale.exception.code, "stale_generation")
            self.assertEqual(restarted.capture_status()["snapshot"]["device_state"], "recording")
            restarted.submit_frame(frame())
            restarted.stop_capture(stop_command("remount-stop"))
        finally:
            restarted.close()

    def test_calibration_output_uses_the_same_python_read_contract(self) -> None:
        coordinator = self.coordinator()
        try:
            session_id = self.seal_one(coordinator, prefix="cal", mode="calibration")
            session = self.mountpoint / "sessions" / session_id
            manifest = validate_device_session_directory(session)
            self.assertEqual(manifest["capture_mode"], "calibration")
            digest = hashlib.sha256((session / "manifest.json").read_bytes()).hexdigest()
            with (
                DirectorySessionStore(
                    self.mountpoint / "sessions",
                    verified_manifests={session_id: digest},
                ) as store,
                store.open_manifest(session_id, "v3") as representation,
            ):
                self.assertEqual(json.loads(representation.read())["capture_mode"], "calibration")
        finally:
            coordinator.close()

    def test_capture_sources_receive_generation_callbacks_and_release_before_seal(self) -> None:
        sources = FakeSources()
        coordinator = CaptureCoordinator(
            CoordinatorConfig(
                self.mountpoint,
                self.state_root,
                self.session_config,
                minimum_available_bytes=0,
                minimum_available_inodes=0,
            ),
            mount_checker=lambda path: path == self.mountpoint.resolve(),
            sources=sources,
        )
        try:
            coordinator.start_capture(start_command("sources-start", mode="calibration"))
            self.assertEqual(sources.mode, "calibration")
            self.assertEqual(sources.generation_id, coordinator.generation_id)
            self.assertTrue(callable(sources.submit_frame))
            self.assertTrue(callable(sources.submit_imu))
            self.assertEqual(coordinator.open_handle_count, 6)
            sources.submit_frame(frame())  # type: ignore[operator]
            coordinator.stop_capture(stop_command("sources-stop"))
            self.assertEqual(sources.open_handle_count, 0)
            self.assertEqual(coordinator.open_handle_count, 0)
        finally:
            coordinator.close()

    def test_enospc_never_publishes_a_success_manifest(self) -> None:
        def disk_full(role: str, payload: bytes) -> None:
            if role == "video.raw-side-by-side" and payload:
                raise OSError(errno.ENOSPC, "模拟磁盘写满")

        coordinator = self.coordinator(before_write=disk_full)
        try:
            coordinator.start_capture(start_command("full-start"))
            session_id = self.active_session_id(coordinator)
            coordinator.submit_frame(frame())
            with self.assertRaises(ProviderError) as stopped:
                coordinator.stop_capture(stop_command("full-stop"))
            self.assertEqual(stopped.exception.code, "storage_full")
            partial = self.mountpoint / "sessions" / f"{session_id}.partial"
            self.assertTrue(partial.is_dir())
            self.assertFalse((partial / "manifest.json").exists())
            state = json.loads((partial / "recording.json").read_bytes())
            self.assertEqual(state["state"], "failed")
            self.assertEqual(state["diagnostics"][0]["code"], "storage_full")
            retained = coordinator.retained_unsuccessful_outcome(session_id)
            self.assertEqual(retained["outcome"]["recording_state"]["state"], "failed")
        finally:
            coordinator.close()

    def test_media_loss_and_stale_generation_fail_closed(self) -> None:
        mounted = True
        coordinator = CaptureCoordinator(
            CoordinatorConfig(
                self.mountpoint,
                self.state_root,
                self.session_config,
                minimum_available_bytes=0,
                minimum_available_inodes=0,
            ),
            mount_checker=lambda path: mounted and path == self.mountpoint.resolve(),
        )
        try:
            coordinator.start_capture(start_command("media-start"))
            session_id = self.active_session_id(coordinator)
            with self.assertRaises(DeviceRecordingError) as stale:
                coordinator.submit_frame(frame(), generation_id=str(uuid.uuid4()))
            self.assertEqual(stale.exception.code, "stale_generation")
            self.assertEqual(coordinator.capture_status()["snapshot"]["device_state"], "recording")

            mounted = False
            with self.assertRaises(DeviceRecordingError) as lost:
                coordinator.submit_frame(frame())
            self.assertEqual(lost.exception.code, "media_lost")
            partial = self.mountpoint / "sessions" / f"{session_id}.partial"
            self.assertFalse((partial / "manifest.json").exists())
            state = json.loads((partial / "recording.json").read_bytes())
            self.assertEqual(state["state"], "recoverable")
            self.assertEqual(state["storage"]["status"], "media_lost")
            self.assertFalse(state["storage"]["writable"])
        finally:
            mounted = True
            coordinator.close()

    def test_restart_settles_interrupted_partial_without_promoting_it(self) -> None:
        first = self.coordinator()
        first.start_capture(start_command("restart-start"))
        session_id = self.active_session_id(first)
        recording = first.capture_status()["snapshot"]["active_recording"]["recording_state"]
        first.close()
        partial = self.mountpoint / "sessions" / f"{session_id}.partial"
        recording["state"] = "recording"
        recording["diagnostics"] = []
        (partial / "recording.json").write_text(json.dumps(recording), encoding="utf-8")
        (partial / "manifest.json").write_text("{}", encoding="utf-8")

        restarted = self.coordinator()
        try:
            self.assertFalse((partial / "manifest.json").exists())
            settled = json.loads((partial / "recording.json").read_bytes())
            self.assertEqual(settled["state"], "recoverable")
            self.assertEqual(settled["diagnostics"][0]["code"], "process_interrupted")
            self.assertFalse((self.mountpoint / "sessions" / session_id).exists())
            validate_capture_status(restarted.capture_status())
        finally:
            restarted.close()

    def test_restart_completes_only_the_manifest_durable_rename_window(self) -> None:
        first = self.coordinator()
        try:
            session_id = self.seal_one(first, prefix="rename")
        finally:
            first.close()
        final = self.mountpoint / "sessions" / session_id
        partial = final.with_name(f"{session_id}.partial")
        final.rename(partial)

        restarted = self.coordinator()
        try:
            self.assertTrue(final.is_dir())
            self.assertFalse(partial.exists())
            validate_device_session_directory(final)
        finally:
            restarted.close()

    def test_digest_error_is_not_listed_as_a_success(self) -> None:
        first = self.coordinator()
        try:
            session_id = self.seal_one(first, prefix="digest")
        finally:
            first.close()
        video = self.mountpoint / "sessions" / session_id / "video/raw-sbs.mjpeg"
        video.write_bytes(b"tampered")

        restarted = self.coordinator()
        try:
            listed = restarted.list_sessions(cursor=None, limit=50, take_id=None)
            validate_session_list(listed, limit=50, take_id=None)
            self.assertEqual(listed["items"], [])
            self.assertEqual(listed["diagnostics"][0]["code"], "manifest_invalid")
            with self.assertRaises(ArtifactAccessError):
                restarted.open_verified_artifact(session_id, "0" * 64, "v3")
        finally:
            restarted.close()

    def test_digest_change_before_manifest_publish_becomes_failed_partial(self) -> None:
        tampered = False

        def change_after_digest(role: str, payload: bytes) -> None:
            nonlocal tampered
            if role != "manifest" or tampered:
                return
            tampered = True
            active = next((self.mountpoint / "sessions").glob("*.partial"))
            (active / "video/raw-sbs.mjpeg").write_bytes(b"\xff\xd8tampered-content\xff\xd9")

        coordinator = self.coordinator(before_write=change_after_digest)
        try:
            coordinator.start_capture(start_command("digest-race-start"))
            session_id = self.active_session_id(coordinator)
            coordinator.submit_frame(frame())
            with self.assertRaises(ProviderError) as rejected:
                coordinator.stop_capture(stop_command("digest-race-stop"))
            self.assertEqual(rejected.exception.code, "digest_mismatch")
            partial = self.mountpoint / "sessions" / f"{session_id}.partial"
            self.assertTrue(partial.is_dir())
            self.assertFalse((partial / "manifest.json").exists())
            self.assertFalse((self.mountpoint / "sessions" / session_id).exists())
            state = json.loads((partial / "recording.json").read_bytes())
            self.assertEqual(state["state"], "failed")
            self.assertEqual(state["diagnostics"][0]["code"], "digest_mismatch")
        finally:
            coordinator.close()

    def test_post_publish_readback_failure_never_reopens_final_session(self) -> None:
        real_rename = os.rename

        def corrupt_after_publish(source: object, target: object) -> None:
            real_rename(source, target)
            target_path = Path(target)  # type: ignore[arg-type]
            if target_path.parent == self.mountpoint / "sessions":
                video = target_path / "video/raw-sbs.mjpeg"
                payload = bytearray(video.read_bytes())
                payload[2] ^= 1
                video.write_bytes(payload)

        coordinator = self.coordinator()
        try:
            coordinator.start_capture(start_command("post-publish-start"))
            session_id = self.active_session_id(coordinator)
            coordinator.submit_frame(frame())
            with (
                patch("rp_ylx.recording.device_session.os.rename", corrupt_after_publish),
                self.assertRaises(ProviderError) as rejected,
            ):
                coordinator.stop_capture(stop_command("post-publish-stop"))
            self.assertEqual(rejected.exception.code, "digest_mismatch")
            final = self.mountpoint / "sessions" / session_id
            self.assertTrue(final.is_dir())
            self.assertTrue((final / "manifest.json").is_file())
            self.assertFalse(final.with_name(f"{session_id}.partial").exists())
            self.assertIsNotNone(coordinator.retained_unsuccessful_outcome(session_id))
            listed = coordinator.list_sessions(cursor=None, limit=50, take_id=None)
            self.assertEqual(listed["items"], [])
            self.assertEqual(listed["diagnostics"][0]["code"], "manifest_invalid")
        finally:
            coordinator.close()

    def test_safe_swap_waits_for_all_download_handles(self) -> None:
        coordinator = self.coordinator()
        try:
            previous_id = self.seal_one(coordinator, prefix="previous")
            held = coordinator.open_manifest(previous_id, "v3")
            coordinator.start_capture(start_command("swap-start"))
            swap_session = self.active_session_id(coordinator)
            coordinator.submit_frame(frame())
            stop = stop_command("swap-stop", reason="safe_swap")
            with self.assertRaises(ProviderError) as blocked:
                coordinator.stop_capture(stop)
            self.assertEqual(blocked.exception.code, "safe_swap_blocked")
            self.assertEqual(blocked.exception.details["open_handle_count"], 1)
            self.assertIsNone(coordinator.current_safe_swap_receipt())

            held.close()
            result = coordinator.stop_capture(stop)
            self.assertEqual(result.status, 204)
            receipt_resource = coordinator.current_safe_swap_receipt()
            receipt = receipt_resource["receipt"]
            validate_safe_swap_v3_receipt(receipt)
            self.assertEqual(receipt["session_id"], swap_session)
            self.assertEqual(receipt["open_handle_count"], 0)
            self.assertEqual(coordinator.artifact_io_state(), "device-released")
        finally:
            coordinator.close()

    def test_pending_safe_swap_survives_restart_and_requires_zero_handles(self) -> None:
        first = self.coordinator()
        previous_id = self.seal_one(first, prefix="pending-previous")
        held = first.open_manifest(previous_id, "v3")
        first.start_capture(start_command("pending-swap-start"))
        swap_session = self.active_session_id(first)
        first.submit_frame(frame())
        stop = stop_command("pending-swap-stop", reason="safe_swap")
        with self.assertRaises(ProviderError) as blocked:
            first.stop_capture(stop)
        self.assertEqual(blocked.exception.code, "safe_swap_blocked")
        held.close()
        first.close()

        restarted = self.coordinator()
        try:
            self.assertIsNone(restarted.current_safe_swap_receipt())
            completed = restarted.stop_capture(stop_command("resume-safe-swap", reason="safe_swap"))
            self.assertEqual(completed.status, 204)
            receipt = restarted.current_safe_swap_receipt()["receipt"]
            validate_safe_swap_v3_receipt(receipt)
            self.assertEqual(receipt["session_id"], swap_session)
        finally:
            restarted.close()


if __name__ == "__main__":
    unittest.main()

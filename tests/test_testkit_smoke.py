"""Smoke tests verifying all tests/testkit modules, doubles, and harnesses."""

from __future__ import annotations

import importlib
import json
import tempfile
import threading
import unittest
from pathlib import Path

from rp_ylx.native import NativeRecordingPlan
from rp_ylx.recording.device_session import validate_device_session_directory
from rp_ylx.recording.recovery import recover_device_session
from tests.testkit import (
    Cancelled,
    CoordinatorHarness,
    DelayedLock,
    DeploymentHarness,
    DeviceProvider,
    FakeAudioRecorder,
    FakeNmcli,
    FakeSessionStore,
    FakeSources,
    FakeSplitEyeEncoder,
    FakeStereoEncoder,
    FirmwareHarness,
    GatewayHarness,
    MemoryRepresentation,
    NetworkHarness,
    RecoveryFixtureBuilder,
    ShortOutput,
    SplitEyeHarness,
    frame,
    imu_observation,
    single_imu_observation,
    split_eye_frame,
    start_command,
    stop_command,
)


class TestkitSmokeTests(unittest.TestCase):
    def test_all_testkit_modules_import_cleanly(self) -> None:
        for module_name in (
            "tests.testkit",
            "tests.testkit.media",
            "tests.testkit.recording",
            "tests.testkit.coordinator",
            "tests.testkit.gateway",
            "tests.testkit.network",
            "tests.testkit.deployment",
            "tests.testkit.firmware",
        ):
            with self.subTest(module=module_name):
                module = importlib.import_module(module_name)
                self.assertTrue(hasattr(module, "__all__"))

    def test_cancelled_delayed_lock_and_short_output_primitives(self) -> None:
        self.assertTrue(issubclass(Cancelled, BaseException))
        self.assertFalse(issubclass(Cancelled, Exception))

        short_str = ShortOutput()
        self.assertEqual(short_str.write("hello"), 4)
        self.assertEqual(short_str.getvalue(), "hell")  # type: ignore[operator]

        lock = DelayedLock("worker-thread", timeout=2.0)
        errors: list[BaseException] = []

        def worker() -> None:
            try:
                with lock:
                    pass
            except BaseException as exc:  # noqa: BLE001
                errors.append(exc)

        thread = threading.Thread(target=worker, name="worker-thread")
        thread.start()
        self.assertTrue(lock.entered.wait(1.0))
        lock.release.set()
        thread.join(2.0)
        self.assertFalse(thread.is_alive())
        self.assertEqual(errors, [])

    def test_media_and_recording_doubles_instantiate_and_produce_artifacts(self) -> None:
        self.assertEqual(frame(1).frame.source_sequence, 1)
        self.assertEqual(split_eye_frame(2).frame.host_monotonic_ns, 66_000_000)
        self.assertEqual(len(imu_observation(0).samples), 2)
        self.assertEqual(
            len(
                single_imu_observation(
                    host_monotonic_ns=100, accelerometer=(1, 2, 3), gyroscope=(4, 5, 6)
                ).samples
            ),
            1,
        )

        sources = FakeSources()
        sources.start(
            mode="production",
            generation_id="gen-1",
            submit_frame=lambda _: True,
            submit_imu=lambda _: True,
            on_failure=lambda *_: None,
        )
        self.assertEqual(sources.open_handle_count, 2)
        sources.stop()
        self.assertEqual(sources.open_handle_count, 0)

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            encoder = FakeSplitEyeEncoder(root / "video", segment_frames=2)
            self.assertIs(FakeSplitEyeEncoder, FakeStereoEncoder)
            encoder.start()
            encoder.submit(b"jpeg")
            encoder.submit(b"jpeg")
            self.assertEqual(len(encoder.finish()), 1)

            audio = FakeAudioRecorder(root / "session")
            audio.start()
            audio_result = audio.stop()
            self.assertEqual(audio_result["sample_count"], 4_800)

            store = FakeSessionStore()
            tx = store.begin_recording(
                NativeRecordingPlan(
                    session_root=str(root / "tx"),
                    session_id="01989f6a-2c00-7a1b-8c2d-3e4f50617283",
                    encoder_executable="ffmpeg",
                    width=3840,
                    height=1080,
                    fps=30,
                    bitrate_kbps=12_000,
                    segment_frames=3,
                    recording_start_monotonic_ns=1_000_000,
                    audio_enabled=False,
                    audio_device="hw:0,0",
                    audio_sample_rate_hz=48_000,
                    audio_channels=2,
                    audio_segment_seconds=1.0,
                )
            )
            (root / "tx/video").mkdir(parents=True)
            tx.advance(frames=3)
            self.assertEqual(tx.finish(0.1, 1.0)["planner"]["segment_count"], 1)

            recorder, encoders, _ = SplitEyeHarness().build(root / "split")
            self.assertEqual(len(encoders), 0)
            self.assertIsNotNone(recorder)

            recovery_root = root / "recovery"
            recovery_root.mkdir()
            recovered_source = RecoveryFixtureBuilder().recording(recovery_root, frames=6)
            recovered = recover_device_session(recovered_source.partial_path)
            self.assertIsNotNone(recovered)
            assert recovered is not None
            validate_device_session_directory(recovered.path)

    def test_coordinator_harness_seals_session_and_cleans_up(self) -> None:
        with CoordinatorHarness() as harness:
            harness.assert_network_operation_lock_available()
            self.assertEqual(start_command("k1").idempotency_key, "k1")
            self.assertEqual(stop_command("k2").idempotency_key, "k2")
            with harness.coordinator() as coordinator:
                sid = harness.seal_one(coordinator)
                self.assertEqual(
                    len(coordinator.list_sessions(cursor=None, limit=10, take_id=None)["items"]),
                    1,
                )
                cmd = harness.deletion_command(coordinator, {sid})
                self.assertEqual(cmd.body["sessions"][0]["session_id"], sid)
        self.assertFalse(harness.root.exists())

    def test_gateway_harness_serves_requests(self) -> None:
        self.assertIsInstance(DeviceProvider(), DeviceProvider)
        self.assertEqual(MemoryRepresentation(b"abc").size, 3)
        with GatewayHarness() as harness:
            status, payload, _ = harness.request("/api/v3/device", token="reader-token")
            self.assertEqual(status, 200)
            self.assertEqual(json.loads(payload)["schema"], "ylx.device.v3")
        self.assertFalse(harness.thread.is_alive())

    def test_network_harness_and_fake_nmcli(self) -> None:
        with NetworkHarness() as harness:
            self.assertIsInstance(harness.nmcli, FakeNmcli)
            code, payload, _ = harness.run_cli(["network", "status"])
            self.assertEqual(code, 0)
            self.assertIn("devices", payload)
            with harness.controller() as controller:
                status = controller.handle(
                    {"schema": "ylx.network-control-request.v1", "operation": "status"}
                )
                self.assertTrue(status["ok"])
        self.assertFalse(harness.root.exists())

    def test_firmware_harness_checks_update_and_cleans_up(self) -> None:
        with FirmwareHarness() as harness:
            result = harness.supervisor.check()
            self.assertTrue(result["has_update"])
        self.assertFalse(harness.root.exists())

    def test_deployment_harness_builds_bundle_and_manager(self) -> None:
        with DeploymentHarness() as harness:
            bundle = harness.bundle("a")
            self.assertTrue((bundle / "bundle.json").is_file())
            manager = harness.manager()
            self.assertFalse(manager.status()["installed"])
        self.assertFalse(harness.root.exists())

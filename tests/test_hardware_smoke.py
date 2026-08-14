from __future__ import annotations

import io
import json
import struct
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from PIL import Image

from rp_ylx.camera import CameraDescriptor, CameraMode, StereoFrame, SyntheticCameraBackend
from rp_ylx.contracts import validate_session
from rp_ylx.hardware import HardwareSmokeError, record_hardware_smoke
from rp_ylx.imu import ImuError, ImuPacketRead, SyntheticImuSource

MODE = CameraMode(3840, 1080, 60.0, "mjpg")


def jpeg(gray: int) -> bytes:
    output = io.BytesIO()
    Image.new("L", (8, 4), gray).save(output, format="JPEG")
    return output.getvalue()


def imu_packet(timestamp: int, host_ns: int) -> ImuPacketRead:
    axes = tuple(range(12))
    return ImuPacketRead(
        timestamp.to_bytes(3, "big") + struct.pack(">12h", *axes),
        host_read_start_ns=host_ns - 100,
        host_read_end_ns=host_ns + 100,
    )


class HardwareSmokeBehaviorTest(unittest.TestCase):
    def test_camera_read_failure_survives_eager_cleanup_failure(self) -> None:
        descriptor = CameraDescriptor("camera-rdk", "/dev/video0", "YLX 2UQ2", (MODE,))

        class ReadAndCleanupFailureStream:
            def __init__(self) -> None:
                self.close_attempted = False

            def start(self) -> None:
                pass

            def read(self, timeout: float) -> StereoFrame:
                raise TimeoutError("fixture camera stalled")

            def stop(self) -> None:
                pass

            def close(self) -> None:
                self.close_attempted = True
                raise OSError("fixture camera close failed")

        stream = ReadAndCleanupFailureStream()

        class ReadAndCleanupFailureBackend:
            def discover(self) -> tuple[CameraDescriptor, ...]:
                return (descriptor,)

            def open(
                self, selected: CameraDescriptor, mode: CameraMode
            ) -> ReadAndCleanupFailureStream:
                return stream

        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "smoke"
            with self.assertRaises(HardwareSmokeError) as raised:
                record_hardware_smoke(
                    output=output,
                    camera_backend=ReadAndCleanupFailureBackend(),
                    imu_source_factory=lambda selected: SyntheticImuSource([]),
                    device=Path("/dev/video0"),
                    mode=MODE,
                    covered_eye="left",
                    frames=1,
                    imu_packets=1,
                    software_version="test",
                    evidence_kind="fixture",
                )

            self.assertEqual(raised.exception.code, "frame_timeout")
            self.assertTrue(stream.close_attempted)
            partial = next(output.glob("*.partial"))
            self.assertEqual(
                json.loads((partial / "session.json").read_text())["state"], "interrupted"
            )

    def test_imu_read_failure_survives_eager_cleanup_failure(self) -> None:
        descriptor = CameraDescriptor("camera-rdk", "/dev/video0", "YLX 2UQ2", (MODE,))
        backend = SyntheticCameraBackend(
            (descriptor,),
            frames={descriptor.stable_id: [StereoFrame(10, 1_000_000, jpeg(0), jpeg(240))]},
        )

        class ReadAndCleanupFailureSource(SyntheticImuSource):
            def close(self) -> None:
                self.closed = True
                raise OSError("fixture IMU close failed")

        imu_source = ReadAndCleanupFailureSource([TimeoutError("fixture IMU stalled")])
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "smoke"
            with self.assertRaises(HardwareSmokeError) as raised:
                record_hardware_smoke(
                    output=output,
                    camera_backend=backend,
                    imu_source_factory=lambda selected: imu_source,
                    device=Path("/dev/video0"),
                    mode=MODE,
                    covered_eye="left",
                    frames=1,
                    imu_packets=1,
                    software_version="test",
                    evidence_kind="fixture",
                )

            self.assertEqual(raised.exception.code, "sensor_stalled")
            self.assertTrue(imu_source.closed)
            self.assertTrue(backend.opened_streams[0].closed)
            partial = next(output.glob("*.partial"))
            self.assertEqual(
                json.loads((partial / "session.json").read_text())["state"], "interrupted"
            )

    def test_imu_cleanup_failure_has_a_stable_error_and_still_closes_camera(self) -> None:
        descriptor = CameraDescriptor("camera-rdk", "/dev/video0", "YLX 2UQ2", (MODE,))
        backend = SyntheticCameraBackend(
            (descriptor,),
            frames={descriptor.stable_id: [StereoFrame(10, 1_000_000, jpeg(0), jpeg(240))]},
        )

        class CleanupFailureSource(SyntheticImuSource):
            def close(self) -> None:
                self.closed = True
                raise OSError("fixture IMU close failed")

        imu_source = CleanupFailureSource([imu_packet(1_000, 1_500_000)])
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "smoke"
            with self.assertRaises(HardwareSmokeError) as raised:
                record_hardware_smoke(
                    output=output,
                    camera_backend=backend,
                    imu_source_factory=lambda selected: imu_source,
                    device=Path("/dev/video0"),
                    mode=MODE,
                    covered_eye="left",
                    frames=1,
                    imu_packets=1,
                    software_version="test",
                    evidence_kind="fixture",
                )

            self.assertEqual(raised.exception.code, "imu_cleanup_failed")
            self.assertTrue(imu_source.closed)
            self.assertTrue(backend.opened_streams[0].closed)
            partial = next(output.glob("*.partial"))
            self.assertEqual(
                json.loads((partial / "session.json").read_text())["state"], "interrupted"
            )

    def test_camera_cleanup_failure_has_a_stable_error_and_still_closes_imu(self) -> None:
        descriptor = CameraDescriptor("camera-rdk", "/dev/video0", "YLX 2UQ2", (MODE,))

        class CleanupFailureStream:
            def __init__(self) -> None:
                self.close_attempted = False

            def start(self) -> None:
                pass

            def read(self, timeout: float) -> StereoFrame:
                return StereoFrame(10, 1_000_000, jpeg(0), jpeg(240))

            def stop(self) -> None:
                pass

            def close(self) -> None:
                self.close_attempted = True
                raise OSError("fixture camera close failed")

        stream = CleanupFailureStream()

        class CleanupFailureBackend:
            def discover(self) -> tuple[CameraDescriptor, ...]:
                return (descriptor,)

            def open(self, selected: CameraDescriptor, mode: CameraMode) -> CleanupFailureStream:
                return stream

        imu_source = SyntheticImuSource([imu_packet(1_000, 1_500_000)])
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "smoke"
            with self.assertRaises(HardwareSmokeError) as raised:
                record_hardware_smoke(
                    output=output,
                    camera_backend=CleanupFailureBackend(),
                    imu_source_factory=lambda selected: imu_source,
                    device=Path("/dev/video0"),
                    mode=MODE,
                    covered_eye="left",
                    frames=1,
                    imu_packets=1,
                    software_version="test",
                    evidence_kind="fixture",
                )

            self.assertEqual(raised.exception.code, "camera_cleanup_failed")
            self.assertTrue(stream.close_attempted)
            self.assertTrue(imu_source.closed)
            partial = next(output.glob("*.partial"))
            self.assertEqual(
                json.loads((partial / "session.json").read_text())["state"], "interrupted"
            )

    def test_primary_failure_survives_both_cleanup_failures(self) -> None:
        descriptor = CameraDescriptor("camera-rdk", "/dev/video0", "YLX 2UQ2", (MODE,))

        class CleanupFailureStream:
            def __init__(self) -> None:
                self.close_attempted = False

            def start(self) -> None:
                pass

            def read(self, timeout: float) -> StereoFrame:
                return StereoFrame(10, 1_000_000, jpeg(220), jpeg(10))

            def stop(self) -> None:
                pass

            def close(self) -> None:
                self.close_attempted = True
                raise OSError("fixture camera close failed")

        stream = CleanupFailureStream()

        class CleanupFailureBackend:
            def discover(self) -> tuple[CameraDescriptor, ...]:
                return (descriptor,)

            def open(self, selected: CameraDescriptor, mode: CameraMode) -> CleanupFailureStream:
                return stream

        class CleanupFailureSource(SyntheticImuSource):
            def close(self) -> None:
                self.closed = True
                raise OSError("fixture IMU close failed")

        imu_source = CleanupFailureSource([imu_packet(1_000, 1_500_000)])
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "smoke"
            with self.assertRaises(HardwareSmokeError) as raised:
                record_hardware_smoke(
                    output=output,
                    camera_backend=CleanupFailureBackend(),
                    imu_source_factory=lambda selected: imu_source,
                    device=Path("/dev/video0"),
                    mode=MODE,
                    covered_eye="left",
                    frames=1,
                    imu_packets=1,
                    software_version="test",
                    evidence_kind="fixture",
                )

            self.assertEqual(raised.exception.code, "stereo_orientation_failed")
            self.assertTrue(imu_source.closed)
            self.assertTrue(stream.close_attempted)
            partial = next(output.glob("*.partial"))
            self.assertEqual(
                json.loads((partial / "session.json").read_text())["state"], "interrupted"
            )

    def test_camera_discovery_failure_is_a_stable_smoke_failure(self) -> None:
        class DiscoveryFailureBackend:
            def discover(self) -> tuple[CameraDescriptor, ...]:
                raise OSError("fixture sysfs unavailable")

            def open(self, selected: CameraDescriptor, mode: CameraMode) -> None:
                raise AssertionError("open must not run after discovery failure")

        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "smoke"
            with self.assertRaises(HardwareSmokeError) as raised:
                record_hardware_smoke(
                    output=output,
                    camera_backend=DiscoveryFailureBackend(),
                    imu_source_factory=lambda selected: SyntheticImuSource([]),
                    device=Path("/dev/video0"),
                    mode=MODE,
                    covered_eye="left",
                    frames=1,
                    imu_packets=1,
                    software_version="test",
                    evidence_kind="fixture",
                )

            self.assertEqual(raised.exception.code, "camera_discovery_failed")
            self.assertFalse(output.exists())

    def test_unavailable_output_parent_is_an_explicit_smoke_failure(self) -> None:
        descriptor = CameraDescriptor("camera-rdk", "/dev/video0", "YLX 2UQ2", (MODE,))
        backend = SyntheticCameraBackend((descriptor,))

        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "missing-parent" / "smoke"
            with self.assertRaises(HardwareSmokeError) as raised:
                record_hardware_smoke(
                    output=output,
                    camera_backend=backend,
                    imu_source_factory=lambda selected: SyntheticImuSource([]),
                    device=Path("/dev/video0"),
                    mode=MODE,
                    covered_eye="left",
                    frames=1,
                    imu_packets=1,
                    software_version="test",
                    evidence_kind="fixture",
                )

            self.assertEqual(raised.exception.code, "output_create_failed")
            self.assertFalse(output.exists())

    def test_missing_camera_is_explicit_before_a_session_is_created(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "smoke"
            with self.assertRaises(HardwareSmokeError) as raised:
                record_hardware_smoke(
                    output=output,
                    camera_backend=SyntheticCameraBackend(()),
                    imu_source_factory=lambda selected: SyntheticImuSource([]),
                    device=Path("/dev/video0"),
                    mode=MODE,
                    covered_eye="left",
                    frames=1,
                    imu_packets=1,
                    software_version="test",
                    evidence_kind="fixture",
                )

            self.assertEqual(raised.exception.code, "camera_missing")
            self.assertFalse(output.exists())

    def test_seals_and_validates_a_stereo_imu_session_without_claiming_hardware(self) -> None:
        descriptor = CameraDescriptor("camera-rdk", "/dev/video0", "YLX 2UQ2", (MODE,))
        backend = SyntheticCameraBackend(
            (descriptor,),
            frames={
                descriptor.stable_id: [
                    StereoFrame(10, 1_000_000, jpeg(0), jpeg(240)),
                    StereoFrame(11, 2_000_000, jpeg(0), jpeg(240)),
                ]
            },
        )
        imu_source = SyntheticImuSource(
            [imu_packet(1_000, 1_500_000), imu_packet(2_000, 2_500_000)]
        )

        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "smoke"
            result = record_hardware_smoke(
                output=output,
                camera_backend=backend,
                imu_source_factory=lambda selected: imu_source,
                device=Path("/dev/video0"),
                mode=MODE,
                covered_eye="left",
                frames=2,
                imu_packets=2,
                software_version="test",
                evidence_kind="fixture",
            )

            session = output / result["session"]["directory"]
            manifest = validate_session(session)
            self.assertEqual(manifest["counts"]["frames"], 2)
            self.assertEqual(manifest["counts"]["imu_samples"], 4)
            self.assertEqual(result["evidence"]["kind"], "fixture")
            self.assertTrue(result["stereo_orientation"]["passed"])
            self.assertEqual(
                result["camera"],
                {
                    "frames_recorded": 2,
                    "first_source_sequence": 10,
                    "last_source_sequence": 11,
                    "first_host_monotonic_ns": 1_000_000,
                    "last_host_monotonic_ns": 2_000_000,
                    "dropped_frames": 0,
                    "timestamps_monotonic": True,
                },
            )
            self.assertEqual(result["imu"]["packets_recorded"], 2)
            self.assertEqual(result["imu"]["samples_recorded"], 4)
            self.assertEqual(result["imu"]["first_device_ticks"], 1_000)
            self.assertEqual(result["imu"]["last_device_ticks"], 2_000)
            self.assertEqual(result["imu"]["sync_quality_samples"], {"insufficient": 4})
            self.assertTrue(result["imu"]["timestamps_monotonic"])
            self.assertIsNone(result["imu"]["xu_unit"])
            self.assertEqual(json.loads((output / "summary.json").read_text()), result)
            self.assertTrue(backend.opened_streams[0].closed)
            self.assertTrue(imu_source.closed)

    def test_independent_session_validation_failure_is_stable(self) -> None:
        descriptor = CameraDescriptor("camera-rdk", "/dev/video0", "YLX 2UQ2", (MODE,))
        backend = SyntheticCameraBackend(
            (descriptor,),
            frames={descriptor.stable_id: [StereoFrame(10, 1_000_000, jpeg(0), jpeg(240))]},
        )
        imu_source = SyntheticImuSource([imu_packet(1_000, 1_500_000)])

        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "smoke"
            with (
                patch(
                    "rp_ylx.hardware.smoke.validate_session",
                    side_effect=OSError("fixture validator unavailable"),
                ),
                self.assertRaises(HardwareSmokeError) as raised,
            ):
                record_hardware_smoke(
                    output=output,
                    camera_backend=backend,
                    imu_source_factory=lambda selected: imu_source,
                    device=Path("/dev/video0"),
                    mode=MODE,
                    covered_eye="left",
                    frames=1,
                    imu_packets=1,
                    software_version="test",
                    evidence_kind="fixture",
                )

            self.assertEqual(raised.exception.code, "session_validation_failed")
            self.assertFalse((output / "summary.json").exists())
            partials = list(output.glob("*.partial"))
            self.assertEqual(len(partials), 1)
            self.assertEqual(
                json.loads((partials[0] / "session.json").read_text())["state"],
                "interrupted",
            )
            self.assertFalse((partials[0] / "manifest.json").exists())
            self.assertEqual(
                [path for path in output.iterdir() if not path.name.endswith(".partial")], []
            )

    def test_summary_write_failure_keeps_an_interrupted_partial(self) -> None:
        descriptor = CameraDescriptor("camera-rdk", "/dev/video0", "YLX 2UQ2", (MODE,))
        backend = SyntheticCameraBackend(
            (descriptor,),
            frames={descriptor.stable_id: [StereoFrame(10, 1_000_000, jpeg(0), jpeg(240))]},
        )
        imu_source = SyntheticImuSource([imu_packet(1_000, 1_500_000)])

        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "smoke"
            with (
                patch.object(Path, "write_text", side_effect=OSError("fixture summary disk full")),
                self.assertRaises(HardwareSmokeError) as raised,
            ):
                record_hardware_smoke(
                    output=output,
                    camera_backend=backend,
                    imu_source_factory=lambda selected: imu_source,
                    device=Path("/dev/video0"),
                    mode=MODE,
                    covered_eye="left",
                    frames=1,
                    imu_packets=1,
                    software_version="test",
                    evidence_kind="fixture",
                )

            self.assertEqual(raised.exception.code, "summary_write_failed")
            self.assertFalse((output / "summary.json").exists())
            partials = list(output.glob("*.partial"))
            self.assertEqual(len(partials), 1)
            self.assertEqual(
                json.loads((partials[0] / "session.json").read_text())["state"],
                "interrupted",
            )
            self.assertFalse((partials[0] / "manifest.json").exists())
            self.assertEqual(
                [path for path in output.iterdir() if not path.name.endswith(".partial")], []
            )

    def test_camera_start_failure_is_explicit_and_keeps_an_interrupted_session(self) -> None:
        descriptor = CameraDescriptor("camera-rdk", "/dev/video0", "YLX 2UQ2", (MODE,))

        class StartFailureStream:
            closed = False

            def start(self) -> None:
                raise OSError("fixture stream-on failed")

            def read(self, timeout: float) -> StereoFrame:
                raise AssertionError("read must not run after start failure")

            def stop(self) -> None:
                pass

            def close(self) -> None:
                self.closed = True

        class StartFailureBackend:
            def __init__(self) -> None:
                self.stream = StartFailureStream()

            def discover(self) -> tuple[CameraDescriptor, ...]:
                return (descriptor,)

            def open(self, selected: CameraDescriptor, mode: CameraMode) -> StartFailureStream:
                return self.stream

        backend = StartFailureBackend()

        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "smoke"
            with self.assertRaises(HardwareSmokeError) as raised:
                record_hardware_smoke(
                    output=output,
                    camera_backend=backend,
                    imu_source_factory=lambda selected: SyntheticImuSource([]),
                    device=Path("/dev/video0"),
                    mode=MODE,
                    covered_eye="left",
                    frames=1,
                    imu_packets=1,
                    software_version="test",
                    evidence_kind="fixture",
                )

            self.assertEqual(raised.exception.code, "camera_start_failed")
            partials = list(output.glob("*.partial"))
            self.assertEqual(len(partials), 1)
            state = json.loads((partials[0] / "session.json").read_text())["state"]
            self.assertEqual(state, "interrupted")
            self.assertTrue(backend.stream.closed)

    def test_camera_open_failure_is_a_stable_smoke_failure(self) -> None:
        descriptor = CameraDescriptor("camera-rdk", "/dev/video0", "YLX 2UQ2", (MODE,))

        class OpenFailureBackend:
            def discover(self) -> tuple[CameraDescriptor, ...]:
                return (descriptor,)

            def open(self, selected: CameraDescriptor, mode: CameraMode) -> None:
                raise PermissionError("fixture permission denied")

        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "smoke"
            with self.assertRaises(HardwareSmokeError) as raised:
                record_hardware_smoke(
                    output=output,
                    camera_backend=OpenFailureBackend(),
                    imu_source_factory=lambda selected: SyntheticImuSource([]),
                    device=Path("/dev/video0"),
                    mode=MODE,
                    covered_eye="left",
                    frames=1,
                    imu_packets=1,
                    software_version="test",
                    evidence_kind="fixture",
                )

            self.assertEqual(raised.exception.code, "permission_denied")
            partials = list(output.glob("*.partial"))
            self.assertEqual(len(partials), 1)
            state = json.loads((partials[0] / "session.json").read_text())["state"]
            self.assertEqual(state, "interrupted")

    def test_missing_imu_is_explicit_and_releases_the_started_camera(self) -> None:
        descriptor = CameraDescriptor("camera-rdk", "/dev/video0", "YLX 2UQ2", (MODE,))
        backend = SyntheticCameraBackend(
            (descriptor,),
            frames={descriptor.stable_id: [StereoFrame(10, 1_000_000, jpeg(0), jpeg(240))]},
        )

        def missing_imu(selected: CameraDescriptor) -> SyntheticImuSource:
            raise ImuError("xu_not_found", "fixture has no YLX XU")

        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "smoke"
            with self.assertRaises(HardwareSmokeError) as raised:
                record_hardware_smoke(
                    output=output,
                    camera_backend=backend,
                    imu_source_factory=missing_imu,
                    device=Path("/dev/video0"),
                    mode=MODE,
                    covered_eye="left",
                    frames=1,
                    imu_packets=1,
                    software_version="test",
                    evidence_kind="fixture",
                )

            self.assertEqual(raised.exception.code, "imu_missing")
            self.assertTrue(backend.opened_streams[0].closed)
            partials = list(output.glob("*.partial"))
            self.assertEqual(len(partials), 1)
            state = json.loads((partials[0] / "session.json").read_text())["state"]
            self.assertEqual(state, "interrupted")

    def test_wrong_stereo_orientation_is_rejected_instead_of_sealed(self) -> None:
        descriptor = CameraDescriptor("camera-rdk", "/dev/video0", "YLX 2UQ2", (MODE,))
        backend = SyntheticCameraBackend(
            (descriptor,),
            frames={descriptor.stable_id: [StereoFrame(10, 1_000_000, jpeg(220), jpeg(10))]},
        )
        imu_source = SyntheticImuSource([imu_packet(1_000, 1_500_000)])

        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "smoke"
            with self.assertRaises(HardwareSmokeError) as raised:
                record_hardware_smoke(
                    output=output,
                    camera_backend=backend,
                    imu_source_factory=lambda selected: imu_source,
                    device=Path("/dev/video0"),
                    mode=MODE,
                    covered_eye="left",
                    frames=1,
                    imu_packets=1,
                    software_version="test",
                    evidence_kind="fixture",
                )

            self.assertEqual(raised.exception.code, "stereo_orientation_failed")
            self.assertFalse((output / "summary.json").exists())
            self.assertEqual(len(list(output.glob("*.partial"))), 1)
            sealed = [path for path in output.iterdir() if not path.name.endswith(".partial")]
            self.assertEqual(sealed, [])
            self.assertTrue(backend.opened_streams[0].closed)
            self.assertTrue(imu_source.closed)

    def test_camera_timestamp_regression_is_a_stable_smoke_failure(self) -> None:
        descriptor = CameraDescriptor("camera-rdk", "/dev/video0", "YLX 2UQ2", (MODE,))
        backend = SyntheticCameraBackend(
            (descriptor,),
            frames={
                descriptor.stable_id: [
                    StereoFrame(10, 2_000_000, jpeg(0), jpeg(240)),
                    StereoFrame(11, 1_000_000, jpeg(0), jpeg(240)),
                ]
            },
        )
        imu_source = SyntheticImuSource(
            [imu_packet(1_000, 1_500_000), imu_packet(2_000, 2_500_000)]
        )

        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "smoke"
            with self.assertRaises(HardwareSmokeError) as raised:
                record_hardware_smoke(
                    output=output,
                    camera_backend=backend,
                    imu_source_factory=lambda selected: imu_source,
                    device=Path("/dev/video0"),
                    mode=MODE,
                    covered_eye="left",
                    frames=2,
                    imu_packets=2,
                    software_version="test",
                    evidence_kind="fixture",
                )

            self.assertEqual(raised.exception.code, "timestamp_regression")
            self.assertEqual(len(list(output.glob("*.partial"))), 1)
            self.assertTrue(backend.opened_streams[0].closed)
            self.assertTrue(imu_source.closed)

    def test_recording_seal_failure_is_a_stable_smoke_failure(self) -> None:
        descriptor = CameraDescriptor("camera-rdk", "/dev/video0", "YLX 2UQ2", (MODE,))
        output: Path

        class ReadOnlyOnStopStream:
            def __init__(self) -> None:
                self.closed = False

            def start(self) -> None:
                pass

            def read(self, timeout: float) -> StereoFrame:
                return StereoFrame(10, 1_000_000, jpeg(0), jpeg(240))

            def stop(self) -> None:
                staging = next(output.parent.glob(f".{output.name}.*.partial"))
                partial = next(staging.glob("*.partial"))
                partial.chmod(0o500)

            def close(self) -> None:
                self.closed = True

        stream = ReadOnlyOnStopStream()

        class ReadOnlyOnStopBackend:
            def discover(self) -> tuple[CameraDescriptor, ...]:
                return (descriptor,)

            def open(self, selected: CameraDescriptor, mode: CameraMode) -> ReadOnlyOnStopStream:
                return stream

        imu_source = SyntheticImuSource([imu_packet(1_000, 1_500_000)])
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "smoke"
            try:
                with self.assertRaises(HardwareSmokeError) as raised:
                    record_hardware_smoke(
                        output=output,
                        camera_backend=ReadOnlyOnStopBackend(),
                        imu_source_factory=lambda selected: imu_source,
                        device=Path("/dev/video0"),
                        mode=MODE,
                        covered_eye="left",
                        frames=1,
                        imu_packets=1,
                        software_version="test",
                        evidence_kind="fixture",
                    )
            finally:
                for partial in output.glob("*.partial"):
                    partial.chmod(0o700)

            self.assertEqual(raised.exception.code, "seal_failed")
            self.assertFalse((output / "summary.json").exists())
            self.assertTrue(stream.closed)
            self.assertTrue(imu_source.closed)

    def test_fixture_adapters_cannot_claim_physical_hardware(self) -> None:
        descriptor = CameraDescriptor("camera-rdk", "/dev/video0", "YLX 2UQ2", (MODE,))
        backend = SyntheticCameraBackend(
            (descriptor,),
            frames={descriptor.stable_id: [StereoFrame(10, 1_000_000, jpeg(0), jpeg(240))]},
        )
        imu_source = SyntheticImuSource([imu_packet(1_000, 1_500_000)])
        facts = {
            "format": "ylx.hardware-probe.v0",
            "observed_at": "2026-08-12T00:00:00Z",
            "platform": {
                "machine": "aarch64",
                "kernel": "6.1.83",
                "model": "D-Robotics RDK X5 V1.0",
                "os_release": {"PRETTY_NAME": "Ubuntu 22.04.5 LTS"},
            },
            "target": {
                "board": "rdk_x5_v1.0",
                "camera": "ylx_2uq2",
                "supported": True,
                "reason": "matched",
            },
            "usb_devices": [
                {
                    "vendor_id": "1bcf",
                    "product_id": "0b15",
                    "device_release_bcd": "0100",
                    "product": "YLX 2UQ2",
                }
            ],
        }

        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "smoke"
            with self.assertRaises(HardwareSmokeError) as raised:
                record_hardware_smoke(
                    output=output,
                    camera_backend=backend,
                    imu_source_factory=lambda selected: imu_source,
                    device=Path("/dev/video0"),
                    mode=MODE,
                    covered_eye="left",
                    frames=1,
                    imu_packets=1,
                    software_version="test",
                    evidence_kind="hardware",
                    hardware_facts=facts,
                )

            self.assertEqual(raised.exception.code, "fixture_evidence_forbidden")
            self.assertFalse(output.exists())


if __name__ == "__main__":
    unittest.main()

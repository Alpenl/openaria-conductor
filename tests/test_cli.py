from __future__ import annotations

import io
import json
import tempfile
import unittest
import uuid
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from unittest.mock import patch

from rp_ylx import __commit__, __version__
from rp_ylx.camera import CameraError, CameraMode, FrameObservation, StereoFrame
from rp_ylx.cli import build_parser, main
from rp_ylx.hardware import HardwareSmokeError
from rp_ylx.recording import (
    DeviceSessionConfig,
    DeviceSessionRecorder,
    SessionPlan,
    StorageStatus,
    uuid7,
)


class CliTest(unittest.TestCase):
    def produce_v1(self, root: Path) -> Path:
        session_id = uuid7()
        revision = 0

        def allocate_revision() -> int:
            nonlocal revision
            revision += 1
            return revision

        recorder = DeviceSessionRecorder(
            root,
            DeviceSessionConfig(
                device_id=str(uuid.uuid4()),
                device_label="YLX-12AB34CD",
                hardware_fingerprint="sha256:" + "a" * 64,
                platform="D-Robotics RDK X5 V1.0 + YLX 2UQ2",
                software_version="0.5.0",
                commit="b" * 40,
                width=3840,
                height=1080,
                sensor_fps=60.0,
            ),
            SessionPlan(
                session_id=session_id,
                volume_id=str(uuid.uuid4()),
                generation_id=str(uuid.uuid4()),
                capture_mode="production",
                display_name="CLI v1 fixture",
                take_id=uuid7(),
                take_sequence=1,
                continuation_of=None,
            ),
            authority_epoch=str(uuid.uuid4()),
            allocate_revision=allocate_revision,
            storage_status=lambda: StorageStatus(1024 * 1024, True),
        )
        recorder.start()
        recorder.submit_frame(
            FrameObservation(
                StereoFrame(
                    source_sequence=0,
                    host_monotonic_ns=1,
                    left=b"left",
                    right=b"right",
                    raw_side_by_side=b"\xff\xd8cli-v1\xff\xd9",
                ),
                dropped_before=0,
            )
        )
        return recorder.stop().path

    def test_production_service_failure_is_machine_readable(self) -> None:
        error = io.StringIO()
        failure = OSError("missing volume")
        with (
            patch("rp_ylx.daemon.run_production_service", side_effect=failure),
            redirect_stderr(error),
        ):
            self.assertEqual(main(["serve", "--config", "/etc/rp-ylx/device.json"]), 2)
        rendered = json.loads(error.getvalue())
        self.assertEqual(rendered["error"]["code"], "service_start_failed")
        self.assertNotIn("Traceback", error.getvalue())

    def test_volume_init_is_explicit_and_machine_readable(self) -> None:
        output = io.StringIO()
        with (
            patch(
                "rp_ylx.recording.initialize_capture_volume",
                return_value="550e8400-e29b-41d4-a716-446655440000",
            ) as initialize,
            redirect_stdout(output),
        ):
            self.assertEqual(main(["volume", "init", "/mnt/recording"]), 0)
        initialize.assert_called_once_with("/mnt/recording")
        self.assertTrue(json.loads(output.getvalue())["ok"])

    def test_hardware_preview_parser_has_real_camera_defaults(self) -> None:
        args = build_parser().parse_args(["serve-hardware-preview"])
        self.assertEqual(
            (
                args.device,
                args.host,
                args.port,
                args.width,
                args.height,
                args.fps,
                args.allow_origin,
            ),
            ("/dev/video0", "127.0.0.1", 8080, 3840, 1080, 60, []),
        )

    def test_hardware_preview_starts_real_controller_and_closes_on_interrupt(self) -> None:
        class FakeServer:
            server_port = 9123

            def __init__(self) -> None:
                self.closed = False

            def serve_forever(self) -> None:
                raise KeyboardInterrupt

            def server_close(self) -> None:
                self.closed = True

        class FakePump:
            def __init__(self) -> None:
                self.started = False
                self.stopped = False

            def start(self) -> None:
                self.started = True

            def stop(self) -> None:
                self.stopped = True

        device = object()
        controller = object()
        backend = object()
        server = FakeServer()
        pump = FakePump()
        output = io.StringIO()
        with (
            patch("rp_ylx.cli.MockDevice", return_value=device) as mock_device,
            patch("rp_ylx.cli.V4L2DiscoveryBackend", return_value=backend) as backend_factory,
            patch("rp_ylx.cli.CameraController", return_value=controller) as controller_factory,
            patch("rp_ylx.cli.stable_id_for_device", return_value="camera-a"),
            patch("rp_ylx.cli.CameraPreviewPump", return_value=pump) as pump_factory,
            patch("rp_ylx.cli.create_server", return_value=server) as server_factory,
            redirect_stdout(output),
        ):
            result = main(
                [
                    "serve-hardware-preview",
                    "--device",
                    "/dev/video9",
                    "--host",
                    "0.0.0.0",
                    "--port",
                    "9123",
                    "--allow-origin",
                    "http://127.0.0.1:4173",
                ]
            )

        self.assertEqual(result, 0)
        mock_device.assert_called_once_with()
        backend_factory.assert_called_once_with()
        controller_factory.assert_called_once_with(backend)
        pump_factory.assert_called_once_with(
            device,
            controller,
            CameraMode(3840, 1080, 60.0, "mjpg"),
            stable_id="camera-a",
        )
        server_factory.assert_called_once_with(
            "0.0.0.0", 9123, device, allowed_origins=["http://127.0.0.1:4173"]
        )
        self.assertTrue(pump.started)
        self.assertTrue(pump.stopped)
        self.assertTrue(server.closed)
        self.assertIn("http://0.0.0.0:9123/api/v0", output.getvalue())

    def test_hardware_preview_start_error_is_observable_and_returns_failure(self) -> None:
        class FakePump:
            def start(self) -> None:
                raise CameraError("open_failed", "camera unavailable", retryable=True)

            def stop(self) -> None:
                self.stopped = True

        error = io.StringIO()
        with (
            patch("rp_ylx.cli.stable_id_for_device", return_value="camera-a"),
            patch("rp_ylx.cli.CameraPreviewPump", return_value=FakePump()),
            patch("rp_ylx.cli.CameraController"),
            patch("rp_ylx.cli.V4L2DiscoveryBackend"),
            redirect_stderr(error),
        ):
            result = main(["serve-hardware-preview"])

        self.assertEqual(result, 2)
        self.assertIn("hardware preview failed", error.getvalue())
        self.assertIn("open_failed", error.getvalue())

    def test_version_contains_packaged_commit(self) -> None:
        output = io.StringIO()
        with redirect_stdout(output):
            self.assertEqual(main(["--version"]), 0)
        self.assertEqual(output.getvalue().strip(), f"rp-ylx {__version__} ({__commit__})")

    def test_status_runs_without_hardware(self) -> None:
        output = io.StringIO()
        with redirect_stdout(output):
            self.assertEqual(main(["status"]), 0)
        status = json.loads(output.getvalue())
        self.assertEqual(status["commit"], __commit__)
        self.assertEqual(status["hardware"], "not-probed")
        self.assertEqual(status["recording"], "idle")

    def test_validate_valid_session(self) -> None:
        session_id = "0198c9a8-7a3c-7000-8000-000000000001"
        session = (
            Path(__file__).resolve().parents[1] / "contracts" / "examples" / "valid" / session_id
        )
        output = io.StringIO()
        with redirect_stdout(output):
            self.assertEqual(main(["validate", str(session)]), 0)
        self.assertEqual(json.loads(output.getvalue())["session_id"], session_id)

    def test_validate_dispatches_real_producer_v1(self) -> None:
        output = io.StringIO()
        with tempfile.TemporaryDirectory() as directory, redirect_stdout(output):
            session = self.produce_v1(Path(directory))
            self.assertEqual(main(["validate", str(session)]), 0)
        rendered = json.loads(output.getvalue())
        self.assertTrue(rendered["valid"])
        self.assertEqual(rendered["session_id"], session.name)

    def test_validate_v1_rejects_artifact_symlink_even_when_bytes_match(self) -> None:
        error = io.StringIO()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            session = self.produce_v1(root)
            manifest = json.loads((session / "manifest.json").read_bytes())
            artifact = session / manifest["video"]["artifact"]["path"]
            outside = root / "outside.mjpeg"
            artifact.replace(outside)
            artifact.symlink_to(outside)
            with redirect_stderr(error):
                self.assertEqual(main(["validate", str(session)]), 2)
        rendered = json.loads(error.getvalue())
        self.assertFalse(rendered["valid"])
        self.assertIn(rendered["error"]["code"], {"artifact_invalid", "manifest_invalid"})

    def test_validate_rejects_missing_unknown_and_conflicting_discriminators(self) -> None:
        cases = (
            ({"sealed": True}, "missing_discriminator"),
            ({"schema": "ylx.device-session.v9"}, "unsupported_version"),
            (
                {
                    "schema": "ylx.device-session.v1",
                    "format": "ylx.recording-session.v0",
                },
                "conflicting_discriminator",
            ),
        )
        for manifest, expected in cases:
            with self.subTest(expected=expected), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                (root / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
                error = io.StringIO()
                with redirect_stderr(error):
                    self.assertEqual(main(["validate", str(root)]), 2)
                self.assertEqual(json.loads(error.getvalue())["error"]["code"], expected)

    def test_hardware_smoke_uses_product_pipeline_and_prints_json(self) -> None:
        output = io.StringIO()
        facts = {"target": {"supported": True}}
        summary = {"format": "ylx.hardware-smoke.v0", "session": {"validated": True}}
        with (
            tempfile.TemporaryDirectory() as directory,
            patch("rp_ylx.cli.collect_hardware_facts", return_value=facts) as probe,
            patch("rp_ylx.cli.record_hardware_smoke", return_value=summary) as smoke,
            redirect_stdout(output),
        ):
            target = Path(directory) / "smoke"
            result = main(
                [
                    "hardware-smoke",
                    "--device",
                    "/dev/video9",
                    "--covered-eye",
                    "right",
                    "--frames",
                    "3",
                    "--imu-packets",
                    "2",
                    "--output",
                    str(target),
                ]
            )

        self.assertEqual(result, 0)
        rendered = json.loads(output.getvalue())
        self.assertTrue(rendered["ok"])
        self.assertTrue(rendered["session"]["validated"])
        probe.assert_called_once_with(storage_path=target.parent)
        self.assertEqual(smoke.call_args.kwargs["output"], target)
        self.assertEqual(smoke.call_args.kwargs["device"], Path("/dev/video9"))
        self.assertEqual(smoke.call_args.kwargs["mode"], CameraMode(3840, 1080, 60.0, "mjpg"))
        self.assertEqual(smoke.call_args.kwargs["covered_eye"], "right")
        self.assertEqual(smoke.call_args.kwargs["frames"], 3)
        self.assertEqual(smoke.call_args.kwargs["imu_packets"], 2)
        self.assertEqual(smoke.call_args.kwargs["software_version"], f"{__version__}+{__commit__}")
        self.assertEqual(smoke.call_args.kwargs["evidence_kind"], "hardware")
        self.assertIs(smoke.call_args.kwargs["hardware_facts"], facts)

    def test_hardware_smoke_invalid_argument_is_machine_readable(self) -> None:
        output = io.StringIO()
        error = io.StringIO()
        with (
            tempfile.TemporaryDirectory() as directory,
            redirect_stdout(output),
            redirect_stderr(error),
        ):
            result = main(
                [
                    "hardware-smoke",
                    "--output",
                    str(Path(directory) / "smoke"),
                    "--covered-eye",
                    "left",
                    "--frames",
                    "0",
                ]
            )

        rendered = json.loads(error.getvalue())
        self.assertEqual(result, 2)
        self.assertEqual(output.getvalue(), "")
        self.assertFalse(rendered["ok"])
        self.assertEqual(rendered["error"]["code"], "invalid_argument")
        self.assertNotIn("Traceback", error.getvalue())

    def test_hardware_smoke_error_is_json_without_traceback(self) -> None:
        error = io.StringIO()
        with (
            tempfile.TemporaryDirectory() as directory,
            patch("rp_ylx.cli.collect_hardware_facts", return_value={}),
            patch(
                "rp_ylx.cli.record_hardware_smoke",
                side_effect=HardwareSmokeError("camera_start_failed", "camera unavailable"),
            ),
            redirect_stderr(error),
        ):
            result = main(
                [
                    "hardware-smoke",
                    "--output",
                    str(Path(directory) / "smoke"),
                    "--covered-eye",
                    "left",
                ]
            )
        rendered = json.loads(error.getvalue())
        self.assertEqual(result, 2)
        self.assertEqual(rendered["error"]["code"], "camera_start_failed")
        self.assertNotIn("Traceback", error.getvalue())


if __name__ == "__main__":
    unittest.main()

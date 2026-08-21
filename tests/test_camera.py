from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

import rp_ylx.camera.v4l2 as v4l2
from rp_ylx.camera import (
    CameraController,
    CameraDescriptor,
    CameraError,
    CameraMode,
    StereoFrame,
    SyntheticCameraBackend,
    V4L2CameraStream,
    V4L2DiscoveryBackend,
    parse_v4l2_formats,
    split_sbs_mjpeg,
    split_sbs_mjpeg_native,
    v4l2_production_stream_factory,
    v4l2_stream_factory,
)
from rp_ylx.camera.v4l2 import (
    V4L2_BUF_FLAG_TIMESTAMP_MONOTONIC,
    V4L2_BUF_TYPE_VIDEO_CAPTURE,
    V4L2_CAP_STREAMING,
    V4L2_CAP_VIDEO_CAPTURE,
    VIDIOC_DQBUF,
    VIDIOC_QBUF,
    VIDIOC_QUERYBUF,
    VIDIOC_QUERYCAP,
    VIDIOC_REQBUFS,
    VIDIOC_S_FMT,
    VIDIOC_S_PARM,
    VIDIOC_STREAMOFF,
    VIDIOC_STREAMON,
)

MODE = CameraMode(1920, 1080, 30.0, "mjpg")
FOCUS_STATUS = {
    "schema": "ylx.camera-focus.v1",
    "value": 42,
    "minimum": 0,
    "maximum": 255,
    "step": 1,
    "default": 32,
    "auto_supported": True,
    "auto_enabled": False,
}


def descriptor(stable_id: str) -> CameraDescriptor:
    return CameraDescriptor(stable_id, f"/dev/{stable_id}", "YLX Stereo", (MODE,))


def frame(sequence: int, timestamp: int | None = None) -> StereoFrame:
    return StereoFrame(
        source_sequence=sequence,
        host_monotonic_ns=timestamp if timestamp is not None else 1_000_000 + sequence,
        left=f"left-{sequence}".encode(),
        right=f"right-{sequence}".encode(),
    )


class CameraControllerTest(unittest.TestCase):
    def test_no_device_and_multiple_device_selection(self) -> None:
        with self.assertRaises(CameraError) as missing:
            CameraController(SyntheticCameraBackend(())).open(MODE)
        self.assertEqual(missing.exception.code, "no_device")

        devices = (descriptor("camera-b"), descriptor("camera-a"))
        controller = CameraController(SyntheticCameraBackend(devices))
        self.assertEqual(
            [item.stable_id for item in controller.discover()], ["camera-a", "camera-b"]
        )
        with self.assertRaises(CameraError) as selection:
            controller.open(MODE)
        self.assertEqual(selection.exception.code, "selection_required")
        self.assertEqual(controller.open(MODE, stable_id="camera-b").stable_id, "camera-b")

    def test_unsupported_mode_permission_and_busy_are_explicit(self) -> None:
        device = descriptor("camera-a")
        controller = CameraController(SyntheticCameraBackend((device,)))
        with self.assertRaises(CameraError) as unsupported:
            controller.open(CameraMode(640, 480, 30, "mjpg"))
        self.assertEqual(unsupported.exception.code, "unsupported_mode")

        permission_backend = SyntheticCameraBackend(
            (device,), open_errors={device.stable_id: PermissionError("denied")}
        )
        with self.assertRaises(CameraError) as permission:
            CameraController(permission_backend).open(MODE)
        self.assertEqual(permission.exception.code, "permission_denied")

        busy_backend = SyntheticCameraBackend(
            (device,),
            open_errors={
                device.stable_id: CameraError("device_busy", "相机已被占用", retryable=True)
            },
        )
        with self.assertRaises(CameraError) as busy:
            CameraController(busy_backend).open(MODE)
        self.assertEqual(busy.exception.code, "device_busy")

    def test_lifecycle_and_sequence_gap_are_observable(self) -> None:
        device = descriptor("camera-a")
        backend = SyntheticCameraBackend(
            (device,), frames={device.stable_id: [frame(10), frame(13)]}
        )
        controller = CameraController(backend)
        controller.open(MODE)
        controller.start()
        self.assertEqual(controller.read().dropped_before, 0)
        self.assertEqual(controller.read().dropped_before, 2)
        controller.stop()
        self.assertEqual(controller.state, "open")
        controller.close()
        self.assertTrue(backend.opened_streams[0].stopped)
        self.assertTrue(backend.opened_streams[0].closed)

    def test_controller_uses_native_frame_validator_when_available(self) -> None:
        device = descriptor("camera-native-validator")
        backend = SyntheticCameraBackend(
            (device,),
            frames={device.stable_id: [frame(5, 100)]},
        )
        validator = Mock()
        validator.validate_frame.return_value = {
            "dropped_before": 7,
            "queue_rejected": 0,
            "source_gap": 7,
        }
        with patch(
            "rp_ylx.camera.controller.create_native_camera_frame_validator",
            return_value=validator,
        ):
            controller = CameraController(backend)
        controller.open(MODE)
        controller.start()
        self.assertEqual(controller.read().dropped_before, 7)
        controller.close()
        validator.validate_frame.assert_called_once_with(5, 100, True, True, True, False, 0)
        self.assertGreaterEqual(validator.reset.call_count, 2)

    def test_bad_frame_regression_and_hot_unplug_release_resources(self) -> None:
        cases = (
            (StereoFrame(0, 1, b"", b"right", True), "bad_frame"),
            ([frame(2), frame(2, 2_000_000)], "sequence_regression"),
            ([frame(1), OSError("unplugged")], "disconnected"),
        )
        for configured, expected in cases:
            with self.subTest(expected=expected):
                items = configured if isinstance(configured, list) else [configured]
                device = descriptor(f"camera-{expected}")
                backend = SyntheticCameraBackend((device,), frames={device.stable_id: items})
                controller = CameraController(backend)
                controller.open(MODE)
                controller.start()
                if expected != "bad_frame":
                    controller.read()
                with self.assertRaises(CameraError) as raised:
                    controller.read()
                self.assertEqual(raised.exception.code, expected)
                self.assertEqual(controller.state, "closed")
                self.assertTrue(backend.opened_streams[0].closed)

    def test_raw_sbs_frame_does_not_require_materialized_eyes(self) -> None:
        device = descriptor("camera-raw-sbs")
        raw = StereoFrame(0, 1_000_000, b"", b"", True, b"raw-sbs")
        backend = SyntheticCameraBackend((device,), frames={device.stable_id: [raw]})
        controller = CameraController(backend)
        controller.open(MODE)
        controller.start()

        self.assertEqual(controller.read().frame.raw_side_by_side, b"raw-sbs")
        controller.close()

    def test_focus_control_uses_open_camera_device_node(self) -> None:
        device = descriptor("camera-focus")
        controller = CameraController(SyntheticCameraBackend((device,)))
        controller.open(MODE)

        with patch(
            "rp_ylx.camera.controller.native_camera_focus_status",
            return_value=FOCUS_STATUS,
        ) as read_focus:
            self.assertEqual(controller.camera_focus_status(), FOCUS_STATUS)
        read_focus.assert_called_once_with("/dev/camera-focus")

        updated = {**FOCUS_STATUS, "value": 77}
        with patch(
            "rp_ylx.camera.controller.set_native_camera_focus",
            return_value=updated,
        ) as set_focus:
            self.assertEqual(
                controller.set_camera_focus(value=77, auto_enabled=False),
                updated,
            )
        set_focus.assert_called_once_with(
            "/dev/camera-focus",
            value=77,
            auto_enabled=False,
        )
        controller.close()

    def test_context_manager_releases_open_stream(self) -> None:
        device = descriptor("camera-a")
        backend = SyntheticCameraBackend((device,), frames={device.stable_id: [frame(0)]})
        with CameraController(backend) as controller:
            controller.open(MODE)
            controller.start()
            controller.read()
        self.assertTrue(backend.opened_streams[0].closed)


class V4L2DiscoveryTest(unittest.TestCase):
    FORMATS = """
        [0]: 'MJPG' (Motion-JPEG, compressed)
            Size: Discrete 3840x1080
                Interval: Discrete 0.017s (60.000 fps)
                Interval: Discrete 0.033s (30.000 fps)
            Size: Discrete 1920x540
                Interval: Discrete 0.017s (60.000 fps)
    """

    def test_parses_discrete_modes(self) -> None:
        modes = parse_v4l2_formats(self.FORMATS)
        self.assertEqual(
            modes,
            (
                CameraMode(1920, 540, 60.0, "mjpg"),
                CameraMode(3840, 1080, 30.0, "mjpg"),
                CameraMode(3840, 1080, 60.0, "mjpg"),
            ),
        )

    def test_discovers_stable_usb_identity_without_opening_device(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            usb = root / "sys/devices/usb1/1-1"
            video_device = usb / "video4linux/video0"
            video_device.mkdir(parents=True)
            (usb / "idVendor").write_text("1bcf\n")
            (usb / "idProduct").write_text("0b15\n")
            (usb / "serial").write_text("YLX-001\n")
            (video_device / "name").write_text("YLX 2UQ2\n")
            video_class = root / "sys/class/video4linux"
            video_class.mkdir(parents=True)
            (video_class / "video0").symlink_to(video_device)
            dev = root / "dev"
            dev.mkdir()

            backend = V4L2DiscoveryBackend(
                sys_root=root / "sys", dev_root=dev, v4l2_ctl="", stream_factory=None
            )
            first = backend.discover()
            second = backend.discover()

            self.assertEqual(first, second)
            self.assertEqual(first[0].name, "YLX 2UQ2")
            self.assertTrue(first[0].stable_id.startswith("v4l2:"))
            self.assertEqual(first[0].modes, ())
            with self.assertRaises(CameraError) as unavailable:
                backend.open(first[0], MODE)
            self.assertEqual(unavailable.exception.code, "backend_unavailable")

    def test_default_factory_is_real_v4l2_stream(self) -> None:
        backend = V4L2DiscoveryBackend(v4l2_ctl="", sys_root=Path("/missing"))
        self.assertIs(backend._stream_factory, v4l2_stream_factory)


class _FakeMmap:
    def __init__(self, length: int) -> None:
        self.data = bytearray(length)
        self.closed = False

    def __len__(self) -> int:
        return len(self.data)

    def __getitem__(self, item: object) -> object:
        return self.data[item]  # type: ignore[index]

    def __setitem__(self, item: object, value: object) -> None:
        self.data[item] = value  # type: ignore[index]

    def close(self) -> None:
        self.closed = True


class _FakeV4L2Device:
    def __init__(
        self,
        payloads: list[bytes],
        *,
        bad_flags: bool = False,
        timestamps: tuple[int, ...] | None = None,
        sequences: tuple[int, ...] | None = None,
    ) -> None:
        self.payloads = iter(payloads)
        self.bad_flags = bad_flags
        self.timestamps = iter(timestamps) if timestamps is not None else None
        self.sequences = iter(sequences) if sequences is not None else None
        self.buffers = [_FakeMmap(4096), _FakeMmap(4096)]
        self.ioctls: list[int] = []
        self.queued: list[int] = []
        self.closed_fds: list[int] = []
        self.sequence = 10
        self.timestamp = 1

    def ioctl(self, fd: int, request: int, payload: bytearray) -> None:
        import struct

        self.ioctls.append(request)
        if request == VIDIOC_QUERYCAP:
            struct.pack_into("<II", payload, 84, V4L2_CAP_VIDEO_CAPTURE | V4L2_CAP_STREAMING, 0)
        elif request == VIDIOC_S_FMT:
            self.assert_format(payload)
        elif request == VIDIOC_S_PARM:
            struct.pack_into("<II", payload, 12, 1, 30)
        elif request == VIDIOC_REQBUFS:
            struct.pack_into("<I", payload, 0, 2)
        elif request == VIDIOC_QUERYBUF:
            index = struct.unpack_from("<I", payload, 0)[0]
            struct.pack_into("<I", payload, 64, index * 4096)
            struct.pack_into("<I", payload, 72, 4096)
        elif request == VIDIOC_QBUF:
            self.queued.append(struct.unpack_from("<I", payload, 0)[0])
        elif request == VIDIOC_DQBUF:
            index = self.queued.pop(0) if self.queued else 0
            frame = next(self.payloads)
            self.buffers[index].data[: len(frame)] = frame
            struct.pack_into("<IIII", payload, 0, index, V4L2_BUF_TYPE_VIDEO_CAPTURE, len(frame), 0)
            flags = V4L2_BUF_FLAG_TIMESTAMP_MONOTONIC if not self.bad_flags else 0x40
            struct.pack_into("<I", payload, 12, flags)
            timestamp = next(self.timestamps) if self.timestamps is not None else self.timestamp
            struct.pack_into("<qq", payload, 24, timestamp, 0)
            sequence = next(self.sequences) if self.sequences is not None else self.sequence
            struct.pack_into("<I", payload, 56, sequence)
            self.sequence += 1
            self.timestamp += 1
        elif request in {VIDIOC_STREAMON, VIDIOC_STREAMOFF}:
            return
        else:
            raise AssertionError(f"unexpected ioctl {request:#x}")

    @staticmethod
    def assert_format(payload: bytearray) -> None:
        import struct

        width, height = struct.unpack_from("<II", payload, 8)
        if (width, height) != (3840, 1080):
            raise AssertionError((width, height))

    def mmap(self, fd: int, length: int, prot: int, flags: int, offset: int) -> _FakeMmap:
        return self.buffers[offset // 4096]


class V4L2StreamTest(unittest.TestCase):
    LEFT = b"\xff\xd8left-eye\xff\xd9"
    RIGHT = b"\xff\xd8right-eye\xff\xd9"
    SBS = LEFT + RIGHT
    MODE = CameraMode(3840, 1080, 30.0, "mjpg")

    def test_production_factory_uses_complete_native_camera(self) -> None:
        owner = Mock()
        owner.stats.return_value = {
            "capacity": 4,
            "depth": 0,
            "peak_depth": 1,
            "enqueued": 1,
            "delivered": 1,
            "rejected": 0,
        }
        owner.read.return_value = (10, 1_000_000, 0, self.LEFT, self.RIGHT, self.SBS)
        with patch.object(v4l2, "create_native_camera", return_value=owner) as create:
            stream = v4l2_production_stream_factory(Path("/dev/video0"), self.MODE)

        create.assert_called_once_with(
            "/dev/video0",
            3840,
            1080,
            30,
            "mjpg",
            buffer_count=16,
            queue_capacity=64,
            split_eyes=False,
        )
        stream.start()
        self.assertEqual(
            stream.read(1.0),
            StereoFrame(10, 1_000_000, self.LEFT, self.RIGHT, True, self.SBS),
        )
        stream.stop()
        stream.close()
        stream.close()
        owner.start.assert_called_once_with()
        owner.read.assert_called_once_with(1.0)
        owner.stats.assert_not_called()
        owner.stop.assert_called_once_with()
        owner.close.assert_called_once_with()

    def test_production_factory_does_not_fallback_when_native_is_unavailable(self) -> None:
        from rp_ylx.native import NativeModuleError

        with (
            patch.object(
                v4l2,
                "create_native_camera",
                side_effect=NativeModuleError("native_camera_unavailable", "missing"),
            ),
            patch.object(v4l2, "V4L2CameraStream") as stream,
            self.assertRaises(CameraError) as raised,
        ):
            v4l2_production_stream_factory(Path("/dev/video0"), self.MODE)
        self.assertEqual(raised.exception.code, "native_camera_unavailable")
        stream.assert_not_called()

    def test_native_adapter_translates_errors_and_closes_owner(self) -> None:
        cases = [
            (RuntimeError("frame_timeout: no frame"), TimeoutError, None),
            (RuntimeError("bad_frame: damaged"), CameraError, "bad_frame"),
            (RuntimeError("opaque"), CameraError, "native_camera_failed"),
        ]
        for error, exception_type, code in cases:
            with self.subTest(error=str(error)):
                owner = Mock()
                owner.read.side_effect = error
                stream = v4l2.NativeV4L2CameraStream(owner)
                with self.assertRaises(exception_type) as raised:
                    stream.read(1.0)
                if code is not None:
                    self.assertEqual(raised.exception.code, code)
                self.assertTrue(stream.closed)
                owner.close.assert_called_once_with()

    def test_native_adapter_accepts_raw_sbs_without_split_eyes(self) -> None:
        owner = Mock()
        owner.stats.return_value = {
            "capacity": 64,
            "depth": 0,
            "peak_depth": 1,
            "enqueued": 1,
            "delivered": 1,
            "rejected": 0,
        }
        owner.read.return_value = (10, 1_000_000, 0, b"", b"", self.SBS)
        stream = v4l2.NativeV4L2CameraStream(owner)
        self.assertEqual(
            stream.read(1.0),
            StereoFrame(10, 1_000_000, b"", b"", True, self.SBS),
        )

    def test_native_adapter_rejects_invalid_frame_and_fractional_fps(self) -> None:
        owner = Mock()
        owner.read.return_value = (True, 1, 0, self.LEFT, self.RIGHT, self.SBS)
        stream = v4l2.NativeV4L2CameraStream(owner)
        with self.assertRaises(CameraError) as invalid:
            stream.read(1.0)
        self.assertEqual(invalid.exception.code, "invalid_native_frame")
        owner.close.assert_called_once_with()

        owner = Mock()
        owner.read.return_value = (10, 1_000_000, 0, b"", b"", b"")
        stream = v4l2.NativeV4L2CameraStream(owner)
        with self.assertRaises(CameraError) as invalid:
            stream.read(1.0)
        self.assertEqual(invalid.exception.code, "invalid_native_frame")
        owner.close.assert_called_once_with()

        with self.assertRaises(CameraError) as unsupported:
            v4l2_production_stream_factory(
                Path("/dev/video0"), CameraMode(3840, 1080, 29.97, "mjpg")
            )
        self.assertEqual(unsupported.exception.code, "unsupported_mode")

    def test_native_adapter_observes_bounded_queue_only_when_enabled(self) -> None:
        from rp_ylx.performance.metrics import PerformanceMetrics

        metrics = PerformanceMetrics()
        owner = Mock()
        owner.read.side_effect = [
            (10, 1_000_000, 0, self.LEFT, self.RIGHT, self.SBS),
            (12, 2_000_000, 1, self.LEFT, self.RIGHT, self.SBS),
        ]
        owner.stats.side_effect = [
            {
                "capacity": 4,
                "depth": 3,
                "peak_depth": 4,
                "enqueued": 4,
                "delivered": 1,
                "rejected": 0,
            },
            {
                "capacity": 4,
                "depth": 2,
                "peak_depth": 4,
                "enqueued": 5,
                "delivered": 2,
                "rejected": 1,
            },
        ]
        stream = v4l2.NativeV4L2CameraStream(owner, metrics=metrics)
        stream.read(1.0)
        second = stream.read(1.0)
        self.assertEqual(second._application_dropped_before, 1)
        snapshot = metrics.snapshot()
        self.assertEqual(snapshot.queue, {"capacity": 4, "peak_depth": 4, "rejected": 1})

    def test_split_preserves_two_jpeg_payloads_without_reencoding(self) -> None:
        self.assertEqual(
            split_sbs_mjpeg(b"padding" + self.SBS + b"\x00", 3840, 1080),
            (self.LEFT, self.RIGHT),
        )

    def test_split_crops_one_side_by_side_jpeg_into_two_eye_jpegs(self) -> None:
        import io

        from PIL import Image

        source = Image.new("RGB", (40, 20), "red")
        source.paste(Image.new("RGB", (20, 20), "blue"), (20, 0))
        encoded = io.BytesIO()
        source.save(encoded, format="JPEG", quality=100, subsampling=0)

        with patch.object(v4l2, "lossless_crop_sbs_jpeg", return_value=None) as lossless_crop:
            left_payload, right_payload = split_sbs_mjpeg(encoded.getvalue(), 40, 20)

        lossless_crop.assert_called_once_with(encoded.getvalue(), 40, 20)
        with (
            Image.open(io.BytesIO(left_payload)) as left,
            Image.open(io.BytesIO(right_payload)) as right,
        ):
            self.assertEqual((left.size, right.size), ((20, 20), (20, 20)))
            left_pixel = left.getpixel((5, 10))
            right_pixel = right.getpixel((15, 10))
            self.assertGreater(left_pixel[0], left_pixel[2])
            self.assertGreater(right_pixel[2], right_pixel[0])

    def test_split_prefers_lossless_crop_for_single_sbs_jpeg(self) -> None:
        import io

        from PIL import Image

        source = Image.new("RGB", (40, 20), "red")
        encoded = io.BytesIO()
        source.save(encoded, format="JPEG", quality=100, subsampling=0)
        left = io.BytesIO()
        right = io.BytesIO()
        Image.new("RGB", (20, 20), "red").save(left, format="JPEG")
        Image.new("RGB", (20, 20), "blue").save(right, format="JPEG")
        lossless_result = (left.getvalue(), right.getvalue())

        with patch.object(
            v4l2,
            "lossless_crop_sbs_jpeg",
            return_value=lossless_result,
        ) as lossless_crop:
            self.assertEqual(
                split_sbs_mjpeg(encoded.getvalue(), 40, 20),
                lossless_result,
            )

        lossless_crop.assert_called_once_with(encoded.getvalue(), 40, 20)

    def test_split_does_not_transform_two_complete_jpegs(self) -> None:
        with patch.object(
            v4l2,
            "lossless_crop_sbs_jpeg",
            return_value=(b"fast-left", b"fast-right"),
        ) as lossless_crop:
            self.assertEqual(
                split_sbs_mjpeg(b"padding" + self.SBS + b"\x00", 3840, 1080),
                (self.LEFT, self.RIGHT),
            )

        lossless_crop.assert_not_called()

    def test_production_split_rejects_pillow_fallback(self) -> None:
        with (
            patch.object(v4l2, "lossless_crop_sbs_jpeg", return_value=None),
            self.assertRaises(CameraError) as rejected,
        ):
            split_sbs_mjpeg_native(b"\xff\xd8single-sbs\xff\xd9", 3840, 1080)
        self.assertEqual(rejected.exception.code, "native_split_unavailable")

    def test_production_split_accepts_driver_double_jpeg_without_transform(self) -> None:
        with patch.object(v4l2, "lossless_crop_sbs_jpeg") as lossless_crop:
            self.assertEqual(
                split_sbs_mjpeg_native(b"padding" + self.SBS + b"\x00", 3840, 1080),
                (self.LEFT, self.RIGHT),
            )
        lossless_crop.assert_not_called()

    def test_split_rejects_odd_width_without_calling_lossless_crop(self) -> None:
        with (
            patch.object(v4l2, "lossless_crop_sbs_jpeg") as lossless_crop,
            self.assertRaises(CameraError) as raised,
        ):
            split_sbs_mjpeg(b"\xff\xd8source\xff\xd9", 39, 20)

        self.assertEqual(raised.exception.code, "bad_frame")
        lossless_crop.assert_not_called()

    def test_mmap_lifecycle_sequence_timestamp_and_stereo_payload(self) -> None:
        fake = _FakeV4L2Device([self.SBS, self.SBS])
        stream = V4L2CameraStream(
            "/dev/video-test",
            self.MODE,
            ioctl=fake.ioctl,
            mmap_factory=fake.mmap,
            open_file=lambda path, flags: 7,
            close_file=fake.closed_fds.append,
            wait_readable=lambda fd, timeout: True,
        )
        stream.start()
        first = stream.read(1.0)
        second = stream.read(1.0)
        self.assertEqual((first.source_sequence, second.source_sequence), (10, 11))
        self.assertEqual(
            (first.host_monotonic_ns, second.host_monotonic_ns),
            (1_000_000_000, 2_000_000_000),
        )
        self.assertEqual((first.left, first.right), (self.LEFT, self.RIGHT))
        self.assertEqual(first.raw_side_by_side, self.SBS)
        stream.stop()
        stream.close()
        self.assertTrue(stream.closed)
        self.assertEqual(fake.closed_fds, [7])
        self.assertTrue(all(item.closed for item in fake.buffers))
        self.assertIn(VIDIOC_STREAMON, fake.ioctls)
        self.assertIn(VIDIOC_STREAMOFF, fake.ioctls)

    def test_mmap_raw_sbs_mode_skips_eye_materialization(self) -> None:
        fake = _FakeV4L2Device([self.SBS])
        stream = V4L2CameraStream(
            "/dev/video-test",
            self.MODE,
            ioctl=fake.ioctl,
            mmap_factory=fake.mmap,
            open_file=lambda path, flags: 7,
            close_file=fake.closed_fds.append,
            wait_readable=lambda fd, timeout: True,
            split_frame=None,
        )
        stream.start()

        captured = stream.read(1.0)

        self.assertEqual((captured.left, captured.right), (b"", b""))
        self.assertEqual(captured.raw_side_by_side, self.SBS)
        stream.close()

    def test_source_sequence_rollover_unwraps_without_a_gap(self) -> None:
        fake = _FakeV4L2Device(
            [self.SBS, self.SBS],
            sequences=(0xFFFFFFFF, 0),
        )
        stream = V4L2CameraStream(
            "/dev/video-test",
            self.MODE,
            ioctl=fake.ioctl,
            mmap_factory=fake.mmap,
            open_file=lambda path, flags: 7,
            close_file=fake.closed_fds.append,
            wait_readable=lambda fd, timeout: True,
        )
        stream.start()
        first = stream.read(1.0)
        second = stream.read(1.0)
        stream.close()

        self.assertEqual(first.source_sequence, 0xFFFFFFFF)
        self.assertEqual(second.source_sequence, 1 << 32)
        self.assertEqual(second.source_sequence - first.source_sequence, 1)

    def test_bad_buffer_closes_and_requeues_deterministically(self) -> None:
        fake = _FakeV4L2Device([self.SBS], bad_flags=True)
        stream = V4L2CameraStream(
            "/dev/video-test",
            self.MODE,
            ioctl=fake.ioctl,
            mmap_factory=fake.mmap,
            open_file=lambda path, flags: 7,
            close_file=fake.closed_fds.append,
            wait_readable=lambda fd, timeout: True,
        )
        stream.start()
        with self.assertRaises(CameraError) as raised:
            stream.read(1.0)
        self.assertEqual(raised.exception.code, "bad_frame")
        self.assertTrue(stream.closed)
        self.assertEqual(fake.closed_fds, [7])

    def test_driver_timestamp_regression_is_explicit_and_closes_stream(self) -> None:
        fake = _FakeV4L2Device([self.SBS, self.SBS], timestamps=(2, 1))
        stream = V4L2CameraStream(
            "/dev/video-test",
            self.MODE,
            ioctl=fake.ioctl,
            mmap_factory=fake.mmap,
            open_file=lambda path, flags: 7,
            close_file=fake.closed_fds.append,
            wait_readable=lambda fd, timeout: True,
        )
        stream.start()
        stream.read(1.0)

        with self.assertRaises(CameraError) as raised:
            stream.read(1.0)

        self.assertEqual(raised.exception.code, "timestamp_regression")
        self.assertTrue(stream.closed)
        self.assertEqual(fake.closed_fds, [7])
        self.assertTrue(all(item.closed for item in fake.buffers))

    def test_timeout_closes_the_stream(self) -> None:
        fake = _FakeV4L2Device([])
        stream = V4L2CameraStream(
            "/dev/video-test",
            self.MODE,
            ioctl=fake.ioctl,
            mmap_factory=fake.mmap,
            open_file=lambda path, flags: 7,
            close_file=fake.closed_fds.append,
            wait_readable=lambda fd, timeout: False,
        )
        stream.start()
        with self.assertRaises(TimeoutError):
            stream.read(1.0)
        self.assertTrue(stream.closed)
        self.assertEqual(fake.closed_fds, [7])


if __name__ == "__main__":
    unittest.main()

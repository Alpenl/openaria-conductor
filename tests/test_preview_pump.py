from __future__ import annotations

import threading
import time
import unittest
from collections.abc import Callable

from rp_ylx.api import CameraPreviewPump, MockDevice
from rp_ylx.camera import CameraError, CameraMode, FrameObservation, StereoFrame

MODE = CameraMode(3840, 1080, 60.0, "mjpg")


def frame(sequence: int, timestamp: int) -> StereoFrame:
    return StereoFrame(sequence, timestamp, b"left-jpeg", b"right-jpeg")


class FakeStream:
    def __init__(self, items: list[StereoFrame | Exception]) -> None:
        self._items = iter(items)
        self.release = threading.Event()
        self.started = False
        self.stopped = False
        self.closed = False

    def start(self) -> None:
        self.started = True

    def read(self, timeout: float) -> StereoFrame:
        try:
            item = next(self._items)
        except StopIteration as exc:
            self.release.wait(timeout)
            raise CameraError("disconnected", "fake stream stopped", retryable=True) from exc
        if isinstance(item, Exception):
            raise item
        return item

    def stop(self) -> None:
        self.stopped = True
        self.started = False

    def close(self) -> None:
        self.closed = True
        self.started = False
        self.release.set()


class FakeController:
    def __init__(self, items: list[StereoFrame | Exception]) -> None:
        self.stream = FakeStream(items)
        self.state = "closed"
        self.open_calls: list[tuple[CameraMode, str | None]] = []
        self.close_calls = 0

    def open(self, mode: CameraMode, *, stable_id: str | None = None) -> None:
        self.open_calls.append((mode, stable_id))
        self.state = "open"

    def start(self) -> None:
        self.stream.start()
        self.state = "streaming"

    def read(self, *, timeout: float) -> FrameObservation:
        return FrameObservation(self.stream.read(timeout), 0)

    def stop(self) -> None:
        self.stream.stop()
        self.state = "open"

    def close(self) -> None:
        self.close_calls += 1
        self.stream.close()
        self.state = "closed"


class FailingOpenController(FakeController):
    def open(self, mode: CameraMode, *, stable_id: str | None = None) -> None:
        self.open_calls.append((mode, stable_id))
        raise CameraError("open_failed", "fake camera unavailable", retryable=True)


def wait_until(predicate: Callable[[], bool], timeout: float = 1.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.005)
    raise AssertionError("condition was not reached before timeout")


class CameraPreviewPumpTest(unittest.TestCase):
    def test_publishes_source_metadata_and_releases_stream_on_stop(self) -> None:
        controller = FakeController([frame(0, 100), frame(3, 200)])
        device = MockDevice()
        pump = CameraPreviewPump(
            device,
            controller,
            MODE,
            stable_id="camera-a",
            read_timeout=0.01,
        )

        pump.start()
        wait_until(lambda: pump.frames_published == 2)
        payload, sequence, monotonic_ns = device.preview("left")
        self.assertEqual((payload, sequence, monotonic_ns), (b"left-jpeg", 3, 200))
        right, right_sequence, right_time = device.preview("right", sequence=sequence)
        self.assertEqual((right, right_sequence, right_time), (b"right-jpeg", 3, 200))
        self.assertIsNotNone(pump.thread)
        self.assertFalse(pump.thread.daemon)  # type: ignore[union-attr]

        pump.stop(timeout=0.05)
        self.assertFalse(pump.is_alive)
        self.assertEqual(pump.state, "stopped")
        self.assertIsNone(pump.error)
        self.assertTrue(controller.stream.stopped)
        self.assertTrue(controller.stream.closed)
        self.assertEqual(controller.open_calls, [(MODE, "camera-a")])

    def test_camera_error_is_observable_and_closes_stream(self) -> None:
        errors: list[BaseException] = []
        controller = FakeController(
            [frame(1, 100), CameraError("disconnected", "fake unplugged", retryable=True)]
        )
        device = MockDevice()
        pump = CameraPreviewPump(
            device,
            controller,
            MODE,
            read_timeout=0.01,
            on_error=errors.append,
        )

        pump.start()
        wait_until(lambda: pump.error is not None)
        pump.join(timeout=1)

        self.assertEqual(pump.state, "failed")
        self.assertIsNotNone(pump.error)
        self.assertEqual(getattr(pump.error, "code", None), "disconnected")
        self.assertEqual(len(errors), 1)
        self.assertEqual(device.status()["health"], "error")
        self.assertEqual(device.status()["issues"][0]["code"], "hardware_unavailable")
        self.assertFalse(pump.is_alive)
        self.assertTrue(controller.stream.closed)

        pump.stop()
        self.assertFalse(pump.is_alive)

    def test_start_failure_closes_controller_without_thread(self) -> None:
        controller = FailingOpenController([])
        pump = CameraPreviewPump(MockDevice(), controller, MODE)

        with self.assertRaises(CameraError) as raised:
            pump.start()

        self.assertEqual(raised.exception.code, "open_failed")
        self.assertEqual(pump.state, "failed")
        self.assertIsNotNone(pump.error)
        self.assertIsNone(pump.thread)
        self.assertTrue(controller.stream.closed)


if __name__ == "__main__":
    unittest.main()

from __future__ import annotations

import gc
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import patch

from rp_ylx.camera import (
    CameraController,
    CameraDescriptor,
    FrameObservation,
    StereoFrame,
    SyntheticCameraBackend,
)
from rp_ylx.native import NativeModuleError
from rp_ylx.performance.metrics import PerformanceMetrics
from rp_ylx.recording import RecordingConfig, SessionRecorder
from tests.test_camera import MODE


class PerformanceMetricsTest(unittest.TestCase):
    def test_zero_byte_copies_are_not_reported(self) -> None:
        with patch(
            "rp_ylx.performance.metrics.create_native_performance_metrics",
            side_effect=NativeModuleError("native_unavailable", "test fallback"),
        ):
            metrics = PerformanceMetrics()

        metrics.record_copy("empty_left", 0)
        metrics.record_copy("empty_right", 0, count=3)
        metrics.record_copy("no_count", 512, count=0)

        self.assertEqual(metrics.snapshot().copies, ())

    def test_queue_observation_accepts_real_peak_and_rejects_impossible_values(self) -> None:
        metrics = PerformanceMetrics()
        metrics.observe_queue(depth=2, peak_depth=4, capacity=4, rejected=1)
        self.assertEqual(
            metrics.snapshot().queue,
            {"capacity": 4, "peak_depth": 4, "rejected": 1},
        )
        with self.assertRaises(ValueError):
            metrics.observe_queue(depth=3, peak_depth=2, capacity=4)

    def test_controller_separates_native_queue_rejection_from_source_gap(self) -> None:
        metrics = PerformanceMetrics()
        backend = SyntheticCameraBackend(
            [
                CameraDescriptor("camera-a", "/dev/video0", "YLX", (MODE,)),
            ],
            frames={
                "camera-a": [
                    StereoFrame(10, 100, b"left", b"right"),
                    StereoFrame(
                        14,
                        200,
                        b"left",
                        b"right",
                        _application_dropped_before=2,
                    ),
                ]
            },
        )
        with CameraController(backend, metrics=metrics) as controller:
            controller.open(MODE)
            controller.start()
            controller.read()
            observation = controller.read()
        self.assertEqual(observation.dropped_before, 3)
        self.assertEqual(metrics.snapshot().loss["queue_rejected"], 2)
        self.assertEqual(metrics.snapshot().loss["source_gap"], 1)

    def test_stage_histograms_are_bounded_and_thread_safe(self) -> None:
        metrics = PerformanceMetrics()

        def record() -> None:
            for value in range(1, 101):
                metrics.record_stage("jpeg_split", value)

        threads = [threading.Thread(target=record) for _ in range(4)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        [stage] = metrics.snapshot().stages
        self.assertEqual(stage["name"], "jpeg_split")
        self.assertEqual(stage["samples"], 400)
        self.assertEqual(stage["total_ns"], 20_200)
        self.assertLessEqual(stage["p50_ns"], stage["p95_ns"])

    def test_payload_leases_track_current_and_peak_without_content(self) -> None:
        metrics = PerformanceMetrics()
        first = metrics.retain_payload("camera_frame", 100)
        second = metrics.retain_payload("camera_frame", 60)
        second.release()

        [payload] = metrics.snapshot().payloads
        self.assertEqual(
            payload,
            {
                "name": "camera_frame",
                "acquired": 2,
                "live": 1,
                "live_bytes": 100,
                "peak_live": 2,
                "peak_bytes": 160,
            },
        )
        del first
        gc.collect()
        self.assertEqual(metrics.snapshot().payloads[0]["live"], 0)

    def test_controller_classifies_only_source_sequence_gaps(self) -> None:
        frames = (
            StereoFrame(10, 100, b"left", b"right"),
            StereoFrame(13, 200, b"left", b"right"),
        )
        metrics = PerformanceMetrics()
        descriptor = CameraDescriptor("camera-a", "/dev/camera-a", "YLX Stereo", (MODE,))
        controller = CameraController(
            SyntheticCameraBackend((descriptor,), frames={descriptor.stable_id: frames}),
            metrics=metrics,
        )
        controller.open(MODE)
        controller.start()
        controller.read()
        observation = controller.read()
        controller.close()

        self.assertEqual(observation.dropped_before, 2)
        self.assertEqual(metrics.snapshot().loss["source_gap"], 2)
        self.assertEqual(metrics.snapshot().loss["queue_rejected"], 0)

    def test_recorder_accounts_queue_rejection_and_payload_lifetime(self) -> None:
        blocked = threading.Event()
        release = threading.Event()

        def slow_write(role: str, payload: bytes) -> None:
            if role == "video.left" and payload != b"YLXFRM0\n":
                blocked.set()
                release.wait(timeout=2)

        config = RecordingConfig("device", "0.1.0", 20, 10, 60, "jpeg")
        metrics = PerformanceMetrics()
        with tempfile.TemporaryDirectory() as directory:
            recorder = SessionRecorder(
                Path(directory),
                config,
                queue_capacity=1,
                enqueue_timeout=0,
                before_write=slow_write,
                metrics=metrics,
            )
            recorder.start(session_id="0198c9a8-7a3c-7000-8000-000000000001")
            observations = [
                FrameObservation(StereoFrame(sequence, sequence, b"left", b"right"), 0)
                for sequence in range(1, 4)
            ]
            self.assertTrue(recorder.submit_frame(observations[0]))
            self.assertTrue(blocked.wait(timeout=1))
            self.assertTrue(recorder.submit_frame(observations[1]))
            with recorder._queue.mutex:
                queued = tuple(recorder._queue.queue)
            self.assertIs(queued[0].observation, observations[1])
            self.assertIs(queued[0].observation.frame.left, observations[1].frame.left)
            self.assertFalse(recorder.submit_frame(observations[2]))
            snapshot = metrics.snapshot()
            self.assertEqual(snapshot.queue, {"capacity": 1, "peak_depth": 1, "rejected": 1})
            self.assertEqual(snapshot.loss["queue_rejected"], 1)
            # The rejected producer owns a short-lived reference while put() decides.
            self.assertEqual(snapshot.payloads[0]["peak_live"], 3)
            release.set()
            recorder.stop()
        self.assertEqual(metrics.snapshot().payloads[0]["live"], 0)


if __name__ == "__main__":
    unittest.main()

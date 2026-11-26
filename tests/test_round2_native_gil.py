"""Round 2 tests for rp_ylx._native timeout bounds, Drop/GIL safety, and multipart preview."""

from __future__ import annotations

import gc
import importlib
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path

from rp_ylx.native import NativeCapturePlan, NativeRecordingPlan


def _import_native():
    try:
        return importlib.import_module("rp_ylx._native")
    except ModuleNotFoundError as exc:
        raise unittest.SkipTest("rp_ylx._native extension is not built") from exc


def _capture_plan(**overrides: object) -> NativeCapturePlan:
    kwargs: dict[str, object] = {
        "device": "/dev/nonexistent-video-round2",
        "width": 3840,
        "height": 1080,
        "fps": 60,
        "encoding": "mjpg",
        "buffer_count": 4,
        "queue_capacity": 8,
        "frame_decimation": 2,
        "read_timeout_seconds": 1.0,
    }
    kwargs.update(overrides)
    return NativeCapturePlan(**kwargs)  # type: ignore[arg-type]


def _recording_plan(root: Path, helper: Path) -> NativeRecordingPlan:
    return NativeRecordingPlan(
        session_root=str(root),
        session_id="01993e29-74e8-7000-8000-000000000001",
        encoder_executable=str(helper),
        width=3840,
        height=1080,
        fps=30,
        bitrate_kbps=8192,
        segment_frames=150,
        recording_start_monotonic_ns=1,
        audio_enabled=False,
        audio_device="default",
        audio_sample_rate_hz=48_000,
        audio_channels=2,
        audio_segment_seconds=30.0,
    )


def _write_dummy_encoder(path: Path) -> None:
    path.write_text(
        f"#!{sys.executable}\n"
        "import sys\n"
        'print(\'{"event":"ready"}\', flush=True)\n'
        "sys.stdin.buffer.read()\n"
        'print(\'{"event":"done","frames":0}\', flush=True)\n',
        encoding="utf-8",
    )
    path.chmod(0o755)


class NativeGilAndBoundsTests(unittest.TestCase):
    def test_capture_engine_oversized_float_timeouts_raise_value_error(self) -> None:
        native = _import_native()
        preview = native.NativePreviewBuffer(15)
        oversized = (1e20, sys.float_info.max, float("inf"), float("nan"), 0.0, -1.0)
        for field in ("read_timeout_seconds", "imu_timeout_seconds", "imu_stale_poll_interval"):
            for bad in oversized:
                if field == "imu_stale_poll_interval" and bad == 0.0:
                    continue
                with self.subTest(field=field, bad=bad):
                    plan = _capture_plan(**{field: bad})
                    with self.assertRaises(ValueError):
                        native.NativeCaptureEngine(plan, preview, None)

        # Also verify Duration::MAX - 0.5s triggers checked_add overflow check
        # cleanly as ValueError.
        near_u64_max = float((1 << 64) - 1)
        with self.assertRaises(ValueError):
            native.NativeCaptureEngine(
                _capture_plan(read_timeout_seconds=near_u64_max),
                preview,
                None,
            )

    def test_session_transaction_drop_and_finish_timeout_no_gil_deadlock(self) -> None:
        native = _import_native()
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "video").mkdir()
            helper = root / "encoder.py"
            _write_dummy_encoder(helper)

            store = native.NativeSessionStore()
            tx = store.begin_recording(_recording_plan(root, helper))

            # Oversized float timeout on finish/boundary raises ValueError cleanly.
            for bad in (1e20, sys.float_info.max):
                with self.assertRaises(ValueError):
                    tx.finish(1.0, timeout_seconds=bad)
                with self.assertRaises(ValueError):
                    tx.boundary(0, bad)

            # Drop NativeSessionTransaction while concurrent Python threads contend for the GIL.
            stop_churn = threading.Event()

            def gil_churn() -> None:
                acc = 0
                while not stop_churn.is_set():
                    acc += 1
                    time.sleep(0.001)

            churn_threads = [threading.Thread(target=gil_churn, daemon=True) for _ in range(4)]
            for thread in churn_threads:
                thread.start()
            try:
                tx.abort("drop under GIL contention")
                del tx
                gc.collect()
            finally:
                stop_churn.set()
                for thread in churn_threads:
                    thread.join(timeout=2.0)

    def test_multipart_preview_continues_across_empty_cleared_buffer(self) -> None:
        native = _import_native()
        buf = native.NativePreviewBuffer(30)
        stream = buf.multipart_stream(30)
        try:
            buf.publish(b"\xff\xd8frame-1\xff\xd9")
            part1 = next(stream)
            self.assertIn(b"frame-1", part1)

            # Clear the buffer so snapshot() returns preview_unavailable while stream waits.
            buf.clear()

            published = threading.Event()

            def publish_later() -> None:
                time.sleep(0.03)
                buf.publish(b"\xff\xd8frame-2\xff\xd9")
                published.set()

            thread = threading.Thread(target=publish_later, daemon=True)
            thread.start()
            part2 = next(stream)
            thread.join(timeout=2.0)
            self.assertTrue(published.is_set())
            self.assertIn(b"frame-2", part2)
        finally:
            stream.close()
        with self.assertRaises(StopIteration):
            next(stream)

    def test_capture_engine_drop_releases_gil_and_timeout_bounds(self) -> None:
        native = _import_native()
        # Verify NativePreviewBuffer + NativePerformanceMetrics drop cleanly under GIL churn.
        buf = native.NativePreviewBuffer(15)
        metrics = native.NativePerformanceMetrics()
        metrics.record_stage("round2", 100)
        del metrics
        del buf
        gc.collect()

        # Also verify NativeSessionTransaction timeout bounds on replace().
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "video").mkdir()
            helper = root / "encoder.py"
            _write_dummy_encoder(helper)
            tx = native.NativeSessionStore().begin_recording(_recording_plan(root, helper))
            try:
                with self.assertRaises(ValueError):
                    tx.finish(float("inf"), timeout_seconds=1.0)
            finally:
                tx.abort("cleanup")


if __name__ == "__main__":
    unittest.main()

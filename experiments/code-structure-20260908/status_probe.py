"""Measure coordinator contention and event-pump sampling without hardware."""

from __future__ import annotations

import argparse
import hashlib
import json
import statistics
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest.mock import Mock, patch


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("repository", type=Path)
    parser.add_argument("--delay", type=float, default=0.3)
    parser.add_argument("--trials", type=int, default=5)
    args = parser.parse_args()
    if args.delay <= 0 or args.trials < 1:
        parser.error("delay and trials must be positive")
    repository = args.repository.resolve()
    sys.path[:0] = [str(repository / "src"), str(repository / "tests")]

    from test_capture_coordinator import (
        CaptureCoordinatorTest,
        FakeSources,
        frame,
        start_command,
        stop_command,
    )

    from rp_ylx.daemon import CaptureEventPump

    def stop_latency() -> float:
        case = CaptureCoordinatorTest()
        case.setUp()
        sources = FakeSources()
        coordinator = case.coordinator(sources=sources)
        blocked = threading.Event()
        poll_thread = None

        def status() -> object:
            nonlocal poll_thread
            poll_thread = threading.get_ident()
            return coordinator.capture_status()

        def focus() -> None:
            if threading.get_ident() == poll_thread:
                blocked.set()
                time.sleep(args.delay)

        try:
            coordinator.start_capture(start_command("latency-start"))
            coordinator.submit_frame(frame())
            with (
                patch.object(sources, "camera_focus_status", side_effect=focus),
                ThreadPoolExecutor(max_workers=2) as executor,
            ):
                polling = executor.submit(status)
                if not blocked.wait(timeout=5):
                    raise TimeoutError("status query did not reach the slow focus read")
                started = time.perf_counter()
                stopped = executor.submit(coordinator.stop_capture, stop_command("latency-stop"))
                result = stopped.result(timeout=args.delay + 10)
                elapsed = time.perf_counter() - started
                assert result.body["snapshot"]["device_state"] == "idle"
                polling.result(timeout=args.delay + 5)
                return elapsed
        finally:
            coordinator.close()
            case.tearDown()

    latencies = [stop_latency() for _ in range(args.trials)]
    case = CaptureCoordinatorTest()
    case.setUp()
    sources = FakeSources()
    coordinator = case.coordinator(sources=sources)
    try:
        coordinator.start_capture(start_command("sampling-start"))
        with patch("rp_ylx.daemon.threading.Thread"):
            pump = CaptureEventPump(coordinator, Mock())
        pump._stop = Mock()
        pump._stop.wait.side_effect = [False] * 25 + [True]
        with (
            patch.object(coordinator, "_runtime", wraps=coordinator._runtime) as runtime,
            patch.object(
                sources, "camera_focus_status", wraps=sources.camera_focus_status
            ) as focus,
        ):
            pump._run()
        counts = {"ticks": 25, "runtime_reads": runtime.call_count, "focus_reads": focus.call_count}
    finally:
        coordinator.close()
        case.tearDown()

    print(
        json.dumps(
            {
                "coordinator_sha256": hashlib.sha256(
                    (repository / "src/rp_ylx/recording/coordinator.py").read_bytes()
                ).hexdigest(),
                "focus_delay_seconds": args.delay,
                "stop_latency_seconds": latencies,
                "stop_latency_median_seconds": statistics.median(latencies),
                "active_event_pump": counts,
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()

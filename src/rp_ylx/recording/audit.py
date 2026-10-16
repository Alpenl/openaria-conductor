"""Streaming recording clock audit; correspondence is not ADC calibration."""

from __future__ import annotations

import json
from collections import deque
from pathlib import Path
from threading import Event

from .cadence import FrameCadence


def _journal_lines(stream, finished: Event | None):
    """Tail complete records; a producer may be halfway through one write."""
    pending = b""
    while True:
        line = stream.readline()
        if line:
            pending += line
            if len(pending) > 1_048_576:
                raise ValueError("capture audit record exceeds limit")
            if pending.endswith(b"\n"):
                yield pending
                pending = b""
            continue
        if finished is None or finished.is_set():
            if pending:
                raise ValueError("capture audit found a truncated record")
            return
        finished.wait(0.02)


def capture_audit(
    root: Path, nominal_fps: float, frame_decimation: int = 1, *, finished: Event | None = None
) -> dict:
    cadence = FrameCadence(nominal_fps)
    first = previous = last = None
    frames = matched = 0
    maximum_interval = 0
    maximum_dequeue_delay = 0
    clocks: set[str] = set()
    recent: deque[tuple[int, int]] = deque(maxlen=512)
    imu_first = imu_last = None
    imu_count = packets = 0
    previous_packet = None
    maximum_read = 0
    imu_previous_host = None
    imu_regressions = 0
    counter_wraps = 0
    counter_resets = 0
    previous_counter = None
    previous_sequence = None
    source_missing = 0
    intervals = [0] * 10_001  # bounded 100 us bins, last bin is overflow
    counter_available = 0

    def observe_imu(row: dict) -> None:
        nonlocal imu_first, imu_last, imu_count, packets, previous_packet, maximum_read
        nonlocal imu_previous_host, imu_regressions
        host = row["host_monotonic_ns"]
        if imu_first is None:
            imu_first = host
        if imu_previous_host is not None and host < imu_previous_host:
            imu_regressions += 1
        imu_previous_host = imu_last = host
        imu_count += 1
        packet = row.get("packet_sequence")
        if packet != previous_packet:
            packets += 1
            previous_packet = packet
            recent.append((row["device_timestamp_raw"], host))
        maximum_read = max(maximum_read, row["host_read_end_ns"] - row["host_read_start_ns"])

    with (root / "imu.ndjson").open("rb") as imu, (root / "frames.ndjson").open("rb") as camera:
        imu_rows = iter(_journal_lines(imu, finished))
        pending = None
        for line in _journal_lines(camera, finished):
            frame = json.loads(line)
            host = frame["host_monotonic_ns"]
            cadence.observe(host)
            if first is None:
                first = host
            if previous is not None:
                if host <= previous:
                    raise ValueError("camera audit found non-increasing timestamps")
                maximum_interval = max(maximum_interval, host - previous)
                intervals[min(10_000, (host - previous) // 100_000)] += 1
            if previous_sequence is not None:
                source_missing += max(
                    0, frame["source_sequence"] - previous_sequence - frame_decimation
                )
            previous_sequence = frame["source_sequence"]
            previous = last = host
            frames += 1
            while True:
                if pending is None:
                    data = next(imu_rows, None)
                    if data is None:
                        break
                    pending = json.loads(data)
                if pending["host_monotonic_ns"] > host + 50_000_000:
                    break
                observe_imu(pending)
                pending = None
            audit = frame.get("timestamp_audit", {})
            counter = audit.get("camera_counter_raw")
            if counter is not None:
                counter_available += 1
                matched += int(any(c == counter and abs(t - host) < 100_000_000 for c, t in recent))
                if previous_counter is not None and counter < previous_counter:
                    if previous_counter - counter > 2**23:
                        counter_wraps += 1
                    else:
                        counter_resets += 1
                previous_counter = counter
            clocks.add(audit.get("timestamp_clock", "legacy_unspecified"))
            dequeue = audit.get("host_dequeue_monotonic_ns")
            if dequeue is not None:
                maximum_dequeue_delay = max(maximum_dequeue_delay, dequeue - host)
        if pending is not None:
            observe_imu(pending)
        for line in imu_rows:
            observe_imu(json.loads(line))
    frame_span = 0 if first is None or last is None else (last - first) / 1e9
    imu_span = 0 if imu_first is None or imu_last is None else (imu_last - imu_first) / 1e9
    try:
        boot_id = Path("/proc/sys/kernel/random/boot_id").read_text().strip()
    except OSError:
        boot_id = None

    def percentile(fraction: float) -> int | None:
        target = fraction * (frames - 1)
        total = 0
        if frames < 2:
            return None
        for index, count in enumerate(intervals):
            total += count
            if total >= target:
                return (
                    min(maximum_interval, (index + 1) * 100_000)
                    if index < 10_000
                    else maximum_interval
                )
        return None

    return {
        "schema": "openaria.capture-audit.v1",
        "boot_id": boot_id,
        "camera": {
            "frame_count": frames,
            "first_monotonic_ns": first,
            "last_monotonic_ns": last,
            "actual_fps": (frames - 1) / frame_span if frame_span else None,
            "nominal_fps": nominal_fps,
            "cadence": cadence.summary(),
            "max_interval_ns": maximum_interval,
            "max_dequeue_delay_ns": maximum_dequeue_delay,
            "clock_sources": sorted(clocks),
            "counter_matched_frames": matched,
            "counter_wraps": counter_wraps,
            "counter_resets": counter_resets,
            "counter_available_frames": counter_available,
            "counter_match_status": "matched" if matched == frames and frames else "incomplete",
            "source_missing_frames": source_missing,
            "interval_p50_ns": percentile(0.50),
            "interval_p95_ns": percentile(0.95),
            "interval_p99_ns": percentile(0.99),
            "interval_histogram_resolution_ns": 100_000,
            "nominal_timeline_error_seconds": frame_span - (frames - 1) / nominal_fps
            if frames
            else 0,
        },
        "imu": {
            "sample_slots": imu_count,
            "packets": packets,
            "delivered_slots_hz": (imu_count - 2) / imu_span
            if imu_span and imu_count >= 2
            else None,
            "max_read_duration_ns": maximum_read,
            "host_timestamp_regressions": imu_regressions,
            "sample_time_source": "host_read_midpoint_shared_by_two_slots",
            "independent_adc_timestamps": False,
        },
        "alignment": {
            "method": "shared_24bit_counter_and_host_monotonic",
            "applied_offset_ns": 0,
            "physical_time_calibration": "unavailable",
            "intrinsics_extrinsics": "not_provided",
            "vio_drift": "not_measured",
            "uvc_pts_scr_association": "unverified_not_used",
        },
        "timeline": {
            "authority": "frames.host_monotonic_ns",
            "segment_mp4_clock": "nominal_frame_rate",
            "export_requirement": "index_driven_variable_frame_timestamps",
        },
    }

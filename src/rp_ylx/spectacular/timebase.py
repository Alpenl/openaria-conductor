"""Loss-intolerant frame and IMU timing reconstruction for calibration captures."""

from __future__ import annotations

import math
from dataclasses import dataclass
from pathlib import Path
from statistics import median
from typing import Any

from .adapter import CaptureValidationError, LoadedCapture, load_capture


@dataclass(frozen=True, slots=True)
class ClockDiagnostics:
    expected_rate_hz: float
    measured_rate_hz: float
    rate_error_ppm: float
    residual_p50_ms: float
    residual_p95_ms: float
    residual_max_ms: float

    def as_dict(self) -> dict[str, float]:
        return {
            "expected_rate_hz": self.expected_rate_hz,
            "measured_rate_hz": self.measured_rate_hz,
            "rate_error_ppm": self.rate_error_ppm,
            "residual_p50_ms": self.residual_p50_ms,
            "residual_p95_ms": self.residual_p95_ms,
            "residual_max_ms": self.residual_max_ms,
        }


@dataclass(frozen=True, slots=True)
class CaptureTiming:
    capture: LoadedCapture
    frame_times_ns: tuple[float, ...]
    imu_times_ns: tuple[float, ...]
    frame_clock: ClockDiagnostics
    imu_clock: ClockDiagnostics
    imu_read_p95_ms: float

    @property
    def manifest(self) -> dict[str, Any]:
        return self.capture.manifest

    @property
    def frames(self) -> tuple[dict[str, Any], ...]:
        return self.capture.frames

    @property
    def imu_samples(self) -> tuple[dict[str, Any], ...]:
        return self.capture.imu_samples

    def diagnostics(self) -> dict[str, Any]:
        return {
            "source_schema": self.capture.source_schema,
            "session_id": self.capture.session_id,
            "frames": len(self.frames),
            "imu_samples": len(self.imu_samples),
            "frame_clock": self.frame_clock.as_dict(),
            "imu_clock": self.imu_clock.as_dict(),
            "imu_read_p95_ms": self.imu_read_p95_ms,
        }


def _percentile(values: list[float], fraction: float) -> float:
    if not values:
        raise CaptureValidationError("cannot compute a timing percentile from no values")
    ordered = sorted(values)
    index = min(len(ordered) - 1, math.ceil(fraction * len(ordered)) - 1)
    return ordered[max(index, 0)]


def _unwrap_contiguous(values: list[int], bits: int, label: str) -> list[int]:
    if not values:
        raise CaptureValidationError(f"no {label} sequences")
    modulus = 1 << bits
    mask = modulus - 1
    unwrapped = [values[0]]
    for index, current in enumerate(values[1:], 1):
        previous = values[index - 1]
        delta = (current - previous) & mask
        if delta != 1:
            raise CaptureValidationError(
                f"{label} sequence gap or regression at record {index}: {previous} -> {current}"
            )
        unwrapped.append(unwrapped[-1] + 1)
    return unwrapped


def _unwrap_forward(values: list[int], bits: int, label: str) -> list[int]:
    if not values:
        raise CaptureValidationError(f"no {label} timestamps")
    modulus = 1 << bits
    mask = modulus - 1
    unwrapped = [values[0]]
    for index, current in enumerate(values[1:], 1):
        previous = values[index - 1]
        delta = (current - previous) & mask
        if delta == 0 or delta >= modulus // 2:
            raise CaptureValidationError(
                f"{label} timestamp is not strictly forward at record {index}: "
                f"{previous} -> {current}"
            )
        unwrapped.append(unwrapped[-1] + delta)
    return unwrapped


def _fit_clock(
    counters: list[int],
    host_ns: list[int],
    expected_rate_hz: float,
    label: str,
    max_rate_error_ppm: float,
    max_residual_p95_ms: float,
) -> tuple[list[float], ClockDiagnostics]:
    if len(counters) != len(host_ns) or len(counters) < 3:
        raise CaptureValidationError(f"{label} clock requires at least three records")
    if any(later <= earlier for earlier, later in zip(host_ns, host_ns[1:], strict=False)):
        raise CaptureValidationError(f"{label} host timestamps are not strictly increasing")

    origin = counters[0]
    relative = [float(value - origin) for value in counters]
    mean_x = sum(relative) / len(relative)
    mean_y = sum(host_ns) / len(host_ns)
    denominator = sum((value - mean_x) ** 2 for value in relative)
    if denominator == 0:
        raise CaptureValidationError(f"{label} clock has no counter variation")
    period_ns = (
        sum(
            (counter - mean_x) * (host - mean_y)
            for counter, host in zip(relative, host_ns, strict=True)
        )
        / denominator
    )
    if not math.isfinite(period_ns) or period_ns <= 0:
        raise CaptureValidationError(f"{label} clock period is non-positive")

    measured_rate_hz = 1e9 / period_ns
    rate_error_ppm = (measured_rate_hz / expected_rate_hz - 1) * 1e6
    if abs(rate_error_ppm) > max_rate_error_ppm:
        raise CaptureValidationError(
            f"{label} measured rate {measured_rate_hz:.6f} Hz differs from "
            f"expected {expected_rate_hz:.6f} Hz by {rate_error_ppm:.1f} ppm"
        )

    intercept_ns = median(
        host - counter * period_ns for host, counter in zip(host_ns, relative, strict=True)
    )
    modeled = [intercept_ns + counter * period_ns for counter in relative]
    residual_ms = [abs(host - model) / 1e6 for host, model in zip(host_ns, modeled, strict=True)]
    residual_p95_ms = _percentile(residual_ms, 0.95)
    if residual_p95_ms > max_residual_p95_ms:
        raise CaptureValidationError(
            f"{label} host timing p95 residual {residual_p95_ms:.3f} ms exceeds "
            f"{max_residual_p95_ms:.3f} ms"
        )
    return modeled, ClockDiagnostics(
        expected_rate_hz=expected_rate_hz,
        measured_rate_hz=measured_rate_hz,
        rate_error_ppm=rate_error_ppm,
        residual_p50_ms=_percentile(residual_ms, 0.50),
        residual_p95_ms=residual_p95_ms,
        residual_max_ms=max(residual_ms),
    )


def _fit_timestamp_clock(
    timestamps: list[int],
    host_ns: list[int],
    expected_event_rate_hz: float,
    label: str,
    max_rate_error_ppm: float,
    max_residual_p95_ms: float,
) -> tuple[list[float], ClockDiagnostics]:
    if len(timestamps) != len(host_ns) or len(timestamps) < 3:
        raise CaptureValidationError(f"{label} clock requires at least three records")
    if any(later <= earlier for earlier, later in zip(host_ns, host_ns[1:], strict=False)):
        raise CaptureValidationError(f"{label} host timestamps are not strictly increasing")

    origin = timestamps[0]
    relative = [float(value - origin) for value in timestamps]
    mean_x = sum(relative) / len(relative)
    mean_y = sum(host_ns) / len(host_ns)
    denominator = sum((value - mean_x) ** 2 for value in relative)
    if denominator == 0:
        raise CaptureValidationError(f"{label} device timestamp has no variation")
    nanoseconds_per_tick = (
        sum(
            (timestamp - mean_x) * (host - mean_y)
            for timestamp, host in zip(relative, host_ns, strict=True)
        )
        / denominator
    )
    if not math.isfinite(nanoseconds_per_tick) or nanoseconds_per_tick <= 0:
        raise CaptureValidationError(f"{label} device timestamp scale is non-positive")

    intercept_ns = median(
        host - timestamp * nanoseconds_per_tick
        for host, timestamp in zip(host_ns, relative, strict=True)
    )
    modeled = [intercept_ns + timestamp * nanoseconds_per_tick for timestamp in relative]
    duration_ns = modeled[-1] - modeled[0]
    if duration_ns <= 0:
        raise CaptureValidationError(f"{label} modeled duration is non-positive")
    measured_rate_hz = (len(modeled) - 1) * 1e9 / duration_ns
    rate_error_ppm = (measured_rate_hz / expected_event_rate_hz - 1) * 1e6
    if abs(rate_error_ppm) > max_rate_error_ppm:
        raise CaptureValidationError(
            f"{label} measured event rate {measured_rate_hz:.6f} Hz differs from "
            f"expected {expected_event_rate_hz:.6f} Hz by {rate_error_ppm:.1f} ppm"
        )

    residual_ms = [abs(host - model) / 1e6 for host, model in zip(host_ns, modeled, strict=True)]
    residual_p95_ms = _percentile(residual_ms, 0.95)
    if residual_p95_ms > max_residual_p95_ms:
        raise CaptureValidationError(
            f"{label} host timing p95 residual {residual_p95_ms:.3f} ms exceeds "
            f"{max_residual_p95_ms:.3f} ms"
        )
    return modeled, ClockDiagnostics(
        expected_rate_hz=expected_event_rate_hz,
        measured_rate_hz=measured_rate_hz,
        rate_error_ppm=rate_error_ppm,
        residual_p50_ms=_percentile(residual_ms, 0.50),
        residual_p95_ms=residual_p95_ms,
        residual_max_ms=max(residual_ms),
    )


def _imu_packets(capture: LoadedCapture) -> list[list[dict[str, Any]]]:
    samples_per_packet = capture.imu_samples_per_packet
    packets: list[list[dict[str, Any]]] = []
    current: list[dict[str, Any]] = []
    for sample_number, record in enumerate(capture.imu_samples):
        if record["sample_number"] != sample_number:
            raise CaptureValidationError(
                f"IMU sample_number is not contiguous at record {sample_number}"
            )
        sample_index = int(record["sample_index"])
        if sample_index == 0:
            if current:
                raise CaptureValidationError("new IMU packet started before the prior packet ended")
            current = [record]
        elif not current or sample_index != len(current):
            raise CaptureValidationError(
                f"IMU sample_index order is invalid at record {sample_number}"
            )
        else:
            current.append(record)

        if len(current) == samples_per_packet:
            first = current[0]
            shared = (
                "device_timestamp_raw",
                "host_read_start_ns",
                "host_read_end_ns",
                "host_monotonic_ns",
            )
            if any(
                int(sample["samples_in_packet"]) != samples_per_packet
                or any(sample[key] != first[key] for key in shared)
                for sample in current
            ):
                raise CaptureValidationError("records within an IMU packet disagree")
            if capture.source_schema == "ylx.device-session.v2" and any(
                sample["packet_sequence"] != first["packet_sequence"]
                or sample["device_ticks"] != first["device_ticks"]
                for sample in current
            ):
                raise CaptureValidationError("Device Session records within an IMU packet disagree")
            packets.append(current)
            current = []
    if current:
        raise CaptureValidationError("capture ends with an incomplete IMU packet")
    if len(packets) < 3:
        raise CaptureValidationError("IMU timing requires at least three complete packets")
    return packets


def _packet_timestamps(capture: LoadedCapture, packets: list[list[dict[str, Any]]]) -> list[int]:
    first_records = [packet[0] for packet in packets]
    if capture.source_schema == "ylx.device-session.v2":
        _unwrap_contiguous(
            [int(record["packet_sequence"]) for record in first_records],
            32,
            "IMU packet",
        )
        timestamps = [int(record["device_ticks"]) for record in first_records]
        if any(
            later <= earlier for earlier, later in zip(timestamps, timestamps[1:], strict=False)
        ):
            raise CaptureValidationError("Device Session IMU device_ticks are not strictly forward")
        if any(
            (int(record["device_ticks"]) & ((1 << 24) - 1)) != int(record["device_timestamp_raw"])
            for record in first_records
        ):
            raise CaptureValidationError("Device Session IMU raw and unwrapped timestamps disagree")
        return timestamps

    assert capture.imu_timestamp_bits is not None
    return _unwrap_forward(
        [int(record["device_timestamp_raw"]) for record in first_records],
        capture.imu_timestamp_bits,
        "IMU packet",
    )


def _analyze_capture(
    capture_dir: str | Path,
    *,
    imu_rate_hz: float,
    max_rate_error_ppm: float,
    max_residual_p95_ms: float,
    max_imu_read_p95_ms: float,
) -> CaptureTiming:
    if imu_rate_hz <= 0:
        raise CaptureValidationError("IMU rate must be positive")
    capture = load_capture(capture_dir)
    frame_sequences = _unwrap_contiguous(
        [int(frame["uvc_sequence"]) for frame in capture.frames], 32, "video frame"
    )
    frame_hosts = [int(frame["callback_monotonic_ns"]) for frame in capture.frames]
    frame_times, frame_clock = _fit_clock(
        frame_sequences,
        frame_hosts,
        capture.fps,
        "video frame",
        max_rate_error_ppm,
        max_residual_p95_ms,
    )

    packets = _imu_packets(capture)
    packet_records = [packet[0] for packet in packets]
    packet_times, imu_clock = _fit_timestamp_clock(
        _packet_timestamps(capture, packets),
        [int(record["host_monotonic_ns"]) for record in packet_records],
        imu_rate_hz / capture.imu_samples_per_packet,
        "IMU packet",
        max_rate_error_ppm,
        max_residual_p95_ms,
    )
    read_duration_ms = [
        (int(record["host_read_end_ns"]) - int(record["host_read_start_ns"])) / 1e6
        for record in packet_records
    ]
    read_p95_ms = _percentile(read_duration_ms, 0.95)
    if read_p95_ms > max_imu_read_p95_ms:
        raise CaptureValidationError(
            f"IMU control-read p95 duration {read_p95_ms:.3f} ms exceeds "
            f"{max_imu_read_p95_ms:.3f} ms"
        )

    sample_period_ns = 1e9 / imu_rate_hz
    imu_times: list[float] = []
    previous_time = -math.inf
    for packet, packet_time in zip(packets, packet_times, strict=True):
        for record in packet:
            sample_time = (
                packet_time
                - (capture.imu_samples_per_packet - 1 - int(record["sample_index"]))
                * sample_period_ns
            )
            if sample_time <= previous_time:
                raise CaptureValidationError("reconstructed IMU timestamps are not increasing")
            imu_times.append(sample_time)
            previous_time = sample_time

    overlap_start = max(frame_times[0], imu_times[0])
    overlap_end = min(frame_times[-1], imu_times[-1])
    if overlap_end <= overlap_start:
        raise CaptureValidationError("video and IMU timing windows do not overlap")

    return CaptureTiming(
        capture=capture,
        frame_times_ns=tuple(frame_times),
        imu_times_ns=tuple(imu_times),
        frame_clock=frame_clock,
        imu_clock=imu_clock,
        imu_read_p95_ms=read_p95_ms,
    )


def analyze_capture(
    capture_dir: str | Path,
    *,
    imu_rate_hz: float = 120.0,
    max_rate_error_ppm: float = 20_000.0,
    max_residual_p95_ms: float = 5.0,
    max_imu_read_p95_ms: float = 10.0,
) -> CaptureTiming:
    """Validate capture integrity and reconstruct one monotonic calibration timeline."""

    try:
        return _analyze_capture(
            capture_dir,
            imu_rate_hz=imu_rate_hz,
            max_rate_error_ppm=max_rate_error_ppm,
            max_residual_p95_ms=max_residual_p95_ms,
            max_imu_read_p95_ms=max_imu_read_p95_ms,
        )
    except CaptureValidationError:
        raise
    except (
        ArithmeticError,
        AttributeError,
        IndexError,
        KeyError,
        OSError,
        TypeError,
        ValueError,
    ) as error:
        raise CaptureValidationError(f"malformed capture metadata: {error}") from error

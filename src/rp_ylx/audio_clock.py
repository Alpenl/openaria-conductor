"""Audio clock evidence shared by the device and Bridge readers.

PCM segment times remain sample-relative. sync is session-relative; legacy
thread timestamps provide no continuity proof. Keep this module byte-identical
in Conductor and Bridge SDK when changing the clock contract.
"""

from __future__ import annotations

import math
from collections.abc import Mapping


def audio_clock_report(audio: Mapping, session_duration: float) -> dict:
    if audio.get("state") == "not_recorded":
        return {"continuity": "no-audio"}
    rate = audio["sample_rate"]
    count = audio["sample_count"]
    if type(rate) is not int or rate <= 0 or type(count) is not int or count <= 0:
        raise ValueError("invalid audio sample domain")
    segments = audio.get("segments")
    channels = audio.get("channels")
    if not isinstance(segments, list) or not segments or type(channels) is not int or channels <= 0:
        raise ValueError("invalid audio segments or channels")
    previous = 0
    for index, segment in enumerate(segments):
        if not isinstance(segment, Mapping):
            raise ValueError("invalid audio segment")
        first_sample, last_sample = segment.get("start_sample"), segment.get("end_sample")
        if (
            type(first_sample) is not int
            or type(last_sample) is not int
            or type(segment.get("index")) is not int
            or segment["index"] != index
            or first_sample != previous
            or last_sample <= first_sample
        ):
            raise ValueError("noncontiguous audio sample domain")
        for key, expected in (
            ("start_time_seconds", first_sample / rate),
            ("end_time_seconds", last_sample / rate),
        ):
            value = segment.get(key)
            if (
                type(value) not in (int, float)
                or not math.isfinite(value)
                or abs(value - expected) > 1e-9
            ):
                raise ValueError("audio segment time does not match sample domain")
        payload, header = segment.get("pcm_payload_bytes"), segment.get("wav_header_bytes")
        artifact = segment.get("artifact")
        if (
            type(payload) is not int
            or type(header) is not int
            or not 44 <= header <= 65536
            or payload != (last_sample - first_sample) * channels * 2
            or not isinstance(artifact, Mapping)
            or artifact.get("bytes") != payload + header
        ):
            raise ValueError("audio segment byte count does not match samples")
        previous = last_sample
    if previous != count:
        raise ValueError("audio sample_count differs from segments")
    sync = audio["sync"]
    start, end = sync["start_time_seconds"], sync["end_time_seconds"]
    if any(
        type(v) not in (int, float) or not math.isfinite(v) for v in (start, end, session_duration)
    ):
        raise ValueError("nonfinite audio timeline")
    if not 0 <= start < end <= session_duration + 1e-6:
        raise ValueError("audio sync outside session")
    pcm_duration = count / rate
    report = {
        "continuity": "unknown",
        "pcm_duration_seconds": pcm_duration,
        "sync_span_seconds": end - start,
        "duration_difference_seconds": end - start - pcm_duration,
    }
    clock = audio.get("capture_clock")
    if clock is None:
        return report
    if not isinstance(clock, Mapping):
        raise ValueError("invalid audio capture_clock")
    expected = {
        "schema",
        "clock",
        "timestamp_source",
        "continuity",
        "device",
        "period_frames",
        "buffer_frames",
        "thread_started_monotonic_ns",
        "thread_stopped_monotonic_ns",
        "sample_start_monotonic_ns",
        "sample_end_monotonic_ns",
        "max_residual_ns",
        "anchors",
        "queue_capacity_frames",
        "queue_peak_frames",
        "max_write_ns",
        "xrun_count",
        "suspend_count",
        "session_start_monotonic_ns",
    }
    if set(clock) != expected:
        raise ValueError("audio capture_clock fields mismatch")
    if (
        clock["schema"] != "openaria.audio-clock.v1"
        or clock["clock"] != "host_monotonic"
        or clock["timestamp_source"] != "alsa_htimestamp_dma"
        or clock["continuity"] != "verified"
        or not isinstance(clock["device"], str)
        or not clock["device"]
    ):
        raise ValueError("unsupported audio capture clock")
    integers = expected - {"schema", "clock", "timestamp_source", "continuity", "device", "anchors"}
    if any(type(clock[k]) is not int or clock[k] < 0 for k in integers):
        raise ValueError("invalid audio clock integer")
    if (
        clock["xrun_count"]
        or clock["suspend_count"]
        or not 0 < clock["period_frames"] <= clock["buffer_frames"]
        or not 0 < clock["queue_peak_frames"] <= clock["queue_capacity_frames"]
        or clock["max_residual_ns"] > 20_000_000
    ):
        raise ValueError("audio capture continuity failed")
    anchors = clock["anchors"]
    if not isinstance(anchors, list) or len(anchors) < 2:
        raise ValueError("audio clock requires multiple anchors")
    for anchor in anchors:
        if (
            not isinstance(anchor, list)
            or len(anchor) != 2
            or any(type(v) is not int or v <= 0 for v in anchor)
        ):
            raise ValueError("invalid audio sample clock anchor")
    if any(b[0] <= a[0] or b[1] <= a[1] for a, b in zip(anchors, anchors[1:], strict=False)):
        raise ValueError("nonmonotonic audio clock")
    first, last = anchors[0], anchors[-1]
    ns_per_sample = (last[1] - first[1]) / (last[0] - first[0])
    actual_rate = 1e9 / ns_per_sample
    residual = max(abs((t - first[1]) - (n - first[0]) * ns_per_sample) for n, t in anchors)
    predicted_start = first[1] - first[0] * ns_per_sample
    predicted_end = predicted_start + count * ns_per_sample
    origin = clock["session_start_monotonic_ns"]
    if (
        abs(actual_rate / rate - 1) > 0.01
        or abs(residual - clock["max_residual_ns"]) > 2
        or abs(predicted_start - clock["sample_start_monotonic_ns"]) > 2
        or abs(predicted_end - clock["sample_end_monotonic_ns"]) > 2
        or abs(start * 1e9 - (clock["sample_start_monotonic_ns"] - origin)) > 2
        or abs(end * 1e9 - (clock["sample_end_monotonic_ns"] - origin)) > 2
    ):
        raise ValueError("audio sync does not match sample clock evidence")
    thread_start = clock["thread_started_monotonic_ns"]
    thread_stop = clock["thread_stopped_monotonic_ns"]
    if (
        not origin <= thread_start < thread_stop
        or predicted_start < thread_start - 20_000_000
        or predicted_end > thread_stop + 20_000_000
        or thread_stop - origin > session_duration * 1e9 + 1_000
    ):
        raise ValueError("audio sample clock outside capture lifetime")
    if (
        first[0] > clock["buffer_frames"] + clock["period_frames"]
        or not count <= last[0] <= count + clock["buffer_frames"]
    ):
        raise ValueError("audio anchors do not cover captured samples")
    if any(
        b[0] - a[0] > rate + clock["buffer_frames"]
        for a, b in zip(anchors, anchors[1:], strict=False)
    ):
        raise ValueError("audio clock evidence has missing intervals")
    report.update(
        continuity="verified",
        actual_sample_rate=actual_rate,
        max_residual_seconds=residual / 1e9,
        thread_span_seconds=(thread_stop - thread_start) / 1e9,
        timestamp_source=clock["timestamp_source"],
    )
    return report

"""Audio recorder finalization, bounded inventory readback, and timeline construction."""

from __future__ import annotations

import json
from collections import deque
from collections.abc import Callable, Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor
from contextlib import suppress
from pathlib import Path
from typing import Protocol

from rp_ylx.recording.manifest_builder import (
    DeviceRecordingError,
    FinalizedArtifact,
    recording_error,
)
from rp_ylx.recording.verified import VerifiedArtifact


class AudioRecorderAdapter(Protocol):
    """Explicit source-checkout test adapter; production audio is owned by SessionStore."""

    def start(self) -> None: ...

    def snapshot(self) -> Mapping[str, object]: ...

    def stop(self, timeout_seconds: float = 5.0) -> Mapping[str, object]: ...

    def abort(self) -> None: ...

    def close(self) -> None: ...


def strict_int(value: object, field: str) -> int:
    """Validate that `value` is a non-boolean non-negative integer."""
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise DeviceRecordingError("audio_invalid", f"{field} 必须是非负整数")
    return value


def apply_audio_result(
    result: object,
    *,
    partial: Path,
    verified_finalization_fn: Callable[..., tuple[FinalizedArtifact, VerifiedArtifact]],
) -> tuple[
    dict[str, object],
    list[dict[str, object]],
    int,
    dict[str, tuple[int, int, int, int]],
    dict[str, VerifiedArtifact],
]:
    """Validate producer audio inventory and verify WAV segment contents."""
    if not isinstance(result, Mapping):
        raise DeviceRecordingError("audio_invalid", "原生音频结果无效")
    segments = result.get("segments")
    if not isinstance(segments, list) or not segments:
        raise DeviceRecordingError("audio_empty", "音频录制没有可封存分段")
    sample_rate_hz = strict_int(result.get("sample_rate_hz"), "audio.sample_rate_hz")
    channels = strict_int(result.get("channels"), "audio.channels")
    sample_format = str(result.get("sample_format"))
    if sample_rate_hz <= 0:
        raise DeviceRecordingError("audio_invalid", "audio.sample_rate_hz 必须是正整数")
    if channels <= 0:
        raise DeviceRecordingError("audio_invalid", "audio.channels 必须是正整数")
    if sample_format != "S16_LE":
        raise DeviceRecordingError("audio_invalid", "audio.sample_format 必须是 S16_LE")
    bytes_per_sample_frame = channels * 2
    inventory = []
    expected_sample = 0
    for expected_index, raw_segment in enumerate(segments):
        if not isinstance(raw_segment, Mapping):
            raise DeviceRecordingError("audio_invalid", "音频分段记录无效")
        index = strict_int(raw_segment.get("index"), "audio.segments[].index")
        start_sample = strict_int(raw_segment.get("start_sample"), "audio.segments[].start_sample")
        end_sample = strict_int(raw_segment.get("end_sample"), "audio.segments[].end_sample")
        relative = raw_segment.get("path")
        if index != expected_index or start_sample != expected_sample or end_sample <= start_sample:
            raise DeviceRecordingError("audio_invalid", "音频分段序号或采样域不连续")
        if not isinstance(relative, str) or relative != f"audio/audio_{index:05d}.wav":
            raise DeviceRecordingError("audio_invalid", "音频 artifact 路径无效")
        inventory.append((index, start_sample, end_sample, relative, raw_segment))
        expected_sample = end_sample
    sample_count = strict_int(result.get("sample_count"), "audio.sample_count")
    if sample_count != expected_sample:
        raise DeviceRecordingError("audio_invalid", "音频总采样数与分段不一致")

    records = []
    proofs = {}
    identities = {}
    audio_bytes = 0
    with ThreadPoolExecutor(max_workers=2, thread_name_prefix="audio-inventory") as pool:
        pending = deque(
            pool.submit(
                verified_finalization_fn,
                partial / segment[3],
                None,
                code="audio_invalid",
            )
            for segment in inventory[:2]
        )
        for offset, segment in enumerate(inventory):
            future = pending.popleft()
            index, start_sample, end_sample, relative, raw_segment = segment
            finalized, proof = future.result()
            pcm_payload_bytes = (end_sample - start_sample) * bytes_per_sample_frame
            wav_header_bytes = finalized.bytes - pcm_payload_bytes
            if wav_header_bytes < 44 or wav_header_bytes > 65_536:
                raise DeviceRecordingError("audio_invalid", "音频 WAV header 与采样字节数不一致")
            identities[relative] = finalized.identity
            proofs[relative] = proof
            audio_bytes += finalized.bytes
            records.append(
                {
                    "index": index,
                    "start_sample": start_sample,
                    "end_sample": end_sample,
                    "start_time_seconds": float(raw_segment["start_time_seconds"]),
                    "end_time_seconds": float(raw_segment["end_time_seconds"]),
                    "pcm_payload_bytes": pcm_payload_bytes,
                    "wav_header_bytes": wav_header_bytes,
                    "artifact": {
                        "artifact_id": finalized.sha256,
                        "role": "audio.wav",
                        "path": relative,
                        "media_type": "audio/wav",
                        "bytes": finalized.bytes,
                        "sha256": finalized.sha256,
                    },
                }
            )
            if offset + 2 < len(inventory):
                pending.append(
                    pool.submit(
                        verified_finalization_fn,
                        partial / inventory[offset + 2][3],
                        None,
                        code="audio_invalid",
                    )
                )
    return dict(result), records, audio_bytes, identities, proofs


def build_audio_block(
    audio_result: Mapping[str, object] | None,
    audio_segment_records: Sequence[Mapping[str, object]],
    started_monotonic_ns: int,
) -> dict[str, object]:
    """Assemble the `audio` manifest block from validated segment records and clock anchors."""
    if audio_result is None or not audio_segment_records:
        raise DeviceRecordingError("audio_invalid", "音频结果缺失")
    started_ns = strict_int(audio_result.get("started_monotonic_ns"), "audio.started_monotonic_ns")
    stopped_ns = strict_int(audio_result.get("stopped_monotonic_ns"), "audio.stopped_monotonic_ns")
    if stopped_ns < started_ns:
        raise DeviceRecordingError("audio_invalid", "音频单调时间回退")
    sample_rate_hz = strict_int(audio_result.get("sample_rate_hz"), "audio.sample_rate_hz")
    if sample_rate_hz <= 0:
        raise DeviceRecordingError("audio_invalid", "audio.sample_rate_hz 必须是正整数")
    start_offset_ns = started_ns - started_monotonic_ns
    stop_offset_ns = stopped_ns - started_monotonic_ns
    sync = {
        "clock": "host_monotonic",
        "timebase": "monotonic_ns",
        "session_start_monotonic_ns": started_monotonic_ns,
        "started_monotonic_ns": started_ns,
        "stopped_monotonic_ns": stopped_ns,
        "session_start_offset_ns": start_offset_ns,
        "session_stop_offset_ns": stop_offset_ns,
        "session_start_offset_seconds": start_offset_ns / 1e9,
        "session_stop_offset_seconds": stop_offset_ns / 1e9,
        "sample_duration_ns": 1_000_000_000 // sample_rate_hz,
    }
    if (
        sync.get("clock") != "host_monotonic"
        or sync.get("timebase") != "monotonic_ns"
        or sync.get("started_monotonic_ns") != started_ns
        or sync.get("stopped_monotonic_ns") != stopped_ns
    ):
        raise DeviceRecordingError("audio_invalid", "音频时间线结果无效")
    start_time_seconds = float(sync["session_start_offset_seconds"])
    end_time_seconds = float(sync["session_stop_offset_seconds"])
    capture_clock = None
    clock_json = audio_result.get("capture_clock_json")
    if clock_json is not None:
        capture_clock = json.loads(clock_json)
        capture_clock["session_start_monotonic_ns"] = started_monotonic_ns
        start_time_seconds = (
            capture_clock["sample_start_monotonic_ns"] - started_monotonic_ns
        ) / 1e9
        end_time_seconds = (capture_clock["sample_end_monotonic_ns"] - started_monotonic_ns) / 1e9
    if start_time_seconds < 0 or end_time_seconds <= start_time_seconds:
        raise DeviceRecordingError("audio_invalid", "音频时间线 offset 无效")
    return {
        "state": "recorded",
        "requested_mode": "enabled",
        "resolved_mode": "enabled",
        "codec": "pcm_s16le",
        "container": "wav",
        "sample_format": "S16_LE",
        "sample_rate": sample_rate_hz,
        "channels": strict_int(audio_result.get("channels"), "audio.channels"),
        "sample_count": strict_int(audio_result.get("sample_count"), "audio.sample_count"),
        "sync": {
            "time_base": "host_monotonic",
            "start_time_seconds": start_time_seconds,
            "end_time_seconds": end_time_seconds,
            "video_time_reference": "session_time_seconds",
        },
        "segments": list(audio_segment_records),
        **({"capture_clock": capture_clock} if capture_clock is not None else {}),
    }


def stop_and_close_audio(recorder: AudioRecorderAdapter) -> Mapping[str, object]:
    """Stop `recorder` and always close it before returning or raising."""
    try:
        return recorder.stop(timeout_seconds=5.0)
    except BaseException as error:
        raise recording_error(error, "audio_failed") from error
    finally:
        with suppress(Exception):
            recorder.close()


def abandon_audio_recorder(recorder: AudioRecorderAdapter | None) -> None:
    """Best-effort abort and close for an audio recorder adapter."""
    if recorder is None:
        return
    with suppress(Exception):
        recorder.abort()
    with suppress(Exception):
        recorder.close()

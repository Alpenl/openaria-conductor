#!/usr/bin/env python3
"""Collect one RDK X5 audio/video sync acceptance sample.

This script is intentionally small and device-local.  It starts a short
production recording through the Device API, waits for a fixed duration, stops
the take, then verifies that the sealed Device Session contains split-eye video
artifacts plus Rust/ALSA WAV audio artifacts on the same host-monotonic
timeline.

Run on the RDK as root when the recording files are service-owned, for example:

    sudo /opt/rp-ylx/current/bin/python3 scripts/rdk_audio_sync_acceptance.py
"""

from __future__ import annotations

import argparse
import json
import subprocess
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

UTC = timezone.utc  # noqa: UP017 - RDK Ubuntu 22.04 system Python is 3.10.

DEFAULT_BASE_URL = "http://127.0.0.1:8080"
DEFAULT_RECORDING_ROOT = Path("/data/recordings")
DEFAULT_ACCEPTANCE_DIR = Path("/data/acceptance")
DEFAULT_RP_YLX_BIN = Path("/opt/rp-ylx/current/bin/rp-ylx")


@dataclass(frozen=True)
class HttpResult:
    status: int
    headers: dict[str, str]
    payload: Any
    body: bytes


def _utc_now() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def _utc_stamp() -> str:
    return datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")


def _url(base_url: str, path: str) -> str:
    return base_url.rstrip("/") + path


def _request(
    base_url: str,
    method: str,
    path: str,
    *,
    payload: dict[str, Any] | None = None,
    headers: dict[str, str] | None = None,
    timeout: float = 10.0,
) -> HttpResult:
    data = None
    request_headers = {"Accept": "application/json"}
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
        request_headers["Content-Type"] = "application/json"
    if headers:
        request_headers.update(headers)
    request = urllib.request.Request(
        _url(base_url, path),
        data=data,
        headers=request_headers,
        method=method,
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            body = response.read()
            return HttpResult(
                response.status,
                dict(response.headers),
                _parse_json_body(body),
                body,
            )
    except urllib.error.HTTPError as exc:
        body = exc.read()
        return HttpResult(exc.code, dict(exc.headers), _parse_json_body(body), body)


def _parse_json_body(body: bytes) -> Any:
    if not body:
        return None
    try:
        return json.loads(body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return body.decode("utf-8", "replace")


def _range_request(base_url: str, path: str, byte_range: str) -> HttpResult:
    request = urllib.request.Request(
        _url(base_url, path),
        headers={"Range": byte_range},
        method="GET",
    )
    try:
        with urllib.request.urlopen(request, timeout=15.0) as response:
            body = response.read()
            return HttpResult(response.status, dict(response.headers), None, body)
    except urllib.error.HTTPError as exc:
        return HttpResult(exc.code, dict(exc.headers), None, exc.read())


def _require_status(result: HttpResult, expected: set[int], action: str) -> None:
    if result.status not in expected:
        raise RuntimeError(f"{action} failed: HTTP {result.status}: {result.payload!r}")


def _active_recording(status_payload: dict[str, Any]) -> dict[str, Any] | None:
    snapshot = status_payload.get("snapshot")
    if not isinstance(snapshot, dict):
        return None
    active = snapshot.get("active_recording")
    return active if isinstance(active, dict) else None


def _session_id_from_active(active: dict[str, Any]) -> str:
    recording_state = active.get("recording_state")
    if not isinstance(recording_state, dict):
        raise RuntimeError(f"active recording has no recording_state: {active!r}")
    session_id = recording_state.get("session_id")
    if not isinstance(session_id, str) or not session_id:
        raise RuntimeError(f"active recording has no session_id: {active!r}")
    return session_id


def _iter_video_artifacts(manifest: dict[str, Any]) -> list[dict[str, Any]]:
    video = manifest.get("video")
    if not isinstance(video, dict):
        return []
    artifacts: list[dict[str, Any]] = []
    for segment in video.get("segments") or []:
        if not isinstance(segment, dict):
            continue
        eyes = segment.get("artifacts")
        if not isinstance(eyes, dict):
            continue
        for artifact in eyes.values():
            if isinstance(artifact, dict):
                artifacts.append(artifact)
    return artifacts


def _first_audio_artifact(audio: dict[str, Any]) -> dict[str, Any]:
    segments = audio.get("segments")
    if not isinstance(segments, list) or not segments:
        raise RuntimeError("manifest has no audio segments")
    first = segments[0]
    if not isinstance(first, dict) or not isinstance(first.get("artifact"), dict):
        raise RuntimeError(f"first audio segment is invalid: {first!r}")
    return first["artifact"]


def _audio_sample_rate(audio: dict[str, Any]) -> Any:
    return audio.get("sample_rate", audio.get("sample_rate_hz"))


def _audio_sync_is_host_monotonic(sync: Any) -> bool:
    if not isinstance(sync, dict):
        return False
    return (sync.get("clock") == "host_monotonic" and sync.get("timebase") == "monotonic_ns") or (
        sync.get("time_base") == "host_monotonic"
        and sync.get("video_time_reference") == "session_time_seconds"
    )


def _audio_sync_offsets_are_monotonic(sync: Any) -> bool:
    if not isinstance(sync, dict):
        return False
    if isinstance(sync.get("session_start_offset_ns"), int) and isinstance(
        sync.get("session_stop_offset_ns"), int
    ):
        return sync["session_stop_offset_ns"] >= sync["session_start_offset_ns"]
    start_seconds = sync.get("start_time_seconds")
    end_seconds = sync.get("end_time_seconds")
    return (
        isinstance(start_seconds, (int, float))
        and isinstance(end_seconds, (int, float))
        and end_seconds >= start_seconds
    )


def _run_validate(rp_ylx_bin: Path, session_dir: Path) -> dict[str, Any]:
    completed = subprocess.run(
        [str(rp_ylx_bin), "validate", str(session_dir)],
        check=False,
        capture_output=True,
        text=True,
        timeout=90,
    )
    return {
        "returncode": completed.returncode,
        "stdout": _parse_validation_stdout(completed.stdout),
        "stderr": completed.stderr.strip(),
    }


def _parse_validation_stdout(stdout: str) -> Any:
    stripped = stdout.strip()
    if not stripped:
        return None
    try:
        return json.loads(stripped)
    except json.JSONDecodeError:
        return stripped


def _manifest_summary(manifest: dict[str, Any]) -> dict[str, Any]:
    frames = manifest.get("frames")
    integrity = manifest.get("integrity")
    time_info = manifest.get("time")
    return {
        "sealed": manifest.get("sealed"),
        "duration_seconds": (
            time_info.get("duration_seconds") if isinstance(time_info, dict) else None
        ),
        "frames": frames.get("count") if isinstance(frames, dict) else None,
        "dropped_frames": (
            integrity.get("dropped_frames") if isinstance(integrity, dict) else None
        ),
        "drop_events": (integrity.get("drop_events") if isinstance(integrity, dict) else None),
        "fatal_errors": (integrity.get("fatal_errors") if isinstance(integrity, dict) else None),
    }


def _checks(
    *,
    device_commit: str | None,
    expected_commit: str | None,
    session_id: str,
    manifest: dict[str, Any],
    validation: dict[str, Any],
    audio: dict[str, Any],
    first_audio_artifact: dict[str, Any],
    wav_header: bytes,
    range_result: HttpResult,
    mid_recording_samples: list[dict[str, Any]],
) -> dict[str, bool]:
    video_artifacts = _iter_video_artifacts(manifest)
    sync = audio.get("sync")
    integrity = manifest.get("integrity")
    sample_rate = _audio_sample_rate(audio)
    sample_count = audio.get("sample_count")
    segments = audio.get("segments")
    return {
        "device_commit_matches": expected_commit is None or device_commit == expected_commit,
        "session_id_matches": manifest.get("session_id") == session_id,
        "sealed": manifest.get("sealed") is True,
        "zero_dropped_frames": isinstance(integrity, dict) and integrity.get("dropped_frames") == 0,
        "no_drop_events": isinstance(integrity, dict) and integrity.get("drop_events") == [],
        "no_fatal_errors": isinstance(integrity, dict) and integrity.get("fatal_errors") == [],
        "validate_ok": validation.get("returncode") == 0
        and isinstance(validation.get("stdout"), dict)
        and validation["stdout"].get("valid") is True,
        "has_video_left": any(item.get("role") == "video.left" for item in video_artifacts),
        "has_video_right": any(item.get("role") == "video.right" for item in video_artifacts),
        "has_audio_manifest": bool(audio),
        "audio_state_recorded": audio.get("state", "recorded") == "recorded",
        "audio_codec_pcm_s16le": audio.get("codec") == "pcm_s16le",
        "audio_container_wav": audio.get("container") == "wav",
        "audio_sample_rate_48000": sample_rate == 48_000,
        "audio_channels_stereo": audio.get("channels") == 2,
        "audio_sample_count_positive": isinstance(sample_count, int) and sample_count > 0,
        "audio_segments_positive": isinstance(segments, list) and len(segments) > 0,
        "audio_artifact_declared": first_audio_artifact.get("role") == "audio.wav"
        and first_audio_artifact.get("media_type") == "audio/wav",
        "audio_sync_host_monotonic": _audio_sync_is_host_monotonic(sync),
        "audio_offsets_monotonic": _audio_sync_offsets_are_monotonic(sync),
        "audio_wav_header_ok": wav_header[:4] == b"RIFF" and wav_header[8:12] == b"WAVE",
        "audio_range_ok": range_result.status == 206
        and range_result.body[:4] == b"RIFF"
        and len(range_result.body) == 44,
        "live_imu_seen_during_recording": any(
            sample.get("live_imu_present") is True for sample in mid_recording_samples
        ),
    }


def collect(args: argparse.Namespace) -> dict[str, Any]:
    started_at = _utc_now()
    device = _request(args.base_url, "GET", "/api/v3/device")
    _require_status(device, {200}, "device")
    build = device.payload.get("build") if isinstance(device.payload, dict) else None
    device_commit = build.get("commit") if isinstance(build, dict) else None

    initial_status = _request(args.base_url, "GET", "/api/v3/capture/status")
    _require_status(initial_status, {200}, "capture status")
    if _active_recording(initial_status.payload) is not None:
        raise RuntimeError("device has an active recording; refusing to interrupt it")

    session_id: str | None = None
    start_result: HttpResult | None = None
    stop_attempted = False
    mid_samples: list[dict[str, Any]] = []
    try:
        start_result = _request(
            args.base_url,
            "POST",
            "/api/v3/capture/start",
            payload={
                "schema": "ylx.capture-start.v2",
                "mode": "production",
                "display_name": args.display_name,
                "take": {"kind": "new"},
            },
            headers={"Idempotency-Key": f"audio-sync-start-{int(time.time())}"},
        )
        _require_status(start_result, {200, 202}, "capture start")
        active = _active_recording(start_result.payload)
        if active is None:
            time.sleep(1.0)
            active_status = _request(args.base_url, "GET", "/api/v3/capture/status")
            _require_status(active_status, {200}, "capture status after start")
            active = _active_recording(active_status.payload)
        if active is None:
            raise RuntimeError(f"capture start did not expose active_recording: {start_result}")
        session_id = _session_id_from_active(active)

        deadline = time.monotonic() + args.duration
        while time.monotonic() < deadline:
            sample_result = _request(args.base_url, "GET", "/api/v3/capture/status")
            if sample_result.status == 200 and isinstance(sample_result.payload, dict):
                snapshot = sample_result.payload.get("snapshot")
                runtime = snapshot.get("runtime") if isinstance(snapshot, dict) else None
                active_sample = _active_recording(sample_result.payload)
                state = (
                    active_sample.get("recording_state")
                    if isinstance(active_sample, dict)
                    else None
                )
                mid_samples.append(
                    {
                        "observed_at": (
                            runtime.get("observed_at") if isinstance(runtime, dict) else None
                        ),
                        "live_imu_present": isinstance(runtime, dict)
                        and runtime.get("live_imu") is not None,
                        "progress": state.get("progress") if isinstance(state, dict) else None,
                    }
                )
            time.sleep(args.sample_interval)

        stop_attempted = True
        stop_result = _request(
            args.base_url,
            "POST",
            "/api/v3/capture/stop",
            payload={"schema": "ylx.capture-stop.v2", "reason": "user"},
            headers={"Idempotency-Key": f"audio-sync-stop-{int(time.time())}"},
        )
        _require_status(stop_result, {200, 202, 204}, "capture stop")
        _wait_for_idle(args.base_url, timeout_seconds=args.stop_timeout)
    finally:
        if session_id is not None and not stop_attempted:
            _request(
                args.base_url,
                "POST",
                "/api/v3/capture/stop",
                payload={"schema": "ylx.capture-stop.v2", "reason": "acceptance_abort"},
                headers={"Idempotency-Key": f"audio-sync-abort-{int(time.time())}"},
            )

    assert session_id is not None
    session_dir = args.recording_root / session_id
    manifest_path = session_dir / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    audio = manifest.get("audio")
    if not isinstance(audio, dict):
        raise RuntimeError("manifest has no audio object")
    first_audio_artifact = _first_audio_artifact(audio)
    audio_path = session_dir / str(first_audio_artifact["path"])
    wav_header = audio_path.read_bytes()[:44]
    validation = _run_validate(args.rp_ylx_bin, session_dir)
    range_result = _range_request(
        args.base_url,
        f"/api/v3/sessions/{session_id}/artifacts/{first_audio_artifact['artifact_id']}",
        "bytes=0-43",
    )
    sample_rate = _audio_sample_rate(audio)
    sample_count = audio.get("sample_count")
    duration_from_samples = (
        sample_count / sample_rate
        if isinstance(sample_count, int) and isinstance(sample_rate, int) and sample_rate > 0
        else None
    )
    checks = _checks(
        device_commit=device_commit,
        expected_commit=args.expected_commit,
        session_id=session_id,
        manifest=manifest,
        validation=validation,
        audio=audio,
        first_audio_artifact=first_audio_artifact,
        wav_header=wav_header,
        range_result=range_result,
        mid_recording_samples=mid_samples,
    )
    result = {
        "schema": "ylx.acceptance.audio-video-sync-smoke.v1",
        "started_at": started_at,
        "finished_at": _utc_now(),
        "device": {
            "base_url": args.base_url,
            "device_label": (
                device.payload.get("device", {}).get("device_label")
                if isinstance(device.payload, dict)
                else None
            ),
            "build_commit": device_commit,
        },
        "display_name": args.display_name,
        "session_id": session_id,
        "session_dir": str(session_dir),
        "manifest_path": str(manifest_path),
        "manifest_summary": _manifest_summary(manifest),
        "validation": validation,
        "audio": {
            "codec": audio.get("codec"),
            "container": audio.get("container"),
            "device": audio.get("device"),
            "sample_rate_hz": sample_rate,
            "channels": audio.get("channels"),
            "sample_format": audio.get("sample_format"),
            "sample_count": sample_count,
            "duration_from_samples_s": duration_from_samples,
            "segments": len(audio.get("segments") or []),
            "first_segment": {
                "path": first_audio_artifact.get("path"),
                "role": first_audio_artifact.get("role"),
                "media_type": first_audio_artifact.get("media_type"),
                "bytes": first_audio_artifact.get("bytes"),
                "sha256": first_audio_artifact.get("sha256"),
            },
            "sync": audio.get("sync"),
            "range": {
                "status": range_result.status,
                "content_range": range_result.headers.get("Content-Range"),
                "content_length": range_result.headers.get("Content-Length"),
                "bytes_read": len(range_result.body),
            },
        },
        "mid_recording_samples": mid_samples,
        "checks": checks,
        "ok": all(checks.values()),
    }
    return result


def _wait_for_idle(base_url: str, *, timeout_seconds: float) -> None:
    deadline = time.monotonic() + timeout_seconds
    last_payload: Any = None
    while time.monotonic() < deadline:
        result = _request(base_url, "GET", "/api/v3/capture/status")
        if result.status == 200 and isinstance(result.payload, dict):
            last_payload = result.payload
            if _active_recording(result.payload) is None:
                return
        time.sleep(1.0)
    raise RuntimeError(f"capture did not return idle before timeout: {last_payload!r}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL)
    parser.add_argument("--duration", type=float, default=12.0)
    parser.add_argument("--sample-interval", type=float, default=2.0)
    parser.add_argument("--stop-timeout", type=float, default=60.0)
    parser.add_argument("--recording-root", type=Path, default=DEFAULT_RECORDING_ROOT)
    parser.add_argument("--acceptance-dir", type=Path, default=DEFAULT_ACCEPTANCE_DIR)
    parser.add_argument("--rp-ylx-bin", type=Path, default=DEFAULT_RP_YLX_BIN)
    parser.add_argument(
        "--display-name",
        default="audio-sync-smoke",
        help="display_name written into the short acceptance session",
    )
    parser.add_argument(
        "--expected-commit",
        help="expected Device API build commit; omit to skip exact commit matching",
    )
    parser.add_argument("--output", type=Path, help="explicit JSON output path")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.duration <= 0 or args.sample_interval <= 0 or args.stop_timeout <= 0:
        raise SystemExit("duration, sample-interval and stop-timeout must be positive")
    result = collect(args)
    args.acceptance_dir.mkdir(parents=True, exist_ok=True)
    output = args.output or args.acceptance_dir / f"audio-sync-{_utc_stamp()}.json"
    output.write_text(
        json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(
        json.dumps(
            {
                "ok": result["ok"],
                "path": str(output),
                "session_id": result["session_id"],
                "checks": result["checks"],
            },
            ensure_ascii=False,
            sort_keys=True,
        )
    )
    return 0 if result["ok"] else 2


if __name__ == "__main__":
    raise SystemExit(main())

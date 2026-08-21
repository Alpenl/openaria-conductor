#!/usr/bin/env python3
"""Collect one RDK X5 live projection acceptance sample.

The harness is intended to run on the RDK. It starts one short production
recording, samples /capture/status once per second, records a parallel SSE
stream including a Last-Event-ID reconnect, stops the recording in a finally
guard, and then reconciles the last live projection with the sealed manifest
and validator output.

The default run is deliberately narrow: it refuses an already-active device,
uses /data/recordings, admits only the current /data loopback mount, and never
performs reboot, network, power, partition, or service mutation.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import threading
import time
import urllib.error
import urllib.parse
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
DEFAULT_SERVICE_NAME = "rp-ylx"
SCHEMA = "ylx.acceptance.live-projection.v1"
SSE_READ_TIMEOUT_SECONDS = 30.0
MAX_STATUS_LATENCY_SECONDS = 2.0
MAX_FINAL_ELAPSED_LAG_SECONDS = 5.0
MAX_FINAL_FRAME_LAG = 180
# One open 30-second pair of 8-Mbit/s eye segments plus poll/stop tail overhead.
MAX_FINAL_BYTE_LAG = 80 * 1024 * 1024
REQUIRED_ARTIFACT_ROLES = {
    "audio.wav",
    "frames.index",
    "imu.samples",
    "video.left",
    "video.right",
}


@dataclass(frozen=True)
class HttpResult:
    status: int
    headers: dict[str, str]
    payload: Any
    body: bytes
    latency_s: float


def _utc_now() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def _utc_stamp() -> str:
    return datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")


def _url(base_url: str, path: str) -> str:
    return base_url.rstrip("/") + path


def _parse_json_body(body: bytes) -> Any:
    if not body:
        return None
    try:
        return json.loads(body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return body.decode("utf-8", "replace")


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
    started = time.monotonic()
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            body = response.read()
            latency = time.monotonic() - started
            return HttpResult(
                response.status,
                dict(response.headers),
                _parse_json_body(body),
                body,
                latency,
            )
    except urllib.error.HTTPError as exc:
        body = exc.read()
        latency = time.monotonic() - started
        return HttpResult(exc.code, dict(exc.headers), _parse_json_body(body), body, latency)


def _require_status(result: HttpResult, expected: set[int], action: str) -> None:
    if result.status not in expected:
        raise RuntimeError(f"{action} failed: HTTP {result.status}: {result.payload!r}")


def _active_recording(status_payload: MappingLike) -> dict[str, Any] | None:
    snapshot = status_payload.get("snapshot") if isinstance(status_payload, dict) else None
    active = snapshot.get("active_recording") if isinstance(snapshot, dict) else None
    return active if isinstance(active, dict) else None


def _session_id_from_active(active: dict[str, Any]) -> str:
    state = active.get("recording_state")
    if not isinstance(state, dict):
        raise RuntimeError(f"active recording has no recording_state: {active!r}")
    session_id = state.get("session_id")
    if not isinstance(session_id, str) or not session_id:
        raise RuntimeError(f"active recording has no session_id: {active!r}")
    return session_id


def _service_status(service_name: str) -> dict[str, Any]:
    fields = (
        "ActiveState",
        "SubState",
        "MainPID",
        "NRestarts",
        "FragmentPath",
        "ExecMainStartTimestamp",
    )
    command = ["systemctl", "show", service_name, *[f"--property={field}" for field in fields]]
    try:
        completed = subprocess.run(
            command,
            check=False,
            capture_output=True,
            text=True,
            timeout=5,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        return {"available": False, "error": str(exc)}
    values: dict[str, Any] = {
        "available": completed.returncode == 0,
        "returncode": completed.returncode,
    }
    if completed.stderr.strip():
        values["stderr"] = completed.stderr.strip()
    for line in completed.stdout.splitlines():
        key, _, value = line.partition("=")
        if key:
            values[key] = (
                int(value) if key in {"MainPID", "NRestarts"} and value.isdigit() else value
            )
    return values


def _data_mount_info(path: Path = Path("/data")) -> dict[str, Any]:
    try:
        completed = subprocess.run(
            ["findmnt", "-J", str(path)],
            check=False,
            capture_output=True,
            text=True,
            timeout=5,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        return {"available": False, "error": str(exc), "target": str(path)}
    result: dict[str, Any] = {
        "available": completed.returncode == 0,
        "returncode": completed.returncode,
        "target": str(path),
    }
    if completed.stderr.strip():
        result["stderr"] = completed.stderr.strip()
    try:
        parsed = json.loads(completed.stdout or "{}")
    except json.JSONDecodeError:
        result["stdout"] = completed.stdout.strip()
        return result
    filesystems = parsed.get("filesystems")
    first = filesystems[0] if isinstance(filesystems, list) and filesystems else None
    if isinstance(first, dict):
        result.update(
            {
                "source": first.get("source"),
                "fstype": first.get("fstype"),
                "options": first.get("options"),
            }
        )
    return result


def _require_safe_default_storage(args: argparse.Namespace, mount: dict[str, Any]) -> None:
    if args.recording_root != DEFAULT_RECORDING_ROOT and not args.allow_recording_root:
        raise RuntimeError(
            f"refusing recording root {args.recording_root}; pass --allow-recording-root"
        )
    if args.allow_non_loopback_data:
        return
    source = mount.get("source")
    if not isinstance(source, str) or not Path(source).name.startswith("loop"):
        raise RuntimeError(
            f"refusing non-loopback /data source {source!r}; pass --allow-non-loopback-data"
        )


def _parse_sse_block(lines: list[str]) -> dict[str, Any] | None:
    if not lines or lines[0].startswith(":"):
        return None
    fields: dict[str, str] = {}
    data_lines: list[str] = []
    for line in lines:
        if line.startswith("data:"):
            data_lines.append(line.removeprefix("data:").lstrip())
            continue
        key, separator, value = line.partition(":")
        if separator:
            fields[key] = value.lstrip()
    if "id" not in fields or "event" not in fields or not data_lines:
        return {"malformed": lines}
    payload = "\n".join(data_lines)
    return {
        "id": fields["id"],
        "event": fields["event"],
        "data": _parse_json_body(payload.encode("utf-8")),
    }


def _read_sse_event(response: Any, stop: threading.Event) -> dict[str, Any] | None:
    lines: list[str] = []
    while not stop.is_set():
        try:
            raw = response.readline()
        except TimeoutError:
            return None
        if not raw:
            return None
        line = raw.decode("utf-8", "replace").rstrip("\r\n")
        if not line:
            return _parse_sse_block(lines)
        lines.append(line)
    return None


def _sse_monitor(
    base_url: str,
    stop: threading.Event,
    output: list[dict[str, Any]],
    *,
    reconnect_after_s: float,
    timeout: float = SSE_READ_TIMEOUT_SECONDS,
) -> None:
    last_event_id: str | None = None
    reconnect_done = False
    connected_at = time.monotonic()
    while not stop.is_set():
        headers = {"Accept": "text/event-stream"}
        if last_event_id is not None:
            headers["Last-Event-ID"] = last_event_id
        request = urllib.request.Request(
            _url(base_url, "/api/v3/capture/events"),
            headers=headers,
            method="GET",
        )
        try:
            with urllib.request.urlopen(request, timeout=timeout) as response:
                output.append(
                    {
                        "kind": "connected",
                        "observed_at": _utc_now(),
                        "last_event_id": last_event_id,
                        "status": response.status,
                    }
                )
                while not stop.is_set():
                    event = _read_sse_event(response, stop)
                    if event is not None:
                        event["kind"] = "event"
                        event["observed_at"] = _utc_now()
                        output.append(event)
                        event_id = event.get("id")
                        if isinstance(event_id, str):
                            last_event_id = event_id
                    if (
                        not reconnect_done
                        and last_event_id is not None
                        and time.monotonic() - connected_at >= reconnect_after_s
                    ):
                        output.append(
                            {
                                "kind": "reconnect",
                                "observed_at": _utc_now(),
                                "last_event_id": last_event_id,
                            }
                        )
                        reconnect_done = True
                        break
                    if event is None:
                        continue
                connected_at = time.monotonic()
        except Exception as exc:  # noqa: BLE001 - acceptance evidence should record failures.
            output.append({"kind": "error", "observed_at": _utc_now(), "error": str(exc)})
            stop.wait(min(1.0, timeout))


def _run_validate(rp_ylx_bin: Path, session_dir: Path) -> dict[str, Any]:
    completed = subprocess.run(
        [str(rp_ylx_bin), "validate", str(session_dir)],
        check=False,
        capture_output=True,
        text=True,
        timeout=120,
    )
    stdout = completed.stdout.strip()
    return {
        "returncode": completed.returncode,
        "stdout": _parse_json_body(stdout.encode("utf-8")) if stdout else None,
        "stderr": completed.stderr.strip(),
    }


def _artifact_inventory(manifest: dict[str, Any]) -> list[dict[str, Any]]:
    artifacts: list[dict[str, Any]] = []

    def visit(value: Any) -> None:
        if isinstance(value, dict):
            if {
                "artifact_id",
                "role",
                "path",
                "bytes",
            }.issubset(value):
                artifacts.append(
                    {
                        "artifact_id": value.get("artifact_id"),
                        "role": value.get("role"),
                        "path": value.get("path"),
                        "bytes": value.get("bytes"),
                        "sha256": value.get("sha256"),
                    }
                )
            for child in value.values():
                visit(child)
        elif isinstance(value, list):
            for child in value:
                visit(child)

    visit(manifest)
    return artifacts


def _manifest_summary(manifest: dict[str, Any]) -> dict[str, Any]:
    frames = manifest.get("frames")
    integrity = manifest.get("integrity")
    time_info = manifest.get("time")
    imu = manifest.get("imu")
    artifacts = _artifact_inventory(manifest)
    return {
        "sealed": manifest.get("sealed"),
        "duration_seconds": (
            time_info.get("duration_seconds") if isinstance(time_info, dict) else None
        ),
        "frames": frames.get("count") if isinstance(frames, dict) else None,
        "imu_samples": imu.get("sample_count") if isinstance(imu, dict) else None,
        "dropped_frames": (
            integrity.get("dropped_frames") if isinstance(integrity, dict) else None
        ),
        "drop_events": integrity.get("drop_events") if isinstance(integrity, dict) else None,
        "fatal_errors": integrity.get("fatal_errors") if isinstance(integrity, dict) else None,
        "artifact_count": len(artifacts),
        "artifact_bytes": sum(
            item["bytes"] for item in artifacts if isinstance(item["bytes"], int)
        ),
        "roles": sorted({str(item["role"]) for item in artifacts}),
    }


def _header(headers: dict[str, str], name: str) -> str | None:
    expected = name.lower()
    return next((value for key, value in headers.items() if key.lower() == expected), None)


def _artifact_range_probe(
    base_url: str,
    session_id: str,
    manifest: dict[str, Any],
) -> dict[str, Any]:
    artifact = next(
        (
            item
            for item in _artifact_inventory(manifest)
            if isinstance(item.get("artifact_id"), str)
            and isinstance(item.get("bytes"), int)
            and item["bytes"] > 0
        ),
        None,
    )
    if artifact is None:
        return {"available": False, "error": "manifest has no nonempty artifact"}
    artifact_id = str(artifact["artifact_id"])
    path = (
        f"/api/v3/sessions/{urllib.parse.quote(session_id, safe='')}"
        f"/artifacts/{urllib.parse.quote(artifact_id, safe='')}"
    )
    result = _request(
        base_url,
        "GET",
        path,
        headers={"Accept": "*/*", "Range": "bytes=0-0"},
        timeout=30.0,
    )
    return {
        "available": True,
        "artifact_id": artifact_id,
        "role": artifact.get("role"),
        "declared_bytes": artifact.get("bytes"),
        "http_status": result.status,
        "body_bytes": len(result.body),
        "content_range": _header(result.headers, "Content-Range"),
        "latency_s": result.latency_s,
    }


def _status_sample(result: HttpResult) -> dict[str, Any]:
    payload = result.payload if isinstance(result.payload, dict) else {}
    snapshot = payload.get("snapshot") if isinstance(payload, dict) else None
    runtime = snapshot.get("runtime") if isinstance(snapshot, dict) else None
    active = _active_recording(payload)
    recording_state = active.get("recording_state") if isinstance(active, dict) else None
    progress = recording_state.get("progress") if isinstance(recording_state, dict) else None
    live_imu = runtime.get("live_imu") if isinstance(runtime, dict) else None
    clock = live_imu.get("clock") if isinstance(live_imu, dict) else None
    raw = live_imu.get("raw") if isinstance(live_imu, dict) else None
    sync = live_imu.get("sync") if isinstance(live_imu, dict) else None
    return {
        "observed_at": _utc_now(),
        "http_status": result.status,
        "latency_s": result.latency_s,
        "authority_epoch": payload.get("authority_epoch"),
        "source_revision": payload.get("source_revision"),
        "device_state": snapshot.get("device_state") if isinstance(snapshot, dict) else None,
        "recording_state": (
            recording_state.get("state") if isinstance(recording_state, dict) else None
        ),
        "state_revision": (
            recording_state.get("state_revision") if isinstance(recording_state, dict) else None
        ),
        "progress": progress if isinstance(progress, dict) else None,
        "live_imu": {
            "timestamp_ns": clock.get("timestamp_ns") if isinstance(clock, dict) else None,
            "time_base": clock.get("time_base") if isinstance(clock, dict) else None,
            "raw": raw if isinstance(raw, dict) else None,
            "sync": sync if isinstance(sync, dict) else None,
        }
        if isinstance(live_imu, dict)
        else None,
    }


def _first_last_status(
    samples: list[dict[str, Any]],
) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    valid = [item for item in samples if item.get("http_status") == 200]
    return (valid[0], valid[-1]) if valid else (None, None)


def _counter_reconciliation(
    samples: list[dict[str, Any]],
    manifest_summary: dict[str, Any],
) -> dict[str, Any]:
    first, last = _first_last_status(samples)
    last_progress = last.get("progress") if isinstance(last, dict) else None
    first_progress = first.get("progress") if isinstance(first, dict) else None
    result = {
        "first_progress": first_progress,
        "last_progress": last_progress,
        "manifest": {
            "duration_seconds": manifest_summary.get("duration_seconds"),
            "frames": manifest_summary.get("frames"),
            "artifact_bytes": manifest_summary.get("artifact_bytes"),
            "imu_samples": manifest_summary.get("imu_samples"),
        },
    }
    if isinstance(first_progress, dict) and isinstance(last_progress, dict):
        result["live_delta"] = {
            "elapsed_seconds": _numeric_delta(first_progress, last_progress, "elapsed_seconds"),
            "captured_frames": _numeric_delta(first_progress, last_progress, "captured_frames"),
            "bytes_written": _numeric_delta(first_progress, last_progress, "bytes_written"),
        }
    if isinstance(last_progress, dict):
        result["final_delta"] = {
            "elapsed_seconds": _numeric_pair_delta(
                last_progress.get("elapsed_seconds"), manifest_summary.get("duration_seconds")
            ),
            "captured_frames": _numeric_pair_delta(
                last_progress.get("captured_frames"), manifest_summary.get("frames")
            ),
            "bytes_written": _numeric_pair_delta(
                last_progress.get("bytes_written"), manifest_summary.get("artifact_bytes")
            ),
        }
    return result


def _same_non_null_value(samples: list[dict[str, Any]], key: str) -> bool:
    values = [sample.get(key) for sample in samples]
    return bool(values) and all(value is not None for value in values) and len(set(values)) == 1


def _final_lag_is_bounded(reconciliation: dict[str, Any]) -> bool:
    delta = reconciliation.get("final_delta")
    if not isinstance(delta, dict):
        return False
    elapsed = delta.get("elapsed_seconds")
    frames = delta.get("captured_frames")
    written = delta.get("bytes_written")
    if any(isinstance(value, bool) for value in (elapsed, frames, written)):
        return False
    if not all(isinstance(value, (int, float)) for value in (elapsed, frames, written)):
        return False
    return (
        0 <= elapsed <= MAX_FINAL_ELAPSED_LAG_SECONDS
        and 0 <= frames <= MAX_FINAL_FRAME_LAG
        and 0 <= written <= MAX_FINAL_BYTE_LAG
    )


def _sse_reconnect_converged(events: list[dict[str, Any]]) -> bool:
    for reconnect_index, reconnect in enumerate(events):
        if reconnect.get("kind") != "reconnect":
            continue
        last_event_id = reconnect.get("last_event_id")
        for connected_index in range(reconnect_index + 1, len(events)):
            connected = events[connected_index]
            if connected.get("kind") != "connected":
                continue
            if connected.get("last_event_id") != last_event_id:
                continue
            return any(item.get("kind") == "event" for item in events[connected_index + 1 :])
    return False


def _sse_progress_advances(events: list[dict[str, Any]]) -> bool:
    progress = [
        item.get("data", {}).get("data")
        for item in events
        if item.get("kind") == "event"
        and item.get("event") == "progress"
        and isinstance(item.get("data"), dict)
        and isinstance(item["data"].get("data"), dict)
    ]
    if len(progress) < 2:
        return False
    return _progress_increases(
        progress[0], progress[-1], "elapsed_seconds"
    ) and _progress_increases(progress[0], progress[-1], "completed_units")


def _numeric_delta(first: dict[str, Any], last: dict[str, Any], key: str) -> float | int | None:
    return _numeric_pair_delta(first.get(key), last.get(key))


def _numeric_pair_delta(first: Any, last: Any) -> float | int | None:
    if isinstance(first, bool) or isinstance(last, bool):
        return None
    if isinstance(first, (int, float)) and isinstance(last, (int, float)):
        return last - first
    return None


def _checks(
    *,
    expected_commit: str | None,
    device_commit: str | None,
    pre_status: HttpResult,
    samples: list[dict[str, Any]],
    manifest_summary: dict[str, Any],
    validation: dict[str, Any],
    sse_events: list[dict[str, Any]],
    service_before: dict[str, Any],
    service_after: dict[str, Any],
    duration_requested_s: float,
    sample_interval_s: float,
    reconciliation: dict[str, Any],
    range_probe: dict[str, Any],
) -> dict[str, bool]:
    first, last = _first_last_status(samples)
    first_progress = first.get("progress") if isinstance(first, dict) else None
    last_progress = last.get("progress") if isinstance(last, dict) else None
    first_imu = first.get("live_imu") if isinstance(first, dict) else None
    last_imu = last.get("live_imu") if isinstance(last, dict) else None
    progress_events = [
        item
        for item in sse_events
        if item.get("kind") == "event" and item.get("event") == "progress"
    ]
    valid_samples = [item for item in samples if item.get("http_status") == 200]
    revision_stable = _same_non_null_value(
        valid_samples, "authority_epoch"
    ) and _same_non_null_value(valid_samples, "source_revision")
    state_revision_stable = _same_non_null_value(valid_samples, "state_revision")
    expected_samples = max(2, int(duration_requested_s / sample_interval_s * 0.7))
    expected_source = (
        (first.get("authority_epoch"), first.get("source_revision"))
        if isinstance(first, dict)
        else None
    )
    progress_sources_match = bool(progress_events) and all(
        isinstance(item.get("data"), dict)
        and (item["data"].get("authority_epoch"), item["data"].get("source_revision"))
        == expected_source
        for item in progress_events
    )
    roles = manifest_summary.get("roles")
    content_range = range_probe.get("content_range")
    return {
        "device_commit_matches": expected_commit is None or device_commit == expected_commit,
        "preflight_idle": _active_recording(pre_status.payload) is None,
        "status_samples_present": len(samples) >= expected_samples,
        "status_samples_healthy": len(valid_samples) == len(samples)
        and all(
            item.get("recording_state") == "recording" and isinstance(item.get("progress"), dict)
            for item in valid_samples
        ),
        "status_latency_bounded": bool(valid_samples)
        and max(float(item.get("latency_s", float("inf"))) for item in valid_samples)
        <= MAX_STATUS_LATENCY_SECONDS,
        "recording_source_revision_stable": revision_stable,
        "recording_state_revision_stable": state_revision_stable,
        "same_revision_progress_elapsed_advances": _progress_increases(
            first_progress, last_progress, "elapsed_seconds"
        )
        and revision_stable,
        "same_revision_progress_frames_advance": _progress_increases(
            first_progress, last_progress, "captured_frames"
        )
        and revision_stable,
        "same_revision_progress_bytes_advance": _progress_increases(
            first_progress, last_progress, "bytes_written"
        )
        and revision_stable,
        "same_revision_live_imu_timestamp_advances": _imu_timestamp_advances(first_imu, last_imu)
        and revision_stable,
        "live_imu_raw_changes": isinstance(first_imu, dict)
        and isinstance(last_imu, dict)
        and first_imu.get("raw") != last_imu.get("raw"),
        "sse_progress_rate_bounded": len(progress_events)
        >= max(2, int(duration_requested_s * 0.5)),
        "sse_progress_source_revision_stable": progress_sources_match,
        "sse_progress_payload_advances": _sse_progress_advances(sse_events),
        "sse_reconnect_attempted": any(item.get("kind") == "reconnect" for item in sse_events),
        "sse_reconnect_converged": _sse_reconnect_converged(sse_events),
        "sse_errors_absent": not any(item.get("kind") == "error" for item in sse_events),
        "sealed_manifest": manifest_summary.get("sealed") is True,
        "zero_dropped_frames": manifest_summary.get("dropped_frames") == 0,
        "no_drop_events": manifest_summary.get("drop_events") == [],
        "no_fatal_errors": manifest_summary.get("fatal_errors") == [],
        "validate_ok": validation.get("returncode") == 0
        and isinstance(validation.get("stdout"), dict)
        and validation["stdout"].get("valid") is True,
        "complete_artifact_inventory": isinstance(roles, list)
        and set(roles) >= REQUIRED_ARTIFACT_ROLES,
        "live_to_manifest_lag_bounded": _final_lag_is_bounded(reconciliation),
        "artifact_one_byte_range_ok": range_probe.get("http_status") == 206
        and range_probe.get("body_bytes") == 1
        and isinstance(content_range, str)
        and content_range.startswith("bytes 0-0/"),
        "service_restart_count_stable": service_before.get("NRestarts")
        == service_after.get("NRestarts"),
        "service_main_pid_stable": service_before.get("MainPID") == service_after.get("MainPID"),
    }


def _progress_increases(first: Any, last: Any, key: str) -> bool:
    if not isinstance(first, dict) or not isinstance(last, dict):
        return False
    before = first.get(key)
    after = last.get(key)
    if isinstance(before, bool) or isinstance(after, bool):
        return False
    return isinstance(before, (int, float)) and isinstance(after, (int, float)) and after > before


def _imu_timestamp_advances(first: Any, last: Any) -> bool:
    if not isinstance(first, dict) or not isinstance(last, dict):
        return False
    before = first.get("timestamp_ns")
    after = last.get("timestamp_ns")
    return isinstance(before, int) and isinstance(after, int) and after > before


def collect(args: argparse.Namespace) -> dict[str, Any]:
    started_at = _utc_now()
    service_before = _service_status(args.service_name)
    mount = _data_mount_info(Path("/data"))
    _require_safe_default_storage(args, mount)

    device = _request(args.base_url, "GET", "/api/v3/device")
    _require_status(device, {200}, "device")
    build = device.payload.get("build") if isinstance(device.payload, dict) else None
    device_commit = build.get("commit") if isinstance(build, dict) else None

    pre_status = _request(args.base_url, "GET", "/api/v3/capture/status")
    _require_status(pre_status, {200}, "capture status")
    if _active_recording(pre_status.payload) is not None:
        raise RuntimeError("device has an active recording; refusing to interrupt it")

    session_id: str | None = None
    stop_confirmed_idle = False
    status_samples: list[dict[str, Any]] = []
    sse_events: list[dict[str, Any]] = []
    sse_stop = threading.Event()
    sse_thread = threading.Thread(
        target=_sse_monitor,
        args=(args.base_url, sse_stop, sse_events),
        kwargs={"reconnect_after_s": args.sse_reconnect_after},
        name="rp-ylx-live-projection-sse",
        daemon=True,
    )

    try:
        start = _request(
            args.base_url,
            "POST",
            "/api/v3/capture/start",
            payload={
                "schema": "ylx.capture-start.v2",
                "mode": "production",
                "display_name": args.display_name,
                "take": {"kind": "new"},
            },
            headers={"Idempotency-Key": f"live-projection-start-{int(time.time())}"},
        )
        _require_status(start, {200, 202}, "capture start")
        active = _active_recording(start.payload)
        if active is None:
            active_status = _request(args.base_url, "GET", "/api/v3/capture/status")
            _require_status(active_status, {200}, "capture status after start")
            active = _active_recording(active_status.payload)
        if active is None:
            raise RuntimeError(f"capture start did not expose active_recording: {start.payload!r}")
        session_id = _session_id_from_active(active)

        sse_thread.start()
        deadline = time.monotonic() + args.duration
        while time.monotonic() < deadline:
            sample = _request(args.base_url, "GET", "/api/v3/capture/status", timeout=5)
            status_samples.append(_status_sample(sample))
            time.sleep(args.sample_interval)

        stop_result = _request(
            args.base_url,
            "POST",
            "/api/v3/capture/stop",
            payload={"schema": "ylx.capture-stop.v2", "reason": "user"},
            headers={"Idempotency-Key": f"live-projection-stop-{int(time.time())}"},
        )
        _require_status(stop_result, {200, 202, 204}, "capture stop")
        _wait_for_idle(args.base_url, timeout_seconds=args.stop_timeout)
        stop_confirmed_idle = True
    finally:
        sse_stop.set()
        if sse_thread.is_alive():
            sse_thread.join(timeout=3.0)
        if session_id is not None and not stop_confirmed_idle:
            cleanup = _request(
                args.base_url,
                "POST",
                "/api/v3/capture/stop",
                payload={"schema": "ylx.capture-stop.v2", "reason": "user"},
                headers={"Idempotency-Key": f"live-projection-cleanup-{int(time.time())}"},
            )
            _require_status(cleanup, {200, 202, 204}, "capture cleanup stop")
            _wait_for_idle(args.base_url, timeout_seconds=args.stop_timeout)

    assert session_id is not None
    session_dir = args.recording_root / session_id
    manifest_path = session_dir / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest_summary = _manifest_summary(manifest)
    validation = _run_validate(args.rp_ylx_bin, session_dir)
    range_probe = _artifact_range_probe(args.base_url, session_id, manifest)
    service_after = _service_status(args.service_name)
    reconciliation = _counter_reconciliation(status_samples, manifest_summary)
    checks = _checks(
        expected_commit=args.expected_commit,
        device_commit=device_commit,
        pre_status=pre_status,
        samples=status_samples,
        manifest_summary=manifest_summary,
        validation=validation,
        sse_events=sse_events,
        service_before=service_before,
        service_after=service_after,
        duration_requested_s=args.duration,
        sample_interval_s=args.sample_interval,
        reconciliation=reconciliation,
        range_probe=range_probe,
    )
    return {
        "schema": SCHEMA,
        "started_at": started_at,
        "finished_at": _utc_now(),
        "duration_requested_s": args.duration,
        "base_url": args.base_url,
        "display_name": args.display_name,
        "session_id": session_id,
        "session_dir": str(session_dir),
        "manifest_path": str(manifest_path),
        "device": {
            "build_commit": device_commit,
            "expected_commit": args.expected_commit,
            "payload": device.payload,
        },
        "service": {"before": service_before, "after": service_after},
        "storage": {"mount": mount, "recording_root": str(args.recording_root)},
        "profile": {
            "api_security_profile": (
                device.payload.get("security_profile") if isinstance(device.payload, dict) else None
            ),
            "non_coverage": [
                "no reboot",
                "no network mutation",
                "no power interruption",
                "no partition mutation",
                "loopback /data is software regression evidence only",
            ],
        },
        "status_samples": status_samples,
        "sse": {
            "events": sse_events,
            "event_count": sum(1 for item in sse_events if item.get("kind") == "event"),
            "progress_count": sum(
                1
                for item in sse_events
                if item.get("kind") == "event" and item.get("event") == "progress"
            ),
            "reconnect_count": sum(1 for item in sse_events if item.get("kind") == "reconnect"),
        },
        "manifest_summary": manifest_summary,
        "validation": validation,
        "artifact_range_probe": range_probe,
        "reconciliation": reconciliation,
        "checks": checks,
        "ok": all(checks.values()),
    }


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
    parser.add_argument("--duration", type=float, default=20.0)
    parser.add_argument("--sample-interval", type=float, default=1.0)
    parser.add_argument("--sse-reconnect-after", type=float, default=5.0)
    parser.add_argument("--stop-timeout", type=float, default=90.0)
    parser.add_argument("--recording-root", type=Path, default=DEFAULT_RECORDING_ROOT)
    parser.add_argument("--acceptance-dir", type=Path, default=DEFAULT_ACCEPTANCE_DIR)
    parser.add_argument("--rp-ylx-bin", type=Path, default=DEFAULT_RP_YLX_BIN)
    parser.add_argument("--service-name", default=DEFAULT_SERVICE_NAME)
    parser.add_argument("--display-name", default="live-projection-acceptance")
    parser.add_argument("--expected-commit")
    parser.add_argument("--output", type=Path, help="explicit JSON output path")
    parser.add_argument(
        "--allow-non-loopback-data",
        action="store_true",
        help="allow /data that is not mounted from /dev/loop*",
    )
    parser.add_argument(
        "--allow-recording-root",
        action="store_true",
        help="allow a recording root other than /data/recordings",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.duration < 15 or args.duration > 30:
        raise SystemExit("duration must be between 15 and 30 seconds")
    if args.sample_interval <= 0 or args.stop_timeout <= 0 or args.sse_reconnect_after <= 0:
        raise SystemExit("sample-interval, stop-timeout and sse-reconnect-after must be positive")
    result = collect(args)
    args.acceptance_dir.mkdir(parents=True, exist_ok=True)
    output = args.output or args.acceptance_dir / f"live-projection-{_utc_stamp()}.json"
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


MappingLike = dict[str, Any]


if __name__ == "__main__":
    raise SystemExit(main())

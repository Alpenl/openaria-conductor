"""生成录制会话 v0 的确定性正反例。"""

from __future__ import annotations

import hashlib
import json
import shutil
from pathlib import Path
from typing import Any

from rp_ylx.contracts.frame_stream import write_frame, write_header

SESSION_ID = "0198c9a8-7a3c-7000-8000-000000000001"
ROOT = Path(__file__).resolve().parents[1] / "contracts" / "examples"


def write_json(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )


def write_ndjson(path: Path, records: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "".join(
            json.dumps(record, ensure_ascii=False, sort_keys=True) + "\n" for record in records
        ),
        encoding="utf-8",
    )


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def artifact(root: Path, role: str, relative: str, media_type: str, records: int) -> dict[str, Any]:
    path = root / relative
    return {
        "role": role,
        "path": relative,
        "media_type": media_type,
        "bytes": path.stat().st_size,
        "sha256": sha256(path),
        "records": records,
    }


def build_valid(root: Path) -> None:
    (root / "video").mkdir(parents=True, exist_ok=True)
    state = {
        "format": "ylx.recording-session-state.v0",
        "session_id": SESSION_ID,
        "state": "sealed",
        "started_at": "2026-08-10T01:00:00Z",
        "updated_at": "2026-08-10T01:00:01Z",
        "failure": None,
    }
    write_json(root / "session.json", state)
    for eye in ("left", "right"):
        with (root / "video" / f"{eye}.bin").open("wb") as stream:
            write_header(stream)
            write_frame(stream, f"{eye}-frame-0".encode())
            write_frame(stream, f"{eye}-frame-1".encode())
    frames = [
        {
            "format": "ylx.frame.v0",
            "session_id": SESSION_ID,
            "sequence": index,
            "source_sequence": 40 + index,
            "host_monotonic_ns": 1_000_000_000 + index * 33_333_333,
        }
        for index in range(2)
    ]
    imu = []
    for packet_sequence in range(2):
        device_timestamp = 20_000 + packet_sequence * 1_000
        read_start = 1_000_000_000 + packet_sequence * 10_000_000
        read_end = read_start + 100_000
        for sample_index in range(2):
            sequence = packet_sequence * 2 + sample_index
            imu.append(
                {
                    "format": "ylx.imu.v0",
                    "session_id": SESSION_ID,
                    "sequence": sequence,
                    "packet_sequence": packet_sequence,
                    "sample_index": sample_index,
                    "device_timestamp_raw": device_timestamp,
                    "device_ticks": device_timestamp,
                    "host_read_start_ns": read_start,
                    "host_read_end_ns": read_end,
                    "host_monotonic_ns": (read_start + read_end) // 2,
                    "raw": {
                        "accelerometer": [100 + sequence, -200, 16000],
                        "gyroscope": [1, 2 + sequence, -3],
                    },
                    "sync": {
                        "offset_ns": 900_000_000,
                        "residual_ns": 20_000,
                        "quality": "good",
                    },
                }
            )
    diagnostics = [
        {
            "format": "ylx.diagnostic.v0",
            "session_id": SESSION_ID,
            "monotonic_ns": 1_100_000_000,
            "severity": "info",
            "code": "recording_complete",
            "message": "录制正常结束",
            "count": 1,
        }
    ]
    write_ndjson(root / "frames.ndjson", frames)
    write_ndjson(root / "imu.ndjson", imu)
    write_ndjson(root / "diagnostics.ndjson", diagnostics)
    manifest = {
        "format": "ylx.recording-session.v0",
        "state": "sealed",
        "session_id": SESSION_ID,
        "time": {
            "started_at": "2026-08-10T01:00:00Z",
            "ended_at": "2026-08-10T01:00:01Z",
        },
        "device": {"id": "rp-ylx-test", "software_version": "0.1.0"},
        "capture": {
            "video": {
                "width": 640,
                "height": 480,
                "fps": 30,
                "encoding": "fixture-bytes",
                "coordinate_frame": "opencv_optical",
            },
            "imu": {
                "coordinate_frame": "opencv_optical",
                "device_tick_hz": 1_000_000,
                "ranges": None,
            },
            "clock": {"domain": "host_monotonic", "unit": "nanosecond"},
        },
        "counts": {
            "frames": 2,
            "imu_samples": 4,
            "diagnostics": 1,
            "dropped_frames": 0,
            "dropped_imu_samples": 0,
        },
        "artifacts": [
            artifact(root, "session.metadata", "session.json", "application/json", 1),
            artifact(
                root,
                "video.left",
                "video/left.bin",
                "application/vnd.ylx.frame-stream",
                2,
            ),
            artifact(
                root,
                "video.right",
                "video/right.bin",
                "application/vnd.ylx.frame-stream",
                2,
            ),
            artifact(root, "frames.timeline", "frames.ndjson", "application/x-ndjson", 2),
            artifact(root, "imu.samples", "imu.ndjson", "application/x-ndjson", 4),
            artifact(
                root,
                "diagnostics.events",
                "diagnostics.ndjson",
                "application/x-ndjson",
                1,
            ),
        ],
    }
    write_json(root / "manifest.json", manifest)


def clone_valid(name: str) -> Path:
    target = ROOT / "invalid" / name / SESSION_ID
    shutil.copytree(ROOT / "valid" / SESSION_ID, target, dirs_exist_ok=True)
    return target


def load_manifest(root: Path) -> dict[str, Any]:
    return json.loads((root / "manifest.json").read_text(encoding="utf-8"))


def main() -> int:
    valid = ROOT / "valid" / SESSION_ID
    build_valid(valid)

    target = clone_valid("digest-mismatch")
    manifest = load_manifest(target)
    manifest["artifacts"][1]["sha256"] = "0" * 64
    write_json(target / "manifest.json", manifest)

    target = clone_valid("path-traversal")
    manifest = load_manifest(target)
    manifest["artifacts"][1]["path"] = "../left.bin"
    write_json(target / "manifest.json", manifest)

    target = clone_valid("duplicate-role")
    manifest = load_manifest(target)
    manifest["artifacts"][-1]["role"] = "imu.samples"
    write_json(target / "manifest.json", manifest)

    target = clone_valid("future-version")
    manifest = load_manifest(target)
    manifest["format"] = "ylx.recording-session.v1"
    write_json(target / "manifest.json", manifest)

    target = clone_valid("count-mismatch")
    manifest = load_manifest(target)
    manifest["counts"]["frames"] = 3
    write_json(target / "manifest.json", manifest)

    target = clone_valid("missing-file")
    (target / "video" / "right.bin").unlink(missing_ok=True)

    interrupted = ROOT / "invalid" / "interrupted" / f"{SESSION_ID}.partial"
    interrupted.mkdir(parents=True, exist_ok=True)
    write_json(
        interrupted / "session.json",
        {
            "format": "ylx.recording-session-state.v0",
            "session_id": SESSION_ID,
            "state": "interrupted",
            "started_at": "2026-08-10T01:00:00Z",
            "updated_at": "2026-08-10T01:00:01Z",
            "failure": {"code": "process_interrupted", "message": "进程中断"},
        },
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

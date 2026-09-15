"""Paired local ablations of real recovery/catalog/download code (mock media)."""

# Local source imports must follow the checkout path setup below.
# ruff: noqa: E402

import ast
import hashlib
import json
import shutil
import subprocess
import sys
import tempfile
import time
import wave
from contextlib import ExitStack
from pathlib import Path
from unittest.mock import patch

REPO = Path(__file__).resolve().parents[2]
OUT = Path(__file__).parent
sys.path[:0] = [str(REPO), str(REPO / "src")]
from rp_ylx.api import downloads
from rp_ylx.audio_clock import audio_clock_report
from rp_ylx.recording.coordinator import (
    CaptureCoordinator,
    CoordinatorConfig,
    initialize_capture_volume,
)
from rp_ylx.recording.device_session import write_json_atomic
from tests.test_recording_recovery import RecordingRecoveryTests


def old_collector():
    source = subprocess.check_output(
        ["git", "show", "f89a3b7:src/rp_ylx/api/downloads.py"], cwd=REPO, text=True
    )
    tree = ast.parse(source)
    node = next(
        n
        for n in tree.body
        if isinstance(n, ast.FunctionDef) and n.name == "iter_device_session_v1_artifacts"
    )
    scope = dict(vars(downloads))
    exec(
        compile(
            "from __future__ import annotations\n" + ast.get_source_segment(source, node),
            "<baseline-collector>",
            "exec",
        ),
        scope,
    )
    return scope[node.name]


def add_audio(root, recorder):
    capture = json.loads((root / "capture.json").read_bytes())
    capture["config"]["audio_enabled"] = True
    write_json_atomic(root / "capture.json", capture)
    (root / "audio").mkdir()
    rate = 48000
    with wave.open(str(root / "audio/audio_00000.wav"), "wb") as output:
        output.setparams((2, 2, rate, 0, "NONE", "not compressed"))
        output.writeframes(b"\x01\x00\xfe\xff" * (2 * rate))
    start = recorder._started_monotonic_ns + 20_000_000
    write_json_atomic(
        root / "audio/checkpoint.json",
        {
            "sample_count": 2 * rate,
            "thread_started_monotonic_ns": start,
            "period_frames": 1024,
            "buffer_frames": 8192,
            "anchors": [
                [n, start + round(n * 1e9 / rate)] for n in range(1024, 2 * rate + 1024, 4800)
            ],
            "queue_capacity_frames": 786432,
            "queue_peak_frames": 1024,
            "max_write_ns": 400000,
            "segments": [
                {"path": "audio/audio_00000.wav", "start_sample": 0, "end_sample": 2 * rate}
            ],
        },
    )


def raw_hashes(root):
    return {
        str(p.relative_to(root)): hashlib.sha256(p.read_bytes()).hexdigest()
        for p in root.rglob("*")
        if p.is_file() and (p.suffix in {".mp4", ".wav", ".ndjson"}) and "recovery" not in p.parts
    }


variants = [
    "full",
    "no_segment_checkpoint",
    "no_capture_context",
    "no_recovery",
    "no_checkpoint_and_recovery",
    "no_audio_checkpoint",
    "old_log_collector",
    "no_audio_and_old_collector",
    "corrupt_last_eye",
    "torn_last_index",
    "audio_clock_invalid",
    "old_audio_schema",
]
rows = []
old = old_collector()
baseline_schemas = {
    version: json.loads(
        subprocess.check_output(
            ["git", "show", f"f89a3b7:src/rp_ylx/schemas/ylx-device-session-{version}.schema.json"],
            cwd=REPO,
        )
    )
    for version in ["v2", "v3"]
}
with tempfile.TemporaryDirectory(prefix="openaria-ablation-") as temporary:
    workspace = Path(temporary)
    volume = workspace / "template" / "volume"
    volume.mkdir(parents=True)
    volume_id = initialize_capture_volume(volume)
    sessions = volume / "recordings"
    sessions.mkdir(exist_ok=True)
    recorder = RecordingRecoveryTests().recording(sessions, frames=32)
    partial = recorder.partial_path
    capture = json.loads((partial / "capture.json").read_bytes())
    capture["plan"]["volume_id"] = volume_id
    write_json_atomic(partial / "capture.json", capture)
    add_audio(partial, recorder)
    (partial / "video/left_00010.mp4").write_bytes(b"failed-incomplete-tail")
    template_id = recorder._plan.session_id
    for repetition in range(5):
        # Rotate order to avoid always running full on a cold cache.
        order = variants[repetition:] + variants[:repetition]
        for variant in order:
            case = workspace / f"{variant}-{repetition}"
            shutil.copytree(volume, case / "volume")
            trial_volume = case / "volume"
            root = trial_volume / "recordings" / f"{template_id}.partial"
            if variant in {"no_segment_checkpoint", "no_checkpoint_and_recovery"}:
                (root / "segments.json").unlink()
            if variant == "no_capture_context":
                data = json.loads((root / "capture.json").read_bytes())
                for key in ["config", "plan", "started_monotonic_ns"]:
                    data.pop(key)
                write_json_atomic(root / "capture.json", data)
            if variant in {"no_audio_checkpoint", "no_audio_and_old_collector", "old_audio_schema"}:
                (root / "audio/checkpoint.json").unlink()
            if variant == "audio_clock_invalid":
                data = json.loads((root / "audio/checkpoint.json").read_bytes())
                data["anchors"][3][1] = data["anchors"][2][1] - 1
                write_json_atomic(root / "audio/checkpoint.json", data)
            if variant == "corrupt_last_eye":
                (root / "video/right_00009.mp4").write_bytes(b"corrupted-eye")
            if variant == "torn_last_index":
                path = root / "frames.ndjson"
                lines = path.read_bytes().splitlines(keepends=True)
                path.write_bytes(b"".join(lines[:28]) + lines[28][:25])
            original = raw_hashes(root)
            config = CoordinatorConfig(
                trial_volume,
                case / "state",
                recorder._config,
                minimum_available_bytes=0,
                minimum_available_inodes=0,
            )
            row = {
                "variant": variant,
                "repeat": repetition,
                "input_frames": 32,
                "complete_input_pairs": 10,
                "fault": "process_interruption_with_partial_tail",
                "catalog_sessions": 0,
                "saved_frames": 0,
                "saved_pairs": 0,
                "audio_samples": 0,
                "all_artifacts_readable": False,
                "error": None,
            }
            with ExitStack() as stack:
                downloads._VALIDATED_MANIFEST_CACHE.clear()
                if variant in {"no_recovery", "no_checkpoint_and_recovery"}:
                    stack.enter_context(
                        patch.object(
                            CaptureCoordinator, "_recover_completed_prefix", return_value=None
                        )
                    )
                if variant in {"old_log_collector", "no_audio_and_old_collector"}:
                    stack.enter_context(
                        patch.object(downloads, "iter_device_session_v1_artifacts", old)
                    )
                if variant == "old_audio_schema":
                    for version, schema in baseline_schemas.items():
                        stack.enter_context(
                            patch.object(
                                downloads,
                                f"_DEVICE_SESSION_{version.upper()}_VALIDATOR",
                                downloads.Draft202012Validator(
                                    schema, format_checker=downloads.FormatChecker()
                                ),
                            )
                        )
                started = time.perf_counter_ns()
                coordinator = None
                try:
                    coordinator = CaptureCoordinator(
                        config, mount_checker=lambda p, volume=trial_volume: p == volume
                    )
                    listed = coordinator.list_sessions(
                        cursor=None, limit=50, take_id=None, api_version="v4"
                    )
                    row["catalog_sessions"] = len(listed["items"])
                    final = trial_volume / "recordings" / template_id
                    if final.is_dir():
                        manifest = json.loads((final / "manifest.json").read_bytes())
                        row.update(
                            saved_frames=manifest["frames"]["count"],
                            saved_pairs=len(manifest["video"]["segments"]),
                            audio_state=manifest["audio"]["state"],
                            audio_samples=manifest["audio"].get("sample_count", 0),
                            audio_reason=manifest["audio"].get("reason"),
                        )
                        if row["audio_samples"]:
                            row["audio_continuity"] = audio_clock_report(
                                manifest["audio"], manifest["time"]["duration_seconds"]
                            )["continuity"]
                        # Enumerate with the full implementation even in collector-off cases:
                        # every descriptor in the manifest must actually be downloadable.
                        descriptors = list(downloads.iter_device_session_v1_artifacts(manifest))
                        expected = [
                            a
                            for s in manifest["video"]["segments"]
                            for a in s["artifacts"].values()
                        ]
                        expected += [manifest["frames"]["artifact"], manifest["imu"]["artifact"]]
                        expected += [s["artifact"] for s in manifest["audio"].get("segments", [])]
                        expected += manifest["logs"]
                        row["indexed_artifacts"] = len(descriptors)
                        row["declared_artifacts"] = len(expected)
                        downloaded = []
                        for descriptor in expected:
                            try:
                                representation = coordinator.open_verified_artifact(
                                    template_id, descriptor["artifact_id"], "v4"
                                )
                                try:
                                    data = representation.read()
                                finally:
                                    representation.close()
                                assert hashlib.sha256(data).hexdigest() == descriptor["sha256"]
                                downloaded.append(descriptor["path"])
                            except Exception as error:
                                row.setdefault("download_errors", []).append(
                                    {"path": descriptor["path"], "error": str(error)}
                                )
                        row["downloaded_artifacts"] = len(downloaded)
                        row["all_artifacts_readable"] = len(downloaded) == len(expected)
                except Exception as error:
                    row["error"] = f"{type(error).__name__}: {error}"
                finally:
                    if coordinator is not None:
                        coordinator.close()
                row["elapsed_ms"] = (time.perf_counter_ns() - started) / 1e6
            persisted = trial_volume / "recordings" / template_id
            if not persisted.exists():
                persisted = root
            row["raw_media_unchanged"] = original == raw_hashes(persisted)
            assert row["raw_media_unchanged"]
            rows.append(row)
            print(json.dumps(row, ensure_ascii=False), flush=True)
            shutil.rmtree(case)

assert all(
    row["saved_frames"] == 30 and row["all_artifacts_readable"]
    for row in rows
    if row["variant"] == "full"
)
(OUT / "python-results.json").write_text(json.dumps(rows, ensure_ascii=False, indent=2) + "\n")

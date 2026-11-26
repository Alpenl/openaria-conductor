"""Recording session doubles, fault injection helpers, and recovery fixture builders."""

from __future__ import annotations

import hashlib
import json
import os
import uuid
import wave
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import BinaryIO, Self
from unittest.mock import patch

from rp_ylx.api.downloads import iter_device_session_v1_artifacts
from rp_ylx.camera import FrameObservation, StereoFrame
from rp_ylx.native import NativeRecordingPlan
from rp_ylx.recording import (
    DeviceSessionConfig,
    DeviceSessionRecorder,
    SessionPlan,
    StorageStatus,
    uuid7,
)
from rp_ylx.recording.device_session import write_json_atomic
from rp_ylx.recording.encoding import RecordingEncoding
from rp_ylx.recording.live_seal import LiveSealing

from . import ShortOutput
from .media import FRAME, FakeAudioRecorder, FakeStereoEncoder


class FaultingBinaryStream:
    """Binary stream wrapper injecting short writes or flush errors."""

    def __init__(self, stream: BinaryIO, fault: str) -> None:
        self._stream = stream
        self._fault = fault

    def write(self, payload: bytes) -> int:
        if self._fault == "short_write":
            return self._stream.write(payload[:-1])
        return self._stream.write(payload)

    def flush(self) -> None:
        if self._fault == "flush":
            raise OSError("模拟 flush 失败")
        self._stream.flush()

    def fileno(self) -> int:
        return self._stream.fileno()

    def tell(self) -> int:
        return self._stream.tell()

    def close(self) -> None:
        self._stream.close()

    def __enter__(self) -> Self:
        return self

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        self.close()


class RecordingFaultInjection:
    """File-level fault injector targeting a named file for short_write, flush, or fsync."""

    def __init__(self, target_name: str, fault: str) -> None:
        self._target_name = target_name
        self._fault = fault
        self._descriptors: set[int] = set()
        self._real_open = Path.open
        self._real_fsync = os.fsync

    def open(self, path: Path, *args: object, **kwargs: object) -> BinaryIO:
        stream = self._real_open(path, *args, **kwargs)  # type: ignore[arg-type]
        if path.name != self._target_name:
            return stream
        if self._fault == "fsync":
            self._descriptors.add(stream.fileno())
        return FaultingBinaryStream(  # type: ignore[return-value]
            stream,
            self._fault if self._fault != "fsync" else "none",
        )

    def fsync(self, descriptor: int) -> None:
        if descriptor in self._descriptors:
            self._descriptors.remove(descriptor)
            raise OSError("模拟 fsync 失败")
        self._real_fsync(descriptor)


def path_open_fault(
    injection: RecordingFaultInjection,
) -> Callable[..., BinaryIO]:
    """Return a Path.open replacement delegating to RecordingFaultInjection."""

    def open_with_fault(path: Path, *args: object, **kwargs: object) -> BinaryIO:
        return injection.open(path, *args, **kwargs)

    return open_with_fault


class FakeNativeSessionStore:
    """In-process fake for NativeSessionStore verification methods."""

    def __init__(self) -> None:
        self.verify_calls: list[tuple[int, int, str]] = []

    def verify_fd(
        self,
        descriptor: int,
        expected_bytes: int,
        expected_sha256: str,
    ) -> dict[str, object]:
        self.verify_calls.append((descriptor, expected_bytes, expected_sha256))
        payload = os.pread(descriptor, expected_bytes + 1, 0)
        if len(payload) != expected_bytes or hashlib.sha256(payload).hexdigest() != expected_sha256:
            raise ValueError("native fixture verification mismatch")
        return {}

    def verify_fd_interruptible(
        self,
        descriptor: int,
        expected_bytes: int,
        expected_sha256: str,
        interrupt_check: Callable[[], None],
    ) -> dict[str, object]:
        self.verify_calls.append((descriptor, expected_bytes, expected_sha256))
        digest = hashlib.sha256()
        offset = 0
        while offset < expected_bytes:
            interrupt_check()
            block = os.pread(descriptor, min(1024 * 1024, expected_bytes - offset), offset)
            if not block:
                raise ValueError("native fixture short read")
            digest.update(block)
            offset += len(block)
        if digest.hexdigest() != expected_sha256:
            raise ValueError("digest_mismatch: native fixture verification mismatch")
        return {}


class FakeSessionTransaction:
    """In-process fake for the native recording transaction handle."""

    def __init__(self, plan: NativeRecordingPlan, *, fail_finish: bool = False) -> None:
        self.audio_stop_requested = False
        self.capture_stopped_monotonic_ns: int | None = None
        self.plan = plan
        self.root = Path(plan.session_root)
        self.session_id = plan.session_id
        self.recording_start_monotonic_ns = plan.recording_start_monotonic_ns
        self.segment_frames = plan.segment_frames
        self.frames_written = 0
        self.imu_samples_written = 0
        self._segments: list[dict[str, object]] = []
        self.boundary_calls: list[tuple[int, float]] = []
        self.seal_calls: list[tuple[str, str, str, list[str]]] = []
        self.fail_finish = fail_finish
        self.finished = False
        self.aborted = False
        self.sealed = False

    def request_audio_stop(self) -> None:
        self.audio_stop_requested = True

    @staticmethod
    def _identity(path: Path) -> dict[str, int]:
        metadata = path.stat(follow_symlinks=False)
        return {
            "device": metadata.st_dev,
            "inode": metadata.st_ino,
            "size": metadata.st_size,
            "mtime_ns": metadata.st_mtime_ns,
        }

    def advance(self, *, frames: int, imu_samples: int = 0) -> None:
        if frames <= 0 or frames < self.frames_written:
            raise ValueError("fake transaction frames must increase")
        self.frames_written = frames
        self.imu_samples_written = imu_samples
        (self.root / "frames.ndjson").write_bytes(b'{"frame":0}\n' * frames)
        (self.root / "imu.ndjson").write_bytes(b'{"sample":0}\n' * imu_samples)
        self._segments = []
        for start in range(0, frames, self.segment_frames):
            end = min(frames, start + self.segment_frames)
            index = len(self._segments)
            paths: dict[str, tuple[str, int]] = {}
            for eye in ("left", "right"):
                relative = f"video/{eye}_{index:05d}.mp4"
                payload = f"{eye}-{index}-{start}-{end}".encode() * 8
                (self.root / relative).write_bytes(payload)
                paths[eye] = (relative, len(payload))
            self._segments.append(
                {
                    "index": index,
                    "start_frame": start,
                    "end_frame": end,
                    "left_path": paths["left"][0],
                    "left_bytes": paths["left"][1],
                    "right_path": paths["right"][0],
                    "right_bytes": paths["right"][1],
                }
            )

    def _active_take(self) -> dict[str, object]:
        return {
            "session_id": self.session_id,
            "frame_domain": self.frames_written,
            "frames_written": self.frames_written,
            "pending_frames": 0,
            "drop_events": [],
        }

    def _sink(self, *, artifacts: bool) -> dict[str, object]:
        selected: dict[str, object] = {}
        for role, relative in (
            ("frames.index", "frames.ndjson"),
            ("imu.samples", "imu.ndjson"),
        ):
            path = self.root / relative
            payload = path.read_bytes()
            identity = self._identity(path)
            selected[role] = {
                "role": role,
                "path": relative,
                "bytes": len(payload),
                "sha256": hashlib.sha256(payload).hexdigest(),
                "identity": identity,
            }
        return {
            "frames_written": self.frames_written,
            "imu_samples_written": self.imu_samples_written,
            "frames_indexed": self.frames_written,
            "imu_samples_indexed": self.imu_samples_written,
            "bytes_written": sum(
                int(item["bytes"]) for item in selected.values() if isinstance(item, dict)
            ),
            "artifacts": selected if artifacts else {},
        }

    def snapshot(self) -> dict[str, object]:
        return {
            "state": "finished" if self.finished else "recording",
            "active_take": self._active_take(),
            "sink": self._sink(artifacts=False),
            "segments": list(self._segments),
            "submitted_frames": self.frames_written,
            "audio": None,
        }

    def segments(self) -> list[dict[str, object]]:
        return list(self._segments)

    def finish(self, duration_seconds: float, timeout_seconds: float) -> dict[str, object]:
        del timeout_seconds
        if self.fail_finish:
            raise RuntimeError("encoder_failed: fake transaction finish failure")
        if self.frames_written == 0:
            raise RuntimeError("no_frames: fake transaction has no frames")
        self.finished = True
        return {
            "active_take": self._active_take(),
            "sink": self._sink(artifacts=True),
            "audio": None,
            "segments": list(self._segments),
            "encoder_stats": {"frames": self.frames_written},
            "planner": {
                "segment_frames": self.segment_frames,
                "frames_written": self.frames_written,
                "segment_count": len(self._segments),
                "covered_frames": self.frames_written,
                "boundary_count": len(self._segments) + 1,
                "duration_seconds": duration_seconds,
            },
        }

    def boundary(self, ordinal: int, duration_seconds: float) -> dict[str, object]:
        self.boundary_calls.append((ordinal, duration_seconds))
        return {
            "frame": ordinal,
            "time_seconds": duration_seconds * ordinal / self.frames_written,
        }

    def abort(self, reason: str) -> None:
        del reason
        self.aborted = True

    def open_handle_count(self) -> int:
        return 0 if self.finished or self.aborted else 1

    def seal(
        self,
        partial_path: str,
        final_path: str,
        session_id: str,
        manifest: bytes,
        expected_identities: dict[str, tuple[int, int, int, int]],
        control_names: list[str] | None = None,
    ) -> dict[str, object]:
        if not self.finished:
            raise RuntimeError("invalid_state: transaction must finish before seal")
        artifacts = list(iter_device_session_v1_artifacts(json.loads(manifest)))
        paths = [str(item["path"]) for item in artifacts]
        self.seal_calls.append((partial_path, final_path, session_id, paths))
        partial = Path(partial_path)
        final = Path(final_path)
        for artifact in artifacts:
            relative = str(artifact["path"])
            metadata = (partial / relative).stat(follow_symlinks=False)
            actual = (
                metadata.st_dev,
                metadata.st_ino,
                metadata.st_size,
                metadata.st_mtime_ns,
            )
            if actual != expected_identities[relative]:
                raise RuntimeError("digest_mismatch: artifact changed")
        (partial / "manifest.json").write_bytes(manifest)
        for control_name in control_names or ["recording.json", "capture.json"]:
            (partial / control_name).unlink(missing_ok=True)
        os.rename(partial, final)
        self.sealed = True
        return {
            "manifest_sha256": hashlib.sha256(manifest).hexdigest(),
            "artifact_count": len(artifacts),
            "manifest_bytes": len(manifest),
        }


class FakeSessionStore:
    """In-process fake for native session recording transaction creation."""

    def __init__(self, *, fail_finish: bool = False) -> None:
        self.fail_finish = fail_finish
        self.begin_calls: list[NativeRecordingPlan] = []
        self.transaction: FakeSessionTransaction | None = None

    def begin_recording(self, plan: NativeRecordingPlan) -> FakeSessionTransaction:
        self.begin_calls.append(plan)
        self.transaction = FakeSessionTransaction(plan, fail_finish=self.fail_finish)
        return self.transaction


class FakeNativeSessionStoreIo:
    """In-process fake for native read/verify file descriptor operations."""

    def __init__(self) -> None:
        self.open_calls: list[tuple[int, str]] = []
        self.read_calls: list[tuple[int, int]] = []
        self.verify_calls: list[tuple[int, int, str]] = []

    def open_relative_regular(self, root_descriptor: int, relative_path: str) -> int:
        self.open_calls.append((root_descriptor, relative_path))
        return os.open(
            relative_path,
            os.O_RDONLY | os.O_CLOEXEC | getattr(os, "O_NOFOLLOW", 0),
            dir_fd=root_descriptor,
        )

    def read_bounded_fd(self, descriptor: int, maximum_bytes: int) -> bytes:
        self.read_calls.append((descriptor, maximum_bytes))
        return os.pread(descriptor, maximum_bytes + 1, 0)

    def verify_fd(
        self,
        descriptor: int,
        expected_bytes: int,
        expected_sha256: str,
    ) -> dict[str, object]:
        self.verify_calls.append((descriptor, expected_bytes, expected_sha256))
        return {}


class SplitEyeHarness:
    """Builder/feeder helper for DeviceSessionRecorder split-eye fixtures."""

    def __init__(self, *, segment_seconds: float = 0.1) -> None:
        self.segment_seconds = segment_seconds

    def build(
        self,
        root: Path,
        *,
        queue_capacity: int = 128,
        enqueue_timeout: float = 0.05,
        before_write: object | None = None,
        audio_enabled: bool = False,
        audio_fail_stop: bool = False,
        native_data_plane: bool = False,
        recording_encoding: RecordingEncoding | None = None,
        **encoder_options: object,
    ) -> tuple[DeviceSessionRecorder, list[FakeStereoEncoder], list[FakeAudioRecorder]]:
        revision = 0
        encoders: list[FakeStereoEncoder] = []
        audios: list[FakeAudioRecorder] = []

        def allocate_revision() -> int:
            nonlocal revision
            revision += 1
            return revision

        config = DeviceSessionConfig(
            device_id=str(uuid.uuid4()),
            device_label="YLX-12AB34CD",
            hardware_fingerprint="sha256:" + "a" * 64,
            platform="D-Robotics RDK X5 V1.0 + YLX 2UQ2",
            software_version="0.5.0",
            commit="b" * 40,
            width=3840,
            height=1080,
            sensor_fps=60.0,
            frame_decimation=2,
            video_layout="split-eyes",
            segment_seconds=self.segment_seconds,
            audio_enabled=audio_enabled,
            recording_encoding=recording_encoding,
        )

        def factory(partial: Path) -> FakeStereoEncoder:
            encoder = FakeStereoEncoder(
                partial / "video",
                segment_frames=round(
                    config.segment_seconds * config.sensor_fps / config.frame_decimation
                ),
                **encoder_options,  # type: ignore[arg-type]
            )
            encoders.append(encoder)
            return encoder

        def audio_factory(partial: Path) -> FakeAudioRecorder:
            audio = FakeAudioRecorder(partial, fail_stop=audio_fail_stop)
            audios.append(audio)
            return audio

        recorder = DeviceSessionRecorder(
            root,
            config,
            SessionPlan(
                session_id=uuid7(),
                volume_id=str(uuid.uuid4()),
                generation_id=str(uuid.uuid4()),
                capture_mode="production",
                display_name="split-eye fixture",
                take_id=uuid7(),
                take_sequence=1,
                continuation_of=None,
            ),
            authority_epoch=str(uuid.uuid4()),
            allocate_revision=allocate_revision,
            storage_status=lambda: StorageStatus(1024 * 1024 * 1024, True),
            encoder_factory=None if native_data_plane else factory,  # type: ignore[arg-type]
            checkpoint_interval=0.0,
            queue_capacity=queue_capacity,
            enqueue_timeout=enqueue_timeout,
            before_write=None if native_data_plane else before_write,  # type: ignore[arg-type]
            audio_recorder_factory=(None if native_data_plane else audio_factory),
            native_data_plane=native_data_plane,
        )
        return recorder, encoders, audios

    @staticmethod
    def feed(recorder: DeviceSessionRecorder, count: int) -> None:
        for index in range(count):
            recorder.submit_frame(
                FrameObservation(
                    StereoFrame(
                        source_sequence=index,
                        host_monotonic_ns=index * 33_000_000,
                        left=b"",
                        right=b"",
                        raw_side_by_side=FRAME,
                    ),
                    dropped_before=0,
                )
            )


class RecoveryFixtureBuilder:
    """Builder for interrupted partial recording directories used by recovery tests."""

    def __init__(self, *, segment_seconds: float = 0.1) -> None:
        self._split_eye = SplitEyeHarness(segment_seconds=segment_seconds)

    def recording(
        self,
        root: Path,
        frames: int = 32,
        *,
        recording_encoding: RecordingEncoding | None = None,
    ) -> DeviceSessionRecorder:
        """Create an unsealed partial recording with `frames` frames flushed and abandoned."""
        recorder, _, _ = self._split_eye.build(
            root,
            recording_encoding=recording_encoding,
        )
        with patch.object(recorder, "_now", return_value=datetime.now(UTC) - timedelta(seconds=10)):
            recorder.start()
        origin = recorder._started_monotonic_ns
        assert origin is not None
        for index in range(frames):
            recorder.submit_frame(
                FrameObservation(
                    StereoFrame(
                        source_sequence=100 + index * 2,
                        host_monotonic_ns=origin + 10_000_000 + round(index * 1e9 / 30),
                        left=b"",
                        right=b"",
                        raw_side_by_side=FRAME,
                    ),
                    dropped_before=0,
                )
            )
        recorder._stop_writer()
        recorder._abandon_encoder()
        recorder._close_files(ignore_errors=False)
        if recorder._writer_error is not None:
            raise AssertionError(f"recovery fixture writer failed: {recorder._writer_error}")
        if recorder._frames_written != frames:
            raise AssertionError(
                f"expected {frames} written frames, got {recorder._frames_written}"
            )
        return recorder

    @staticmethod
    def write_audio_prefix(
        partial_root: Path,
        started_monotonic_ns: int,
        *,
        raw_clock: bool = False,
        journal: bool = False,
        journal_publisher: Callable[[Path, list[dict[str, object]]], dict[str, object]]
        | None = None,
    ) -> tuple[bytes, int]:
        """Populate a partial directory with a 2-second 48kHz WAV and audio checkpoint."""
        capture = json.loads((partial_root / "capture.json").read_bytes())
        capture["config"]["audio_enabled"] = True
        write_json_atomic(partial_root / "capture.json", capture)
        (partial_root / "audio").mkdir(exist_ok=True)
        rate = 48_000
        pcm = b"\x01\x00\xfe\xff" * (2 * rate)
        with wave.open(str(partial_root / "audio/audio_00000.wav"), "wb") as output:
            output.setparams((2, 2, rate, 0, "NONE", "not compressed"))
            output.writeframes(pcm)
        start = started_monotonic_ns + 20_000_000
        anchors: list[list[int]] = [
            [n, start + round(n * 1e9 / rate)] for n in range(1024, 2 * rate + 1024, 4800)
        ]
        if raw_clock:
            anchors = [
                [
                    n,
                    t + 1_000_000_000,
                    t - round(min(max(n / rate - 0.3, 0), 0.4) * 500_000),
                    t + 1_000_000_000,
                    t + 1_000_000_000,
                ]
                for n, t in anchors
            ]
        write_json_atomic(
            partial_root / "audio/checkpoint.json",
            {
                "sample_count": 2 * rate,
                "thread_started_monotonic_ns": start,
                "period_frames": 1024,
                "buffer_frames": 8192,
                "schema": (
                    "openaria.audio-checkpoint.v2" if raw_clock else "openaria.audio-checkpoint.v1"
                ),
                "anchors": anchors,
                "queue_capacity_frames": 786432,
                "queue_peak_frames": 1024,
                "max_write_ns": 400000,
                "segments": [
                    {"path": "audio/audio_00000.wav", "start_sample": 0, "end_sample": 2 * rate}
                ],
            },
        )
        if journal:
            if journal_publisher is None:
                raise ValueError("journal_publisher is required when journal=True")
            head = json.loads((partial_root / "audio/checkpoint.json").read_bytes())
            head["segments"][0]["index"] = 0
            published = journal_publisher(
                partial_root,
                [{"sequence": 0, "anchors": anchors, "segments": head["segments"]}],
            )
            published["thread_started_monotonic_ns"] = head["thread_started_monotonic_ns"]
            write_json_atomic(partial_root / "audio/checkpoint.json", published)
            with (partial_root / "audio/checkpoint.ndjson").open("ab") as output:
                output.write(b'{"partial":')
        return pcm, rate


def audio_sealing(root: Path) -> LiveSealing:
    """Create a LiveSealing instance with checkpoint polling but mocked ThreadPoolExecutor."""
    with patch("rp_ylx.recording.live_seal.ThreadPoolExecutor"):
        return LiveSealing(root, 30, 2)


def publish_audio_checkpoint(root: Path, indices: list[int]) -> None:
    """Write dummy audio WAV segments and atomically publish audio/checkpoint.json."""
    directory = root / "audio"
    directory.mkdir(exist_ok=True)
    segments = []
    for index in indices:
        relative = f"audio/audio_{index:05d}.wav"
        path = root / relative
        if not path.exists():
            path.write_bytes(f"closed audio segment {index}".encode())
        segments.append({"path": relative, "index": index})
    temporary = directory / "checkpoint.tmp"
    temporary.write_text(json.dumps({"segments": segments}))
    temporary.replace(directory / "checkpoint.json")


def compact_audio_stub(source: Path, *, durable: bool = True) -> tuple[Path, dict[str, str]]:
    """Stub for rp_ylx.recording.live_seal.compact_audio writing a dummy .flac."""
    del durable
    target = source.with_suffix(".flac")
    target.write_bytes(b"compacted audio")
    return target, {"codec": "flac"}


def audio_inventory_recorder(root: Path) -> DeviceSessionRecorder:
    """Build a DeviceSessionRecorder with `_partial` pointed at `root` for inventory tests."""
    recorder, _, _ = SplitEyeHarness().build(root)
    recorder._partial = root
    return recorder


def make_audio_result(root: Path, relative: str = "audio/audio_00000.wav") -> dict[str, object]:
    """Create a 4-sample 48kHz WAV file at `root / relative` and return audio stop metadata."""
    path = root / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    with wave.open(str(path), "wb") as wav:
        wav.setnchannels(2)
        wav.setsampwidth(2)
        wav.setframerate(48000)
        wav.writeframes(b"\x01\x02\x03\x04" * 4)
    return {
        "sample_rate_hz": 48000,
        "channels": 2,
        "sample_format": "S16_LE",
        "sample_count": 4,
        "segments": [
            {
                "index": 0,
                "start_sample": 0,
                "end_sample": 4,
                "start_time_seconds": 0.0,
                "end_time_seconds": 4 / 48000,
                "path": relative,
            }
        ],
    }


def make_multiple_audio_result(root: Path, count: int = 5) -> dict[str, object]:
    """Create `count` WAV segments under `root/audio` and return combined audio stop metadata."""
    segments = []
    result: dict[str, object] = {}
    for index in range(count):
        result = make_audio_result(root, f"audio/audio_{index:05d}.wav")
        segment = result["segments"][0]  # type: ignore[index]
        assert isinstance(segment, dict)
        segment.update(
            index=index,
            start_sample=index * 4,
            end_sample=(index + 1) * 4,
            start_time_seconds=index * 4 / 48000,
            end_time_seconds=(index + 1) * 4 / 48000,
        )
        segments.append(segment)
    result.update(segments=segments, sample_count=count * 4)
    return result


__all__ = [
    "FakeNativeSessionStore",
    "FakeNativeSessionStoreIo",
    "FakeSessionStore",
    "FakeSessionTransaction",
    "FaultingBinaryStream",
    "RecordingFaultInjection",
    "RecoveryFixtureBuilder",
    "ShortOutput",
    "SplitEyeHarness",
    "audio_inventory_recorder",
    "audio_sealing",
    "compact_audio_stub",
    "make_audio_result",
    "make_multiple_audio_result",
    "path_open_fault",
    "publish_audio_checkpoint",
]

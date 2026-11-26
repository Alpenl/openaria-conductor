"""Synthetic media frames, IMU observations, and fake capture/encoder/audio doubles."""

from __future__ import annotations

import struct
import time
from copy import deepcopy
from pathlib import Path

from rp_ylx.camera import FrameObservation, StereoFrame
from rp_ylx.camera.v4l2 import (
    V4L2_BUF_FLAG_TIMESTAMP_MONOTONIC,
    V4L2_BUF_TYPE_VIDEO_CAPTURE,
    V4L2_CAP_STREAMING,
    V4L2_CAP_VIDEO_CAPTURE,
    VIDIOC_DQBUF,
    VIDIOC_QBUF,
    VIDIOC_QUERYBUF,
    VIDIOC_QUERYCAP,
    VIDIOC_REQBUFS,
    VIDIOC_S_FMT,
    VIDIOC_S_PARM,
    VIDIOC_STREAMOFF,
    VIDIOC_STREAMON,
)
from rp_ylx.imu import ImuObservation, ImuSample, RawVector3
from rp_ylx.recording.stereo_encoder import ClosedSegment, StereoEncoderError

JPEG = b"\xff\xd8raw-side-by-side\xff\xd9"
FRAME = b"\xff\xd8split-eye\xff\xd9"

CAMERA_FOCUS_STATUS: dict[str, object] = {
    "schema": "ylx.camera-focus.v1",
    "value": 42,
    "minimum": 0,
    "maximum": 255,
    "step": 1,
    "default": 32,
    "auto_supported": True,
    "auto_enabled": False,
}


def frame(
    sequence: int = 0,
    *,
    generation_payload: bytes = JPEG,
    host_monotonic_ns: int | None = None,
    left: bytes = b"left-preview",
    right: bytes = b"right-preview",
    dropped_before: int = 0,
) -> FrameObservation:
    """Build a synthetic stereo FrameObservation for coordinator/recording tests."""
    return FrameObservation(
        StereoFrame(
            source_sequence=sequence,
            host_monotonic_ns=(
                1_000_000 + sequence if host_monotonic_ns is None else host_monotonic_ns
            ),
            left=left,
            right=right,
            raw_side_by_side=generation_payload,
        ),
        dropped_before=dropped_before,
    )


def split_eye_frame(
    sequence: int = 0,
    *,
    payload: bytes = FRAME,
    host_monotonic_ns: int | None = None,
    dropped_before: int = 0,
) -> FrameObservation:
    """Build a 30fps-spaced FrameObservation carrying split-eye JPEG bytes."""
    return frame(
        sequence,
        generation_payload=payload,
        host_monotonic_ns=(
            sequence * 33_000_000 if host_monotonic_ns is None else host_monotonic_ns
        ),
        left=b"",
        right=b"",
        dropped_before=dropped_before,
    )


def imu_observation(
    sequence: int = 0,
    *,
    accelerometer: tuple[int, int, int] = (1, 2, 3),
    gyroscope: tuple[int, int, int] = (4, 5, 6),
    sync_quality: str = "insufficient",
) -> ImuObservation:
    """Build a 2-sample synthetic ImuObservation."""
    samples = []
    for sample_index in range(2):
        sample_sequence = sequence + sample_index
        samples.append(
            ImuSample(
                sequence=sample_sequence,
                packet_sequence=sequence // 2,
                sample_index=sample_index,
                device_timestamp_raw=sample_sequence,
                device_ticks=sample_sequence,
                host_read_start_ns=10_000 + sample_sequence,
                host_read_end_ns=10_100 + sample_sequence,
                host_monotonic_ns=10_050 + sample_sequence,
                accelerometer=RawVector3(*accelerometer),
                gyroscope=RawVector3(*gyroscope),
                sync_offset_ns=None,
                sync_residual_ns=None,
                sync_quality=sync_quality,
            )
        )
    return ImuObservation((samples[0], samples[1]), dropped_samples=0)


def single_imu_observation(
    *,
    host_monotonic_ns: int,
    accelerometer: tuple[int, int, int],
    gyroscope: tuple[int, int, int],
    sync_quality: str = "degraded",
) -> ImuObservation:
    """Build a single-sample ImuObservation anchored at host_monotonic_ns."""
    sample = ImuSample(
        sequence=host_monotonic_ns,
        packet_sequence=0,
        sample_index=0,
        device_timestamp_raw=host_monotonic_ns,
        device_ticks=host_monotonic_ns,
        host_read_start_ns=host_monotonic_ns,
        host_read_end_ns=host_monotonic_ns,
        host_monotonic_ns=host_monotonic_ns,
        accelerometer=RawVector3(*accelerometer),
        gyroscope=RawVector3(*gyroscope),
        sync_offset_ns=None,
        sync_residual_ns=None,
        sync_quality=sync_quality,
    )
    return ImuObservation((sample,), dropped_samples=0)


class FakeSources:
    """In-memory CaptureSources test double tracking lifecycle, camera state, and focus."""

    supports_calibration_capture = True

    def __init__(self) -> None:
        self.open_handle_count = 0
        self.mode: str | None = None
        self.generation_id: str | None = None
        self.submit_frame: object | None = None
        self.submit_imu: object | None = None
        self.focus: dict[str, object] | None = None
        self.camera_state = "connected"

    def camera_connection_status(self) -> dict[str, object]:
        return {
            "schema": "ylx.camera-connection.v1",
            "state": self.camera_state,
        }

    def start(
        self,
        *,
        mode: str,
        generation_id: str,
        submit_frame: object,
        submit_imu: object,
        on_failure: object,
        native_recorder: object | None = None,
    ) -> None:
        del on_failure, native_recorder
        self.mode = mode
        self.generation_id = generation_id
        self.submit_frame = submit_frame
        self.submit_imu = submit_imu
        self.open_handle_count = 2

    def stop(self) -> None:
        self.open_handle_count = 0

    def camera_focus_status(self) -> dict[str, object] | None:
        return deepcopy(self.focus)

    def set_camera_focus(
        self,
        *,
        value: int | None = None,
        auto_enabled: bool | None = None,
    ) -> dict[str, object]:
        if self.focus is None:
            raise RuntimeError("焦距控制不可用")
        if value is not None:
            self.focus["value"] = value
            if self.focus["auto_supported"] is True:
                self.focus["auto_enabled"] = False
        if auto_enabled is not None:
            self.focus["auto_enabled"] = auto_enabled
        return deepcopy(self.focus)


class FakeSourcesWithLatestImu(FakeSources):
    """FakeSources returning a mutable single latest IMU observation."""

    def __init__(self, observation: ImuObservation) -> None:
        super().__init__()
        self.observation = observation

    def latest_imu_observation(self) -> ImuObservation:
        return self.observation


class FakeSourcesWithSequentialLatestImu(FakeSources):
    """FakeSources stepping through a list of IMU observations, holding the last."""

    def __init__(self, observations: list[ImuObservation]) -> None:
        super().__init__()
        self._observations = observations
        self._index = 0

    def latest_imu_observation(self) -> ImuObservation:
        if self._index < len(self._observations):
            observation = self._observations[self._index]
            self._index += 1
            return observation
        return self._observations[-1]


class FakeSourcesWithScriptedLatestImu(FakeSources):
    """FakeSources yielding scripted IMU observations, None, or raised exceptions."""

    def __init__(self, observations: list[ImuObservation | BaseException | None]) -> None:
        super().__init__()
        self._observations = observations
        self._index = 0

    def latest_imu_observation(self) -> ImuObservation | None:
        if self._index < len(self._observations):
            observation = self._observations[self._index]
            self._index += 1
        else:
            observation = self._observations[-1]
        if isinstance(observation, BaseException):
            raise observation
        return observation


class FakeStereoEncoder:
    """Fake split-eye encoder writing verifiable segment files without hardware."""

    def __init__(
        self,
        out_dir: Path,
        *,
        segment_frames: int,
        fail_after: int | None = None,
        drop_tail_segment: bool = False,
        **unused: object,
    ) -> None:
        del unused
        self._out_dir = out_dir
        self._segment_frames = segment_frames
        self._fail_after = fail_after
        self._drop_tail_segment = drop_tail_segment
        self._segments: list[ClosedSegment] = []
        self._submitted = 0
        self._started = False
        self.aborted = False

    @property
    def segments(self) -> tuple[ClosedSegment, ...]:
        return tuple(self._segments)

    @property
    def submitted_frames(self) -> int:
        return self._submitted

    def start(self) -> None:
        self._out_dir.mkdir(parents=True, exist_ok=True)
        self._started = True

    def submit(self, jpeg: bytes) -> None:
        del jpeg
        if not self._started:
            raise StereoEncoderError("invalid_state", "助手未启动")
        if self._fail_after is not None and self._submitted >= self._fail_after:
            raise StereoEncoderError("encoder_failed", "助手拒绝该帧")
        self._submitted += 1
        if self._submitted % self._segment_frames == 0:
            self._close(self._submitted - self._segment_frames, self._submitted)

    def finish(self, *, timeout: float = 30.0) -> tuple[ClosedSegment, ...]:
        del timeout
        closed = len(self._segments) * self._segment_frames
        if closed < self._submitted and not self._drop_tail_segment:
            self._close(closed, self._submitted)
        return self.segments

    def abort(self) -> None:
        self.aborted = True

    def _close(self, start_frame: int, end_frame: int) -> None:
        index = len(self._segments)
        payloads: dict[str, tuple[str, int]] = {}
        for eye in ("left", "right"):
            name = f"{eye}_{index:05d}.mp4"
            body = f"{eye}-{index}-{start_frame}-{end_frame}".encode() * 8
            (self._out_dir / name).write_bytes(body)
            payloads[eye] = (f"video/{name}", len(body))
        self._segments.append(
            ClosedSegment(
                index=index,
                start_frame=start_frame,
                end_frame=end_frame,
                left_path=payloads["left"][0],
                left_bytes=payloads["left"][1],
                right_path=payloads["right"][0],
                right_bytes=payloads["right"][1],
            )
        )


FakeSplitEyeEncoder = FakeStereoEncoder


class FakeAudioRecorder:
    """Fake audio recorder writing a valid 48kHz stereo PCM WAV segment on stop."""

    def __init__(self, session_root: Path, *, fail_stop: bool = False) -> None:
        self._session_root = session_root
        self._fail_stop = fail_stop
        self.started = False
        self.aborted = False
        self.started_monotonic_ns = 0
        self.live_sample_count = 0
        self.live_bytes_written = 0

    def start(self) -> None:
        self.started = True
        self.started_monotonic_ns = time.monotonic_ns()

    def advance_live(self, *, sample_count: int, bytes_written: int) -> None:
        self.live_sample_count = sample_count
        self.live_bytes_written = bytes_written

    def snapshot(self) -> dict[str, object]:
        return {
            "sample_count": self.live_sample_count,
            "bytes_written": self.live_bytes_written,
        }

    def stop(self, timeout_seconds: float = 5.0) -> dict[str, object]:
        del timeout_seconds
        if self._fail_stop:
            raise RuntimeError("audio_failed: fake audio stop failure")
        target_stop = self.started_monotonic_ns + 100_000_000
        while time.monotonic_ns() < target_stop:
            time.sleep(0.001)
        stopped_monotonic_ns = time.monotonic_ns()
        path = self._session_root / "audio/audio_00000.wav"
        path.parent.mkdir(parents=True, exist_ok=True)
        sample_count = 4_800
        channels = 2
        bits_per_sample = 16
        pcm_payload = b"\x00" * (sample_count * channels * (bits_per_sample // 8))
        header = bytearray(44)
        header[0:4] = b"RIFF"
        header[4:8] = (36 + len(pcm_payload)).to_bytes(4, "little")
        header[8:12] = b"WAVE"
        header[12:16] = b"fmt "
        header[16:20] = (16).to_bytes(4, "little")
        header[20:22] = (1).to_bytes(2, "little")
        header[22:24] = channels.to_bytes(2, "little")
        header[24:28] = (48_000).to_bytes(4, "little")
        header[28:32] = (48_000 * channels * (bits_per_sample // 8)).to_bytes(4, "little")
        header[32:34] = (channels * (bits_per_sample // 8)).to_bytes(2, "little")
        header[34:36] = bits_per_sample.to_bytes(2, "little")
        header[36:40] = b"data"
        header[40:44] = len(pcm_payload).to_bytes(4, "little")
        path.write_bytes(bytes(header) + pcm_payload)
        return {
            "device": "hw:0,0",
            "codec": "pcm_s16le",
            "container": "wav",
            "sample_rate_hz": 48_000,
            "channels": 2,
            "sample_format": "S16_LE",
            "sample_count": sample_count,
            "started_monotonic_ns": self.started_monotonic_ns,
            "stopped_monotonic_ns": stopped_monotonic_ns,
            "segments": [
                {
                    "index": 0,
                    "path": "audio/audio_00000.wav",
                    "start_sample": 0,
                    "end_sample": sample_count,
                    "start_time_seconds": 0.0,
                    "end_time_seconds": 0.1,
                }
            ],
        }

    def abort(self) -> None:
        self.aborted = True

    def close(self) -> None:
        self.aborted = True


def audio_fixture() -> dict[str, object]:
    """Build a synthetic 2-second 10kHz stereo audio section with capture_clock evidence."""
    return {
        "channels": 2,
        "segments": [
            {
                "index": 0,
                "start_sample": 0,
                "end_sample": 20000,
                "start_time_seconds": 0.0,
                "end_time_seconds": 2.0,
                "pcm_payload_bytes": 80000,
                "wav_header_bytes": 44,
                "artifact": {"bytes": 80044},
            }
        ],
        "state": "recorded",
        "sample_rate": 10000,
        "sample_count": 20000,
        "sync": {"start_time_seconds": 0.1, "end_time_seconds": 2.1},
        "capture_clock": {
            "schema": "openaria.audio-clock.v1",
            "clock": "host_monotonic",
            "timestamp_source": "alsa_htimestamp_dma",
            "continuity": "verified",
            "device": "hw:CARD=D2UQ2,DEV=0",
            "period_frames": 1024,
            "buffer_frames": 8192,
            "thread_started_monotonic_ns": 1100000000,
            "thread_stopped_monotonic_ns": 3110000000,
            "sample_start_monotonic_ns": 1100000000,
            "sample_end_monotonic_ns": 3100000000,
            "session_start_monotonic_ns": 1000000000,
            "max_residual_ns": 0,
            "anchors": [[1024, 1202400000], [11024, 2202400000], [20000, 3100000000]],
            "queue_capacity_frames": 262144,
            "queue_peak_frames": 1024,
            "max_write_ns": 500000,
            "xrun_count": 0,
            "suspend_count": 0,
        },
    }


class FakeMmap:
    """In-memory mmap buffer double for V4L2 device tests."""

    def __init__(self, length: int) -> None:
        self.data = bytearray(length)
        self.closed = False

    def __len__(self) -> int:
        return len(self.data)

    def __getitem__(self, item: object) -> object:
        return self.data[item]  # type: ignore[index]

    def __setitem__(self, item: object, value: object) -> None:
        self.data[item] = value  # type: ignore[index]

    def close(self) -> None:
        self.closed = True


class FakeV4L2Device:
    """In-memory V4L2 ioctl/mmap device double producing scripted JPEG payloads."""

    def __init__(
        self,
        payloads: list[bytes],
        *,
        bad_flags: bool = False,
        timestamps: tuple[int, ...] | None = None,
        sequences: tuple[int, ...] | None = None,
    ) -> None:
        self.payloads = iter(payloads)
        self.bad_flags = bad_flags
        self.timestamps = iter(timestamps) if timestamps is not None else None
        self.sequences = iter(sequences) if sequences is not None else None
        self.buffers = [FakeMmap(4096), FakeMmap(4096)]
        self.ioctls: list[int] = []
        self.queued: list[int] = []
        self.closed_fds: list[int] = []
        self.sequence = 10
        self.timestamp = 1

    def ioctl(self, fd: int, request: int, payload: bytearray) -> None:
        del fd
        self.ioctls.append(request)
        if request == VIDIOC_QUERYCAP:
            struct.pack_into("<II", payload, 84, V4L2_CAP_VIDEO_CAPTURE | V4L2_CAP_STREAMING, 0)
        elif request == VIDIOC_S_FMT:
            self.assert_format(payload)
        elif request == VIDIOC_S_PARM:
            struct.pack_into("<II", payload, 12, 1, 30)
        elif request == VIDIOC_REQBUFS:
            struct.pack_into("<I", payload, 0, 2)
        elif request == VIDIOC_QUERYBUF:
            index = struct.unpack_from("<I", payload, 0)[0]
            struct.pack_into("<I", payload, 64, index * 4096)
            struct.pack_into("<I", payload, 72, 4096)
        elif request == VIDIOC_QBUF:
            self.queued.append(struct.unpack_from("<I", payload, 0)[0])
        elif request == VIDIOC_DQBUF:
            index = self.queued.pop(0) if self.queued else 0
            payload_bytes = next(self.payloads)
            self.buffers[index].data[: len(payload_bytes)] = payload_bytes
            struct.pack_into(
                "<IIII", payload, 0, index, V4L2_BUF_TYPE_VIDEO_CAPTURE, len(payload_bytes), 0
            )
            flags = V4L2_BUF_FLAG_TIMESTAMP_MONOTONIC if not self.bad_flags else 0x40
            struct.pack_into("<I", payload, 12, flags)
            timestamp = next(self.timestamps) if self.timestamps is not None else self.timestamp
            struct.pack_into("<qq", payload, 24, timestamp, 0)
            sequence = next(self.sequences) if self.sequences is not None else self.sequence
            struct.pack_into("<I", payload, 56, sequence)
            self.sequence += 1
            self.timestamp += 1
        elif request in {VIDIOC_STREAMON, VIDIOC_STREAMOFF}:
            return
        else:
            raise AssertionError(f"unexpected ioctl {request:#x}")

    @staticmethod
    def assert_format(payload: bytearray) -> None:
        width, height = struct.unpack_from("<II", payload, 8)
        if (width, height) != (3840, 1080):
            raise AssertionError((width, height))

    def mmap(self, fd: int, length: int, prot: int, flags: int, offset: int) -> FakeMmap:
        del fd, length, prot, flags
        return self.buffers[offset // 4096]


__all__ = [
    "CAMERA_FOCUS_STATUS",
    "ClosedSegment",
    "FRAME",
    "FakeAudioRecorder",
    "FakeMmap",
    "FakeSources",
    "FakeSourcesWithLatestImu",
    "FakeSourcesWithScriptedLatestImu",
    "FakeSourcesWithSequentialLatestImu",
    "FakeSplitEyeEncoder",
    "FakeStereoEncoder",
    "FakeV4L2Device",
    "FrameObservation",
    "ImuObservation",
    "ImuSample",
    "JPEG",
    "RawVector3",
    "StereoEncoderError",
    "StereoFrame",
    "audio_fixture",
    "frame",
    "imu_observation",
    "single_imu_observation",
    "split_eye_frame",
]

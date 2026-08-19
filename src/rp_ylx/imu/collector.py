"""从可替换来源采集完整 IMU 包并生成 v0 样本。"""

from __future__ import annotations

from collections.abc import Iterable
from contextlib import suppress

from rp_ylx.imu.models import ImuError, ImuObservation, ImuPacketRead, ImuSample, ImuSource
from rp_ylx.imu.protocol import decode_packet
from rp_ylx.imu.time_sync import TimestampUnwrapper, TimeSynchronizer


class SyntheticImuSource:
    def __init__(self, reads: Iterable[ImuPacketRead | Exception]) -> None:
        self._reads = iter(reads)
        self.closed = False

    def read_packet(self, timeout: float) -> ImuPacketRead:
        if self.closed:
            raise ImuError("invalid_state", "IMU 来源已经关闭")
        try:
            item = next(self._reads)
        except StopIteration as exc:
            raise TimeoutError("模拟 IMU 停止更新") from exc
        if isinstance(item, Exception):
            raise item
        return item

    def close(self) -> None:
        self.closed = True


class ImuCollector:
    def __init__(
        self,
        source: ImuSource,
        *,
        synchronizer: TimeSynchronizer | None = None,
        unwrapper: TimestampUnwrapper | None = None,
    ) -> None:
        self._source = source
        self._synchronizer = synchronizer or TimeSynchronizer()
        self._unwrapper = unwrapper or TimestampUnwrapper()
        self._packet_sequence = 0
        self._sample_sequence = 0
        self._closed = False

    @property
    def closed(self) -> bool:
        return self._closed

    def read(self, *, timeout: float = 1.0) -> ImuObservation:
        if timeout <= 0:
            raise ValueError("timeout 必须大于零")
        if self._closed:
            raise ImuError("invalid_state", "IMU 采集器已经关闭")
        try:
            packet_read = self._source.read_packet(timeout)
            packet = decode_packet(packet_read.payload)
            device_ticks = self._unwrapper.update(packet.device_timestamp_raw)
            estimate = self._synchronizer.add(
                device_ticks,
                packet_read.host_read_start_ns,
                packet_read.host_read_end_ns,
            )
        except TimeoutError as exc:
            with suppress(Exception):
                self.close()
            raise ImuError("sensor_stalled", "IMU 在超时前没有更新", retryable=True) from exc
        except OSError as exc:
            with suppress(Exception):
                self.close()
            raise ImuError("disconnected", f"IMU 读取失败：{exc}", retryable=True) from exc
        except ImuError:
            with suppress(Exception):
                self.close()
            raise

        dropped_samples = packet_read.lost_packets * 2
        self._packet_sequence += packet_read.lost_packets
        self._sample_sequence += dropped_samples
        host_ns = (packet_read.host_read_start_ns + packet_read.host_read_end_ns) // 2
        samples = []
        for raw_sample in packet.samples:
            samples.append(
                ImuSample(
                    sequence=self._sample_sequence,
                    packet_sequence=self._packet_sequence,
                    sample_index=raw_sample.sample_index,
                    device_timestamp_raw=packet.device_timestamp_raw,
                    device_ticks=device_ticks,
                    host_read_start_ns=packet_read.host_read_start_ns,
                    host_read_end_ns=packet_read.host_read_end_ns,
                    host_monotonic_ns=host_ns,
                    accelerometer=raw_sample.accelerometer,
                    gyroscope=raw_sample.gyroscope,
                    sync_offset_ns=estimate.offset_ns,
                    sync_residual_ns=estimate.residual_ns,
                    sync_quality=estimate.quality,
                )
            )
            self._sample_sequence += 1
        self._packet_sequence += 1
        return ImuObservation((samples[0], samples[1]), dropped_samples)

    def close(self) -> None:
        if not self._closed:
            self._closed = True
            self._source.close()

    def __enter__(self) -> ImuCollector:
        return self

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        self.close()

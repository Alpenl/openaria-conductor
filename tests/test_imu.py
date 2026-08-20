from __future__ import annotations

import struct
import tempfile
import unittest
from pathlib import Path

from rp_ylx.imu import (
    ImuCollector,
    ImuError,
    ImuPacketRead,
    NativeImuCollector,
    SyntheticImuSource,
    TimestampUnwrapper,
    TimeSynchronizer,
    UvcXuImuSource,
    decode_packet,
    discover_uvc_xu_unit,
    find_uvc_xu_unit,
    parse_uvc_extension_units,
)
from rp_ylx.imu.protocol import XU_GUID
from rp_ylx.imu.uvc_xu import UVC_GET_CUR, UVC_GET_LEN, XU_GUID_BYTES

ROOT = Path(__file__).resolve().parent


def packet(timestamp: int, seed: int = 0) -> bytes:
    axes = tuple(seed + value for value in range(12))
    return timestamp.to_bytes(3, "big") + struct.pack(">12h", *axes)


def packet_read(timestamp: int, host_ns: int, *, lost_packets: int = 0) -> ImuPacketRead:
    return ImuPacketRead(
        packet(timestamp),
        host_read_start_ns=host_ns - 100,
        host_read_end_ns=host_ns + 100,
        lost_packets=lost_packets,
    )


class ProtocolTest(unittest.TestCase):
    def test_decodes_fixed_binary_packet_and_int16_boundaries(self) -> None:
        payload = bytes.fromhex((ROOT / "fixtures/imu-packet.hex").read_text().strip())
        decoded = decode_packet(payload)
        self.assertEqual(decoded.device_timestamp_raw, 0x123456)
        self.assertEqual(decoded.samples[0].accelerometer.as_list(), [1, -2, 32767])
        self.assertEqual(decoded.samples[0].gyroscope.as_list(), [-32768, 100, -100])
        self.assertEqual(decoded.samples[1].accelerometer.as_list(), [-1, 2, -32768])
        self.assertEqual(decoded.samples[1].gyroscope.as_list(), [32767, -200, 200])

    def test_rejects_short_and_long_packets(self) -> None:
        for length in (0, 26, 28):
            with self.subTest(length=length):
                with self.assertRaises(ImuError) as raised:
                    decode_packet(b"\x00" * length)
                self.assertEqual(raised.exception.code, "invalid_packet_length")


class TimestampTest(unittest.TestCase):
    def test_unwraps_rollover(self) -> None:
        unwrapper = TimestampUnwrapper(modulus=256)
        self.assertEqual(unwrapper.update(250), 250)
        self.assertEqual(unwrapper.update(3), 259)

    def test_detects_stall_and_regression(self) -> None:
        stalled = TimestampUnwrapper(modulus=256)
        stalled.update(10)
        with self.assertRaises(ImuError) as duplicate:
            stalled.update(10)
        self.assertEqual(duplicate.exception.code, "timestamp_stalled")

        regressed = TimestampUnwrapper(modulus=256)
        regressed.update(10)
        with self.assertRaises(ImuError) as backward:
            regressed.update(9)
        self.assertEqual(backward.exception.code, "timestamp_regression")


class SynchronizerTest(unittest.TestCase):
    def test_reports_insufficient_then_good_fit(self) -> None:
        synchronizer = TimeSynchronizer(expected_tick_hz=1_000_000)
        first = synchronizer.add(1_000, 10_999_900, 11_000_100)
        second = synchronizer.add(2_000, 11_999_900, 12_000_100)
        third = synchronizer.add(3_000, 12_999_900, 13_000_100)
        self.assertEqual(first.quality, "insufficient")
        self.assertIsNone(first.offset_ns)
        self.assertEqual(second.quality, "insufficient")
        self.assertEqual(third.quality, "good")
        self.assertEqual(third.offset_ns, 10_000_000)
        self.assertEqual(third.residual_ns, 100)
        self.assertAlmostEqual(third.estimated_tick_hz or 0, 1_000_000)

    def test_drift_is_degraded_not_hidden(self) -> None:
        synchronizer = TimeSynchronizer(expected_tick_hz=1_000_000)
        estimate = None
        for ticks in (1_000, 2_000, 3_000):
            host = 10_000_000 + ticks * 1_100
            estimate = synchronizer.add(ticks, host - 100, host + 100)
        assert estimate is not None
        self.assertEqual(estimate.quality, "degraded")
        self.assertGreater(abs(estimate.drift_ppm or 0), 2_000)


class CollectorTest(unittest.TestCase):
    def test_emits_two_raw_samples_with_shared_packet_evidence(self) -> None:
        source = SyntheticImuSource(
            [
                packet_read(1_000, 11_000_000),
                packet_read(2_000, 12_000_000),
                packet_read(3_000, 13_000_000),
            ]
        )
        collector = ImuCollector(source, synchronizer=TimeSynchronizer(expected_tick_hz=1_000_000))
        first = collector.read()
        collector.read()
        third = collector.read()

        self.assertEqual([sample.sample_index for sample in first.samples], [0, 1])
        self.assertEqual(first.samples[0].device_ticks, first.samples[1].device_ticks)
        self.assertEqual(first.samples[0].host_monotonic_ns, first.samples[1].host_monotonic_ns)
        self.assertEqual(first.samples[0].sync_quality, "insufficient")
        self.assertEqual(third.samples[0].sync_quality, "good")
        record = first.samples[0].as_record("0198c9a8-7a3c-7000-8000-000000000004")
        self.assertEqual(set(record["raw"]), {"accelerometer", "gyroscope"})
        self.assertNotIn("units", record["raw"])
        self.assertIsNone(record["sync"]["offset_ns"])

    def test_known_transport_loss_creates_sequence_gaps(self) -> None:
        collector = ImuCollector(
            SyntheticImuSource(
                [packet_read(1_000, 11_000_000), packet_read(2_000, 12_000_000, lost_packets=2)]
            )
        )
        first = collector.read()
        second = collector.read()
        self.assertEqual([sample.sequence for sample in first.samples], [0, 1])
        self.assertEqual([sample.sequence for sample in second.samples], [6, 7])
        self.assertEqual(second.samples[0].packet_sequence, 3)
        self.assertEqual(second.dropped_samples, 4)

    def test_stall_and_timestamp_error_close_source(self) -> None:
        stalled_source = SyntheticImuSource([])
        stalled = ImuCollector(stalled_source)
        with self.assertRaises(ImuError) as timeout:
            stalled.read(timeout=0.01)
        self.assertEqual(timeout.exception.code, "sensor_stalled")
        self.assertTrue(stalled_source.closed)

        duplicate_source = SyntheticImuSource(
            [packet_read(1_000, 11_000_000), packet_read(1_000, 12_000_000)]
        )
        duplicate = ImuCollector(duplicate_source)
        duplicate.read()
        with self.assertRaises(ImuError) as timestamp:
            duplicate.read()
        self.assertEqual(timestamp.exception.code, "timestamp_stalled")
        self.assertTrue(duplicate_source.closed)


class NativeCollectorAdapterTest(unittest.TestCase):
    def test_converts_native_observation_to_existing_model(self) -> None:
        class Owner:
            def __init__(self) -> None:
                self.timeout: float | None = None
                self.closed = False

            def read(self, timeout_seconds: float) -> dict[str, object]:
                self.timeout = timeout_seconds
                return {
                    "dropped_samples": 0,
                    "samples": [
                        {
                            "sequence": 0,
                            "packet_sequence": 0,
                            "sample_index": 0,
                            "device_timestamp_raw": 1000,
                            "device_ticks": 1000,
                            "host_read_start_ns": 10_999_900,
                            "host_read_end_ns": 11_000_100,
                            "host_monotonic_ns": 11_000_000,
                            "raw": {
                                "accelerometer": [1, 2, 3],
                                "gyroscope": [4, 5, 6],
                            },
                            "sync": {
                                "offset_ns": None,
                                "residual_ns": None,
                                "quality": "insufficient",
                            },
                        },
                        {
                            "sequence": 1,
                            "packet_sequence": 0,
                            "sample_index": 1,
                            "device_timestamp_raw": 1000,
                            "device_ticks": 1000,
                            "host_read_start_ns": 10_999_900,
                            "host_read_end_ns": 11_000_100,
                            "host_monotonic_ns": 11_000_000,
                            "raw": {
                                "accelerometer": [-1, -2, -3],
                                "gyroscope": [-4, -5, -6],
                            },
                            "sync": {
                                "offset_ns": None,
                                "residual_ns": None,
                                "quality": "insufficient",
                            },
                        },
                    ],
                }

            def unit(self) -> int:
                return 7

            def close(self) -> None:
                self.closed = True

        owner = Owner()
        collector = NativeImuCollector("/dev/video-test", owner=owner)
        observation = collector.read(timeout=0.25)
        self.assertEqual(owner.timeout, 0.25)
        self.assertEqual(collector.unit, 7)
        self.assertEqual(observation.samples[0].accelerometer.as_list(), [1, 2, 3])
        self.assertEqual(observation.samples[1].gyroscope.as_list(), [-4, -5, -6])
        self.assertEqual(observation.samples[0].sync_quality, "insufficient")
        collector.close()
        self.assertTrue(owner.closed)

    def test_native_error_keeps_code_retryability_and_closes_owner(self) -> None:
        class Owner:
            def __init__(self) -> None:
                self.closed = False

            def read(self, timeout_seconds: float) -> dict[str, object]:
                del timeout_seconds
                raise RuntimeError("sensor_stalled: stale IMU packet")

            def close(self) -> None:
                self.closed = True

        owner = Owner()
        collector = NativeImuCollector("/dev/video-test", owner=owner)
        with self.assertRaises(ImuError) as raised:
            collector.read(timeout=0.25)
        self.assertEqual(raised.exception.code, "sensor_stalled")
        self.assertTrue(raised.exception.retryable)
        self.assertTrue(owner.closed)
        self.assertTrue(collector.closed)


class UvcXuImuSourceTest(unittest.TestCase):
    def test_discovers_the_xu_unit_by_guid_from_usb_descriptors(self) -> None:
        extension = bytes([24, 0x24, 0x06, 7]) + XU_GUID_BYTES + bytes(4)
        descriptors = bytes([4, 4, 0, 0]) + extension
        self.assertEqual(parse_uvc_extension_units(descriptors), ((7, XU_GUID_BYTES),))
        self.assertEqual(find_uvc_xu_unit(descriptors, XU_GUID), 7)
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            usb = root / "sys/devices/usb1/1-1"
            interface = usb / "1-1:1.0"
            interface.mkdir(parents=True)
            (usb / "descriptors").write_bytes(descriptors)
            video = root / "sys/class/video4linux/video0"
            video.mkdir(parents=True)
            (video / "device").symlink_to(interface)
            self.assertEqual(
                discover_uvc_xu_unit("/dev/video0", sys_root=root / "sys"),
                7,
            )

    def test_auto_unit_selection_uses_guid_discovery_callback(self) -> None:
        queried: list[tuple[int, int]] = []
        discovered: list[tuple[str | Path, object]] = []

        def query(fd: int, unit: int, selector: int, request: int, size: int) -> bytes:
            queried.append((unit, request))
            return (27).to_bytes(2, "little")

        def discover(device: str | Path, guid: object) -> int:
            discovered.append((device, guid))
            return 9

        source = UvcXuImuSource(
            "/dev/video-test",
            discover_unit=discover,
            query_control=query,
            open_file=lambda path, flags: 7,
            close_file=lambda fd: None,
        )
        source.close()
        self.assertEqual(queried, [(9, UVC_GET_LEN)])
        self.assertEqual(source.unit, 9)
        self.assertEqual(discovered, [("/dev/video-test", XU_GUID)])

    def test_requires_the_exact_27_byte_control_length(self) -> None:
        closed: list[int] = []

        def query(fd: int, unit: int, selector: int, request: int, size: int) -> bytes:
            self.assertEqual((fd, unit, selector, request, size), (7, 3, 1, UVC_GET_LEN, 2))
            return (26).to_bytes(2, "little")

        with self.assertRaises(ImuError) as raised:
            UvcXuImuSource(
                "/dev/video-test",
                unit=3,
                query_control=query,
                open_file=lambda path, flags: 7,
                close_file=closed.append,
            )
        self.assertEqual(raised.exception.code, "unsupported_packet_length")
        self.assertEqual(closed, [7])

    def test_skips_duplicate_timestamp_and_preserves_host_interval(self) -> None:
        payloads = iter([packet(10), packet(10), packet(11)])
        clock_value = 90
        sleeps: list[float] = []

        def clock() -> int:
            nonlocal clock_value
            clock_value += 10
            return clock_value

        def query(fd: int, unit: int, selector: int, request: int, size: int) -> bytes:
            if request == UVC_GET_LEN:
                return (27).to_bytes(2, "little")
            self.assertEqual(request, UVC_GET_CUR)
            return next(payloads)

        source = UvcXuImuSource(
            "/dev/video-test",
            unit=3,
            query_control=query,
            clock_ns=clock,
            sleep=sleeps.append,
            open_file=lambda path, flags: 7,
            close_file=lambda fd: None,
        )
        first = source.read_packet(1.0)
        second = source.read_packet(1.0)
        self.assertEqual(int.from_bytes(first.payload[:3], "big"), 10)
        self.assertEqual(int.from_bytes(second.payload[:3], "big"), 11)
        self.assertEqual(
            (second.host_read_start_ns, second.host_read_end_ns),
            (160, 170),
        )
        self.assertEqual(len(sleeps), 1)

    def test_times_out_when_timestamp_remains_stale(self) -> None:
        clock_value = 0

        def clock() -> int:
            nonlocal clock_value
            clock_value += 1_000_000
            return clock_value

        def query(fd: int, unit: int, selector: int, request: int, size: int) -> bytes:
            if request == UVC_GET_LEN:
                return (27).to_bytes(2, "little")
            return packet(10)

        source = UvcXuImuSource(
            "/dev/video-test",
            unit=3,
            query_control=query,
            clock_ns=clock,
            sleep=lambda duration: None,
            open_file=lambda path, flags: 7,
            close_file=lambda fd: None,
        )
        source.read_packet(0.002)
        with self.assertRaises(TimeoutError):
            source.read_packet(0.002)

    def test_close_is_idempotent(self) -> None:
        closed: list[int] = []
        source = UvcXuImuSource(
            "/dev/video-test",
            unit=3,
            query_control=lambda fd, unit, selector, request, size: (27).to_bytes(2, "little"),
            open_file=lambda path, flags: 7,
            close_file=closed.append,
        )
        source.close()
        source.close()
        self.assertTrue(source.closed)
        self.assertEqual(closed, [7])


if __name__ == "__main__":
    unittest.main()

from __future__ import annotations

import ctypes
import errno
import json
import struct
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import rp_ylx.imu.uvc_xu as uvc_xu
from rp_ylx.imu import (
    ImuCollector,
    ImuError,
    ImuPacketRead,
    SyntheticImuSource,
    TimestampUnwrapper,
    TimeSynchronizer,
    UvcXuImuSource,
    decode_native_imu_observation,
    decode_packet,
    discover_uvc_xu_unit,
    find_uvc_xu_unit,
    parse_uvc_extension_units,
)
from rp_ylx.imu.protocol import XU_GUID
from rp_ylx.imu.uvc_xu import UVC_GET_CUR, UVC_GET_LEN, UVCIOC_CTRL_QUERY, XU_GUID_BYTES

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


class NativeObservationDecoderTest(unittest.TestCase):
    @staticmethod
    def payload() -> dict[str, object]:
        def sample(sequence: int, sample_index: int, sign: int) -> dict[str, object]:
            return {
                "sequence": sequence,
                "packet_sequence": 4,
                "sample_index": sample_index,
                "device_timestamp_raw": 1000,
                "device_ticks": 1000,
                "host_read_start_ns": 10_000_000,
                "host_read_end_ns": 10_010_000,
                "host_monotonic_ns": 10_005_000,
                "raw": {
                    "accelerometer": [sign, sign * 2, sign * 3],
                    "gyroscope": [sign * 4, sign * 5, sign * 6],
                },
                "sync": {
                    "offset_ns": None,
                    "residual_ns": None,
                    "quality": "insufficient",
                },
            }

        return {
            "dropped_samples": 0,
            "samples": [sample(8, 0, 1), sample(9, 1, -1)],
        }

    def test_converts_capture_engine_snapshot_to_existing_model(self) -> None:
        observation = decode_native_imu_observation(self.payload())
        self.assertEqual(observation.samples[0].sequence, 8)
        self.assertEqual(observation.samples[0].accelerometer.as_list(), [1, 2, 3])
        self.assertEqual(observation.samples[1].gyroscope.as_list(), [-4, -5, -6])
        self.assertEqual(observation.samples[0].sync_quality, "insufficient")

    def test_rejects_malformed_capture_engine_snapshot(self) -> None:
        payload = self.payload()
        samples = payload["samples"]
        assert isinstance(samples, list)
        payload["samples"] = [samples[0]]
        with self.assertRaises(ImuError) as raised:
            decode_native_imu_observation(payload)
        self.assertEqual(raised.exception.code, "invalid_native_observation")


class UvcXuImuSourceTest(unittest.TestCase):
    class FakeLibc:
        def __init__(self, rc: int = 0, write: bytes = b"") -> None:
            self.rc = rc
            self.write = write
            self.calls: list[tuple[int, int, int, int, int, int]] = []
            self.initial_buffers: list[bytes] = []

        def ioctl(self, fd: int, request: int, payload: object) -> int:
            control = ctypes.cast(
                payload,
                ctypes.POINTER(uvc_xu._UvcXuControlQuery),
            ).contents
            self.calls.append(
                (fd, request, control.unit, control.selector, control.query, control.size)
            )
            self.initial_buffers.append(ctypes.string_at(control.data, control.size))
            if self.write:
                ctypes.memmove(control.data, self.write, min(len(self.write), control.size))
            if self.rc < 0:
                ctypes.set_errno(errno.ENOTTY)
            return self.rc

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

    def test_linux_control_query_accepts_only_exact_zero_ioctl_success(self) -> None:
        payload = packet(0x123456)
        fake = self.FakeLibc(rc=0, write=payload)
        with patch.object(uvc_xu, "_LIBC", fake):
            result = uvc_xu._linux_control_query(7, 3, 1, UVC_GET_CUR, len(payload))

        self.assertEqual(result, payload)
        self.assertEqual(fake.calls, [(7, UVCIOC_CTRL_QUERY, 3, 1, UVC_GET_CUR, len(payload))])
        self.assertEqual(fake.initial_buffers, [b"\x00" * len(payload)])

        for rc in (-1, 1):
            with self.subTest(rc=rc):
                sentinel = b"XU-SENTINEL-DO-NOT-LEAK!!!!"
                fake = self.FakeLibc(rc=rc, write=sentinel)
                with (
                    patch.object(uvc_xu, "_LIBC", fake),
                    self.assertRaises(ImuError) as raised,
                ):
                    uvc_xu._linux_control_query(
                        7,
                        3,
                        1,
                        UVC_GET_CUR,
                        len(sentinel),
                    )
                self.assertEqual(raised.exception.code, "xu_query_failed")
                self.assertIn("UVC XU ioctl", raised.exception.message)
                self.assertNotIn(sentinel.decode(), str(raised.exception))
                self.assertNotIn(sentinel.decode(), repr(raised.exception))
                self.assertNotIn(
                    sentinel.decode(),
                    json.dumps(
                        {
                            "code": raised.exception.code,
                            "message": raised.exception.message,
                            "retryable": raised.exception.retryable,
                        },
                        ensure_ascii=False,
                    ),
                )

    def test_linux_control_query_rejects_unsafe_requests_before_ioctl(self) -> None:
        fake = self.FakeLibc()
        with patch.object(uvc_xu, "_LIBC", fake):
            with self.assertRaises(ImuError) as oversized:
                uvc_xu._linux_control_query(7, 4, 9, UVC_GET_CUR, 257)
            with self.assertRaises(ImuError) as denylisted_10:
                uvc_xu._linux_control_query(7, 4, 10, UVC_GET_CUR, 16)
            with self.assertRaises(ImuError) as denylisted_15:
                uvc_xu._linux_control_query(7, 4, 15, UVC_GET_CUR, 16)
            with self.assertRaises(ImuError) as set_request:
                uvc_xu._linux_control_query(7, 4, 9, 0x01, 1)
            with self.assertRaises(ImuError) as wrong_imu_size:
                uvc_xu._linux_control_query(7, 3, 1, UVC_GET_CUR, 28)

        self.assertEqual(fake.calls, [])
        for raised in (
            oversized,
            denylisted_10,
            denylisted_15,
            set_request,
            wrong_imu_size,
        ):
            self.assertEqual(raised.exception.code, "xu_query_denied")

    def test_uvc_imu_source_denies_unit_4_risky_get_cur_before_query(self) -> None:
        calls: list[tuple[int, int, int, int, int]] = []

        def query(fd: int, unit: int, selector: int, request: int, size: int) -> bytes:
            calls.append((fd, unit, selector, request, size))
            return (27).to_bytes(2, "little")

        source = UvcXuImuSource(
            "/dev/video-test",
            unit=4,
            selector=10,
            query_control=query,
            open_file=lambda path, flags: 7,
            close_file=lambda fd: None,
        )
        with self.assertRaises(ImuError) as denied:
            source.read_packet(1.0)
        source.close()

        self.assertEqual(denied.exception.code, "xu_query_denied")
        self.assertEqual(calls, [(7, 4, 10, UVC_GET_LEN, 2)])

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

"""Linux UVC extension-unit transport for the YLX IMU packet."""

from __future__ import annotations

import ctypes
import os
import time
from collections.abc import Callable
from pathlib import Path
from uuid import UUID

from rp_ylx.imu.models import ImuError, ImuPacketRead
from rp_ylx.imu.protocol import PACKET_BYTES, XU_GUID, XU_SELECTOR

UVC_GET_CUR = 0x81
UVC_GET_LEN = 0x85
UVC_CS_INTERFACE = 0x24
UVC_EXTENSION_UNIT = 0x06
XU_GUID_BYTES = UUID(XU_GUID).bytes_le


class _UvcXuControlQuery(ctypes.Structure):
    _fields_ = [
        ("unit", ctypes.c_uint8),
        ("selector", ctypes.c_uint8),
        ("query", ctypes.c_uint8),
        ("size", ctypes.c_uint16),
        ("data", ctypes.POINTER(ctypes.c_uint8)),
    ]


def _ioc(direction: int, kind: int, number: int, size: int) -> int:
    return (direction << 30) | (size << 16) | (kind << 8) | number


UVCIOC_CTRL_QUERY = _ioc(3, ord("u"), 0x21, ctypes.sizeof(_UvcXuControlQuery))

_LIBC = ctypes.CDLL(None, use_errno=True)
_LIBC.ioctl.argtypes = [ctypes.c_int, ctypes.c_ulong, ctypes.c_void_p]
_LIBC.ioctl.restype = ctypes.c_int

ControlQuery = Callable[[int, int, int, int, int], bytes]
Clock = Callable[[], int]
Sleeper = Callable[[float], None]
OpenFile = Callable[[str, int], int]
CloseFile = Callable[[int], None]
Guid = str | bytes | UUID
DiscoverUnit = Callable[[str | Path, Guid], int]


def _guid_bytes(guid: Guid) -> bytes:
    if isinstance(guid, UUID):
        value = guid.bytes_le
    elif isinstance(guid, str):
        try:
            value = UUID(guid).bytes_le
        except ValueError as exc:
            raise ValueError("UVC XU GUID must be a canonical UUID") from exc
    else:
        value = bytes(guid)
    if len(value) != 16:
        raise ValueError("UVC XU GUID must contain exactly 16 bytes")
    return value


def parse_uvc_extension_units(descriptors: bytes) -> tuple[tuple[int, bytes], ...]:
    """Extract ``(unit_id, guid_bytes)`` from a USB configuration descriptor."""

    units: list[tuple[int, bytes]] = []
    offset = 0
    while offset < len(descriptors):
        if len(descriptors) - offset < 2:
            raise ImuError("xu_descriptor_invalid", "USB descriptor has a truncated header")
        length = descriptors[offset]
        if length < 2 or offset + length > len(descriptors):
            raise ImuError("xu_descriptor_invalid", "USB descriptor has an invalid length")
        descriptor_type = descriptors[offset + 1]
        if (
            descriptor_type == UVC_CS_INTERFACE
            and length >= 20
            and descriptors[offset + 2] == UVC_EXTENSION_UNIT
        ):
            units.append((descriptors[offset + 3], bytes(descriptors[offset + 4 : offset + 20])))
        offset += length
    return tuple(units)


def find_uvc_xu_unit(descriptors: bytes, guid: Guid = XU_GUID) -> int:
    """Find the unique UVC extension-unit ID matching ``guid``."""

    target = _guid_bytes(guid)
    matches = [
        unit for unit, candidate in parse_uvc_extension_units(descriptors) if candidate == target
    ]
    if not matches:
        raise ImuError("xu_not_found", f"UVC XU GUID {guid} was not found")
    if len(matches) != 1:
        raise ImuError("xu_ambiguous", f"UVC XU GUID {guid} matched multiple units")
    return matches[0]


def discover_uvc_xu_unit(
    device: str | Path,
    guid: Guid = XU_GUID,
    *,
    sys_root: Path = Path("/sys"),
) -> int:
    """Discover an XU unit by GUID using the Linux USB descriptor sysfs file."""

    node = Path(device)
    class_entry = sys_root / "class" / "video4linux" / node.name / "device"
    try:
        resolved = class_entry.resolve(strict=True)
    except OSError as exc:
        raise ImuError(
            "xu_discovery_unavailable",
            f"无法定位视频节点的 USB sysfs 描述：{class_entry}",
        ) from exc
    for parent in (resolved, *resolved.parents):
        descriptor_path = parent / "descriptors"
        try:
            descriptors = descriptor_path.read_bytes()
        except OSError:
            continue
        try:
            return find_uvc_xu_unit(descriptors, guid)
        except ImuError as exc:
            if exc.code != "xu_not_found":
                raise
    raise ImuError("xu_not_found", f"UVC XU GUID {guid} was not found for {device}")


def _linux_control_query(fd: int, unit: int, selector: int, query: int, size: int) -> bytes:
    if size <= 0 or size > 0xFFFF:
        raise ValueError("UVC XU query size is out of range")
    buffer = (ctypes.c_uint8 * size)()
    control = _UvcXuControlQuery(
        unit=unit,
        selector=selector,
        query=query,
        size=size,
        data=ctypes.cast(buffer, ctypes.POINTER(ctypes.c_uint8)),
    )
    result = _LIBC.ioctl(fd, UVCIOC_CTRL_QUERY, ctypes.byref(control))
    if result < 0:
        error_number = ctypes.get_errno()
        raise OSError(error_number, os.strerror(error_number))
    return bytes(buffer)


class UvcXuImuSource:
    """Read fresh YLX IMU packets through Linux ``UVCIOC_CTRL_QUERY``."""

    def __init__(
        self,
        device: str | Path,
        *,
        unit: int | None = None,
        selector: int = XU_SELECTOR,
        xu_guid: Guid = XU_GUID,
        discover_unit: DiscoverUnit = discover_uvc_xu_unit,
        query_control: ControlQuery = _linux_control_query,
        clock_ns: Clock = time.monotonic_ns,
        sleep: Sleeper = time.sleep,
        stale_poll_interval: float = 0.001,
        open_file: OpenFile = os.open,
        close_file: CloseFile = os.close,
    ) -> None:
        if unit is None:
            unit = discover_unit(device, xu_guid)
        if not 1 <= unit <= 0xFF or not 1 <= selector <= 0xFF:
            raise ValueError("UVC XU unit and selector must fit in one non-zero byte")
        if stale_poll_interval < 0:
            raise ValueError("stale_poll_interval must not be negative")
        self._unit = unit
        self._selector = selector
        self._query_control = query_control
        self._clock_ns = clock_ns
        self._sleep = sleep
        self._stale_poll_interval = stale_poll_interval
        self._close_file = close_file
        self._last_device_timestamp: int | None = None
        self._closed = False
        flags = os.O_RDWR | getattr(os, "O_CLOEXEC", 0)
        self._fd = open_file(os.fspath(device), flags)
        try:
            declared = self._query_control(self._fd, unit, selector, UVC_GET_LEN, 2)
            if len(declared) != 2:
                raise ImuError("xu_query_failed", "UVC GET_LEN did not return two bytes")
            packet_bytes = int.from_bytes(declared, "little")
            if packet_bytes != PACKET_BYTES:
                raise ImuError(
                    "unsupported_packet_length",
                    f"YLX XU packet must be {PACKET_BYTES} bytes, device reports {packet_bytes}",
                )
        except BaseException:
            self._closed = True
            self._close_file(self._fd)
            raise

    @property
    def closed(self) -> bool:
        return self._closed

    @property
    def unit(self) -> int:
        return self._unit

    @property
    def unit_id(self) -> int:
        return self._unit

    def read_packet(self, timeout: float) -> ImuPacketRead:
        if timeout <= 0:
            raise ValueError("timeout must be greater than zero")
        if self._closed:
            raise ImuError("invalid_state", "IMU source is closed")
        deadline_ns = self._clock_ns() + int(timeout * 1_000_000_000)
        while True:
            host_read_start_ns = self._clock_ns()
            payload = self._query_control(
                self._fd,
                self._unit,
                self._selector,
                UVC_GET_CUR,
                PACKET_BYTES,
            )
            host_read_end_ns = self._clock_ns()
            if len(payload) != PACKET_BYTES:
                raise ImuError(
                    "invalid_packet_length",
                    f"UVC GET_CUR returned {len(payload)} bytes instead of {PACKET_BYTES}",
                )
            device_timestamp = int.from_bytes(payload[:3], "big")
            if device_timestamp != self._last_device_timestamp:
                self._last_device_timestamp = device_timestamp
                return ImuPacketRead(payload, host_read_start_ns, host_read_end_ns)
            if host_read_end_ns >= deadline_ns:
                raise TimeoutError("UVC XU IMU packet did not advance before timeout")
            remaining = (deadline_ns - host_read_end_ns) / 1_000_000_000
            self._sleep(min(self._stale_poll_interval, remaining))

    def close(self) -> None:
        if not self._closed:
            self._closed = True
            self._close_file(self._fd)

    def __enter__(self) -> UvcXuImuSource:
        return self

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        self.close()

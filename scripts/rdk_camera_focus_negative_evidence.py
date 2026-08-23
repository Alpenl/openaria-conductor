#!/usr/bin/env python3
"""Collect fail-closed camera-focus protocol evidence from the known YLX camera.

The collector is deliberately narrow. It admits one exact USB/UVC descriptor
identity, issues only UVC GET_INFO/GET_LEN requests for selectors advertised by
bmControls, and permits one hash-only GET_CUR when GET_INFO allows it and the
declared length is safe. It never stores a raw GET_CUR payload or a plaintext
USB/media serial.
"""

from __future__ import annotations

import argparse
import base64
import contextlib
import ctypes
import hashlib
import json
import os
import re
import signal
import stat
import subprocess
import sys
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Protocol
from urllib import error, request
from uuid import UUID

UTC = timezone.utc  # noqa: UP017 - the deployed RDK system Python may be 3.10.

SCHEMA = "ylx.camera-focus-negative-evidence.v1"
COLLECTOR_VERSION = "1.0.0"
ISSUE = "mirrorbloom/openaria-score#6"
DEFAULT_DEVICE = "/dev/video0"
DEFAULT_BASE_URL = "http://127.0.0.1:8080"
RP_YLX_BIN = "/opt/rp-ylx/current/bin/rp-ylx"
SERVICE_NAME = "rp-ylx"
MAX_HTTP_BYTES = 1024 * 1024
MAX_MEDIA_BYTES = 128 * 1024
MAX_CUR_BYTES = 256

UVC_GET_CUR = 0x81
UVC_GET_LEN = 0x85
UVC_GET_INFO = 0x86
UVC_CS_INTERFACE = 0x24
UVC_VC_HEADER = 0x01
UVC_EXTENSION_UNIT = 0x06
READ_ONLY_UVC_QUERIES = frozenset({UVC_GET_CUR, UVC_GET_LEN, UVC_GET_INFO})
QUERY_NAMES = {
    UVC_GET_CUR: "GET_CUR",
    UVC_GET_LEN: "GET_LEN",
    UVC_GET_INFO: "GET_INFO",
}
DENIED_CUR_SELECTORS = frozenset({(4, 10), (4, 15)})

EXPECTED_USB = {
    "idVendor": "1bcf",
    "idProduct": "0b15",
    "bcdDevice": "0701",
    "descriptor_length": 1656,
    "descriptor_sha256": "68aa58b949495ffe125f732bfe18e345c555ad3cd12e5ff40502562cca617a06",
    "driver": "uvcvideo",
}
EXPECTED_BCD_UVC = "0x0100"
EXPECTED_EXTENSION_UNITS = (
    {
        "unit_id": 3,
        "guid_raw_hex": "2cf4c2d508189f4dbe56753e271c9244",
        "uuid_canonical_bytes_le": "d5c2f42c-1808-4d9f-be56-753e271c9244",
        "bNumControls": 3,
        "bControlSize": 4,
        "bmControls_hex": "07000000",
        "selectors_from_bmControls": [1, 2, 3],
    },
    {
        "unit_id": 4,
        "guid_raw_hex": "820661637050ab49b8ccb3855e8d221d",
        "uuid_canonical_bytes_le": "63610682-5070-49ab-b8cc-b3855e8d221d",
        "bNumControls": 25,
        "bControlSize": 4,
        "bmControls_hex": "ffff7707",
        "selectors_from_bmControls": [
            1,
            2,
            3,
            4,
            5,
            6,
            7,
            8,
            9,
            10,
            11,
            12,
            13,
            14,
            15,
            16,
            17,
            18,
            19,
            21,
            22,
            23,
            25,
            26,
            27,
        ],
    },
)

VERSION_RE = re.compile(r"^rp-ylx\s+(\S+)\s+\(([0-9a-f]{40})\)\s*$")
VIDEO_NODE_RE = re.compile(r"^/dev/video[0-9]+$")
MEDIA_NODE_RE = re.compile(r"^/dev/media[0-9]+$")
MEDIA_ENTITY_RE = re.compile(r"^- entity\s+(\d+):\s*(.*?)\s+\(")
MEDIA_DEVICE_NODE_RE = re.compile(r"^device node name\s+(\S+)\s*$")
V4L2_CONTROL_RE = re.compile(r"^([A-Za-z0-9_]+)\s+(0x[0-9a-fA-F]{8})\s+\(([^)]+)\)\s*:\s*(.*)$")
V4L2_ATTRIBUTE_RE = re.compile(r"(?:^|\s)(min|max|step|default|value|flags)=([^\s]+)")


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


class EvidenceError(RuntimeError):
    """An expected fail-closed collector error with a safe public code."""

    def __init__(self, code: str, *, phase: str, details: dict[str, Any] | None = None) -> None:
        super().__init__(code)
        self.code = code
        self.phase = phase
        self.details = details or {}

    def reason(self) -> dict[str, Any]:
        return {"code": self.code, "phase": self.phase, **self.details}


class OutputUnavailable(EvidenceError):
    def __init__(self) -> None:
        super().__init__("output_unavailable", phase="output")


class WatchdogExpired(EvidenceError):
    def __init__(self) -> None:
        super().__init__("watchdog_expired", phase="watchdog")


@dataclass(frozen=True)
class CollectionConfig:
    output: Path
    expected_commit: str
    device: str = DEFAULT_DEVICE
    watchdog_seconds: float = 30.0


@dataclass(frozen=True)
class BindingToken:
    query_node: str
    query_node_rdev: int
    query_node_index: str
    video_device_target: str
    usb_device_target: str
    descriptor_sha256: str
    media_node: str
    media_node_rdev: int
    media_device_target: str

    def public(self) -> dict[str, Any]:
        return {
            "query_node": self.query_node,
            "query_node_rdev": self.query_node_rdev,
            "query_node_index": self.query_node_index,
            "video_device_identity_sha256": _hash_text(self.video_device_target),
            "usb_device_identity_sha256": _hash_text(self.usb_device_target),
            "descriptor_sha256": self.descriptor_sha256,
            "media_node": self.media_node,
            "media_node_rdev": self.media_node_rdev,
            "media_device_identity_sha256": _hash_text(self.media_device_target),
            "media_bound_to_same_usb_identity": True,
        }


@dataclass(frozen=True)
class IdentityObservation:
    public: dict[str, Any]
    binding: BindingToken
    private_values: tuple[str, ...] = ()


class Runtime(Protocol):
    def now(self) -> str: ...

    def health_snapshot(self) -> dict[str, Any]: ...

    def identity_snapshot(self, device: str) -> IdentityObservation: ...

    def binding_token(self, device: str, media_node: str) -> BindingToken: ...

    def open_query_node(self, device: str, binding: BindingToken) -> int: ...

    def close_query_node(self, fd: int) -> None: ...

    def query_get(
        self,
        fd: int,
        unit: int,
        selector: int,
        query: int,
        size: int,
    ) -> bytes: ...


def _utc_now() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def _hash_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _hash_text(value: str) -> str:
    return f"sha256:{_hash_bytes(value.encode('utf-8', 'replace'))}"


def _script_sha256() -> str | None:
    try:
        return _hash_bytes(Path(__file__).read_bytes())
    except OSError:
        return None


def _write_all(fd: int, payload: bytes) -> None:
    offset = 0
    while offset < len(payload):
        written = os.write(fd, payload[offset:])
        if written <= 0:
            raise OSError("short write while checkpointing evidence")
        offset += written


class AtomicCheckpoint:
    """Create once without replacement, then atomically replace complete checkpoints."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self._created = False
        self._sequence = 0

    @staticmethod
    def _encode(payload: dict[str, Any]) -> bytes:
        return (
            json.dumps(
                payload,
                ensure_ascii=True,
                allow_nan=False,
                indent=2,
                sort_keys=True,
            )
            + "\n"
        ).encode("utf-8")

    def _temporary_path(self) -> Path:
        self._sequence += 1
        return self.path.with_name(f".{self.path.name}.{os.getpid()}.{self._sequence}.tmp")

    def _write_temporary(self, payload: dict[str, Any]) -> Path:
        temporary = self._temporary_path()
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_CLOEXEC", 0)
        fd: int | None = None
        try:
            fd = os.open(temporary, flags, 0o600)
            _write_all(fd, self._encode(payload))
            os.fsync(fd)
        except BaseException:
            if fd is not None:
                os.close(fd)
            with contextlib.suppress(OSError):
                temporary.unlink()
            raise
        os.close(fd)
        return temporary

    def _fsync_parent(self) -> None:
        flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_CLOEXEC", 0)
        directory_fd = os.open(self.path.parent, flags)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)

    def create(self, payload: dict[str, Any]) -> None:
        if self._created:
            raise OutputUnavailable()
        temporary: Path | None = None
        try:
            temporary = self._write_temporary(payload)
            os.link(temporary, self.path)
            temporary.unlink()
            temporary = None
            self._fsync_parent()
        except OSError as exc:
            if temporary is not None:
                with contextlib.suppress(OSError):
                    temporary.unlink()
            raise OutputUnavailable() from exc
        self._created = True

    def update(self, payload: dict[str, Any]) -> None:
        if not self._created:
            raise OutputUnavailable()
        temporary: Path | None = None
        try:
            temporary = self._write_temporary(payload)
            os.replace(temporary, self.path)
            temporary = None
            self._fsync_parent()
        except OSError as exc:
            if temporary is not None:
                with contextlib.suppress(OSError):
                    temporary.unlink()
            raise OutputUnavailable() from exc


class ProcessWatchdog:
    def __init__(self, seconds: float) -> None:
        self.seconds = seconds
        self._old_handler: Any = None
        self._old_timer: tuple[float, float] | None = None

    def __enter__(self) -> ProcessWatchdog:
        if not hasattr(signal, "setitimer"):
            return self
        self._old_handler = signal.getsignal(signal.SIGALRM)
        self._old_timer = signal.getitimer(signal.ITIMER_REAL)
        signal.signal(signal.SIGALRM, self._expire)
        signal.setitimer(signal.ITIMER_REAL, self.seconds)
        return self

    def __exit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
        if not hasattr(signal, "setitimer"):
            return
        signal.setitimer(signal.ITIMER_REAL, 0.0)
        signal.signal(signal.SIGALRM, self._old_handler)
        if self._old_timer and self._old_timer[0] > 0:
            signal.setitimer(signal.ITIMER_REAL, *self._old_timer)

    @staticmethod
    def _expire(signum: int, frame: Any) -> None:
        del signum, frame
        raise WatchdogExpired()


def selectors_from_bm_controls(value: bytes) -> list[int]:
    selectors: list[int] = []
    for byte_index, control_byte in enumerate(value):
        for bit_index in range(8):
            if control_byte & (1 << bit_index):
                selectors.append(byte_index * 8 + bit_index + 1)
    return selectors


def parse_uvc_descriptors(data: bytes) -> dict[str, Any]:
    offset = 0
    bcd_uvc_values: list[str] = []
    extension_units: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []

    while offset < len(data):
        if len(data) - offset < 2:
            errors.append({"code": "truncated_descriptor_header", "offset": offset})
            break
        length = data[offset]
        descriptor_type = data[offset + 1]
        if length < 2 or offset + length > len(data):
            errors.append({"code": "invalid_descriptor_length", "offset": offset, "length": length})
            break
        descriptor = data[offset : offset + length]
        if descriptor_type == UVC_CS_INTERFACE and length >= 3:
            subtype = descriptor[2]
            if subtype == UVC_VC_HEADER:
                if length < 5:
                    errors.append({"code": "short_vc_header", "offset": offset})
                else:
                    value = int.from_bytes(descriptor[3:5], "little")
                    bcd_uvc_values.append(f"0x{value:04x}")
            elif subtype == UVC_EXTENSION_UNIT:
                try:
                    if length < 24:
                        raise ValueError("short_extension_unit")
                    unit_id = descriptor[3]
                    guid = bytes(descriptor[4:20])
                    number_of_controls = descriptor[20]
                    number_of_pins = descriptor[21]
                    control_size_offset = 22 + number_of_pins
                    if control_size_offset >= len(descriptor):
                        raise ValueError("missing_control_size")
                    control_size = descriptor[control_size_offset]
                    controls_start = control_size_offset + 1
                    controls_end = controls_start + control_size
                    if controls_end >= len(descriptor):
                        raise ValueError("truncated_bmcontrols_or_extension_index")
                    controls = bytes(descriptor[controls_start:controls_end])
                    extension_units.append(
                        {
                            "unit_id": unit_id,
                            "guid_raw_hex": guid.hex(),
                            "uuid_canonical_bytes_le": str(UUID(bytes_le=guid)),
                            "bNumControls": number_of_controls,
                            "bControlSize": control_size,
                            "bmControls_hex": controls.hex(),
                            "selectors_from_bmControls": selectors_from_bm_controls(controls),
                        }
                    )
                except (ValueError, IndexError) as exc:
                    errors.append(
                        {
                            "code": "invalid_extension_unit",
                            "offset": offset,
                            "reason": str(exc),
                        }
                    )
        offset += length

    unique_bcd_uvc = sorted(set(bcd_uvc_values))
    return {
        "bcdUVC": unique_bcd_uvc[0] if len(unique_bcd_uvc) == 1 else None,
        "bcdUVC_values": unique_bcd_uvc,
        "extension_units": sorted(extension_units, key=lambda item: item["unit_id"]),
        "parse_errors": errors,
    }


def parse_media_topology(text: str) -> tuple[dict[str, Any], tuple[str, ...]]:
    """Return selected structured fields and plaintexts that must not reach evidence."""

    information: dict[str, Any] = {}
    private_values: list[str] = []
    entities: list[dict[str, Any]] = []
    current_entity: dict[str, Any] | None = None
    in_information = False

    for raw_line in text.splitlines():
        line = raw_line.strip()
        if line == "Media device information":
            in_information = True
            continue
        if line == "Device topology":
            in_information = False
            continue
        if in_information and line and set(line) != {"-"}:
            key, separator, value = line.partition(" ")
            if separator:
                normalized_value = value.strip()
                if key == "driver":
                    if normalized_value.startswith("version "):
                        information["driver_version"] = normalized_value.removeprefix(
                            "version "
                        ).strip()
                    else:
                        information["driver"] = normalized_value
                elif key == "model":
                    if normalized_value:
                        private_values.append(normalized_value)
                        information["model_sha256"] = _hash_text(normalized_value)
                    else:
                        information["model_sha256"] = None
                elif key == "serial":
                    if normalized_value:
                        private_values.append(normalized_value)
                        information["serial_sha256"] = _hash_text(normalized_value)
                    else:
                        information["serial_sha256"] = None
                elif key == "bus":
                    bus_key, bus_separator, bus_value = normalized_value.partition(" ")
                    if bus_key == "info" and bus_separator:
                        bus_value = bus_value.strip()
                        private_values.append(bus_value)
                        information["bus_info_sha256"] = _hash_text(bus_value)
                elif key == "hw":
                    hw_key, hw_separator, hw_value = normalized_value.partition(" ")
                    if hw_key == "revision" and hw_separator:
                        information["hw_revision"] = hw_value.strip()

        entity_match = MEDIA_ENTITY_RE.match(line)
        if entity_match:
            current_entity = {
                "id": int(entity_match.group(1)),
                "name_sha256": _hash_text(entity_match.group(2)),
                "device_nodes": [],
            }
            entities.append(current_entity)
            continue
        node_match = MEDIA_DEVICE_NODE_RE.match(line)
        if node_match and current_entity is not None:
            current_entity["device_nodes"].append(node_match.group(1))

    device_nodes = sorted(
        {node for entity in entities for node in entity["device_nodes"] if isinstance(node, str)}
    )
    return (
        {
            "information": information,
            "entities": entities,
            "device_nodes": device_nodes,
            "raw_output_stored": False,
        },
        tuple(value for value in private_values if value),
    )


def parse_v4l2_controls(text: str) -> dict[str, Any]:
    section: str | None = None
    controls: list[dict[str, Any]] = []
    unparsed_nonempty_lines = 0

    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        if line.endswith(" Controls") and "=" not in line:
            section = line
            continue
        match = V4L2_CONTROL_RE.fullmatch(line)
        if match is None:
            unparsed_nonempty_lines += 1
            continue
        attributes = {
            attribute.group(1): attribute.group(2)
            for attribute in V4L2_ATTRIBUTE_RE.finditer(match.group(4))
        }
        controls.append(
            {
                "section": section,
                "name": match.group(1),
                "id": match.group(2).lower(),
                "type": match.group(3),
                "attributes": attributes,
            }
        )

    return {
        "controls": controls,
        "control_count": len(controls),
        "unparsed_nonempty_lines": unparsed_nonempty_lines,
        "raw_output_stored": False,
    }


@dataclass(frozen=True)
class _CommandResult:
    returncode: int | None
    stdout: str
    error_code: str | None


class SystemRuntime:
    def __init__(
        self,
        *,
        sys_root: Path = Path("/sys"),
        dev_root: Path = Path("/dev"),
        base_url: str = DEFAULT_BASE_URL,
    ) -> None:
        self.sys_root = sys_root
        self.dev_root = dev_root
        self.base_url = base_url.rstrip("/")

    def now(self) -> str:
        return _utc_now()

    @staticmethod
    def _command_allowed(args: list[str]) -> bool:
        if args[:3] == ["systemctl", "show", SERVICE_NAME]:
            return all(item.startswith("--property=") for item in args[3:])
        if args == [RP_YLX_BIN, "--version"]:
            return True
        if (
            len(args) == 4
            and args[:3] == ["v4l2-ctl", "--list-ctrls", "-d"]
            and VIDEO_NODE_RE.fullmatch(args[3]) is not None
        ):
            return True
        return (
            len(args) == 4
            and args[:2] == ["media-ctl", "-p"]
            and args[2] == "-d"
            and MEDIA_NODE_RE.fullmatch(args[3]) is not None
        )

    def _run_read_only(self, args: list[str], *, timeout: float) -> _CommandResult:
        if not self._command_allowed(args):
            raise EvidenceError("command_not_allowed", phase="preflight")
        try:
            completed = subprocess.run(
                args,
                check=False,
                capture_output=True,
                text=True,
                timeout=timeout,
            )
        except FileNotFoundError:
            return _CommandResult(None, "", "command_not_found")
        except subprocess.TimeoutExpired:
            return _CommandResult(None, "", "command_timeout")
        except OSError:
            return _CommandResult(None, "", "command_os_error")
        stdout = completed.stdout
        if len(stdout.encode("utf-8", "replace")) > MAX_MEDIA_BYTES:
            return _CommandResult(completed.returncode, "", "command_output_too_large")
        return _CommandResult(completed.returncode, stdout, None)

    def _capture_status(self) -> dict[str, Any]:
        probe = request.Request(
            f"{self.base_url}/api/v4/capture/status",
            headers={"Accept": "application/json"},
            method="GET",
        )
        status: int | None = None
        body: bytes = b""
        error_code: str | None = None
        try:
            with request.urlopen(probe, timeout=3.0) as response:
                status = response.status
                body = response.read(MAX_HTTP_BYTES + 1)
        except error.HTTPError as exc:
            status = exc.code
            body = exc.read(MAX_HTTP_BYTES + 1)
        except TimeoutError:
            error_code = "http_timeout"
        except OSError:
            error_code = "http_os_error"

        payload: Any = None
        if len(body) > MAX_HTTP_BYTES:
            error_code = "http_body_too_large"
        elif body:
            try:
                payload = json.loads(body)
            except (json.JSONDecodeError, UnicodeDecodeError):
                error_code = "http_body_invalid_json"

        snapshot = payload.get("snapshot") if isinstance(payload, dict) else None
        active_recording = (
            snapshot.get("active_recording") if isinstance(snapshot, dict) else "unavailable"
        )
        return {
            "http_status": status,
            "error_code": error_code,
            "schema": payload.get("schema") if isinstance(payload, dict) else None,
            "snapshot_schema": snapshot.get("schema") if isinstance(snapshot, dict) else None,
            "device_state": snapshot.get("device_state") if isinstance(snapshot, dict) else None,
            "active_recording_present": active_recording is not None,
            "body_stored": False,
        }

    def _service_status(self) -> dict[str, Any]:
        fields = (
            "ActiveState",
            "SubState",
            "MainPID",
            "NRestarts",
            "Result",
            "ExecMainStatus",
        )
        result = self._run_read_only(
            ["systemctl", "show", SERVICE_NAME, *[f"--property={field}" for field in fields]],
            timeout=5.0,
        )
        parsed: dict[str, str] = {}
        for line in result.stdout.splitlines():
            key, separator, value = line.partition("=")
            if separator and key in fields:
                parsed[key] = value
        return {
            "command_returncode": result.returncode,
            "error_code": result.error_code,
            "active_state": parsed.get("ActiveState"),
            "sub_state": parsed.get("SubState"),
            "main_pid": parsed.get("MainPID"),
            "restart_count": parsed.get("NRestarts"),
            "result": parsed.get("Result"),
            "exec_main_status": parsed.get("ExecMainStatus"),
        }

    def _version(self) -> dict[str, Any]:
        result = self._run_read_only([RP_YLX_BIN, "--version"], timeout=5.0)
        match = VERSION_RE.fullmatch(result.stdout)
        return {
            "command_returncode": result.returncode,
            "error_code": result.error_code,
            "format_valid": match is not None,
            "version": match.group(1) if match else None,
            "commit": match.group(2) if match else None,
            "raw_output_stored": False,
        }

    def health_snapshot(self) -> dict[str, Any]:
        return {
            "service": self._service_status(),
            "capture": self._capture_status(),
            "version": self._version(),
        }

    def _video_class_entry(self, device: str) -> Path:
        if VIDEO_NODE_RE.fullmatch(device) is None:
            raise EvidenceError("invalid_video_node", phase="identity")
        return self.sys_root / "class" / "video4linux" / Path(device).name

    @staticmethod
    def _find_usb_device(device_target: Path) -> Path:
        for candidate in (device_target, *device_target.parents):
            if all(
                (candidate / name).is_file()
                for name in ("idVendor", "idProduct", "bcdDevice", "descriptors")
            ):
                return candidate
        raise EvidenceError("usb_identity_not_found", phase="identity")

    @staticmethod
    def _read_text(path: Path) -> str:
        try:
            return path.read_text(encoding="utf-8").strip()
        except OSError as exc:
            raise EvidenceError("identity_file_unavailable", phase="identity") from exc

    @staticmethod
    def _rdev(path: Path) -> int:
        try:
            metadata = path.stat()
        except OSError as exc:
            raise EvidenceError("device_node_unavailable", phase="identity") from exc
        if not stat.S_ISCHR(metadata.st_mode):
            raise EvidenceError("device_node_not_character_device", phase="identity")
        return metadata.st_rdev

    def _media_entries_for_usb(self, usb_target: Path) -> list[tuple[str, Path]]:
        entries: list[tuple[str, Path]] = []
        for class_entry in sorted((self.sys_root / "class" / "media").glob("media*")):
            try:
                target = (class_entry / "device").resolve(strict=True)
                candidate_usb = self._find_usb_device(target)
            except (OSError, EvidenceError):
                continue
            if candidate_usb == usb_target:
                entries.append((class_entry.name, target))
        return entries

    def _binding_core(self, device: str, media_node: str | None = None) -> BindingToken:
        class_entry = self._video_class_entry(device)
        try:
            video_target = (class_entry / "device").resolve(strict=True)
        except OSError as exc:
            raise EvidenceError("video_sysfs_unavailable", phase="identity") from exc
        usb_target = self._find_usb_device(video_target)
        try:
            descriptors = (usb_target / "descriptors").read_bytes()
        except OSError as exc:
            raise EvidenceError("descriptor_unavailable", phase="identity") from exc

        media_entries = self._media_entries_for_usb(usb_target)
        if media_node is None:
            if len(media_entries) != 1:
                raise EvidenceError(
                    "media_identity_ambiguous",
                    phase="identity",
                    details={"matching_media_nodes": len(media_entries)},
                )
            media_name, media_target = media_entries[0]
            selected_media_node = f"/dev/{media_name}"
        else:
            matches = [item for item in media_entries if f"/dev/{item[0]}" == media_node]
            if len(matches) != 1:
                raise EvidenceError("media_identity_changed", phase="identity")
            media_name, media_target = matches[0]
            selected_media_node = media_node

        return BindingToken(
            query_node=device,
            query_node_rdev=self._rdev(Path(device)),
            query_node_index=self._read_text(class_entry / "index"),
            video_device_target=str(video_target),
            usb_device_target=str(usb_target),
            descriptor_sha256=_hash_bytes(descriptors),
            media_node=selected_media_node,
            media_node_rdev=self._rdev(self.dev_root / media_name),
            media_device_target=str(media_target),
        )

    def binding_token(self, device: str, media_node: str) -> BindingToken:
        return self._binding_core(device, media_node)

    def _bound_video_nodes(self, usb_target: Path) -> list[dict[str, Any]]:
        nodes: list[dict[str, Any]] = []
        root = self.sys_root / "class" / "video4linux"
        for class_entry in sorted(root.glob("video*")):
            try:
                video_target = (class_entry / "device").resolve(strict=True)
                candidate_usb = self._find_usb_device(video_target)
            except (OSError, EvidenceError):
                continue
            if candidate_usb != usb_target:
                continue
            nodes.append(
                {
                    "node": f"/dev/{class_entry.name}",
                    "index": self._read_text(class_entry / "index"),
                    "name_sha256": _hash_text(self._read_text(class_entry / "name")),
                    "device_identity_sha256": _hash_text(str(video_target)),
                }
            )
        return nodes

    def _v4l2_inventory(self, nodes: list[dict[str, Any]]) -> dict[str, Any]:
        inventory: dict[str, Any] = {}
        complete = True
        for node in nodes:
            node_path = str(node["node"])
            result = self._run_read_only(
                ["v4l2-ctl", "--list-ctrls", "-d", node_path],
                timeout=5.0,
            )
            parsed = (
                parse_v4l2_controls(result.stdout)
                if result.returncode == 0
                else {
                    "controls": [],
                    "control_count": 0,
                    "unparsed_nonempty_lines": 0,
                    "raw_output_stored": False,
                }
            )
            parsed["command_returncode"] = result.returncode
            parsed["error_code"] = result.error_code
            inventory[node_path] = parsed
            if result.returncode != 0 or result.error_code is not None:
                complete = False
        return {
            "nodes": inventory,
            "enumeration_complete": complete and bool(nodes),
            "raw_output_stored": False,
        }

    def identity_snapshot(self, device: str) -> IdentityObservation:
        binding = self._binding_core(device)
        usb_target = Path(binding.usb_device_target)
        try:
            descriptors = (usb_target / "descriptors").read_bytes()
        except OSError as exc:
            raise EvidenceError("descriptor_unavailable", phase="identity") from exc

        serial = self._read_text(usb_target / "serial")
        manufacturer = self._read_text(usb_target / "manufacturer")
        product = self._read_text(usb_target / "product")
        driver: str | None = None
        with contextlib.suppress(OSError):
            driver = (
                (self._video_class_entry(device) / "device" / "driver").resolve(strict=True).name
            )

        media_result = self._run_read_only(
            ["media-ctl", "-p", "-d", binding.media_node],
            timeout=5.0,
        )
        if media_result.returncode == 0 and media_result.error_code is None:
            media, media_private = parse_media_topology(media_result.stdout)
        else:
            media = {
                "information": {},
                "entities": [],
                "device_nodes": [],
                "raw_output_stored": False,
            }
            media_private = ()
        media["node"] = binding.media_node
        media["command_returncode"] = media_result.returncode
        media["error_code"] = media_result.error_code
        bound_video_nodes = self._bound_video_nodes(usb_target)

        public = {
            "usb": {
                "idVendor": self._read_text(usb_target / "idVendor").lower(),
                "idProduct": self._read_text(usb_target / "idProduct").lower(),
                "bcdDevice": self._read_text(usb_target / "bcdDevice").lower(),
                "descriptor_length": len(descriptors),
                "descriptor_sha256": _hash_bytes(descriptors),
                "driver": driver,
                "manufacturer_sha256": _hash_text(manufacturer) if manufacturer else None,
                "product_sha256": _hash_text(product) if product else None,
                "serial_sha256": _hash_text(serial) if serial else None,
            },
            "uvc": parse_uvc_descriptors(descriptors),
            "binding": {
                **binding.public(),
                "bound_video_nodes": bound_video_nodes,
            },
            "media": media,
            "v4l2": self._v4l2_inventory(bound_video_nodes),
        }
        return IdentityObservation(
            public=public,
            binding=binding,
            private_values=tuple(
                value for value in (serial, manufacturer, product, *media_private) if value
            ),
        )

    def open_query_node(self, device: str, binding: BindingToken) -> int:
        try:
            fd = os.open(device, os.O_RDWR | getattr(os, "O_CLOEXEC", 0))
        except OSError as exc:
            raise EvidenceError("query_node_open_failed", phase="query_open") from exc
        try:
            metadata = os.fstat(fd)
            if not stat.S_ISCHR(metadata.st_mode) or metadata.st_rdev != binding.query_node_rdev:
                raise EvidenceError("query_node_changed_during_open", phase="query_open")
        except BaseException:
            os.close(fd)
            raise
        return fd

    def close_query_node(self, fd: int) -> None:
        os.close(fd)

    def query_get(
        self,
        fd: int,
        unit: int,
        selector: int,
        query: int,
        size: int,
    ) -> bytes:
        if query not in READ_ONLY_UVC_QUERIES or not query & 0x80:
            raise EvidenceError("uvc_query_not_allowed", phase="query")
        if not 1 <= unit <= 0xFF or not 1 <= selector <= 0xFF:
            raise EvidenceError("uvc_address_invalid", phase="query")
        if size <= 0 or size > 0xFFFF:
            raise EvidenceError("uvc_query_size_invalid", phase="query")
        if query == UVC_GET_INFO and size != 1:
            raise EvidenceError("get_info_size_invalid", phase="query")
        if query == UVC_GET_LEN and size != 2:
            raise EvidenceError("get_len_size_invalid", phase="query")
        if query == UVC_GET_CUR:
            if (unit, selector) in DENIED_CUR_SELECTORS:
                raise EvidenceError("get_cur_selector_denied", phase="query")
            if size > MAX_CUR_BYTES:
                raise EvidenceError("get_cur_size_denied", phase="query")

        buffer = (ctypes.c_uint8 * size)()
        ctypes.memset(ctypes.addressof(buffer), 0, size)
        control = _UvcXuControlQuery(
            unit=unit,
            selector=selector,
            query=query,
            size=size,
            data=ctypes.cast(buffer, ctypes.POINTER(ctypes.c_uint8)),
        )
        ctypes.set_errno(0)
        result = _LIBC.ioctl(fd, UVCIOC_CTRL_QUERY, ctypes.byref(control))
        if result != 0:
            error_number = ctypes.get_errno() if result < 0 else None
            raise EvidenceError(
                "uvc_ioctl_nonzero",
                phase="query",
                details={
                    "ioctl_result": result,
                    "errno": error_number,
                },
            )
        return bytes(buffer)


def _expected_identity() -> dict[str, Any]:
    return {
        "usb": dict(EXPECTED_USB),
        "bcdUVC": EXPECTED_BCD_UVC,
        "extension_units": [dict(unit) for unit in EXPECTED_EXTENSION_UNITS],
        "query_node_index": "0",
        "media_driver": "uvcvideo",
    }


def deployment_fingerprint(
    *,
    health: dict[str, Any],
    identity: dict[str, Any],
) -> dict[str, Any]:
    version = health.get("version") if isinstance(health.get("version"), dict) else {}
    usb = identity.get("usb") if isinstance(identity.get("usb"), dict) else {}
    uvc = identity.get("uvc") if isinstance(identity.get("uvc"), dict) else {}
    canonical = {
        "deployed_commit": version.get("commit"),
        "usb": {key: usb.get(key) for key in EXPECTED_USB},
        "bcdUVC": uvc.get("bcdUVC"),
        "extension_units": uvc.get("extension_units"),
    }
    encoded = json.dumps(
        canonical,
        ensure_ascii=True,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return {
        "algorithm": "sha256",
        "deployed_commit": version.get("commit"),
        "descriptor_sha256": usb.get("descriptor_sha256"),
        "canonical_identity_sha256": _hash_bytes(encoded),
        "canonical_fields": [
            "deployed_commit",
            "idVendor",
            "idProduct",
            "bcdDevice",
            "descriptor_length",
            "descriptor_sha256",
            "driver",
            "bcdUVC",
            "extension_units",
        ],
    }


def _check(name: str, expected: Any, actual: Any) -> dict[str, Any]:
    return {"name": name, "expected": expected, "actual": actual, "passed": actual == expected}


def _query_scope() -> dict[str, Any]:
    return {
        "source": "exact_descriptor_bmControls",
        "allowed_query_names": ["GET_INFO", "GET_LEN", "GET_CUR"],
        "attempts_per_query": 1,
        "automatic_retries": 0,
        "cur_max_bytes": MAX_CUR_BYTES,
        "cur_requires_get_info_support": True,
        "cur_payload_storage": "length_and_sha256_only",
        "denied_cur_selectors": [list(item) for item in sorted(DENIED_CUR_SELECTORS)],
        "get_min_max_res_def_allowed": False,
    }


def evaluate_health_admission(
    *,
    health: dict[str, Any],
    expected_commit: str,
) -> dict[str, Any]:
    service = health.get("service") if isinstance(health.get("service"), dict) else {}
    capture = health.get("capture") if isinstance(health.get("capture"), dict) else {}
    version = health.get("version") if isinstance(health.get("version"), dict) else {}
    checks = [
        _check("service.command_returncode", 0, service.get("command_returncode")),
        _check("service.active_state", "active", service.get("active_state")),
        _check("service.sub_state", "running", service.get("sub_state")),
        _check("capture.http_status", 200, capture.get("http_status")),
        _check("capture.device_state", "idle", capture.get("device_state")),
        _check(
            "capture.active_recording_present",
            False,
            capture.get("active_recording_present"),
        ),
        _check("version.command_returncode", 0, version.get("command_returncode")),
        _check("version.format_valid", True, version.get("format_valid")),
        _check("version.commit", expected_commit, version.get("commit")),
    ]
    failed_checks = [item["name"] for item in checks if not item["passed"]]
    return {
        "admitted": not failed_checks,
        "checks": checks,
        "failed_checks": failed_checks,
        "deployed_fingerprint": None,
        "query_scope": _query_scope(),
    }


def evaluate_protocol_admission(
    *,
    health: dict[str, Any],
    identity: dict[str, Any],
    expected_commit: str,
    device: str,
) -> dict[str, Any]:
    usb = identity.get("usb") if isinstance(identity.get("usb"), dict) else {}
    uvc = identity.get("uvc") if isinstance(identity.get("uvc"), dict) else {}
    binding = identity.get("binding") if isinstance(identity.get("binding"), dict) else {}
    media = identity.get("media") if isinstance(identity.get("media"), dict) else {}
    media_information = (
        media.get("information") if isinstance(media.get("information"), dict) else {}
    )
    v4l2 = identity.get("v4l2") if isinstance(identity.get("v4l2"), dict) else {}
    v4l2_nodes = v4l2.get("nodes") if isinstance(v4l2.get("nodes"), dict) else {}

    checks = list(
        evaluate_health_admission(
            health=health,
            expected_commit=expected_commit,
        )["checks"]
    )
    checks.extend(_check(f"usb.{key}", value, usb.get(key)) for key, value in EXPECTED_USB.items())
    checks.extend(
        [
            _check("uvc.parse_errors", [], uvc.get("parse_errors")),
            _check("uvc.bcdUVC", EXPECTED_BCD_UVC, uvc.get("bcdUVC")),
            _check(
                "uvc.extension_units",
                [dict(unit) for unit in EXPECTED_EXTENSION_UNITS],
                uvc.get("extension_units"),
            ),
            _check("binding.query_node", device, binding.get("query_node")),
            _check("binding.query_node_index", "0", binding.get("query_node_index")),
            _check(
                "binding.descriptor_sha256",
                EXPECTED_USB["descriptor_sha256"],
                binding.get("descriptor_sha256"),
            ),
            _check(
                "binding.media_bound_to_same_usb_identity",
                True,
                binding.get("media_bound_to_same_usb_identity"),
            ),
            _check("media.command_returncode", 0, media.get("command_returncode")),
            _check("media.error_code", None, media.get("error_code")),
            _check("media.driver", "uvcvideo", media_information.get("driver")),
            _check(
                "media.contains_query_node",
                True,
                device in media.get("device_nodes", []),
            ),
            _check("v4l2.enumeration_complete", True, v4l2.get("enumeration_complete")),
            _check("v4l2.contains_query_node", True, device in v4l2_nodes),
        ]
    )
    failed_checks = [item["name"] for item in checks if not item["passed"]]
    return {
        "admitted": not failed_checks,
        "checks": checks,
        "failed_checks": failed_checks,
        "deployed_fingerprint": deployment_fingerprint(health=health, identity=identity),
        "query_scope": _query_scope(),
    }


def _admission_stop_reason(admission: dict[str, Any]) -> dict[str, Any]:
    failures = set(admission["failed_checks"])
    if any(name.startswith("capture.") for name in failures):
        code = "capture_not_idle"
    elif any(name.startswith("service.") for name in failures):
        code = "rp_ylx_not_active"
    elif "version.commit" in failures:
        code = "expected_commit_mismatch"
    elif any(name.startswith("version.") for name in failures):
        code = "deployed_version_unavailable"
    else:
        code = "exact_identity_mismatch"
    return {"code": code, "phase": "protocol_admission", "failed_checks": sorted(failures)}


def _binding_changes(expected: BindingToken, actual: BindingToken) -> list[str]:
    expected_fields = asdict(expected)
    actual_fields = asdict(actual)
    return sorted(
        field
        for field, expected_value in expected_fields.items()
        if actual_fields[field] != expected_value
    )


def _privacy_audit(
    report: dict[str, Any],
    *,
    private_values: list[str],
    cur_payloads: list[bytes],
) -> dict[str, Any]:
    report_without_audit = {key: value for key, value in report.items() if key != "privacy_audit"}
    encoded = json.dumps(report_without_audit, ensure_ascii=True, sort_keys=True)
    serial_plaintext_absent = all(value not in encoded for value in private_values if value)

    forbidden_cur_encoding_absent = True
    for payload in cur_payloads:
        if len(payload) < 4:
            continue
        candidates = {payload.hex(), base64.b64encode(payload).decode("ascii")}
        try:
            decoded = payload.decode("utf-8")
        except UnicodeDecodeError:
            decoded = ""
        if decoded and all(character.isprintable() for character in decoded):
            candidates.add(decoded)
        if any(candidate and candidate in encoded for candidate in candidates):
            forbidden_cur_encoding_absent = False
            break

    structural_keys_absent = True
    stack: list[Any] = [report_without_audit]
    while stack:
        value = stack.pop()
        if isinstance(value, dict):
            if any(
                key in value for key in ("raw_cur", "cur_payload", "stdout", "stderr", "serial")
            ):
                structural_keys_absent = False
            stack.extend(value.values())
        elif isinstance(value, list):
            stack.extend(value)

    checks = {
        "serial_plaintext_absent": serial_plaintext_absent,
        "raw_cur_encodings_absent": forbidden_cur_encoding_absent,
        "forbidden_storage_keys_absent": structural_keys_absent,
        "media_raw_output_absent": True,
    }
    return {
        "passed": all(checks.values()),
        "checks": checks,
        "serial_storage": "sha256_only",
        "cur_storage": "length_and_sha256_only",
        "media_storage": "structured_redacted_fields_only",
        "v4l2_storage": "structured_control_fields_only",
        "usb_descriptor_storage": "sha256_and_admission_fields_only",
        "http_body_storage": "selected_health_fields_only",
        "public_protocol_identity_fields": [
            "idVendor",
            "idProduct",
            "bcdDevice",
            "bcdUVC",
            "descriptor_sha256",
            "extension_unit_guid",
            "bmControls",
        ],
        "sensitive_identity_storage": "serial_bus_and_sysfs_values_sha256_only",
    }


def _checkpoint(
    writer: AtomicCheckpoint,
    report: dict[str, Any],
    *,
    private_values: list[str],
    cur_payloads: list[bytes],
) -> None:
    report["checkpointing"]["completed_count"] += 1
    report["checkpointing"]["last_status"] = report["status"]
    audit = _privacy_audit(report, private_values=private_values, cur_payloads=cur_payloads)
    if not audit["passed"]:
        raise EvidenceError("privacy_audit_failed", phase="privacy")
    report["privacy_audit"] = audit
    writer.update(report)


def _mark_stopped(report: dict[str, Any], reason: dict[str, Any]) -> None:
    if report.get("stop_reason") is None:
        report["stop_reason"] = reason
    report["status"] = "stopped"


def _safe_exception_reason(exc: BaseException, *, phase: str) -> dict[str, Any]:
    if isinstance(exc, EvidenceError):
        return exc.reason()
    return {
        "code": "collector_exception",
        "phase": phase,
        "exception_type": type(exc).__name__,
    }


def _execute_query(
    *,
    runtime: Runtime,
    writer: AtomicCheckpoint,
    report: dict[str, Any],
    baseline: BindingToken,
    fd: int,
    unit: int,
    selector: int,
    query: int,
    size: int,
    expected_response_bytes: int,
    private_values: list[str],
    cur_payloads: list[bytes],
) -> bytes:
    query_name = QUERY_NAMES[query]
    try:
        current = runtime.binding_token(baseline.query_node, baseline.media_node)
    except BaseException as exc:
        raise EvidenceError(
            "identity_recheck_failed",
            phase="query_gate",
            details={"query": query_name, "unit": unit, "selector": selector},
        ) from exc
    changes = _binding_changes(baseline, current)
    if changes:
        raise EvidenceError(
            "node_or_descriptor_changed",
            phase="query_gate",
            details={
                "query": query_name,
                "unit": unit,
                "selector": selector,
                "changed_fields": changes,
            },
        )

    counters = report["query_execution"]
    attempt = {
        "ordinal": counters["executed_count"] + 1,
        "query": query_name,
        "unit": unit,
        "selector": selector,
        "request_bytes": size,
    }
    counters["executed_count"] += 1
    counters["by_query"][query_name]["executed"] += 1
    counters["in_progress"] = attempt
    _checkpoint(
        writer,
        report,
        private_values=private_values,
        cur_payloads=cur_payloads,
    )

    try:
        payload = runtime.query_get(fd, unit, selector, query, size)
    except WatchdogExpired:
        raise
    except BaseException as exc:
        counters["failed_count"] += 1
        counters["by_query"][query_name]["failed"] += 1
        counters["in_progress"] = None
        attempt["outcome"] = "failed"
        attempt["error_code"] = (
            exc.code if isinstance(exc, EvidenceError) else "uvc_query_exception"
        )
        counters["attempts"].append(attempt)
        _checkpoint(
            writer,
            report,
            private_values=private_values,
            cur_payloads=cur_payloads,
        )
        raise EvidenceError(
            "uvc_query_failed",
            phase="query",
            details={"query": query_name, "unit": unit, "selector": selector},
        ) from exc

    if len(payload) != expected_response_bytes:
        counters["failed_count"] += 1
        counters["by_query"][query_name]["failed"] += 1
        counters["in_progress"] = None
        attempt.update(
            {
                "outcome": "invalid_response_length",
                "response_bytes": len(payload),
                "expected_response_bytes": expected_response_bytes,
            }
        )
        counters["attempts"].append(attempt)
        _checkpoint(
            writer,
            report,
            private_values=private_values,
            cur_payloads=cur_payloads,
        )
        raise EvidenceError(
            "uvc_response_length_mismatch",
            phase="query",
            details={"query": query_name, "unit": unit, "selector": selector},
        )

    counters["successful_count"] += 1
    counters["by_query"][query_name]["successful"] += 1
    counters["in_progress"] = None
    attempt.update({"outcome": "success", "response_bytes": len(payload)})
    counters["attempts"].append(attempt)
    _checkpoint(
        writer,
        report,
        private_values=private_values,
        cur_payloads=cur_payloads,
    )
    return payload


def _collect_queries(
    *,
    config: CollectionConfig,
    runtime: Runtime,
    writer: AtomicCheckpoint,
    report: dict[str, Any],
    identity: IdentityObservation,
    private_values: list[str],
    cur_payloads: list[bytes],
) -> None:
    current = runtime.binding_token(config.device, identity.binding.media_node)
    changes = _binding_changes(identity.binding, current)
    if changes:
        raise EvidenceError(
            "node_or_descriptor_changed",
            phase="query_open_gate",
            details={"changed_fields": changes},
        )

    fd = runtime.open_query_node(config.device, identity.binding)
    try:
        report["status"] = "querying"
        _checkpoint(
            writer,
            report,
            private_values=private_values,
            cur_payloads=cur_payloads,
        )
        units = identity.public["uvc"]["extension_units"]
        for unit_descriptor in units:
            unit = int(unit_descriptor["unit_id"])
            for selector_value in unit_descriptor["selectors_from_bmControls"]:
                selector = int(selector_value)
                entry: dict[str, Any] = {
                    "unit": unit,
                    "selector": selector,
                    "advertised_by_bmControls": True,
                    "get_info": None,
                    "get_len": None,
                    "get_cur": None,
                }
                report["selectors"].append(entry)
                _checkpoint(
                    writer,
                    report,
                    private_values=private_values,
                    cur_payloads=cur_payloads,
                )

                info_payload = _execute_query(
                    runtime=runtime,
                    writer=writer,
                    report=report,
                    baseline=identity.binding,
                    fd=fd,
                    unit=unit,
                    selector=selector,
                    query=UVC_GET_INFO,
                    size=1,
                    expected_response_bytes=1,
                    private_values=private_values,
                    cur_payloads=cur_payloads,
                )
                info = info_payload[0]
                entry["get_info"] = {
                    "value": info,
                    "value_hex": f"0x{info:02x}",
                    "get_supported": bool(info & 0x01),
                    "write_support_advertised": bool(info & 0x02),
                    "auto_update_advertised": bool(info & 0x08),
                }
                del info_payload
                _checkpoint(
                    writer,
                    report,
                    private_values=private_values,
                    cur_payloads=cur_payloads,
                )

                length_payload = _execute_query(
                    runtime=runtime,
                    writer=writer,
                    report=report,
                    baseline=identity.binding,
                    fd=fd,
                    unit=unit,
                    selector=selector,
                    query=UVC_GET_LEN,
                    size=2,
                    expected_response_bytes=2,
                    private_values=private_values,
                    cur_payloads=cur_payloads,
                )
                declared_length = int.from_bytes(length_payload, "little")
                entry["get_len"] = {"declared_bytes": declared_length}
                del length_payload

                if (unit, selector) in DENIED_CUR_SELECTORS:
                    policy = "skipped_denylist"
                elif not entry["get_info"]["get_supported"]:
                    policy = "skipped_get_not_supported"
                elif declared_length < 1:
                    policy = "skipped_invalid_declared_length"
                elif declared_length > MAX_CUR_BYTES:
                    policy = "skipped_declared_length_over_256"
                else:
                    policy = "admitted_hash_only"

                entry["get_cur"] = {
                    "policy": policy,
                    "attempted": False,
                    "raw_payload_stored": False,
                }
                _checkpoint(
                    writer,
                    report,
                    private_values=private_values,
                    cur_payloads=cur_payloads,
                )
                if policy != "admitted_hash_only":
                    continue

                payload = _execute_query(
                    runtime=runtime,
                    writer=writer,
                    report=report,
                    baseline=identity.binding,
                    fd=fd,
                    unit=unit,
                    selector=selector,
                    query=UVC_GET_CUR,
                    size=declared_length,
                    expected_response_bytes=declared_length,
                    private_values=private_values,
                    cur_payloads=cur_payloads,
                )
                cur_payloads.append(payload)
                entry["get_cur"] = {
                    "policy": policy,
                    "attempted": True,
                    "length": len(payload),
                    "sha256": _hash_bytes(payload),
                    "raw_payload_stored": False,
                }
                del payload
                _checkpoint(
                    writer,
                    report,
                    private_values=private_values,
                    cur_payloads=cur_payloads,
                )
    finally:
        runtime.close_query_node(fd)


def _post_checks(
    *,
    pre_binding: BindingToken | None,
    pre_health: dict[str, Any] | None,
    post_identity: IdentityObservation | None,
    post_health: dict[str, Any] | None,
    expected_commit: str,
) -> list[dict[str, Any]]:
    checks: list[dict[str, Any]] = []
    if pre_binding is None or post_identity is None:
        checks.append(_check("post.identity_available", True, False))
    else:
        checks.append(
            _check("post.identity_stable", [], _binding_changes(pre_binding, post_identity.binding))
        )

    if post_health is None:
        checks.append(_check("post.health_available", True, False))
        return checks
    service = post_health.get("service", {})
    capture = post_health.get("capture", {})
    version = post_health.get("version", {})
    pre_service = pre_health.get("service", {}) if pre_health is not None else {}
    checks.extend(
        [
            _check("post.service_active", "active", service.get("active_state")),
            _check("post.service_running", "running", service.get("sub_state")),
            _check(
                "post.service_main_pid_stable",
                pre_service.get("main_pid"),
                service.get("main_pid"),
            ),
            _check(
                "post.service_restart_count_stable",
                pre_service.get("restart_count"),
                service.get("restart_count"),
            ),
            _check("post.capture_http_status", 200, capture.get("http_status")),
            _check("post.capture_idle", "idle", capture.get("device_state")),
            _check(
                "post.capture_active_recording_present",
                False,
                capture.get("active_recording_present"),
            ),
            _check("post.expected_commit", expected_commit, version.get("commit")),
        ]
    )
    if post_identity is not None and pre_binding is not None:
        post_admission = evaluate_protocol_admission(
            health=post_health,
            identity=post_identity.public,
            expected_commit=expected_commit,
            device=pre_binding.query_node,
        )
        checks.append(
            _check(
                "post.exact_protocol_admission_failures",
                [],
                post_admission["failed_checks"],
            )
        )
    return checks


def _initial_report(config: CollectionConfig, now: str) -> dict[str, Any]:
    return {
        "schema": SCHEMA,
        "collector": {
            "version": COLLECTOR_VERSION,
            "script_sha256": _script_sha256(),
        },
        "issue": ISSUE,
        "created_at": now,
        "completed_at": None,
        "status": "checkpoint_created",
        "stop_reason": None,
        "configuration": {
            "expected_commit": config.expected_commit,
            "query_node": config.device,
            "watchdog_seconds": config.watchdog_seconds,
        },
        "read_only_contract": {
            "uvc_query_codes": {
                "GET_INFO": f"0x{UVC_GET_INFO:02x}",
                "GET_LEN": f"0x{UVC_GET_LEN:02x}",
                "GET_CUR": f"0x{UVC_GET_CUR:02x}",
            },
            "arbitrary_query_codes_accepted": False,
            "set_cur_performed": False,
            "unknown_xu_writes_performed": 0,
            "write_requests_performed": 0,
            "automatic_retries": 0,
            "cur_attempts_per_selector": 1,
            "get_min_max_res_def_performed": False,
            "usb_reset_performed": False,
            "service_restart_performed": False,
            "device_open_mode": "O_RDWR_required_for_UVCIOC_CTRL_QUERY_GET_requests",
        },
        "expected_identity": _expected_identity(),
        "pre": {
            "identity": None,
            "health": None,
            "query_gate_health": None,
            "deployed_fingerprint": None,
        },
        "protocol_admission": {
            "admitted": False,
            "checks": [],
            "failed_checks": ["not_evaluated"],
        },
        "selectors": [],
        "query_execution": {
            "executed_count": 0,
            "successful_count": 0,
            "failed_count": 0,
            "in_progress": None,
            "by_query": {
                name: {"executed": 0, "successful": 0, "failed": 0}
                for name in ("GET_INFO", "GET_LEN", "GET_CUR")
            },
            "attempts": [],
        },
        "post": {
            "identity": None,
            "health": None,
            "health_admission": None,
            "deployed_fingerprint": None,
            "checks": [],
        },
        "checkpointing": {
            "mode": "same_directory_atomic_replace_with_fsync",
            "output_created_before_probes": True,
            "completed_count": 1,
            "last_status": "checkpoint_created",
        },
        "privacy_audit": {
            "passed": True,
            "checks": {"initial_checkpoint_contains_no_observations": True},
            "serial_storage": "sha256_only",
            "cur_storage": "length_and_sha256_only",
            "media_storage": "structured_redacted_fields_only",
            "v4l2_storage": "structured_control_fields_only",
            "usb_descriptor_storage": "sha256_and_admission_fields_only",
            "http_body_storage": "selected_health_fields_only",
            "sensitive_identity_storage": "serial_bus_and_sysfs_values_sha256_only",
        },
    }


def collect_evidence(
    config: CollectionConfig,
    *,
    runtime: Runtime | None = None,
    writer: AtomicCheckpoint | None = None,
) -> dict[str, Any]:
    runtime = runtime or SystemRuntime()
    writer = writer or AtomicCheckpoint(config.output)
    report = _initial_report(config, runtime.now())

    # The output must exist durably before any health, media, descriptor, or UVC query.
    writer.create(report)

    private_values: list[str] = []
    cur_payloads: list[bytes] = []
    pre_identity: IdentityObservation | None = None
    pre_health: dict[str, Any] | None = None
    query_gate_health: dict[str, Any] | None = None
    post_identity: IdentityObservation | None = None
    post_health: dict[str, Any] | None = None

    try:
        with ProcessWatchdog(config.watchdog_seconds):
            report["status"] = "preflight"
            _checkpoint(
                writer,
                report,
                private_values=private_values,
                cur_payloads=cur_payloads,
            )
            pre_health = runtime.health_snapshot()
            report["pre"]["health"] = pre_health
            health_admission = evaluate_health_admission(
                health=pre_health,
                expected_commit=config.expected_commit,
            )
            report["protocol_admission"] = health_admission
            _checkpoint(
                writer,
                report,
                private_values=private_values,
                cur_payloads=cur_payloads,
            )
            if not health_admission["admitted"]:
                _mark_stopped(report, _admission_stop_reason(health_admission))
                _checkpoint(
                    writer,
                    report,
                    private_values=private_values,
                    cur_payloads=cur_payloads,
                )
            else:
                pre_identity = runtime.identity_snapshot(config.device)
                private_values.extend(pre_identity.private_values)
                report["pre"]["identity"] = pre_identity.public
                _checkpoint(
                    writer,
                    report,
                    private_values=private_values,
                    cur_payloads=cur_payloads,
                )

                query_gate_health = runtime.health_snapshot()
                report["pre"]["query_gate_health"] = query_gate_health
                report["pre"]["deployed_fingerprint"] = deployment_fingerprint(
                    health=query_gate_health,
                    identity=pre_identity.public,
                )
                admission = evaluate_protocol_admission(
                    health=query_gate_health,
                    identity=pre_identity.public,
                    expected_commit=config.expected_commit,
                    device=config.device,
                )
                report["protocol_admission"] = admission
                _checkpoint(
                    writer,
                    report,
                    private_values=private_values,
                    cur_payloads=cur_payloads,
                )
                if not admission["admitted"]:
                    _mark_stopped(report, _admission_stop_reason(admission))
                    _checkpoint(
                        writer,
                        report,
                        private_values=private_values,
                        cur_payloads=cur_payloads,
                    )
                else:
                    _collect_queries(
                        config=config,
                        runtime=runtime,
                        writer=writer,
                        report=report,
                        identity=pre_identity,
                        private_values=private_values,
                        cur_payloads=cur_payloads,
                    )
    except OutputUnavailable:
        raise
    except WatchdogExpired as exc:
        reason = exc.reason()
        if report["query_execution"]["in_progress"] is not None:
            reason["in_progress_query"] = dict(report["query_execution"]["in_progress"])
        _mark_stopped(report, reason)
    except BaseException as exc:
        _mark_stopped(report, _safe_exception_reason(exc, phase=str(report["status"])))

    report["status"] = "postflight" if report["stop_reason"] is None else "stopped"
    try:
        post_health = runtime.health_snapshot()
        report["post"]["health"] = post_health
        report["post"]["health_admission"] = evaluate_health_admission(
            health=post_health,
            expected_commit=config.expected_commit,
        )
    except BaseException as exc:
        report["post"]["health"] = {
            "available": False,
            "error_code": exc.code if isinstance(exc, EvidenceError) else "post_health_exception",
        }
    _checkpoint(
        writer,
        report,
        private_values=private_values,
        cur_payloads=cur_payloads,
    )

    unsafe_identity_stop_codes = {
        "capture_not_idle",
        "rp_ylx_not_active",
        "expected_commit_mismatch",
        "deployed_version_unavailable",
    }
    stop_code = report["stop_reason"].get("code") if report["stop_reason"] else None
    post_health_admitted = bool(
        isinstance(report["post"]["health_admission"], dict)
        and report["post"]["health_admission"].get("admitted")
    )
    try:
        if pre_identity is None:
            report["post"]["identity"] = {
                "available": False,
                "skipped": "pre_identity_not_admitted",
            }
        elif stop_code in unsafe_identity_stop_codes:
            report["post"]["identity"] = {
                "available": False,
                "skipped": "unsafe_precondition_stop_reason",
            }
        elif not post_health_admitted:
            report["post"]["identity"] = {
                "available": False,
                "skipped": "post_health_not_admitted",
            }
        else:
            post_identity = runtime.identity_snapshot(config.device)
            private_values.extend(post_identity.private_values)
            report["post"]["identity"] = post_identity.public
    except BaseException as exc:
        report["post"]["identity"] = {
            "available": False,
            "error_code": (
                exc.code if isinstance(exc, EvidenceError) else "post_identity_exception"
            ),
        }
    if post_identity is not None and post_health is not None:
        report["post"]["deployed_fingerprint"] = deployment_fingerprint(
            health=post_health,
            identity=post_identity.public,
        )
    _checkpoint(
        writer,
        report,
        private_values=private_values,
        cur_payloads=cur_payloads,
    )

    post_checks = _post_checks(
        pre_binding=pre_identity.binding if pre_identity is not None else None,
        pre_health=query_gate_health or pre_health,
        post_identity=post_identity,
        post_health=post_health,
        expected_commit=config.expected_commit,
    )
    report["post"]["checks"] = post_checks
    failed_post_checks = [item["name"] for item in post_checks if not item["passed"]]
    if failed_post_checks and report["stop_reason"] is None:
        _mark_stopped(
            report,
            {
                "code": "postcondition_failed",
                "phase": "postflight",
                "failed_checks": failed_post_checks,
            },
        )
    elif report["stop_reason"] is None:
        report["status"] = "complete"

    report["completed_at"] = runtime.now()
    _checkpoint(
        writer,
        report,
        private_values=private_values,
        cur_payloads=cur_payloads,
    )
    return report


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--expected-commit", required=True)
    parser.add_argument("--device", default=DEFAULT_DEVICE)
    parser.add_argument("--watchdog-seconds", type=float, default=30.0)
    return parser


def _validate_args(args: argparse.Namespace) -> None:
    if re.fullmatch(r"[0-9a-f]{40}", args.expected_commit) is None:
        raise SystemExit("--expected-commit must be exactly 40 lowercase hexadecimal characters")
    if VIDEO_NODE_RE.fullmatch(args.device) is None:
        raise SystemExit("--device must match /dev/videoN")
    if not 1.0 <= args.watchdog_seconds <= 300.0:
        raise SystemExit("--watchdog-seconds must be between 1 and 300")


def main(argv: list[str] | None = None) -> int:
    args = build_arg_parser().parse_args(argv)
    _validate_args(args)
    config = CollectionConfig(
        output=args.output,
        expected_commit=args.expected_commit,
        device=args.device,
        watchdog_seconds=args.watchdog_seconds,
    )
    try:
        report = collect_evidence(config)
    except OutputUnavailable:
        print(
            json.dumps({"ok": False, "error_code": "output_unavailable"}, sort_keys=True),
            file=sys.stderr,
        )
        return 3
    except EvidenceError as exc:
        print(
            json.dumps({"ok": False, "error_code": exc.code}, sort_keys=True),
            file=sys.stderr,
        )
        return 2

    print(
        json.dumps(
            {
                "ok": report["status"] == "complete",
                "output": str(config.output),
                "status": report["status"],
                "executed_query_count": report["query_execution"]["executed_count"],
                "stop_reason": report["stop_reason"],
            },
            ensure_ascii=True,
            sort_keys=True,
        )
    )
    return 0 if report["status"] == "complete" else 2


if __name__ == "__main__":
    raise SystemExit(main())

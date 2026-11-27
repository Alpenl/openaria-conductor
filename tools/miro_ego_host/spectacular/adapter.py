"""Strict Miro-Ego Device Session v1 ingestion for the Spectacular calibration toolchain."""

from __future__ import annotations

import ctypes
import ctypes.util
import hashlib
import io
import json
import math
import os
import re
import stat
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any, BinaryIO, Literal

DEVICE_SESSION_SCHEMA = "miro-ego.device-session.v1"
FRAME_INDEX_SCHEMA = "miro-ego.frame-index.v1"
FRAME_TIMESTAMP_AUDIT_SCHEMA = "miro-ego.frame-timestamp-audit.v1"
IMU_SAMPLE_FORMAT = "miro-ego.imu.v1"
RECORDING_ENCODING_SCHEMA = "miro-ego.recording-encoding.v1"
CAPTURE_AUDIT_SCHEMA = "miro-ego.capture-audit.v1"

MAX_MANIFEST_BYTES = 16 * 1024 * 1024
MAX_NDJSON_LINE_BYTES = 1024 * 1024
READ_CHUNK_BYTES = 1024 * 1024
ZSTD_BLOCK_BYTES = 1024 * 1024
ZSTD_MAGIC = (0xFD2FB528).to_bytes(4, "little")

_SHA256_RE = re.compile(r"^[0-9a-f]{64}\Z")
_UUID_V7_RE = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}\Z")
_UUID_V4_RE = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}\Z")
_LOG_ROLE_RE = re.compile(r"^log(?:[._-][a-z0-9]+)+\Z")
_RESERVED_CONTROL_FILES = frozenset(
    {
        "manifest.json",
        "recording.json",
        "capture.json",
        "segments.json",
        "checkpoint.json",
    }
)


class CaptureValidationError(ValueError):
    """The selected capture cannot be consumed without guessing or trusting unsafe data."""


VideoAuthority = Literal["device_session"]
VideoCodec = Literal["h264", "hevc"]


@dataclass(frozen=True, slots=True)
class CaptureVideoSegment:
    index: int
    start_frame: int
    end_frame: int
    left_path: Path
    right_path: Path


@dataclass(frozen=True, slots=True)
class CaptureVideo:
    authority: VideoAuthority
    codec: VideoCodec
    container: str = "mp4"
    layout: str = "split-eyes"
    segments: tuple[CaptureVideoSegment, ...] = ()


@dataclass(frozen=True, slots=True)
class CaptureLogArtifact:
    role: str
    path: Path
    media_type: str
    bytes: int
    sha256: str


@dataclass(frozen=True, slots=True)
class LoadedCapture:
    root: Path
    source_schema: str
    session_id: str
    manifest: dict[str, Any]
    frames: tuple[dict[str, Any], ...]
    imu_samples: tuple[dict[str, Any], ...]
    video: CaptureVideo
    fps: float
    sensor_fps: float
    frame_decimation: int
    width: int
    eye_width: int
    height: int
    imu_samples_per_packet: int
    imu_timestamp_bits: int | None
    logs: tuple[CaptureLogArtifact, ...] = ()


class _ZstdInBuffer(ctypes.Structure):
    _fields_ = [
        ("src", ctypes.c_void_p),
        ("size", ctypes.c_size_t),
        ("pos", ctypes.c_size_t),
    ]


class _ZstdOutBuffer(ctypes.Structure):
    _fields_ = [
        ("dst", ctypes.c_void_p),
        ("size", ctypes.c_size_t),
        ("pos", ctypes.c_size_t),
    ]


_LIBZSTD_CACHE: ctypes.CDLL | None | Literal[False] = False


def _load_libzstd() -> ctypes.CDLL | None:
    global _LIBZSTD_CACHE
    if _LIBZSTD_CACHE is not False:
        return _LIBZSTD_CACHE

    candidates: list[str] = []
    found = ctypes.util.find_library("zstd")
    if found:
        candidates.append(found)
    candidates.extend(("libzstd.so.1", "libzstd.so", "libzstd.1.dylib", "libzstd.dll", "zstd.dll"))

    for name in candidates:
        try:
            lib = ctypes.CDLL(name)
        except OSError:
            continue
        lib.ZSTD_isError.argtypes = [ctypes.c_size_t]
        lib.ZSTD_isError.restype = ctypes.c_uint
        lib.ZSTD_getErrorName.argtypes = [ctypes.c_size_t]
        lib.ZSTD_getErrorName.restype = ctypes.c_char_p
        lib.ZSTD_createDStream.argtypes = []
        lib.ZSTD_createDStream.restype = ctypes.c_void_p
        lib.ZSTD_freeDStream.argtypes = [ctypes.c_void_p]
        lib.ZSTD_freeDStream.restype = ctypes.c_size_t
        lib.ZSTD_initDStream.argtypes = [ctypes.c_void_p]
        lib.ZSTD_initDStream.restype = ctypes.c_size_t
        lib.ZSTD_decompressStream.argtypes = [
            ctypes.c_void_p,
            ctypes.POINTER(_ZstdOutBuffer),
            ctypes.POINTER(_ZstdInBuffer),
        ]
        lib.ZSTD_decompressStream.restype = ctypes.c_size_t
        _LIBZSTD_CACHE = lib
        return lib

    _LIBZSTD_CACHE = None
    return None


class _CtypesZstdRawReader(io.RawIOBase):
    """Streaming Zstandard frame decoder backed by system libzstd without owning source fd."""

    def __init__(self, source: BinaryIO, lib: ctypes.CDLL) -> None:
        super().__init__()
        self._source = source
        self._lib = lib
        self._dstream: int | None = lib.ZSTD_createDStream()
        if not self._dstream:
            raise CaptureValidationError("failed to allocate Zstandard decoder state")
        rc = lib.ZSTD_initDStream(self._dstream)
        if lib.ZSTD_isError(rc):
            self._free()
            raise CaptureValidationError("failed to initialize Zstandard decoder state")
        self._in_buf: ctypes.Array[ctypes.c_ubyte] | None = None
        self._in_len = 0
        self._in_pos = 0
        self._in_frame = False
        self._frames_decoded = 0
        self._eof = False

    def _free(self) -> None:
        if self._dstream is not None:
            self._lib.ZSTD_freeDStream(self._dstream)
            self._dstream = None

    def readable(self) -> bool:
        return True

    def readinto(self, target: Any) -> int:
        if self.closed or self._dstream is None:
            raise ValueError("I/O operation on closed Zstandard stream")
        view = memoryview(target).cast("B")
        want = len(view)
        if want == 0:
            return 0

        out_cap = min(want, 65536)
        out_arr = (ctypes.c_ubyte * out_cap)()

        while True:
            if self._in_pos >= self._in_len and not self._eof:
                chunk = self._source.read(65536)
                if not chunk:
                    self._eof = True
                else:
                    self._in_buf = (ctypes.c_ubyte * len(chunk)).from_buffer_copy(chunk)
                    self._in_len = len(chunk)
                    self._in_pos = 0

            if self._eof and self._in_pos >= self._in_len:
                if self._in_frame or self._frames_decoded == 0:
                    raise CaptureValidationError("incomplete or empty Zstandard metadata stream")
                return 0

            assert self._in_buf is not None
            in_struct = _ZstdInBuffer(
                ctypes.addressof(self._in_buf),
                self._in_len,
                self._in_pos,
            )
            out_struct = _ZstdOutBuffer(ctypes.addressof(out_arr), out_cap, 0)
            ret = self._lib.ZSTD_decompressStream(
                self._dstream,
                ctypes.byref(out_struct),
                ctypes.byref(in_struct),
            )
            if self._lib.ZSTD_isError(ret):
                raw_name = self._lib.ZSTD_getErrorName(ret)
                detail = raw_name.decode("utf-8", errors="replace") if raw_name else "unknown error"
                raise CaptureValidationError(f"corrupt Zstandard metadata stream: {detail}")

            progressed = (in_struct.pos > self._in_pos) or (out_struct.pos > 0)
            self._in_pos = in_struct.pos
            if ret == 0:
                self._in_frame = False
                self._frames_decoded += 1
            else:
                self._in_frame = True

            if out_struct.pos > 0:
                produced = out_struct.pos
                view[:produced] = memoryview(out_arr).cast("B")[:produced]
                return produced

            if not progressed and self._in_pos < self._in_len:
                raise CaptureValidationError("Zstandard metadata decoder made no forward progress")

    def close(self) -> None:
        try:
            self._free()
        finally:
            super().close()


@contextmanager
def _open_zstd_stream(raw: BinaryIO) -> Iterator[io.BufferedReader]:
    try:
        from compression import zstd as stdlib_zstd  # type: ignore[import-not-found]
    except ImportError:
        stdlib_zstd = None

    if stdlib_zstd is not None:
        try:
            with stdlib_zstd.open(raw, "rb") as reader:
                yield io.BufferedReader(reader)
                return
        except Exception as error:
            raise CaptureValidationError(f"corrupt Zstandard metadata stream: {error}") from error

    try:
        import zstandard  # type: ignore[import-not-found]
    except ImportError:
        zstandard = None

    if zstandard is not None:
        try:
            dctx = zstandard.ZstdDecompressor(max_window_size=8 * ZSTD_BLOCK_BYTES)
            with dctx.stream_reader(raw) as reader:
                yield io.BufferedReader(reader)
                return
        except Exception as error:
            raise CaptureValidationError(f"corrupt Zstandard metadata stream: {error}") from error

    lib = _load_libzstd()
    if lib is None:
        raise CaptureValidationError(
            "Zstandard metadata decompression requires Python 3.14+, zstandard, or system libzstd"
        )
    raw_decoder = _CtypesZstdRawReader(raw, lib)
    try:
        yield io.BufferedReader(raw_decoder)
    finally:
        raw_decoder.close()


def _stat_identity(metadata: os.stat_result, label: str) -> tuple[int, ...]:
    if not stat.S_ISREG(metadata.st_mode) or metadata.st_nlink != 1:
        raise CaptureValidationError(f"{label} is not an exclusive regular file")
    return (
        metadata.st_dev,
        metadata.st_ino,
        metadata.st_mode,
        metadata.st_nlink,
        metadata.st_size,
        metadata.st_mtime_ns,
    )


def open_regular(path: str | Path, *, label: str = "artifact") -> BinaryIO:
    """Open an exclusively-linked regular file while rejecting symlinks and FIFOs."""
    target = Path(path)
    try:
        lstat_meta = target.lstat()
    except OSError as error:
        raise CaptureValidationError(f"missing {label}: {target}") from error
    _stat_identity(lstat_meta, f"{label} ({target})")

    flags = (
        os.O_RDONLY
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_NOFOLLOW", 0)
        | getattr(os, "O_NONBLOCK", 0)
        | getattr(os, "O_BINARY", 0)
    )
    try:
        descriptor = os.open(target, flags)
    except OSError as error:
        raise CaptureValidationError(f"cannot safely open {label} ({target}): {error}") from error

    try:
        fstat_meta = os.fstat(descriptor)
        opened_id = _stat_identity(fstat_meta, f"{label} ({target})")
        if os.name == "posix" and (lstat_meta.st_dev, lstat_meta.st_ino) != (
            fstat_meta.st_dev,
            fstat_meta.st_ino,
        ):
            raise CaptureValidationError(f"{label} inode changed during open: {target}")
        _ = opened_id
        return os.fdopen(descriptor, "rb")
    except BaseException:
        os.close(descriptor)
        raise


@contextmanager
def open_metadata(path: str | Path, *, label: str = "metadata") -> Iterator[io.BufferedReader]:
    """Stream NDJSON metadata transparently across plain UTF-8 and Zstandard (.zst) files."""
    target = Path(path)
    with open_regular(target, label=label) as raw:
        buffered = io.BufferedReader(raw)
        magic = buffered.peek(4)[:4]
        has_zst_suffix = target.suffix.lower() == ".zst"
        has_zst_magic = magic == ZSTD_MAGIC
        if has_zst_suffix and not has_zst_magic:
            raise CaptureValidationError(
                f"{label} has .zst suffix without Zstandard magic: {target}"
            )
        if has_zst_suffix or has_zst_magic:
            with _open_zstd_stream(buffered) as decoded:
                yield decoded
        else:
            yield buffered


def _mapping(value: object, label: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise CaptureValidationError(f"{label} must be an object")
    return value


def _ensure_keys(
    mapping: dict[str, Any],
    *,
    required: set[str] | frozenset[str],
    optional: set[str] | frozenset[str] = frozenset(),
    label: str,
) -> None:
    keys = set(mapping)
    missing = required - keys
    extra = keys - (required | optional)
    if missing or extra:
        raise CaptureValidationError(f"{label} is not a closed object")


def _integer(
    value: object,
    label: str,
    *,
    minimum: int = 0,
    maximum: int | None = None,
) -> int:
    if type(value) is not int or value < minimum or (maximum is not None and value > maximum):
        raise CaptureValidationError(f"{label} is not a bounded integer")
    return value


def _number(
    value: object,
    label: str,
    *,
    positive: bool = False,
    non_negative: bool = False,
) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise CaptureValidationError(f"{label} is not numeric")
    result = float(value)
    if not math.isfinite(result):
        raise CaptureValidationError(f"{label} must be finite")
    if positive and result <= 0.0:
        raise CaptureValidationError(f"{label} must be positive")
    if non_negative and result < 0.0:
        raise CaptureValidationError(f"{label} must be non-negative")
    return result


def _non_empty_str(value: object, label: str, *, max_length: int | None = None) -> str:
    if not isinstance(value, str) or not value:
        raise CaptureValidationError(f"{label} must be a non-empty string")
    if max_length is not None and len(value) > max_length:
        raise CaptureValidationError(f"{label} exceeds maximum length")
    return value


def _strict_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate JSON key: {key}")
        result[key] = value
    return result


def _reject_json_constant(value: str) -> None:
    raise ValueError(f"non-finite JSON number: {value}")


def _loads_json(payload: str | bytes, label: str) -> object:
    try:
        text = payload.decode("utf-8") if isinstance(payload, bytes) else payload
        return json.loads(
            text,
            object_pairs_hook=_strict_object,
            parse_constant=_reject_json_constant,
        )
    except (UnicodeError, json.JSONDecodeError, ValueError) as error:
        raise CaptureValidationError(f"invalid JSON in {label}: {error}") from error


def _require_directory(path: Path, label: str) -> None:
    try:
        metadata = path.lstat()
    except OSError as error:
        raise CaptureValidationError(f"missing {label}: {path}") from error
    if not stat.S_ISDIR(metadata.st_mode):
        raise CaptureValidationError(f"{label} is not a real directory: {path}")


def _is_temp_component(part: str) -> bool:
    idx = part.find(".tmp")
    if idx < 0:
        return False
    suffix = part[idx + 4 :]
    return suffix == "" or (len(suffix) >= 2 and suffix[0] in "._-")


def _safe_artifact_relative(value: object, label: str) -> PurePosixPath:
    if (
        not isinstance(value, str)
        or not value
        or len(value) > 1024
        or value.startswith("/")
        or "\\" in value
        or any(ord(ch) < 0x20 or 0x7F <= ord(ch) <= 0x9F for ch in value)
    ):
        raise CaptureValidationError(f"invalid {label} path")
    relative = PurePosixPath(value)
    if (
        relative.is_absolute()
        or relative.as_posix() != value
        or any(part in {"", ".", ".."} or _is_temp_component(part) for part in relative.parts)
    ):
        raise CaptureValidationError(f"unsafe {label} path: {value!r}")
    if (
        relative.parts[0].casefold() in _RESERVED_CONTROL_FILES
        or relative.parts[-1].casefold() == "checkpoint.json"
    ):
        raise CaptureValidationError(f"{label} cannot reference session control file: {value!r}")
    return relative


def _verify_artifact_file(
    root: Path,
    relative: PurePosixPath,
    *,
    expected_bytes: int,
    expected_sha256: str,
    allow_empty: bool,
    label: str,
) -> Path:
    current = root
    for component in relative.parts[:-1]:
        current = current / component
        _require_directory(current, f"{label} parent")
    path = root.joinpath(*relative.parts)
    if not allow_empty and expected_bytes <= 0:
        raise CaptureValidationError(f"{label} must be non-empty")

    with open_regular(path, label=label) as stream:
        fd = stream.fileno()
        before = _stat_identity(os.fstat(fd), label)
        if os.fstat(fd).st_size != expected_bytes:
            raise CaptureValidationError(
                f"Device Session integrity validation failed: {label} size mismatch"
            )
        hasher = hashlib.sha256()
        remaining = expected_bytes
        while remaining > 0:
            chunk = stream.read(min(READ_CHUNK_BYTES, remaining))
            if not chunk:
                raise CaptureValidationError(
                    f"Device Session integrity validation failed: short read on {label}"
                )
            hasher.update(chunk)
            remaining -= len(chunk)
        if stream.read(1):
            raise CaptureValidationError(
                f"Device Session integrity validation failed: {label} grew during read"
            )
        after = _stat_identity(os.fstat(fd), label)
        if after != before:
            raise CaptureValidationError(
                f"Device Session integrity validation failed: {label} mutated during verification"
            )
        if hasher.hexdigest() != expected_sha256:
            raise CaptureValidationError(
                f"Device Session integrity validation failed: {label} SHA-256 mismatch"
            )
    return path


def _validate_storage_encoding(
    descriptor: dict[str, Any],
    *,
    path_str: str,
    media_type: str,
    label: str,
) -> dict[str, Any] | None:
    raw_se = descriptor.get("storage_encoding")
    if media_type == "application/zstd":
        if not path_str.endswith(".ndjson.zst"):
            raise CaptureValidationError(f"{label} zstd path must end with .ndjson.zst")
        se = _mapping(raw_se, f"{label}.storage_encoding")
        _ensure_keys(
            se,
            required={
                "codec",
                "level",
                "block_bytes",
                "uncompressed_bytes",
                "uncompressed_sha256",
            },
            label=f"{label}.storage_encoding",
        )
        if (
            se["codec"] != "zstd"
            or se["level"] != 1
            or se["block_bytes"] != ZSTD_BLOCK_BYTES
            or type(se["uncompressed_bytes"]) is not int
            or se["uncompressed_bytes"] < 0
            or not isinstance(se["uncompressed_sha256"], str)
            or _SHA256_RE.fullmatch(se["uncompressed_sha256"]) is None
        ):
            raise CaptureValidationError(f"{label} zstd storage_encoding metadata is invalid")
        return se

    if raw_se is not None:
        raise CaptureValidationError(f"{label} unexpected storage_encoding for {media_type}")
    if path_str.endswith(".zst"):
        raise CaptureValidationError(f"{label} uncompressed path must not end with .zst")
    return None


def _verify_descriptor(
    root: Path,
    value: object,
    *,
    expected_role: str | None,
    allowed_media_types: set[str] | frozenset[str] | None,
    allow_empty: bool,
    seen_paths: set[str],
    label: str,
) -> tuple[dict[str, Any], Path, dict[str, Any] | None]:
    descriptor = _mapping(value, label)
    _ensure_keys(
        descriptor,
        required={"artifact_id", "role", "path", "media_type", "bytes", "sha256"},
        optional={"storage_encoding"},
        label=label,
    )
    role = _non_empty_str(descriptor["role"], f"{label}.role", max_length=96)
    if expected_role is not None and role != expected_role:
        raise CaptureValidationError(
            f"Device Session integrity validation failed: "
            f"{label} role {role!r} != {expected_role!r}"
        )
    media_type = _non_empty_str(descriptor["media_type"], f"{label}.media_type")
    if allowed_media_types is not None and media_type not in allowed_media_types:
        raise CaptureValidationError(
            f"Device Session integrity validation failed: {label} media_type {media_type!r} invalid"
        )
    sha256 = _non_empty_str(descriptor["sha256"], f"{label}.sha256")
    artifact_id = _non_empty_str(descriptor["artifact_id"], f"{label}.artifact_id")
    if _SHA256_RE.fullmatch(sha256) is None or artifact_id != sha256:
        raise CaptureValidationError(
            f"Device Session integrity validation failed: {label} digest identity invalid"
        )
    expected_bytes = _integer(
        descriptor["bytes"],
        f"{label}.bytes",
        minimum=0 if allow_empty else 1,
    )
    relative = _safe_artifact_relative(descriptor["path"], label)
    folded = relative.as_posix().casefold()
    if folded in seen_paths:
        raise CaptureValidationError(f"duplicate artifact path in manifest: {relative.as_posix()}")
    seen_paths.add(folded)

    storage_encoding = _validate_storage_encoding(
        descriptor,
        path_str=relative.as_posix(),
        media_type=media_type,
        label=label,
    )
    verified_path = _verify_artifact_file(
        root,
        relative,
        expected_bytes=expected_bytes,
        expected_sha256=sha256,
        allow_empty=allow_empty,
        label=label,
    )
    return descriptor, verified_path, storage_encoding


def _read_json_object(path: Path, label: str) -> dict[str, Any]:
    with open_regular(path, label=label) as stream:
        size = os.fstat(stream.fileno()).st_size
        if size <= 0 or size > MAX_MANIFEST_BYTES:
            raise CaptureValidationError(f"{label} size is out of bounds")
        payload = stream.read(MAX_MANIFEST_BYTES + 1)
    if len(payload) > MAX_MANIFEST_BYTES:
        raise CaptureValidationError(f"{label} grew beyond bounded size")
    return _mapping(_loads_json(payload, label), label)


def _read_jsonl(
    path: Path,
    label: str,
    *,
    storage_encoding: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    if storage_encoding is None:
        with open_regular(path, label=label) as raw_check:
            if raw_check.read(4) == ZSTD_MAGIC:
                raise CaptureValidationError(
                    f"{label} contains Zstandard magic without application/zstd media_type"
                )

    records: list[dict[str, Any]] = []
    decoded_bytes = 0
    decoded_hasher = hashlib.sha256() if storage_encoding is not None else None
    max_uncompressed = (
        int(storage_encoding["uncompressed_bytes"]) if storage_encoding is not None else None
    )

    try:
        with open_metadata(path, label=label) as stream:
            line_number = 0
            while True:
                line = stream.readline(MAX_NDJSON_LINE_BYTES + 1)
                if not line:
                    break
                line_number += 1
                if len(line) > MAX_NDJSON_LINE_BYTES or not line.endswith(b"\n"):
                    raise CaptureValidationError(
                        f"{label}:{line_number} is overlong or lacks a trailing newline"
                    )
                decoded_bytes += len(line)
                if max_uncompressed is not None and decoded_bytes > max_uncompressed:
                    raise CaptureValidationError(
                        f"{label} decoded length exceeds declared uncompressed_bytes"
                    )
                if decoded_hasher is not None:
                    decoded_hasher.update(line)
                value = _loads_json(line, f"{label}:{line_number}")
                records.append(_mapping(value, f"{label}:{line_number}"))
    except CaptureValidationError:
        raise
    except OSError as error:
        raise CaptureValidationError(f"cannot read {label}: {error}") from error

    if storage_encoding is not None:
        assert decoded_hasher is not None
        if (
            decoded_bytes != storage_encoding["uncompressed_bytes"]
            or decoded_hasher.hexdigest() != storage_encoding["uncompressed_sha256"]
        ):
            raise CaptureValidationError(
                f"{label} decoded content does not match storage_encoding digest/bytes"
            )

    if not records:
        raise CaptureValidationError(f"{label} is empty")
    return records


def valid_frame_record(record: dict[str, Any]) -> bool:
    """Validate closed ``miro-ego.frame-index.v1`` rows (with optional timestamp_audit)."""
    base_keys = {
        "schema",
        "session_id",
        "frame",
        "source_sequence",
        "host_monotonic_ns",
        "segment_index",
        "segment_frame",
    }
    if record.get("schema") != FRAME_INDEX_SCHEMA:
        return False
    record_keys = set(record)
    if record_keys == base_keys:
        return True
    if record_keys != (base_keys | {"timestamp_audit"}):
        return False
    audit = record["timestamp_audit"]
    if not isinstance(audit, dict) or set(audit) != {
        "schema",
        "v4l2_buffer_flags",
        "host_dequeue_monotonic_ns",
        "timestamp_clock",
        "timestamp_source",
        "camera_counter_raw",
        "counter_bits",
    }:
        return False
    counter = audit["camera_counter_raw"]
    return (
        audit["schema"] == FRAME_TIMESTAMP_AUDIT_SCHEMA
        and type(audit["v4l2_buffer_flags"]) is int
        and 0 <= audit["v4l2_buffer_flags"] < (1 << 32)
        and type(audit["host_dequeue_monotonic_ns"]) is int
        and audit["host_dequeue_monotonic_ns"] > 0
        and audit["timestamp_clock"] in {"v4l2_monotonic", "host_monotonic_fallback"}
        and audit["timestamp_source"] in {"start_of_exposure", "end_of_frame", "unknown"}
        and audit["counter_bits"] == 24
        and (counter is None or (type(counter) is int and 0 <= counter < (1 << 24)))
    )


def _raw_vector(value: object, label: str) -> list[int]:
    if not isinstance(value, list) or len(value) != 3:
        raise CaptureValidationError(f"{label} must contain exactly three axes")
    return [
        _integer(axis, f"{label}[{index}]", minimum=-32768, maximum=32767)
        for index, axis in enumerate(value)
    ]


def _normalize_device_frames(
    records: list[dict[str, Any]],
    *,
    session_id: str,
    frame_decimation: int,
    segments: tuple[CaptureVideoSegment, ...],
) -> tuple[dict[str, Any], ...]:
    normalized: list[dict[str, Any]] = []
    previous_host = -1
    previous_source: int | None = None
    for index, record in enumerate(records):
        if not valid_frame_record(record):
            raise CaptureValidationError(f"Device Session frame {index} is not a closed record")
        if record["session_id"] != session_id:
            raise CaptureValidationError(f"Device Session frame {index} identity is invalid")
        frame = _integer(record["frame"], f"frame[{index}].frame")
        source_sequence = _integer(record["source_sequence"], f"frame[{index}].source_sequence")
        host = _integer(record["host_monotonic_ns"], f"frame[{index}].host_monotonic_ns")
        segment_index = _integer(record["segment_index"], f"frame[{index}].segment_index")
        segment_frame = _integer(record["segment_frame"], f"frame[{index}].segment_frame")
        if frame != index or host <= previous_host:
            raise CaptureValidationError(
                f"Device Session frame {index} order or host timestamp is invalid"
            )
        if segment_index >= len(segments):
            raise CaptureValidationError(f"Device Session frame {index} segment is out of range")
        segment = segments[segment_index]
        if not segment.start_frame <= frame < segment.end_frame:
            raise CaptureValidationError(
                f"Device Session frame {index} is outside its declared segment"
            )
        if segment_frame != frame - segment.start_frame:
            raise CaptureValidationError(
                f"Device Session frame {index} segment-local index is invalid"
            )
        if previous_source is not None and source_sequence != previous_source + frame_decimation:
            raise CaptureValidationError(
                f"Device Session source sequence gap or regression at record {index}: "
                f"{previous_source} -> {source_sequence}; expected declared "
                f"frame_decimation {frame_decimation}"
            )
        normalized.append(
            {
                "frame_index": frame,
                "uvc_sequence": source_sequence,
                "callback_monotonic_ns": host,
                "segment_index": segment_index,
                "segment_frame": segment_frame,
                "source": dict(record),
            }
        )
        previous_host = host
        previous_source = source_sequence

    if segments[0].start_frame != 0 or segments[-1].end_frame != len(records):
        raise CaptureValidationError(
            "Device Session frame index does not cover the declared video segments"
        )
    return tuple(normalized)


def _normalize_device_imu(
    records: list[dict[str, Any]],
    *,
    session_id: str,
) -> tuple[dict[str, Any], ...]:
    expected_keys = {
        "format",
        "session_id",
        "sequence",
        "packet_sequence",
        "sample_index",
        "device_timestamp_raw",
        "device_ticks",
        "host_read_start_ns",
        "host_read_end_ns",
        "host_monotonic_ns",
        "raw",
        "sync",
    }
    normalized: list[dict[str, Any]] = []
    previous_sequence: int | None = None
    for index, record in enumerate(records):
        if set(record) != expected_keys:
            raise CaptureValidationError(f"Device Session IMU {index} is not a closed record")
        if record["format"] != IMU_SAMPLE_FORMAT or record["session_id"] != session_id:
            raise CaptureValidationError(f"Device Session IMU {index} identity is invalid")
        sequence = _integer(record["sequence"], f"imu[{index}].sequence")
        if previous_sequence is not None and sequence != previous_sequence + 1:
            raise CaptureValidationError(f"Device Session IMU sequence gap at record {index}")
        packet_sequence = _integer(
            record["packet_sequence"],
            f"imu[{index}].packet_sequence",
            maximum=(1 << 64) - 1,
        )
        sample_index = _integer(record["sample_index"], f"imu[{index}].sample_index", maximum=1)
        device_timestamp_raw = _integer(
            record["device_timestamp_raw"],
            f"imu[{index}].device_timestamp_raw",
            maximum=(1 << 24) - 1,
        )
        device_ticks = _integer(record["device_ticks"], f"imu[{index}].device_ticks")
        read_start = _integer(record["host_read_start_ns"], f"imu[{index}].host_read_start_ns")
        read_end = _integer(record["host_read_end_ns"], f"imu[{index}].host_read_end_ns")
        host = _integer(record["host_monotonic_ns"], f"imu[{index}].host_monotonic_ns")
        if read_end < read_start or not read_start <= host <= read_end:
            raise CaptureValidationError(f"Device Session IMU {index} host interval is invalid")
        raw = _mapping(record["raw"], f"imu[{index}].raw")
        if set(raw) != {"accelerometer", "gyroscope"}:
            raise CaptureValidationError(f"Device Session IMU {index} raw axes are not closed")
        accel = _raw_vector(raw["accelerometer"], f"imu[{index}].accelerometer")
        gyro = _raw_vector(raw["gyroscope"], f"imu[{index}].gyroscope")
        sync = _mapping(record["sync"], f"imu[{index}].sync")
        if set(sync) != {"offset_ns", "residual_ns", "quality"}:
            raise CaptureValidationError(f"Device Session IMU {index} sync is not closed")
        if sync["offset_ns"] is not None and type(sync["offset_ns"]) is not int:
            raise CaptureValidationError(f"Device Session IMU {index} sync.offset_ns is invalid")
        if sync["residual_ns"] is not None and (
            type(sync["residual_ns"]) is not int or sync["residual_ns"] < 0
        ):
            raise CaptureValidationError(f"Device Session IMU {index} sync.residual_ns is invalid")
        if sync["quality"] not in {"insufficient", "degraded", "good"}:
            raise CaptureValidationError(f"Device Session IMU {index} sync quality is invalid")

        normalized.append(
            {
                "sample_number": index,
                "sequence": sequence,
                "packet_sequence": packet_sequence,
                "sample_index": sample_index,
                "samples_in_packet": 2,
                "device_timestamp_raw": device_timestamp_raw,
                "device_ticks": device_ticks,
                "host_read_start_ns": read_start,
                "host_read_end_ns": read_end,
                "host_monotonic_ns": host,
                "accel_raw": accel,
                "gyro_raw": gyro,
                "sync": dict(sync),
                "source": {
                    "format": record["format"],
                    "session_id": record["session_id"],
                    "sequence": sequence,
                    "packet_sequence": packet_sequence,
                    "sample_index": sample_index,
                    "device_timestamp_raw": device_timestamp_raw,
                    "device_ticks": device_ticks,
                    "host_read_start_ns": read_start,
                    "host_read_end_ns": read_end,
                    "host_monotonic_ns": host,
                    "raw": {
                        "accelerometer": list(accel),
                        "gyroscope": list(gyro),
                    },
                    "sync": dict(sync),
                },
            }
        )
        previous_sequence = sequence
    return tuple(normalized)


def _validate_manifest_metadata(manifest: dict[str, Any]) -> str:
    schema = manifest.get("schema")
    if schema != DEVICE_SESSION_SCHEMA:
        raise CaptureValidationError(f"unsupported Device Session schema: {schema!r}")

    _ensure_keys(
        manifest,
        required={
            "schema",
            "manifest_id",
            "sealed",
            "sealed_at",
            "session_id",
            "volume_id",
            "capture_mode",
            "display_name",
            "device",
            "time",
            "take",
            "camera",
            "video",
            "imu",
            "frames",
            "audio",
            "logs",
            "integrity",
        },
        optional={"capture_audit"},
        label="Device Session manifest",
    )
    if manifest["sealed"] is not True:
        raise CaptureValidationError("Device Session manifest is not sealed")
    if manifest["capture_mode"] != "calibration":
        raise CaptureValidationError("Spectacular only accepts calibration Device Sessions")

    manifest_id = _non_empty_str(manifest["manifest_id"], "manifest_id")
    session_id = _non_empty_str(manifest["session_id"], "session_id")
    volume_id = _non_empty_str(manifest["volume_id"], "volume_id")
    if _UUID_V7_RE.fullmatch(manifest_id) is None or _UUID_V7_RE.fullmatch(session_id) is None:
        raise CaptureValidationError("manifest_id and session_id must be valid UUIDv7 strings")
    if _UUID_V4_RE.fullmatch(volume_id) is None:
        raise CaptureValidationError("volume_id must be a valid UUIDv4 string")
    _non_empty_str(manifest["sealed_at"], "sealed_at")
    _non_empty_str(manifest["display_name"], "display_name", max_length=160)

    device = _mapping(manifest["device"], "device")
    _ensure_keys(
        device,
        required={
            "device_id",
            "device_label",
            "hardware_fingerprint",
            "platform",
            "software_version",
            "commit",
        },
        label="device",
    )
    if _UUID_V4_RE.fullmatch(_non_empty_str(device["device_id"], "device.device_id")) is None:
        raise CaptureValidationError("device.device_id must be a valid UUIDv4 string")

    time_block = _mapping(manifest["time"], "time")
    _ensure_keys(
        time_block,
        required={"started_at", "ended_at", "timezone", "duration_seconds"},
        optional={"duration_clock"},
        label="time",
    )
    _number(time_block["duration_seconds"], "time.duration_seconds", non_negative=True)
    if "duration_clock" in time_block and time_block["duration_clock"] != "host_monotonic":
        raise CaptureValidationError("time.duration_clock must be 'host_monotonic'")

    take = _mapping(manifest["take"], "take")
    _ensure_keys(take, required={"take_id", "sequence", "continuation_of"}, label="take")
    if _UUID_V7_RE.fullmatch(_non_empty_str(take["take_id"], "take.take_id")) is None:
        raise CaptureValidationError("take.take_id must be a valid UUIDv7 string")
    take_seq = _integer(take["sequence"], "take.sequence", minimum=1)
    if take_seq == 1:
        if take["continuation_of"] is not None:
            raise CaptureValidationError("take.continuation_of must be null for sequence 1")
    elif (
        not isinstance(take["continuation_of"], str)
        or _UUID_V7_RE.fullmatch(take["continuation_of"]) is None
    ):
        raise CaptureValidationError("take.continuation_of must be UUIDv7 for sequence >= 2")

    integrity = _mapping(manifest["integrity"], "integrity")
    _ensure_keys(
        integrity,
        required={"verified_at", "dropped_frames", "drop_events", "fatal_errors"},
        optional={"quality_policy", "media_write_throughput_bytes_per_second"},
        label="integrity",
    )
    dropped_frames = _integer(integrity["dropped_frames"], "integrity.dropped_frames", minimum=0)
    if (
        not isinstance(integrity["drop_events"], list)
        or not isinstance(integrity["fatal_errors"], list)
        or integrity["fatal_errors"]
    ):
        raise CaptureValidationError("Device Session integrity block reports errors")
    if dropped_frames != 0 or integrity["drop_events"]:
        raise CaptureValidationError(
            "calibration Device Session must have zero dropped frames and no drop events"
        )

    if "capture_audit" in manifest:
        capture_audit = _mapping(manifest["capture_audit"], "capture_audit")
        if capture_audit.get("schema") != CAPTURE_AUDIT_SCHEMA:
            raise CaptureValidationError("capture_audit.schema is invalid")

    return session_id


def _validate_video_encoding(encoding_raw: object, *, video_codec: str) -> None:
    encoding = _mapping(encoding_raw, "video.encoding")
    _ensure_keys(
        encoding,
        required={
            "schema",
            "codec",
            "rate_control",
            "profile",
            "pixel_format",
            "bit_depth",
            "b_frames",
            "bitrate_kbps",
            "min_qp",
            "max_qp",
            "intra_qp",
            "initial_qp",
            "gop_frames",
            "vbv_ms",
        },
        label="video.encoding",
    )
    expected_profile = "high" if video_codec == "h264" else "main"
    if (
        encoding["schema"] != RECORDING_ENCODING_SCHEMA
        or encoding["codec"] != video_codec
        or encoding["profile"] != expected_profile
        or encoding["pixel_format"] != "yuv420p"
        or encoding["bit_depth"] != 8
        or encoding["b_frames"] != 0
    ):
        raise CaptureValidationError("video.encoding violates Miro-Ego recording contract")
    _integer(encoding["intra_qp"], "video.encoding.intra_qp", minimum=0, maximum=51)
    _integer(encoding["gop_frames"], "video.encoding.gop_frames", minimum=1, maximum=300)
    rc = encoding["rate_control"]
    if rc in {"cbr", "avbr"}:
        _integer(encoding["bitrate_kbps"], "video.encoding.bitrate_kbps", minimum=1, maximum=100000)
        _integer(encoding["min_qp"], "video.encoding.min_qp", minimum=0, maximum=51)
        _integer(encoding["max_qp"], "video.encoding.max_qp", minimum=0, maximum=51)
        _integer(encoding["initial_qp"], "video.encoding.initial_qp", minimum=0, maximum=51)
        _integer(encoding["vbv_ms"], "video.encoding.vbv_ms", minimum=1, maximum=3000)
    elif rc == "vbr":
        for null_key in ("bitrate_kbps", "min_qp", "max_qp", "vbv_ms", "initial_qp"):
            if encoding[null_key] is not None:
                raise CaptureValidationError(f"video.encoding.{null_key} must be null for vbr")
    elif rc == "fixqp":
        for null_key in ("bitrate_kbps", "min_qp", "max_qp", "vbv_ms"):
            if encoding[null_key] is not None:
                raise CaptureValidationError(f"video.encoding.{null_key} must be null for fixqp")
        _integer(encoding["initial_qp"], "video.encoding.initial_qp", minimum=0, maximum=51)
    else:
        raise CaptureValidationError(f"unsupported video.encoding.rate_control: {rc!r}")


def _load_device_session(root: Path, manifest: dict[str, Any]) -> LoadedCapture:
    session_id = _validate_manifest_metadata(manifest)

    video = _mapping(manifest["video"], "Device Session video")
    _ensure_keys(
        video,
        required={"layout", "codec", "container", "segments", "encoding"},
        label="Device Session video",
    )
    if video["layout"] != "split-eyes":
        raise CaptureValidationError("calibration Device Session must use split-eyes video")
    raw_codec = video["codec"]
    if raw_codec not in {"h264", "hevc"} or video["container"] != "mp4":
        raise CaptureValidationError("calibration Device Session segmented video is invalid")
    codec: VideoCodec = "h264" if raw_codec == "h264" else "hevc"
    _validate_video_encoding(video["encoding"], video_codec=codec)

    audio = _mapping(manifest["audio"], "Device Session audio")
    if audio.get("state") != "not_recorded":
        raise CaptureValidationError(
            "calibration Device Session must not depend on an audio artifact"
        )
    _ensure_keys(
        audio,
        required={"state", "requested_mode", "resolved_mode", "reason"},
        label="Device Session audio",
    )
    if (
        audio["requested_mode"] not in {"device_default", "disabled", "enabled"}
        or audio["resolved_mode"] != "disabled"
        or audio["reason"]
        not in {
            "user_disabled",
            "device_default_disabled",
            "recording_interrupted",
            "audio_open_failed",
        }
    ):
        raise CaptureValidationError("Device Session not_recorded audio block is invalid")

    camera = _mapping(manifest["camera"], "Device Session camera")
    _ensure_keys(
        camera,
        required={
            "width",
            "height",
            "eye_width",
            "sensor_fps",
            "frame_decimation",
            "effective_fps",
            "coordinate_frame",
        },
        optional={"nominal_fps"},
        label="Device Session camera",
    )
    if camera["coordinate_frame"] != "opencv_optical":
        raise CaptureValidationError("camera.coordinate_frame must be 'opencv_optical'")
    width = _integer(camera["width"], "camera.width", minimum=2)
    height = _integer(camera["height"], "camera.height", minimum=1)
    eye_width = _integer(camera["eye_width"], "camera.eye_width", minimum=1)
    if width != eye_width * 2:
        raise CaptureValidationError("stereo camera width must equal two eye widths")
    sensor_fps = _number(camera["sensor_fps"], "camera.sensor_fps", positive=True)
    frame_decimation = _integer(camera["frame_decimation"], "camera.frame_decimation", minimum=1)
    effective_fps = _number(camera["effective_fps"], "camera.effective_fps", positive=True)
    expected_fps = sensor_fps / frame_decimation
    if not math.isclose(effective_fps, expected_fps, rel_tol=1e-6, abs_tol=1e-6):
        raise CaptureValidationError(
            f"camera.effective_fps ({effective_fps}) is inconsistent with "
            f"sensor_fps / frame_decimation ({sensor_fps} / {frame_decimation} = {expected_fps})"
        )
    if "nominal_fps" in camera:
        nominal_fps = _number(camera["nominal_fps"], "camera.nominal_fps", positive=True)
        if not math.isclose(nominal_fps, expected_fps, rel_tol=1e-6, abs_tol=1e-6):
            raise CaptureValidationError(
                "camera.nominal_fps is inconsistent with sensor_fps / frame_decimation"
            )

    frames_block = _mapping(manifest["frames"], "Device Session frames")
    _ensure_keys(frames_block, required={"artifact", "count"}, label="Device Session frames")
    expected_frame_count = _integer(frames_block["count"], "frames.count", minimum=1)

    imu_block = _mapping(manifest["imu"], "Device Session IMU")
    _ensure_keys(
        imu_block,
        required={"artifact", "sample_count", "units", "coordinate_frame"},
        label="Device Session IMU",
    )
    if imu_block["units"] != "raw_int16" or imu_block["coordinate_frame"] != "raw_device_axes":
        raise CaptureValidationError("Device Session IMU units or coordinate_frame invalid")
    expected_imu_count = _integer(imu_block["sample_count"], "imu.sample_count", minimum=1)

    raw_logs = manifest["logs"]
    if not isinstance(raw_logs, list):
        raise CaptureValidationError("Device Session logs must be a list")

    raw_segments = video["segments"]
    if not isinstance(raw_segments, list) or not raw_segments:
        raise CaptureValidationError("Device Session has no split-eye video segments")

    seen_paths: set[str] = set()
    segments: list[CaptureVideoSegment] = []
    expected_start = 0
    for expected_index, raw_segment in enumerate(raw_segments):
        segment = _mapping(raw_segment, f"video segment {expected_index}")
        _ensure_keys(
            segment,
            required={
                "index",
                "start_frame",
                "end_frame",
                "start_time_seconds",
                "end_time_seconds",
                "artifacts",
            },
            label=f"video segment {expected_index}",
        )
        index = _integer(segment["index"], f"video segment {expected_index}.index")
        start_frame = _integer(
            segment["start_frame"], f"video segment {expected_index}.start_frame"
        )
        end_frame = _integer(
            segment["end_frame"], f"video segment {expected_index}.end_frame", minimum=1
        )
        start_sec = _number(
            segment["start_time_seconds"],
            f"video segment {expected_index}.start_time_seconds",
            non_negative=True,
        )
        end_sec = _number(
            segment["end_time_seconds"],
            f"video segment {expected_index}.end_time_seconds",
            positive=True,
        )
        if (
            index != expected_index
            or start_frame != expected_start
            or end_frame <= start_frame
            or end_sec < start_sec
        ):
            raise CaptureValidationError(
                "Device Session video segment frame/time domain is not contiguous"
            )
        artifacts = _mapping(segment["artifacts"], f"video segment {expected_index}.artifacts")
        _ensure_keys(
            artifacts,
            required={"left", "right"},
            label=f"video segment {expected_index}.artifacts",
        )
        _, left_path, _ = _verify_descriptor(
            root,
            artifacts["left"],
            expected_role="video.left",
            allowed_media_types={"video/mp4"},
            allow_empty=False,
            seen_paths=seen_paths,
            label=f"video segment {expected_index} left",
        )
        _, right_path, _ = _verify_descriptor(
            root,
            artifacts["right"],
            expected_role="video.right",
            allowed_media_types={"video/mp4"},
            allow_empty=False,
            seen_paths=seen_paths,
            label=f"video segment {expected_index} right",
        )
        segments.append(CaptureVideoSegment(index, start_frame, end_frame, left_path, right_path))
        expected_start = end_frame

    _, frames_path, frames_se = _verify_descriptor(
        root,
        frames_block["artifact"],
        expected_role="frames.index",
        allowed_media_types={"application/x-ndjson", "application/zstd"},
        allow_empty=False,
        seen_paths=seen_paths,
        label="frame index",
    )
    _, imu_path, imu_se = _verify_descriptor(
        root,
        imu_block["artifact"],
        expected_role="imu.samples",
        allowed_media_types={"application/x-ndjson", "application/zstd"},
        allow_empty=False,
        seen_paths=seen_paths,
        label="IMU samples",
    )

    verified_logs: list[CaptureLogArtifact] = []
    for log_idx, raw_log in enumerate(raw_logs):
        log_desc, log_path, _ = _verify_descriptor(
            root,
            raw_log,
            expected_role=None,
            allowed_media_types=None,
            allow_empty=True,
            seen_paths=seen_paths,
            label=f"log artifact {log_idx}",
        )
        if _LOG_ROLE_RE.fullmatch(log_desc["role"]) is None:
            raise CaptureValidationError(
                f"Device Session integrity validation failed: invalid log role {log_desc['role']!r}"
            )
        verified_logs.append(
            CaptureLogArtifact(
                role=log_desc["role"],
                path=log_path,
                media_type=log_desc["media_type"],
                bytes=log_desc["bytes"],
                sha256=log_desc["sha256"],
            )
        )

    frame_records = _read_jsonl(frames_path, "frame index", storage_encoding=frames_se)
    imu_records = _read_jsonl(imu_path, "IMU samples", storage_encoding=imu_se)
    frames = _normalize_device_frames(
        frame_records,
        session_id=session_id,
        frame_decimation=frame_decimation,
        segments=tuple(segments),
    )
    imu_samples = _normalize_device_imu(imu_records, session_id=session_id)
    if len(frames) != expected_frame_count:
        raise CaptureValidationError("Device Session frame count differs from the manifest")
    if len(imu_samples) != expected_imu_count:
        raise CaptureValidationError("Device Session IMU count differs from the manifest")

    return LoadedCapture(
        root=root,
        source_schema=DEVICE_SESSION_SCHEMA,
        session_id=session_id,
        manifest=manifest,
        frames=frames,
        imu_samples=imu_samples,
        video=CaptureVideo(
            authority="device_session",
            codec=codec,
            container="mp4",
            layout="split-eyes",
            segments=tuple(segments),
        ),
        fps=effective_fps,
        sensor_fps=sensor_fps,
        frame_decimation=frame_decimation,
        width=width,
        eye_width=eye_width,
        height=height,
        imu_samples_per_packet=2,
        imu_timestamp_bits=None,
        logs=tuple(verified_logs),
    )


def load_capture(path: str | Path) -> LoadedCapture:
    """Load and cryptographically verify a sealed Miro-Ego calibration Device Session."""
    root = Path(path)
    _require_directory(root, "capture root")
    for control_name in ("capture.json", "recording.json", "segments.json", "checkpoint.json"):
        control_path = root / control_name
        if control_path.exists() or control_path.is_symlink():
            raise CaptureValidationError(
                f"capture root contains forbidden unsealed or legacy file: {control_name}"
            )
    manifest_path = root / "manifest.json"
    if not manifest_path.exists() and not manifest_path.is_symlink():
        raise CaptureValidationError(f"missing Device Session manifest: {manifest_path}")
    manifest = _read_json_object(manifest_path, "Device Session manifest")
    return _load_device_session(root, manifest)


def artifact_roles(capture: LoadedCapture) -> tuple[str, ...]:
    """Return the authoritative required calibration roles without filename inference."""
    _ = capture
    return ("video.left", "video.right", "frames.index", "imu.samples")

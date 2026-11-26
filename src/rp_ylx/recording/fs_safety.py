"""Recording-specific POSIX safety, exFAT identity, and framed Zstd helpers."""

from __future__ import annotations

import hashlib
import io
import os
import stat
import uuid
from collections.abc import Callable
from contextlib import suppress
from pathlib import Path, PurePosixPath
from typing import BinaryIO

import zstandard

from rp_ylx.core.fs import (
    EXFAT_UNSUPPORTED_ERRNOS,
    O_CLOEXEC,
    O_DIRECTORY,
    O_NOFOLLOW,
    O_NONBLOCK,
    fsync_directory,
    is_exfat_or_msdos_fd,
    is_exfat_or_msdos_path,
    sha256_file,
    write_bytes_atomic,
)
from rp_ylx.core.fs import (
    write_json_atomic as _core_write_json_atomic,
)
from rp_ylx.recording.metadata_frames import MetadataFrames
from rp_ylx.recording_layout import open_relative_directory

ZSTD_BLOCK_BYTES = 1024 * 1024

__all__ = [
    "ZSTD_BLOCK_BYTES",
    "compress_zstd_stream",
    "copy_regular_atomic",
    "decoded_metadata_stream",
    "file_identity",
    "fsync_directory",
    "is_exfat_or_msdos_fd",
    "is_exfat_or_msdos_path",
    "make_zstd_compressor",
    "open_regular",
    "open_regular_at",
    "open_relative_directory",
    "sha256_file",
    "stream_digest",
    "write_bytes_atomic",
    "write_json_atomic",
    "write_zstd_frame_checked",
]


def write_json_atomic(
    path: Path,
    value: object,
    *,
    pretty: bool = True,
    mode: int | None = 0o640,
) -> None:
    """Serialize value as UTF-8 JSON and write it atomically with restrictive mode."""
    _core_write_json_atomic(
        path,
        value,
        mode=mode,
        sort_keys=True,
        indent=2 if pretty else None,
    )


def copy_regular_atomic(
    source_path: Path,
    target_path: Path,
    *,
    mode: int | None = 0o640,
) -> bool:
    """Copy regular source_path to target_path atomically; return False if missing."""
    flags = os.O_RDONLY | O_NOFOLLOW | O_NONBLOCK | O_CLOEXEC
    try:
        src_fd = os.open(source_path, flags)
    except FileNotFoundError:
        return False
    try:
        src_stream = os.fdopen(src_fd, "rb")
    except BaseException:
        os.close(src_fd)
        raise
    temporary = target_path.with_name(f".{target_path.name}.{uuid.uuid4().hex}.tmp")
    replaced = False
    create_mode = 0o640 if mode is None else mode
    try:
        with src_stream:
            info = os.fstat(src_stream.fileno())
            if not stat.S_ISREG(info.st_mode):
                raise ValueError(f"unsafe recovery control file: {source_path.name}")
            dst_fd = os.open(
                temporary,
                os.O_WRONLY | os.O_CREAT | os.O_EXCL | O_NOFOLLOW | O_CLOEXEC,
                create_mode,
            )
            try:
                dst_stream = os.fdopen(dst_fd, "wb")
            except BaseException:
                os.close(dst_fd)
                raise
            with dst_stream:
                if mode is not None:
                    try:
                        os.fchmod(dst_stream.fileno(), mode)
                    except OSError as error:
                        if error.errno not in EXFAT_UNSUPPORTED_ERRNOS or not is_exfat_or_msdos_fd(
                            dst_stream.fileno()
                        ):
                            raise
                while chunk := src_stream.read(ZSTD_BLOCK_BYTES):
                    if dst_stream.write(chunk) != len(chunk):
                        raise OSError(f"short write copying {source_path.name}")
                dst_stream.flush()
                os.fsync(dst_stream.fileno())
        os.replace(temporary, target_path)
        replaced = True
        fsync_directory(target_path.parent)
        return True
    finally:
        if not replaced:
            with suppress(OSError):
                temporary.unlink(missing_ok=True)


def file_identity(st: os.stat_result, *, include_inode: bool = True) -> tuple[int, ...]:
    """Return canonical exclusive regular-file identity tuple, optionally omitting st_ino."""
    if not stat.S_ISREG(st.st_mode) or (os.name == "posix" and st.st_nlink != 1):
        raise ValueError("verified artifact must be an exclusively linked regular file")
    if include_inode:
        return (
            st.st_dev,
            st.st_ino,
            st.st_mode,
            st.st_nlink,
            st.st_size,
            st.st_mtime_ns,
            st.st_ctime_ns,
        )
    return (
        st.st_dev,
        st.st_mode,
        st.st_nlink,
        st.st_size,
        st.st_mtime_ns,
        st.st_ctime_ns,
    )


def open_regular(path: Path) -> BinaryIO:
    """Reject links and special files on the opened fd without waiting on a FIFO."""
    descriptor = os.open(path, os.O_RDONLY | O_CLOEXEC | O_NOFOLLOW | O_NONBLOCK)
    try:
        file_identity(os.fstat(descriptor))
        return os.fdopen(descriptor, "rb")
    except BaseException:
        os.close(descriptor)
        raise


def open_regular_at(
    root_fd: int,
    relative: str,
    *,
    error_factory: Callable[[str], BaseException] | None = None,
) -> int:
    """Safely open a single-link regular file relative to root_fd without following symlinks."""
    make_error = error_factory or (lambda message: ValueError(message))
    path = PurePosixPath(relative)
    if (
        path.is_absolute()
        or not path.parts
        or any(part in {"", ".", ".."} for part in relative.split("/"))
    ):
        raise make_error(f"路径无效：{relative}")
    directory_flags = os.O_RDONLY | O_CLOEXEC | O_DIRECTORY | O_NOFOLLOW
    file_flags = os.O_RDONLY | O_CLOEXEC | O_NOFOLLOW | O_NONBLOCK
    current_fd = root_fd
    owned_directory_fd: int | None = None
    descriptor: int | None = None
    try:
        for component in path.parts[:-1]:
            next_fd = os.open(component, directory_flags, dir_fd=current_fd)
            if owned_directory_fd is not None:
                os.close(owned_directory_fd)
            owned_directory_fd = next_fd
            current_fd = next_fd
        descriptor = os.open(path.parts[-1], file_flags, dir_fd=current_fd)
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode) or (os.name == "posix" and metadata.st_nlink != 1):
            raise make_error(f"不是独占普通文件：{relative}")
        result = descriptor
        descriptor = None
        return result
    except OSError as error:
        raise make_error(f"无法安全打开 {relative}：{error}") from error
    finally:
        if descriptor is not None:
            os.close(descriptor)
        if owned_directory_fd is not None:
            os.close(owned_directory_fd)


def make_zstd_compressor(*, level: int = 1) -> zstandard.ZstdCompressor:
    """Construct a checksummed Zstandard frame compressor."""
    return zstandard.ZstdCompressor(level=level, write_checksum=True)


def decoded_metadata_stream(
    raw: BinaryIO,
    *,
    block_bytes: int = ZSTD_BLOCK_BYTES,
) -> io.BufferedReader:
    """Wrap raw stream in a bounded, frame-verified MetadataFrames reader."""
    return io.BufferedReader(MetadataFrames(raw, block_bytes))


def stream_digest(stream: BinaryIO, *, block_bytes: int = ZSTD_BLOCK_BYTES) -> tuple[int, str]:
    """Read stream completely and return (byte_count, lowercase_sha256_hex)."""
    size = 0
    digest = hashlib.sha256()
    while block := stream.read(block_bytes):
        size += len(block)
        digest.update(block)
    return size, digest.hexdigest()


def write_zstd_frame_checked(
    output: BinaryIO,
    compressor: zstandard.ZstdCompressor,
    block: bytes | bytearray,
    *,
    verify_pread: bool = False,
) -> bytes:
    """Compress one block into an independent checksummed frame and check for short writes."""
    encoded = compressor.compress(block)
    offset = output.tell() if verify_pread else 0
    if output.write(encoded) != len(encoded):
        raise OSError("short metadata compression write")
    if verify_pread:
        output.flush()
        fd = output.fileno()
        stored = bytearray()
        while len(stored) < len(encoded):
            chunk = os.pread(fd, len(encoded) - len(stored), offset + len(stored))
            if not chunk:
                raise OSError("short metadata compression readback")
            stored.extend(chunk)
        if zstandard.ZstdDecompressor().decompress(bytes(stored)) != block:
            raise ValueError("incremental metadata round-trip mismatch")
    return encoded


def compress_zstd_stream(
    source: BinaryIO,
    target: Path,
    *,
    level: int = 1,
    block_bytes: int = ZSTD_BLOCK_BYTES,
    sync_dir: bool = False,
    open_regular_fn: Callable[[Path], BinaryIO] | None = None,
) -> dict[str, object]:
    """Compress source into 1 MiB framed Zstd with round-trip verify and atomic replace."""
    temporary = target.with_name(f".{target.name}.{uuid.uuid4().hex}.tmp")
    compressor = make_zstd_compressor(level=level)
    size = 0
    digest = hashlib.sha256()
    replaced = False
    open_fn = open_regular_fn or open_regular
    try:
        with temporary.open("xb") as output:
            while block := source.read(block_bytes):
                size += len(block)
                digest.update(block)
                write_zstd_frame_checked(output, compressor, block)
            if size == 0:
                write_zstd_frame_checked(output, compressor, b"")
            output.flush()
            os.fsync(output.fileno())
        with (
            open_fn(temporary) as raw,
            decoded_metadata_stream(raw, block_bytes=block_bytes) as decoded,
        ):
            if stream_digest(decoded, block_bytes=block_bytes) != (size, digest.hexdigest()):
                raise ValueError("metadata compression round-trip mismatch")
        os.replace(temporary, target)
        replaced = True
        if sync_dir:
            fsync_directory(target.parent)
        return {
            "codec": "zstd",
            "level": level,
            "block_bytes": block_bytes,
            "uncompressed_bytes": size,
            "uncompressed_sha256": digest.hexdigest(),
        }
    finally:
        if not replaced:
            with suppress(OSError):
                temporary.unlink(missing_ok=True)

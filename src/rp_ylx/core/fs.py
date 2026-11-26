"""Filesystem durability, atomic writes, bounded reads, and hashing primitives."""

from __future__ import annotations

import ctypes
import errno
import hashlib
import json
import math
import os
import stat
import sys
import uuid
from collections.abc import Callable
from contextlib import suppress
from functools import cache
from pathlib import Path

from rp_ylx.core.ids import valid_customer_bearer_token

O_CLOEXEC = getattr(os, "O_CLOEXEC", 0)
O_DIRECTORY = getattr(os, "O_DIRECTORY", 0)
O_NOFOLLOW = getattr(os, "O_NOFOLLOW", 0)
O_NONBLOCK = getattr(os, "O_NONBLOCK", 0)

EXFAT_SUPER_MAGIC = 0x2011BAB0
MSDOS_SUPER_MAGIC = 0x4D44
FUSE_SUPER_MAGIC = 0x65735546
BTRFS_SUPER_MAGIC = 0x9123683E

EXFAT_UNSUPPORTED_ERRNOS = frozenset(
    err
    for err in (
        errno.EPERM,
        errno.EINVAL,
        getattr(errno, "EOPNOTSUPP", errno.EPERM),
        getattr(errno, "ENOTSUP", errno.EPERM),
        getattr(errno, "EXDEV", errno.EPERM),
    )
)

__all__ = [
    "BTRFS_SUPER_MAGIC",
    "EXFAT_SUPER_MAGIC",
    "EXFAT_UNSUPPORTED_ERRNOS",
    "FUSE_SUPER_MAGIC",
    "MSDOS_SUPER_MAGIC",
    "O_CLOEXEC",
    "O_DIRECTORY",
    "O_NOFOLLOW",
    "O_NONBLOCK",
    "chown_to_parent",
    "export_atomic_no_overwrite",
    "fsync_directory",
    "is_exfat_or_msdos_fd",
    "is_exfat_or_msdos_path",
    "read_bearer_token_file",
    "read_bounded_bytes",
    "read_bounded_json",
    "reject_nonfinite_constant",
    "require_regular_file",
    "sha256_file",
    "unique_object_pairs",
    "validate_json_tree",
    "write_bytes_atomic",
    "write_json_atomic",
]


@cache
def _libc_fstatfs() -> Callable[[int, object], int] | None:
    if not sys.platform.startswith("linux"):
        return None
    try:
        libc = ctypes.CDLL(None)
        fn = libc.fstatfs
    except (OSError, AttributeError):
        return None
    fn.argtypes = [ctypes.c_int, ctypes.c_void_p]
    fn.restype = ctypes.c_int
    return fn


def _fstatfs_type(fd: int) -> int | None:
    fn = _libc_fstatfs()
    if fn is None:
        return None
    buf = (ctypes.c_ulong * 32)()
    try:
        if fn(fd, ctypes.byref(buf)) != 0:
            return None
    except (OSError, ctypes.ArgumentError, TypeError):
        return None
    return int(buf[0])


def is_exfat_or_msdos_fd(fd: int) -> bool:
    """Return True when fd resides on exFAT/FAT (including exfat-fuse or mocked fstatvfs)."""
    f_type = _fstatfs_type(fd)
    if f_type in {EXFAT_SUPER_MAGIC, MSDOS_SUPER_MAGIC}:
        return True
    if f_type == BTRFS_SUPER_MAGIC:
        return False
    fstatvfs = getattr(os, "fstatvfs", None)
    if fstatvfs is None:
        return False
    try:
        return fstatvfs(fd).f_files == 0
    except OSError:
        return False


def is_exfat_or_msdos_path(path: Path) -> bool:
    """Check whether path (or its parent if missing) resides on an exFAT/FAT filesystem."""
    target = Path(path)
    for candidate in (target, target.parent):
        try:
            fd = os.open(candidate, os.O_RDONLY | O_NOFOLLOW | O_NONBLOCK | O_CLOEXEC)
        except OSError:
            continue
        try:
            return is_exfat_or_msdos_fd(fd)
        finally:
            os.close(fd)
    return False


def _write_all(fd: int, payload: bytes, path: Path) -> None:
    view = memoryview(payload)
    written = 0
    while written < len(view):
        count = os.write(fd, view[written:])
        if count <= 0:
            raise OSError(f"short write to {path}")
        written += count


def _file_identity(info: os.stat_result, *, max_bytes: int | None = None) -> tuple[int, ...]:
    if not stat.S_ISREG(info.st_mode):
        raise ValueError("path is not a regular file")
    if os.name == "posix" and info.st_nlink != 1:
        raise ValueError("regular file must not have hardlinks")
    if max_bytes is not None and info.st_size > max_bytes:
        raise ValueError(f"file exceeds maximum size of {max_bytes} bytes")
    return (
        info.st_dev,
        info.st_ino,
        info.st_mode,
        info.st_nlink,
        info.st_size,
        info.st_mtime_ns,
        info.st_ctime_ns,
    )


def _unique_object_pairs(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, item in pairs:
        if key in result:
            raise ValueError(f"duplicate JSON key: {key}")
        result[key] = item
    return result


def _reject_nonfinite_constant(token: str) -> object:
    raise ValueError(f"non-finite JSON constant: {token}")


def _validate_json_tree(value: object, *, max_depth: int) -> None:
    pending: list[tuple[object, int]] = [(value, 0)]
    while pending:
        item, depth = pending.pop()
        if depth > max_depth:
            raise ValueError(f"JSON nesting depth exceeds {max_depth}")
        if isinstance(item, dict):
            for key, child in item.items():
                if not isinstance(key, str):
                    raise ValueError("JSON object keys must be strings")
                key.encode("utf-8")
                pending.append((child, depth + 1))
        elif isinstance(item, list):
            pending.extend((child, depth + 1) for child in item)
        elif isinstance(item, str):
            item.encode("utf-8")
        elif isinstance(item, float) and not math.isfinite(item):
            raise ValueError("non-finite JSON number")


def fsync_directory(path: Path) -> None:
    """Flush directory entry metadata to disk."""
    directory = Path(path)
    fd = os.open(directory, os.O_RDONLY | O_DIRECTORY | O_CLOEXEC)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def chown_to_parent(path: Path, parent: Path | None = None) -> None:
    """When running as root on POSIX, match path ownership to its parent directory."""
    target = Path(path)
    parent_dir = target.parent if parent is None else Path(parent)
    if hasattr(os, "geteuid") and os.geteuid() == 0:
        parent_stat = parent_dir.stat()
        os.chown(target, parent_stat.st_uid, parent_stat.st_gid, follow_symlinks=False)


def write_bytes_atomic(
    path: Path,
    payload: bytes,
    *,
    mode: int | None = 0o644,
    sync_dir: bool = True,
    unique_suffix: bool = True,
) -> None:
    """Atomically write bytes to path via temp file + fchmod + fsync + replace."""
    target = Path(path)
    suffix = f".{uuid.uuid4().hex}.tmp" if unique_suffix else ".tmp"
    temporary = target.with_name(f".{target.name}{suffix}")
    replaced = False
    create_mode = 0o644 if mode is None else mode
    fd = os.open(
        temporary,
        os.O_WRONLY | os.O_CREAT | os.O_EXCL | O_NOFOLLOW | O_CLOEXEC,
        create_mode,
    )
    try:
        try:
            if mode is not None:
                try:
                    os.fchmod(fd, mode)
                except OSError as error:
                    if error.errno not in EXFAT_UNSUPPORTED_ERRNOS or not is_exfat_or_msdos_fd(fd):
                        raise
            _write_all(fd, payload, target)
            os.fsync(fd)
        finally:
            os.close(fd)
        os.replace(temporary, target)
        replaced = True
        if sync_dir:
            fsync_directory(target.parent)
    except BaseException:
        if not replaced:
            with suppress(OSError):
                temporary.unlink(missing_ok=True)
        raise


def write_json_atomic(
    path: Path,
    payload: object,
    *,
    mode: int | None = 0o644,
    sort_keys: bool = False,
    indent: int | None = None,
) -> None:
    """Serialize payload as UTF-8 JSON and write it atomically."""
    separators = (",", ":") if indent is None else None
    rendered = json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=sort_keys,
        indent=indent,
        separators=separators,
        allow_nan=False,
    )
    write_bytes_atomic(path, (rendered + "\n").encode("utf-8"), mode=mode)


def _close_fds(*fds: int | None) -> None:
    primary = sys.exc_info()[1]
    cleanup_error: BaseException | None = None
    for fd in fds:
        if fd is not None:
            try:
                os.close(fd)
            except BaseException as error:
                if cleanup_error is None:
                    cleanup_error = error
    if primary is None and cleanup_error is not None:
        raise cleanup_error


def export_atomic_no_overwrite(
    path: Path,
    payload: bytes | None = None,
    *,
    mode: int = 0o600,
    temp_prefix: str | None = None,
    temp_name: str | None = None,
    writer: Callable[[int], None] | None = None,
    error_factory: Callable[[int, str, str], BaseException] | None = None,
) -> None:
    """Publish payload to path without overwriting any existing entry, supporting exFAT."""
    if (payload is None) == (writer is None):
        raise ValueError("exactly one of payload or writer must be provided")
    target = Path(path)
    parent_fd = os.open(target.parent, os.O_RDONLY | O_DIRECTORY | O_CLOEXEC)
    fd: int | None = None
    temporary: str | None = None
    staging_identity: tuple[int, int] | None = None

    def make_error(err: int, message: str, *, permission: bool = False) -> BaseException:
        if error_factory is not None:
            return error_factory(err, message, str(target))
        if permission:
            return PermissionError(err, message, str(target))
        return OSError(err, message, str(target))

    def same_staging() -> bool:
        if temporary is None or staging_identity is None:
            return False
        try:
            info = os.stat(temporary, dir_fd=parent_fd, follow_symlinks=False)
        except FileNotFoundError:
            return False
        return (info.st_dev, info.st_ino) == staging_identity

    def cleanup_staging() -> None:
        if same_staging():
            assert temporary is not None
            os.unlink(temporary, dir_fd=parent_fd)

    def check_parent() -> None:
        opened = os.fstat(parent_fd)
        current = target.parent.stat()
        if (opened.st_dev, opened.st_ino) != (current.st_dev, current.st_ino):
            raise make_error(errno.ESTALE, "export parent directory changed during write")

    try:
        try:
            os.stat(target.name, dir_fd=parent_fd, follow_symlinks=False)
        except FileNotFoundError:
            pass
        else:
            raise FileExistsError(errno.EEXIST, "export destination already exists", str(target))

        if temp_name is not None:
            temporary = temp_name
        else:
            prefix = f".{target.name}." if temp_prefix is None else temp_prefix
            temporary = f"{prefix}{uuid.uuid4().hex}.tmp"
        fd = os.open(
            temporary,
            os.O_RDWR | os.O_CREAT | os.O_EXCL | O_NOFOLLOW | O_CLOEXEC,
            mode,
            dir_fd=parent_fd,
        )
        info = os.fstat(fd)
        staging_identity = (info.st_dev, info.st_ino)

        chmod_supported = True
        try:
            os.fchmod(fd, mode)
        except OSError as error:
            if error.errno not in {
                errno.EPERM,
                getattr(errno, "EOPNOTSUPP", errno.EPERM),
                getattr(errno, "ENOTSUP", errno.EPERM),
            }:
                raise
            chmod_supported = False
        if chmod_supported and (os.fstat(fd).st_mode & 0o777) != mode:
            raise make_error(
                errno.EPERM,
                "export filesystem did not preserve permissions",
                permission=True,
            )

        if writer is not None:
            writer(fd)
        else:
            assert payload is not None
            _write_all(fd, payload, target)
            os.fsync(fd)
        check_parent()
        if not same_staging():
            raise make_error(errno.ESTALE, "export staging file changed during write")

        published_identity = staging_identity
        try:
            os.link(
                f"/proc/self/fd/{fd}",
                target.name,
                dst_dir_fd=parent_fd,
                follow_symlinks=True,
            )
        except OSError as error:
            if error.errno not in EXFAT_UNSUPPORTED_ERRNOS:
                raise
            # exFAT/FAT mounts do not support hardlinks; create destination exclusively.
            dest_fd = os.open(
                target.name,
                os.O_WRONLY | os.O_CREAT | os.O_EXCL | O_NOFOLLOW | O_CLOEXEC,
                mode,
                dir_fd=parent_fd,
            )
            dest_written = False
            try:
                if payload is not None:
                    _write_all(dest_fd, payload, target)
                else:
                    os.lseek(fd, 0, os.SEEK_SET)
                    while True:
                        chunk = os.read(fd, 64 * 1024)
                        if not chunk:
                            break
                        _write_all(dest_fd, chunk, target)
                os.fsync(dest_fd)
                dest_written = True
                dest_info = os.fstat(dest_fd)
                published_identity = (dest_info.st_dev, dest_info.st_ino)
            finally:
                try:
                    os.close(dest_fd)
                finally:
                    if not dest_written:
                        with suppress(OSError):
                            os.unlink(target.name, dir_fd=parent_fd)

        cleanup_staging()
        temporary = None
        os.fsync(parent_fd)
        check_parent()
        published = os.stat(target.name, dir_fd=parent_fd, follow_symlinks=False)
        if (published.st_dev, published.st_ino) != published_identity:
            raise make_error(errno.ESTALE, "export destination changed during publication")
    except BaseException:
        with suppress(OSError):
            cleanup_staging()
        raise
    finally:
        _close_fds(fd, parent_fd)


def sha256_file(path: Path, *, chunk_size: int = 1024 * 1024) -> str:
    """Compute lowercase SHA-256 hex digest of a non-symlink, single-link regular file."""
    if type(chunk_size) is not int or chunk_size <= 0:
        raise ValueError("chunk_size must be a positive integer")
    target = Path(path)
    fd = os.open(target, os.O_RDONLY | O_NONBLOCK | O_NOFOLLOW | O_CLOEXEC)
    try:
        info = os.fstat(fd)
        _file_identity(info)
        digest = hashlib.sha256()
        while True:
            chunk = os.read(fd, chunk_size)
            if not chunk:
                break
            digest.update(chunk)
        return digest.hexdigest()
    finally:
        os.close(fd)


def require_regular_file(path: Path, *, allow_hardlinks: bool = False) -> os.stat_result:
    """Verify path is a regular file (not symlink) and return its lstat metadata."""
    target = Path(path)
    info = target.lstat()
    if not stat.S_ISREG(info.st_mode):
        raise ValueError(f"path is not a regular file: {target}")
    if not allow_hardlinks and os.name == "posix" and info.st_nlink != 1:
        raise ValueError(f"regular file must not have hardlinks: {target}")
    return info


def read_bounded_bytes(path: Path, *, max_bytes: int = 256 * 1024) -> bytes:
    """TOCTOU-safe bounded regular-file reader rejecting symlinks and hardlinks."""
    if type(max_bytes) is not int or max_bytes <= 0:
        raise ValueError("max_bytes must be a positive integer")

    target = Path(path)
    directory: int | None = None
    descriptor: int | None = None
    try:
        directory = os.open(target.parent, os.O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC)
        parent = os.fstat(directory)
        expected = _file_identity(
            os.stat(target.name, dir_fd=directory, follow_symlinks=False),
            max_bytes=max_bytes,
        )
        descriptor = os.open(
            target.name,
            os.O_RDONLY | O_NOFOLLOW | O_NONBLOCK | O_CLOEXEC,
            dir_fd=directory,
        )
        if _file_identity(os.fstat(descriptor), max_bytes=max_bytes) != expected:
            raise ValueError("file changed before read")
        raw = bytearray()
        while len(raw) <= max_bytes:
            chunk = os.read(descriptor, min(8192, max_bytes + 1 - len(raw)))
            if not chunk:
                break
            raw.extend(chunk)
        if (
            len(raw) > max_bytes
            or _file_identity(os.fstat(descriptor), max_bytes=max_bytes) != expected
        ):
            raise ValueError("file exceeded size limit or changed during read")
        if (
            _file_identity(
                os.stat(target.name, dir_fd=directory, follow_symlinks=False),
                max_bytes=max_bytes,
            )
            != expected
        ):
            raise ValueError("file replaced during read")
        after = target.parent.lstat()
        if not stat.S_ISDIR(after.st_mode) or (after.st_dev, after.st_ino) != (
            parent.st_dev,
            parent.st_ino,
        ):
            raise ValueError("parent directory replaced during read")
    finally:
        _close_fds(descriptor, directory)
    return bytes(raw)


def read_bounded_json(
    path: Path,
    *,
    max_bytes: int = 256 * 1024,
    max_depth: int = 64,
    reject_duplicate_keys: bool = True,
) -> object:
    """TOCTOU-safe bounded JSON reader rejecting symlinks, hardlinks, NaN, and deep nesting."""
    if type(max_depth) is not int or max_depth < 0:
        raise ValueError("max_depth must be a non-negative integer")
    raw = read_bounded_bytes(path, max_bytes=max_bytes)
    try:
        value = json.loads(
            raw.decode("utf-8"),
            object_pairs_hook=_unique_object_pairs if reject_duplicate_keys else None,
            parse_constant=_reject_nonfinite_constant,
        )
    except RecursionError as error:
        raise ValueError("JSON nesting depth exceeds parser limit") from error
    _validate_json_tree(value, max_depth=max_depth)
    return value


reject_nonfinite_constant = _reject_nonfinite_constant
unique_object_pairs = _unique_object_pairs
validate_json_tree = _validate_json_tree


def read_bearer_token_file(path: Path, *, max_bytes: int = 4096) -> str:
    """Safely read and validate a customer bearer token file (rejects mode bits 0o027)."""
    if type(max_bytes) is not int or max_bytes <= 0:
        raise ValueError("max_bytes must be a positive integer")
    target = Path(path)
    try:
        descriptor = os.open(target, os.O_RDONLY | O_NONBLOCK | O_CLOEXEC | O_NOFOLLOW)
        try:
            metadata = os.fstat(descriptor)
            if not stat.S_ISREG(metadata.st_mode):
                raise ValueError("Bearer token file must be a regular file and not a symlink")
            if os.name == "posix" and metadata.st_nlink != 1:
                raise ValueError("Bearer token file must not have hardlinks")
            if metadata.st_mode & 0o027:
                raise ValueError("Bearer token 文件权限必须禁止组写入和其他用户访问")
            chunks = bytearray()
            while len(chunks) <= max_bytes:
                chunk = os.read(descriptor, max_bytes + 1 - len(chunks))
                if not chunk:
                    break
                chunks.extend(chunk)
            raw = bytes(chunks)
        finally:
            os.close(descriptor)
    except ValueError:
        raise
    except OSError as error:
        raise ValueError("unable to read Bearer token file") from error
    if len(raw) > max_bytes:
        raise ValueError("Bearer token file exceeds maximum size")
    try:
        token = raw.rstrip(b"\r\n").decode("ascii")
    except UnicodeDecodeError as error:
        raise ValueError("Bearer token must be ASCII") from error
    if not valid_customer_bearer_token(token) or raw.rstrip(b"\r\n") != raw.removesuffix(
        b"\n"
    ).removesuffix(b"\r"):
        raise ValueError("Bearer token format is invalid")
    return token

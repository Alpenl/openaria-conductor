"""Bundle packaging helpers: manifest generation, SHA256SUMS, and runtime normalization."""

from __future__ import annotations

import os
import posixpath
import tarfile
import uuid
from collections.abc import Mapping
from pathlib import Path, PurePosixPath

from .bundle import _inspect_runtime, _wheel_identity, load_bundle
from .schema import (
    BUNDLE_SCHEMA,
    MAX_RUNTIME_FILES,
    MAX_UNPACKED_BYTES,
    TARGET_PLATFORM,
    DeploymentError,
    _fsync_directory,
    _sha256,
    _write_bytes_atomic,
    facade_attr,
)


def write_bundle_sha256sums(
    directory: str | Path,
    *,
    filename: str = "SHA256SUMS",
) -> Mapping[str, object]:
    root = Path(directory).resolve()
    if (
        PurePosixPath(filename).name != filename
        or "\n" in filename
        or "\r" in filename
        or not filename
    ):
        raise DeploymentError("bundle_invalid", "SHA256SUMS 文件名无效")
    entries: list[dict[str, str]] = []
    lines: list[str] = []
    for path in sorted(root.iterdir(), key=lambda candidate: candidate.name):
        if path.name == filename:
            continue
        if path.is_symlink() or not path.is_file():
            raise DeploymentError("bundle_invalid", f"bundle 只能包含顶层普通文件：{path.name}")
        if "\n" in path.name or "\r" in path.name or PurePosixPath(path.name).name != path.name:
            raise DeploymentError("bundle_invalid", f"bundle 文件名无效：{path.name}")
        digest = _sha256(path)
        entries.append({"file": path.name, "sha256": digest})
        lines.append(f"{digest}  {path.name}\n")
    if not entries:
        raise DeploymentError("bundle_invalid", "bundle 没有可校验文件")
    target = root / filename
    _write_bytes_atomic(target, "".join(lines).encode("ascii"), 0o644)
    return {
        "file": filename,
        "bytes": target.stat().st_size,
        "sha256": _sha256(target),
        "entries": entries,
    }


def write_bundle_manifest(
    directory: str | Path,
    *,
    application_wheel: str,
    version: str,
) -> Mapping[str, object]:
    root = Path(directory).resolve()
    app = root / application_wheel
    commit, wheel_version = _wheel_identity(app)
    if wheel_version != version:
        raise DeploymentError("bundle_invalid", "指定版本与 application wheel 不一致")
    wheels = sorted(root.glob("*.whl"), key=lambda path: path.name)
    if app not in wheels:
        raise DeploymentError("bundle_invalid", "application wheel 不存在")
    installer = root / "rdk_x5_install.py"
    if not installer.is_file():
        raise DeploymentError("bundle_invalid", "缺少首次安装脚本")
    runtimes = sorted(root.glob("cpython-3.11.*-aarch64.tar.gz"))
    if len(runtimes) != 1:
        raise DeploymentError("bundle_invalid", "必须包含唯一 CPython 3.11 aarch64 runtime")
    runtime = runtimes[0]
    runtime_version = runtime.name.removeprefix("cpython-").removesuffix("-aarch64.tar.gz")
    _inspect_runtime(runtime)
    value: dict[str, object] = {
        "schema": BUNDLE_SCHEMA,
        "platform": TARGET_PLATFORM,
        "commit": commit,
        "version": version,
        "application_wheel": application_wheel,
        "installer": {
            "file": installer.name,
            "bytes": installer.stat().st_size,
            "sha256": _sha256(installer),
        },
        "runtime": {
            "file": runtime.name,
            "bytes": runtime.stat().st_size,
            "sha256": _sha256(runtime),
            "implementation": "cpython",
            "python_version": runtime_version,
            "platform": "linux_aarch64",
            "executable": "runtime/bin/python3",
        },
        "wheels": [
            {"file": wheel.name, "bytes": wheel.stat().st_size, "sha256": _sha256(wheel)}
            for wheel in wheels
        ],
    }
    writer = facade_attr("_write_atomic", None)
    if writer is None:
        from .schema import _write_atomic

        writer = _write_atomic
    writer(root / "bundle.json", value)
    load_bundle(root)
    return value


def normalize_runtime_archive(source_path: Path, output_path: Path) -> None:
    """Convert a verified standalone CPython archive into the no-link bundle format."""
    try:
        with tarfile.open(source_path, "r:gz") as source:
            source_members = source.getmembers()
            if not source_members or len(source_members) > MAX_RUNTIME_FILES:
                raise DeploymentError("runtime_unsafe", "runtime 文件数量无效")
            by_name: dict[str, tarfile.TarInfo] = {}
            roots: set[str] = set()
            unpacked_bytes = 0
            for member in source_members:
                path = PurePosixPath(member.name)
                raw_parts = member.name.split("/")
                unpacked_bytes += member.size
                if (
                    path.is_absolute()
                    or len(path.parts) < 2
                    or path.parts[0] not in {"python", "runtime"}
                    or any(part in {"", ".", ".."} for part in raw_parts)
                    or member.name in by_name
                    or member.islnk()
                    or not (member.isdir() or member.isfile() or member.issym())
                    or unpacked_bytes > MAX_UNPACKED_BYTES
                ):
                    raise DeploymentError("runtime_unsafe", f"runtime 源成员不安全：{member.name}")
                roots.add(path.parts[0])
                by_name[member.name] = member
            if len(roots) != 1:
                raise DeploymentError("runtime_unsafe", "runtime 源归档根目录不唯一")
            source_root = next(iter(roots))

            def resolve(member: tarfile.TarInfo) -> tarfile.TarInfo:
                seen: set[str] = set()
                current = member
                while current.issym():
                    if current.name in seen or PurePosixPath(current.linkname).is_absolute():
                        raise DeploymentError(
                            "runtime_unsafe", f"runtime 链接不安全：{member.name}"
                        )
                    seen.add(current.name)
                    target_name = posixpath.normpath(
                        posixpath.join(posixpath.dirname(current.name), current.linkname)
                    )
                    target = PurePosixPath(target_name)
                    if (
                        not target.parts
                        or target.parts[0] != source_root
                        or target_name not in by_name
                    ):
                        raise DeploymentError(
                            "runtime_unsafe", f"runtime 链接越界或缺失：{member.name}"
                        )
                    current = by_name[target_name]
                if not current.isfile():
                    raise DeploymentError(
                        "runtime_unsafe", f"runtime 链接未指向普通文件：{member.name}"
                    )
                return current

            temporary = output_path.with_name(f".{output_path.name}.{uuid.uuid4().hex}.tmp")
            try:
                with tarfile.open(temporary, "w:gz", format=tarfile.PAX_FORMAT) as output:
                    for member in source_members:
                        path = PurePosixPath(member.name)
                        normalized_name = str(PurePosixPath("runtime", *path.parts[1:]))
                        if member.isdir():
                            normalized = tarfile.TarInfo(normalized_name)
                            normalized.type = tarfile.DIRTYPE
                            normalized.mode = 0o755
                            output.addfile(normalized)
                            continue
                        resolved = resolve(member)
                        stream = source.extractfile(resolved)
                        if stream is None:
                            raise DeploymentError(
                                "runtime_unsafe", f"runtime 成员不可读：{member.name}"
                            )
                        normalized = tarfile.TarInfo(normalized_name)
                        normalized.size = resolved.size
                        normalized.mode = 0o755 if resolved.mode & 0o111 else 0o644
                        with stream:
                            output.addfile(normalized, stream)
                _inspect_runtime(temporary)
                os.replace(temporary, output_path)
                _fsync_directory(output_path.parent)
            finally:
                temporary.unlink(missing_ok=True)
    except (OSError, tarfile.TarError) as error:
        raise DeploymentError(
            "bundle_invalid", f"无法规范化 package-owned runtime：{error}"
        ) from error

"""PEP 517 adapter that binds maturin distributions to one source commit."""

from __future__ import annotations

import ast
import base64
import copy
import gzip
import hashlib
import io
import os
import re
import subprocess
import tarfile
import tempfile
import zipfile
from collections.abc import Mapping
from pathlib import Path, PurePosixPath
from typing import Any

import maturin

ROOT = Path(__file__).resolve().parent
BUILD_INFO = ROOT / "src/rp_ylx/_build_info.py"
COMMIT_PATTERN = re.compile(r"[0-9a-f]{40}")
WHEEL_BUILD_INFO_PATH = "rp_ylx/_build_info.py"


def _source_commit() -> str:
    try:
        tree = ast.parse(BUILD_INFO.read_text(encoding="utf-8"))
    except (OSError, SyntaxError):
        return "unknown"
    for statement in tree.body:
        if not isinstance(statement, ast.Assign) or len(statement.targets) != 1:
            continue
        target = statement.targets[0]
        if (
            isinstance(target, ast.Name)
            and target.id == "__commit__"
            and isinstance(statement.value, ast.Constant)
            and isinstance(statement.value.value, str)
        ):
            return statement.value.value
    return "unknown"


def _repository_commit() -> str:
    try:
        top_level = subprocess.run(
            ["git", "rev-parse", "--show-toplevel"],
            cwd=ROOT,
            check=True,
            capture_output=True,
            text=True,
            timeout=5,
        ).stdout.strip()
        commit = subprocess.run(
            ["git", "rev-parse", "--verify", "HEAD"],
            cwd=ROOT,
            check=True,
            capture_output=True,
            text=True,
            timeout=5,
        ).stdout.strip()
    except (OSError, subprocess.SubprocessError):
        return _source_commit()
    if Path(top_level).resolve() != ROOT or COMMIT_PATTERN.fullmatch(commit) is None:
        return _source_commit()
    return commit


def _render_build_info(commit: str) -> str:
    return (
        '"""Build identity generated for distributions; source checkouts use the fallback."""\n\n'
        f'__commit__ = "{commit}"\n'
    )


def _copy_zip_info(info: zipfile.ZipInfo) -> zipfile.ZipInfo:
    copied = zipfile.ZipInfo(filename=info.filename, date_time=info.date_time)
    copied.compress_type = info.compress_type
    copied.comment = info.comment
    copied.extra = info.extra
    copied.create_system = info.create_system
    copied.create_version = info.create_version
    copied.extract_version = info.extract_version
    copied.flag_bits = info.flag_bits
    copied.volume = info.volume
    copied.internal_attr = info.internal_attr
    copied.external_attr = info.external_attr
    return copied


def _record_entry(path: str, payload: bytes) -> str:
    digest = base64.urlsafe_b64encode(hashlib.sha256(payload).digest()).rstrip(b"=").decode("ascii")
    return f"{path},sha256={digest},{len(payload)}"


def _stamp_wheel(wheel_path: Path, commit: str) -> None:
    new_build_info = _render_build_info(commit).encode("utf-8")
    new_record_line = _record_entry(WHEEL_BUILD_INFO_PATH, new_build_info)
    with zipfile.ZipFile(wheel_path, "r") as src_zip:
        entries = src_zip.infolist()
        names = [info.filename for info in entries]
        if names.count(WHEEL_BUILD_INFO_PATH) != 1:
            raise RuntimeError(f"Generated wheel is missing unique {WHEEL_BUILD_INFO_PATH}")
        record_names = [
            name for name in names if name.endswith(".dist-info/RECORD") and name.count("/") == 1
        ]
        if len(record_names) != 1:
            raise RuntimeError("Generated wheel is missing unique .dist-info/RECORD")
        record_name = record_names[0]
        record_text = src_zip.read(record_name).decode("utf-8")
        updated_lines: list[str] = []
        found_entry = False
        for line in record_text.splitlines():
            if not line:
                continue
            if line.split(",", 1)[0] == WHEEL_BUILD_INFO_PATH:
                updated_lines.append(new_record_line)
                found_entry = True
            else:
                updated_lines.append(line)
        if not found_entry:
            updated_lines.append(new_record_line)
        new_record_bytes = ("\n".join(updated_lines) + "\n").encode("utf-8")

        fd, tmp_name = tempfile.mkstemp(dir=wheel_path.parent, suffix=".whl")
        tmp_path = Path(tmp_name)
        try:
            with os.fdopen(fd, "wb") as tmp_raw, zipfile.ZipFile(tmp_raw, "w") as dst_zip:
                for info in entries:
                    if info.filename == WHEEL_BUILD_INFO_PATH:
                        dst_zip.writestr(_copy_zip_info(info), new_build_info)
                    elif info.filename == record_name:
                        dst_zip.writestr(_copy_zip_info(info), new_record_bytes)
                    else:
                        dst_zip.writestr(_copy_zip_info(info), src_zip.read(info.filename))
        except BaseException:
            tmp_path.unlink(missing_ok=True)
            raise
    os.replace(tmp_path, wheel_path)


def _stamp_sdist(sdist_path: Path, commit: str) -> None:
    new_build_info = _render_build_info(commit).encode("utf-8")
    with tarfile.open(sdist_path, "r:gz") as src_tar:
        fd, tmp_name = tempfile.mkstemp(dir=sdist_path.parent, suffix=".tar.gz")
        tmp_path = Path(tmp_name)
        try:
            with (
                os.fdopen(fd, "wb") as tmp_raw,
                gzip.GzipFile(fileobj=tmp_raw, mode="wb", mtime=0, filename="") as gz,
                tarfile.open(fileobj=gz, mode="w", format=src_tar.format) as dst_tar,
            ):
                replaced = False
                for member in src_tar.getmembers():
                    parts = PurePosixPath(member.name).parts
                    if len(parts) == 4 and parts[1:] == ("src", "rp_ylx", "_build_info.py"):
                        copied = copy.copy(member)
                        copied.size = len(new_build_info)
                        dst_tar.addfile(copied, io.BytesIO(new_build_info))
                        replaced = True
                    elif member.isfile():
                        payload = src_tar.extractfile(member)
                        if payload is None:
                            raise RuntimeError(f"Unreadable sdist member: {member.name}")
                        with payload:
                            dst_tar.addfile(copy.copy(member), payload)
                    else:
                        dst_tar.addfile(copy.copy(member))
                if not replaced:
                    raise RuntimeError("Generated sdist is missing src/rp_ylx/_build_info.py")
        except BaseException:
            tmp_path.unlink(missing_ok=True)
            raise
    os.replace(tmp_path, sdist_path)


def build_wheel(
    wheel_directory: str,
    config_settings: Mapping[str, Any] | None = None,
    metadata_directory: str | None = None,
) -> str:
    commit = _repository_commit()
    wheel_name = maturin.build_wheel(wheel_directory, config_settings, metadata_directory)
    _stamp_wheel(Path(wheel_directory) / wheel_name, commit)
    return wheel_name


def build_sdist(
    sdist_directory: str,
    config_settings: Mapping[str, Any] | None = None,
) -> str:
    commit = _repository_commit()
    sdist_name = maturin.build_sdist(sdist_directory, config_settings)
    _stamp_sdist(Path(sdist_directory) / sdist_name, commit)
    return sdist_name


def build_editable(
    wheel_directory: str,
    config_settings: Mapping[str, Any] | None = None,
    metadata_directory: str | None = None,
) -> str:
    return maturin.build_editable(wheel_directory, config_settings, metadata_directory)


def prepare_metadata_for_build_wheel(
    metadata_directory: str,
    config_settings: Mapping[str, Any] | None = None,
) -> str:
    return maturin.prepare_metadata_for_build_wheel(metadata_directory, config_settings)


def prepare_metadata_for_build_editable(
    metadata_directory: str,
    config_settings: Mapping[str, Any] | None = None,
) -> str:
    return maturin.prepare_metadata_for_build_editable(metadata_directory, config_settings)


def get_requires_for_build_wheel(
    config_settings: Mapping[str, Any] | None = None,
) -> list[str]:
    return maturin.get_requires_for_build_wheel(config_settings)


def get_requires_for_build_sdist(
    config_settings: Mapping[str, Any] | None = None,
) -> list[str]:
    return maturin.get_requires_for_build_sdist(config_settings)


def get_requires_for_build_editable(
    config_settings: Mapping[str, Any] | None = None,
) -> list[str]:
    return maturin.get_requires_for_build_editable(config_settings)

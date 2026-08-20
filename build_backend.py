"""PEP 517 adapter that binds maturin distributions to one source commit."""

from __future__ import annotations

import ast
import fcntl
import re
import subprocess
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import maturin

ROOT = Path(__file__).resolve().parent
BUILD_INFO = ROOT / "src/rp_ylx/_build_info.py"
LOCK_FILE = ROOT / "target/build-backend.lock"
COMMIT_PATTERN = re.compile(r"[0-9a-f]{40}")


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


@contextmanager
def _bound_source() -> Iterator[None]:
    LOCK_FILE.parent.mkdir(exist_ok=True)
    with LOCK_FILE.open("a+b") as lock_file:
        fcntl.flock(lock_file, fcntl.LOCK_EX)
        original = BUILD_INFO.read_bytes()
        try:
            BUILD_INFO.write_text(_render_build_info(_repository_commit()), encoding="utf-8")
            yield
        finally:
            BUILD_INFO.write_bytes(original)
            fcntl.flock(lock_file, fcntl.LOCK_UN)


def build_wheel(
    wheel_directory: str,
    config_settings: Mapping[str, Any] | None = None,
    metadata_directory: str | None = None,
) -> str:
    with _bound_source():
        return maturin.build_wheel(wheel_directory, config_settings, metadata_directory)


def build_sdist(
    sdist_directory: str,
    config_settings: Mapping[str, Any] | None = None,
) -> str:
    with _bound_source():
        return maturin.build_sdist(sdist_directory, config_settings)


def build_editable(
    wheel_directory: str,
    config_settings: Mapping[str, Any] | None = None,
    metadata_directory: str | None = None,
) -> str:
    with _bound_source():
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

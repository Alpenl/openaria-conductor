from __future__ import annotations

import ast
import re
import subprocess
from pathlib import Path

from setuptools import setup
from setuptools.command.build_py import build_py as _build_py
from setuptools.command.sdist import sdist as _sdist

ROOT = Path(__file__).resolve().parent
BUILD_INFO = Path("src/rp_ylx/_build_info.py")
COMMIT_PATTERN = re.compile(r"[0-9a-f]{40}")


def _source_commit() -> str:
    try:
        tree = ast.parse((ROOT / BUILD_INFO).read_text(encoding="utf-8"))
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


class build_py(_build_py):
    def run(self) -> None:
        super().run()
        destination = Path(self.build_lib) / "rp_ylx/_build_info.py"
        destination.write_text(_render_build_info(_repository_commit()), encoding="utf-8")


class sdist(_sdist):
    def make_release_tree(self, base_dir: str, files: list[str]) -> None:
        super().make_release_tree(base_dir, files)
        destination = Path(base_dir) / BUILD_INFO
        destination.write_text(_render_build_info(_repository_commit()), encoding="utf-8")


setup(cmdclass={"build_py": build_py, "sdist": sdist})

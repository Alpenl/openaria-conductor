"""本地和 CI 共用的无硬件检查入口。"""

from __future__ import annotations

import os
import subprocess
import sys


def run(*command: str) -> None:
    subprocess.run(command, check=True, env={**os.environ, "PYTHONPATH": "src"})


def main() -> int:
    run(sys.executable, "-m", "compileall", "-q", "src", "tests")
    run(sys.executable, "-m", "unittest", "discover", "-s", "tests", "-v")
    try:
        run("ruff", "check", ".")
        run("ruff", "format", "--check", ".")
    except FileNotFoundError:
        print("未安装 ruff，已跳过静态检查；请使用 `uv sync --extra dev` 安装。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

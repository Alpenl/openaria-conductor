from __future__ import annotations

import argparse
import json
import re
import shutil
import subprocess
from pathlib import Path

from rp_ylx.deployment import normalize_runtime_archive, write_bundle_manifest

RUNTIME_NAME = re.compile(
    r"^cpython-(3\.11\.\d+)\+\d+-aarch64-unknown-linux-gnu-"
    r"install_only(?:_stripped)?\.tar\.gz$"
)


def main() -> None:
    parser = argparse.ArgumentParser(description="生成 RDK X5 CPython 3.11 aarch64 离线安装 bundle")
    parser.add_argument("--application-wheel", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--version", required=True)
    parser.add_argument(
        "--runtime",
        required=True,
        type=Path,
        help="python-build-standalone CPython 3.11 aarch64 tar.gz",
    )
    args = parser.parse_args()
    application_wheel = args.application_wheel.resolve()
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=True)
    if any(output.iterdir()):
        parser.error("output 必须为空，避免旧依赖混入候选 bundle")
    command = [
        "uvx",
        "--python",
        "3.11",
        "--from",
        "pip",
        "pip",
        "download",
        "--dest",
        str(output),
        "--only-binary=:all:",
        "--platform",
        "manylinux_2_28_aarch64",
        "--platform",
        "manylinux2014_aarch64",
        "--python-version",
        "311",
        "--implementation",
        "cp",
        "--abi",
        "cp311",
        str(application_wheel),
    ]
    subprocess.run(command, check=True, timeout=600)
    installer_source = Path(__file__).with_name("rdk_x5_install.py")
    shutil.copy2(installer_source, output / installer_source.name)
    runtime = args.runtime.resolve()
    runtime_match = RUNTIME_NAME.fullmatch(runtime.name)
    if runtime_match is None:
        parser.error("runtime 必须是 CPython 3.11 aarch64 unknown-linux-gnu install_only tar.gz")
    normalized_runtime = output / f"cpython-{runtime_match.group(1)}-aarch64.tar.gz"
    normalize_runtime_archive(runtime, normalized_runtime)
    result = write_bundle_manifest(
        output,
        application_wheel=application_wheel.name,
        version=args.version,
    )
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))


if __name__ == "__main__":
    main()

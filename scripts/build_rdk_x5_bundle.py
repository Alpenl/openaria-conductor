from __future__ import annotations

import argparse
import json
import re
import shutil
import subprocess
import tempfile
from pathlib import Path

from rp_ylx.deployment import (
    normalize_runtime_archive,
    write_bundle_manifest,
    write_bundle_sha256sums,
)

ROOT = Path(__file__).resolve().parents[1]

RUNTIME_NAME = re.compile(
    r"^cpython-(3\.11\.\d+)\+\d+-aarch64-unknown-linux-gnu-"
    r"install_only(?:_stripped)?\.tar\.gz$"
)


def _uv_export_target_flags() -> list[str]:
    help_text = subprocess.run(
        ["uv", "export", "--help"],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
        timeout=30,
    ).stdout
    if "--python-platform" in help_text and "--python-version" in help_text:
        return ["--python-platform", "aarch64-unknown-linux-gnu", "--python-version", "3.11"]
    return ["--python", "3.11"]


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
    parser.add_argument(
        "--build-info",
        help="optional build-info.txt content to include before SHA256SUMS is generated",
    )
    args = parser.parse_args()
    application_wheel = args.application_wheel.resolve()
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=True)
    if any(output.iterdir()):
        parser.error("output 必须为空，避免旧依赖混入候选 bundle")
    shutil.copy2(application_wheel, output / application_wheel.name)
    with tempfile.TemporaryDirectory() as temporary:
        requirements = Path(temporary) / "requirements.txt"
        exported = subprocess.run(
            [
                "uv",
                "export",
                "--frozen",
                "--no-dev",
                "--no-emit-project",
                *_uv_export_target_flags(),
                "--format",
                "requirements.txt",
            ],
            cwd=ROOT,
            check=True,
            capture_output=True,
            text=True,
            timeout=120,
        )
        requirements.write_text(exported.stdout, encoding="utf-8")
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
            "--require-hashes",
            "--no-deps",
            "-r",
            str(requirements),
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
    if args.build_info is not None:
        if "\n" in args.build_info or "\r" in args.build_info:
            parser.error("build-info 不能包含换行")
        (output / "build-info.txt").write_text(f"{args.build_info}\n", encoding="utf-8")
    manifest = write_bundle_manifest(
        output,
        application_wheel=application_wheel.name,
        version=args.version,
    )
    sha256sums = write_bundle_sha256sums(output)
    print(
        json.dumps(
            {"manifest": manifest, "sha256sums": sha256sums},
            ensure_ascii=False,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()

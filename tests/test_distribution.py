from __future__ import annotations

import hashlib
import json
import os
import shutil
import subprocess
import sys
import tempfile
import tomllib
import unittest
from pathlib import Path

from rp_ylx.web import ECHO_WEB_SOURCE_COMMIT

REPOSITORY = Path(__file__).resolve().parents[1]


class InstalledWheelTest(unittest.TestCase):
    def test_external_install_reports_the_exact_packaged_commit(self) -> None:
        uv = shutil.which("uv")
        self.assertIsNotNone(uv, "wheel black-box test requires uv")
        expected_commit = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=REPOSITORY,
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
        source_build_info = (REPOSITORY / "src/rp_ylx/_build_info.py").read_bytes()
        package_root = REPOSITORY / "src/rp_ylx"
        expected_version = tomllib.loads((REPOSITORY / "pyproject.toml").read_text())["project"][
            "version"
        ]
        web_manifest = json.loads((package_root / "web/assets.json").read_text())
        expected_web = {
            "source_commit": ECHO_WEB_SOURCE_COMMIT,
            "required_device_api_major": web_manifest["compatibility"]["device_api"][
                "required_major"
            ],
            "assets": {
                entry["path"]: {key: entry[key] for key in ("bytes", "content_type", "sha256")}
                for entry in web_manifest["files"]
            },
        }
        resource_paths = [
            REPOSITORY / relative
            for relative in subprocess.run(
                ["git", "ls-files", "src/rp_ylx"],
                cwd=REPOSITORY,
                check=True,
                capture_output=True,
                text=True,
            ).stdout.splitlines()
            if Path(relative).suffix != ".py"
        ]
        expected_resources = {
            path.relative_to(package_root).as_posix(): hashlib.sha256(path.read_bytes()).hexdigest()
            for path in resource_paths
        }

        environment = os.environ.copy()
        for name in ("PYTHONPATH", "RP_YLX_COMMIT", "UV_PROJECT_ENVIRONMENT", "VIRTUAL_ENV"):
            environment.pop(name, None)
        if cargo_target_dir := os.environ.get("CARGO_TARGET_DIR"):
            environment["CARGO_TARGET_DIR"] = cargo_target_dir

        with tempfile.TemporaryDirectory() as directory:
            external_root = Path(directory).resolve()
            self.assertNotIn(REPOSITORY.resolve(), external_root.parents)
            direct_distributions = external_root / "direct"
            subprocess.run(
                [
                    uv,
                    "build",
                    "--wheel",
                    "--out-dir",
                    str(direct_distributions),
                    "--no-build-logs",
                    str(REPOSITORY),
                ],
                cwd=external_root,
                env=environment,
                check=True,
                capture_output=True,
                text=True,
            )
            direct_wheel = next(direct_distributions.glob("rp_ylx-*.whl"))

            source_distributions = external_root / "source"
            subprocess.run(
                [
                    uv,
                    "build",
                    "--sdist",
                    "--out-dir",
                    str(source_distributions),
                    "--no-build-logs",
                    str(REPOSITORY),
                ],
                cwd=external_root,
                env=environment,
                check=True,
                capture_output=True,
                text=True,
            )
            source_distribution = next(source_distributions.glob("rp_ylx-*.tar.gz"))
            rebuilt_distributions = external_root / "rebuilt"
            subprocess.run(
                [
                    uv,
                    "build",
                    "--wheel",
                    "--out-dir",
                    str(rebuilt_distributions),
                    "--no-build-logs",
                    str(source_distribution),
                ],
                cwd=external_root,
                env=environment,
                check=True,
                capture_output=True,
                text=True,
            )
            rebuilt_wheel = next(rebuilt_distributions.glob("rp_ylx-*.whl"))

            verification_script = """
import contextlib, hashlib, io, json, os, pathlib, runpy, sys
from importlib.resources import files
import rp_ylx
from rp_ylx.web import (
    ECHO_WEB_SOURCE_COMMIT,
    asset_content_type,
    echo_web_required_device_api_major,
    read_asset,
    web_assets,
)

venv = pathlib.Path(sys.argv[1])
resource_names = json.loads(sys.argv[2])

def run_script(script_name, *args):
    script_path = venv / "bin" / script_name
    assert os.access(script_path, os.X_OK), f"{script_path} is not executable"
    buf = io.StringIO()
    old_argv = sys.argv
    sys.argv = [str(script_path), *args]
    try:
        with contextlib.redirect_stdout(buf):
            try:
                runpy.run_path(str(script_path), run_name="__main__")
            except SystemExit as exc:
                assert exc.code in (0, None), f"{script_name} {args} exited {exc.code}"
    finally:
        sys.argv = old_argv
    return buf.getvalue()

root = files("rp_ylx")
result = {
    "version": run_script("openaria", "--version").strip(),
    "legacy_version": run_script("rp-ylx", "--version").strip(),
    "spectacular_help": run_script("openaria-spectacular-check", "--help"),
    "button_help": run_script("rp-ylx-recording-button", "--help"),
    "status": json.loads(run_script("openaria", "status")),
    "embedded_web": {
        "source_commit": ECHO_WEB_SOURCE_COMMIT,
        "required_device_api_major": echo_web_required_device_api_major(),
        "assets": {
            name: {
                "bytes": len(read_asset(name)),
                "sha256": hashlib.sha256(read_asset(name)).hexdigest(),
                "content_type": asset_content_type(name),
            }
            for name in sorted(web_assets())
        },
    },
    "module_path": str(pathlib.Path(rp_ylx.__file__).resolve()),
    "installed_resources": {
        name: hashlib.sha256(root.joinpath(*name.split("/")).read_bytes()).hexdigest()
        for name in sorted(resource_names)
    },
}
print(json.dumps(result, sort_keys=True))
"""
            for index, wheel in enumerate((direct_wheel, rebuilt_wheel)):
                virtual_environment = external_root / f"venv-{index}"
                subprocess.run(
                    [
                        uv,
                        "venv",
                        "--no-project",
                        "--python",
                        sys.executable,
                        str(virtual_environment),
                    ],
                    cwd=external_root,
                    env=environment,
                    check=True,
                    capture_output=True,
                    text=True,
                )
                python = virtual_environment / "bin" / "python"
                subprocess.run(
                    [uv, "pip", "install", "--python", str(python), str(wheel)],
                    cwd=external_root,
                    env=environment,
                    check=True,
                    capture_output=True,
                    text=True,
                )
                verified = json.loads(
                    subprocess.run(
                        [
                            str(python),
                            "-c",
                            verification_script,
                            str(virtual_environment),
                            json.dumps(sorted(expected_resources)),
                        ],
                        cwd=external_root,
                        env=environment,
                        check=True,
                        capture_output=True,
                        text=True,
                    ).stdout
                )
                version = verified["version"]
                legacy_version = verified["legacy_version"]
                spectacular_help = verified["spectacular_help"]
                button_help = verified["button_help"]
                status = verified["status"]
                embedded_web = verified["embedded_web"]
                module_path = Path(verified["module_path"])
                installed_resources = verified["installed_resources"]

                self.assertIn("--monitor", button_help)
                self.assertEqual(version, f"Open Aria {expected_version} ({expected_commit})")
                self.assertEqual(legacy_version, version)
                self.assertIn("usage: openaria-spectacular-check", spectacular_help)
                self.assertEqual(status["commit"], expected_commit)
                self.assertEqual(status["native"]["adapter"], "rust")
                self.assertTrue(status["native"]["module_available"])
                self.assertEqual(status["native"]["abi"], 6)
                self.assertIn("capability_probe", status["native"]["features"])
                self.assertIn("jpeg_contract", status["native"]["features"])
                self.assertIn("frame_stream", status["native"]["features"])
                self.assertEqual(installed_resources, expected_resources)
                self.assertEqual(embedded_web, expected_web)
                self.assertIn(virtual_environment, module_path.parents)
                self.assertIn("site-packages", module_path.parts)
                self.assertNotIn(REPOSITORY.resolve(), module_path.parents)
            self.assertEqual(
                (REPOSITORY / "src/rp_ylx/_build_info.py").read_bytes(), source_build_info
            )


if __name__ == "__main__":
    unittest.main()

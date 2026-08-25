from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

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

        environment = os.environ.copy()
        for name in ("PYTHONPATH", "RP_YLX_COMMIT", "UV_PROJECT_ENVIRONMENT", "VIRTUAL_ENV"):
            environment.pop(name, None)

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
                executable = virtual_environment / "bin" / "rp-ylx"
                subprocess.run(
                    [uv, "pip", "install", "--python", str(python), str(wheel)],
                    cwd=external_root,
                    env=environment,
                    check=True,
                    capture_output=True,
                    text=True,
                )

                version = subprocess.run(
                    [str(executable), "--version"],
                    cwd=external_root,
                    env=environment,
                    check=True,
                    capture_output=True,
                    text=True,
                ).stdout.strip()
                status = json.loads(
                    subprocess.run(
                        [str(executable), "status"],
                        cwd=external_root,
                        env=environment,
                        check=True,
                        capture_output=True,
                        text=True,
                    ).stdout
                )
                embedded_web = json.loads(
                    subprocess.run(
                        [
                            str(python),
                            "-c",
                            (
                                "import hashlib, json; "
                                "from rp_ylx.web import "
                                "ECHO_WEB_SOURCE_COMMIT, asset_content_type, "
                                "read_asset, web_assets; "
                                "payload={"
                                "'source_commit': ECHO_WEB_SOURCE_COMMIT, "
                                "'assets': {"
                                "name: {"
                                "'bytes': len(read_asset(name)), "
                                "'sha256': hashlib.sha256(read_asset(name)).hexdigest(), "
                                "'content_type': asset_content_type(name),"
                                "} for name in sorted(web_assets())"
                                "}"
                                "}; "
                                "print(json.dumps(payload, sort_keys=True))"
                            ),
                        ],
                        cwd=external_root,
                        env=environment,
                        check=True,
                        capture_output=True,
                        text=True,
                    ).stdout
                )
                module_path = Path(
                    subprocess.run(
                        [
                            str(python),
                            "-c",
                            "import pathlib, rp_ylx; "
                            "print(pathlib.Path(rp_ylx.__file__).resolve())",
                        ],
                        cwd=external_root,
                        env=environment,
                        check=True,
                        capture_output=True,
                        text=True,
                    ).stdout.strip()
                )

                self.assertEqual(version, f"rp-ylx 0.1.0 ({expected_commit})")
                self.assertEqual(status["commit"], expected_commit)
                self.assertEqual(status["native"]["adapter"], "rust")
                self.assertTrue(status["native"]["module_available"])
                self.assertEqual(status["native"]["abi"], 4)
                self.assertIn("capability_probe", status["native"]["features"])
                self.assertIn("jpeg_contract", status["native"]["features"])
                self.assertIn("frame_stream", status["native"]["features"])
                self.assertEqual(
                    embedded_web,
                    {
                        "source_commit": "65b14d8d4f4e71402e9ea35261c05c4fa9bbfca9",
                        "assets": {
                            "app.js": {
                                "bytes": 96301,
                                "content_type": "text/javascript; charset=utf-8",
                                "sha256": (
                                    "9961f11a436c645ab3d35cfffbfcd77ab456bbc7469830f56dcecce63f3c3d5d"
                                ),
                            },
                            "index.html": {
                                "bytes": 454,
                                "content_type": "text/html; charset=utf-8",
                                "sha256": (
                                    "6149533de647bab93b56d60bd2c3568e78ce675b83c9dc438ea8c614e94a2272"
                                ),
                            },
                            "styles.css": {
                                "bytes": 21539,
                                "content_type": "text/css; charset=utf-8",
                                "sha256": (
                                    "80c0d651c9d79f658d32b4e4c333d1c8339377c4098d7b8865fca359f6b739ae"
                                ),
                            },
                        },
                    },
                )
                self.assertIn(virtual_environment, module_path.parents)
                self.assertIn("site-packages", module_path.parts)
                self.assertNotIn(REPOSITORY.resolve(), module_path.parents)
            self.assertEqual(
                (REPOSITORY / "src/rp_ylx/_build_info.py").read_bytes(), source_build_info
            )


if __name__ == "__main__":
    unittest.main()

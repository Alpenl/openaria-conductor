#!/usr/bin/env python3
"""从发布清单下载并安装/更新 Open Aria RDK X5 bundle。

清单格式：{"schema":"openaria.rdk-x5-release.v1", "version": ..., 
"bundle":{"url": ..., "sha256": ..., "bytes": ...}}
"""
from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import subprocess
import tarfile
import tempfile
import urllib.request
import urllib.error
from pathlib import Path


def _download(url: str, destination: Path) -> None:
    request = urllib.request.Request(url, headers={"User-Agent": "openaria-updater/1"})
    with urllib.request.urlopen(request, timeout=60) as response, destination.open("wb") as out:
        shutil.copyfileobj(response, out, length=1024 * 1024)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _safe_extract(archive: Path, target: Path) -> None:
    with tarfile.open(archive, "r:gz") as tar:
        root = target.resolve()
        for member in tar.getmembers():
            destination = (target / member.name).resolve()
            if destination != root and root not in destination.parents:
                raise RuntimeError(f"bundle 包含越界路径: {member.name}")
            if member.issym() or member.islnk():
                raise RuntimeError(f"bundle 不允许链接: {member.name}")
        tar.extractall(target)


def fetch_release(manifest_url: str, cache_dir: Path) -> tuple[Path, dict]:
    cache_dir.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(dir=cache_dir, suffix=".json", delete=False) as tmp:
        manifest_path = Path(tmp.name)
    try:
        _download(manifest_url, manifest_path)
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    finally:
        manifest_path.unlink(missing_ok=True)
    if not isinstance(manifest, dict) or manifest.get("schema") != "openaria.rdk-x5-release.v1":
        raise RuntimeError("发布清单 schema 无效")
    bundle = manifest.get("bundle")
    if not isinstance(bundle, dict) or not isinstance(bundle.get("url"), str):
        raise RuntimeError("发布清单缺少 bundle.url")
    expected = bundle.get("sha256")
    if not isinstance(expected, str) or len(expected) != 64:
        raise RuntimeError("发布清单缺少有效 bundle.sha256")
    destination = cache_dir / f"openaria-{manifest.get('version', 'unknown')}.tar.gz"
    if not destination.exists() or _sha256(destination) != expected:
        partial = destination.with_suffix(destination.suffix + ".part")
        _download(bundle["url"], partial)
        if bundle.get("bytes") is not None and partial.stat().st_size != bundle["bytes"]:
            partial.unlink(missing_ok=True)
            raise RuntimeError("bundle 大小校验失败")
        if _sha256(partial) != expected:
            partial.unlink(missing_ok=True)
            raise RuntimeError("bundle SHA-256 校验失败")
        partial.replace(destination)
    return destination, manifest


def install(manifest_url: str, *, cache_dir: Path, install_root: Path, no_activate: bool) -> dict:
    archive, manifest = fetch_release(manifest_url, cache_dir)
    with tempfile.TemporaryDirectory(prefix="openaria-bundle-") as directory:
        bundle_dir = Path(directory)
        _safe_extract(archive, bundle_dir)
        installer = bundle_dir / "rdk_x5_install.py"
        if not installer.is_file():
            raise RuntimeError("bundle 缺少 rdk_x5_install.py")
        command = ["python3", str(installer), "--install-root", str(install_root), "install", str(bundle_dir)]
        if no_activate:
            command.append("--no-activate")
        subprocess.run(command, check=True)
    return {"version": manifest.get("version"), "bundle": str(archive), "installed": True}


def main() -> int:
    parser = argparse.ArgumentParser(description="Open Aria RDK X5 一键安装/更新")
    parser.add_argument("manifest", help="latest.json 的 HTTPS URL")
    parser.add_argument("--cache-dir", type=Path, default=Path("/var/cache/openaria"))
    parser.add_argument("--install-root", type=Path, default=Path("/opt/rp-ylx"))
    parser.add_argument("--no-activate", action="store_true")
    args = parser.parse_args()
    try:
        print(json.dumps(install(args.manifest, cache_dir=args.cache_dir, install_root=args.install_root, no_activate=args.no_activate), ensure_ascii=False))
    except (OSError, RuntimeError, ValueError, tarfile.TarError, urllib.error.URLError, subprocess.CalledProcessError) as error:
        parser.error(str(error))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

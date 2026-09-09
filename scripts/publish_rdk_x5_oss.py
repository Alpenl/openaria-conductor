#!/usr/bin/env python3
"""发布 RDK X5 bundle 到阿里云 OSS（调用已配置好的 ossutil）。"""
from __future__ import annotations
import argparse, hashlib, json, subprocess
from pathlib import Path

def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for block in iter(lambda: f.read(1024 * 1024), b""): h.update(block)
    return h.hexdigest()

def main() -> int:
    p = argparse.ArgumentParser(description="发布 Open Aria RDK X5 固件 bundle 到阿里云 OSS")
    p.add_argument("--bundle", type=Path, required=True)
    p.add_argument("--version", required=True)
    p.add_argument("--oss-prefix", required=True, help="oss://bucket/path 前缀")
    p.add_argument("--public-base-url", required=True, help="用户可访问的 HTTPS 基础 URL")
    p.add_argument("--ossutil", default="ossutil")
    args = p.parse_args(); bundle = args.bundle.resolve()
    if not bundle.is_file(): p.error("bundle 文件不存在")
    remote = args.oss_prefix.rstrip("/") + f"/openaria-{args.version}.tar.gz"
    subprocess.run([args.ossutil, "cp", str(bundle), remote], check=True)
    manifest = {"schema":"openaria.rdk-x5-release.v1", "version":args.version,
                "bundle":{"url":args.public_base_url.rstrip("/")+f"/openaria-{args.version}.tar.gz",
                          "bytes":bundle.stat().st_size, "sha256":sha256(bundle)}}
    path = bundle.parent / "latest.json"; path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2)+"\n")
    subprocess.run([args.ossutil, "cp", str(path), args.oss_prefix.rstrip("/")+"/latest.json"], check=True)
    print(json.dumps(manifest, ensure_ascii=False))
    return 0
if __name__ == "__main__": raise SystemExit(main())

#!/usr/bin/env python3
# /// script
# dependencies = ["oss2==2.19.1"]
# ///
"""Prepare, verify and publish a closed offline bundle to Alibaba Cloud OSS."""

from __future__ import annotations

import argparse
import gzip
import json
import re
import shlex
import sys
import tarfile
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from rp_ylx.deployment import load_bundle  # noqa: E402
from rp_ylx.update import (  # noqa: E402
    PLATFORM,
    RELEASE_SCHEMA,
    download,
    https_url,
    sha256,
    verify_bundle,
)

ROOT = Path(__file__).resolve().parents[1]


def bootstrap_script(base_url: str, updater_key: str, digest: str) -> str:
    url = shlex.quote(https_url(base_url.rstrip("/") + "/" + updater_key))
    return f"""#!/bin/sh
set -eu
umask 077
if ! command -v python3 >/dev/null 2>&1; then
    echo "需要带 Python 3 的 RDK X5 官方 Ubuntu 系统" >&2
    exit 2
fi
python3 -c 'import sys; sys.exit(0 if sys.version_info >= (3, 10) else 2)' || {{
    echo "需要 Python 3.10 或更新版本（RDK Ubuntu 22.04/24.04）" >&2
    exit 2
}}
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT HUP INT TERM
curl --proto '=https' --proto-redir '=https' -fSL --retry 3 --connect-timeout 15 \\
    --max-time 300 {url} -o "$work/update.py"
printf '%s  %s\\n' {shlex.quote(digest)} "$work/update.py" | sha256sum --check --status
python3 "$work/update.py" "$@"
"""


def prepare(bundle_dir: Path, output: Path, config: dict) -> dict:
    bundle = load_bundle(bundle_dir)
    manifest = {
        "schema": RELEASE_SCHEMA,
        "platform": PLATFORM,
        "commit": bundle.commit,
        "version": bundle.version,
    }
    verify_bundle(bundle.root, manifest)
    if output.exists() and any(output.iterdir()):
        raise ValueError("output 必须为空")
    output.mkdir(parents=True, exist_ok=True)
    archive = output / "bundle.tar.gz"
    # Stable archive bytes: timestamps/owners do not change release identity.
    with (
        archive.open("wb") as raw,
        gzip.GzipFile(fileobj=raw, mode="wb", mtime=0, filename="") as gz,
        tarfile.open(fileobj=gz, mode="w") as tar,
    ):
        for path in sorted(bundle.root.iterdir()):
            if not path.is_file() or path.is_symlink():
                raise ValueError("发布目录只能包含普通文件")
            info = tar.gettarinfo(str(path), arcname=path.name)
            info.uid = info.gid = info.mtime = 0
            info.uname = info.gname = ""
            info.mode = 0o644
            with path.open("rb") as stream:
                tar.addfile(info, stream)
    base = https_url(config["public_base_url"]).rstrip("/")
    digest = sha256(archive)
    bundle_key = f"releases/{bundle.commit}/{digest}.tar.gz"
    manifest["bundle"] = {
        "url": base + "/" + bundle_key,
        "bytes": archive.stat().st_size,
        "sha256": digest,
    }
    updater = output / "update.py"
    updater.write_bytes((ROOT / "src/rp_ylx/update.py").read_bytes())
    updater_hash = sha256(updater)
    updater_key = f"installers/{updater_hash}/update.py"
    manifest["updater"] = {
        "url": base + "/" + updater_key,
        "bytes": updater.stat().st_size,
        "sha256": updater_hash,
    }
    (output / "latest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    (output / "install.sh").write_text(bootstrap_script(base, updater_key, updater_hash))
    return {"manifest": manifest, "bundle_key": bundle_key, "updater_key": updater_key}


def oss_bucket(config: dict, profile: str | None):
    import oss2

    if profile:
        data = json.loads((Path.home() / ".aliyun/config.json").read_bytes())
        selected = next((p for p in data["profiles"] if p["name"] == profile), None)
        if selected is None or selected.get("mode") != "AK":
            raise ValueError("指定 Alibaba Cloud CLI AK profile 不存在")
        auth = oss2.AuthV4(selected["access_key_id"], selected["access_key_secret"])
    else:
        from oss2.credentials import EnvironmentVariableCredentialsProvider

        auth = oss2.ProviderAuthV4(EnvironmentVariableCredentialsProvider())
    return oss2.Bucket(auth, config["endpoint"], config["bucket"], region=config["region"])


def publish(output: Path, prepared: dict, config: dict, bucket) -> None:
    import oss2

    prefix = config["prefix"].strip("/")
    base = config["public_base_url"].rstrip("/")

    def upload(local: Path, key: str, content_type: str, immutable: bool):
        headers = {
            "Content-Type": content_type,
            "x-oss-object-acl": "public-read",
            "Cache-Control": "public, max-age=31536000, immutable" if immutable else "no-cache",
            "x-oss-meta-sha256": sha256(local),
        }
        if immutable:
            headers["x-oss-forbid-overwrite"] = "true"
        try:
            bucket.put_object_from_file(prefix + "/" + key, str(local), headers=headers)
        except oss2.exceptions.ObjectAlreadyExists:
            if not immutable:
                raise
        # Anonymous full GET proves that public access works and verifies exact bytes.
        with tempfile.TemporaryDirectory() as temp:
            downloaded = Path(temp) / "object"
            download(base + "/" + key, downloaded, local.stat().st_size)
            if downloaded.stat().st_size != local.stat().st_size or sha256(downloaded) != sha256(
                local
            ):
                raise ValueError("OSS 匿名下载内容不匹配，停止发布")

    upload(output / "bundle.tar.gz", prepared["bundle_key"], "application/gzip", True)
    upload(output / "update.py", prepared["updater_key"], "text/x-python; charset=utf-8", True)
    # Immutable manifest enables pinning and channel restoration.
    release_key = f"releases/{prepared['manifest']['commit']}/{sha256(output / 'latest.json')}.json"
    upload(output / "latest.json", release_key, "application/json", True)
    upload(output / "install.sh", "install.sh", "text/x-shellscript; charset=utf-8", False)
    # The channel is changed only after all referenced content is publicly verified.
    upload(output / "latest.json", "latest.json", "application/json", False)


def main() -> int:
    parser = argparse.ArgumentParser(description="准备并发布 RDK X5 离线固件到阿里云 OSS")
    parser.add_argument("--bundle-dir", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--config", type=Path, default=ROOT / "deploy/oss-release.json")
    parser.add_argument("--publish", action="store_true", help="上传并切换 latest；默认仅本地打包")
    parser.add_argument(
        "--aliyun-profile", help="读取本机 ~/.aliyun/config.json；默认使用 OSS 环境凭据"
    )
    args = parser.parse_args()
    try:
        config = json.loads(args.config.read_bytes())
        https_url(config["endpoint"])
        https_url(config["public_base_url"])
        if not re.fullmatch(r"[a-z0-9][a-z0-9/\-]*", config["prefix"]):
            raise ValueError("OSS prefix 无效")
        prepared = prepare(args.bundle_dir, args.output, config)
        if args.publish:
            publish(args.output, prepared, config, oss_bucket(config, args.aliyun_profile))
        print(json.dumps({"published": args.publish, **prepared}, ensure_ascii=False))
    except Exception as error:
        # SDK exception bodies can include signed request details. Do not print them.
        print(f"发布失败：{type(error).__name__}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

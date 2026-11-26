#!/usr/bin/env python3
# /// script
# dependencies = ["oss2==2.19.1"]
# ///
"""Prepare, verify and publish a closed offline bundle to Alibaba Cloud OSS."""

from __future__ import annotations

import argparse
import base64
import gzip
import json
import os
import re
import shlex
import subprocess
import sys
import tarfile
import tempfile
import time
from datetime import UTC, datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from rp_ylx.deployment import Bundle, DeploymentError, load_bundle  # noqa: E402
from rp_ylx.update import (  # noqa: E402
    PLATFORM,
    RELEASE_SCHEMA,
    download,
    fetch_manifest,
    https_url,
    manifest_display_metadata,
    sha256,
    verify_bundle,
)

ROOT = Path(__file__).resolve().parents[1]
OPENARIA_RELEASE_SIGNING_KEY_ENV = "OPENARIA_RELEASE_SIGNING_KEY"


def _load_signing_key_bytes(signing_key: Path | str) -> bytes:
    if isinstance(signing_key, Path):
        if signing_key.is_symlink() or not signing_key.is_file():
            raise ValueError("发布签名私钥文件缺失或无效")
        raw = signing_key.read_bytes()
    else:
        text = signing_key.strip()
        if not text:
            raise ValueError("发布签名私钥为空")
        if "-----BEGIN" in text:
            return text.encode("ascii")
        candidate = Path(text)
        if candidate.exists():
            if candidate.is_symlink() or not candidate.is_file():
                raise ValueError("发布签名私钥文件缺失或无效")
            raw = candidate.read_bytes()
        else:
            raw = text.encode("ascii")
    if len(raw) > 16 * 1024:
        raise ValueError("发布签名私钥过大")
    return raw


def _sign_ed25519_bytes(raw_key: bytes, message: bytes) -> bytes:
    try:
        from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
        from cryptography.hazmat.primitives.serialization import load_pem_private_key
    except ImportError:
        Ed25519PrivateKey = None
        load_pem_private_key = None

    if load_pem_private_key is not None and Ed25519PrivateKey is not None:
        if b"-----BEGIN" in raw_key:
            key = load_pem_private_key(raw_key, password=None)
            if not isinstance(key, Ed25519PrivateKey):
                raise ValueError("发布签名私钥不是 Ed25519")
            return key.sign(message)
        if len(raw_key) == 32:
            return Ed25519PrivateKey.from_private_bytes(raw_key).sign(message)
        try:
            decoded = base64.b64decode(raw_key.strip(), validate=True)
        except ValueError as error:
            raise ValueError("发布签名私钥格式无效") from error
        if len(decoded) != 32:
            raise ValueError("Ed25519 私钥长度无效")
        return Ed25519PrivateKey.from_private_bytes(decoded).sign(message)

    if b"-----BEGIN" not in raw_key:
        raise ValueError("未安装 cryptography，仅支持 PEM 格式 Ed25519 私钥")
    with tempfile.TemporaryDirectory(prefix=".sign-") as temp:
        root = Path(temp)
        key_file = root / "priv.pem"
        msg_file = root / "msg.bin"
        sig_file = root / "sig.bin"
        key_file.write_bytes(raw_key)
        key_file.chmod(0o600)
        msg_file.write_bytes(message)
        result = subprocess.run(
            [
                "openssl",
                "pkeyutl",
                "-sign",
                "-inkey",
                str(key_file),
                "-rawin",
                "-in",
                str(msg_file),
                "-out",
                str(sig_file),
            ],
            capture_output=True,
            timeout=10,
            check=False,
        )
        if result.returncode != 0:
            raise ValueError("OpenSSL Ed25519 签名失败")
        sig = sig_file.read_bytes()
        if len(sig) != 64:
            raise ValueError("OpenSSL Ed25519 签名长度无效")
        return sig


def sign_manifest(manifest: dict, signing_key: Path | str) -> str:
    """Return base64 Ed25519 signature over canonical manifest JSON (excluding signature)."""
    canonical = json.dumps(
        {k: v for k, v in manifest.items() if k != "signature"},
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    raw_key = _load_signing_key_bytes(signing_key)
    signature = _sign_ed25519_bytes(raw_key, canonical)
    return base64.b64encode(signature).decode("ascii")


def check_channel_promotion(prepared: dict, config: dict) -> None:
    """A delayed/retried release cannot downgrade or replace a released version."""
    with tempfile.TemporaryDirectory() as directory:
        current = fetch_manifest(
            config["public_base_url"].rstrip("/") + "/latest.json", Path(directory)
        )
    incoming = prepared["manifest"]

    def version(value):
        if not re.fullmatch(r"[0-9]+\.[0-9]+\.[0-9]+", value):
            raise ValueError("渠道版本无效")
        return tuple(map(int, value.split(".")))

    if version(incoming["version"]) < version(current["version"]):
        raise ValueError("拒绝通过正式发布降级渠道；撤回渠道须使用专门回退流程")
    if incoming["version"] == current["version"] and (
        incoming["commit"] != current["commit"] or incoming["bundle"] != current["bundle"]
    ):
        raise ValueError("该版本已发布不同产物，请递增 0.2.x 补丁号")


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


def prepare(
    bundle_dir: Path,
    output: Path,
    config: dict,
    *,
    release_notes: str | None = None,
    signing_key: Path | str | None = None,
    bundle: Bundle | None = None,
) -> dict:
    if bundle is None:
        bundle = load_bundle(bundle_dir)
    manifest = {
        "schema": RELEASE_SCHEMA,
        "platform": PLATFORM,
        "commit": bundle.commit,
        "version": bundle.version,
    }
    if release_notes is not None:
        if not re.fullmatch(r"0\.2\.[1-9][0-9]*", bundle.version):
            raise ValueError("正式发布版本或说明无效")
        manifest["release_notes"] = release_notes
        manifest["published_at"] = datetime.now(UTC).isoformat()
    manifest_display_metadata(manifest)
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
    resolved_key = (
        signing_key
        if signing_key is not None
        else os.environ.get(OPENARIA_RELEASE_SIGNING_KEY_ENV, "").strip() or None
    )
    if resolved_key is not None:
        manifest["signature"] = sign_manifest(manifest, resolved_key)
    (output / "latest.json").write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    (output / "install.sh").write_text(
        bootstrap_script(base, updater_key, updater_hash), encoding="utf-8"
    )
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
        local_sha256 = sha256(local)
        local_bytes = local.stat().st_size
        headers = {
            "Content-Type": content_type,
            "x-oss-object-acl": "public-read",
            "Cache-Control": "public, max-age=31536000, immutable" if immutable else "no-cache",
            "x-oss-meta-sha256": local_sha256,
        }
        if immutable:
            headers["x-oss-forbid-overwrite"] = "true"
        print(f"上传 {key} ({local_bytes} bytes)", file=sys.stderr, flush=True)
        for attempt in range(3):
            try:
                bucket.put_object_from_file(prefix + "/" + key, str(local), headers=headers)
                break
            except oss2.exceptions.ObjectAlreadyExists:
                if not immutable:
                    raise
                break
            except Exception as error:
                status = getattr(error, "status", 0)
                # OSS can report FileAlreadyExists as a generic ServerError,
                # rather than the SDK's ObjectAlreadyExists subclass.
                if (
                    immutable
                    and status == 409
                    and getattr(error, "code", "") == "FileAlreadyExists"
                ):
                    break
                retryable = status == 429 or status >= 500 or type(error).__name__ == "RequestError"
                if not retryable or attempt == 2:
                    raise
                print(f"上传暂时失败 HTTP {status}，正在重试…", file=sys.stderr, flush=True)
                time.sleep(2 ** (attempt + 1))
        # Anonymous full GET proves that public access works and verifies exact bytes.
        with tempfile.TemporaryDirectory() as temp:
            downloaded = Path(temp) / "object"
            download(base + "/" + key, downloaded, local_bytes)
            if downloaded.stat().st_size != local_bytes or sha256(downloaded) != local_sha256:
                raise ValueError("OSS 匿名下载内容不匹配，停止发布")

    upload(output / "bundle.tar.gz", prepared["bundle_key"], "application/gzip", True)
    upload(output / "update.py", prepared["updater_key"], "text/x-python; charset=utf-8", True)
    # Immutable manifest enables pinning and channel restoration.
    release_key = f"releases/{prepared['manifest']['commit']}/{sha256(output / 'latest.json')}.json"
    upload(output / "latest.json", release_key, "application/json", True)
    upload(output / "install.sh", "install.sh", "text/x-shellscript; charset=utf-8", False)
    # The channel is changed only after all referenced content is publicly verified.
    upload(output / "latest.json", "latest.json", "application/json", False)


def _redact_local_error(message: str, config: dict | None) -> str:
    redacted = message
    for env_name in (
        "OSS_ACCESS_KEY_ID",
        "OSS_ACCESS_KEY_SECRET",
        "OSS_SESSION_TOKEN",
        OPENARIA_RELEASE_SIGNING_KEY_ENV,
    ):
        secret = os.environ.get(env_name, "").strip()
        if len(secret) >= 4:
            redacted = redacted.replace(secret, "[REDACTED]")
    if isinstance(config, dict):
        for key in ("endpoint", "bucket"):
            value = config.get(key)
            if isinstance(value, str) and len(value) >= 4:
                redacted = redacted.replace(value, "[REDACTED]")
    return redacted


def main() -> int:
    parser = argparse.ArgumentParser(description="准备并发布 RDK X5 离线固件到阿里云 OSS")
    parser.add_argument("--bundle-dir", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--config", type=Path, default=ROOT / "deploy/oss-release.json")
    parser.add_argument("--publish", action="store_true", help="上传并切换 latest；默认仅本地打包")
    parser.add_argument("--tag", help="正式发布必须指定与 bundle 对应的附注标签")
    parser.add_argument(
        "--aliyun-profile", help="读取本机 ~/.aliyun/config.json；默认使用 OSS 环境凭据"
    )
    parser.add_argument(
        "--signing-key",
        "--signing-key-file",
        dest="signing_key",
        help=(
            "Ed25519 私钥路径（PEM 或 32 字节原始/Base64 密钥）；"
            "默认读取 OPENARIA_RELEASE_SIGNING_KEY"
        ),
    )
    parser.add_argument(
        "--allow-unsigned-dev",
        action="store_true",
        help="允许开发/测试阶段在无 Ed25519 签名私钥时执行 --publish",
    )
    args = parser.parse_args()
    config: dict | None = None
    try:
        config = json.loads(args.config.read_bytes())
        https_url(config["endpoint"])
        https_url(config["public_base_url"])
        if not re.fullmatch(r"[a-z0-9][a-z0-9/\-]*", config["prefix"]):
            raise ValueError("OSS prefix 无效")
        notes = None
        bundle = load_bundle(args.bundle_dir)
        resolved_signing_key = (
            args.signing_key
            if args.signing_key is not None
            else os.environ.get(OPENARIA_RELEASE_SIGNING_KEY_ENV, "").strip() or None
        )
        if args.publish:
            try:
                from release_version import release_tag
            except ImportError:
                from scripts.release_version import release_tag

            if resolved_signing_key is None and not args.allow_unsigned_dev:
                raise ValueError(
                    "正式发布需要提供 Ed25519 签名私钥（OPENARIA_RELEASE_SIGNING_KEY 或 "
                    "--signing-key-file），或显式指定 --allow-unsigned-dev"
                )
            if not args.tag:
                raise ValueError("发布需要 --tag v0.2.x")
            version, notes = release_tag(args.tag)
            if bundle.version != version:
                raise ValueError("bundle 版本与附注标签不一致")
            commit = subprocess.check_output(
                ["git", "rev-parse", f"refs/tags/{args.tag}^{{commit}}"], cwd=ROOT, text=True
            ).strip()
            if bundle.commit != commit:
                raise ValueError("bundle commit 与附注标签不一致")
        prepared = prepare(
            args.bundle_dir,
            args.output,
            config,
            release_notes=notes,
            signing_key=resolved_signing_key,
            bundle=bundle,
        )
        if args.publish:
            check_channel_promotion(prepared, config)
            publish(args.output, prepared, config, oss_bucket(config, args.aliyun_profile))
        print(json.dumps({"published": args.publish, **prepared}, ensure_ascii=False))
    except (ValueError, DeploymentError) as error:
        detail = _redact_local_error(str(error), config)
        print(f"发布失败：{type(error).__name__}: {detail}", file=sys.stderr)
        return 2
    except Exception as error:
        # SDK exception bodies can include signed request details. Do not print them.
        print(
            f"发布失败：{type(error).__name__} HTTP {getattr(error, 'status', 'n/a')} "
            f"code={getattr(error, 'code', 'n/a')}",
            file=sys.stderr,
        )
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

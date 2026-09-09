#!/usr/bin/env python3
"""Standalone, standard-library-only RDK X5 online installer (Python >= 3.10)."""

from __future__ import annotations

import argparse
import ctypes.util
import fcntl
import hashlib
import json
import os
import platform
import re
import shutil
import ssl
import stat
import subprocess
import sys
import tarfile
import tempfile
import time
import urllib.parse
import urllib.request
from contextlib import contextmanager
from pathlib import Path

DEFAULT_MANIFEST = "https://openaria-firmware-cn.oss-cn-beijing.aliyuncs.com/rdk-x5/latest.json"
RELEASE_SCHEMA = "openaria.rdk-x5-release.v1"
PLATFORM = "linux_aarch64_rdk_x5_v1"
MAX_ARCHIVE = 2 * 1024**3
MAX_UNPACKED = 4 * 1024**3
CONFIG = Path("/etc/rp-ylx/device.json")
CURRENT = Path("/opt/rp-ylx/current")
DEFAULT_CACHE = Path("/var/cache/openaria")


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def https_url(url: object) -> str:
    if not isinstance(url, str):
        raise ValueError("下载地址必须是 HTTPS URL")
    parsed = urllib.parse.urlsplit(url)
    if (
        parsed.scheme != "https"
        or not parsed.hostname
        or parsed.username
        or parsed.password
        or parsed.fragment
        or parsed.query
    ):
        raise ValueError("下载地址必须是无凭据、无查询参数的 HTTPS URL")
    return url


class HTTPSRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        https_url(newurl)
        return super().redirect_request(req, fp, code, msg, headers, newurl)


def download(url: str, destination: Path, limit: int) -> None:
    https_url(url)
    opener = urllib.request.build_opener(HTTPSRedirect())
    for attempt in range(3):
        try:
            request = urllib.request.Request(url, headers={"User-Agent": "openaria-updater/1"})
            with opener.open(request, timeout=60) as response, destination.open("wb") as out:
                count = 0
                for block in iter(lambda: response.read(1024 * 1024), b""):
                    count += len(block)
                    if count > limit:
                        raise ValueError("下载超过清单大小或安全上限")
                    out.write(block)
                out.flush()
                os.fsync(out.fileno())
            return
        except OSError:
            if attempt == 2:
                raise
            time.sleep(attempt + 1)


def validate_descriptor(value: dict, maximum: int) -> None:
    digest, size = value.get("sha256"), value.get("bytes")
    if (
        not isinstance(digest, str)
        or not re.fullmatch("[0-9a-f]{64}", digest)
        or type(size) is not int
        or not 0 < size <= maximum
    ):
        raise ValueError("文件大小或 SHA-256 无效")


def validate_manifest(value: object) -> dict:
    if not isinstance(value, dict) or value.get("schema") != RELEASE_SCHEMA:
        raise ValueError("发布清单 schema 无效")
    if value.get("platform") != PLATFORM:
        raise ValueError("发布清单不支持 RDK X5")
    if not isinstance(value.get("commit"), str) or not re.fullmatch(
        "[0-9a-f]{40}", value["commit"]
    ):
        raise ValueError("发布清单 commit 无效")
    if not isinstance(value.get("version"), str) or not value["version"]:
        raise ValueError("发布清单 version 无效")
    if not isinstance(value.get("bundle"), dict):
        raise ValueError("发布清单 bundle 无效")
    validate_descriptor(value["bundle"], MAX_ARCHIVE)
    https_url(value["bundle"].get("url"))
    if not isinstance(value.get("updater"), dict):
        raise ValueError("发布清单 updater 无效")
    validate_descriptor(value["updater"], 1024 * 1024)
    https_url(value["updater"].get("url"))
    return value


def fetch_manifest(url: str, directory: Path) -> dict:
    target = directory / "manifest.json"
    download(url, target, 128 * 1024)
    return validate_manifest(json.loads(target.read_bytes()))


def fetch_bundle(manifest: dict, cache: Path) -> Path:
    descriptor = manifest["bundle"]
    target = cache / (descriptor["sha256"] + ".tar.gz")
    if target.is_symlink():
        raise ValueError("缓存文件不允许符号链接")
    if (
        target.is_file()
        and target.stat().st_size == descriptor["bytes"]
        and sha256(target) == descriptor["sha256"]
    ):
        return target
    with tempfile.TemporaryDirectory(prefix=".download-", dir=cache) as temp:
        partial = Path(temp) / "bundle.part"
        print("正在下载固件…", file=sys.stderr, flush=True)
        download(descriptor["url"], partial, descriptor["bytes"])
        if partial.stat().st_size != descriptor["bytes"] or sha256(partial) != descriptor["sha256"]:
            raise ValueError("固件大小或 SHA-256 校验失败，未执行安装")
        partial.replace(target)
    return target


def extract_bundle(archive: Path, target: Path) -> None:
    """Release archives are deliberately flat: only regular bundle files."""
    with tarfile.open(archive, "r:gz") as stream:
        seen = set()
        total = 0
        for member in stream:
            name = member.name
            total += member.size
            if (
                not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.+\-]*", name)
                or not member.isfile()
                or member.issparse()
                or name in seen
                or len(seen) >= 256
                or total > MAX_UNPACKED
            ):
                raise ValueError("固件归档含非法路径、特殊文件或超过上限")
            seen.add(name)
            source = stream.extractfile(member)
            if source is None:
                raise ValueError("固件归档文件不可读")
            with source, (target / name).open("xb") as out:
                shutil.copyfileobj(source, out, length=1024 * 1024)
            (target / name).chmod(0o644)


def verify_bundle(root: Path, manifest: dict) -> None:
    """Check every executable payload before the bundled Python is executed."""
    data = (root / "bundle.json").read_bytes()
    if len(data) > 128 * 1024:
        raise ValueError("bundle.json 过大")
    bundle = json.loads(data)
    if not isinstance(bundle, dict) or bundle.get("schema") != "ylx.rdk-x5-bundle.v1":
        raise ValueError("bundle.json schema 无效")
    for field in ("commit", "version", "platform"):
        if bundle.get(field) != manifest[field]:
            raise ValueError("固件身份与发布清单不一致")
    if not isinstance(bundle.get("wheels"), list) or not 1 <= len(bundle["wheels"]) <= 128:
        raise ValueError("wheel 列表无效")
    names = {"bundle.json", "SHA256SUMS", "build-info.txt"}
    descriptors = [bundle.get("installer"), bundle.get("runtime"), *bundle["wheels"]]
    for item in descriptors:
        if not isinstance(item, dict):
            raise ValueError("bundle 文件描述无效")
        name = item.get("file")
        if (
            not isinstance(name, str)
            or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.+\-]*", name)
            or name in names
        ):
            raise ValueError("bundle 文件名无效或重复")
        names.add(name)
        validate_descriptor(item, 512 * 1024**2)
        path = root / name
        if path.is_symlink() or not path.is_file():
            raise ValueError("bundle 文件缺失或不是普通文件")
        if path.stat().st_size != item["bytes"] or sha256(path) != item["sha256"]:
            raise ValueError("bundle 内部完整性校验失败")
    if bundle["installer"]["file"] != "rdk_x5_install.py":
        raise ValueError("bundle 安装器名称无效")
    wheels = [item["file"] for item in bundle["wheels"]]
    if [name for name in wheels if name.startswith("rp_ylx-")] != [bundle.get("application_wheel")]:
        raise ValueError("bundle 应包含唯一应用 wheel")
    if {path.name for path in root.iterdir()} - names:
        raise ValueError("bundle 包含未声明文件")


def current_commit() -> str | None:
    if not CURRENT.is_symlink():
        return None
    name = CURRENT.resolve().name
    if not re.fullmatch("[0-9a-f]{40}", name):
        raise ValueError("已安装版本链接无效")
    return name


def require_target() -> None:
    if os.geteuid() != 0:
        raise ValueError("安装或更新需要 sudo；只查看更新可使用 --check")
    if platform.machine() not in {"aarch64", "arm64"}:
        raise ValueError("仅支持 RDK X5 aarch64；本机可以使用 --check 或 --download-only")
    model = Path("/proc/device-tree/model").read_text().strip("\x00\n ")
    if " ".join(model.split()).casefold() != "d-robotics rdk x5 v1.0":
        raise ValueError("仅支持 D-Robotics RDK X5 V1.0")
    if not Path("/run/systemd/system").is_dir():
        raise ValueError("必须在使用 systemd 的 RDK 系统中安装")


def require_idle() -> None:
    if not current_commit():
        return
    config = json.loads(CONFIG.read_bytes())
    security = config["security"]
    headers = {}
    scheme, context = "http", None
    if security["profile"] == "customer":
        scheme = "https"
        context = ssl.create_default_context(cafile=security["tls_certificate_file"])
        headers["Authorization"] = (
            "Bearer " + Path(security["bearer_token_file"]).read_text().strip()
        )
    port = config["listen"]["port"]
    request = urllib.request.Request(
        f"{scheme}://127.0.0.1:{port}/api/v4/capture/status", headers=headers
    )
    opener = urllib.request.build_opener(
        urllib.request.ProxyHandler({}), urllib.request.HTTPSHandler(context=context)
    )
    try:
        with opener.open(request, timeout=15) as response:
            value = json.loads(response.read(1024 * 1024))
    except (OSError, ValueError) as error:
        raise ValueError("无法确认设备空闲；请先检查 rp-ylx 服务") from error
    snapshot = value.get("snapshot", {})
    if snapshot.get("device_state") != "idle" or snapshot.get("active_recording") is not None:
        raise ValueError("设备尚未空闲，请先停止录制并等待封存完成")


def prepare_dependencies() -> None:
    packages = {
        "nmcli": "network-manager",
        "make": "make",
        "cc": "gcc",
        "openssl": "openssl",
        "mkfs.ext4": "e2fsprogs",
        "avahi-daemon": "avahi-daemon",
        "sshd": "openssh-server",
        "sudo": "sudo",
        "runuser": "util-linux",
    }
    needed = {package for command, package in packages.items() if shutil.which(command) is None}
    for library, package in (
        ("turbojpeg", "libturbojpeg0"),
        ("asound", "libasound2"),
        ("multimedia", "hobot-multimedia"),
    ):
        if ctypes.util.find_library(library) is None:
            needed.add(package)
    if needed:
        print("安装系统依赖：" + ", ".join(sorted(needed)), file=sys.stderr, flush=True)
        env = {**os.environ, "DEBIAN_FRONTEND": "noninteractive"}
        subprocess.run(["apt-get", "update"], check=True, env=env, timeout=600)
        subprocess.run(
            ["apt-get", "install", "-y", "--no-install-recommends", *sorted(needed)],
            check=True,
            env=env,
            timeout=1200,
        )


def data_volume_size(free_bytes: int) -> int:
    # Leave at least 8 GiB or 20% for the OS, downloads and a second release.
    free_gib = free_bytes // 1024**3
    size = min(80, free_gib - max(8, (free_gib + 4) // 5))
    if size < 4:
        raise ValueError("首次安装至少需要 12 GiB 可用空间（4 GiB 数据区 + 8 GiB 系统预留）")
    return size


def configure_data_volume() -> None:
    image = Path("/var/lib/rp-ylx-data-volume/data-volume.img")
    override = Path("/etc/systemd/system/rp-ylx-data-volume.service.d/10-size.conf")
    if image.exists() or override.exists():
        return
    if os.path.ismount("/data"):
        raise ValueError("检测到已挂载 /data，需核对现有存储布局后再安装")
    data = Path("/data")
    if data.exists() and any(data.iterdir()):
        raise ValueError("/data 已有未挂载数据，请先核对存储布局")
    size = data_volume_size(shutil.disk_usage("/var/lib").free)
    override.parent.mkdir(parents=True, exist_ok=True)
    override.write_text(f"[Service]\nEnvironment=RP_YLX_DATA_VOLUME_SIZE={size}G\n")
    print(f"自动配置 {size} GiB 固定数据区，保留系统空间", file=sys.stderr)


def fetch_updater(manifest: dict, directory: Path) -> Path:
    item = manifest["updater"]
    target = directory / "update.py"
    download(item["url"], target, item["bytes"])
    if target.stat().st_size != item["bytes"] or sha256(target) != item["sha256"]:
        raise ValueError("更新器校验失败")
    return target


def install_update_command(source: Path) -> None:
    directory = Path("/usr/local/lib/openaria")
    directory.mkdir(parents=True, exist_ok=True)
    target = directory / "update.py"
    temporary = directory / ".update.py.tmp"
    temporary.write_bytes(source.read_bytes())
    temporary.chmod(0o644)
    temporary.replace(target)
    launcher = Path("/usr/local/sbin/openaria-update")
    launcher.parent.mkdir(parents=True, exist_ok=True)
    temporary = launcher.with_name(".openaria-update.tmp")
    temporary.write_text(
        '#!/bin/sh\nexec /usr/bin/python3 /usr/local/lib/openaria/update.py "$@"\n'
    )
    temporary.chmod(0o755)
    temporary.replace(launcher)


@contextmanager
def cache_lock(cache: Path):
    cache.mkdir(parents=True, exist_ok=True, mode=0o700)
    mode = cache.lstat()
    if not stat.S_ISDIR(mode.st_mode) or mode.st_uid != os.geteuid() or mode.st_mode & 0o022:
        raise ValueError("缓存目录必须归当前用户所有且不允许其他用户写入")
    fd = os.open(cache / ".lock", os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
    with os.fdopen(fd, "w") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise ValueError("另一个安装或更新任务正在运行") from error
        yield


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Open Aria 一键安装、检查更新、升级与回退")
    parser.add_argument("manifest", nargs="?", default=DEFAULT_MANIFEST, help="发布清单 HTTPS URL")
    action = parser.add_mutually_exclusive_group()
    action.add_argument("--check", action="store_true", help="仅检查版本，不安装")
    action.add_argument("--download-only", action="store_true", help="下载并校验固件，不安装")
    action.add_argument("--rollback", action="store_true", help="回退到设备上的上一版本（离线）")
    action.add_argument("--status", action="store_true", help="查看本机部署状态（离线）")
    parser.add_argument("--reinstall", action="store_true", help="同一 commit 也重新安装")
    parser.add_argument("--cache-dir", type=Path, default=None)
    args = parser.parse_args(argv)
    try:
        if args.status:
            return subprocess.call(["/usr/local/sbin/rp-ylx-deploy", "status"])
        if not (args.check or args.download_only):
            require_target()
            if args.cache_dir is not None:
                raise ValueError(
                    "--cache-dir 只用于 --check / --download-only；安装共用固定锁和缓存"
                )
        cache = args.cache_dir or (
            DEFAULT_CACHE if os.geteuid() == 0 else Path.home() / ".cache/openaria"
        )
        with cache_lock(cache), tempfile.TemporaryDirectory(prefix=".stage-", dir=cache) as temp:
            if args.rollback:
                require_idle()
                return subprocess.call(["/usr/local/sbin/rp-ylx-deploy", "rollback"])
            directory = Path(temp)
            manifest = fetch_manifest(args.manifest, directory)
            current = current_commit()
            result = {
                "current": current,
                "available": manifest["commit"],
                "version": manifest["version"],
            }
            if args.check:
                print(json.dumps({**result, "update_available": current != manifest["commit"]}))
                return 0
            updater = fetch_updater(manifest, directory)
            if current == manifest["commit"] and not (args.reinstall or args.download_only):
                install_update_command(updater)
                print(json.dumps({**result, "changed": False}))
                return 0
            archive = fetch_bundle(manifest, cache)
            bundle = directory / "bundle"
            bundle.mkdir()
            extract_bundle(archive, bundle)
            verify_bundle(bundle, manifest)
            if args.download_only:
                print(json.dumps({**result, "archive": str(archive), "verified": True}))
                return 0
            require_idle()
            prepare_dependencies()
            configure_data_volume()
            require_idle()
            print("校验通过，正在安装并等待服务就绪…", file=sys.stderr, flush=True)
            subprocess.run(
                ["/usr/bin/python3", str(bundle / "rdk_x5_install.py"), "install", str(bundle)],
                check=True,
            )
            if current_commit() != manifest["commit"]:
                raise ValueError("安装后版本不匹配")
            install_update_command(updater)
            config = json.loads(CONFIG.read_bytes())
            scheme = "https" if config["security"]["profile"] == "customer" else "http"
            print(json.dumps({**result, "installed": True, "changed": True}))
            print(
                f"安装完成。浏览器访问 {scheme}://设备IP:{config['listen']['port']}/",
                file=sys.stderr,
            )
            print(
                "以后运行 sudo openaria-update 更新；sudo openaria-update --rollback 回退。",
                file=sys.stderr,
            )
            if scheme == "https":
                print(
                    "首次连接所需访问令牌保存在 /etc/rp-ylx/customer.token（sudo 可读）。",
                    file=sys.stderr,
                )
        return 0
    except (
        OSError,
        ValueError,
        KeyError,
        TypeError,
        tarfile.TarError,
        subprocess.SubprocessError,
    ) as error:
        print(f"Open Aria 更新失败：{error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())

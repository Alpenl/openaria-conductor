#!/usr/bin/env python3
"""OpenAria factory card recovery. Python 3.10+, paramiko and cryptography."""

from __future__ import annotations

import argparse
import base64
import getpass
import hashlib
import json
import os
import shlex
import sys
import time
import uuid
from pathlib import Path

if __package__:
    from . import device, disks, prepare_image, provision
else:
    import device
    import disks
    import prepare_image
    import provision


def encrypt(raw, password):
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM
    from cryptography.hazmat.primitives.kdf.scrypt import Scrypt

    salt, nonce = os.urandom(16), os.urandom(12)
    key = Scrypt(salt=salt, length=32, n=2**15, r=8, p=1).derive(password.encode())
    return b"OAR1" + salt + nonce + AESGCM(key).encrypt(nonce, raw, b"OAR1")


def decrypt(raw, password):
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM
    from cryptography.hazmat.primitives.kdf.scrypt import Scrypt

    if raw[:4] != b"OAR1" or len(raw) > 16 * 1024**2:
        raise RuntimeError("Invalid encrypted profile")
    key = Scrypt(salt=raw[4:20], length=32, n=2**15, r=8, p=1).derive(password.encode())
    return AESGCM(key).decrypt(raw[20:32], raw[32:], b"OAR1")


def save_private(path, raw):
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, "wb") as f:
        f.write(raw)
        f.flush()
        os.fsync(f.fileno())


def connection(host, user):
    import paramiko

    class ConfirmKey(paramiko.MissingHostKeyPolicy):
        def missing_host_key(self, client, hostname, key):
            fp = "SHA256:" + base64.b64encode(
                hashlib.sha256(key.asbytes()).digest()
            ).decode().rstrip("=")
            print(f"SSH host {hostname}: {fp}")
            if input("Verify with device/operator, then type the fingerprint: ").strip() != fp:
                raise RuntimeError("SSH host identity not confirmed")

    client = paramiko.SSHClient()
    client.load_system_host_keys()
    client.set_missing_host_key_policy(ConfirmKey())
    password = getpass.getpass("Device login / sudo password: ")
    try:
        client.connect(
            host,
            username=user,
            password=password,
            timeout=15,
            look_for_keys=False,
            allow_agent=False,
        )
    except paramiko.BadHostKeyException as changed:
        client.close()
        print("SSH key differs from this computer's saved key (expected after a clean reflash).")
        client = paramiko.SSHClient()
        # Explicit fingerprint confirmation, without deleting global known_hosts entries.
        ConfirmKey().missing_host_key(client, host, changed.key)
        client.get_host_keys().add(host, changed.key.get_name(), changed.key)
        client.connect(
            host,
            username=user,
            password=password,
            timeout=15,
            look_for_keys=False,
            allow_agent=False,
        )
    return client, password


def remote(client, password, user, script, args=(), payload=b""):
    code = base64.b64encode(script.encode()).decode()
    marker = ("OPENARIA_INPUT_" + uuid.uuid4().hex + "\n").encode()
    wrapper = (
        "import base64,io,sys;"
        "raw=sys.stdin.buffer.read();"
        f"sys.stdin=io.TextIOWrapper(io.BytesIO(raw.split({marker!r},1)[1]));"
        f"exec(base64.b64decode({code!r}))"
    )
    command = "python3 -c " + shlex.quote(wrapper)
    command += " " + " ".join(shlex.quote(a) for a in args)
    payload = marker + payload
    if user != "root":
        command = "sudo -S -p '' -- " + command
        payload = password.encode() + b"\n" + payload
    stdin, stdout, stderr = client.exec_command(command, timeout=900)
    stdin.write(payload)
    stdin.flush()
    stdin.channel.shutdown_write()
    out = stdout.read()
    err = stderr.read()
    if stdout.channel.recv_exit_status():
        # Never echo remote configuration, passwords or an untrusted traceback.
        raise RuntimeError("Device operation failed (remote details suppressed to protect secrets)")
    if len(out) > 16 * 1024**2 or len(err) > 1024**2:
        raise RuntimeError("Unexpected remote response size")
    return out


def backup(args):
    if args.profile.exists():
        raise RuntimeError("Profile already exists; choose a new filename")
    password = getpass.getpass("Set backup encryption password (at least 12 characters): ")
    if len(password) < 12 or getpass.getpass("Repeat backup password: ") != password:
        raise RuntimeError("Backup passwords do not match or are too short")
    client, login = connection(args.host, args.user)
    try:
        raw = remote(
            client, login, args.user, Path(__file__).with_name("device.py").read_text(), ("backup",)
        )
        device.validate(json.loads(raw))
        encrypted = encrypt(raw, password)
        if decrypt(encrypted, password) != raw:
            raise RuntimeError("Backup encryption self-check failed")
        save_private(args.profile, encrypted)
        print("Encrypted configuration backup verified. No recordings or history included.")
    finally:
        client.close()


INSTALL = r"""
import hashlib, json, os, pathlib, subprocess, sys, tarfile
root = pathlib.Path(sys.argv[1])
if root.is_symlink() or not root.name.startswith('openaria-factory-'):
    raise RuntimeError('invalid staging directory')
os.chown(root, 0, 0)
root.chmod(0o700)
persistent = pathlib.Path('/var/lib/openaria-factory')
persistent.mkdir(mode=0o700, exist_ok=True)
if persistent.is_symlink(): raise RuntimeError('invalid persistent directory')
root = pathlib.Path(root.rename(persistent / root.name))
archive = root / 'bundle.tar'
with archive.open('rb') as f:
    digest = None
if digest is None:
    h = hashlib.sha256()
    with archive.open('rb') as f:
        for b in iter(lambda:f.read(1024*1024), b''): h.update(b)
    digest = h.hexdigest()
if digest != sys.argv[2]: raise RuntimeError('bundle digest mismatch')
bundle = root / 'bundle'
bundle.mkdir(mode=0o700)
with tarfile.open(archive) as tar:
    members = tar.getmembers()
    if len(members) > 10000 or sum(m.size for m in members) > 4*1024**3:
        raise RuntimeError('archive too large')
    seen = set()
    for m in members:
        p = pathlib.PurePosixPath(m.name)
        if m.name in ('.','./') and m.isdir(): continue
        if p.is_absolute() or '..' in p.parts or not p.parts or p in seen:
            raise RuntimeError('invalid archive path')
        seen.add(p)
        if not (m.isfile() or m.isdir()): raise RuntimeError('invalid archive member')
    for m in members:
        if m.name in ('.','./') and m.isdir(): continue
        p = bundle / m.name
        if m.isdir(): p.mkdir(parents=True, exist_ok=True)
        else:
            p.parent.mkdir(parents=True, exist_ok=True)
            with tar.extractfile(m) as src, p.open('xb') as dst:
                import shutil
                shutil.copyfileobj(src,dst)
            p.chmod(0o755 if m.mode & 0o111 else 0o600)
subprocess.run(['sha256sum','-c','SHA256SUMS'],cwd=bundle,check=True,stdout=subprocess.DEVNULL)
print(json.dumps({'root':str(root)}))
"""

ACTIVATE = r"""
import json, pathlib, subprocess, sys
root=pathlib.Path(sys.argv[1])
bundle=root/'bundle'
unit='openaria-factory-'+root.name.removeprefix('openaria-factory-')
subprocess.run(['systemd-run','--unit='+unit,'--property=Type=exec',
    '--property=StandardOutput=append:'+str(root/'install.log'),
    '--property=StandardError=append:'+str(root/'install.log'),
    '/usr/bin/flock','/run/openaria-factory.lock','/usr/bin/python3',
    str(bundle/'rdk_x5_install.py'),'install',str(bundle)],check=True,stdout=subprocess.DEVNULL)
print(json.dumps({'unit':unit,'log':str(root/'install.log')}))
"""


def restore(args):
    if not args.bundle.is_file() or disks.digest(args.bundle) != args.bundle_sha256.lower():
        raise RuntimeError("Offline firmware bundle SHA-256 mismatch")
    raw = decrypt(args.profile.read_bytes(), getpass.getpass("Backup encryption password: "))
    profile = json.loads(raw)
    device.validate(profile)
    client, login = connection(args.host, args.user)
    try:
        # Stage package before changing any device configuration.
        stage = "/tmp/openaria-factory-" + uuid.uuid4().hex
        with client.open_sftp() as sftp:
            sftp.mkdir(stage, mode=0o700)
            sftp.put(str(args.bundle), stage + "/bundle.tar")
        prepared = json.loads(
            remote(client, login, args.user, INSTALL, (stage, args.bundle_sha256.lower()))
        )
        remote(
            client,
            login,
            args.user,
            Path(__file__).with_name("device.py").read_text(),
            ("restore",),
            raw,
        )
        receipt = json.loads(remote(client, login, args.user, ACTIVATE, (prepared["root"],)))
        print("Persistent installation started. Remote log:", receipt["log"])
        for _ in range(180):
            script = (
                "import subprocess; print(subprocess.check_output("
                "['systemctl','show',"
                + repr(receipt["unit"])
                + ",'--property=ActiveState,Result','--value']).decode())"
            )
            state = remote(client, login, args.user, script).decode().split()
            if "failed" in state:
                raise RuntimeError("Installer failed; retain remote install log for diagnosis")
            if "inactive" in state and "success" in state:
                break
            time.sleep(5)
        else:
            raise RuntimeError("Install still pending; do not reflash or clear logs")
        check = """import subprocess
subprocess.run(['systemctl','is-active','--quiet','rp-ylx'],check=True)
subprocess.run(['systemctl','daemon-reload'],check=True)
subprocess.run(['alsactl','restore'],check=True)
print('installed')
"""
        remote(client, login, args.user, check)
        print("Software installed. Reboot to activate restored network, hostname and machine-id.")
        print("DHCP may assign a different address. Check the router or use Ethernet.")
        if input("Reboot now? Type REBOOT: ") == "REBOOT":
            remote(
                client,
                login,
                args.user,
                "import subprocess; subprocess.run(['shutdown','-r','+1'],check=True)",
            )
            print("Reboot scheduled in one minute. Verify network and recording after boot.")
    finally:
        client.close()


def parser():
    p = argparse.ArgumentParser(description=__doc__)
    sub = p.add_subparsers(dest="action", required=True)
    sub.add_parser("disks", help="list eligible external whole disks")
    c = sub.add_parser("prepare-image", help="download and verify official system image")
    c.add_argument("--directory", type=Path, default=Path(__file__).parent / "images")
    for action in ("compose", "auto-flash"):
        c = sub.add_parser(action, help="prepare offline first-boot recovery image")
        c.add_argument("--image", required=True, type=Path)
        c.add_argument("--sha256", required=True)
        c.add_argument("--bundle", required=True, type=Path)
        c.add_argument("--bundle-sha256", required=True)
        c.add_argument("--profile", required=True, type=Path)
        c.add_argument("--output", required=True, type=Path)
        if action == "auto-flash":
            c.add_argument("--disk", required=True)
            c.add_argument("--full-erase", action="store_true")
    for action in ("erase", "flash"):
        c = sub.add_parser(action)
        c.add_argument("--disk", required=True)
        if action == "flash":
            c.add_argument("--image", type=Path, required=True)
            c.add_argument("--sha256", required=True)
            c.add_argument(
                "--quick", action="store_true", help="Write image without full-card erasure"
            )
    for action in ("backup", "restore"):
        c = sub.add_parser(action)
        c.add_argument("--host", required=True)
        c.add_argument("--user", default="sunrise")
        c.add_argument("--profile", type=Path, required=True)
        if action == "restore":
            c.add_argument("--bundle", type=Path, required=True)
            c.add_argument("--bundle-sha256", required=True)
    return p


def wizard():
    print("OpenAria 整卡恢复工具（待实卡验收）")
    print("0. 准备系统镜像  1. 备份配置  2. 整卡清空  3. 仅刷系统  4. 手动恢复")
    print("5. 一键刷机（系统＋配置＋离线软件，开机自动恢复）")
    choice = input("选择步骤 [0-5]: ").strip()
    if choice == "5":
        for d in disks.discover():
            print(d.path, d.size // 1024**3, "GiB", d.model, d.identity)
        args = [
            "auto-flash",
            "--disk",
            input("目标整卡路径: ").strip(),
            "--profile",
            input("加密配置备份 .oar 路径: ").strip(),
            "--output",
            input("电脑上的新刷机镜像路径（不可已存在）: ").strip(),
        ]
        prepared = Path(__file__).parent / "images" / "image.json"
        if prepared.is_file():
            info = json.loads(prepared.read_bytes())
            args += [
                "--image",
                str(prepared.parent / Path(info["image"]).name),
                "--sha256",
                info["sha256"],
            ]
        else:
            args += [
                "--image",
                input("官方系统 .img 路径: ").strip(),
                "--sha256",
                input("可信镜像 SHA-256（见image.json）: ").strip(),
            ]
        bundled = Path(__file__).with_name("firmware.json")
        if bundled.is_file():
            info = json.loads(bundled.read_bytes())
            args += [
                "--bundle",
                str(bundled.parent / "bundle.tar"),
                "--bundle-sha256",
                info["sha256"],
            ]
        else:
            args += [
                "--bundle",
                input("离线软件 bundle.tar 路径: ").strip(),
                "--bundle-sha256",
                input("可信软件包 SHA-256: ").strip(),
            ]
        return args
    if choice == "0":
        return ["prepare-image"]
    if choice in {"1", "4"}:
        action = "backup" if choice == "1" else "restore"
        args = [
            action,
            "--host",
            input("设备当前 IP: ").strip(),
            "--user",
            input("SSH 用户 [sunrise]: ").strip() or "sunrise",
            "--profile",
            input("电脑上的配置备份文件路径: ").strip(),
        ]
        if choice == "4":
            bundled = Path(__file__).with_name("firmware.json")
            if bundled.is_file():
                info = json.loads(bundled.read_bytes())
                print("Using bundled firmware:", info["commit"])
                args += [
                    "--bundle",
                    str(bundled.parent / "bundle.tar"),
                    "--bundle-sha256",
                    info["sha256"],
                ]
            else:
                args += [
                    "--bundle",
                    input("离线软件 bundle.tar 路径: ").strip(),
                    "--bundle-sha256",
                    input("可信 bundle SHA-256: ").strip(),
                ]
        return args
    if choice in {"2", "3"}:
        for d in disks.discover():
            print(d.path, d.size // 1024**3, "GiB", d.model, d.identity)
        args = ["erase" if choice == "2" else "flash", "--disk", input("目标整卡路径: ").strip()]
        if choice == "3":
            if input("快速刷写？跳过整卡清空，可能残留旧数据 [y/N]: ").strip().lower() == "y":
                args.append("--quick")
            prepared = Path(__file__).parent / "images" / "image.json"
            if prepared.is_file():
                info = json.loads(prepared.read_bytes())
                image = prepared.parent / Path(info["image"]).name
                args += ["--image", str(image), "--sha256", info["sha256"]]
            else:
                args += [
                    "--image",
                    input("已解压 RDK X5 .img 路径: ").strip(),
                    "--sha256",
                    input("可信镜像 SHA-256: ").strip(),
                ]
        return args
    raise RuntimeError("Invalid menu selection")


def main():
    args = parser().parse_args(wizard() if len(sys.argv) == 1 else None)
    if args.action == "disks":
        from dataclasses import asdict

        print(json.dumps([asdict(d) for d in disks.discover()], ensure_ascii=False, indent=2))
    elif args.action == "prepare-image":
        prepare_image.prepare(args.directory)
    elif args.action == "backup":
        backup(args)
    elif args.action == "restore":
        restore(args)
    elif args.action in {"compose", "auto-flash"}:
        disk = disks.select(args.disk) if args.action == "auto-flash" else None
        profile = json.loads(
            decrypt(args.profile.read_bytes(), getpass.getpass("Backup encryption password: "))
        )
        receipt = provision.compose(
            args.image,
            args.sha256.lower(),
            args.bundle,
            args.bundle_sha256.lower(),
            profile,
            args.output,
        )
        print(json.dumps(receipt))
        if disk is not None:
            disks.operate(disk, args.output, receipt["sha256"], quick=not args.full_erase)
            print(
                "Put the card in the original board. Offline restore runs on boot and reboots once."
            )
    else:
        disks.operate(
            disks.select(args.disk),
            getattr(args, "image", None),
            getattr(args, "sha256", None),
            quick=getattr(args, "quick", False),
        )


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print(
            "Interrupted. Card may be incomplete; repeat the operation before use.", file=sys.stderr
        )
        sys.exit(130)
    except Exception as error:
        # Do not echo secrets from SSH, decryption errors or configuration parsing.
        print(
            "Operation failed (" + type(error).__name__ + "). No success claimed.", file=sys.stderr
        )
        sys.exit(1)

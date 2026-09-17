"""Persistent, privileged firmware supervisor. Standard library / Python 3.10.

Only fixed update/rollback commands are accepted over a group-restricted Unix
socket. The supervisor survives capture/network service restarts. Task receipts
are persisted before acknowledgement, and request keys survive browser reloads.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import signal
import socket
import socketserver
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path

SOCKET = "/run/rp-ylx/firmware-control.sock"
STATE = Path("/var/lib/openaria-update")
INSTALL = Path("/opt/rp-ylx")
LIMIT = 64 * 1024
TTL = 1200
SEMVER = re.compile(r"^0\.2\.([1-9][0-9]*|0)$")
COMMIT = re.compile(r"^[0-9a-f]{40}$")
KEY = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$")


class FirmwareError(RuntimeError):
    def __init__(self, code: str, message: str, status: int = 409):
        self.code, self.message, self.status = code, message, status
        super().__init__(message)


def updater_module():
    if __package__:
        from rp_ylx import update
    else:
        import update
    return update


def atomic_json(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    with tempfile.NamedTemporaryFile(mode="w", dir=path.parent, delete=False) as stream:
        temporary = Path(stream.name)
        json.dump(value, stream, ensure_ascii=False)
        stream.flush()
        os.fsync(stream.fileno())
    try:
        os.replace(temporary, path)
        fd = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
    finally:
        temporary.unlink(missing_ok=True)


def installed_release(name: str, root: Path = INSTALL) -> dict | None:
    link = root / name
    if not link.is_symlink():
        return None
    target = link.resolve()
    if target.parent != (root / "releases").resolve() or not COMMIT.fullmatch(target.name):
        raise FirmwareError("installed_release_invalid", "设备版本目录身份无效", 503)
    metadata = json.loads((target / "release.json").read_bytes())
    if metadata.get("commit") != target.name or not isinstance(metadata.get("version"), str):
        raise FirmwareError("installed_release_invalid", "设备版本元数据无效", 503)
    return {"version": metadata["version"], "commit": target.name}


def newer(available: str, current: str) -> bool:
    def parts(value):
        if not re.fullmatch(r"[0-9]+\.[0-9]+\.[0-9]+", value):
            raise FirmwareError("version_invalid", "版本号无效", 503)
        return tuple(map(int, value.split(".")))

    return parts(available) > parts(current)


class FirmwareSupervisor:
    def __init__(self, state: Path = STATE, install: Path = INSTALL, runner=None, on_success=None):
        self.state, self.install = state, install
        self.runner = runner or self._run_update
        self.on_success = on_success or (lambda: None)
        self.lock = threading.RLock()
        self.start_lock = threading.Lock()
        self.check_lock = threading.Lock()
        self.cache = None
        self.checked_at = 0.0
        self.warning = None
        self.task = None
        path = self.state / "task.json"
        if path.exists():
            self.task = json.loads(path.read_bytes())
            if self.task["status"] in {"accepted", "running"}:
                self.task.update(
                    status="failed",
                    message="更新服务曾中断，请检查当前版本后重试",
                    finished_at=int(time.time()),
                )
                self._save_task()

    def _release(self, name):
        return installed_release(name, self.install)

    def status(self) -> dict:
        with self.lock:
            current, previous = self._release("current"), self._release("previous")
            available = self.cache
            has_update = bool(
                current
                and available
                and (
                    newer(available["version"], current["version"])
                    or (
                        available["version"] == current["version"]
                        and available["commit"] != current["commit"]
                    )
                )
            )
            return {
                "schema": "openaria.firmware-status.v1",
                "supported": current is not None,
                "current": current,
                "previous": previous,
                "available": available,
                "has_update": has_update,
                "checked_at": self.checked_at or None,
                "warning": self.warning,
                "task": dict(self.task) if self.task else None,
            }

    def check(self, force=False) -> dict:
        with self.check_lock:
            if not force and self.cache and time.time() - self.checked_at < TTL:
                return self.status()
            try:
                update = updater_module()
                with tempfile.TemporaryDirectory() as directory:
                    manifest = update.fetch_manifest(update.DEFAULT_MANIFEST, Path(directory))
                if not SEMVER.fullmatch(manifest["version"]) or manifest["version"] == "0.2.0":
                    raise ValueError("channel version outside 0.2.x")
                notes = manifest.get("release_notes", "")
                if not isinstance(notes, str) or len(notes) > 16000:
                    raise ValueError("invalid release notes")
                with self.lock:
                    self.cache = {
                        "version": manifest["version"],
                        "commit": manifest["commit"],
                        "release_notes": notes,
                        "published_at": manifest.get("published_at"),
                    }
                    self.checked_at = int(time.time())
                    self.warning = None
            except (OSError, ValueError, KeyError, TypeError):
                self.warning = "无法取得有效的 0.2.x 发布清单，请检查设备互联网连接或稍后重试"
                return self.status()
            return self.status()

    def start(self, action: str, key: str, commit: str) -> dict:
        if (
            action not in {"update", "rollback"}
            or not KEY.fullmatch(key)
            or not COMMIT.fullmatch(commit)
        ):
            raise FirmwareError("invalid_request", "更新请求无效", 400)
        with self.start_lock:
            # Retain bounded receipts separately so a delayed retry cannot start
            # an old operation again after another operation has completed.
            receipt = self.state / "receipts" / (key + ".json")
            if receipt.exists():
                old = json.loads(receipt.read_bytes())
                if old["action"] != action or old["target_commit"] != commit:
                    raise FirmwareError("idempotency_conflict", "该请求标识已用于不同任务")
                return old
            if self.task and self.task["status"] in {"accepted", "running"}:
                raise FirmwareError("update_busy", "已有更新任务正在执行")
            if self._release("current") is None:
                raise FirmwareError("update_unsupported", "当前安装方式不支持在线更新")
            if action == "update":
                checked = self.check(force=True)
                if checked["warning"]:
                    raise FirmwareError("update_check_failed", checked["warning"], 503)
                if not checked["has_update"] or checked["available"]["commit"] != commit:
                    raise FirmwareError("release_changed", "可用版本已变化，请重新检查更新")
            else:
                previous = self._release("previous")
                if previous is None or previous["commit"] != commit:
                    raise FirmwareError("rollback_unavailable", "上一版本已变化或不可回退")
            try:
                updater_module().require_idle()
            except (OSError, ValueError, KeyError) as error:
                raise FirmwareError("capture_active", "请先停止录制并等待设备空闲") from error
            self.task = {
                "id": key,
                "action": action,
                "target_commit": commit,
                "status": "accepted",
                "message": "更新任务已接收",
                "started_at": int(time.time()),
                "finished_at": None,
            }
            self._save_task()
            result = dict(self.task)
            threading.Thread(target=self._execute, daemon=True).start()
            return result

    def _save_task(self):
        atomic_json(self.state / "task.json", self.task)
        receipts = self.state / "receipts"
        atomic_json(receipts / (self.task["id"] + ".json"), self.task)
        # Keep receipts for 30 days, including failures. Never prune active work.
        for path in receipts.glob("*.json"):
            if path.stem != self.task["id"] and path.stat().st_mtime < time.time() - 30 * 86400:
                path.unlink()

    def _execute(self):
        with self.lock:
            self.task.update(status="running", message="正在下载、校验并安装；设备服务将自动重启")
            self._save_task()
            task = dict(self.task)
        try:
            self.runner(task)
            current = self._release("current")
            if not current or current["commit"] != task["target_commit"]:
                raise RuntimeError("installed identity mismatch")
            status, message = "succeeded", "更新完成，设备服务已启动"
        except Exception:
            status, message = "failed", "更新未完成；已保留失败记录，请检查当前版本后重试"
        with self.lock:
            self.task.update(status=status, message=message, finished_at=int(time.time()))
            self._save_task()
        if status == "succeeded":
            self.on_success()

    def _run_update(self, task):
        command = ["/usr/bin/python3", "/usr/local/lib/openaria/update.py"]
        if task["action"] == "rollback":
            command += ["--rollback", "--expected-commit", task["target_commit"]]
        else:
            command += ["--expected-commit", task["target_commit"]]
        # Logs are root-only; arbitrary command output is never returned by API.
        with (
            (self.state / "last-update.log").open("wb") as log,
            subprocess.Popen(command, stdout=log, stderr=log, start_new_session=True) as process,
        ):
            try:
                code = process.wait(timeout=3600)
            except subprocess.TimeoutExpired:
                # Terminate installer descendants too before releasing the
                # task slot; an orphan must never race a later transaction.
                os.killpg(process.pid, signal.SIGKILL)
                process.wait()
                raise
            if code:
                raise RuntimeError("updater exited unsuccessfully")


def request_firmware(operation: str, **fields) -> dict:
    payload = json.dumps({"operation": operation, **fields}).encode() + b"\n"
    try:
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
            client.settimeout(75 if operation in {"check", "update"} else 20)
            client.connect(SOCKET)
            client.sendall(payload)
            with client.makefile("rb") as stream:
                raw = stream.readline(LIMIT + 1)
        if len(raw) > LIMIT:
            raise ValueError("oversized firmware response")
        value = json.loads(raw)
        if not value.get("ok"):
            error = value["error"]
            raise FirmwareError(error["code"], error["message"], error["status"])
        return value["body"]
    except (OSError, ValueError, KeyError, TypeError) as error:
        raise FirmwareError("update_service_unavailable", "设备更新服务暂时不可用", 503) from error


def dispatch(supervisor, request):
    if not isinstance(request, dict):
        raise FirmwareError("invalid_request", "更新请求无效", 400)
    operation = request.get("operation")
    if operation == "status" and set(request) == {"operation"}:
        return supervisor.status()
    if (
        operation == "check"
        and set(request) == {"operation", "force"}
        and type(request["force"]) is bool
    ):
        return supervisor.check(request["force"])
    if (
        operation in {"update", "rollback"}
        and set(request) == {"operation", "key", "commit"}
        and isinstance(request["key"], str)
        and isinstance(request["commit"], str)
    ):
        return supervisor.start(operation, request["key"], request["commit"])
    raise FirmwareError("invalid_request", "更新请求无效", 400)


class ControlHandler(socketserver.StreamRequestHandler):
    def handle(self):
        self.connection.settimeout(10)
        try:
            raw = self.rfile.readline(LIMIT + 1)
            if len(raw) > LIMIT:
                raise ValueError("request too large")
            result = {"ok": True, "body": dispatch(self.server.supervisor, json.loads(raw))}
        except FirmwareError as error:
            result = {
                "ok": False,
                "error": {"code": error.code, "message": error.message, "status": error.status},
            }
        except Exception:
            result = {
                "ok": False,
                "error": {
                    "code": "update_request_failed",
                    "message": "更新服务无法处理该请求",
                    "status": 503,
                },
            }
        self.wfile.write(json.dumps(result, ensure_ascii=False).encode() + b"\n")


class ControlServer(socketserver.ThreadingUnixStreamServer):
    daemon_threads = True


def main():
    argparse.ArgumentParser(description="OpenAria firmware supervisor").parse_args()
    os.umask(0o077)
    STATE.mkdir(parents=True, exist_ok=True, mode=0o700)
    path = Path(SOCKET)
    path.unlink(missing_ok=True)
    with ControlServer(SOCKET, ControlHandler) as server:
        os.chmod(SOCKET, 0o660)
        server.supervisor = FirmwareSupervisor(
            on_success=lambda: subprocess.run(
                ["systemctl", "--no-block", "try-restart", "openaria-update.service"],
                check=False,
                timeout=10,
            )
        )
        server.serve_forever()


if __name__ == "__main__":
    sys.exit(main())

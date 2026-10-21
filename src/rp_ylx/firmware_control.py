"""Persistent, privileged firmware supervisor. Standard library / Python 3.10.

Only fixed update/rollback commands are accepted over a group-restricted Unix
socket. The supervisor survives capture/network service restarts. Task receipts
are persisted before acknowledgement, and request keys survive browser reloads.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import re
import signal
import socket
import socketserver
import stat
import subprocess
import sys
import tempfile
import threading
import time
from contextlib import suppress
from pathlib import Path

SOCKET = "/run/rp-ylx/firmware-control.sock"
STATE = Path("/var/lib/openaria-update")
INSTALL = Path("/opt/rp-ylx")
LIMIT = 64 * 1024
MAX_CONTROL_DEPTH = 64
ERROR_CODE = re.compile(r"^[a-z][a-z0-9_]{0,127}$")
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
    updater_module().atomic_json(path, value)


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


def _validate_task_record(value, expected_key=None):
    fields = {"id", "action", "target_commit", "status", "message", "started_at", "finished_at"}
    if not isinstance(value, dict) or set(value) != fields:
        raise ValueError("invalid firmware task fields")
    if (
        not isinstance(value["id"], str)
        or KEY.fullmatch(value["id"]) is None
        or (expected_key is not None and value["id"] != expected_key)
        or value["action"] not in ("update", "rollback")
        or not isinstance(value["target_commit"], str)
        or COMMIT.fullmatch(value["target_commit"]) is None
        or value["status"] not in ("accepted", "running", "failed", "succeeded")
        or not isinstance(value["message"], str)
        or not 1 <= len(value["message"]) <= 4096
        or type(value["started_at"]) is not int
        or not 0 <= value["started_at"] <= 2**63 - 1
    ):
        raise ValueError("invalid firmware task values")
    value["message"].encode("utf-8")
    finished = value["finished_at"]
    if value["status"] in ("accepted", "running"):
        if finished is not None:
            raise ValueError("unfinished firmware task has end time")
    elif type(finished) is not int or not 0 <= finished <= 2**63 - 1:
        raise ValueError("finished firmware task lacks end time")
    # Wall-clock corrections can put finished_at before started_at.
    return value


def _read_task_record(path, *, expected_key=None, missing_ok=False):
    try:
        before = path.lstat()
    except FileNotFoundError:
        if missing_ok:
            return None
        raise

    def identity(info):
        if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or info.st_size > LIMIT:
            raise ValueError("unsafe firmware task file")
        return (
            info.st_dev,
            info.st_ino,
            info.st_mode,
            info.st_nlink,
            info.st_size,
            info.st_mtime_ns,
            info.st_ctime_ns,
        )

    expected = identity(before)
    fd = os.open(path, os.O_RDONLY | os.O_CLOEXEC | os.O_NONBLOCK | os.O_NOFOLLOW)
    try:
        if identity(os.fstat(fd)) != expected:
            raise ValueError("firmware task changed before read")
        raw = bytearray()
        while len(raw) <= LIMIT:
            chunk = os.read(fd, min(8192, LIMIT + 1 - len(raw)))
            if not chunk:
                break
            raw.extend(chunk)
        if len(raw) > LIMIT or identity(os.fstat(fd)) != expected:
            raise ValueError("firmware task changed during read")
        if identity(path.lstat()) != expected:
            raise ValueError("firmware task replaced during read")
    finally:
        os.close(fd)
    value = json.loads(
        raw.decode("utf-8"),
        object_pairs_hook=_unique_control_object,
        parse_constant=_reject_control_constant,
    )
    _validate_control_json(value)
    return _validate_task_record(value, expected_key)


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
        self._worker = None
        self._task_save_pending = False
        self._success_notification_pending = False
        self._callback_active = False
        self._task_load_pending = False
        self._restore_task_state(allow_missing=True)

    def _restore_task_state(self, *, allow_missing=False):
        try:
            task = _read_task_record(self.state / "task.json", missing_ok=allow_missing)
        except (OSError, ValueError, TypeError, RecursionError):
            self._task_load_pending = True
            return False
        self._task_load_pending = False
        if task is None:
            return True
        self.task = task
        if task["status"] in {"accepted", "running"}:
            self._finish_task("failed", "更新服务曾中断，请检查当前版本后重试")
        # Repair task and receipt together, but never execute restored work.
        self._task_save_pending = True
        self._try_save_task()
        return True

    def _release(self, name):
        return installed_release(name, self.install)

    def status(self) -> dict:
        self._repair_task_state()
        with self.lock:
            current, previous = self._release("current"), self._release("previous")
            available = self.cache
            sqlite_state = updater_module().sqlite_state_required()
            has_update = bool(
                not self._task_load_pending
                and current
                and available
                and (
                    newer(available["version"], current["version"])
                    or (
                        available["version"] == current["version"]
                        and available["commit"] != current["commit"]
                        and not sqlite_state
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
                "warning": (
                    "更新任务状态无法读取或校验，已暂停更新；请修复保留的任务状态文件"
                    if self._task_load_pending
                    else "更新任务状态尚未完成持久保存，请检查存储后重试"
                    if self._task_save_pending
                    else "更新已完成，但更新服务重新加载尚未完成"
                    if self._success_notification_pending or self._callback_active
                    else self.warning
                )
                or (
                    "设备已使用 SQLite，同版本旧固件不可直接替换；请使用支持 SQLite 的新版本。"
                    if sqlite_state
                    and available
                    and current
                    and available["version"] == current["version"]
                    and available["commit"] != current["commit"]
                    else None
                ),
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
                    with self.lock:
                        self.cache = None
                        self.checked_at = int(time.time())
                        self.warning = (
                            "已连接 OSS，但公共渠道尚无兼容的 0.2.x 正式版本；"
                            "请等待正式发布后重新检查。当前固件不会降级。"
                        )
                    return self.status()
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
            except OSError:
                self.warning = "无法连接 OSS 发布源，请检查设备互联网连接后重试"
                return self.status()
            except (ValueError, KeyError, TypeError):
                self.warning = "OSS 发布清单校验失败，请联系维护者检查发布内容后重试"
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
            self._repair_task_state()
            with self.lock:
                if self._task_load_pending:
                    raise FirmwareError(
                        "update_state_unavailable", "更新任务状态尚未恢复，请检查状态文件", 503
                    )
                # The in-memory task is authoritative while an I/O repair is
                # pending; a stale accepted receipt must never win over it.
                if self.task and self.task["id"] == key:
                    self._check_task_identity(self.task, action, commit)
                    return dict(self.task)
                receipt = self.state / "receipts" / (key + ".json")
                try:
                    old = _read_task_record(receipt, expected_key=key, missing_ok=True)
                except (OSError, ValueError, TypeError, RecursionError) as error:
                    raise FirmwareError(
                        "update_state_unavailable",
                        "更新任务回执无法读取或校验，请检查状态文件",
                        503,
                    ) from error
                if old is not None:
                    self._check_task_identity(old, action, commit)
                    if old["status"] in {"accepted", "running"}:
                        # No current worker owns this historical task. Keep
                        # its idempotency receipt, but never replay ghost work.
                        old.update(
                            status="failed",
                            message="更新任务曾中断，请检查当前版本后重试",
                            finished_at=int(time.time()),
                        )
                        try:
                            atomic_json(receipt, old)
                        except OSError as error:
                            raise FirmwareError(
                                "update_state_unavailable", "更新任务回执尚未恢复，请检查存储", 503
                            ) from error
                    return old
                if self._task_save_pending or self._success_notification_pending:
                    raise FirmwareError(
                        "update_state_unavailable", "更新任务状态尚未恢复，请稍后重试", 503
                    )
                if self._worker is not None or self._callback_active:
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
                if (
                    updater_module().sqlite_state_required()
                    and not (
                        self.install / "releases" / commit / "site-packages/rp_ylx/persistence.py"
                    ).is_file()
                ):
                    raise FirmwareError(
                        "state_schema_incompatible",
                        "上一版本不支持 SQLite，必须离线恢复配套状态后才能回退",
                    )
            try:
                updater_module().require_idle()
            except (OSError, ValueError, KeyError) as error:
                raise FirmwareError("capture_active", "请先停止录制并等待设备空闲") from error
            gate = threading.Event()
            admitted = False

            def run_admitted():
                try:
                    gate.wait()
                    if admitted:
                        self._execute()
                finally:
                    with self.lock:
                        if self._worker is threading.current_thread():
                            self._worker = None

            with self.lock:
                self.task = {
                    "id": key,
                    "action": action,
                    "target_commit": commit,
                    "status": "accepted",
                    "message": "更新任务已接收",
                    "started_at": int(time.time()),
                    "finished_at": None,
                }
                try:
                    self._save_task()
                    worker = threading.Thread(target=run_admitted, daemon=True)
                    self._worker = worker
                    worker.start()
                except BaseException as error:
                    self._worker = None
                    self._finish_task("failed", "更新任务未能启动，请检查设备状态后重试")
                    # Preserve the primary failure/cancellation even when the
                    # terminal receipt cannot currently be saved.
                    with suppress(BaseException):
                        self._save_task()
                    if isinstance(error, Exception):
                        raise FirmwareError(
                            "update_start_failed", "更新任务未能启动，请稍后重试", 503
                        ) from error
                    raise
                else:
                    result = dict(self.task)
                    admitted = True
                    return result
                finally:
                    # A start() that raises after creating a real thread must
                    # release it without ever granting permission to install.
                    gate.set()

    @staticmethod
    def _check_task_identity(task, action, commit):
        if task["action"] != action or task["target_commit"] != commit:
            raise FirmwareError("idempotency_conflict", "该请求标识已用于不同任务")

    def _finish_task(self, status, message):
        self.task.update(status=status, message=message, finished_at=int(time.time()))
        self._task_save_pending = True

    def _save_task(self):
        self._task_save_pending = True
        atomic_json(self.state / "task.json", self.task)
        receipts = self.state / "receipts"
        atomic_json(receipts / (self.task["id"] + ".json"), self.task)
        self._task_save_pending = False
        # Pruning old history is best-effort; failure must not invalidate an
        # already published task or prevent its worker from being admitted.
        with suppress(OSError):
            for path in receipts.glob("*.json"):
                if path.stem != self.task["id"] and path.stat().st_mtime < time.time() - 30 * 86400:
                    path.unlink()

    def _try_save_task(self):
        try:
            self._save_task()
        except Exception:
            return False
        return True

    def _repair_task_state(self):
        with self.lock:
            if self._task_load_pending and not self._restore_task_state():
                return
            if self._task_save_pending and not self._try_save_task():
                return
            if not self._success_notification_pending or self._callback_active:
                return
            self._callback_active = True
            self._success_notification_pending = False
        try:
            self.on_success()
        except BaseException as error:
            with self.lock:
                self._success_notification_pending = True
            if not isinstance(error, Exception):
                raise
        finally:
            with self.lock:
                self._callback_active = False

    def _execute(self):
        try:
            with self.lock:
                self.task.update(
                    status="running", message="正在下载、校验并安装；设备服务将自动重启"
                )
                self._save_task()
                task = dict(self.task)
            self.runner(task)
            current = self._release("current")
            if not current or current["commit"] != task["target_commit"]:
                raise RuntimeError("installed identity mismatch")
        except BaseException as error:
            with self.lock:
                self._finish_task("failed", "更新未完成；请检查当前版本和设备存储后重试")
                with suppress(BaseException):
                    self._save_task()
            if not isinstance(error, Exception):
                raise
        else:
            with self.lock:
                self._finish_task("succeeded", "更新完成，设备服务已启动")
                self._success_notification_pending = True
            self._repair_task_state()

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


def _unique_control_object(pairs):
    value = {}
    for key, item in pairs:
        if key in value:
            raise ValueError("duplicate control field")
        value[key] = item
    return value


def _reject_control_constant(_value):
    raise ValueError("non-finite control number")


def _validate_control_json(value):
    pending = [(value, 0)]
    while pending:
        item, depth = pending.pop()
        if depth > MAX_CONTROL_DEPTH:
            raise ValueError("control JSON nesting is too deep")
        if isinstance(item, dict):
            for key, child in item.items():
                if not isinstance(key, str):
                    raise ValueError("invalid control object key")
                key.encode("utf-8")
                pending.append((child, depth + 1))
        elif isinstance(item, list):
            pending.extend((child, depth + 1) for child in item)
        elif isinstance(item, str):
            item.encode("utf-8")
        elif isinstance(item, float):
            if not math.isfinite(item):
                raise ValueError("non-finite control number")
        elif item is not None and type(item) not in {bool, int}:
            raise ValueError("invalid control JSON value")


def _decode_control_frame(raw):
    if len(raw) > LIMIT or not raw.endswith(b"\n"):
        raise ValueError("oversized or incomplete control frame")
    value = json.loads(
        raw.decode("utf-8"),
        object_pairs_hook=_unique_control_object,
        parse_constant=_reject_control_constant,
    )
    _validate_control_json(value)
    return value


def _validate_control_response(value):
    if not isinstance(value, dict) or type(value.get("ok")) is not bool:
        raise ValueError("invalid control response envelope")
    if value["ok"]:
        if set(value) != {"ok", "body"} or not isinstance(value["body"], dict):
            raise ValueError("invalid control response body")
        return
    error = value.get("error")
    if (
        set(value) != {"ok", "error"}
        or not isinstance(error, dict)
        or set(error) != {"code", "message", "status"}
        or not isinstance(error["code"], str)
        or ERROR_CODE.fullmatch(error["code"]) is None
        or not isinstance(error["message"], str)
        or not error["message"]
        or type(error["status"]) is not int
        or not 400 <= error["status"] <= 599
    ):
        raise ValueError("invalid control error response")


def _control_failure_response():
    return {
        "ok": False,
        "error": {
            "code": "update_request_failed",
            "message": "更新服务无法处理该请求",
            "status": 503,
        },
    }


def _encode_control_response(value):
    _validate_control_json(value)
    _validate_control_response(value)
    raw = json.dumps(value, ensure_ascii=False, allow_nan=False).encode("utf-8") + b"\n"
    if len(raw) > LIMIT:
        raise ValueError("oversized control response")
    return raw


def request_firmware(operation: str, **fields) -> dict:
    try:
        payload = json.dumps({"operation": operation, **fields}, allow_nan=False).encode() + b"\n"
        if len(payload) > LIMIT:
            raise ValueError("oversized control request")
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
            client.settimeout(75 if operation in {"check", "update"} else 20)
            client.connect(SOCKET)
            # A saturated server can queue a refusal and close before our
            # write. Preserve that typed response even when send sees EPIPE.
            with suppress(BrokenPipeError, ConnectionResetError, ConnectionAbortedError):
                client.sendall(payload)
            with client.makefile("rb") as stream:
                raw = stream.readline(LIMIT + 1)
        value = _decode_control_frame(raw)
        _validate_control_response(value)
        if not value["ok"]:
            error = value["error"]
            raise FirmwareError(error["code"], error["message"], error["status"])
        return value["body"]
    except (OSError, ValueError, KeyError, TypeError, RecursionError) as error:
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
        try:
            raw = self.server.read_control_request(self.connection)
            if not raw:
                return
            request = _decode_control_frame(raw)
            result = {"ok": True, "body": self.server.dispatch_control(request)}
        except (ConnectionResetError, ConnectionAbortedError, TimeoutError):
            return
        except FirmwareError as error:
            result = {
                "ok": False,
                "error": {"code": error.code, "message": error.message, "status": error.status},
            }
        except Exception:
            result = _control_failure_response()
        try:
            raw = _encode_control_response(result)
        except (ValueError, TypeError, RecursionError):
            raw = _encode_control_response(_control_failure_response())
        self.connection.settimeout(self.server.request_timeout_seconds)
        with suppress(BrokenPipeError, ConnectionResetError, ConnectionAbortedError, TimeoutError):
            self.wfile.write(raw)


class ControlServer(socketserver.ThreadingUnixStreamServer):
    daemon_threads = True

    def __init__(
        self,
        server_address,
        RequestHandlerClass,
        bind_and_activate=True,
        *,
        max_connections=16,
        request_timeout_seconds=10.0,
        close_timeout_seconds=2.0,
    ):
        if type(max_connections) is not int or max_connections < 1:
            raise ValueError("max_connections must be a positive integer")
        for value in (request_timeout_seconds, close_timeout_seconds):
            if type(value) not in {int, float} or not math.isfinite(value) or value <= 0:
                raise ValueError("control timeouts must be finite positive numbers")
        self.max_connections = max_connections
        self.request_timeout_seconds = request_timeout_seconds
        self.close_timeout_seconds = close_timeout_seconds
        self._control_condition = threading.Condition()
        self._control_connections = {}
        self._control_workers = {}
        self._control_closing = False
        super().__init__(server_address, RequestHandlerClass, bind_and_activate)

    def _release_control_request(self, request):
        with self._control_condition:
            self._control_connections.pop(request, None)
            self._control_workers.pop(request, None)
            self._control_condition.notify_all()

    def _reject_control_request(self, request):
        try:
            # A rejected peer must not allocate a worker or block the acceptor.
            with suppress(OSError):
                request.settimeout(0.1)
                request.sendall(
                    _encode_control_response(
                        {
                            "ok": False,
                            "error": {
                                "code": "update_service_busy",
                                "message": "更新服务连接繁忙，请稍后重试",
                                "status": 503,
                            },
                        }
                    )
                )
        finally:
            self.shutdown_request(request)

    def process_request(self, request, client_address):
        worker = None
        admitted = False
        gate = None
        try:
            with self._control_condition:
                if self._control_closing or len(self._control_connections) >= self.max_connections:
                    rejected = True
                else:
                    rejected = False
                    self._control_connections[request] = (
                        time.monotonic() + self.request_timeout_seconds
                    )
                    try:
                        gate = threading.Event()

                        def serve_admitted():
                            try:
                                gate.wait()
                                if admitted:
                                    self.process_request_thread(request, client_address)
                            finally:
                                self._release_control_request(request)

                        worker = threading.Thread(target=serve_admitted, daemon=True)
                        self._control_workers[request] = worker
                        worker.start()
                        admitted = True
                    finally:
                        if gate is not None:
                            gate.set()
        except BaseException as error:
            self._release_control_request(request)
            try:
                if (
                    worker is not None
                    and worker is not threading.current_thread()
                    and worker.ident is not None
                ):
                    with suppress(Exception):
                        worker.join(self.close_timeout_seconds)
            finally:
                if isinstance(error, Exception):
                    self._reject_control_request(request)
                else:
                    with suppress(OSError):
                        self.shutdown_request(request)
            if isinstance(error, Exception):
                return
            raise
        if rejected:
            self._reject_control_request(request)

    def read_control_request(self, connection):
        with self._control_condition:
            deadline = self._control_connections[connection]
        raw = bytearray()
        while len(raw) <= LIMIT:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError("control request deadline expired")
            connection.settimeout(remaining)
            chunk = connection.recv(min(8192, LIMIT + 1 - len(raw)))
            if not chunk:
                break
            newline = chunk.find(b"\n")
            if newline >= 0:
                raw.extend(chunk[: newline + 1])
                break
            raw.extend(chunk)
        return bytes(raw)

    def dispatch_control(self, request):
        with self._control_condition:
            if self._control_closing:
                raise FirmwareError("update_service_unavailable", "更新服务正在关闭", 503)
        # Once admitted here, application work may finish even if the caller
        # disconnects. Do not hold the connection registry lock during updates.
        return dispatch(self.supervisor, request)

    def server_close(self):
        deadline = time.monotonic() + self.close_timeout_seconds
        with self._control_condition:
            self._control_closing = True
            connections = list(self._control_connections)
            workers = list(self._control_workers.values())
        super().server_close()
        for connection in connections:
            with suppress(OSError):
                connection.shutdown(socket.SHUT_RDWR)
        for worker in workers:
            if worker is not threading.current_thread() and worker.ident is not None:
                worker.join(max(0.0, deadline - time.monotonic()))
        # Slow application calls cannot be killed safely. Their ownership stays
        # registered until they exit; later close calls may finish draining them.


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
                check=True,
                timeout=10,
            )
        )
        server.serve_forever()


if __name__ == "__main__":
    sys.exit(main())

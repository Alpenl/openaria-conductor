from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import platform
import select
import shutil
import signal
import socket
import ssl
import struct
import subprocess
import sys
import tempfile
import threading
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
EXPERIMENT_ROOT = Path(__file__).resolve().parent
DEFAULT_HARNESS = EXPERIMENT_ROOT / "harness/target/release/gateway-runtime-harness"
PYTHON_FAKE = EXPERIMENT_ROOT / "python_control_fake.py"
PREVIEW_MARKER = b"\xff\xd9\r\n"
SSE_MARKER = b"\n\n"


class ExperimentFailure(RuntimeError):
    pass


class RawClient:
    def __init__(self, port: int, *, tls: bool) -> None:
        connection = socket.create_connection(("127.0.0.1", port), timeout=2)
        if tls:
            context = ssl.create_default_context()
            context.check_hostname = False
            context.verify_mode = ssl.CERT_NONE
            self.socket = context.wrap_socket(connection, server_hostname="localhost")
        else:
            self.socket = connection
        self.socket.settimeout(3)

    def request(
        self,
        path: str,
        marker: bytes,
        *,
        accept: str = "application/json",
        last_event_id: str | None = None,
    ) -> bytes:
        cursor = "" if last_event_id is None else f"Last-Event-ID: {last_event_id}\r\n"
        request = (
            f"GET {path} HTTP/1.1\r\n"
            "Host: 127.0.0.1\r\n"
            "Authorization: Bearer reader-token\r\n"
            f"Accept: {accept}\r\n"
            f"{cursor}"
            "Connection: close\r\n\r\n"
        ).encode("ascii")
        self.socket.sendall(request)
        response = bytearray()
        while marker not in response:
            chunk = self.socket.recv(4096)
            if not chunk:
                break
            response.extend(chunk)
            if len(response) > 512 * 1024:
                raise ExperimentFailure("response exceeded the experiment bound")
        return bytes(response)

    def abort(self) -> None:
        try:
            self.socket.setsockopt(
                socket.SOL_SOCKET,
                socket.SO_LINGER,
                struct.pack("ii", 1, 0),
            )
        finally:
            self.socket.close()

    def close(self) -> None:
        self.socket.close()


def assert_ok(response: bytes, context: str) -> None:
    if not response.startswith(b"HTTP/1.1 200 "):
        raise ExperimentFailure(f"{context}: expected HTTP 200, got {response[:160]!r}")


def complete_request(
    port: int,
    path: str,
    *,
    tls: bool,
    timeout: float = 3,
) -> tuple[int, bytes]:
    client = RawClient(port, tls=tls)
    client.socket.settimeout(timeout)
    try:
        response = client.request(path, b"\r\n\r\n")
        headers, separator, body = response.partition(b"\r\n\r\n")
        if not separator:
            raise ExperimentFailure(f"{path}: incomplete HTTP response")
        content_length = 0
        for line in headers.split(b"\r\n")[1:]:
            if line.lower().startswith(b"content-length:"):
                content_length = int(line.split(b":", 1)[1])
        while len(body) < content_length:
            chunk = client.socket.recv(4096)
            if not chunk:
                break
            body += chunk
        status = int(headers.split(b" ", 2)[1])
        return status, body
    finally:
        client.close()


def _proc_snapshot(pid: int) -> dict[str, int]:
    status: dict[str, int] = {}
    for line in Path(f"/proc/{pid}/status").read_text().splitlines():
        if line.startswith("VmRSS:"):
            status["rss_kib"] = int(line.split()[1])
        elif line.startswith("Threads:"):
            status["threads"] = int(line.split()[1])
    status["fds"] = len(list(Path(f"/proc/{pid}/fd").iterdir()))
    fields = Path(f"/proc/{pid}/stat").read_text().split()
    status["cpu_ticks"] = int(fields[13]) + int(fields[14])
    return status


class ResourceSampler:
    def __init__(self, pid: int) -> None:
        self.pid = pid
        self.baseline = _proc_snapshot(pid)
        self.peak = dict(self.baseline)
        self.end = dict(self.baseline)
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._sample, name="experiment-proc-sampler")

    def __enter__(self) -> ResourceSampler:
        self._thread.start()
        return self

    def _sample(self) -> None:
        while not self._stop.wait(0.005):
            try:
                snapshot = _proc_snapshot(self.pid)
            except FileNotFoundError:
                return
            for field in ("rss_kib", "threads", "fds", "cpu_ticks"):
                self.peak[field] = max(self.peak[field], snapshot[field])

    def __exit__(self, *args: object) -> None:
        self._stop.set()
        self._thread.join(timeout=1)
        try:
            self.end = _proc_snapshot(self.pid)
        except FileNotFoundError:
            self.end = dict(self.peak)

    def result(self) -> dict[str, Any]:
        ticks_per_second = os.sysconf("SC_CLK_TCK")
        return {
            "baseline": {key: self.baseline[key] for key in ("rss_kib", "threads", "fds")},
            "peak": {key: self.peak[key] for key in ("rss_kib", "threads", "fds")},
            "end": {key: self.end[key] for key in ("rss_kib", "threads", "fds")},
            "cpu_seconds": round(
                (self.end["cpu_ticks"] - self.baseline["cpu_ticks"]) / ticks_per_second,
                6,
            ),
        }


@dataclass
class ServerProcess:
    backend: str
    tls: bool
    process: subprocess.Popen[str]
    port: int

    @classmethod
    def start_rust(
        cls,
        harness: Path,
        *,
        tls: bool,
        certificate: Path,
        private_key: Path,
    ) -> ServerProcess:
        command = [
            str(harness),
            "--listen",
            "127.0.0.1:0",
            "--capacity",
            "1",
            "--python",
            sys.executable,
            "--python-fake",
            str(PYTHON_FAKE),
        ]
        if tls:
            command.extend(["--cert", str(certificate), "--key", str(private_key)])
        return cls._start("rust_harness", tls, command)

    @classmethod
    def start_python(
        cls,
        *,
        tls: bool,
        certificate: Path,
        private_key: Path,
    ) -> ServerProcess:
        command = [sys.executable, str(Path(__file__).resolve()), "--python-server"]
        if tls:
            command.extend(["--cert", str(certificate), "--key", str(private_key)])
        return cls._start("python_gateway", tls, command)

    @classmethod
    def _start(cls, backend: str, tls: bool, command: list[str]) -> ServerProcess:
        process = subprocess.Popen(
            command,
            cwd=ROOT,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        assert process.stdout is not None
        ready, _, _ = select.select([process.stdout], [], [], 15)
        if not ready:
            process.kill()
            _, stderr = process.communicate()
            raise ExperimentFailure(f"{backend} did not start: {stderr}")
        line = process.stdout.readline()
        try:
            startup = json.loads(line)
        except json.JSONDecodeError as error:
            process.kill()
            _, stderr = process.communicate()
            raise ExperimentFailure(f"{backend} invalid startup {line!r}: {stderr}") from error
        return cls(backend=backend, tls=tls, process=process, port=int(startup["port"]))

    def stop(self) -> tuple[float, dict[str, Any] | None]:
        started = time.perf_counter()
        if self.process.poll() is None:
            if self.backend == "rust_harness":
                status, _ = complete_request(self.port, "/shutdown", tls=self.tls)
                if status != 200:
                    raise ExperimentFailure(f"Rust shutdown returned {status}")
            else:
                self.process.terminate()
        try:
            stdout, stderr = self.process.communicate(timeout=3)
        except subprocess.TimeoutExpired as error:
            self.process.kill()
            stdout, stderr = self.process.communicate()
            raise ExperimentFailure(f"{self.backend} shutdown exceeded 3 seconds") from error
        elapsed = time.perf_counter() - started
        if self.process.returncode != 0:
            raise ExperimentFailure(
                f"{self.backend} exited {self.process.returncode}: {stderr.strip()}"
            )
        final_metrics = None
        for line in stdout.splitlines():
            try:
                candidate = json.loads(line)
            except json.JSONDecodeError:
                continue
            if "preview" in candidate:
                final_metrics = candidate
        return elapsed, final_metrics


def _route(index: int) -> tuple[str, str, bytes]:
    routes = (
        ("/api/v4/preview?fps=30", "multipart/x-mixed-replace", PREVIEW_MARKER),
        ("/api/v4/capture/events", "text/event-stream", SSE_MARKER),
        ("/api/v4/network/events", "text/event-stream", SSE_MARKER),
    )
    return routes[index % len(routes)]


def percentile(values: list[float], value: float) -> float:
    ordered = sorted(values)
    index = max(0, math.ceil(len(ordered) * value) - 1)
    return ordered[index]


def run_reconnect_workload(server: ServerProcess, count: int) -> dict[str, Any]:
    latencies_ms: list[float] = []
    sampler: ResourceSampler
    started = time.perf_counter()
    with ResourceSampler(server.process.pid) as sampler:
        for index in range(count):
            path, accept, marker = _route(index)
            request_started = time.perf_counter()
            client = RawClient(server.port, tls=server.tls)
            response = client.request(path, marker, accept=accept)
            latency_ms = (time.perf_counter() - request_started) * 1_000
            try:
                assert_ok(response, f"{server.backend} reconnect {index}")
                if latency_ms >= 250:
                    raise ExperimentFailure(
                        f"{server.backend} reconnect {index} took {latency_ms:.3f} ms"
                    )
            finally:
                client.abort()
            latencies_ms.append(latency_ms)
        time.sleep(0.25)
    resources = sampler.result()
    stop_seconds, final_metrics = server.stop()
    result: dict[str, Any] = {
        "connections": count,
        "elapsed_seconds": round(time.perf_counter() - started, 6),
        "latency_ms": {
            "mean": round(sum(latencies_ms) / len(latencies_ms), 6),
            "p95": round(percentile(latencies_ms, 0.95), 6),
            "max": round(max(latencies_ms), 6),
        },
        "resources": resources,
        "shutdown_seconds": round(stop_seconds, 6),
    }
    if final_metrics is not None:
        result["stream_metrics"] = final_metrics
        if server.backend == "rust_harness":
            for pool_name in ("preview", "sse"):
                pool = final_metrics[pool_name]
                if pool["active"] != 0 or pool["acquired"] != pool["released"]:
                    raise ExperimentFailure(f"{pool_name} permits did not drain: {pool}")
                if pool["rejected"] != 0:
                    raise ExperimentFailure(f"{pool_name} rejected reconnects: {pool}")
    return result


def run_parallel_control_requests(server: ServerProcess) -> tuple[list[int], float]:
    statuses: list[int] = []
    lock = threading.Lock()

    def request() -> None:
        status, _ = complete_request(server.port, "/control?delay_ms=400", tls=server.tls)
        with lock:
            statuses.append(status)

    threads = [threading.Thread(target=request) for _ in range(4)]
    threads[0].start()
    time.sleep(0.03)
    for thread in threads[1:]:
        thread.start()
    time.sleep(0.03)
    health_started = time.perf_counter()
    health_status, _ = complete_request(server.port, "/health", tls=server.tls)
    health_ms = (time.perf_counter() - health_started) * 1_000
    for thread in threads:
        thread.join(timeout=2)
        if thread.is_alive():
            raise ExperimentFailure("control request did not finish")
    if health_status != 200 or health_ms >= 250:
        raise ExperimentFailure(f"control fake blocked health for {health_ms:.3f} ms")
    if 503 not in statuses or 200 not in statuses:
        raise ExperimentFailure(f"bounded control queue was not observable: {statuses}")
    return sorted(statuses), health_ms


def run_rust_contract_case(server: ServerProcess) -> dict[str, Any]:
    first = RawClient(server.port, tls=server.tls)
    assert_ok(
        first.request("/preview", PREVIEW_MARKER, accept="multipart/x-mixed-replace"),
        "frozen preview first connection",
    )
    reconnect_started = time.perf_counter()
    first.abort()
    second = RawClient(server.port, tls=server.tls)
    second_response = second.request("/preview", PREVIEW_MARKER, accept="multipart/x-mixed-replace")
    reconnect_ms = (time.perf_counter() - reconnect_started) * 1_000
    assert_ok(second_response, "frozen preview reconnect")
    second.abort()
    if reconnect_ms >= 250:
        raise ExperimentFailure(f"frozen preview reconnect took {reconnect_ms:.3f} ms")

    first_events = RawClient(server.port, tls=server.tls)
    first_payload = first_events.request("/events", b"id: 1\n", accept="text/event-stream")
    assert_ok(first_payload, "initial SSE")
    first_events.abort()
    replay = RawClient(server.port, tls=server.tls)
    replay_payload = replay.request(
        "/events",
        b"id: 3\n",
        accept="text/event-stream",
        last_event_id="1",
    )
    replay.abort()
    replay_body = replay_payload.partition(b"\r\n\r\n")[2]
    if b"id: 1\n" in replay_body or b"id: 2\n" not in replay_body:
        raise ExperimentFailure(f"invalid Last-Event-ID replay: {replay_body!r}")

    slow = RawClient(server.port, tls=server.tls)
    assert_ok(slow.request("/slow", b"\r\n\r\n"), "slow stream")
    health_started = time.perf_counter()
    health_status, _ = complete_request(server.port, "/health", tls=server.tls)
    slow_health_ms = (time.perf_counter() - health_started) * 1_000
    slow.abort()
    if health_status != 200 or slow_health_ms >= 250:
        raise ExperimentFailure(f"slow stream blocked health for {slow_health_ms:.3f} ms")

    control_statuses, control_health_ms = run_parallel_control_requests(server)

    preview = RawClient(server.port, tls=server.tls)
    assert_ok(
        preview.request("/preview", PREVIEW_MARKER, accept="multipart/x-mixed-replace"),
        "shutdown preview",
    )
    events = RawClient(server.port, tls=server.tls)
    assert_ok(events.request("/events", b"id: 1\n", accept="text/event-stream"), "shutdown SSE")
    stop_seconds, final_metrics = server.stop()
    preview.close()
    events.close()
    if stop_seconds >= 2:
        raise ExperimentFailure(f"shutdown drain took {stop_seconds:.3f} seconds")
    if final_metrics is None:
        raise ExperimentFailure("Rust harness did not emit final metrics")
    for pool_name in ("preview", "sse"):
        pool = final_metrics[pool_name]
        if pool["active"] != 0 or pool["acquired"] != pool["released"]:
            raise ExperimentFailure(f"shutdown leaked {pool_name} permit: {pool}")
    return {
        "frozen_preview_reconnect_ms": round(reconnect_ms, 6),
        "last_event_id_replayed_ids": [2, 3],
        "slow_client_health_ms": round(slow_health_ms, 6),
        "control_queue_statuses": control_statuses,
        "control_fake_health_ms": round(control_health_ms, 6),
        "shutdown_seconds": round(stop_seconds, 6),
        "stream_metrics": final_metrics,
    }


def create_certificate(root: Path) -> tuple[Path, Path]:
    openssl = shutil.which("openssl")
    if openssl is None:
        raise ExperimentFailure("openssl is required for the TLS experiment")
    certificate = root / "device.crt"
    private_key = root / "device.key"
    subprocess.run(
        [
            openssl,
            "req",
            "-x509",
            "-newkey",
            "rsa:2048",
            "-nodes",
            "-days",
            "1",
            "-subj",
            "/CN=localhost",
            "-keyout",
            str(private_key),
            "-out",
            str(certificate),
        ],
        check=True,
        capture_output=True,
    )
    return certificate, private_key


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def source_identity() -> dict[str, Any]:
    commit = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    status = subprocess.run(
        ["git", "status", "--porcelain"],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    ).stdout
    return {"commit": commit, "dirty": bool(status)}


def run_parent(harness: Path, reconnects: int, tls_reconnects: int) -> dict[str, Any]:
    if not harness.is_file():
        raise ExperimentFailure(f"release harness is missing: {harness}")
    with tempfile.TemporaryDirectory(prefix="gateway-runtime-experiment-") as directory:
        certificate, private_key = create_certificate(Path(directory))
        result: dict[str, Any] = {
            "schema": "rp-ylx.gateway-runtime-experiment.v1",
            "started_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
            "source": source_identity(),
            "host": {
                "machine": platform.machine(),
                "platform": platform.platform(),
                "python": platform.python_version(),
                "cpu_count": os.cpu_count(),
            },
            "harness": {
                "path": str(harness.relative_to(ROOT)),
                "sha256": file_sha256(harness),
            },
            "workloads": {},
            "contracts": {},
        }
        for tls, count in ((False, reconnects), (True, tls_reconnects)):
            transport = "tls" if tls else "plain"
            for backend in ("python_gateway", "rust_harness"):
                if backend == "python_gateway":
                    server = ServerProcess.start_python(
                        tls=tls,
                        certificate=certificate,
                        private_key=private_key,
                    )
                else:
                    server = ServerProcess.start_rust(
                        harness,
                        tls=tls,
                        certificate=certificate,
                        private_key=private_key,
                    )
                result["workloads"][f"{backend}_{transport}"] = run_reconnect_workload(
                    server, count
                )

            contract_server = ServerProcess.start_rust(
                harness,
                tls=tls,
                certificate=certificate,
                private_key=private_key,
            )
            result["contracts"][transport] = run_rust_contract_case(contract_server)
        result["decision"] = {
            "result": "no-go",
            "reason": "The harness validates the mechanism, but it cannot delete the complete "
            "Python HTTP/TLS/auth/CORS/body-limit/SSE/error-contract owner in this stage.",
        }
        return result


def run_python_server(certificate: str | None, private_key: str | None) -> None:
    sys.path.insert(0, str(ROOT))
    from rp_ylx.api.events import EventReplayBuffer
    from rp_ylx.api.gateway import create_gateway_server
    from rp_ylx.api.security import Principal, SecurityPolicy
    from tests.test_gateway_stream_lifecycle import _LifecycleProvider

    provider = _LifecycleProvider()
    reader = Principal(
        "reader",
        permissions={
            "getCaptureStatus": None,
            "getPreview": None,
            "streamCaptureEvents": None,
            "streamNetworkEvents": None,
        },
    )
    server = create_gateway_server(
        "127.0.0.1",
        0,
        provider,
        security=SecurityPolicy.customer(tokens={"reader-token": reader}),
        event_buffer=EventReplayBuffer(),
        sse_heartbeat_seconds=60,
        max_sse_connections=1,
        max_preview_streams=1,
    )
    if certificate is not None and private_key is not None:
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        context.minimum_version = ssl.TLSVersion.TLSv1_2
        context.load_cert_chain(certificate, private_key)
        server.socket = context.wrap_socket(server.socket, server_side=True)
    stopped = threading.Event()

    def stop(_signum: int, _frame: Any) -> None:
        stopped.set()

    signal.signal(signal.SIGTERM, stop)
    thread = threading.Thread(target=server.serve_forever, name="python-gateway-main")
    thread.start()
    print(json.dumps({"port": server.server_port, "tls": certificate is not None}), flush=True)
    stopped.wait()
    server.shutdown()
    server.server_close()
    thread.join(timeout=2)
    if thread.is_alive():
        raise ExperimentFailure("Python gateway did not stop")
    print(json.dumps(server.stream_lifecycle_snapshot(), sort_keys=True), flush=True)


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--harness", type=Path, default=DEFAULT_HARNESS)
    parser.add_argument("--reconnects", type=int, default=1_000)
    parser.add_argument("--tls-reconnects", type=int, default=100)
    parser.add_argument("--python-server", action="store_true")
    parser.add_argument("--cert")
    parser.add_argument("--key")
    return parser.parse_args()


def main() -> None:
    arguments = parse_arguments()
    if arguments.python_server:
        run_python_server(arguments.cert, arguments.key)
        return
    if arguments.reconnects < 1 or arguments.tls_reconnects < 1:
        raise ExperimentFailure("reconnect counts must be positive")
    result = run_parent(arguments.harness.resolve(), arguments.reconnects, arguments.tls_reconnects)
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()

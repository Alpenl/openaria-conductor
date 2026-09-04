"""命令行入口。"""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Sequence
from contextlib import suppress
from pathlib import Path

from rp_ylx import PRODUCT_NAME, __commit__, __version__
from rp_ylx.api import CameraPreviewPump, MockDevice, create_server
from rp_ylx.camera import CameraController, CameraError, CameraMode, V4L2DiscoveryBackend
from rp_ylx.cli_helpers import stable_id_for_device
from rp_ylx.hardware import HardwareSmokeError, collect_hardware_facts, record_hardware_smoke
from rp_ylx.native import NativeModuleError, native_capabilities
from rp_ylx.network import (
    NetworkError,
    apply_network,
    network_status,
    reconcile_network,
    rescue_network,
)
from rp_ylx.operational_logging import configure_operational_logging, operational_logger
from rp_ylx.validation import PublicValidationError, validate_public_session

_OPERATIONAL_LOG = operational_logger("cli")


def _print_network_error(exc: NetworkError) -> int:
    failure = {
        "ok": False,
        "error": {"code": exc.code, "message": exc.message},
    }
    if exc.recovery is not None:
        failure["recovery"] = exc.recovery
    print(json.dumps(failure, ensure_ascii=False, sort_keys=True), file=sys.stderr)
    return exc.exit_code


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="openaria", description="Open Aria 设备端录制程序")
    parser.add_argument("--version", action="store_true", help="输出版本和提交标识")
    subcommands = parser.add_subparsers(dest="command")
    subcommands.add_parser("status", help="输出本机可用能力")
    validate = subcommands.add_parser("validate", help="验证一个已封存的录制会话目录")
    validate.add_argument("directory")
    probe = subcommands.add_parser("probe", help="只读探测 RDK X5 硬件和存储事实")
    probe.add_argument("--storage", default="/", help="需要测量容量的录制挂载点")
    probe.add_argument("--output", help="可选 JSON 输出文件；省略时写标准输出")
    smoke = subcommands.add_parser("hardware-smoke", help="在 RDK X5 + YLX 2UQ2 上短录制")
    smoke.add_argument("--device", default="/dev/video0", help="YLX 视频采集节点")
    smoke.add_argument("--output", required=True, help="新建的烟测输出目录")
    smoke.add_argument(
        "--covered-eye",
        required=True,
        choices=["left", "right"],
        help="运行前物理遮挡的眼，用于核对左右方向",
    )
    smoke.add_argument("--frames", type=int, default=30, help="录制双目帧数")
    smoke.add_argument("--imu-packets", type=int, default=20, help="录制 IMU 包数")
    serve = subcommands.add_parser("serve-mock", help="启动设备 API v0 模拟服务")
    serve.add_argument("--host", default="127.0.0.1")
    serve.add_argument("--port", type=int, default=8080)
    serve.add_argument(
        "--fault",
        choices=["hardware_unavailable", "storage_unavailable", "preview_unavailable"],
    )
    serve.add_argument(
        "--allow-origin",
        action="append",
        default=[],
        help="允许跨源访问的完整 Origin，可重复；默认仅同源",
    )
    hardware_preview = subcommands.add_parser(
        "serve-hardware-preview", help="启动真实相机预览 API v0 服务"
    )
    hardware_preview.add_argument("--device", default="/dev/video0")
    hardware_preview.add_argument("--host", default="127.0.0.1")
    hardware_preview.add_argument("--port", type=int, default=8080)
    hardware_preview.add_argument("--width", type=int, default=3840)
    hardware_preview.add_argument("--height", type=int, default=1080)
    hardware_preview.add_argument("--fps", type=int, default=60)
    hardware_preview.add_argument(
        "--allow-origin",
        action="append",
        default=[],
        help="允许跨源访问的完整 Origin，可重复；默认仅同源",
    )
    production = subcommands.add_parser("serve", help="启动 RDK X5 生产录制服务")
    production.add_argument(
        "--config",
        default="/etc/rp-ylx/device.json",
        help="生产服务 JSON 配置",
    )
    volume = subcommands.add_parser("volume", help="管理显式录制卷身份")
    volume_subcommands = volume.add_subparsers(dest="volume_command")
    volume_init = volume_subcommands.add_parser("init", help="初始化当前活动录制卷")
    volume_init.add_argument("mountpoint")
    network = subcommands.add_parser("network", help="管理 RDK X5 本机网络")
    network_subcommands = network.add_subparsers(dest="network_command")
    network_subcommands.add_parser("status", help="输出网络能力和当前状态")
    network_apply = network_subcommands.add_parser("apply", help="原子应用网络配置")
    network_apply.add_argument("--request-id", required=True, help="幂等请求标识")
    network_apply.add_argument("--config", required=True, help="JSON 配置文件或 - 表示标准输入")
    network_subcommands.add_parser("rescue", help="激活已登记的本机救援热点")
    network_subcommands.add_parser("reconcile", help="恢复提交前中断的网络事务")
    network_control = subcommands.add_parser("network-control", help="运行特权网络控制器")
    network_control_subcommands = network_control.add_subparsers(dest="network_control_command")
    network_control_serve = network_control_subcommands.add_parser(
        "serve", help="处理一个 socket 激活的网络控制请求"
    )
    network_control_serve.add_argument(
        "--stdio",
        action="store_true",
        help="从标准输入读取请求并向标准输出写入响应",
    )
    network_control_subcommands.add_parser(
        "desired-mode",
        help="输出 root 网络控制器的无秘密 desired mode",
    )
    network_control_subcommands.add_parser(
        "watchdog-mode",
        help="输出 driver watchdog 是否可执行 Wi-Fi 恢复",
    )
    benchmark = subcommands.add_parser("benchmark", help="运行统一数据面性能工作负载")
    benchmark.add_argument("kind", choices=["fixed_trace", "preview", "recording", "concurrent"])
    benchmark.add_argument("--duration", type=float, default=30.0, help="测量秒数")
    benchmark.add_argument("--round", type=int, default=1, help="从 1 开始的轮次")
    benchmark.add_argument("--wheel-sha256", required=True, help="当前安装 wheel 的 SHA-256")
    benchmark.add_argument(
        "--adapter",
        choices=["python", "rust"],
        required=True,
        help="显式选择 Python 基线或 Rust 候选数据面",
    )
    benchmark.add_argument("--device", default="/dev/video0", help="目标采集节点")
    benchmark.add_argument("--trace", help="仅 fixed_trace 使用的显式 MJPEG 输入")
    benchmark.add_argument("--recording-root", help="recording/concurrent 的空闲输出根")
    benchmark.add_argument("--output", required=True, help="新建或覆盖的严格 JSON 报告")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.version:
        print(f"{PRODUCT_NAME} {__version__} ({__commit__})")
        return 0
    if args.command == "status":
        try:
            native = native_capabilities().as_dict()
        except NativeModuleError as exc:
            native = {
                "adapter": "python",
                "module_available": False,
                "module_version": None,
                "abi": None,
                "features": [],
                "error": {"code": exc.code, "message": exc.message},
            }
        print(
            json.dumps(
                {
                    "service": "openaria",
                    "version": __version__,
                    "commit": __commit__,
                    "hardware": "not-probed",
                    "recording": "idle",
                    "native": native,
                },
                ensure_ascii=False,
                sort_keys=True,
            )
        )
        return 0
    if args.command == "probe":
        facts = collect_hardware_facts(storage_path=Path(args.storage))
        rendered = json.dumps(facts, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
        if args.output:
            Path(args.output).write_text(rendered, encoding="utf-8")
        else:
            print(rendered, end="")
        return 0
    if args.command == "benchmark":
        from rp_ylx.performance.benchmark import BenchmarkConfig, BenchmarkError, run_benchmark

        try:
            report = run_benchmark(
                BenchmarkConfig(
                    kind=args.kind,
                    duration_seconds=args.duration,
                    round=args.round,
                    wheel_sha256=args.wheel_sha256,
                    device=Path(args.device),
                    trace=None if args.trace is None else Path(args.trace),
                    recording_root=(
                        None if args.recording_root is None else Path(args.recording_root)
                    ),
                    adapter=args.adapter,
                )
            )
            Path(args.output).write_text(
                json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
                encoding="utf-8",
            )
        except (BenchmarkError, OSError, RuntimeError, ValueError) as exc:
            print(
                json.dumps(
                    {
                        "ok": False,
                        "error": {
                            "code": str(getattr(exc, "code", "benchmark_failed")),
                            "message": str(getattr(exc, "message", exc)),
                        },
                    },
                    ensure_ascii=False,
                    sort_keys=True,
                ),
                file=sys.stderr,
            )
            return 2
        print(json.dumps({"ok": True, "output": args.output}, ensure_ascii=False, sort_keys=True))
        return 0
    if args.command == "hardware-smoke":
        if args.frames <= 0 or args.imu_packets <= 0:
            print(
                json.dumps(
                    {
                        "ok": False,
                        "error": {
                            "code": "invalid_argument",
                            "message": "frames 和 imu-packets 必须大于零",
                        },
                    },
                    ensure_ascii=False,
                    sort_keys=True,
                ),
                file=sys.stderr,
            )
            return 2
        try:
            facts = collect_hardware_facts(storage_path=Path(args.output).parent)
            result = record_hardware_smoke(
                output=Path(args.output),
                device=Path(args.device),
                mode=CameraMode(3840, 1080, 60.0, "mjpg"),
                covered_eye=args.covered_eye,
                frames=args.frames,
                imu_packets=args.imu_packets,
                software_version=f"{__version__}+{__commit__}",
                evidence_kind="hardware",
                hardware_facts=facts,
            )
        except (HardwareSmokeError, ValueError) as exc:
            code = exc.code if isinstance(exc, HardwareSmokeError) else "invalid_argument"
            message = exc.message if isinstance(exc, HardwareSmokeError) else str(exc)
            print(
                json.dumps(
                    {"ok": False, "error": {"code": code, "message": message}},
                    ensure_ascii=False,
                    sort_keys=True,
                ),
                file=sys.stderr,
            )
            return 2
        print(json.dumps({"ok": True, **result}, ensure_ascii=False, sort_keys=True))
        return 0
    if args.command == "serve-mock":
        device = MockDevice()
        if args.fault:
            device.set_fault(args.fault, "命令行配置的模拟故障")
        server = create_server(args.host, args.port, device, allowed_origins=args.allow_origin)
        print(f"{PRODUCT_NAME} mock API: http://{args.host}:{server.server_port}/api/v0")
        try:
            server.serve_forever()
        except KeyboardInterrupt:
            pass
        finally:
            server.server_close()
        return 0
    if args.command == "serve-hardware-preview":
        server = None
        pump = None
        try:
            mode = CameraMode(args.width, args.height, float(args.fps), "mjpg")
            device = MockDevice()
            controller = CameraController(V4L2DiscoveryBackend())
            stable_id = stable_id_for_device(controller, Path(args.device))
            pump = CameraPreviewPump(device, controller, mode, stable_id=stable_id)
            pump.start()
            server = create_server(args.host, args.port, device, allowed_origins=args.allow_origin)
            print(
                f"{PRODUCT_NAME} hardware preview API: "
                f"http://{args.host}:{server.server_port}/api/v0"
            )
            with suppress(KeyboardInterrupt):
                server.serve_forever()
        except (CameraError, OSError, RuntimeError, ValueError) as exc:
            print(f"hardware preview failed: {exc}", file=sys.stderr)
            return 2
        finally:
            if pump is not None:
                try:
                    pump.stop()
                except (OSError, RuntimeError, ValueError) as exc:
                    print(f"hardware preview shutdown failed: {exc}", file=sys.stderr)
            if server is not None:
                server.server_close()
        return 0
    if args.command == "serve":
        from rp_ylx.daemon import ProductionConfigError, run_production_service
        from rp_ylx.recording import DeviceRecordingError

        configure_operational_logging()
        try:
            run_production_service(args.config)
        except (
            CameraError,
            DeviceRecordingError,
            OSError,
            ProductionConfigError,
            RuntimeError,
        ) as exc:
            code = str(getattr(exc, "code", "service_start_failed"))
            message = str(getattr(exc, "message", exc))
            print(
                json.dumps(
                    {"ok": False, "error": {"code": code, "message": message}},
                    ensure_ascii=False,
                    sort_keys=True,
                ),
                file=sys.stderr,
            )
            return 2
        return 0
    if args.command == "volume":
        if args.volume_command == "init":
            from rp_ylx.recording import DeviceRecordingError, initialize_capture_volume

            try:
                volume_id = initialize_capture_volume(args.mountpoint)
            except (DeviceRecordingError, OSError, ValueError) as exc:
                code = str(getattr(exc, "code", "volume_init_failed"))
                message = str(getattr(exc, "message", exc))
                print(
                    json.dumps(
                        {"ok": False, "error": {"code": code, "message": message}},
                        ensure_ascii=False,
                        sort_keys=True,
                    ),
                    file=sys.stderr,
                )
                return 2
            print(json.dumps({"ok": True, "volume_id": volume_id}, sort_keys=True))
            return 0
        parser.print_help()
        return 0
    if args.command == "network":
        if args.network_command == "status":
            try:
                result = network_status()
            except NetworkError as exc:
                return _print_network_error(exc)
            print(json.dumps(result, ensure_ascii=False, sort_keys=True))
            return 0
        if args.network_command == "apply":
            try:
                result = apply_network(args.request_id, args.config, stdin=sys.stdin)
            except NetworkError as exc:
                return _print_network_error(exc)
            print(json.dumps(result, ensure_ascii=False, sort_keys=True))
            return 0
        if args.network_command == "rescue":
            try:
                result = rescue_network()
            except NetworkError as exc:
                return _print_network_error(exc)
            print(json.dumps(result, ensure_ascii=False, sort_keys=True))
            return 0
        if args.network_command == "reconcile":
            try:
                result = reconcile_network()
            except NetworkError as exc:
                return _print_network_error(exc)
            print(json.dumps(result, ensure_ascii=False, sort_keys=True))
            return 0
        parser.print_help()
        return 0
    if args.command == "network-control":
        if args.network_control_command == "serve" and args.stdio:
            from rp_ylx.network_control import serve_stdio

            configure_operational_logging()
            try:
                return serve_stdio()
            except (NetworkError, OSError, RuntimeError, ValueError) as exc:
                code = getattr(exc, "code", "network_control_process_failed")
                _OPERATIONAL_LOG.event(
                    "network_control_process_failed",
                    level="error",
                    error_code=(
                        code if isinstance(code, str) else "network_control_process_failed"
                    ),
                    exception_type=type(exc).__name__,
                    exit_code=2,
                )
                return 2
        if args.network_control_command in {"desired-mode", "watchdog-mode"}:
            from rp_ylx.network_control import NetworkControlClientError, request_control

            try:
                response = request_control("status")
            except NetworkControlClientError as exc:
                print(exc.code, file=sys.stderr)
                return 1
            body = response.get("body")
            desired = body.get("desired") if isinstance(body, dict) else None
            mode = desired.get("mode") if isinstance(desired, dict) else None
            if response.get("ok") is not True or mode not in {
                "hotspot",
                "wifi-client",
                "ethernet-dhcp",
                "ethernet-static",
            }:
                print("network_controller_status_invalid", file=sys.stderr)
                return 1
            if args.network_control_command == "watchdog-mode" and mode == "wifi-client":
                transaction = body.get("transaction")
                if not isinstance(transaction, dict) or set(transaction) != {
                    "current",
                    "latest",
                }:
                    print("network_controller_status_invalid", file=sys.stderr)
                    return 1
                latest = transaction.get("latest")
                if transaction.get("current") is not None or (
                    isinstance(latest, dict) and latest.get("status") in {"rescued", "failed"}
                ):
                    mode = "defer"
            print(mode)
            return 0
        parser.print_help()
        return 0
    if args.command == "validate":
        try:
            manifest = validate_public_session(args.directory)
        except PublicValidationError as exc:
            print(
                json.dumps(
                    {
                        "valid": False,
                        "error": {
                            "code": exc.code,
                            "location": exc.location,
                            "message": exc.message,
                        },
                    },
                    ensure_ascii=False,
                    sort_keys=True,
                ),
                file=sys.stderr,
            )
            return 2
        print(
            json.dumps(
                {"valid": True, "session_id": manifest["session_id"]},
                ensure_ascii=False,
                sort_keys=True,
            )
        )
        return 0
    parser.print_help()
    return 0

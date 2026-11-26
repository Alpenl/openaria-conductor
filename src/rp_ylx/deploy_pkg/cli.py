"""Command-line parser and JSON-structured entrypoint for rp-ylx-deploy."""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Sequence
from pathlib import Path

from .control_shim import serve_network_control_launcher
from .schema import DeploymentError, facade_attr
from .transaction import ReleaseManager


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="openaria-deploy", description="Open Aria RDK X5 部署工具"
    )
    parser.add_argument("--install-root", type=Path, default=Path("/opt/rp-ylx"))
    parser.add_argument("--config-root", type=Path, default=Path("/etc/rp-ylx"))
    parser.add_argument("--state-root", type=Path, default=Path("/var/lib/rp-ylx"))
    subcommands = parser.add_subparsers(dest="command", required=True)
    install = subcommands.add_parser("install")
    install.add_argument("bundle", type=Path)
    install.add_argument("--no-activate", action="store_true")
    rollback = subcommands.add_parser("rollback")
    rollback.add_argument("--no-activate", action="store_true")
    recover = subcommands.add_parser("recover")
    recover.add_argument("--service-start", action="store_true")
    subcommands.add_parser("status")
    subcommands.add_parser("uninstall")
    subcommands.add_parser("network-control-launch", help=argparse.SUPPRESS)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        if args.command == "network-control-launch":
            launcher = facade_attr(
                "serve_network_control_launcher",
                serve_network_control_launcher,
            )
            return launcher(args.install_root)
        manager_cls = facade_attr("ReleaseManager", ReleaseManager)
        manager = manager_cls(
            install_root=args.install_root,
            config_root=args.config_root,
            state_root=args.state_root,
        )
        if args.command == "install":
            result = manager.install(args.bundle, activate=not args.no_activate)
        elif args.command == "rollback":
            result = manager.rollback(activate=not args.no_activate)
        elif args.command == "recover":
            result = manager.recover(service_start=args.service_start)
        elif args.command == "uninstall":
            result = manager.uninstall()
        else:
            result = manager.status()
    except DeploymentError as error:
        print(
            json.dumps(
                {"ok": False, "error": {"code": error.code, "message": error.message}},
                ensure_ascii=False,
                sort_keys=True,
            ),
            file=sys.stderr,
        )
        return 2
    except Exception as error:
        print(
            json.dumps(
                {
                    "ok": False,
                    "error": {
                        "code": "unexpected_error",
                        "message": f"{type(error).__name__}: {error}",
                    },
                },
                ensure_ascii=False,
                sort_keys=True,
            ),
            file=sys.stderr,
        )
        return 2
    print(json.dumps({"ok": True, **result}, ensure_ascii=False, sort_keys=True))
    return 0

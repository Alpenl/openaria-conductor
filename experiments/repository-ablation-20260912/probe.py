"""Deterministic behavioral probes; run each revision in an isolated Python process."""

from __future__ import annotations

import argparse
import hashlib
import json
import random
import sys
from collections.abc import Mapping
from copy import deepcopy
from dataclasses import asdict
from pathlib import Path


def digest(value: object) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True).encode()).hexdigest()


def paths(value: object, prefix: tuple = ()) -> list[tuple]:
    result = [prefix]
    if isinstance(value, dict):
        for key, child in value.items():
            result.extend(paths(child, (*prefix, key)))
    elif isinstance(value, list):
        for index, child in enumerate(value):
            result.extend(paths(child, (*prefix, index)))
    return result


def replace(value: object, path: tuple, replacement: object) -> object:
    if not path:
        return deepcopy(replacement)
    value = deepcopy(value)
    parent = value
    for key in path[:-1]:
        parent = parent[key]
    parent[path[-1]] = deepcopy(replacement)
    return value


def mutations(seeds: list[object]) -> list[object]:
    values = [
        None,
        False,
        True,
        -1,
        0,
        1,
        32,
        33,
        1.5,
        "",
        "x",
        "a" * 32,
        "a" * 33,
        "中" * 10,
        "中" * 11,
        "cred-a",
        "cred-" + "a" * 124,
        "127.0.0.1",
        "::1",
        "256.1.2.3",
        "01.2.3.4",
        [],
        {},
        ["hotspot"],
        {"mode": "hotspot"},
        ["1.1.1.1", "1.1.1.1"],
        ["1.1.1.1", "8.8.8.8"],
        ["1.1.1.1"] * 4,
        "\ud800",
    ]
    result = list(map(deepcopy, seeds))
    for seed in seeds:
        for path in paths(seed):
            result.extend(replace(seed, path, value) for value in values)
            node = seed
            for key in path:
                node = node[key]
            if isinstance(node, dict):
                result.append(replace(seed, path, {**node, "unexpected": True}))
                for key in node:
                    result.append(replace(seed, path, {k: v for k, v in node.items() if k != key}))
    rng = random.Random(20260912)
    for _ in range(1000):
        value = deepcopy(rng.choice(seeds))
        for _ in range(rng.randint(1, 3)):
            value = replace(value, rng.choice(paths(value)), rng.choice(values))
        result.append(value)
    return result


def outcome(function, *args, **kwargs):
    try:
        return {"value": function(*args, **kwargs)}
    except Exception as error:
        return {"error": type(error).__name__, "code": getattr(error, "code", None)}


def network_probe() -> dict:
    from rp_ylx import network_control
    from rp_ylx.api.gateway import GatewayHandler

    try:
        from rp_ylx.network_validation import valid_network_request
    except ImportError:
        gateway = GatewayHandler._validate_network_body
    else:
        gateway = valid_network_request

    desired = [
        {"mode": "hotspot", "wifi_client": None, "ethernet": None},
        {
            "mode": "ethernet-dhcp",
            "wifi_client": None,
            "ethernet": {"addressing": "dhcp", "static_ipv4": None},
        },
        {
            "mode": "ethernet-static",
            "wifi_client": None,
            "ethernet": {
                "addressing": "static",
                "static_ipv4": {
                    "address": "192.168.1.5",
                    "prefix_length": 24,
                    "gateway": "192.168.1.1",
                    "dns": ["1.1.1.1", "8.8.8.8"],
                },
            },
        },
    ]
    for security in ("open", "wpa2-personal", "wpa3-personal", "wpa2-wpa3-personal"):
        wifi = {"ssid": "Field LAN", "security": security}
        if security != "open":
            wifi["credential_ref"] = "cred-field-lan"
        desired.append({"mode": "wifi-client", "wifi_client": wifi, "ethernet": None})
    seeds = {
        "apply": [{"schema": "ylx.network-apply-request.v1", "desired": item} for item in desired],
        "retry": [
            {
                "schema": "ylx.network-retry-request.v1",
                "transaction_id": "01989f6a-2c00-7a1b-8c2d-3e4f50617283",
            }
        ],
        "forget": [{"schema": "ylx.network-forget-request.v1"}],
    }
    records = []
    corpus = []
    for operation, valid in seeds.items():
        for body in mutations(valid):
            corpus.append([operation, body])
            request = {
                "schema": "ylx.network-control-request.v1",
                "operation": operation,
                "principal_id": "customer",
                "idempotency_key": "ablation",
                "body": body,
            }
            records.append(
                {
                    "http": outcome(gateway, operation, body)
                    if isinstance(body, Mapping)
                    else {"value": False},
                    "control": outcome(network_control._valid_control_request, operation, request),
                    "unhashable_mode": operation == "apply"
                    and isinstance(body, dict)
                    and isinstance(body.get("desired"), dict)
                    and isinstance(body["desired"].get("mode"), (dict, list)),
                }
            )
    return {"cases": len(records), "corpus_sha256": digest(corpus), "records": records}


def manifest_probe() -> dict:
    from rp_ylx.api import downloads
    from rp_ylx.recording.device_session import manifest_artifact_bytes_total

    session_id = "01989f6a-2c00-7a1b-8c2d-3e4f50617283"
    artifact = {
        "artifact_id": "a" * 64,
        "sha256": "a" * 64,
        "role": "left-video",
        "path": "video/left.mp4",
        "media_type": "video/mp4",
        "bytes": 123,
    }
    # Projection probe deliberately supplies the already-decoded manifest. Full schema and
    # payload validation are separately covered by the existing recording/download suites.
    manifest = {
        "schema": "ylx.device-session.v1",
        "sealed": True,
        "session_id": session_id,
        "display_name": "ablation",
        "time": {
            "started_at": "2026-09-12T00:00:00Z",
            "ended_at": "2026-09-12T00:00:01Z",
            "duration_seconds": 1,
        },
        "frames": {
            "count": 30,
            "artifact": {
                **artifact,
                "artifact_id": "b" * 64,
                "sha256": "b" * 64,
                "path": "frames.ndjson",
            },
        },
        "imu": {
            "sample_count": 100,
            "artifact": {
                **artifact,
                "artifact_id": "c" * 64,
                "sha256": "c" * 64,
                "path": "imu.ndjson",
            },
        },
        "video": {"layout": "raw-side-by-side", "artifact": artifact},
    }
    corpus = mutations([manifest])
    records = []
    for value in corpus:
        if not isinstance(value, dict):
            continue
        records.append(
            [
                outcome(
                    downloads.device_session_v1_summary,
                    b"unused-decoded-manifest",
                    session_id,
                    value,
                ),
                outcome(manifest_artifact_bytes_total, value, code="artifact_invalid"),
            ]
        )
    for path in (
        "../escape",
        "/tmp/escape",
        "video/../escape",
        "video//left.mp4",
        "a\x00b",
        "",
        ".",
        "video/left.mp4",
    ):
        for legacy in (False, True):
            raw = {**artifact, "path": path}
            if legacy:
                raw.pop("artifact_id")
                raw["records"] = 1
            records.append(
                outcome(
                    lambda raw, legacy: asdict(downloads._artifact_descriptor(raw, legacy=legacy)),
                    raw,
                    legacy,
                )
            )
    return {
        "cases": len(records),
        "corpus_sha256": digest(corpus),
        "results_sha256": digest(records),
        "records": records,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("root", type=Path)
    parser.add_argument("output", type=Path)
    parser.add_argument("--negative", choices=("network", "paths"))
    args = parser.parse_args()
    sys.path.insert(0, str(args.root.resolve() / "src"))
    if args.negative == "network":
        from rp_ylx import network_control, network_validation

        network_control.valid_network_request = lambda *_: True
        network_validation.valid_network_request = lambda *_: True
    elif args.negative == "paths":
        from rp_ylx.api import downloads

        downloads._safe_relative_components = lambda *_: ()
    result = {"network": network_probe(), "manifest": manifest_probe()}
    args.output.write_text(json.dumps(result, sort_keys=True, indent=2) + "\n")
    print(
        json.dumps(
            {
                key: {k: v for k, v in value.items() if k != "records"}
                for key, value in result.items()
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()

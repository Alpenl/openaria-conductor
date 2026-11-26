"""Unified local and CI verification runner for Miro-Ego 0.3.0."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shlex
import shutil
import struct
import subprocess
import sys
import tempfile
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

# Assemble retired legacy identifiers without embedding literal matches inside this file.
_LEGACY_TERMS: tuple[str, ...] = (
    "".join(("y", "l", "x")),
    "".join(("rp-", "y", "l", "x")),
    "".join(("rp_", "y", "l", "x")),
    "".join(("open", "aria")),
)
_LEGACY_RE = re.compile("|".join(re.escape(term) for term in _LEGACY_TERMS), re.IGNORECASE)
_UNWRAP_RE = re.compile(r"\.(unwrap|expect)\s*\(")
_IGNORED_DIR_NAMES = frozenset({"__pycache__", "target", ".git", ".venv"})
_IGNORED_REL_PREFIXES = ("csrc/miro-ego-encoder/tests/build/",)


def _run(
    *command: str,
    extra_env: dict[str, str] | None = None,
    cwd: Path = ROOT,
) -> None:
    env = dict(os.environ)
    if extra_env:
        env.update(extra_env)
    subprocess.run(command, check=True, cwd=cwd, env=env)


def _require_or_skip(tool: str, *, strict: bool, install_hint: str) -> str | None:
    resolved = shutil.which(tool)
    if resolved is None:
        sibling = Path(sys.executable).parent / tool
        if sibling.is_file() and os.access(sibling, os.X_OK):
            resolved = str(sibling)
    if resolved is not None:
        return resolved
    message = f"Missing required tool {tool!r}; {install_hint}"
    if strict:
        raise RuntimeError(message)
    print(f"WARNING: {message} (skipped)")
    return None


def _check_deploy_shell_scripts(*, require_shellcheck: bool) -> None:
    if os.name != "posix" or not Path("/bin/sh").exists():
        print("WARNING: non-POSIX host; skipping shell script checks")
        return

    deploy_dir = ROOT / "deploy" / "miro-ego"
    if not deploy_dir.is_dir():
        raise RuntimeError(f"Missing deploy directory: {deploy_dir}")

    shebang_scripts: list[Path] = []
    bash_bin = shutil.which("bash")
    for candidate in sorted(deploy_dir.iterdir(), key=lambda p: p.name):
        if not candidate.is_file():
            continue
        lines = candidate.read_text(encoding="utf-8", errors="ignore").splitlines()
        if not lines or not lines[0].startswith("#!"):
            continue
        shebang_scripts.append(candidate)
        tokens = shlex.split(lines[0][2:])
        uses_bash = any(Path(tok).name == "bash" for tok in tokens)
        if uses_bash:
            if bash_bin is None:
                raise RuntimeError(f"bash is required to syntax-check {candidate.name}")
            _run(bash_bin, "-n", str(candidate))
        else:
            _run("/bin/sh", "-n", str(candidate))

    if not shebang_scripts:
        raise RuntimeError("No executable shell scripts found under deploy/miro-ego/")

    shellcheck_bin = shutil.which("shellcheck")
    if shellcheck_bin is not None:
        _run(shellcheck_bin, *(str(script) for script in shebang_scripts))
    elif require_shellcheck:
        raise RuntimeError("shellcheck is required (--require-shellcheck) but not found on PATH")
    else:
        print(
            "WARNING: shellcheck not installed; skipping shellcheck on deploy/miro-ego/ "
            "(pass --require-shellcheck to enforce)"
        )


def _check_contracts() -> None:
    import jsonschema
    import yaml

    support_path = ROOT / "contracts" / "miro-ego-api-support.json"
    support = json.loads(support_path.read_text(encoding="utf-8"))
    if support.get("schema") != "miro-ego.device-api-consumer-support.v1":
        raise RuntimeError("contracts/miro-ego-api-support.json has wrong schema discriminator")
    if support.get("consumer") != "miro-ego":
        raise RuntimeError("contracts/miro-ego-api-support.json has wrong consumer")
    if support.get("supported_device_api_majors") != [1]:
        raise RuntimeError("contracts/miro-ego-api-support.json must pin major [1]")
    if support.get("unknown_major_policy") != "fail_closed":
        raise RuntimeError("contracts/miro-ego-api-support.json must enforce fail_closed policy")

    required_contracts = support.get("required_contracts", [])
    if not required_contracts:
        raise RuntimeError("contracts/miro-ego-api-support.json declares no required_contracts")

    expected_routes = {
        (route["path"], route["method"].lower(), route["operation_id"])
        for route in support.get("routes", [])
    }
    if not expected_routes:
        raise RuntimeError("contracts/miro-ego-api-support.json declares no routes")

    for entry in required_contracts:
        contract_path = ROOT / entry["path"]
        raw = contract_path.read_bytes()
        digest = hashlib.sha256(raw).hexdigest()
        if len(raw) != entry["bytes"] or digest != entry["sha256"]:
            raise RuntimeError(
                f"Contract digest/byte mismatch for {entry['path']}: "
                f"expected ({entry['bytes']} B, {entry['sha256']}), "
                f"actual ({len(raw)} B, {digest})"
            )
        spec = yaml.safe_load(raw.decode("utf-8"))
        if not isinstance(spec, dict):
            raise RuntimeError(f"OpenAPI spec is not a mapping: {entry['path']}")
        openapi_ver = str(spec.get("openapi", ""))
        if not openapi_ver.startswith("3.1."):
            raise RuntimeError(f"Expected OpenAPI 3.1.x in {entry['path']}, got {openapi_ver!r}")
        info_ver = spec.get("info", {}).get("version")
        if info_ver != entry["info_version"]:
            raise RuntimeError(
                f"OpenAPI info.version mismatch in {entry['path']}: "
                f"{info_ver!r} != {entry['info_version']!r}"
            )
        servers = spec.get("servers", [])
        base_path = entry["server_base_path"]
        if not any(
            isinstance(srv, dict) and str(srv.get("url", "")).endswith(base_path) for srv in servers
        ):
            raise RuntimeError(
                f"OpenAPI servers in {entry['path']} do not advertise base path {base_path!r}"
            )
        actual_routes: set[tuple[str, str, str]] = set()
        for path_template, path_item in spec.get("paths", {}).items():
            if not isinstance(path_item, dict):
                continue
            for method in ("get", "post", "put", "patch", "delete", "head", "options"):
                operation = path_item.get(method)
                if isinstance(operation, dict) and "operationId" in operation:
                    actual_routes.add((path_template, method, operation["operationId"]))
        if actual_routes != expected_routes:
            missing = sorted(expected_routes - actual_routes)
            extra = sorted(actual_routes - expected_routes)
            raise RuntimeError(
                f"OpenAPI route table drift vs miro-ego-api-support.json: "
                f"missing={missing}, extra={extra}"
            )

    schemas = support.get("schemas", [])
    if not schemas:
        raise RuntimeError("contracts/miro-ego-api-support.json declares no JSON schemas")
    for entry in schemas:
        schema_path = ROOT / entry["path"]
        raw = schema_path.read_bytes()
        digest = hashlib.sha256(raw).hexdigest()
        if len(raw) != entry["bytes"] or digest != entry["sha256"]:
            raise RuntimeError(
                f"Schema digest/byte mismatch for {entry['path']}: "
                f"expected ({entry['bytes']} B, {entry['sha256']}), "
                f"actual ({len(raw)} B, {digest})"
            )
        schema_doc = json.loads(raw.decode("utf-8"))
        if schema_doc.get("$id") != entry["id"]:
            raise RuntimeError(
                f"Schema $id mismatch in {entry['path']}: "
                f"{schema_doc.get('$id')!r} != {entry['id']!r}"
            )
        validator_cls = jsonschema.validators.validator_for(schema_doc)
        validator_cls.check_schema(schema_doc)


def _iter_legacy_scan_files() -> list[Path]:
    targets = [
        ROOT / "crates",
        ROOT / "csrc",
        ROOT / "deploy" / "miro-ego",
        ROOT / "tools" / "miro_ego_host",
        ROOT / "scripts",
        *sorted((ROOT / "contracts").glob("miro-ego-*")),
        *sorted((ROOT / "contracts" / "schemas").glob("miro-ego-*")),
    ]
    files: list[Path] = []
    for target in targets:
        if target.is_file():
            files.append(target)
        elif target.is_dir():
            for path in sorted(target.rglob("*")):
                if not path.is_file():
                    continue
                if any(part in _IGNORED_DIR_NAMES for part in path.parts):
                    continue
                rel = path.relative_to(ROOT).as_posix()
                if any(rel.startswith(prefix) for prefix in _IGNORED_REL_PREFIXES):
                    continue
                files.append(path)
    return files


def _check_zero_legacy_naming() -> None:
    hits: list[str] = []
    for path in _iter_legacy_scan_files():
        rel = path.relative_to(ROOT).as_posix()
        text = path.read_text(encoding="utf-8", errors="ignore")
        for line_no, line in enumerate(text.splitlines(), 1):
            if _LEGACY_RE.search(line):
                hits.append(f"{rel}:{line_no}: {line.strip()[:120]}")
    if hits:
        preview = "\n".join(hits[:25])
        raise RuntimeError(f"Legacy naming leak detected ({len(hits)} hit(s)):\n{preview}")


def _strip_rust_comments_and_strings(source: str) -> str:
    """Replace Rust comments and string/char literals with spaces while preserving newlines."""
    out: list[str] = []
    i = 0
    n = len(source)
    while i < n:
        ch = source[i]
        # Line comment
        if ch == "/" and i + 1 < n and source[i + 1] == "/":
            i += 2
            while i < n and source[i] != "\n":
                i += 1
            continue
        # Nested block comment
        if ch == "/" and i + 1 < n and source[i + 1] == "*":
            depth = 1
            i += 2
            while i < n and depth > 0:
                if source[i] == "/" and i + 1 < n and source[i + 1] == "*":
                    depth += 1
                    i += 2
                elif source[i] == "*" and i + 1 < n and source[i + 1] == "/":
                    depth -= 1
                    i += 2
                else:
                    if source[i] == "\n":
                        out.append("\n")
                    i += 1
            continue
        # Raw string literal r"...", r#"..."#, br#"..."#
        if ch == "r" or (ch == "b" and i + 1 < n and source[i + 1] == "r"):
            start = i
            j = i + (2 if ch == "b" else 1)
            hashes = 0
            while j < n and source[j] == "#":
                hashes += 1
                j += 1
            if j < n and source[j] == '"':
                j += 1
                closing = '"' + ("#" * hashes)
                out.append(" ")
                while j < n:
                    if source.startswith(closing, j):
                        j += len(closing)
                        break
                    if source[j] == "\n":
                        out.append("\n")
                    j += 1
                i = j
                continue
            out.append(source[start])
            i = start + 1
            continue
        # Normal or byte string literal
        if ch == '"' or (ch == "b" and i + 1 < n and source[i + 1] == '"'):
            i += 2 if ch == "b" else 1
            out.append(" ")
            while i < n:
                if source[i] == "\\":
                    if i + 1 < n and source[i + 1] == "\n":
                        out.append("\n")
                    i += 2
                    continue
                if source[i] == '"':
                    i += 1
                    break
                if source[i] == "\n":
                    out.append("\n")
                i += 1
            continue
        # Char literal ('a', '\n', '{') vs lifetime ('a, 'static)
        if ch == "'":
            if i + 2 < n and source[i + 1] == "\\":
                j = i + 2
                while j < n and source[j] != "'" and source[j] != "\n":
                    j += 1
                if j < n and source[j] == "'":
                    out.append(" ")
                    i = j + 1
                    continue
            elif i + 2 < n and source[i + 2] == "'" and source[i + 1] != "\n":
                out.append(" ")
                i += 3
                continue
        out.append(ch)
        i += 1
    return "".join(out)


def _check_zero_non_test_unwrap() -> None:
    violations: list[str] = []
    for rs_path in sorted(ROOT.glob("crates/*/src/**/*.rs")):
        rel = rs_path.relative_to(ROOT).as_posix()
        if rs_path.name == "tests.rs" or "/tests/" in rel:
            continue
        raw_lines = rs_path.read_text(encoding="utf-8").splitlines()
        cleaned_lines = _strip_rust_comments_and_strings("\n".join(raw_lines)).splitlines()

        in_cfg_test = False
        cfg_test_pending = False
        brace_depth = 0
        for idx, cleaned in enumerate(cleaned_lines, 1):
            stripped = cleaned.strip()
            if in_cfg_test:
                brace_depth += cleaned.count("{") - cleaned.count("}")
                if brace_depth <= 0:
                    in_cfg_test = False
                    brace_depth = 0
                continue

            if re.search(r"#\s*\[\s*cfg\s*\(\s*test\s*\)\s*\]", stripped):
                cfg_test_pending = True
                if "{" in stripped:
                    in_cfg_test = True
                    cfg_test_pending = False
                    brace_depth = stripped.count("{") - stripped.count("}")
                    if brace_depth <= 0:
                        in_cfg_test = False
                        brace_depth = 0
                continue

            if cfg_test_pending:
                if "{" in stripped:
                    in_cfg_test = True
                    cfg_test_pending = False
                    brace_depth = stripped.count("{") - stripped.count("}")
                    if brace_depth <= 0:
                        in_cfg_test = False
                        brace_depth = 0
                    continue
                if stripped == "" or stripped.startswith("#["):
                    continue
                if stripped.endswith(";"):
                    cfg_test_pending = False
                    continue

            if _UNWRAP_RE.search(cleaned):
                raw_line = raw_lines[idx - 1].strip() if idx - 1 < len(raw_lines) else stripped
                violations.append(f"{rel}:{idx}: {raw_line}")

    if violations:
        preview = "\n".join(violations[:25])
        raise RuntimeError(
            f"Non-test .unwrap()/.expect() found in crates/*/src/ ({len(violations)} hit(s)):\n"
            f"{preview}"
        )


def _check_native_bundle_builder() -> None:
    """Smoke-test scripts/build_rdk_x5_bundle.py against provision.require_native_bundle."""
    tools_dir = str(ROOT / "tools")
    if tools_dir not in sys.path:
        sys.path.insert(0, tools_dir)
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))

    from miro_ego_host.factory_recovery.provision import require_native_bundle

    from scripts.build_rdk_x5_bundle import build_bundle
    from scripts.release_version import project_version

    header = bytearray(64)
    header[:4] = b"\x7fELF"
    header[4] = 2
    header[5] = 1
    header[6] = 1
    struct.pack_into("<HH", header, 16, 3, 183)

    with tempfile.TemporaryDirectory(prefix="miro-ego-bundle-check-") as tmp:
        tmp_path = Path(tmp)
        miro_bin = tmp_path / "miro-ego"
        enc_bin = tmp_path / "miro-ego-encoder"
        miro_bin.write_bytes(bytes(header) + b"miro-ego-check")
        enc_bin.write_bytes(bytes(header) + b"miro-ego-encoder-check")
        bundle_dir = tmp_path / "bundle"
        archive_path = tmp_path / "bundle.tar"

        build_bundle(
            miro_ego_bin=miro_bin,
            encoder_bin=enc_bin,
            output=bundle_dir,
            archive=archive_path,
            version=project_version(),
            commit="0" * 40,
        )
        if sha256sum := shutil.which("sha256sum"):
            _run(sha256sum, "--check", "--strict", "SHA256SUMS", cwd=bundle_dir)
        require_native_bundle(archive_path)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Miro-Ego 0.3.0 unified local & CI check runner")
    parser.add_argument(
        "--fast",
        action="store_true",
        help="Fast local mode: skip cargo clippy and run cargo test --lib (refused in CI/strict)",
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Fail when required tools (cargo, ruff, gcc, make) are missing (auto under CI=true)",
    )
    parser.add_argument(
        "--require-shellcheck",
        action="store_true",
        help="Fail if shellcheck binary is unavailable instead of warning and skipping",
    )
    parser.add_argument(
        "--skip-rust",
        action="store_true",
        help="Skip Rust workspace fmt/clippy/test checks (refused in CI/strict)",
    )
    args = parser.parse_args(argv)

    strict = args.strict or os.environ.get("CI") == "true"
    if args.fast and strict:
        print("ERROR: --fast is forbidden under CI=true or --strict", file=sys.stderr)
        return 2
    if args.skip_rust and strict:
        print("ERROR: --skip-rust is forbidden under CI=true or --strict", file=sys.stderr)
        return 2

    started = time.monotonic()

    # Stage 1: Static analysis, contracts, and lint
    print("=== Stage 1: Static Analysis, Contracts & Lint ===", flush=True)
    _run(sys.executable, str(ROOT / "scripts" / "release_version.py"))
    _run(sys.executable, "-m", "compileall", "-q", "tools", "scripts")

    if ruff_bin := _require_or_skip(
        "ruff", strict=strict, install_hint="run `uv sync --frozen --extra dev`"
    ):
        _run(ruff_bin, "check", "tools", "scripts")
        _run(ruff_bin, "format", "--check", "tools", "scripts")

    cargo_bin: str | None = None
    if not args.skip_rust:
        cargo_bin = _require_or_skip(
            "cargo", strict=strict, install_hint="install Rust toolchain via rustup"
        )
        if cargo_bin is not None:
            _run(cargo_bin, "fmt", "--all", "--", "--check")

    _check_deploy_shell_scripts(require_shellcheck=args.require_shellcheck)
    _check_contracts()
    _check_zero_legacy_naming()
    _check_zero_non_test_unwrap()
    print("Stage 1 PASSED", flush=True)

    # Stage 2: C Encoder & Host Tools
    print("=== Stage 2: C Encoder & Host Tools ===", flush=True)
    gcc_bin = _require_or_skip("gcc", strict=strict, install_hint="install gcc")
    make_bin = _require_or_skip("make", strict=strict, install_hint="install make")
    if gcc_bin is not None and make_bin is not None:
        _run(make_bin, "-C", "csrc/miro-ego-encoder", "check")

    _run(
        sys.executable,
        "-m",
        "unittest",
        "discover",
        "-s",
        "tools/miro_ego_host/tests",
        extra_env={"PYTHONPATH": "tools"},
    )
    _check_native_bundle_builder()
    print("Stage 2 PASSED", flush=True)

    # Stage 3: Rust Full Workspace
    print("=== Stage 3: Rust Full Workspace ===", flush=True)
    if cargo_bin is not None:
        if not args.fast:
            _run(cargo_bin, "clippy", "--workspace", "--all-targets", "--", "-D", "warnings")
        test_cmd = [cargo_bin, "test", "--workspace"]
        if args.fast:
            test_cmd.append("--lib")
        _run(*test_cmd)
    print(f"ALL MIRO-EGO CHECKS PASSED in {time.monotonic() - started:.2f}s", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

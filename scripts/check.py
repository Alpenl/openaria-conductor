"""本地和 CI 共用的无硬件检查入口。"""

from __future__ import annotations

import argparse
import fnmatch
import os
import shlex
import shutil
import subprocess
import sys
import sysconfig
import tempfile
import time
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

SERIAL_EXCLUSIVE: frozenset[str] = frozenset({"tests.test_distribution"})

LPT_PRIORITY: tuple[str, ...] = (
    "tests.test_deployment",
    "tests.test_activation_failure_cleanup",
    "tests.test_activation_success_cleanup",
    "tests.test_audio_readback_deadline",
    "tests.test_capture_coordinator",
    "tests.test_audio_checkpoint_reads",
    "tests.test_factory_autoinstall",
    "tests.test_audit_file_admission",
    "tests.test_regression_fixes_part1",
    "tests.test_network",
    "tests.test_gateway",
    "tests.test_network_control",
    "tests.test_regression_fixes_part2",
    "tests.test_session_download",
    "tests.test_factory_recovery",
    "tests.test_capture_sources",
    "tests.test_card_export",
    "tests.test_split_eye_recording",
    "tests.test_gateway_events",
)
_LPT_RANK = {name: idx for idx, name in enumerate(LPT_PRIORITY)}

STUB_HB_MEDIA_CODEC_H = """\
#ifndef HOBOT_STUB_HB_MEDIA_CODEC_H
#define HOBOT_STUB_HB_MEDIA_CODEC_H

#include <stdbool.h>
#include <stdint.h>

typedef int32_t hb_s32;

enum {
    MEDIA_CODEC_ID_MJPEG = 1,
    MEDIA_CODEC_ID_H264 = 2,
    MEDIA_CODEC_ID_H265 = 3,
};

enum {
    MC_FEEDING_MODE_FRAME_SIZE = 1,
    MC_PIXEL_FORMAT_NV12 = 1,
    MC_CCW_0 = 0,
    MC_DIRECTION_NONE = 0,
    MC_H264_PROFILE_HP = 1,
    MC_H264_LEVEL_UNSPECIFIED = 0,
    MC_H265_LEVEL_UNSPECIFIED = 0,
    MC_VIDEO_FRAME_BUFFER = 1,
    MC_VIDEO_STREAM_BUFFER = 2,
};

typedef enum {
    MC_AV_RC_MODE_H264CBR = 0,
    MC_AV_RC_MODE_H264VBR = 1,
    MC_AV_RC_MODE_H264FIXQP = 2,
    MC_AV_RC_MODE_H264AVBR = 3,
    MC_AV_RC_MODE_H265CBR = 4,
    MC_AV_RC_MODE_H265VBR = 5,
    MC_AV_RC_MODE_H265FIXQP = 6,
    MC_AV_RC_MODE_H265AVBR = 7,
} mc_video_rate_control_mode_t;

typedef struct {
    uint32_t frame_rate;
    uint32_t bit_rate;
    uint32_t intra_period;
    uint32_t intra_qp;
    int initial_rc_qp;
    int vbv_buffer_size;
    int mb_level_rc_enalbe;
    int ctu_level_rc_enalbe;
    uint32_t min_qp_I;
    uint32_t min_qp_P;
    uint32_t min_qp_B;
    uint32_t max_qp_I;
    uint32_t max_qp_P;
    uint32_t max_qp_B;
    int hvs_qp_enable;
    int hvs_qp_scale;
    int max_delta_qp;
    int qp_map_enable;
} mc_rate_control_budget_t;

typedef struct {
    uint32_t frame_rate;
    uint32_t intra_period;
    uint32_t intra_qp;
    int qp_map_enable;
} mc_rate_control_vbr_t;

typedef struct {
    uint32_t frame_rate;
    uint32_t intra_period;
    uint32_t force_qp_I;
    uint32_t force_qp_P;
    uint32_t force_qp_B;
} mc_rate_control_fixqp_t;

typedef struct {
    mc_video_rate_control_mode_t mode;
    mc_rate_control_budget_t h264_cbr_params;
    mc_rate_control_vbr_t h264_vbr_params;
    mc_rate_control_fixqp_t h264_fixqp_params;
    mc_rate_control_budget_t h264_avbr_params;
    mc_rate_control_budget_t h265_cbr_params;
    mc_rate_control_vbr_t h265_vbr_params;
    mc_rate_control_fixqp_t h265_fixqp_params;
    mc_rate_control_budget_t h265_avbr_params;
} mc_rate_control_params_t;

typedef struct {
    int feed_mode;
    int pix_fmt;
    uint32_t bitstream_buf_size;
    int bitstream_buf_count;
    int frame_buf_count;
    struct {
        int rot_degree;
        int mir_direction;
        bool frame_crop_enable;
    } mjpeg_dec_config;
} mc_video_codec_dec_params_t;

typedef struct {
    int width;
    int height;
    int pix_fmt;
    uint32_t bitstream_buf_size;
    int frame_buf_count;
    bool external_frame_buf;
    int bitstream_buf_count;
    struct {
        int gop_preset_idx;
        int decoding_refresh_type;
    } gop_params;
    int rot_degree;
    int mir_direction;
    bool frame_cropping_flag;
    int enable_user_pts;
    mc_rate_control_params_t rc_params;
    struct {
        int main_still_picture_profile_enable;
        int h265_level;
    } h265_enc_config;
    struct {
        int h264_profile;
        int h264_level;
    } h264_enc_config;
} mc_video_codec_enc_params_t;

typedef struct {
    int codec_id;
    bool encoder;
    mc_video_codec_dec_params_t video_dec_params;
    mc_video_codec_enc_params_t video_enc_params;
} media_codec_context_t;

typedef struct {
    int reserved;
} mc_av_codec_startup_params_t;

typedef struct {
    int type;
    struct {
        uint8_t *vir_ptr;
        uint32_t size;
        int stream_end;
    } vstream_buf;
    struct {
        uint8_t *vir_ptr[3];
        int width;
        int height;
        int stride;
        int pix_fmt;
        uint32_t size;
    } vframe_buf;
} media_codec_buffer_t;

typedef struct {
    struct {
        int decode_result;
    } video_frame_info;
} media_codec_output_buffer_info_t;

hb_s32 hb_mm_mc_initialize(media_codec_context_t *context);
hb_s32 hb_mm_mc_configure(media_codec_context_t *context);
hb_s32 hb_mm_mc_start(media_codec_context_t *context, const mc_av_codec_startup_params_t *params);
hb_s32 hb_mm_mc_pause(media_codec_context_t *context);
hb_s32 hb_mm_mc_stop(media_codec_context_t *context);
hb_s32 hb_mm_mc_release(media_codec_context_t *context);
hb_s32 hb_mm_mc_get_rate_control_config(
    media_codec_context_t *context, mc_rate_control_params_t *params
);
hb_s32 hb_mm_mc_dequeue_input_buffer(
    media_codec_context_t *context, media_codec_buffer_t *buffer, int timeout_ms
);
hb_s32 hb_mm_mc_queue_input_buffer(
    media_codec_context_t *context, media_codec_buffer_t *buffer, int timeout_ms
);
hb_s32 hb_mm_mc_dequeue_output_buffer(
    media_codec_context_t *context,
    media_codec_buffer_t *buffer,
    media_codec_output_buffer_info_t *info,
    int timeout_ms
);
hb_s32 hb_mm_mc_queue_output_buffer(
    media_codec_context_t *context, media_codec_buffer_t *buffer, int timeout_ms
);

#endif
"""

STUB_HB_MEDIA_MUXER_H = """\
#ifndef HOBOT_STUB_HB_MEDIA_MUXER_H
#define HOBOT_STUB_HB_MEDIA_MUXER_H

#include <stdint.h>

enum {
    MEDIA_MUXER_OUTPUT_FORMAT_MP4 = 1,
};

typedef struct {
    const char *output_file_name;
    int output_format;
} media_muxer_context_t;

typedef struct {
    int codec_id;
    int numerator;
    int denominator;
    struct {
        int width;
        int height;
        int pix_fmt;
        uint32_t frame_rate;
        uint32_t bit_rate;
    } video_params;
} mx_stream_params_t;

typedef struct {
    uint8_t *vir_ptr;
    uint32_t size;
    uint64_t pts;
    int is_key_frame;
} mx_stream_t;

int hb_mm_mx_get_default_context(media_muxer_context_t *context);
int hb_mm_mx_initialize(media_muxer_context_t *context);
int hb_mm_mx_add_stream(media_muxer_context_t *context, const mx_stream_params_t *params);
int hb_mm_mx_start(media_muxer_context_t *context);
int hb_mm_mx_stop(media_muxer_context_t *context);
int hb_mm_mx_write_stream(media_muxer_context_t *context, const mx_stream_t *stream);

#endif
"""

_STUB_HB_MEDIA_CODEC_H = STUB_HB_MEDIA_CODEC_H
_STUB_HB_MEDIA_MUXER_H = STUB_HB_MEDIA_MUXER_H


def run(*command: str, extra_env: dict[str, str] | None = None) -> None:
    env = {**os.environ, "PYTHONPATH": "src"}
    if extra_env:
        env.update(extra_env)
    subprocess.run(command, check=True, cwd=ROOT, env=env)


def _require_or_skip(tool: str, *, strict: bool, install_hint: str) -> str | None:
    resolved = shutil.which(tool)
    if resolved is None:
        sibling = Path(sys.executable).parent / tool
        if sibling.is_file() and os.access(sibling, os.X_OK):
            resolved = str(sibling)
    if resolved is not None:
        return resolved
    message = f"未安装 {tool}；{install_hint}"
    if strict:
        raise RuntimeError(message)
    print(f"{message}（已跳过）")
    return None


def _rust_env() -> dict[str, str]:
    env: dict[str, str] = {}
    if "PYO3_PYTHON" not in os.environ:
        env["PYO3_PYTHON"] = sys.executable
    libdir = sysconfig.get_config_var("LIBDIR")
    if not libdir or not Path(libdir).is_dir():
        candidate = Path(sys.executable).resolve().parent.parent / "lib"
        if candidate.is_dir():
            libdir = str(candidate)
    if libdir and Path(libdir).is_dir():
        rustflags = os.environ.get("RUSTFLAGS", "")
        if f"-L {libdir}" not in rustflags and f"-L{libdir}" not in rustflags:
            env["RUSTFLAGS"] = f"{rustflags} -L {libdir}".strip()
        ld_library_path = os.environ.get("LD_LIBRARY_PATH", "")
        if libdir not in ld_library_path.split(os.pathsep):
            env["LD_LIBRARY_PATH"] = (
                f"{libdir}{os.pathsep}{ld_library_path}" if ld_library_path else libdir
            )
    return env


def _check_hobot_c(gcc: str) -> None:
    hobot_dir = ROOT / "src/rp_ylx/hobot"
    native_hobot_dir = ROOT / "native/hobot"
    c_files = [*sorted(hobot_dir.glob("*.c")), *sorted(native_hobot_dir.glob("*.c"))]
    if not c_files:
        return
    sdk_include = os.environ.get("HOBOT_SDK_INCLUDE")
    with tempfile.TemporaryDirectory() as temporary:
        if sdk_include:
            include_dir = sdk_include
        else:
            stub_dir = Path(temporary)
            (stub_dir / "hb_media_codec.h").write_text(STUB_HB_MEDIA_CODEC_H, encoding="utf-8")
            (stub_dir / "hb_media_muxer.h").write_text(STUB_HB_MEDIA_MUXER_H, encoding="utf-8")
            include_dir = str(stub_dir)
        for source in c_files:
            run(
                gcc,
                "-fsyntax-only",
                "-Wall",
                "-Wextra",
                "-std=gnu11",
                f"-I{include_dir}",
                f"-I{hobot_dir}",
                str(source),
            )


def _check_deploy_shell_scripts() -> None:
    if os.name != "posix" or not Path("/bin/sh").exists():
        return
    deploy_dir = ROOT / "src/rp_ylx/deploy"
    shebang_scripts: list[Path] = []
    bash_bin = shutil.which("bash")
    for candidate in sorted(deploy_dir.glob("rp-ylx-*")):
        if not candidate.is_file():
            continue
        first_line = candidate.read_text(encoding="utf-8", errors="ignore").splitlines()[:1]
        if not first_line or not first_line[0].startswith("#!"):
            continue
        shebang_scripts.append(candidate)
        tokens = shlex.split(first_line[0][2:])
        uses_bash = any(Path(tok).name == "bash" for tok in tokens)
        if uses_bash:
            if bash_bin is None:
                raise RuntimeError(f"未安装 bash，无法语法检查 {candidate.name}")
            run(bash_bin, "-n", str(candidate))
        else:
            run("/bin/sh", "-n", str(candidate))
    if not shebang_scripts:
        return
    if shellcheck := shutil.which("shellcheck"):
        run(shellcheck, *[str(script) for script in shebang_scripts])
    else:
        print("未安装 shellcheck；跳过 deploy 脚本 shellcheck 检查（已跳过）")


def _discover_test_modules() -> list[str]:
    tests_dir = ROOT / "tests"
    return [f"tests.{path.stem}" for path in sorted(tests_dir.glob("test_*.py"))]


def _resolve_test_targets(filters: list[str], discovered: list[str]) -> list[str]:
    if not filters:
        return list(discovered)
    discovered_set = set(discovered)
    selected: list[str] = []
    seen: set[str] = set()

    def add(target: str) -> None:
        if target not in seen:
            seen.add(target)
            selected.append(target)

    for raw in filters:
        normalized = raw.strip()
        if not normalized:
            continue
        as_path = Path(normalized)
        if normalized.endswith(".py") or as_path.is_file():
            candidate_path = (ROOT / as_path).resolve() if not as_path.is_absolute() else as_path
            try:
                rel = candidate_path.relative_to(ROOT)
            except ValueError as exc:
                raise ValueError(f"测试路径不在仓库内：{raw}") from exc
            module = ".".join(rel.with_suffix("").parts)
            if module in discovered_set or candidate_path.is_file():
                add(module)
                continue
        if normalized in discovered_set:
            add(normalized)
            continue
        prefixed = f"tests.{normalized}"
        if prefixed in discovered_set:
            add(prefixed)
            continue
        if "." in normalized:
            parts = normalized.split(".")
            if parts[0] != "tests" and f"tests.{parts[0]}" in discovered_set:
                add(f"tests.{normalized}")
                continue
            if ".".join(parts[:2]) in discovered_set:
                add(normalized)
                continue
        matched = [
            mod
            for mod in discovered
            if fnmatch.fnmatchcase(mod, normalized)
            or fnmatch.fnmatchcase(mod.removeprefix("tests."), normalized)
            or normalized in mod
        ]
        if not matched:
            raise ValueError(f"未匹配到测试模块或用例：{raw}")
        for mod in matched:
            add(mod)
    return selected


def _is_serial_target(target: str) -> bool:
    return target in SERIAL_EXCLUSIVE or any(
        target.startswith(f"{serial}.") for serial in SERIAL_EXCLUSIVE
    )


def _lpt_sort(modules: list[str]) -> list[str]:
    fallback = len(LPT_PRIORITY)
    return sorted(modules, key=lambda name: (_LPT_RANK.get(name, fallback), name))


def _run_unittest_target(target: str, *, verbose: bool) -> tuple[str, int, float, str]:
    worker_tmp = tempfile.mkdtemp(prefix="oaw")
    started = time.monotonic()
    try:
        env = {
            **os.environ,
            "PYTHONPATH": "src",
            "TMPDIR": worker_tmp,
            "TEMP": worker_tmp,
            "TMP": worker_tmp,
        }
        cmd = [sys.executable, "-m", "unittest"]
        if verbose:
            cmd.append("-v")
        cmd.append(target)
        completed = subprocess.run(
            cmd,
            cwd=ROOT,
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            errors="replace",
            check=False,
        )
        elapsed = time.monotonic() - started
        return target, completed.returncode, elapsed, completed.stdout
    finally:
        shutil.rmtree(worker_tmp, ignore_errors=True)


def _run_parallel_tests(
    targets: list[str], *, workers: int, verbose: bool
) -> tuple[bool, list[tuple[str, float]]]:
    if not targets:
        return True, []
    ordered = _lpt_sort(targets)
    total = len(ordered)
    pool_size = max(1, min(workers, total))
    timings: list[tuple[str, float]] = []
    failed = False
    completed_count = 0
    stage_start = time.monotonic()

    with ThreadPoolExecutor(max_workers=pool_size) as executor:
        futures: dict[Future[tuple[str, int, float, str]], str] = {
            executor.submit(_run_unittest_target, target, verbose=verbose): target
            for target in ordered
        }
        for future in as_completed(futures):
            if future.cancelled():
                continue
            target, returncode, elapsed, output = future.result()
            completed_count += 1
            timings.append((target, elapsed))
            status = "PASS" if returncode == 0 else "FAIL"
            print(f"[{completed_count}/{total}] {status} {target} ({elapsed:.2f}s)", flush=True)
            if returncode != 0:
                failed = True
                if output:
                    print(
                        f"--- {target} output ---\n{output.rstrip()}\n--- end {target} ---",
                        flush=True,
                    )
                for pending in futures:
                    if not pending.done():
                        pending.cancel()

    stage_elapsed = time.monotonic() - stage_start
    print(f"Stage 2 并行测试完成：{len(timings)}/{total} 模块，总耗时 {stage_elapsed:.2f}s")
    top_slowest = sorted(timings, key=lambda item: item[1], reverse=True)[:10]
    if top_slowest:
        print("Top 10 最慢测试模块：")
        for idx, (name, secs) in enumerate(top_slowest, 1):
            print(f"  {idx:>2}. {secs:6.2f}s  {name}")
    return not failed, timings


def _run_serial_tests(targets: list[str], *, verbose: bool) -> bool:
    for target in targets:
        print(f"串行执行隔离测试：{target}", flush=True)
        _, returncode, elapsed, output = _run_unittest_target(target, verbose=verbose)
        status = "PASS" if returncode == 0 else "FAIL"
        print(f"[serial] {status} {target} ({elapsed:.2f}s)", flush=True)
        if returncode != 0:
            if output:
                print(
                    f"--- {target} output ---\n{output.rstrip()}\n--- end {target} ---",
                    flush=True,
                )
            return False
    return True


def main(argv: list[str] | None = None) -> int:
    default_workers = min(os.cpu_count() or 4, 8)
    parser = argparse.ArgumentParser(description="无硬件全量检查入口")
    parser.add_argument(
        "--strict",
        action="store_true",
        help="缺少 ruff/cargo/gcc 时报错退出（CI=true 时自动开启）",
    )
    parser.add_argument(
        "--skip-rust",
        action="store_true",
        help="显式跳过 Rust cargo fmt/test/clippy 检查",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=default_workers,
        help=f"Stage 2 并行测试 worker 数量（默认 {default_workers}）",
    )
    parser.add_argument(
        "--fast",
        action="store_true",
        help=(
            "快速本地开发模式：跳过串行分发测试与 cargo clippy，"
            "仅跑 cargo test --lib（严格模式和 CI 禁止）"
        ),
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="透传 -v 给 unittest 子进程",
    )
    parser.add_argument(
        "tests",
        nargs="*",
        help="可选测试过滤（模块名、文件路径或匹配模式）",
    )
    args = parser.parse_args(argv)
    if args.workers < 1:
        parser.error("--workers 必须 >= 1")
    strict = args.strict or os.environ.get("CI") == "true"
    if args.fast and strict:
        print("错误：--strict 或 CI=true 模式下禁止使用 --fast", file=sys.stderr)
        return 2

    total_start = time.monotonic()
    discovered = _discover_test_modules()
    try:
        selected = _resolve_test_targets(args.tests, discovered)
    except ValueError as exc:
        print(f"错误：{exc}", file=sys.stderr)
        return 2

    parallel_targets = [target for target in selected if not _is_serial_target(target)]
    serial_targets = (
        [] if args.fast else [target for target in selected if _is_serial_target(target)]
    )

    cargo_bin: str | None = None
    rust_env: dict[str, str] = {}
    if not args.skip_rust:
        cargo_bin = _require_or_skip("cargo", strict=strict, install_hint="请安装 Rust toolchain")
        if cargo_bin is not None:
            rust_env = _rust_env()

    # Stage 1: Fail-fast static preflight
    run(sys.executable, "-m", "compileall", "-q", "src", "scripts", "tests")
    if ruff_bin := _require_or_skip(
        "ruff", strict=strict, install_hint="请使用 `uv sync --extra dev` 安装"
    ):
        run(ruff_bin, "check", ".")
        run(ruff_bin, "format", "--check", ".")
    if gcc := _require_or_skip("gcc", strict=strict, install_hint="请安装 gcc"):
        _check_hobot_c(gcc)
    _check_deploy_shell_scripts()
    if cargo_bin is not None:
        run(
            cargo_bin,
            "fmt",
            "--manifest-path",
            "native/Cargo.toml",
            "--check",
            extra_env=rust_env,
        )

    # Stage 2: Parallel unittest pool
    parallel_ok, _ = _run_parallel_tests(
        parallel_targets, workers=args.workers, verbose=args.verbose
    )
    if not parallel_ok:
        return 1

    # Stage 3: Serial isolation tests & Cargo test/clippy
    if serial_targets and not _run_serial_tests(serial_targets, verbose=args.verbose):
        return 1

    if cargo_bin is not None:
        test_cmd = [cargo_bin, "test", "--manifest-path", "native/Cargo.toml"]
        if args.fast:
            test_cmd.append("--lib")
        run(*test_cmd, extra_env=rust_env)
        if not args.fast:
            run(
                cargo_bin,
                "clippy",
                "--manifest-path",
                "native/Cargo.toml",
                "--all-targets",
                "--",
                "-D",
                "warnings",
                extra_env=rust_env,
            )

    print(f"全部检查通过，总耗时 {time.monotonic() - total_start:.2f}s")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

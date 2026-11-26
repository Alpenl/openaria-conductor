"""RDK X5 + YLX 2UQ2 的一次性双目与 IMU 短录制。"""

from __future__ import annotations

import io
import os
import shutil
import sys
import uuid
from collections import Counter
from collections.abc import Callable
from contextlib import suppress
from pathlib import Path
from typing import Any

from PIL import Image, ImageStat

from rp_ylx.camera import (
    CameraController,
    CameraDescriptor,
    CameraError,
    CameraMode,
    V4L2DiscoveryBackend,
)
from rp_ylx.camera.models import CameraBackend
from rp_ylx.contracts import SessionValidationError, validate_session
from rp_ylx.core.fs import fsync_directory, write_json_atomic
from rp_ylx.imu import ImuCollector, ImuError, UvcXuImuSource
from rp_ylx.imu.models import ImuSource
from rp_ylx.recording import RecordingConfig, RecordingError, SessionRecorder

from .target import is_supported_target, is_ylx_2uq2_usb

SMOKE_FORMAT = "ylx.hardware-smoke.v0"
ImuSourceFactory = Callable[[CameraDescriptor], ImuSource]

__all__ = ["HardwareSmokeError", "ImuSourceFactory", "SMOKE_FORMAT", "record_hardware_smoke"]


class HardwareSmokeError(RuntimeError):
    def __init__(self, code: str, message: str) -> None:
        self.code = code
        self.message = message
        super().__init__(f"{code}: {message}")


def _default_imu_source(descriptor: CameraDescriptor) -> ImuSource:
    return UvcXuImuSource(descriptor.node)


def _brightness(payload: bytes) -> float:
    try:
        with Image.open(io.BytesIO(payload)) as image:
            image.load()
            return float(ImageStat.Stat(image.convert("L")).mean[0])
    except (OSError, ValueError) as exc:
        raise HardwareSmokeError(
            "stereo_decode_failed", f"左右眼 JPEG 无法完整解码：{exc}"
        ) from exc


def _orientation(
    left_brightness: list[float],
    right_brightness: list[float],
    covered_eye: str,
) -> dict[str, Any]:
    left_mean = sum(left_brightness) / len(left_brightness)
    right_mean = sum(right_brightness) / len(right_brightness)
    covered_mean, open_mean = (
        (left_mean, right_mean) if covered_eye == "left" else (right_mean, left_mean)
    )
    delta = open_mean - covered_mean
    if delta < 20.0:
        raise HardwareSmokeError(
            "stereo_orientation_failed",
            f"遮挡 {covered_eye} 眼后亮度差仅 {delta:.2f}，无法确认左右眼方向",
        )
    return {
        "method": "covered_eye_brightness",
        "covered_eye": covered_eye,
        "left_mean": round(left_mean, 3),
        "right_mean": round(right_mean, 3),
        "minimum_delta": 20.0,
        "observed_delta": round(delta, 3),
        "passed": True,
    }


def _hardware_summary(facts: dict[str, Any]) -> dict[str, Any]:
    target = facts.get("target")
    if not is_supported_target(target):
        raise HardwareSmokeError(
            "unsupported_target",
            "真机烟测只接受探针确认的 RDK X5 V1.0 + YLX 2UQ2",
        )
    usb_devices = facts.get("usb_devices")
    camera = (
        next(
            (
                device
                for device in usb_devices
                if isinstance(device, dict) and is_ylx_2uq2_usb(device)
            ),
            None,
        )
        if isinstance(usb_devices, list)
        else None
    )
    platform = facts.get("platform")
    if camera is None or not isinstance(platform, dict):
        raise HardwareSmokeError(
            "unsupported_target",
            "目标探针缺少 RDK 平台或 YLX 2UQ2 USB 事实",
        )
    return {
        "probe_format": facts.get("format"),
        "observed_at": facts.get("observed_at"),
        "target": target,
        "platform": platform,
        "camera": camera,
    }


def _cleanup_capture(
    controller: CameraController, collector: ImuCollector | None
) -> list[tuple[HardwareSmokeError, Exception]]:
    failures: list[tuple[HardwareSmokeError, Exception]] = []
    if collector is not None:
        try:
            collector.close()
        except Exception as exc:
            failures.append(
                (
                    HardwareSmokeError("imu_cleanup_failed", f"IMU 清理失败：{exc}"),
                    exc,
                )
            )
    try:
        controller.close()
    except Exception as exc:
        failures.append(
            (
                HardwareSmokeError("camera_cleanup_failed", f"相机清理失败：{exc}"),
                exc,
            )
        )
    return failures


def _write_summary(output: Path, summary: dict[str, Any]) -> None:
    try:
        write_json_atomic(output / "summary.json", summary, sort_keys=True, indent=2)
    except (OSError, TypeError, ValueError) as exc:
        raise HardwareSmokeError("summary_write_failed", f"无法写入烟测摘要：{exc}") from exc


def _summary(
    *,
    evidence_kind: str,
    selected: CameraDescriptor,
    mode: CameraMode,
    orientation: dict[str, Any],
    camera_observations: list[Any],
    imu_observations: list[Any],
    xu_unit: int | None,
    manifest: dict[str, Any],
    hardware: dict[str, Any] | None,
) -> dict[str, Any]:
    camera_frames = [observation.frame for observation in camera_observations]
    imu_samples = [sample for observation in imu_observations for sample in observation.samples]
    packet_samples = [observation.samples[0] for observation in imu_observations]
    sync_qualities = Counter(sample.sync_quality for sample in imu_samples)
    result: dict[str, Any] = {
        "format": SMOKE_FORMAT,
        "evidence": {
            "kind": evidence_kind,
            "physical_hardware_claim": evidence_kind == "hardware",
        },
        "device": {
            "stable_id": selected.stable_id,
            "node": selected.node,
            "name": selected.name,
        },
        "mode": {
            "capture_width": mode.width,
            "capture_height": mode.height,
            "eye_width": mode.width // 2,
            "eye_height": mode.height,
            "fps": mode.fps,
            "encoding": mode.encoding,
        },
        "stereo_orientation": orientation,
        "camera": {
            "frames_recorded": len(camera_frames),
            "first_source_sequence": camera_frames[0].source_sequence,
            "last_source_sequence": camera_frames[-1].source_sequence,
            "first_host_monotonic_ns": camera_frames[0].host_monotonic_ns,
            "last_host_monotonic_ns": camera_frames[-1].host_monotonic_ns,
            "dropped_frames": sum(
                observation.dropped_before for observation in camera_observations
            ),
            "timestamps_monotonic": all(
                current.host_monotonic_ns > previous.host_monotonic_ns
                for previous, current in zip(camera_frames, camera_frames[1:], strict=False)
            ),
        },
        "imu": {
            "xu_unit": xu_unit,
            "packets_recorded": len(imu_observations),
            "samples_recorded": len(imu_samples),
            "first_device_ticks": packet_samples[0].device_ticks,
            "last_device_ticks": packet_samples[-1].device_ticks,
            "first_host_monotonic_ns": packet_samples[0].host_monotonic_ns,
            "last_host_monotonic_ns": packet_samples[-1].host_monotonic_ns,
            "dropped_samples": sum(observation.dropped_samples for observation in imu_observations),
            "sync_quality_samples": dict(sorted(sync_qualities.items())),
            "timestamps_monotonic": all(
                current.device_ticks > previous.device_ticks
                and current.host_monotonic_ns > previous.host_monotonic_ns
                for previous, current in zip(packet_samples, packet_samples[1:], strict=False)
            ),
        },
        "session": {
            "id": manifest["session_id"],
            "directory": manifest["session_id"],
            "validated": True,
            "counts": manifest["counts"],
        },
    }
    if hardware is not None:
        result["hardware"] = hardware
    return result


def _prepare_smoke_context(
    *,
    camera_backend: CameraBackend | None,
    imu_source_factory: ImuSourceFactory | None,
    mode: CameraMode,
    covered_eye: str,
    frames: int,
    imu_packets: int,
    evidence_kind: str,
    hardware_facts: dict[str, Any] | None,
) -> tuple[dict[str, Any] | None, CameraBackend, ImuSourceFactory]:
    if frames <= 0 or imu_packets <= 0:
        raise ValueError("frames 和 imu_packets 必须大于零")
    if covered_eye not in {"left", "right"}:
        raise ValueError("covered_eye 必须是 left 或 right")
    if evidence_kind not in {"hardware", "fixture"}:
        raise ValueError("evidence_kind 必须是 hardware 或 fixture")
    if mode.width % 2:
        raise ValueError("双目并排模式宽度必须是偶数")
    hardware = None
    if evidence_kind == "hardware":
        if camera_backend is not None or imu_source_factory is not None:
            raise HardwareSmokeError(
                "fixture_evidence_forbidden",
                "真机证据不能注入相机或 IMU 测试适配器",
            )
        if hardware_facts is None:
            raise HardwareSmokeError("probe_required", "真机烟测必须绑定同次运行的目标硬件探针")
        hardware = _hardware_summary(hardware_facts)
        camera_backend = V4L2DiscoveryBackend()
        imu_source_factory = _default_imu_source
    elif camera_backend is None or imu_source_factory is None:
        raise ValueError("fixture 烟测必须显式提供相机和 IMU 适配器")
    assert camera_backend is not None
    assert imu_source_factory is not None
    return hardware, camera_backend, imu_source_factory


def _descriptor_matches_device(descriptor: CameraDescriptor, device: Path) -> bool:
    target = os.path.normpath(str(device))
    if os.path.normpath(descriptor.node) == target or descriptor.stable_id == str(device):
        return True
    with suppress(OSError):
        return Path(descriptor.node).resolve() == device.resolve()
    return False


def _select_smoke_camera(controller: CameraController, device: Path) -> CameraDescriptor:
    try:
        matches = [
            descriptor
            for descriptor in controller.discover()
            if _descriptor_matches_device(descriptor, device)
        ]
    except CameraError as exc:
        raise HardwareSmokeError(exc.code, exc.message) from exc
    except Exception as exc:
        raise HardwareSmokeError("camera_discovery_failed", f"相机设备发现失败：{exc}") from exc
    if not matches:
        raise HardwareSmokeError("camera_missing", f"未发现请求的视频节点：{device}")
    if len(matches) != 1:
        raise HardwareSmokeError("camera_ambiguous", f"视频节点匹配到多个设备：{device}")
    return matches[0]


def _capture_smoke_samples(
    controller: CameraController,
    imu_source_factory: ImuSourceFactory,
    selected: CameraDescriptor,
    mode: CameraMode,
    recorder: SessionRecorder,
    *,
    covered_eye: str,
    frames: int,
    imu_packets: int,
) -> tuple[dict[str, Any], list[Any], list[Any], int | None]:
    left_brightness: list[float] = []
    right_brightness: list[float] = []
    camera_observations: list[Any] = []
    imu_observations: list[Any] = []
    xu_unit: int | None = None
    collector: ImuCollector | None = None
    try:
        try:
            controller.open(mode, stable_id=selected.stable_id)
        except CameraError as exc:
            raise HardwareSmokeError(exc.code, exc.message) from exc
        try:
            controller.start()
        except Exception as exc:
            raise HardwareSmokeError("camera_start_failed", f"相机采集启动失败：{exc}") from exc
        try:
            imu_source = imu_source_factory(selected)
        except ImuError as exc:
            code = (
                "imu_missing"
                if exc.code in {"xu_not_found", "xu_discovery_unavailable"}
                else "imu_start_failed"
            )
            raise HardwareSmokeError(code, f"IMU 初始化失败：{exc.message}") from exc
        except Exception as exc:
            raise HardwareSmokeError("imu_start_failed", f"IMU 初始化失败：{exc}") from exc
        xu_unit = getattr(imu_source, "unit_id", None)
        collector = ImuCollector(imu_source)
        for index in range(max(frames, imu_packets)):
            if index < frames:
                try:
                    observation = controller.read(timeout=2.0)
                except CameraError as exc:
                    raise HardwareSmokeError(exc.code, exc.message) from exc
                camera_observations.append(observation)
                left_brightness.append(_brightness(observation.frame.left))
                right_brightness.append(_brightness(observation.frame.right))
                try:
                    accepted = recorder.submit_frame(observation)
                except RecordingError as exc:
                    raise HardwareSmokeError(exc.code, exc.message) from exc
                if not accepted:
                    raise HardwareSmokeError(
                        "recording_backpressure", "短录制相机帧未能进入有界队列"
                    )
            if index < imu_packets:
                try:
                    observation = collector.read(timeout=1.0)
                except ImuError as exc:
                    raise HardwareSmokeError(exc.code, exc.message) from exc
                imu_observations.append(observation)
                try:
                    accepted = recorder.submit_imu(observation)
                except RecordingError as exc:
                    raise HardwareSmokeError(exc.code, exc.message) from exc
                if not accepted:
                    raise HardwareSmokeError(
                        "recording_backpressure", "短录制 IMU 样本未能进入有界队列"
                    )
        orientation = _orientation(left_brightness, right_brightness, covered_eye)
        try:
            controller.stop()
        except CameraError as exc:
            raise HardwareSmokeError(exc.code, exc.message) from exc
        except Exception as exc:
            raise HardwareSmokeError("camera_stop_failed", f"相机停止采集失败：{exc}") from exc
        return orientation, camera_observations, imu_observations, xu_unit
    finally:
        primary_failure = sys.exception()
        cleanup_failures = _cleanup_capture(controller, collector)
        if primary_failure is None and cleanup_failures:
            failure, cause = cleanup_failures[0]
            raise failure from cause


def _finalize_smoke_session(
    output: Path,
    recorder: SessionRecorder,
    *,
    evidence_kind: str,
    selected: CameraDescriptor,
    mode: CameraMode,
    orientation: dict[str, Any],
    camera_observations: list[Any],
    imu_observations: list[Any],
    xu_unit: int | None,
    hardware: dict[str, Any] | None,
) -> dict[str, Any]:
    summary: dict[str, Any] | None = None

    def finalize(candidate: Path) -> None:
        nonlocal summary
        try:
            manifest = validate_session(candidate, allow_partial=True)
        except SessionValidationError as exc:
            raise HardwareSmokeError(
                "session_validation_failed",
                f"封存会话复验失败（{exc.code} {exc.location}）：{exc.message}",
            ) from exc
        except Exception as exc:
            raise HardwareSmokeError(
                "session_validation_failed", f"封存会话复验失败：{exc}"
            ) from exc
        summary = _summary(
            evidence_kind=evidence_kind,
            selected=selected,
            mode=mode,
            orientation=orientation,
            camera_observations=camera_observations,
            imu_observations=imu_observations,
            xu_unit=xu_unit,
            manifest=manifest,
            hardware=hardware,
        )
        _write_summary(output, summary)

    try:
        recorder.stop(before_publish=finalize)
    except HardwareSmokeError:
        with suppress(OSError):
            (output / "summary.json").unlink(missing_ok=True)
        raise
    except RecordingError as exc:
        with suppress(OSError):
            (output / "summary.json").unlink(missing_ok=True)
        raise HardwareSmokeError(exc.code, exc.message) from exc

    assert summary is not None
    return summary


def _record_hardware_smoke(
    *,
    output: Path,
    camera_backend: CameraBackend | None = None,
    device: Path,
    mode: CameraMode,
    covered_eye: str,
    frames: int,
    imu_packets: int,
    software_version: str,
    evidence_kind: str,
    imu_source_factory: ImuSourceFactory | None = None,
    hardware_facts: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """录制并验证一个短会话；fixture 结果不会被标记为真机证据。"""
    hardware, camera_backend, imu_source_factory = _prepare_smoke_context(
        camera_backend=camera_backend,
        imu_source_factory=imu_source_factory,
        mode=mode,
        covered_eye=covered_eye,
        frames=frames,
        imu_packets=imu_packets,
        evidence_kind=evidence_kind,
        hardware_facts=hardware_facts,
    )
    controller = CameraController(camera_backend)
    selected = _select_smoke_camera(controller, device)

    try:
        output.mkdir(exist_ok=False)
    except OSError as exc:
        raise HardwareSmokeError(
            "output_create_failed", f"无法新建烟测输出目录 {output}：{exc}"
        ) from exc
    if hardware_facts is not None:
        try:
            write_json_atomic(output / "probe.json", hardware_facts, sort_keys=True, indent=2)
        except (OSError, TypeError, ValueError) as exc:
            raise HardwareSmokeError("probe_write_failed", f"无法写入同次探针事实：{exc}") from exc
    recorder = SessionRecorder(
        output,
        RecordingConfig(
            device_id=selected.stable_id,
            software_version=software_version,
            width=mode.width // 2,
            height=mode.height,
            fps=mode.fps,
            encoding="jpeg",
        ),
    )
    with recorder:
        try:
            recorder.start()
        except RecordingError as exc:
            raise HardwareSmokeError(exc.code, exc.message) from exc
        orientation, camera_observations, imu_observations, xu_unit = _capture_smoke_samples(
            controller,
            imu_source_factory,
            selected,
            mode,
            recorder,
            covered_eye=covered_eye,
            frames=frames,
            imu_packets=imu_packets,
        )
        return _finalize_smoke_session(
            output,
            recorder,
            evidence_kind=evidence_kind,
            selected=selected,
            mode=mode,
            orientation=orientation,
            camera_observations=camera_observations,
            imu_observations=imu_observations,
            xu_unit=xu_unit,
            hardware=hardware,
        )


def _has_sealed_session(bundle: Path) -> bool:
    try:
        return any(
            entry.is_dir() and not entry.name.endswith(".partial") for entry in bundle.iterdir()
        )
    except OSError:
        return False


def _has_partial_session(bundle: Path) -> bool:
    try:
        return any(entry.is_dir() and entry.name.endswith(".partial") for entry in bundle.iterdir())
    except OSError:
        return False


def record_hardware_smoke(
    *,
    output: Path,
    camera_backend: CameraBackend | None = None,
    device: Path,
    mode: CameraMode,
    covered_eye: str,
    frames: int,
    imu_packets: int,
    software_version: str,
    evidence_kind: str,
    imu_source_factory: ImuSourceFactory | None = None,
    hardware_facts: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Record and atomically publish one complete smoke bundle."""

    if os.path.lexists(output):
        raise HardwareSmokeError("output_create_failed", f"烟测输出路径已经存在：{output}")
    staging = output.with_name(f".{output.name}.{uuid.uuid4().hex}.partial")
    try:
        result = _record_hardware_smoke(
            output=staging,
            camera_backend=camera_backend,
            device=device,
            mode=mode,
            covered_eye=covered_eye,
            frames=frames,
            imu_packets=imu_packets,
            software_version=software_version,
            evidence_kind=evidence_kind,
            imu_source_factory=imu_source_factory,
            hardware_facts=hardware_facts,
        )
    except BaseException:
        if staging.exists():
            if (
                _has_partial_session(staging)
                and not _has_sealed_session(staging)
                and not os.path.lexists(output)
            ):
                with suppress(OSError):
                    os.rename(staging, output)
            else:
                shutil.rmtree(staging, ignore_errors=True)
        raise
    try:
        os.rename(staging, output)
        fsync_directory(output.parent)
    except OSError as exc:
        if staging.exists():
            shutil.rmtree(staging, ignore_errors=True)
        raise HardwareSmokeError(
            "output_publish_failed", f"无法原子发布烟测输出目录 {output}：{exc}"
        ) from exc
    return result

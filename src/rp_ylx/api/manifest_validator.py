"""Device-session and v0 manifest schema + semantic validation and LRU cache."""

from __future__ import annotations

import copy
import hashlib
import json
import math
import re
import threading
from collections import OrderedDict
from collections.abc import Iterator, Mapping
from datetime import datetime
from importlib.resources import files

from jsonschema import Draft202012Validator, FormatChecker, validators
from jsonschema.exceptions import ValidationError

from rp_ylx.api.compat import facade_attr
from rp_ylx.api.locked_io import (
    ArtifactAccessError,
    ArtifactDescriptor,
    _safe_relative_components,
)
from rp_ylx.api.schema_validation import validate_manifest_schema
from rp_ylx.audio_clock import audio_clock_report
from rp_ylx.core.ids import parse_rfc3339, valid_sha256
from rp_ylx.manifest_limits import MAX_MANIFEST_BYTES


def _unique_json_key(value: object) -> object:
    # JSON Schema treats 1 and 1.0 as equal, but distinguishes booleans from numbers.
    if value is None:
        return ("null",)
    if isinstance(value, bool):
        return ("boolean", value)
    if isinstance(value, (int, float)):
        if isinstance(value, float) and not math.isfinite(value):
            raise TypeError("non-JSON number")
        return ("number", value)
    if isinstance(value, str):
        return ("string", value)
    if isinstance(value, list):
        return ("array", tuple(_unique_json_key(item) for item in value))
    if isinstance(value, dict) and all(isinstance(key, str) for key in value):
        return ("object", frozenset((key, _unique_json_key(item)) for key, item in value.items()))
    raise TypeError("non-JSON value")


def _unique_items(validator, unique, instance, schema):
    if not unique or not validator.is_type(instance, "array"):
        return
    try:
        seen = set()
        for item in instance:
            key = _unique_json_key(item)
            if key in seen:
                yield ValidationError("array contains duplicate elements")
                return
            seen.add(key)
    except TypeError:
        yield from Draft202012Validator.VALIDATORS["uniqueItems"](
            validator, unique, instance, schema
        )


_ManifestValidator = validators.extend(Draft202012Validator, {"uniqueItems": _unique_items})

_MANIFEST_FORMAT_CHECKER = FormatChecker()
_MANIFEST_FORMAT_CHECKER.checks("date-time", ValueError)(
    lambda value: not isinstance(value, str) or bool(parse_rfc3339(value))
)


_MEDIA_TYPE = re.compile(r"^[a-z0-9!#$&^_.+-]+/[a-z0-9!#$&^_.+-]+$")
_DEVICE_SESSION_V1_SCHEMA_ID = "ylx.device-session.v1"
_DEVICE_SESSION_V2_SCHEMA_ID = "ylx.device-session.v2"
_DEVICE_SESSION_V3_SCHEMA_ID = "ylx.device-session.v3"
_DEVICE_SESSION_V4_SCHEMA_ID = "ylx.device-session.v4"
_DEVICE_SESSION_SCHEMA_IDS = frozenset(
    {
        _DEVICE_SESSION_V1_SCHEMA_ID,
        _DEVICE_SESSION_V2_SCHEMA_ID,
        _DEVICE_SESSION_V3_SCHEMA_ID,
        _DEVICE_SESSION_V4_SCHEMA_ID,
    }
)
_DEVICE_SESSION_V1_SCHEMA = json.loads(
    files("rp_ylx.schemas")
    .joinpath("ylx-device-session-v1.schema.json")
    .read_text(encoding="utf-8")
)
_DEVICE_SESSION_V1_VALIDATOR = _ManifestValidator(
    _DEVICE_SESSION_V1_SCHEMA,
    format_checker=_MANIFEST_FORMAT_CHECKER,
)
_PINNED_DEVICE_SESSION_V2_SCHEMA = json.loads(
    files("rp_ylx.schemas")
    .joinpath("ylx-device-session-v2.schema.json")
    .read_text(encoding="utf-8")
)
# Deep-copy the pinned vendor schema before adding the versioned capture_clock field.
_DEVICE_SESSION_V2_SCHEMA = copy.deepcopy(_PINNED_DEVICE_SESSION_V2_SCHEMA)
_DEVICE_SESSION_V2_SCHEMA["$defs"]["recordedAudio"]["properties"]["capture_clock"] = {
    "type": "object",
    "required": ["schema"],
    # Exact field, source and anchor validation is performed by audio_clock_report.
    "properties": {"schema": {"enum": ["openaria.audio-clock.v1", "openaria.audio-clock.v2"]}},
}
_DEVICE_SESSION_V2_VALIDATOR = _ManifestValidator(
    _DEVICE_SESSION_V2_SCHEMA,
    format_checker=_MANIFEST_FORMAT_CHECKER,
)
_DEVICE_SESSION_V3_VALIDATOR = _ManifestValidator(
    json.loads(
        files("rp_ylx.schemas")
        .joinpath("ylx-device-session-v3.schema.json")
        .read_text(encoding="utf-8")
    ),
    format_checker=_MANIFEST_FORMAT_CHECKER,
)
_DEVICE_SESSION_V4_VALIDATOR = _ManifestValidator(
    json.loads(
        files("rp_ylx.schemas")
        .joinpath("ylx-device-session-v4.schema.json")
        .read_text(encoding="utf-8")
    ),
    format_checker=_MANIFEST_FORMAT_CHECKER,
)
_RECORDING_SESSION_SCHEMA = json.loads(
    files("rp_ylx.contracts")
    .joinpath("recording-session-v0.schema.json")
    .read_text(encoding="utf-8")
)
_RECORDING_SESSION_VALIDATOR = _ManifestValidator(
    _RECORDING_SESSION_SCHEMA,
    format_checker=_MANIFEST_FORMAT_CHECKER,
)
_VALIDATED_MANIFEST_CACHE_LIMIT = 32
_VALIDATED_MANIFEST_CACHE_BYTES = MAX_MANIFEST_BYTES
_VALIDATED_MANIFEST_SIZES: dict[tuple[str, str, str], int] = {}
_VALIDATED_MANIFEST_CACHE_LOCK = threading.Lock()
_VALIDATED_MANIFEST_CACHE: OrderedDict[tuple[str, str, str], Mapping[str, object]] = OrderedDict()


def device_session_v1_summary(
    manifest_bytes: bytes,
    session_id: str,
    manifest: Mapping[str, object] | None = None,
) -> dict[str, object]:
    """Return the sealed Device Session summary from the Python contract owner."""
    if manifest is None:
        manifest = _validated_manifest(manifest_bytes, session_id, "v3")
    try:
        if (
            manifest.get("schema") not in _DEVICE_SESSION_SCHEMA_IDS
            or manifest.get("sealed") is not True
            or manifest.get("session_id") != session_id
        ):
            raise ArtifactAccessError("not_verified", "manifest 不是 sealed device-session")
        time = manifest["time"]
        frames = manifest["frames"]
        imu = manifest["imu"]
        if not all(isinstance(value, Mapping) for value in (time, frames, imu)):
            raise ArtifactAccessError("not_verified", "manifest summary 结构无效")
        audio = manifest.get("audio")
        audio_sample_count = None
        if audio is not None:
            if not isinstance(audio, Mapping):
                raise ArtifactAccessError("not_verified", "manifest audio 结构无效")
            if audio.get("state") != "not_recorded":
                audio_sample_count = _non_negative_int(
                    audio.get("sample_count"),
                    "manifest audio sample_count 无效",
                )
        descriptors = _artifact_descriptors(manifest)
        artifacts = _artifact_descriptor_payloads(descriptors)
        return {
            "session_id": _string_value(manifest.get("session_id"), "manifest session_id 无效"),
            "display_name": _string_value(
                manifest.get("display_name"),
                "manifest display_name 无效",
            ),
            "started_at": _string_value(time.get("started_at"), "manifest started_at 无效"),
            "ended_at": _string_value(time.get("ended_at"), "manifest ended_at 无效"),
            "duration_seconds": _non_negative_number(
                time.get("duration_seconds"),
                "manifest duration_seconds 无效",
            ),
            "frames_count": _non_negative_int(
                frames.get("count"),
                "manifest frames count 无效",
            ),
            "imu_sample_count": _non_negative_int(
                imu.get("sample_count"),
                "manifest imu sample_count 无效",
            ),
            "audio_sample_count": audio_sample_count,
            "total_bytes": sum(descriptor.bytes for descriptor in descriptors.values()),
            "artifacts": artifacts,
        }
    except ArtifactAccessError:
        raise
    except (KeyError, TypeError, ValueError) as error:
        raise ArtifactAccessError("not_verified", "manifest summary 无效") from error


def _validated_manifest_with_digest(
    payload: bytes,
    session_id: str,
    api_version: str,
    *,
    manifest_sha256: str | None = None,
) -> tuple[Mapping[str, object], str]:
    if api_version not in {"v2", "v3", "v4"}:
        raise ValueError("api_version 必须是 v2、v3 或 v4")
    digest = hashlib.sha256(payload).hexdigest() if manifest_sha256 is None else manifest_sha256
    cache_key = (api_version, session_id, digest)
    with _VALIDATED_MANIFEST_CACHE_LOCK:
        cached = _VALIDATED_MANIFEST_CACHE.get(cache_key)
        if cached is not None:
            _VALIDATED_MANIFEST_CACHE.move_to_end(cache_key)
            return cached, digest

    # Decode and validate outside the cache lock so concurrent manifest reads do not serialize.
    decoder = facade_attr(
        "rp_ylx.api.downloads", "_decode_and_validate_manifest", _decode_and_validate_manifest
    )
    manifest = decoder(payload, session_id, api_version)

    with _VALIDATED_MANIFEST_CACHE_LOCK:
        cached = _VALIDATED_MANIFEST_CACHE.get(cache_key)
        if cached is not None:
            _VALIDATED_MANIFEST_CACHE.move_to_end(cache_key)
            return cached, digest
        _VALIDATED_MANIFEST_CACHE[cache_key] = manifest
        _VALIDATED_MANIFEST_SIZES[cache_key] = len(payload)
        _VALIDATED_MANIFEST_CACHE.move_to_end(cache_key)
        while (
            len(_VALIDATED_MANIFEST_CACHE) > _VALIDATED_MANIFEST_CACHE_LIMIT
            or sum(_VALIDATED_MANIFEST_SIZES.values()) > _VALIDATED_MANIFEST_CACHE_BYTES
        ):
            evicted, _ = _VALIDATED_MANIFEST_CACHE.popitem(last=False)
            _VALIDATED_MANIFEST_SIZES.pop(evicted, None)
        return manifest, digest


def _validated_manifest(
    payload: bytes,
    session_id: str,
    api_version: str,
    *,
    manifest_sha256: str | None = None,
) -> Mapping[str, object]:
    manifest, _ = _validated_manifest_with_digest(
        payload,
        session_id,
        api_version,
        manifest_sha256=manifest_sha256,
    )
    return manifest


def _decode_and_validate_manifest(
    payload: bytes, session_id: str, api_version: str
) -> Mapping[str, object]:
    try:
        decoded = payload.decode("utf-8")
        manifest = json.loads(
            decoded,
            object_pairs_hook=_unique_object,
            parse_constant=lambda value: (_ for _ in ()).throw(ValueError(value)),
        )
    except (ValueError, RecursionError) as error:
        raise ArtifactAccessError("not_found", "会话 manifest 无效") from error
    if not isinstance(manifest, dict) or manifest.get("session_id") != session_id:
        raise ArtifactAccessError("not_found", "会话 manifest 身份不匹配")
    schema = manifest.get("schema")
    is_device_session = (
        isinstance(schema, str)
        and schema in _DEVICE_SESSION_SCHEMA_IDS
        and manifest.get("sealed") is True
    )
    is_v0 = (
        manifest.get("format") == "ylx.recording-session.v0" and manifest.get("state") == "sealed"
    )
    if not is_device_session and not (api_version == "v2" and is_v0):
        raise ArtifactAccessError("not_found", "会话不存在或尚未封存")
    try:
        if is_device_session:
            validator = facade_attr(
                "rp_ylx.api.downloads",
                "_validate_device_session_manifest",
                _validate_device_session_manifest,
            )
            validator(manifest)
        else:
            _validate_recording_session_v0(manifest)
    except RecursionError as error:
        raise ArtifactAccessError("not_verified", "manifest 嵌套结构过深") from error
    return manifest


def _clear_validated_manifest_cache_for_tests() -> None:
    with _VALIDATED_MANIFEST_CACHE_LOCK:
        _VALIDATED_MANIFEST_CACHE.clear()
        _VALIDATED_MANIFEST_SIZES.clear()


def _unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate key: {key}")
        result[key] = value
    return result


def _artifact_descriptors(manifest: Mapping[str, object]) -> dict[str, ArtifactDescriptor]:
    raw_descriptors: list[object]
    legacy = False
    if manifest.get("schema") in _DEVICE_SESSION_SCHEMA_IDS:
        raw_descriptors = list(iter_device_session_v1_artifacts(manifest))
    else:
        artifacts = manifest.get("artifacts")
        if not isinstance(artifacts, list):
            raise ArtifactAccessError("not_verified", "manifest artifact 清单无效")
        raw_descriptors = artifacts
        legacy = True

    descriptors: dict[str, ArtifactDescriptor] = {}
    paths: set[str] = set()
    for raw in raw_descriptors:
        descriptor = _artifact_descriptor(raw, legacy=legacy)
        existing = descriptors.get(descriptor.artifact_id)
        if descriptor.path in paths:
            raise ArtifactAccessError("not_verified", "manifest artifact 路径重复")
        if existing is not None and (
            descriptor.bytes != existing.bytes
            or descriptor.media_type != existing.media_type
            or descriptor.sha256 != existing.sha256
        ):
            raise ArtifactAccessError("not_verified", "同一 artifact 身份的表示元数据不一致")
        if existing is None:
            descriptors[descriptor.artifact_id] = descriptor
        paths.add(descriptor.path)
    if not descriptors:
        raise ArtifactAccessError("not_verified", "manifest 未声明 artifact")
    return descriptors


def _artifact_descriptor_payloads(
    descriptors: Mapping[str, ArtifactDescriptor],
) -> list[dict[str, object]]:
    return [
        {
            "artifact_id": descriptor.artifact_id,
            "role": descriptor.role,
            "path": descriptor.path,
            "media_type": descriptor.media_type,
            "bytes": descriptor.bytes,
            "sha256": descriptor.sha256,
        }
        for descriptor in descriptors.values()
    ]


def _string_value(value: object, message: str) -> str:
    if not isinstance(value, str):
        raise ArtifactAccessError("not_verified", message)
    return value


def _non_negative_int(value: object, message: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ArtifactAccessError("not_verified", message)
    return value


def _non_negative_number(value: object, message: str) -> int | float:
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not math.isfinite(value)
        or value < 0
    ):
        raise ArtifactAccessError("not_verified", message)
    return value


def iter_device_session_v1_artifacts(
    manifest: Mapping[str, object],
) -> Iterator[Mapping[str, object]]:
    """Yield every artifact descriptor declared by a Device Session manifest."""
    try:
        video = manifest["video"]
        frames = manifest["frames"]
        imu = manifest["imu"]
    except KeyError as error:
        raise ArtifactAccessError("not_verified", "manifest artifact 清单无效") from error
    if not all(isinstance(value, Mapping) for value in (video, frames, imu)):
        raise ArtifactAccessError("not_verified", "manifest artifact 清单无效")

    layout = video.get("layout")
    if layout == "raw-side-by-side":
        artifact = video.get("artifact")
        if not isinstance(artifact, Mapping):
            raise ArtifactAccessError("not_verified", "manifest video artifact 无效")
        yield artifact
    elif layout == "split-eyes":
        segments = video.get("segments")
        if not isinstance(segments, list):
            raise ArtifactAccessError("not_verified", "manifest video segments 无效")
        for segment in segments:
            if not isinstance(segment, Mapping):
                raise ArtifactAccessError("not_verified", "manifest video segment 无效")
            artifacts = segment.get("artifacts")
            if not isinstance(artifacts, Mapping):
                raise ArtifactAccessError("not_verified", "manifest video segment artifact 无效")
            for eye in ("left", "right"):
                artifact = artifacts.get(eye)
                if not isinstance(artifact, Mapping):
                    raise ArtifactAccessError(
                        "not_verified", "manifest video segment artifact 无效"
                    )
                yield artifact
    else:
        raise ArtifactAccessError("not_verified", "manifest video layout 无效")

    for section in (frames, imu):
        artifact = section.get("artifact")
        if not isinstance(artifact, Mapping):
            raise ArtifactAccessError("not_verified", "manifest artifact 清单无效")
        yield artifact

    audio = manifest.get("audio")
    if audio is not None:
        if not isinstance(audio, Mapping):
            raise ArtifactAccessError("not_verified", "manifest audio 结构无效")
        if audio.get("state") != "not_recorded":
            segments = audio.get("segments")
            if not isinstance(segments, list):
                raise ArtifactAccessError("not_verified", "manifest audio segments 无效")
            for segment in segments:
                if not isinstance(segment, Mapping):
                    raise ArtifactAccessError("not_verified", "manifest audio segment 无效")
                artifact = segment.get("artifact")
                if not isinstance(artifact, Mapping):
                    raise ArtifactAccessError("not_verified", "manifest audio artifact 无效")
                yield artifact

    logs = manifest.get("logs", [])
    if not isinstance(logs, list):
        raise ArtifactAccessError("not_verified", "manifest logs 无效")
    for artifact in logs:
        if not isinstance(artifact, Mapping):
            raise ArtifactAccessError("not_verified", "manifest log artifact 无效")
        yield artifact


def _validate_device_session_v1(manifest: Mapping[str, object]) -> None:
    try:
        validate_manifest_schema(_DEVICE_SESSION_V1_VALIDATOR, manifest)
    except ValidationError as error:
        raise ArtifactAccessError(
            "not_verified", "manifest 不符合 device-session v1 契约"
        ) from error

    _validate_device_session_semantics(manifest, schema_version="v1")


def _validate_device_session_v2(manifest: Mapping[str, object]) -> None:
    try:
        validate_manifest_schema(_DEVICE_SESSION_V2_VALIDATOR, manifest)
    except ValidationError as error:
        raise ArtifactAccessError(
            "not_verified", "manifest 不符合 device-session v2 契约"
        ) from error

    _validate_device_session_semantics(manifest, schema_version="v2")


def _validate_device_session_manifest(manifest: Mapping[str, object]) -> None:
    schema = manifest.get("schema")
    if not isinstance(schema, str):
        raise ArtifactAccessError("not_verified", "manifest schema 必须是字符串")
    if schema == _DEVICE_SESSION_V1_SCHEMA_ID:
        validator = facade_attr(
            "rp_ylx.api.downloads", "_validate_device_session_v1", _validate_device_session_v1
        )
        validator(manifest)
        return
    if schema == _DEVICE_SESSION_V2_SCHEMA_ID:
        _validate_device_session_v2(manifest)
        return
    if schema in {_DEVICE_SESSION_V3_SCHEMA_ID, _DEVICE_SESSION_V4_SCHEMA_ID}:
        is_v4 = schema == _DEVICE_SESSION_V4_SCHEMA_ID
        try:
            validator = _DEVICE_SESSION_V4_VALIDATOR if is_v4 else _DEVICE_SESSION_V3_VALIDATOR
            validate_manifest_schema(validator, manifest)
        except ValidationError as error:
            version_label = "v4" if is_v4 else "v3"
            raise ArtifactAccessError(
                "not_verified", f"manifest 不符合 device-session {version_label} 契约"
            ) from error
        _validate_device_session_semantics(manifest, schema_version="v2")
        return
    raise ArtifactAccessError("not_verified", "manifest 不是支持的 device-session 契约")


def _validate_device_session_semantics(
    manifest: Mapping[str, object],
    *,
    schema_version: str,
) -> None:
    camera = manifest["camera"]
    time = manifest["time"]
    integrity = manifest["integrity"]
    take = manifest["take"]
    if not all(isinstance(value, Mapping) for value in (camera, time, integrity, take)):
        raise ArtifactAccessError("not_verified", "manifest device-session 结构无效")
    if camera["width"] != camera["eye_width"] * 2:
        raise ArtifactAccessError("not_verified", "camera width 与 eye_width 不一致")
    nominal_fps = camera["sensor_fps"] / camera["frame_decimation"]
    if take["continuation_of"] == manifest["session_id"]:
        raise ArtifactAccessError("not_verified", "会话不能继续自身")

    started = _api_datetime(time["started_at"])
    ended = _api_datetime(time["ended_at"])
    verified = _api_datetime(integrity["verified_at"])
    sealed = _api_datetime(manifest["sealed_at"])
    if not started <= ended <= verified <= sealed:
        raise ArtifactAccessError("not_verified", "manifest 时间顺序无效")
    if (
        "duration_clock" not in time
        and abs(time["duration_seconds"] - (ended - started).total_seconds()) > 0.001
    ):
        raise ArtifactAccessError("not_verified", "manifest duration_seconds 与时间戳不一致")
    frames = manifest["frames"]
    if not isinstance(frames, Mapping):
        raise ArtifactAccessError("not_verified", "manifest frames 结构无效")

    drops = integrity["drop_events"]
    dropped_sum = 0
    previous_end: int | None = None
    for drop in drops:
        if (
            drop["end_frame"] <= drop["start_frame"]
            or drop["dropped"] != drop["end_frame"] - drop["start_frame"]
            or (previous_end is not None and drop["start_frame"] <= previous_end)
        ):
            raise ArtifactAccessError("not_verified", "manifest drop event 无效")
        dropped_sum += drop["dropped"]
        previous_end = drop["end_frame"]
    if dropped_sum != integrity["dropped_frames"]:
        raise ArtifactAccessError("not_verified", "dropped_frames 与 drop event 不一致")
    policy = integrity.get("quality_policy")
    has_measured_semantics = "nominal_fps" in camera and policy is not None
    if ("nominal_fps" in camera) != (policy is not None):
        raise ArtifactAccessError("not_verified", "manifest FPS 与质量策略版本不完整")
    if not has_measured_semantics:
        if dropped_sum:
            raise ArtifactAccessError("not_verified", "legacy v1 非零丢帧缺少可验证质量策略")
        if abs(camera["effective_fps"] - nominal_fps) > 1e-9:
            raise ArtifactAccessError(
                "not_verified", "legacy camera effective_fps 与抽帧配置不一致"
            )
    else:
        assert isinstance(policy, Mapping)
        if abs(camera["nominal_fps"] - nominal_fps) > 1e-9:
            raise ArtifactAccessError("not_verified", "camera nominal_fps 与抽帧配置不一致")
        measured_fps = (
            0.0 if time["duration_seconds"] == 0 else frames["count"] / time["duration_seconds"]
        )
        if abs(camera["effective_fps"] - measured_fps) > 1e-9:
            raise ArtifactAccessError("not_verified", "camera effective_fps 不是实际保留帧率")
        total = frames["count"] + dropped_sum
        fraction = 0.0 if total == 0 else dropped_sum / total
        contiguous = max((drop["dropped"] for drop in drops), default=0)
        window_drops = 0
        window_seconds = policy["window_seconds"]
        if drops and window_seconds > 0:
            ordered_drops = sorted(
                ((drop["at_time_seconds"], drop["dropped"]) for drop in drops),
                key=lambda item: item[0],
            )
            right = 0
            running = 0
            for left, (left_time, left_dropped) in enumerate(ordered_drops):
                if right < left:
                    right = left
                    running = 0
                window_end = left_time + window_seconds
                while right < len(ordered_drops) and ordered_drops[right][0] < window_end:
                    running += ordered_drops[right][1]
                    right += 1
                if running > window_drops:
                    window_drops = running
                if right > left:
                    running -= left_dropped
        if (
            contiguous > policy["max_contiguous_dropped_frames"]
            or dropped_sum > policy["max_total_dropped_frames"]
            or fraction > policy["max_drop_fraction"]
            or window_drops > policy["max_dropped_frames_per_window"]
        ):
            raise ArtifactAccessError("not_verified", "manifest 丢帧超过质量策略")

    video = manifest["video"]
    if isinstance(video, Mapping) and video.get("layout") == "split-eyes":
        _validate_split_eye_segments(manifest, video, drops, dropped_sum)
    audio = manifest.get("audio")
    if audio is not None:
        if not isinstance(audio, Mapping):
            raise ArtifactAccessError("not_verified", "manifest audio 结构无效")
        if schema_version == "v2":
            _validate_audio_v2(manifest, audio)
        else:
            _validate_audio_v1(audio)


def validate_device_session_manifest(manifest: Mapping[str, object]) -> None:
    """验证生产者与下载路径共享的 Device Session v1/v2/v3/v4 语义。"""
    _validate_device_session_manifest(manifest)


def validated_device_session_payload(payload: bytes, session_id: str) -> Mapping[str, object]:
    """Validate exact immutable bytes, sharing the download schema cache.

    Only schema/semantic work is cached. Callers must still verify current
    manifest bytes and artifact identities/digests. Return an isolated copy so
    producer or validation callers cannot mutate the cached parsed document.
    """
    manifest = _validated_manifest(payload, session_id, "v4")
    return copy.deepcopy(manifest)


def _validate_recording_session_v0(manifest: Mapping[str, object]) -> None:
    try:
        _RECORDING_SESSION_VALIDATOR.validate(manifest)
    except ValidationError as error:
        raise ArtifactAccessError(
            "not_verified", "manifest 不符合 recording-session v0 契约"
        ) from error

    time = manifest["time"]
    if not isinstance(time, Mapping):
        raise ArtifactAccessError("not_verified", "manifest v0 时间结构无效")
    if _api_datetime(time["ended_at"]) < _api_datetime(time["started_at"]):
        raise ArtifactAccessError("not_verified", "manifest v0 时间顺序无效")


def _validate_split_eye_segments(
    manifest: Mapping[str, object],
    video: Mapping[str, object],
    drops: list[Mapping[str, object]],
    dropped_sum: int,
) -> None:
    segments = video["segments"]
    previous_frame_end: int | None = None
    previous_time_end: float | None = None
    for expected_index, segment in enumerate(segments):
        if (
            segment["index"] != expected_index
            or segment["start_frame"] >= segment["end_frame"]
            or segment["start_time_seconds"] >= segment["end_time_seconds"]
            or (previous_frame_end is not None and segment["start_frame"] != previous_frame_end)
            or (
                previous_time_end is not None
                and abs(segment["start_time_seconds"] - previous_time_end) > 1e-9
            )
        ):
            raise ArtifactAccessError("not_verified", "manifest video segment 无效")
        previous_frame_end = segment["end_frame"]
        previous_time_end = float(segment["end_time_seconds"])
    sequence_start = segments[0]["start_frame"]
    sequence_end = segments[-1]["end_frame"]
    if any(
        drop["start_frame"] < sequence_start or drop["end_frame"] > sequence_end for drop in drops
    ):
        raise ArtifactAccessError("not_verified", "drop event 超出视频帧域")
    frames = manifest["frames"]
    if frames["count"] != sequence_end - sequence_start - dropped_sum:
        raise ArtifactAccessError("not_verified", "frames count 与视频帧域不一致")


def _validate_audio_v1(audio: Mapping[str, object]) -> None:
    sample_rate = audio["sample_rate_hz"]
    sample_count = audio["sample_count"]
    if (
        isinstance(sample_rate, bool)
        or not isinstance(sample_rate, int)
        or isinstance(sample_count, bool)
        or not isinstance(sample_count, int)
    ):
        raise ArtifactAccessError("not_verified", "manifest audio 采样字段无效")
    sync = audio["sync"]
    segments = audio["segments"]
    if not isinstance(sync, Mapping) or not isinstance(segments, list):
        raise ArtifactAccessError("not_verified", "manifest audio 结构无效")
    if sync["stopped_monotonic_ns"] < sync["started_monotonic_ns"]:
        raise ArtifactAccessError("not_verified", "manifest audio 单调时间无效")
    if sync.get("timebase") is not None:
        _validate_audio_timeline_sync(sync)
    previous_end = 0
    for expected_index, segment in enumerate(segments):
        if not isinstance(segment, Mapping):
            raise ArtifactAccessError("not_verified", "manifest audio segment 无效")
        if (
            segment["index"] != expected_index
            or segment["start_sample"] != previous_end
            or segment["end_sample"] <= segment["start_sample"]
            or segment["start_time_seconds"] >= segment["end_time_seconds"]
        ):
            raise ArtifactAccessError("not_verified", "manifest audio segment 无效")
        start_time = segment["start_sample"] / sample_rate
        end_time = segment["end_sample"] / sample_rate
        if (
            abs(segment["start_time_seconds"] - start_time) > 1e-9
            or abs(segment["end_time_seconds"] - end_time) > 1e-9
        ):
            raise ArtifactAccessError("not_verified", "manifest audio 时间域无效")
        previous_end = segment["end_sample"]
    if previous_end != sample_count:
        raise ArtifactAccessError("not_verified", "manifest audio sample_count 不一致")


def _validate_audio_v2(manifest: Mapping[str, object], audio: Mapping[str, object]) -> None:
    state = audio.get("state")
    if state == "not_recorded":
        return
    if state != "recorded":
        raise ArtifactAccessError("not_verified", "manifest audio state 无效")
    try:
        audio_clock_report(audio, manifest["time"]["duration_seconds"])
    except (ValueError, TypeError, KeyError) as error:
        raise ArtifactAccessError("not_verified", str(error)) from error

    sample_rate = audio["sample_rate"]
    channels = audio["channels"]
    sample_count = audio["sample_count"]
    if (
        isinstance(sample_rate, bool)
        or not isinstance(sample_rate, int)
        or isinstance(channels, bool)
        or not isinstance(channels, int)
        or isinstance(sample_count, bool)
        or not isinstance(sample_count, int)
    ):
        raise ArtifactAccessError("not_verified", "manifest audio 采样字段无效")
    if audio.get("sample_format") != "S16_LE":
        raise ArtifactAccessError("not_verified", "manifest audio sample_format 无效")
    sync = audio["sync"]
    segments = audio["segments"]
    if not isinstance(sync, Mapping) or not isinstance(segments, list):
        raise ArtifactAccessError("not_verified", "manifest audio 结构无效")
    if (
        sync.get("time_base") != "host_monotonic"
        or sync.get("video_time_reference") != "session_time_seconds"
    ):
        raise ArtifactAccessError("not_verified", "manifest audio 时间线无效")
    start = sync.get("start_time_seconds")
    end = sync.get("end_time_seconds")
    duration = manifest["time"]["duration_seconds"]
    if (
        isinstance(start, bool)
        or not isinstance(start, (int, float))
        or isinstance(end, bool)
        or not isinstance(end, (int, float))
        or isinstance(duration, bool)
        or not isinstance(duration, (int, float))
        or not math.isfinite(float(start))
        or not math.isfinite(float(end))
        or not math.isfinite(float(duration))
        or float(start) < 0
        or float(end) <= float(start)
        or float(end) > float(duration) + 1e-6
    ):
        raise ArtifactAccessError("not_verified", "manifest audio sync 超出会话时间域")

    previous_end = 0
    bytes_per_sample_frame = channels * 2
    for expected_index, segment in enumerate(segments):
        if not isinstance(segment, Mapping):
            raise ArtifactAccessError("not_verified", "manifest audio segment 无效")
        if (
            segment["index"] != expected_index
            or segment["start_sample"] != previous_end
            or segment["end_sample"] <= segment["start_sample"]
            or segment["start_time_seconds"] >= segment["end_time_seconds"]
        ):
            raise ArtifactAccessError("not_verified", "manifest audio segment 无效")
        start_time = segment["start_sample"] / sample_rate
        end_time = segment["end_sample"] / sample_rate
        if (
            abs(segment["start_time_seconds"] - start_time) > 1e-9
            or abs(segment["end_time_seconds"] - end_time) > 1e-9
        ):
            raise ArtifactAccessError("not_verified", "manifest audio 时间域无效")
        pcm_payload_bytes = segment["pcm_payload_bytes"]
        wav_header_bytes = segment["wav_header_bytes"]
        artifact = segment["artifact"]
        if not isinstance(artifact, Mapping):
            raise ArtifactAccessError("not_verified", "manifest audio artifact 无效")
        expected_payload_bytes = (segment["end_sample"] - segment["start_sample"]) * (
            bytes_per_sample_frame
        )
        if (
            isinstance(pcm_payload_bytes, bool)
            or not isinstance(pcm_payload_bytes, int)
            or isinstance(wav_header_bytes, bool)
            or not isinstance(wav_header_bytes, int)
            or pcm_payload_bytes != expected_payload_bytes
            or (
                artifact.get("media_type") != "audio/flac"
                and artifact["bytes"] != pcm_payload_bytes + wav_header_bytes
            )
            or (
                artifact.get("media_type") == "audio/flac"
                and artifact.get("storage_encoding", {}).get("pcm_bytes") != pcm_payload_bytes
            )
        ):
            raise ArtifactAccessError("not_verified", "manifest audio bytes 域无效")
        previous_end = segment["end_sample"]
    if previous_end != sample_count:
        raise ArtifactAccessError("not_verified", "manifest audio sample_count 不一致")


def _validate_audio_timeline_sync(sync: Mapping[str, object]) -> None:
    if sync.get("clock") != "host_monotonic" or sync.get("timebase") != "monotonic_ns":
        raise ArtifactAccessError("not_verified", "manifest audio 时间线无效")
    required_ints = (
        "session_start_monotonic_ns",
        "started_monotonic_ns",
        "stopped_monotonic_ns",
        "session_start_offset_ns",
        "session_stop_offset_ns",
        "sample_duration_ns",
    )
    for field in required_ints:
        value = sync.get(field)
        if isinstance(value, bool) or not isinstance(value, int):
            raise ArtifactAccessError("not_verified", "manifest audio 时间线无效")
    session_start = sync["session_start_monotonic_ns"]
    started = sync["started_monotonic_ns"]
    stopped = sync["stopped_monotonic_ns"]
    start_offset = sync["session_start_offset_ns"]
    stop_offset = sync["session_stop_offset_ns"]
    sample_duration = sync["sample_duration_ns"]
    if (
        session_start <= 0
        or started <= 0
        or stopped <= 0
        or stopped < started
        or start_offset < 0
        or sample_duration <= 0
        or start_offset != started - session_start
        or stop_offset != stopped - session_start
        or stop_offset < start_offset
    ):
        raise ArtifactAccessError("not_verified", "manifest audio 时间线无效")
    start_seconds = sync.get("session_start_offset_seconds")
    stop_seconds = sync.get("session_stop_offset_seconds")
    if (
        isinstance(start_seconds, bool)
        or not isinstance(start_seconds, (int, float))
        or isinstance(stop_seconds, bool)
        or not isinstance(stop_seconds, (int, float))
        or not math.isfinite(float(start_seconds))
        or not math.isfinite(float(stop_seconds))
        or abs(float(start_seconds) - start_offset / 1e9) > 1e-9
        or abs(float(stop_seconds) - stop_offset / 1e9) > 1e-9
    ):
        raise ArtifactAccessError("not_verified", "manifest audio 时间线无效")


def _api_datetime(value: object) -> datetime:
    try:
        return parse_rfc3339(value)
    except ValueError as error:
        raise ArtifactAccessError("not_verified", "manifest 时间戳无效") from error


def _artifact_descriptor(raw: object, *, legacy: bool) -> ArtifactDescriptor:
    if not isinstance(raw, dict):
        raise ArtifactAccessError("not_verified", "manifest artifact 描述符无效")
    required = {"role", "path", "media_type", "bytes", "sha256"}
    expected = required | ({"records"} if legacy else {"artifact_id"})
    if set(raw) - (set() if legacy else {"storage_encoding"}) != expected:
        raise ArtifactAccessError("not_verified", "manifest artifact 描述符字段无效")
    sha256 = raw.get("sha256")
    artifact_id = sha256 if legacy else raw.get("artifact_id")
    role = raw.get("role")
    path = raw.get("path")
    media_type = raw.get("media_type")
    size = raw.get("bytes")
    if (
        not valid_sha256(artifact_id)
        or not valid_sha256(sha256)
        or artifact_id != sha256
        or (not legacy and (not isinstance(role, str) or not role))
        or not isinstance(path, str)
        or not isinstance(media_type, str)
        or not _MEDIA_TYPE.fullmatch(media_type)
        or isinstance(size, bool)
        or not isinstance(size, int)
        or size < 0
    ):
        raise ArtifactAccessError("not_verified", "manifest artifact 描述符值无效")
    if legacy:
        records = raw.get("records")
        if isinstance(records, bool) or not isinstance(records, int) or records < 0:
            raise ArtifactAccessError("not_verified", "manifest artifact records 无效")
    _safe_relative_components(path)
    return ArtifactDescriptor(
        artifact_id=artifact_id,
        role=role if isinstance(role, str) else "",
        path=path,
        media_type=media_type,
        bytes=size,
        sha256=sha256,
    )

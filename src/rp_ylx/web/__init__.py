"""Open Aria Conductor 托管的 Open Aria Echo / Web 静态制品。

Echo / Web 是独立构建的浏览器客户端；Conductor 只固定并托管一份生成后的
静态制品。托管集合、内容类型、字节数和 sha256 全部来自内部 ``assets.json``，
不从扩展名猜类型，也不继续拥有 Node/TypeScript 客户端源码。
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Iterable
from functools import lru_cache
from importlib.resources import files
from typing import NamedTuple

MANIFEST_NAME = "assets.json"
MANIFEST_SCHEMA_V1 = "openaria.echo-web-artifacts.v1"
MANIFEST_SCHEMA = "openaria.echo-web-artifacts.v2"
ENTRY_ASSET = "index.html"
ECHO_WEB_SOURCE_REPOSITORY = "https://github.com/Alpenl/openaria-echo-web"
ECHO_WEB_SOURCE_COMMIT = "a858d48dfc745ed311fb4150c191a629c69ffaef"
DEVICE_API_VERSION = re.compile(r"^v([1-9][0-9]*)$")


class WebAsset(NamedTuple):
    """制品清单里的一条：路径、字节数、内容类型和 sha256。"""

    path: str
    size: int
    content_type: str
    sha256: str


class EchoWebArtifactError(RuntimeError):
    """制品清单缺失、schema 不认识或实际字节与清单不一致。"""

    def __init__(self, message: str, *, code: str = "echo_web_artifact_invalid") -> None:
        super().__init__(message)
        self.code = code


def _required_device_api_major(document: dict[str, object]) -> int | None:
    schema = document.get("schema")
    if schema == MANIFEST_SCHEMA_V1:
        return None
    if schema != MANIFEST_SCHEMA:
        raise EchoWebArtifactError(f"未知的 Echo / Web 制品清单 schema：{schema!r}")

    compatibility = document.get("compatibility")
    if not isinstance(compatibility, dict) or set(compatibility) != {"device_api"}:
        raise EchoWebArtifactError("Echo / Web v2 制品清单缺少 device_api 兼容声明")
    device_api = compatibility["device_api"]
    if not isinstance(device_api, dict) or set(device_api) != {"required_major"}:
        raise EchoWebArtifactError("Echo / Web v2 制品清单缺少 required_major")
    required_major = device_api["required_major"]
    if (
        isinstance(required_major, bool)
        or not isinstance(required_major, int)
        or required_major < 1
    ):
        raise EchoWebArtifactError("Echo / Web v2 制品清单的 required_major 无效")
    return required_major


@lru_cache(maxsize=1)
def _manifest() -> tuple[str, str, int | None, dict[str, WebAsset]]:
    try:
        raw = files(__package__).joinpath(MANIFEST_NAME).read_bytes()
    except OSError as error:  # pragma: no cover - 安装包损坏
        raise EchoWebArtifactError("Echo / Web 制品清单不可读") from error

    try:
        document = json.loads(raw)
    except json.JSONDecodeError as error:
        raise EchoWebArtifactError("Echo / Web 制品清单不是有效 JSON") from error

    if not isinstance(document, dict):
        raise EchoWebArtifactError("Echo / Web 制品清单根节点必须是 object")
    required_device_api_major = _required_device_api_major(document)

    assets: dict[str, WebAsset] = {}
    entries = document.get("files")
    if not isinstance(entries, list):
        raise EchoWebArtifactError("Echo / Web 制品清单缺少 files 数组")
    for entry in entries:
        try:
            asset = WebAsset(
                path=entry["path"],
                size=int(entry["bytes"]),
                content_type=entry["content_type"],
                sha256=entry["sha256"],
            )
        except (KeyError, TypeError, ValueError) as error:
            raise EchoWebArtifactError("Echo / Web 制品条目无效") from error
        if "/" in asset.path or asset.path in {".", ".."}:
            raise EchoWebArtifactError(f"Echo / Web 制品路径越界：{asset.path!r}")
        assets[asset.path] = asset
    if ENTRY_ASSET not in assets:
        raise EchoWebArtifactError("Echo / Web 制品清单缺少入口 index.html")
    return (
        str(document.get("name", "")),
        str(document.get("version", "")),
        required_device_api_major,
        assets,
    )


def echo_web_release() -> tuple[str, str]:
    """返回当前固定的 Echo / Web 制品身份（名称与版本）。"""
    name, version, _, _ = _manifest()
    return name, version


def echo_web_required_device_api_major() -> int | None:
    """返回 v2 制品要求的 Device API major；旧 v1 制品没有该声明。"""
    return _manifest()[2]


def assert_compatible_web_device_api(provided_versions: Iterable[str]) -> None:
    """在 gateway 启动时拒绝不满足 Device API 要求的 Echo 制品。"""
    required_major = echo_web_required_device_api_major()
    if required_major is None:
        return

    provided_majors = {
        int(match.group(1))
        for version in provided_versions
        if (match := DEVICE_API_VERSION.fullmatch(version)) is not None
    }
    if required_major not in provided_majors:
        raise EchoWebArtifactError(
            "Echo / Web 制品要求 Device API "
            f"v{required_major}，gateway 仅提供 {sorted(provided_majors)}",
            code="echo_web_device_api_incompatible",
        )


def echo_web_source() -> tuple[str, str]:
    """返回生成当前静态制品的独立 Echo / Web 源码仓身份。"""
    return ECHO_WEB_SOURCE_REPOSITORY, ECHO_WEB_SOURCE_COMMIT


def web_assets() -> dict[str, WebAsset]:
    """返回托管闭集：路径 -> 制品条目。"""
    return dict(_manifest()[3])


WEB_ASSETS: tuple[str, ...] = tuple(_manifest()[3])


def asset_content_type(name: str) -> str:
    assets = _manifest()[3]
    if name not in assets:
        raise ValueError(f"未登记的 Echo / Web 资源：{name}")
    return assets[name].content_type


def read_asset(name: str) -> bytes:
    """读取一个已登记的 Echo / Web 资源。

    未登记名字直接拒绝：托管集合是闭集，不接受清单之外的任何路径。
    字节必须与清单声明的 size 和 sha256 一致，否则 fail closed。
    """
    assets = _manifest()[3]
    asset = assets.get(name)
    if asset is None:
        raise ValueError(f"未登记的 Echo / Web 资源：{name}")
    try:
        payload = files(__package__).joinpath(name).read_bytes()
    except OSError as error:
        raise EchoWebArtifactError(f"Echo / Web 制品 {name} 不可读") from error
    if len(payload) != asset.size:
        raise EchoWebArtifactError(
            f"Echo / Web 制品 {name} 大小与清单不符：{len(payload)} != {asset.size}"
        )
    digest = hashlib.sha256(payload).hexdigest()
    if digest != asset.sha256:
        raise EchoWebArtifactError(f"Echo / Web 制品 {name} 的 sha256 与清单不符")
    return payload


__all__ = [
    "ECHO_WEB_SOURCE_COMMIT",
    "ECHO_WEB_SOURCE_REPOSITORY",
    "ENTRY_ASSET",
    "EchoWebArtifactError",
    "MANIFEST_NAME",
    "WEB_ASSETS",
    "WebAsset",
    "asset_content_type",
    "assert_compatible_web_device_api",
    "echo_web_release",
    "echo_web_required_device_api_major",
    "echo_web_source",
    "read_asset",
    "web_assets",
]

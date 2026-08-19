"""随 RP-YLX 安装包分发的离线设备工作台资源。"""

from __future__ import annotations

from importlib.resources import files

WEB_ASSETS = (
    "index.html",
    "styles.css",
    "app.js",
    "api-client.js",
    "state.js",
    "event-stream.js",
    "preview.js",
)


def read_asset(name: str) -> bytes:
    """读取一个已登记的工作台静态资源。"""
    if name not in WEB_ASSETS:
        raise ValueError(f"未知的 Web 资源：{name}")
    return files(__package__).joinpath(name).read_bytes()


__all__ = ["WEB_ASSETS", "read_asset"]

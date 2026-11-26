"""Bootstrap HTTP/HTTPS device endpoint loader reading only device.json."""

from __future__ import annotations

import ipaddress
import ssl
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

from rp_ylx.core.fs import read_bearer_token_file, read_bounded_json, require_regular_file

_MAX_DEVICE_JSON_BYTES = 64 * 1024

__all__ = ["BootstrapEndpoint", "load_bootstrap_endpoint"]


@dataclass(frozen=True, slots=True)
class BootstrapEndpoint:
    """Resolved local device API connection parameters from bootstrap config."""

    scheme: str
    host: str
    port: int
    url: str
    bearer_token: str | None
    ssl_context: ssl.SSLContext | None


def _resolve_connect_host(raw_host: object) -> tuple[str, str]:
    if not isinstance(raw_host, str) or not raw_host or "%" in raw_host:
        raise ValueError("bootstrap listen.host 无效")
    try:
        address = ipaddress.ip_address(raw_host)
    except ValueError as error:
        raise ValueError("bootstrap listen.host 无效") from error
    if address.is_multicast:
        raise ValueError("bootstrap listen.host 不允许组播地址")
    if address.is_unspecified:
        mapped = "::1" if address.version == 6 else "127.0.0.1"
    else:
        mapped = raw_host
    authority = f"[{mapped}]" if address.version == 6 else mapped
    return mapped, authority


def _build_tls_context(ca_path: Path, *, check_hostname: bool) -> ssl.SSLContext:
    try:
        require_regular_file(ca_path)
    except (OSError, ValueError) as error:
        raise ValueError("无法读取 TLS 证书文件") from error
    try:
        context = ssl.create_default_context(cafile=str(ca_path))
    except (OSError, ssl.SSLError) as error:
        raise ValueError("无法初始化 TLS 信任上下文") from error
    context.minimum_version = ssl.TLSVersion.TLSv1_2
    context.verify_mode = ssl.CERT_REQUIRED
    context.check_hostname = check_hostname
    partial_chain = getattr(ssl, "VERIFY_X509_PARTIAL_CHAIN", 0)
    if partial_chain:
        context.verify_flags |= partial_chain
    return context


def load_bootstrap_endpoint(
    config_path: Path,
    *,
    check_hostname: bool = True,
) -> BootstrapEndpoint:
    """Read listen + security from device.json without ever opening settings.sqlite3."""
    try:
        config = read_bounded_json(Path(config_path), max_bytes=_MAX_DEVICE_JSON_BYTES)
    except (OSError, ValueError) as error:
        raise ValueError(f"无法读取生产配置：{error}") from error
    if not isinstance(config, Mapping):
        raise ValueError("生产配置顶层字段无效")

    listen = config.get("listen")
    security = config.get("security")
    if not isinstance(listen, Mapping) or not isinstance(security, Mapping):
        raise ValueError("bootstrap 配置缺少 listen 或 security")

    port = listen.get("port")
    if type(port) is not int or not 1 <= port <= 65535:
        raise ValueError("bootstrap listen.port 无效")
    host, authority = _resolve_connect_host(listen.get("host"))

    profile = security.get("profile")
    if profile not in {"lab", "customer"}:
        raise ValueError("bootstrap security.profile 无效")

    scheme = "https" if profile == "customer" else "http"
    bearer_token: str | None = None
    ssl_context: ssl.SSLContext | None = None
    if profile == "customer":
        token_raw = security.get("bearer_token_file")
        ca_raw = security.get("ca_file")
        cert_raw = security.get("tls_certificate_file") if ca_raw is None else ca_raw
        if not isinstance(token_raw, str) or not token_raw:
            raise ValueError("Customer profile 缺少 bearer_token_file")
        if not isinstance(cert_raw, str) or not cert_raw:
            raise ValueError("Customer profile 缺少 TLS 证书路径")
        token_path = Path(token_raw)
        cert_path = Path(cert_raw)
        if not token_path.is_absolute() or not cert_path.is_absolute():
            raise ValueError("Customer profile 凭据与证书路径必须是绝对路径")
        bearer_token = read_bearer_token_file(token_path)
        ssl_context = _build_tls_context(cert_path, check_hostname=check_hostname)

    return BootstrapEndpoint(
        scheme=scheme,
        host=host,
        port=port,
        url=f"{scheme}://{authority}:{port}",
        bearer_token=bearer_token,
        ssl_context=ssl_context,
    )

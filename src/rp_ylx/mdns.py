"""Embedded mDNS publication for the device Web control plane."""

from __future__ import annotations

import ipaddress
import logging
import threading
from collections.abc import Callable, Mapping
from contextlib import suppress
from typing import Protocol

from zeroconf import IPVersion, ServiceInfo, Zeroconf

from rp_ylx.runtime import collect_linux_runtime

MDNS_HOSTNAME = "rp-ylx.local."
MDNS_SERVICE_TYPES = ("_ylx-capture._tcp.local.", "_http._tcp.local.")
CUSTOMER_MDNS_SERVICE_TYPES = ("_ylx-capture._tcp.local.", "_https._tcp.local.")


class ZeroconfResponder(Protocol):
    def register_service(
        self,
        info: ServiceInfo,
        ttl: int | None = None,
        allow_name_change: bool = False,
        cooperating_responders: bool = False,
        strict: bool = True,
    ) -> None: ...

    def unregister_service(self, info: ServiceInfo) -> None: ...

    def close(self) -> None: ...


AddressProvider = Callable[[], tuple[str, ...]]
ResponderFactory = Callable[[tuple[str, ...]], ZeroconfResponder]


def runtime_ipv4_addresses() -> tuple[str, ...]:
    """Return supported device-network IPv4 addresses, without loopback or link-local values."""

    runtime = collect_linux_runtime()
    network = runtime.get("network")
    if not isinstance(network, Mapping):
        return ()
    discovered: set[str] = set()
    for name in ("ap", "wifi_client", "wired"):
        status = network.get(name)
        if not isinstance(status, Mapping):
            continue
        addresses = status.get("addresses")
        if not isinstance(addresses, list):
            continue
        for candidate in addresses:
            if not isinstance(candidate, str):
                continue
            try:
                address = ipaddress.ip_address(candidate.partition("/")[0])
            except ValueError:
                continue
            if (
                address.version == 4
                and not address.is_loopback
                and not address.is_link_local
                and not address.is_unspecified
            ):
                discovered.add(str(address))
    return tuple(sorted(discovered))


def _default_responder(addresses: tuple[str, ...]) -> ZeroconfResponder:
    return Zeroconf(interfaces=list(addresses), ip_version=IPVersion.V4Only)


def _service_infos(
    port: int,
    addresses: tuple[str, ...],
    scheme: str,
) -> tuple[ServiceInfo, ...]:
    service_types = CUSTOMER_MDNS_SERVICE_TYPES if scheme == "https" else MDNS_SERVICE_TYPES
    return tuple(
        ServiceInfo(
            service_type,
            f"RP-YLX.{service_type}",
            port=port,
            properties={"path": "/", "scheme": scheme, "api": "/api/v4/device"},
            server=MDNS_HOSTNAME,
            parsed_addresses=list(addresses),
        )
        for service_type in service_types
    )


class MdnsPublisher:
    """Publish RP-YLX discovery records and refresh them after address changes."""

    def __init__(
        self,
        port: int,
        *,
        scheme: str = "http",
        address_provider: AddressProvider = runtime_ipv4_addresses,
        responder_factory: ResponderFactory = _default_responder,
        interval: float = 2.0,
        logger: logging.Logger | None = None,
    ) -> None:
        if not 1 <= port <= 65535 or interval <= 0 or scheme not in {"http", "https"}:
            raise ValueError("mDNS port and interval must be positive")
        self._port = port
        self._scheme = scheme
        self._address_provider = address_provider
        self._responder_factory = responder_factory
        self._interval = interval
        self._logger = logger or logging.getLogger(__name__)
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._addresses: tuple[str, ...] = ()
        self._responder: ZeroconfResponder | None = None
        self._services: tuple[ServiceInfo, ...] = ()

    def start(self) -> None:
        if self._thread is not None:
            raise RuntimeError("mDNS publisher has already been started")
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, name="rp-ylx-mdns", daemon=False)
        self._thread.start()

    def _run(self) -> None:
        while not self._stop.is_set():
            try:
                self._reconcile()
            except Exception as error:
                self._logger.error("mDNS publication failed: %s", error)
            self._stop.wait(self._interval)
        self._unpublish()

    def _reconcile(self) -> None:
        addresses = self._address_provider()
        if addresses == self._addresses and self._responder is not None:
            return
        self._unpublish()
        if not addresses:
            return
        responder = self._responder_factory(addresses)
        registered: list[ServiceInfo] = []
        try:
            for service in _service_infos(self._port, addresses, self._scheme):
                responder.register_service(service, allow_name_change=True)
                registered.append(service)
        except Exception:
            for service in reversed(registered):
                with suppress(Exception):
                    responder.unregister_service(service)
            with suppress(Exception):
                responder.close()
            raise
        self._addresses = addresses
        self._responder = responder
        self._services = tuple(registered)

    def _unpublish(self) -> None:
        responder, services = self._responder, self._services
        self._addresses = ()
        self._responder = None
        self._services = ()
        if responder is None:
            return
        for service in reversed(services):
            with suppress(Exception):
                responder.unregister_service(service)
        with suppress(Exception):
            responder.close()

    def close(self) -> None:
        self._stop.set()
        thread = self._thread
        if thread is None:
            self._unpublish()
            return
        thread.join(timeout=self._interval + 2.0)
        if thread.is_alive():
            raise RuntimeError("mDNS publisher did not stop before its deadline")
        self._thread = None


__all__ = [
    "CUSTOMER_MDNS_SERVICE_TYPES",
    "MDNS_HOSTNAME",
    "MDNS_SERVICE_TYPES",
    "MdnsPublisher",
    "runtime_ipv4_addresses",
]

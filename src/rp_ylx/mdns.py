"""Embedded mDNS publication for the device Web control plane."""

from __future__ import annotations

import ipaddress
import logging
import threading
from collections.abc import Callable, Mapping
from contextlib import suppress
from typing import Protocol

from zeroconf import IPVersion, ServiceInfo, Zeroconf

from rp_ylx.operational_logging import operational_logger
from rp_ylx.runtime import collect_linux_runtime

MDNS_HOSTNAME = "rp-ylx.local."
MDNS_SERVICE_TYPES = ("_ylx-capture._tcp.local.", "_http._tcp.local.")
CUSTOMER_MDNS_SERVICE_TYPES = ("_ylx-capture._tcp.local.", "_https._tcp.local.")
_OPERATIONAL_LOG = operational_logger("mdns")


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


def _supported_ipv4_addresses(status: object) -> tuple[str, ...]:
    if not isinstance(status, Mapping):
        return ()
    candidates = status.get("addresses")
    if not isinstance(candidates, list):
        return ()
    discovered: list[str] = []
    for candidate in candidates:
        if not isinstance(candidate, str):
            continue
        try:
            address = ipaddress.ip_address(candidate.partition("/")[0])
        except ValueError:
            continue
        normalized = str(address)
        if (
            address.version == 4
            and not address.is_loopback
            and not address.is_link_local
            and not address.is_unspecified
            and normalized not in discovered
        ):
            discovered.append(normalized)
    return tuple(discovered)


def runtime_ipv4_addresses() -> tuple[str, ...]:
    """Return one reachable IPv4 address for the fixed device hostname."""

    runtime = collect_linux_runtime()
    network = runtime.get("network")
    if not isinstance(network, Mapping):
        return ()
    default_route = network.get("default_route")
    names = [default_route] if default_route in {"wifi_client", "wired"} else []
    names.extend(name for name in ("ap", "wifi_client", "wired") if name not in names)
    for name in names:
        addresses = _supported_ipv4_addresses(network.get(name))
        if addresses:
            return addresses[:1]
    return ()


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
            f"Open Aria.{service_type}",
            port=port,
            properties={"path": "/", "scheme": scheme, "api": "/api/v4/device"},
            server=MDNS_HOSTNAME,
            parsed_addresses=list(addresses),
        )
        for service_type in service_types
    )


class MdnsPublisher:
    """Publish Open Aria discovery records and refresh them after address changes."""

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
        self._logger = logger
        self._lifecycle_lock = threading.Lock()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._addresses: tuple[str, ...] = ()
        self._responder: ZeroconfResponder | None = None
        self._services: tuple[ServiceInfo, ...] = ()
        self._failure_type: str | None = None
        self._suppressed_failures = 0

    def start(self) -> None:
        with self._lifecycle_lock:
            if self._thread is not None:
                raise RuntimeError("mDNS publisher has already been started")
            # Each attempt owns its stop signal. A target delayed past a failed
            # admission must not inherit the cleared signal of a later retry.
            stop = threading.Event()
            self._stop = stop
            thread: threading.Thread | None = None
            try:
                thread = threading.Thread(
                    target=self._run, args=(stop,), name="rp-ylx-mdns", daemon=False
                )
                self._thread = thread
                thread.start()
                _OPERATIONAL_LOG.event(
                    "mdns_publisher_started",
                    port=self._port,
                    scheme=self._scheme,
                )
            except BaseException as error:
                stop.set()
                if thread is None or thread.ident is None:
                    self._thread = None
                # A thread that actually started remains owned until close()
                # joins it. Failure reporting must not mask the admission error.
                with suppress(BaseException):
                    _OPERATIONAL_LOG.event(
                        "mdns_publisher_start_failed",
                        level="warning",
                        exception_type=type(error).__name__,
                        retryable=self._thread is None,
                    )
                raise

    def _record_failure(self, error: BaseException) -> None:
        exception_type = type(error).__name__
        if exception_type == self._failure_type:
            self._suppressed_failures += 1
            return
        suppressed_failures = self._suppressed_failures
        self._failure_type = exception_type
        self._suppressed_failures = 0
        # Diagnostics must not terminate the publication loop or control its
        # failure/recovery bookkeeping. Keep the two sinks independent.
        if self._logger is not None:
            with suppress(Exception):
                self._logger.error("mDNS publication failed: %s", exception_type)
        code = "mdns_publication_failed"
        with suppress(Exception):
            candidate = getattr(error, "code", code)
            if isinstance(candidate, str):
                code = candidate
        with suppress(Exception):
            _OPERATIONAL_LOG.event(
                "mdns_publication_failed",
                level="warning",
                error_code=code,
                exception_type=exception_type,
                suppressed_count=suppressed_failures,
            )

    def _record_recovered(self) -> None:
        if self._failure_type is None:
            return
        failure_type = self._failure_type
        suppressed_failures = self._suppressed_failures
        self._failure_type = None
        self._suppressed_failures = 0
        if self._logger is not None:
            with suppress(Exception):
                self._logger.info("mDNS publication recovered")
        with suppress(Exception):
            _OPERATIONAL_LOG.event(
                "mdns_publication_recovered",
                exception_type=failure_type,
                suppressed_count=suppressed_failures,
            )

    def _run(self, stop: threading.Event) -> None:
        # A failed admission may deliver its target only after a retry starts.
        # It owns no publication and must not unpublish the retry's responder.
        if stop.is_set():
            return
        try:
            while not stop.is_set():
                try:
                    self._reconcile()
                except Exception as error:
                    self._record_failure(error)
                else:
                    self._record_recovered()
                stop.wait(self._interval)
        finally:
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
        with suppress(Exception):
            _OPERATIONAL_LOG.event(
                "mdns_publication_ready",
                port=self._port,
                scheme=self._scheme,
                address_count=len(addresses),
            )

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
        with self._lifecycle_lock:
            self._stop.set()
            thread = self._thread
            if thread is None:
                self._unpublish()
                return
            thread.join(timeout=self._interval + 2.0)
            if thread.is_alive():
                raise RuntimeError("mDNS publisher did not stop before its deadline")
            self._thread = None
            _OPERATIONAL_LOG.event("mdns_publisher_stopped", outcome="closed")


__all__ = [
    "CUSTOMER_MDNS_SERVICE_TYPES",
    "MDNS_HOSTNAME",
    "MDNS_SERVICE_TYPES",
    "MdnsPublisher",
    "runtime_ipv4_addresses",
]

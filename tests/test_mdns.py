from __future__ import annotations

import time
import unittest
from unittest.mock import patch

from rp_ylx.mdns import (
    CUSTOMER_MDNS_SERVICE_TYPES,
    MDNS_HOSTNAME,
    MDNS_SERVICE_TYPES,
    MdnsPublisher,
    runtime_ipv4_addresses,
)


class FakeResponder:
    def __init__(self, addresses: tuple[str, ...]) -> None:
        self.addresses = addresses
        self.registered = []
        self.unregistered = []
        self.closed = False

    def register_service(self, info, **kwargs) -> None:
        self.registered.append((info, kwargs))

    def unregister_service(self, info) -> None:
        self.unregistered.append(info)

    def close(self) -> None:
        self.closed = True


class MdnsPublisherTest(unittest.TestCase):
    def test_runtime_addresses_only_include_supported_nonlocal_ipv4(self) -> None:
        runtime = {
            "network": {
                "ap": {"addresses": ["10.42.0.1/24", "invalid"]},
                "wifi_client": {"addresses": ["198.51.100.36/24", "127.0.0.1/8"]},
                "wired": {"addresses": ["169.254.1.2/16", "198.51.100.36/24"]},
            }
        }
        with patch("rp_ylx.mdns.collect_linux_runtime", return_value=runtime):
            self.assertEqual(runtime_ipv4_addresses(), ("10.42.0.1", "198.51.100.36"))

    def test_publisher_registers_both_services_and_refreshes_after_address_change(self) -> None:
        current = [("198.51.100.36",)]
        responders: list[FakeResponder] = []

        def factory(addresses: tuple[str, ...]) -> FakeResponder:
            responder = FakeResponder(addresses)
            responders.append(responder)
            return responder

        publisher = MdnsPublisher(
            8080,
            address_provider=lambda: current[0],
            responder_factory=factory,
            interval=0.01,
        )
        publisher.start()
        try:
            self._wait_for(lambda: len(responders) == 1)
            first = responders[0]
            self.assertEqual(first.addresses, ("198.51.100.36",))
            self.assertEqual(
                {service.type for service, _ in first.registered}, set(MDNS_SERVICE_TYPES)
            )
            for service, options in first.registered:
                self.assertEqual(service.server, MDNS_HOSTNAME)
                self.assertEqual(service.port, 8080)
                self.assertEqual(service.parsed_addresses(), ["198.51.100.36"])
                self.assertEqual(service.properties[b"scheme"], b"http")
                self.assertEqual(service.properties[b"api"], b"/api/v4/device")
                self.assertEqual(options, {"allow_name_change": True})

            current[0] = ("198.51.100.37",)
            self._wait_for(lambda: len(responders) == 2)
            self.assertTrue(first.closed)
            self.assertEqual(len(first.unregistered), 2)
            self.assertEqual(responders[1].addresses, ("198.51.100.37",))

            current[0] = ()
            self._wait_for(lambda: responders[1].closed)
            self.assertEqual(len(responders[1].unregistered), 2)
        finally:
            publisher.close()

    def test_publisher_scopes_service_addresses_to_each_network_interface(self) -> None:
        responders: list[FakeResponder] = []

        def factory(addresses: tuple[str, ...]) -> FakeResponder:
            responder = FakeResponder(addresses)
            responders.append(responder)
            return responder

        publisher = MdnsPublisher(
            8080,
            address_provider=lambda: ("192.168.110.36", "192.168.127.10"),
            responder_factory=factory,
            interval=0.01,
        )
        publisher.start()
        try:
            self._wait_for(lambda: len(responders) == 2)
            self.assertEqual(
                [responder.addresses for responder in responders],
                [("192.168.110.36",), ("192.168.127.10",)],
            )
            for responder in responders:
                self.assertEqual(len(responder.registered), len(MDNS_SERVICE_TYPES))
                for service, _ in responder.registered:
                    self.assertEqual(service.parsed_addresses(), list(responder.addresses))
        finally:
            publisher.close()

        self.assertTrue(all(responder.closed for responder in responders))
        self.assertTrue(
            all(len(responder.unregistered) == len(MDNS_SERVICE_TYPES) for responder in responders)
        )

    def test_customer_publisher_advertises_https_without_http_alias(self) -> None:
        responders: list[FakeResponder] = []

        def factory(addresses: tuple[str, ...]) -> FakeResponder:
            responder = FakeResponder(addresses)
            responders.append(responder)
            return responder

        publisher = MdnsPublisher(
            8080,
            scheme="https",
            address_provider=lambda: ("198.51.100.36",),
            responder_factory=factory,
            interval=0.01,
        )
        publisher.start()
        try:
            self._wait_for(lambda: len(responders) == 1)
            services = [service for service, _ in responders[0].registered]
            self.assertEqual(
                {service.type for service in services}, set(CUSTOMER_MDNS_SERVICE_TYPES)
            )
            self.assertNotIn("_http._tcp.local.", {service.type for service in services})
            self.assertTrue(all(service.properties[b"scheme"] == b"https" for service in services))
        finally:
            publisher.close()

    @staticmethod
    def _wait_for(predicate) -> None:
        deadline = time.monotonic() + 1.0
        while not predicate() and time.monotonic() < deadline:
            time.sleep(0.01)
        if not predicate():
            raise AssertionError("condition was not reached before the deadline")


if __name__ == "__main__":
    unittest.main()

"""Shared mDNS test doubles for discovery and lifecycle tests."""

from __future__ import annotations


class FakeResponder:
    """In-memory mDNS responder recording registered, unregistered, and closed state."""

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


__all__ = ["FakeResponder"]

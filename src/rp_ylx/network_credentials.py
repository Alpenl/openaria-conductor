"""Short-lived in-memory credentials for the privileged network controller."""

from __future__ import annotations

import re
import secrets
import threading
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass

CREDENTIAL_REF_PATTERN = re.compile(r"^cred-[A-Za-z0-9_.:-]+$")


class NetworkCredentialError(RuntimeError):
    def __init__(self, code: str, message: str) -> None:
        self.code = code
        self.message = message
        super().__init__(f"{code}: {message}")


@dataclass
class _Credential:
    value: bytearray
    expires_at: float
    reserved: bool = False


class NetworkCredentialReservation:
    """One in-flight use that becomes destructive only after durable acceptance."""

    def __init__(
        self,
        store: NetworkCredentialStore,
        credential_ref: str,
        credential: str,
    ) -> None:
        self._store = store
        self._credential_ref = credential_ref
        self._credential = credential
        self._committed = False
        self._closed = False

    @property
    def credential(self) -> str:
        if self._closed:
            raise RuntimeError("credential reservation is closed")
        return self._credential

    def commit(self) -> None:
        if self._closed:
            raise RuntimeError("credential reservation is closed")
        if not self._committed:
            self._store._commit_reservation(self._credential_ref)
            self._committed = True

    def close(self) -> None:
        if self._closed:
            return
        if not self._committed:
            self._store._release_reservation(self._credential_ref)
        self._credential = ""
        self._closed = True


def _wipe(value: bytearray) -> None:
    for index in range(len(value)):
        value[index] = 0


class NetworkCredentialStore:
    """Own opaque personal Wi-Fi references without writing credential material to disk."""

    def __init__(
        self,
        *,
        ttl_seconds: float = 60.0,
        max_entries: int = 128,
        clock: Callable[[], float] = time.monotonic,
        token_factory: Callable[[], str] | None = None,
    ) -> None:
        if (
            isinstance(ttl_seconds, bool)
            or not isinstance(ttl_seconds, (int, float))
            or int(ttl_seconds) != ttl_seconds
            or not 1 <= ttl_seconds <= 120
            or type(max_entries) is not int
            or max_entries <= 0
        ):
            raise ValueError("credential limits are invalid")
        self._ttl_seconds = int(ttl_seconds)
        self._max_entries = max_entries
        self._clock = clock
        self._token_factory = token_factory or (lambda: secrets.token_urlsafe(24))
        self._entries: dict[str, _Credential] = {}
        self._lock = threading.Lock()

    @property
    def ttl_seconds(self) -> int:
        return self._ttl_seconds

    def _purge_expired_locked(self, now: float) -> None:
        for credential_ref, entry in list(self._entries.items()):
            if entry.expires_at <= now and not entry.reserved:
                _wipe(entry.value)
                del self._entries[credential_ref]

    def create(self, credential: str) -> str:
        if (
            not isinstance(credential, str)
            or not 8 <= len(credential.encode("utf-8")) <= 63
            or any(ord(character) < 32 for character in credential)
        ):
            raise NetworkCredentialError(
                "credential_invalid",
                "Wi-Fi credential must contain 8 to 63 bytes",
            )
        encoded = bytearray(credential.encode("utf-8"))
        with self._lock:
            now = self._clock()
            self._purge_expired_locked(now)
            if len(self._entries) >= self._max_entries:
                _wipe(encoded)
                raise NetworkCredentialError(
                    "credential_store_full",
                    "credential store is temporarily full",
                )
            for _ in range(8):
                credential_ref = f"cred-{self._token_factory()}"
                if (
                    len(credential_ref) <= 128
                    and CREDENTIAL_REF_PATTERN.fullmatch(credential_ref) is not None
                    and credential_ref not in self._entries
                ):
                    self._entries[credential_ref] = _Credential(
                        value=encoded,
                        expires_at=now + self._ttl_seconds,
                    )
                    return credential_ref
        _wipe(encoded)
        raise NetworkCredentialError(
            "credential_reference_failed",
            "could not allocate an opaque credential reference",
        )

    def _validated_ref(self, credential_ref: str) -> None:
        if (
            not isinstance(credential_ref, str)
            or CREDENTIAL_REF_PATTERN.fullmatch(credential_ref) is None
        ):
            raise NetworkCredentialError(
                "credential_ref_invalid",
                "credential reference is invalid or already consumed",
            )

    @contextmanager
    def reserve(self, credential_ref: str) -> Iterator[NetworkCredentialReservation]:
        self._validated_ref(credential_ref)
        with self._lock:
            entry = self._entries.get(credential_ref)
            now = self._clock()
            if entry is None or entry.reserved:
                raise NetworkCredentialError(
                    "credential_ref_invalid",
                    "credential reference is invalid or already consumed",
                )
            if entry.expires_at <= now:
                del self._entries[credential_ref]
                _wipe(entry.value)
                raise NetworkCredentialError(
                    "credential_ref_expired",
                    "credential reference has expired",
                )
            entry.reserved = True
            credential = entry.value.decode("utf-8")
        reservation = NetworkCredentialReservation(self, credential_ref, credential)
        try:
            yield reservation
        finally:
            reservation.close()

    def _commit_reservation(self, credential_ref: str) -> None:
        with self._lock:
            entry = self._entries.get(credential_ref)
            if entry is None or not entry.reserved:
                raise RuntimeError("credential reservation is not active")
            del self._entries[credential_ref]
            _wipe(entry.value)

    def _release_reservation(self, credential_ref: str) -> None:
        with self._lock:
            entry = self._entries.get(credential_ref)
            if entry is None or not entry.reserved:
                return
            if entry.expires_at <= self._clock():
                del self._entries[credential_ref]
                _wipe(entry.value)
            else:
                entry.reserved = False

    @contextmanager
    def consume(self, credential_ref: str) -> Iterator[str]:
        with self.reserve(credential_ref) as reservation:
            credential = reservation.credential
            reservation.commit()
            yield credential

    def discard(self, credential_ref: str) -> None:
        with self._lock:
            entry = self._entries.pop(credential_ref, None)
        if entry is not None:
            _wipe(entry.value)

    def clear(self) -> None:
        with self._lock:
            entries = list(self._entries.values())
            self._entries.clear()
        for entry in entries:
            _wipe(entry.value)

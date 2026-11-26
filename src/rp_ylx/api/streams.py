"""SSE replay buffers, bounded stream admission, and shared network SSE polling."""

from __future__ import annotations

import json
import select
import socket
import threading
import time
import uuid
from collections import Counter, OrderedDict, deque
from collections.abc import Callable, Mapping
from contextlib import suppress
from copy import deepcopy
from dataclasses import dataclass
from threading import RLock

from rp_ylx.api.compat import facade_attr
from rp_ylx.api.validators.events import (
    InvalidSourceEvent,
    _event_schema_version,
    _validate_source_event,
    project_source_event,
)
from rp_ylx.api.validators.network import valid_network_status


class InvalidEventCursor(ValueError):
    """Last-Event-ID 不是契约要求的十进制 delivery ID。"""


class UnsupportedEventVersion(ValueError):
    """事件不能无损投影到请求的 API 版本。"""


_NEED_RESYNC = object()
_RESYNC_ANCHOR_LIMIT = 256
_RETIRED_AUTHORITY_EPOCH_LIMIT = 64


@dataclass(frozen=True, slots=True)
class SseEvent:
    delivery_id: int
    api_version: str
    source_event: Mapping[str, object]

    def encode(self) -> bytes:
        delivery_id = str(self.delivery_id)
        payload = {
            "schema": f"ylx.capture-event.{self.api_version}",
            "sse_delivery_id": delivery_id,
            **self.source_event,
        }
        data = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
        return (f"id: {delivery_id}\nevent: {self.source_event['type']}\ndata: {data}\n\n").encode()


@dataclass(frozen=True, slots=True)
class _BufferedEvent:
    delivery_id: int
    source_event: Mapping[str, object]
    publish_index: int = 0


@dataclass(frozen=True, slots=True)
class _ResyncAnchor:
    delivery_id: int
    next_publish_index: int
    source_event: Mapping[str, object]


class EventReplayBuffer:
    """为 daemon 源事件分配独立 delivery ID，并保留有限重放窗口。"""

    def __init__(self, capacity: int = 128) -> None:
        if capacity < 1:
            raise ValueError("capacity 必须大于零")
        self._capacity = capacity
        self._events: deque[_BufferedEvent] = deque(maxlen=capacity)
        self._resync_anchors: OrderedDict[int, _ResyncAnchor] = OrderedDict()
        self._next_delivery_id = 1
        self._publish_count = 0
        self._lock = threading.RLock()
        self._changed = threading.Condition(self._lock)

    def publish(self, source_event: Mapping[str, object]) -> str:
        copied_event = deepcopy(dict(source_event))
        _validate_source_event(copied_event)
        with self._changed:
            event = _BufferedEvent(self._next_delivery_id, copied_event, self._publish_count)
            self._next_delivery_id += 1
            self._publish_count += 1
            self._events.append(event)
            self._changed.notify_all()
            return str(event.delivery_id)

    def replay(
        self,
        last_event_id: str | None,
        *,
        api_version: str,
        snapshot: Callable[[], Mapping[str, object]],
    ) -> tuple[SseEvent, ...]:
        version = _event_schema_version(api_version)
        cursor = _parse_cursor(last_event_id)
        if cursor is None:
            return (self._issue_resync_snapshot(version, snapshot),)
        with self._changed:
            replayed = self._replayed_after_locked(cursor, version)
            if replayed is not _NEED_RESYNC:
                return replayed
        return (self._issue_resync_snapshot(version, snapshot),)

    def wait_after(
        self,
        delivery_id: int,
        timeout: float,
        *,
        api_version: str,
        snapshot: Callable[[], Mapping[str, object]],
        cancel_event: threading.Event | None = None,
    ) -> tuple[SseEvent, ...]:
        """等待 cursor 后的新事件；超时返回空元组供 gateway 发 heartbeat。"""
        if isinstance(delivery_id, bool) or not isinstance(delivery_id, int) or delivery_id < 0:
            raise InvalidEventCursor("delivery_id 必须是非负十进制整数")
        if timeout < 0:
            raise ValueError("timeout 不能小于零")
        version = _event_schema_version(api_version)
        deadline = time.monotonic() + timeout
        need_resync = False
        with self._changed:
            while True:
                if cancel_event is not None and cancel_event.is_set():
                    return ()
                try:
                    replayed = self._replayed_after_locked(delivery_id, version)
                except UnsupportedEventVersion:
                    # Live v2 streams must not disconnect into a 500 reconnect loop on safe_swap;
                    # issue a fresh v2-compatible snapshot advancing the client's delivery cursor.
                    need_resync = True
                    break
                if replayed is _NEED_RESYNC:
                    need_resync = True
                    break
                if replayed:
                    return replayed
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return ()
                self._changed.wait(remaining)
        if need_resync:
            return (self._issue_resync_snapshot(version, snapshot),)
        return ()

    def wake_waiters(self) -> None:
        """Wake blocked SSE readers so they can observe external cancellation."""
        with self._changed:
            self._changed.notify_all()

    def _issue_resync_snapshot(
        self,
        api_version: str,
        snapshot: Callable[[], Mapping[str, object]],
    ) -> SseEvent:
        """Fetch and validate a per-client resync snapshot outside the buffer lock."""
        copied_event: dict[str, object]
        publish_count_before = 0
        for _ in range(3):
            with self._changed:
                publish_count_before = self._publish_count
            raw_snapshot = snapshot()
            copied_event = deepcopy(dict(raw_snapshot))
            _validate_source_event(copied_event)
            if copied_event["type"] != "snapshot":
                raise InvalidSourceEvent("重同步 callback 必须返回 snapshot source event")
            with self._changed:
                if self._publish_count != publish_count_before:
                    continue
                return self._allocate_resync_event_locked(
                    copied_event,
                    api_version,
                    next_publish_index=publish_count_before,
                )
        with self._changed:
            return self._allocate_resync_event_locked(
                copied_event,
                api_version,
                next_publish_index=publish_count_before,
            )

    def _allocate_resync_event_locked(
        self,
        copied_event: Mapping[str, object],
        api_version: str,
        *,
        next_publish_index: int | None = None,
    ) -> SseEvent:
        delivery_id = self._next_delivery_id
        self._next_delivery_id += 1
        anchor_index = self._publish_count if next_publish_index is None else next_publish_index
        self._resync_anchors[delivery_id] = _ResyncAnchor(
            delivery_id=delivery_id,
            next_publish_index=anchor_index,
            source_event=copied_event,
        )
        while len(self._resync_anchors) > _RESYNC_ANCHOR_LIMIT:
            self._resync_anchors.popitem(last=False)
        return _for_version_source(delivery_id, copied_event, api_version)

    def _replayed_after_locked(
        self,
        cursor: int,
        api_version: str,
    ) -> tuple[SseEvent, ...] | object:
        buffered = tuple(self._events)
        anchor = self._resync_anchors.get(cursor)
        if anchor is not None:
            if not buffered:
                if anchor.next_publish_index == self._publish_count:
                    return ()
                return _NEED_RESYNC
            oldest_index = buffered[0].publish_index
            if (
                anchor.next_publish_index < oldest_index
                or anchor.next_publish_index > self._publish_count
            ):
                return _NEED_RESYNC
            start = anchor.next_publish_index - oldest_index
            replayed = buffered[start:]
            if not replayed:
                return ()
            if not _source_pair_is_contiguous(
                anchor.source_event, replayed[0].source_event
            ) or not _source_is_contiguous(replayed):
                return _NEED_RESYNC
            return tuple(_for_version(event, api_version) for event in replayed)

        if not buffered or cursor < buffered[0].delivery_id or cursor > buffered[-1].delivery_id:
            return _NEED_RESYNC

        cursor_index = _find_delivery_index(buffered, cursor)
        if cursor_index is None:
            return _NEED_RESYNC

        replayed = buffered[cursor_index + 1 :]
        if not replayed:
            return ()
        if not _source_is_contiguous(buffered[cursor_index:]):
            return _NEED_RESYNC
        return tuple(_for_version(event, api_version) for event in replayed)


def _find_delivery_index(events: tuple[_BufferedEvent, ...], cursor: int) -> int | None:
    low = 0
    high = len(events) - 1
    while low <= high:
        mid = (low + high) // 2
        candidate = events[mid].delivery_id
        if candidate == cursor:
            return mid
        if candidate < cursor:
            low = mid + 1
        else:
            high = mid - 1
    return None


def heartbeat_comment() -> bytes:
    """返回不携带 id、也不占用 delivery sequence 的 SSE heartbeat。"""
    return b": heartbeat\n\n"


def _parse_cursor(last_event_id: str | None) -> int | None:
    if last_event_id is None:
        return None
    if not last_event_id or not last_event_id.isascii() or not last_event_id.isdecimal():
        raise InvalidEventCursor("Last-Event-ID 必须是十进制 delivery ID")
    try:
        return int(last_event_id)
    except ValueError as error:
        raise InvalidEventCursor("Last-Event-ID 超出十进制 delivery ID 范围") from error


def _for_version(event: _BufferedEvent, api_version: str) -> SseEvent:
    return _for_version_source(event.delivery_id, event.source_event, api_version)


def _for_version_source(
    delivery_id: int, source_event: Mapping[str, object], api_version: str
) -> SseEvent:
    if api_version == "v2" and source_event["type"] == "safe_swap":
        raise UnsupportedEventVersion("v3 safe-swap event 不能投影为 v2")
    return SseEvent(
        delivery_id,
        api_version,
        project_source_event(source_event, api_version=api_version),
    )


def _source_pair_is_contiguous(
    previous_source: Mapping[str, object], current_source: Mapping[str, object]
) -> bool:
    if current_source.get("authority_epoch") != previous_source.get("authority_epoch"):
        return False
    current_revision = current_source.get("source_revision")
    previous_revision = previous_source.get("source_revision")
    if (
        current_source.get("type") == "progress"
        and current_revision == previous_revision
        and current_source.get("session_id") == previous_source.get("session_id")
    ):
        return True
    return current_revision == _next_revision(previous_revision)


def _source_is_contiguous(events: tuple[_BufferedEvent, ...]) -> bool:
    for previous, current in zip(events, events[1:], strict=False):
        if not _source_pair_is_contiguous(previous.source_event, current.source_event):
            return False
    return True


def _next_revision(revision: object) -> int | None:
    if isinstance(revision, int) and not isinstance(revision, bool):
        return revision + 1
    return None


@dataclass(frozen=True, slots=True)
class _NetworkSseEvent:
    delivery_id: int
    source_event: Mapping[str, object]

    def encode(self) -> bytes:
        delivery_id = str(self.delivery_id)
        payload = {
            "schema": "ylx.network-event.v1",
            "sse_delivery_id": delivery_id,
            **self.source_event,
        }
        data = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
        return (f"id: {delivery_id}\nevent: {self.source_event['type']}\ndata: {data}\n\n").encode()


@dataclass(frozen=True, slots=True)
class _BufferedNetworkEvent:
    event: _NetworkSseEvent
    publish_index: int = 0


@dataclass(frozen=True, slots=True)
class _NetworkResyncAnchor:
    delivery_id: int
    next_publish_index: int


class _NetworkEventReplayBuffer:
    """Assign server-wide delivery IDs and retain bounded network SSE replay."""

    def __init__(self, capacity: int = 128, *, initial_delivery_id: int | None = None) -> None:
        if capacity < 1:
            raise ValueError("capacity 必须大于零")
        if initial_delivery_id is not None and (
            type(initial_delivery_id) is not int or initial_delivery_id < 1
        ):
            raise ValueError("initial_delivery_id 必须是正整数")
        self._events: deque[_BufferedNetworkEvent] = deque(maxlen=capacity)
        self._resync_anchors: OrderedDict[int, _NetworkResyncAnchor] = OrderedDict()
        self._next_delivery_id = max(1, initial_delivery_id or uuid.uuid4().int)
        self._publish_count = 0
        self._last_status: dict[str, object] | None = None
        self._retired_authority_epochs: OrderedDict[str, None] = OrderedDict()
        self._lock = RLock()
        self._condition = threading.Condition(self._lock)

    def replay(
        self, cursor: int | None, status: Mapping[str, object]
    ) -> tuple[_NetworkSseEvent, ...]:
        with self._condition:
            if cursor is None:
                return (self._issue_resync_snapshot_locked(status),)
            replayed = self._replayed_after_locked(cursor)
            if replayed is _NEED_RESYNC:
                return (self._issue_resync_snapshot_locked(status, after_cursor=cursor),)
            self._observe_locked(status)
            replayed = self._replayed_after_locked(cursor)
            if replayed is _NEED_RESYNC:
                return (self._issue_resync_snapshot_locked(status, after_cursor=cursor),)
            assert isinstance(replayed, tuple)
            return replayed

    def observe(self, status: Mapping[str, object]) -> None:
        with self._condition:
            before = len(self._events)
            last_id = self._events[-1].event.delivery_id if before else None
            self._observe_locked(status)
            after_id = self._events[-1].event.delivery_id if self._events else None
            if after_id != last_id:
                self._condition.notify_all()

    def wake_waiters(self) -> None:
        with self._condition:
            self._condition.notify_all()

    def events_after(
        self, cursor: int, status: Mapping[str, object]
    ) -> tuple[_NetworkSseEvent, ...]:
        with self._condition:
            replayed = self._replayed_after_locked(cursor)
            if replayed is _NEED_RESYNC:
                return (self._issue_resync_snapshot_locked(status, after_cursor=cursor),)
            self._observe_locked(status)
            replayed = self._replayed_after_locked(cursor)
            if replayed is _NEED_RESYNC:
                return (self._issue_resync_snapshot_locked(status, after_cursor=cursor),)
            assert isinstance(replayed, tuple)
            return replayed

    def wait_after(
        self,
        cursor: int,
        timeout: float,
        *,
        status_provider: Callable[[], Mapping[str, object]],
        cancel_event: threading.Event | None = None,
    ) -> tuple[_NetworkSseEvent, ...]:
        """Wait for buffered network events after cursor without re-polling per client."""
        deadline = time.monotonic() + timeout
        need_resync = False
        cached_status: dict[str, object] | None = None
        with self._condition:
            while True:
                if cancel_event is not None and cancel_event.is_set():
                    return ()
                replayed = self._replayed_after_locked(cursor)
                if replayed is _NEED_RESYNC:
                    need_resync = True
                    if self._last_status is not None:
                        cached_status = deepcopy(self._last_status)
                    break
                if replayed:
                    assert isinstance(replayed, tuple)
                    return replayed
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return ()
                self._condition.wait(remaining)
        if need_resync:
            current_status = cached_status if cached_status is not None else status_provider()
            if cancel_event is not None and cancel_event.is_set():
                return ()
            with self._condition:
                return (self._issue_resync_snapshot_locked(current_status, after_cursor=cursor),)
        return ()

    def _replayed_after_locked(self, cursor: int) -> tuple[_NetworkSseEvent, ...] | object:
        buffered = tuple(self._events)
        anchor = self._resync_anchors.get(cursor)
        if anchor is not None:
            if not buffered:
                if anchor.next_publish_index == self._publish_count:
                    return ()
                return _NEED_RESYNC
            oldest_index = buffered[0].publish_index
            if (
                anchor.next_publish_index < oldest_index
                or anchor.next_publish_index > self._publish_count
            ):
                return _NEED_RESYNC
            start = anchor.next_publish_index - oldest_index
            return tuple(entry.event for entry in buffered[start:])

        if (
            not buffered
            or cursor < buffered[0].event.delivery_id
            or cursor > buffered[-1].event.delivery_id
        ):
            return _NEED_RESYNC

        low = 0
        high = len(buffered) - 1
        cursor_index: int | None = None
        while low <= high:
            mid = (low + high) // 2
            candidate = buffered[mid].event.delivery_id
            if candidate == cursor:
                cursor_index = mid
                break
            if candidate < cursor:
                low = mid + 1
            else:
                high = mid - 1
        if cursor_index is None:
            return _NEED_RESYNC
        return tuple(entry.event for entry in buffered[cursor_index + 1 :])

    def _observe_locked(self, status: Mapping[str, object]) -> None:
        relation = self._status_relation_locked(status)
        if relation in {"same", "stale"}:
            return
        self._publish_locked(
            status,
            force_snapshot=relation in {"new_authority", "projection"},
        )

    def _status_relation_locked(self, status: Mapping[str, object]) -> str:
        if self._last_status is None:
            return "new"
        authority_epoch = str(status["authority_epoch"])
        source_revision = int(status["source_revision"])
        last_authority_epoch = str(self._last_status["authority_epoch"])
        last_source_revision = int(self._last_status["source_revision"])
        if authority_epoch == last_authority_epoch:
            if source_revision < last_source_revision:
                return "stale"
            if source_revision > last_source_revision:
                return "new"
            projection_fields = (
                "saved",
                "verified",
                "desired",
                "observed",
                "transaction",
                "mutation_capability",
                "concurrency_capability",
            )
            if any(
                status.get(field) != self._last_status.get(field) for field in projection_fields
            ):
                return "projection"
            return "same"
        if authority_epoch in self._retired_authority_epochs:
            self._retired_authority_epochs.move_to_end(authority_epoch)
            return "stale"
        return "new_authority"

    def _issue_resync_snapshot_locked(
        self,
        status: Mapping[str, object],
        *,
        after_cursor: int | None = None,
    ) -> _NetworkSseEvent:
        if self._last_status is not None:
            self._observe_locked(status)
        source_status = self._last_status if self._last_status is not None else status
        selected_status = deepcopy(dict(source_status))
        if self._last_status is None:
            self._last_status = deepcopy(selected_status)
        source_event = {
            "authority_epoch": selected_status["authority_epoch"],
            "source_revision": selected_status["source_revision"],
            "occurred_at": selected_status["observed_at"],
            "type": "snapshot",
            "transaction_id": None,
            "data": selected_status,
        }
        # Never allow an untrusted Last-Event-ID to jump _next_delivery_id arbitrarily;
        # only advance monotonically by 1 if the current counter collides with an exact cursor.
        self._next_delivery_id = max(1, self._next_delivery_id)
        if after_cursor is not None and after_cursor == self._next_delivery_id:
            self._next_delivery_id += 1
        delivery_id = self._next_delivery_id
        self._next_delivery_id += 1
        self._resync_anchors[delivery_id] = _NetworkResyncAnchor(
            delivery_id=delivery_id,
            next_publish_index=self._publish_count,
        )
        while len(self._resync_anchors) > _RESYNC_ANCHOR_LIMIT:
            self._resync_anchors.popitem(last=False)
        return _NetworkSseEvent(delivery_id, source_event)

    def _publish_locked(
        self, status: Mapping[str, object], *, force_snapshot: bool
    ) -> _NetworkSseEvent:
        copied_status = deepcopy(dict(status))
        transaction: object = None
        if not force_snapshot:
            transactions = copied_status.get("transaction")
            if isinstance(transactions, Mapping):
                transaction = transactions.get("current") or transactions.get("latest")
            if not (
                isinstance(transaction, Mapping)
                and transaction.get("authority_epoch") == copied_status["authority_epoch"]
                and transaction.get("source_revision") == copied_status["source_revision"]
            ):
                transaction = None
        if isinstance(transaction, Mapping):
            source_event = {
                "authority_epoch": copied_status["authority_epoch"],
                "source_revision": copied_status["source_revision"],
                "occurred_at": transaction["updated_at"],
                "type": "transaction",
                "transaction_id": transaction["transaction_id"],
                "data": deepcopy(dict(transaction)),
            }
        else:
            source_event = {
                "authority_epoch": copied_status["authority_epoch"],
                "source_revision": copied_status["source_revision"],
                "occurred_at": copied_status["observed_at"],
                "type": "snapshot",
                "transaction_id": None,
                "data": copied_status,
            }
        self._next_delivery_id = max(1, self._next_delivery_id)
        event = _NetworkSseEvent(self._next_delivery_id, source_event)
        self._next_delivery_id += 1
        self._events.append(_BufferedNetworkEvent(event, self._publish_count))
        self._publish_count += 1
        if self._last_status is not None:
            previous_authority = str(self._last_status["authority_epoch"])
            current_authority = str(copied_status["authority_epoch"])
            if previous_authority != current_authority:
                self._retired_authority_epochs[previous_authority] = None
                self._retired_authority_epochs.move_to_end(previous_authority)
                while len(self._retired_authority_epochs) > _RETIRED_AUTHORITY_EPOCH_LIMIT:
                    self._retired_authority_epochs.popitem(last=False)
        self._last_status = copied_status
        return event


class _SharedNetworkPoller:
    """Run at most one network status poller while network SSE streams are connected."""

    def __init__(
        self,
        status_provider: Callable[[], Mapping[str, object]],
        buffer: _NetworkEventReplayBuffer,
        poll_seconds: float,
    ) -> None:
        self._status_provider = status_provider
        self._buffer = buffer
        self._poll_seconds = poll_seconds
        self._lock = threading.Lock()
        self._stop_event = threading.Event()
        self._active_clients = 0
        self._thread: threading.Thread | None = None
        self._last_error: Exception | None = None

    def acquire(self) -> None:
        with self._lock:
            self._active_clients += 1
            self._last_error = None
            if self._thread is None or not self._thread.is_alive() or self._stop_event.is_set():
                stop_event = threading.Event()
                self._stop_event = stop_event
                thread = threading.Thread(
                    target=self._run,
                    args=(stop_event,),
                    name="network-sse-poller",
                    daemon=True,
                )
                self._thread = thread
                thread.start()

    def release(self) -> None:
        thread: threading.Thread | None = None
        with self._lock:
            self._active_clients = max(0, self._active_clients - 1)
            if self._active_clients == 0:
                self._stop_event.set()
                thread = self._thread
        if thread is not None and thread is not threading.current_thread():
            thread.join(timeout=0.5)

    def stop(self) -> None:
        thread: threading.Thread | None = None
        with self._lock:
            self._active_clients = 0
            self._stop_event.set()
            thread = self._thread
        if thread is not None and thread is not threading.current_thread():
            thread.join(timeout=0.5)

    def check_error(self) -> None:
        with self._lock:
            error = self._last_error
        if error is not None:
            raise error

    def _run(self, stop_event: threading.Event) -> None:
        try:
            while not stop_event.is_set():
                with self._lock:
                    if self._active_clients <= 0:
                        return
                try:
                    status = self._status_provider()
                    if not valid_network_status(status):
                        raise InvalidSourceEvent("daemon network 状态无效")
                    self._buffer.observe(status)
                except Exception as error:
                    with self._lock:
                        self._last_error = error
                    self._buffer.wake_waiters()
                    return
                if stop_event.wait(self._poll_seconds):
                    return
        finally:
            with self._lock:
                if self._thread is threading.current_thread():
                    self._thread = None


class _StreamSession:
    """Own one long-lived HTTP stream from admission through teardown."""

    _POLL_INTERVAL_MS = 50

    def __init__(
        self,
        registry: _StreamRegistry,
        session_id: int,
        kind: str,
        connection: socket.socket,
        slot: threading.BoundedSemaphore,
        wake_waiters: Callable[[], None],
    ) -> None:
        self.session_id = session_id
        self.kind = kind
        self.started_at = time.monotonic()
        self.cancelled = threading.Event()
        self._registry = registry
        self._connection = connection
        self._slot: threading.BoundedSemaphore | None = slot
        self._wake_waiters = wake_waiters
        self._lock = threading.Lock()
        self._body: object | None = None
        self._close_reason: str | None = None
        self._finished = False
        self._watcher = threading.Thread(
            target=self._watch_peer,
            name=f"gateway-{kind}-peer-{session_id}",
            daemon=True,
        )
        self._watcher.start()

    def attach_body(self, body: object) -> bool:
        close: Callable[[], object] | None = None
        with self._lock:
            if self._close_reason is None:
                self._body = body
                return True
            candidate = getattr(body, "close", None)
            if callable(candidate):
                close = candidate
        if close is not None:
            with suppress(Exception):
                close()
        return False

    def cancel(self, reason: str, *, interrupt_socket: bool = False) -> bool:
        close: Callable[[], object] | None = None
        with self._lock:
            if self._close_reason is not None:
                return False
            self._close_reason = reason
            self.cancelled.set()
            candidate = getattr(self._body, "close", None)
            if callable(candidate):
                close = candidate
            self._body = None

        with suppress(Exception):
            self._wake_waiters()
        if close is not None:
            with suppress(Exception):
                close()
        if interrupt_socket:
            with suppress(OSError):
                self._connection.shutdown(socket.SHUT_RDWR)
        self._registry._cancelled(self, reason)
        return True

    def finish(self, reason: str) -> None:
        self.cancel(reason)
        slot: threading.BoundedSemaphore | None = None
        with self._lock:
            if self._finished:
                return
            self._finished = True
            slot = self._slot
            self._slot = None
        if self._watcher is not threading.current_thread():
            self._watcher.join(timeout=0.25)
        if slot is not None:
            slot.release()
        self._registry._finished(self)

    def _watch_peer(self) -> None:
        poller = select.poll()
        disconnect_mask = select.POLLERR | select.POLLHUP | select.POLLNVAL
        disconnect_mask |= getattr(select, "POLLRDHUP", 0)
        try:
            poller.register(self._connection.fileno(), disconnect_mask)
        except (OSError, ValueError):
            self.cancel("socket_invalid", interrupt_socket=True)
            return
        while not self.cancelled.is_set():
            try:
                events = poller.poll(self._POLL_INTERVAL_MS)
            except (OSError, ValueError):
                self.cancel("socket_error", interrupt_socket=True)
                return
            if any(mask & disconnect_mask for _, mask in events):
                self.cancel("peer_disconnect", interrupt_socket=True)
                return


class _StreamRegistry:
    """Admit, observe, and cancel all gateway stream sessions."""

    _ADMISSION_TIMEOUT_SECONDS = 0.2

    def __init__(self, *, max_sse: int, max_preview: int) -> None:
        self._limits = {"sse": max_sse, "preview": max_preview}
        self._slots = {
            kind: threading.BoundedSemaphore(limit) for kind, limit in self._limits.items()
        }
        self._condition = threading.Condition()
        self._active: dict[int, _StreamSession] = {}
        self._next_id = 1
        self._rejected: Counter[str] = Counter()
        self._close_reasons: dict[str, Counter[str]] = {
            "sse": Counter(),
            "preview": Counter(),
        }

    def begin(
        self,
        kind: str,
        connection: socket.socket,
        wake_waiters: Callable[[], None],
        *,
        session_cls: type[_StreamSession] | None = None,
    ) -> _StreamSession | None:
        slot = self._slots[kind]
        if not slot.acquire(timeout=self._ADMISSION_TIMEOUT_SECONDS):
            with self._condition:
                self._rejected[kind] += 1
            return None
        resolved_session_cls = (
            facade_attr("rp_ylx.api.gateway", "_StreamSession", _StreamSession)
            if session_cls is None
            else session_cls
        )
        with self._condition:
            session_id = self._next_id
            self._next_id += 1
            try:
                session = resolved_session_cls(
                    self,
                    session_id,
                    kind,
                    connection,
                    slot,
                    wake_waiters,
                )
            except RuntimeError:
                # Thread creation can fail transiently. No session owns the
                # admission slot yet, so return it and use the existing 503.
                slot.release()
                self._rejected[kind] += 1
                return None
            except BaseException:
                slot.release()
                raise
            self._active[session_id] = session
            return session

    def cancel_all(self, reason: str) -> None:
        with self._condition:
            sessions = tuple(self._active.values())
        for session in sessions:
            session.cancel(reason, interrupt_socket=True)

    def wait_empty(self, timeout: float) -> bool:
        deadline = time.monotonic() + timeout
        with self._condition:
            while self._active:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return False
                self._condition.wait(remaining)
            return True

    def snapshot(self) -> Mapping[str, object]:
        now = time.monotonic()
        with self._condition:
            result: dict[str, object] = {}
            for kind, limit in self._limits.items():
                sessions = [session for session in self._active.values() if session.kind == kind]
                ages = [max(0.0, now - session.started_at) for session in sessions]
                result[kind] = {
                    "capacity": limit,
                    "active": len(sessions),
                    "canceling": sum(session.cancelled.is_set() for session in sessions),
                    "rejected": self._rejected[kind],
                    "oldest_age_seconds": max(ages, default=0.0),
                    "close_reasons": dict(self._close_reasons[kind]),
                }
            return result

    def _cancelled(self, session: _StreamSession, reason: str) -> None:
        with self._condition:
            self._close_reasons[session.kind][reason] += 1

    def _finished(self, session: _StreamSession) -> None:
        with self._condition:
            self._active.pop(session.session_id, None)
            self._condition.notify_all()

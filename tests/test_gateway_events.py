from __future__ import annotations

import http.client
import json
import threading
import unittest
from copy import deepcopy
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from rp_ylx.api.events import EventReplayBuffer, InvalidSourceEvent
from rp_ylx.api.gateway import (
    CaptureCommand,
    CaptureCommandResult,
    create_gateway_server,
)
from rp_ylx.api.security import Principal, SecurityPolicy

AUTHORITY_EPOCH = "4fa85f64-5717-4562-b3fc-2c963f66afa6"
NEXT_AUTHORITY_EPOCH = "123e4567-e89b-42d3-a456-426614174000"
SESSION_ID = "01989f6a-2c00-7a1b-8c2d-3e4f50617283"

SNAPSHOT_SOURCE_EVENT = {
    "authority_epoch": AUTHORITY_EPOCH,
    "source_revision": 45,
    "type": "snapshot",
    "occurred_at": "2026-08-08T02:25:01Z",
    "session_id": None,
    "data": {
        "schema": "ylx.capture-snapshot-event.v2",
        "device_state": "idle",
        "active_recording": None,
        "retained_unsuccessful": None,
        "runtime": {
            "observed_at": "2026-08-08T10:25:01+08:00",
            "connection_method": "ethernet_lan",
            "temperature_celsius": 52.0,
            "network": {
                "ap": {
                    "state": "active",
                    "interface": "wlan0",
                    "addresses": ["10.42.0.1/24"],
                    "peer_or_ssid": "YLX-30D5872D",
                },
                "wifi_client": {
                    "state": "disconnected",
                    "interface": "wlan1",
                    "addresses": [],
                    "peer_or_ssid": None,
                },
                "wired": {
                    "state": "connected",
                    "interface": "eth0",
                    "addresses": ["192.0.2.24/24"],
                    "peer_or_ssid": None,
                },
                "default_route": "wired",
            },
            "live_imu": None,
        },
    },
}

STATE_SOURCE_EVENT = {
    "authority_epoch": AUTHORITY_EPOCH,
    "source_revision": 46,
    "type": "state",
    "occurred_at": "2026-08-08T02:25:02Z",
    "session_id": SESSION_ID,
    "data": {
        "schema": "ylx.capture-state-event.v2",
        "state": "recording",
        "volume_id": "6ba7b810-9dad-41d1-80b4-00c04fd430c8",
        "generation_id": "7d516b70-d8ab-47d1-b2dc-5b1250138789",
    },
}

PROGRESS_SOURCE_EVENT = {
    "authority_epoch": AUTHORITY_EPOCH,
    "source_revision": 47,
    "type": "progress",
    "occurred_at": "2026-08-08T02:25:03Z",
    "session_id": SESSION_ID,
    "data": {
        "schema": "ylx.capture-progress-event.v2",
        "phase": "verifying",
        "elapsed_seconds": 31.0,
        "completed_units": 4,
        "total_units": 6,
        "unit": "artifacts",
    },
}

DIAGNOSTIC_SOURCE_EVENT = {
    "authority_epoch": AUTHORITY_EPOCH,
    "source_revision": 48,
    "type": "diagnostic",
    "occurred_at": "2026-08-08T02:25:04Z",
    "session_id": SESSION_ID,
    "data": {
        "schema": "ylx.capture-diagnostic-event.v2",
        "diagnostic": {
            "code": "storage_slow",
            "severity": "warning",
            "message": "写入延迟超过目标值",
            "at": "2026-08-08T02:25:04Z",
            "recoverable": True,
        },
    },
}

RECORDING_STATE = {
    "schema": "ylx.recording-state.v1",
    "state": "recording",
    "authority_epoch": AUTHORITY_EPOCH,
    "state_revision": 49,
    "updated_at": "2026-08-08T02:25:05Z",
    "session_id": SESSION_ID,
    "take_id": "01989f69-f000-7c3d-ae4f-5061728394a5",
    "display_name": "第一个录制",
    "device": {
        "device_id": "550e8400-e29b-41d4-a716-446655440000",
        "device_label": "YLX-30D5872D",
    },
    "storage": {
        "volume_id": "6ba7b810-9dad-41d1-80b4-00c04fd430c8",
        "status": "mounted",
        "writable": True,
        "remaining_bytes": 1024,
    },
    "progress": {
        "elapsed_seconds": 1.5,
        "captured_frames": 45,
        "bytes_written": 4096,
    },
    "diagnostics": [],
}

SAFE_SWAP_SOURCE_EVENT_V3 = {
    "authority_epoch": AUTHORITY_EPOCH,
    "source_revision": 46,
    "type": "safe_swap",
    "occurred_at": "2026-08-08T10:25:03+08:00",
    "session_id": SESSION_ID,
    "data": {
        "schema": "ylx.safe-swap-receipt.v3",
        "session_id": SESSION_ID,
        "volume_id": "6ba7b810-9dad-41d1-80b4-00c04fd430c8",
        "generation_id": "7d516b70-d8ab-47d1-b2dc-5b1250138789",
        "manifest_id": "01989f6a-2c01-7b2c-9d3e-4f5061728394",
        "manifest_sha256": "7e53429e512f12d8b6aa2b1794b73c1790792e738df44d20e5487a69d6412815",
        "sealed_at": "2026-08-08T10:24:33+08:00",
        "released_at": "2026-08-08T10:25:03+08:00",
        "release_state": "unmounted",
        "open_handle_count": 0,
    },
}


class EventProvider:
    def __init__(self) -> None:
        self.snapshot_event = deepcopy(SNAPSHOT_SOURCE_EVENT)

    def capture_snapshot_event(self) -> dict[str, object]:
        return deepcopy(self.snapshot_event)

    def device_descriptor(self, api_version: str, security_profile: str) -> dict[str, object]:
        raise AssertionError("事件测试不应读取设备描述")

    def capture_status(self) -> dict[str, object]:
        raise AssertionError("SSE snapshot 必须来自完整 source event")

    def start_capture(self, command: CaptureCommand) -> CaptureCommandResult:
        raise AssertionError("事件测试不应启动录制")

    def stop_capture(self, command: CaptureCommand) -> CaptureCommandResult:
        raise AssertionError("事件测试不应停止录制")


def parse_sse(payload: bytes) -> list[dict[str, object]]:
    events: list[dict[str, object]] = []
    for block in payload.decode().strip().split("\n\n"):
        if not block or block.startswith(":"):
            continue
        fields = dict(line.split(": ", 1) for line in block.splitlines())
        events.append(
            {
                "id": fields["id"],
                "event": fields["event"],
                "data": json.loads(fields["data"]),
            }
        )
    return events


def read_next_sse_event(response: http.client.HTTPResponse) -> dict[str, object]:
    while True:
        lines: list[str] = []
        while True:
            line = response.readline().decode().rstrip("\r\n")
            if not line:
                break
            lines.append(line)
        if lines and not lines[0].startswith(":"):
            fields = dict(line.split(": ", 1) for line in lines)
            return {
                "id": fields["id"],
                "event": fields["event"],
                "data": json.loads(fields["data"]),
            }


class GatewayEventHttpTest(unittest.TestCase):
    def setUp(self) -> None:
        principal = Principal("reader", permissions={"streamCaptureEvents": None})
        self.event_buffer = EventReplayBuffer(capacity=3)
        self.provider = EventProvider()
        self.server = create_gateway_server(
            "127.0.0.1",
            0,
            self.provider,
            security=SecurityPolicy.customer(tokens={"reader-token": principal}),
            event_buffer=self.event_buffer,
            sse_heartbeat_seconds=0.01,
        )
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.base = f"http://127.0.0.1:{self.server.server_port}"

    def tearDown(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=2)

    def request_events(
        self,
        api_version: str,
        *,
        last_event_id: str | None = None,
        expected_blocks: int = 1,
    ) -> tuple[int, bytes, object]:
        headers = {"Authorization": "Bearer reader-token"}
        if last_event_id is not None:
            headers["Last-Event-ID"] = last_event_id
        connection = http.client.HTTPConnection("127.0.0.1", self.server.server_port, timeout=2)
        try:
            connection.request("GET", f"/api/{api_version}/capture/events", headers=headers)
            response = connection.getresponse()
            if response.status != 200:
                return response.status, response.read(), response.headers
            payload = bytearray()
            completed_blocks = 0
            while completed_blocks < expected_blocks:
                line = response.readline()
                if not line:
                    self.fail("SSE 在收到预期事件前关闭")
                payload.extend(line)
                if line in {b"\n", b"\r\n"}:
                    completed_blocks += 1
            return response.status, bytes(payload), response.headers
        finally:
            connection.close()

    def test_first_connection_gets_current_source_snapshot_in_each_api_envelope(self) -> None:
        for expected_id, version in (("1", "v2"), ("2", "v3")):
            with self.subTest(version=version):
                status, payload, headers = self.request_events(version)

                self.assertEqual(status, 200)
                self.assertEqual(headers["Content-Type"], "text/event-stream")
                self.assertEqual(headers["Cache-Control"], "no-cache")
                self.assertEqual(headers["X-Accel-Buffering"], "no")
                self.assertEqual(
                    parse_sse(payload),
                    [
                        {
                            "id": expected_id,
                            "event": "snapshot",
                            "data": {
                                "schema": f"ylx.capture-event.{version}",
                                "sse_delivery_id": expected_id,
                                **SNAPSHOT_SOURCE_EVENT,
                            },
                        }
                    ],
                )

    def test_reconnect_with_buffered_cursor_replays_only_later_source_events(self) -> None:
        _, initial_payload, _ = self.request_events("v3")
        initial_id = str(parse_sse(initial_payload)[0]["id"])
        state_id = self.event_buffer.publish(STATE_SOURCE_EVENT)
        progress_id = self.event_buffer.publish(PROGRESS_SOURCE_EVENT)

        status, payload, _ = self.request_events("v3", last_event_id=initial_id, expected_blocks=2)

        self.assertEqual(status, 200)
        self.assertEqual(
            parse_sse(payload),
            [
                {
                    "id": state_id,
                    "event": "state",
                    "data": {
                        "schema": "ylx.capture-event.v3",
                        "sse_delivery_id": state_id,
                        **STATE_SOURCE_EVENT,
                    },
                },
                {
                    "id": progress_id,
                    "event": "progress",
                    "data": {
                        "schema": "ylx.capture-event.v3",
                        "sse_delivery_id": progress_id,
                        **PROGRESS_SOURCE_EVENT,
                    },
                },
            ],
        )

    def test_reconnect_resynchronizes_from_current_snapshot_at_replay_boundaries(self) -> None:
        _, initial_payload, _ = self.request_events("v3")
        initial_id = str(parse_sse(initial_payload)[0]["id"])
        self.event_buffer.publish(STATE_SOURCE_EVENT)
        self.event_buffer.publish(PROGRESS_SOURCE_EVENT)
        latest_id = self.event_buffer.publish({**PROGRESS_SOURCE_EVENT, "source_revision": 48})

        current_snapshot = {
            **SNAPSHOT_SOURCE_EVENT,
            "source_revision": 80,
            "occurred_at": "2026-08-08T02:26:00Z",
        }
        self.provider.snapshot_event = current_snapshot

        for cursor in (initial_id, str(int(latest_id) + 100)):
            with self.subTest(cursor=cursor):
                status, payload, _ = self.request_events("v3", last_event_id=cursor)
                events = parse_sse(payload)

                self.assertEqual(status, 200)
                self.assertEqual(len(events), 1)
                self.assertEqual(events[0]["event"], "snapshot")
                self.assertEqual(
                    events[0]["data"],
                    {
                        "schema": "ylx.capture-event.v3",
                        "sse_delivery_id": events[0]["id"],
                        **current_snapshot,
                    },
                )

    def test_reconnect_resynchronizes_on_source_revision_gap_and_authority_change(self) -> None:
        _, initial_payload, _ = self.request_events("v3")
        initial_id = str(parse_sse(initial_payload)[0]["id"])
        self.event_buffer.publish({**STATE_SOURCE_EVENT, "source_revision": 47})
        gap_snapshot = {
            **SNAPSHOT_SOURCE_EVENT,
            "source_revision": 47,
            "occurred_at": "2026-08-08T02:25:04Z",
        }
        self.provider.snapshot_event = gap_snapshot

        _, gap_payload, _ = self.request_events("v3", last_event_id=initial_id)
        gap_event = parse_sse(gap_payload)[0]

        self.assertEqual(gap_event["event"], "snapshot")
        self.assertEqual(
            gap_event["data"],
            {
                "schema": "ylx.capture-event.v3",
                "sse_delivery_id": gap_event["id"],
                **gap_snapshot,
            },
        )

        self.event_buffer.publish(
            {
                **STATE_SOURCE_EVENT,
                "authority_epoch": NEXT_AUTHORITY_EPOCH,
                "source_revision": 1,
            }
        )
        epoch_snapshot = {
            **SNAPSHOT_SOURCE_EVENT,
            "authority_epoch": NEXT_AUTHORITY_EPOCH,
            "source_revision": 1,
            "occurred_at": "2026-08-08T02:25:05Z",
        }
        self.provider.snapshot_event = epoch_snapshot

        _, epoch_payload, _ = self.request_events("v3", last_event_id=str(gap_event["id"]))
        epoch_event = parse_sse(epoch_payload)[0]

        self.assertEqual(epoch_event["event"], "snapshot")
        self.assertEqual(
            epoch_event["data"],
            {
                "schema": "ylx.capture-event.v3",
                "sse_delivery_id": epoch_event["id"],
                **epoch_snapshot,
            },
        )

    def test_live_connection_receives_events_published_after_it_opens(self) -> None:
        request = Request(
            f"{self.base}/api/v3/capture/events",
            headers={"Authorization": "Bearer reader-token"},
        )
        with urlopen(request, timeout=2) as response:
            initial = read_next_sse_event(response)
            state_id = self.event_buffer.publish(STATE_SOURCE_EVENT)
            published = read_next_sse_event(response)

        self.assertEqual(response.status, 200)
        self.assertEqual(state_id, str(int(str(initial["id"])) + 1))
        self.assertEqual(
            published,
            {
                "id": state_id,
                "event": "state",
                "data": {
                    "schema": "ylx.capture-event.v3",
                    "sse_delivery_id": state_id,
                    **STATE_SOURCE_EVENT,
                },
            },
        )

    def test_source_event_cannot_override_gateway_delivery_identity(self) -> None:
        self.provider.snapshot_event = {
            **SNAPSHOT_SOURCE_EVENT,
            "schema": "attacker-controlled",
            "sse_delivery_id": "999",
        }

        status, payload, _ = self.request_events("v3")

        self.assertEqual(status, 500)
        self.assertEqual(json.loads(payload)["error"]["code"], "invalid_source_event")

    def test_snapshot_provider_must_return_a_snapshot_source_event(self) -> None:
        self.provider.snapshot_event = deepcopy(STATE_SOURCE_EVENT)

        status, payload, _ = self.request_events("v3")

        self.assertEqual(status, 500)
        self.assertEqual(json.loads(payload)["error"]["code"], "invalid_source_event")

    def test_v2_stream_fails_closed_instead_of_rewriting_new_v3_safe_swap(self) -> None:
        _, initial_payload, _ = self.request_events("v3")
        initial_id = str(parse_sse(initial_payload)[0]["id"])
        self.event_buffer.publish(SAFE_SWAP_SOURCE_EVENT_V3)

        status, payload, _ = self.request_events("v2", last_event_id=initial_id)

        self.assertEqual(status, 500)
        self.assertEqual(json.loads(payload)["error"]["code"], "event_version_unsupported")

    def test_invalid_cursor_is_rejected_and_heartbeat_does_not_advance_delivery_id(self) -> None:
        status, payload, _ = self.request_events("v3", last_event_id="source-45")
        self.assertEqual(status, 400)
        self.assertEqual(json.loads(payload)["error"]["code"], "invalid_request")

        _, first_payload, _ = self.request_events("v3")
        first = parse_sse(first_payload)[0]
        _, heartbeat_payload, _ = self.request_events("v3", last_event_id=str(first["id"]))
        state_id = self.event_buffer.publish(STATE_SOURCE_EVENT)

        self.assertEqual(heartbeat_payload, b": heartbeat\n\n")
        self.assertEqual(state_id, str(int(str(first["id"])) + 1))

        status, payload, _ = self.request_events("v3", last_event_id="9" * 10_000)
        self.assertEqual(status, 400)
        self.assertEqual(json.loads(payload)["error"]["code"], "invalid_request")

    def test_repeated_last_event_id_is_rejected(self) -> None:
        connection = http.client.HTTPConnection("127.0.0.1", self.server.server_port, timeout=2)
        try:
            connection.putrequest("GET", "/api/v3/capture/events")
            connection.putheader("Authorization", "Bearer reader-token")
            connection.putheader("Last-Event-ID", "1")
            connection.putheader("Last-Event-ID", "1")
            connection.endheaders()
            response = connection.getresponse()
            payload = response.read()
        finally:
            connection.close()

        self.assertEqual(response.status, 400)
        self.assertEqual(json.loads(payload)["error"]["code"], "invalid_request")

    def test_undeclared_sse_query_parameters_are_rejected(self) -> None:
        request = Request(
            f"{self.base}/api/v3/capture/events?once=true",
            headers={"Authorization": "Bearer reader-token"},
        )
        with self.assertRaises(HTTPError) as raised:
            urlopen(request, timeout=2)

        self.assertEqual(raised.exception.code, 400)
        self.assertEqual(
            json.loads(raised.exception.read())["error"]["code"],
            "invalid_request",
        )

    def test_new_safe_swap_publication_rejects_governance_fields_and_boolean_handle_count(
        self,
    ) -> None:
        with self.assertRaises(InvalidSourceEvent):
            self.event_buffer.publish(
                {
                    **SAFE_SWAP_SOURCE_EVENT_V3,
                    "data": {
                        **SAFE_SWAP_SOURCE_EVENT_V3["data"],
                        "handle_audit": {},
                    },
                }
            )

    def test_every_source_event_data_is_closed_before_delivery_id_assignment(self) -> None:
        buffer = EventReplayBuffer()
        for source_event in (
            SNAPSHOT_SOURCE_EVENT,
            STATE_SOURCE_EVENT,
            PROGRESS_SOURCE_EVENT,
            DIAGNOSTIC_SOURCE_EVENT,
        ):
            for invalid_data in ({}, {**source_event["data"], "unexpected": True}):
                with (
                    self.subTest(type=source_event["type"], data=invalid_data),
                    self.assertRaises(InvalidSourceEvent),
                ):
                    buffer.publish({**source_event, "data": invalid_data})

        self.assertEqual(buffer.publish(STATE_SOURCE_EVENT), "1")

        active_snapshot = deepcopy(SNAPSHOT_SOURCE_EVENT)
        active_snapshot["source_revision"] = 49
        active_snapshot["session_id"] = SESSION_ID
        active_snapshot["data"]["device_state"] = "recording"
        active_snapshot["data"]["active_recording"] = {
            "generation_id": "7d516b70-d8ab-47d1-b2dc-5b1250138789",
            "recording_state": deepcopy(RECORDING_STATE),
        }
        self.assertEqual(buffer.publish(active_snapshot), "2")
        for invalid_state in (
            {"state": "recording", "session_id": SESSION_ID},
            {**RECORDING_STATE, "unexpected": True},
            {**RECORDING_STATE, "authority_epoch": NEXT_AUTHORITY_EPOCH},
            {**RECORDING_STATE, "state_revision": 50},
        ):
            with self.subTest(recording_state=invalid_state), self.assertRaises(InvalidSourceEvent):
                invalid_snapshot = deepcopy(active_snapshot)
                invalid_snapshot["data"]["active_recording"]["recording_state"] = invalid_state
                buffer.publish(invalid_snapshot)

        with self.assertRaises(InvalidSourceEvent):
            buffer.publish({**STATE_SOURCE_EVENT, "occurred_at": "T"})
        for event_type, field in (
            ("snapshot", "device_state"),
            ("state", "state"),
            ("progress", "phase"),
            ("progress", "unit"),
            ("diagnostic", "severity"),
        ):
            source_event = deepcopy(
                {
                    "snapshot": SNAPSHOT_SOURCE_EVENT,
                    "state": STATE_SOURCE_EVENT,
                    "progress": PROGRESS_SOURCE_EVENT,
                    "diagnostic": DIAGNOSTIC_SOURCE_EVENT,
                }[event_type]
            )
            target = (
                source_event["data"]["diagnostic"]
                if event_type == "diagnostic"
                else source_event["data"]
            )
            target[field] = []
            with self.subTest(type=event_type, field=field), self.assertRaises(InvalidSourceEvent):
                buffer.publish(source_event)
        with self.assertRaises(InvalidSourceEvent):
            self.event_buffer.publish(
                {
                    **SAFE_SWAP_SOURCE_EVENT_V3,
                    "data": {
                        **SAFE_SWAP_SOURCE_EVENT_V3["data"],
                        "open_handle_count": False,
                    },
                }
            )

    def test_abandonment_authorization_identifiers_may_start_with_a_digit(self) -> None:
        for authorization in (
            {"kind": "OPERATOR", "operator_id": "7operator"},
            {
                "kind": "POLICY",
                "policy_id": "7policy",
                "policy_revision": 1,
                "policy_sha256": "a" * 64,
            },
        ):
            with self.subTest(kind=authorization["kind"]):
                recording_state = deepcopy(RECORDING_STATE)
                recording_state.update(
                    {
                        "state": "abandoned",
                        "diagnostics": [
                            {
                                "code": "operator_abandoned",
                                "severity": "error",
                                "message": "操作员放弃录制",
                                "at": "2026-08-08T02:25:05Z",
                                "recoverable": False,
                            }
                        ],
                        "abandonment": {
                            "reason_code": "operator_request",
                            "reason": "操作员确认放弃",
                            "authorized_at": "2026-08-08T02:25:05Z",
                            "authorization": authorization,
                        },
                    }
                )
                snapshot = deepcopy(SNAPSHOT_SOURCE_EVENT)
                snapshot["source_revision"] = recording_state["state_revision"]
                snapshot["data"]["retained_unsuccessful"] = {
                    "generation_id": "7d516b70-d8ab-47d1-b2dc-5b1250138789",
                    "recording_state": recording_state,
                }

                self.assertEqual(EventReplayBuffer().publish(snapshot), "1")


if __name__ == "__main__":
    unittest.main()

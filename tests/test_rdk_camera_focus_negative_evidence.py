from __future__ import annotations

import base64
import copy
import hashlib
import json
import stat
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

from scripts import rdk_camera_focus_negative_evidence as evidence

EXPECTED_COMMIT = "a" * 40
PLAINTEXT_SERIAL = "PRIVATE-SERIAL-01.00.00"
PLAINTEXT_MANUFACTURER = "YLX-260701-W"
PLAINTEXT_PRODUCT = "2UQ2"


def matching_health(commit: str = EXPECTED_COMMIT) -> dict[str, object]:
    return {
        "service": {
            "command_returncode": 0,
            "error_code": None,
            "active_state": "active",
            "sub_state": "running",
            "main_pid": "123",
            "restart_count": "0",
            "result": "success",
            "exec_main_status": "0",
        },
        "capture": {
            "http_status": 200,
            "error_code": None,
            "schema": "ylx.capture-status.v4",
            "snapshot_schema": "ylx.capture-snapshot-event.v4",
            "device_state": "idle",
            "active_recording_present": False,
            "body_stored": False,
        },
        "version": {
            "command_returncode": 0,
            "error_code": None,
            "format_valid": True,
            "version": "0.1.0",
            "commit": commit,
            "raw_output_stored": False,
        },
    }


def baseline_binding() -> evidence.BindingToken:
    return evidence.BindingToken(
        query_node="/dev/video0",
        query_node_rdev=81,
        query_node_index="0",
        video_device_target="/sys/devices/usb1/1-1/1-1:1.0",
        usb_device_target="/sys/devices/usb1/1-1",
        descriptor_sha256=evidence.EXPECTED_USB["descriptor_sha256"],
        media_node="/dev/media0",
        media_node_rdev=243,
        media_device_target="/sys/devices/usb1/1-1/1-1:1.0",
    )


def matching_identity(
    *,
    binding: evidence.BindingToken | None = None,
    private_values: tuple[str, ...] = (
        PLAINTEXT_SERIAL,
        PLAINTEXT_MANUFACTURER,
        PLAINTEXT_PRODUCT,
        "2UQ2:",
        "usb-xhci-1.2",
    ),
) -> evidence.IdentityObservation:
    binding = binding or baseline_binding()
    public = {
        "usb": {
            **evidence.EXPECTED_USB,
            "manufacturer_sha256": evidence._hash_text(PLAINTEXT_MANUFACTURER),
            "product_sha256": evidence._hash_text(PLAINTEXT_PRODUCT),
            "serial_sha256": evidence._hash_text(PLAINTEXT_SERIAL),
        },
        "uvc": {
            "bcdUVC": evidence.EXPECTED_BCD_UVC,
            "bcdUVC_values": [evidence.EXPECTED_BCD_UVC],
            "extension_units": copy.deepcopy(list(evidence.EXPECTED_EXTENSION_UNITS)),
            "parse_errors": [],
        },
        "binding": {
            **binding.public(),
            "bound_video_nodes": [
                {
                    "node": "/dev/video0",
                    "index": "0",
                    "name_sha256": evidence._hash_text("2UQ2:"),
                    "device_identity_sha256": "sha256:video-interface",
                },
                {
                    "node": "/dev/video1",
                    "index": "1",
                    "name_sha256": evidence._hash_text("2UQ2:"),
                    "device_identity_sha256": "sha256:video-interface",
                },
            ],
        },
        "media": {
            "node": "/dev/media0",
            "command_returncode": 0,
            "error_code": None,
            "information": {
                "driver": "uvcvideo",
                "model_sha256": evidence._hash_text("2UQ2:"),
                "serial_sha256": evidence._hash_text(PLAINTEXT_SERIAL),
                "bus_info_sha256": evidence._hash_text("usb-xhci-1.2"),
            },
            "entities": [],
            "device_nodes": ["/dev/video0", "/dev/video1"],
            "raw_output_stored": False,
        },
        "v4l2": {
            "nodes": {
                "/dev/video0": {
                    "controls": [],
                    "control_count": 0,
                    "unparsed_nonempty_lines": 0,
                    "raw_output_stored": False,
                    "command_returncode": 0,
                    "error_code": None,
                },
                "/dev/video1": {
                    "controls": [],
                    "control_count": 0,
                    "unparsed_nonempty_lines": 0,
                    "raw_output_stored": False,
                    "command_returncode": 0,
                    "error_code": None,
                },
            },
            "enumeration_complete": True,
            "raw_output_stored": False,
        },
    }
    return evidence.IdentityObservation(
        public=public,
        binding=binding,
        private_values=private_values,
    )


class FakeRuntime:
    def __init__(
        self,
        *,
        health: dict[str, object] | None = None,
        query_gate_health: dict[str, object] | None = None,
        post_health: dict[str, object] | None = None,
        pre_identity: evidence.IdentityObservation | None = None,
        post_identity: evidence.IdentityObservation | None = None,
    ) -> None:
        self.health = health or matching_health()
        self.query_gate_health = query_gate_health or self.health
        self.post_health = post_health or self.health
        self.pre_identity = pre_identity or matching_identity()
        self.post_identity = post_identity or self.pre_identity
        self.identity_calls = 0
        self.health_calls = 0
        self.binding_calls = 0
        self.query_calls: list[tuple[int, int, int, int]] = []
        self.open_calls = 0
        self.close_calls = 0
        self.binding_hook = None
        self.query_hook = None
        self.lengths: dict[tuple[int, int], int] = {}
        self.cur_payloads: dict[tuple[int, int], bytes] = {}

    def now(self) -> str:
        return "2026-08-23T00:00:00Z"

    def health_snapshot(self) -> dict[str, object]:
        self.health_calls += 1
        if self.health_calls == 1:
            selected = self.health
        elif self.health_calls == 2:
            selected = self.query_gate_health
        else:
            selected = self.post_health
        return copy.deepcopy(selected)

    def identity_snapshot(self, device: str) -> evidence.IdentityObservation:
        self.identity_calls += 1
        self.assert_device(device)
        return self.pre_identity if self.identity_calls == 1 else self.post_identity

    def binding_token(self, device: str, media_node: str) -> evidence.BindingToken:
        self.binding_calls += 1
        self.assert_device(device)
        if media_node != "/dev/media0":
            raise AssertionError(media_node)
        if self.binding_hook is not None:
            return self.binding_hook(self.binding_calls)
        return self.pre_identity.binding

    def open_query_node(self, device: str, binding: evidence.BindingToken) -> int:
        self.open_calls += 1
        self.assert_device(device)
        if binding != self.pre_identity.binding:
            raise AssertionError(binding)
        return 19

    def close_query_node(self, fd: int) -> None:
        if fd != 19:
            raise AssertionError(fd)
        self.close_calls += 1

    def query_get(
        self,
        fd: int,
        unit: int,
        selector: int,
        query: int,
        size: int,
    ) -> bytes:
        if fd != 19:
            raise AssertionError(fd)
        self.query_calls.append((unit, selector, query, size))
        if self.query_hook is not None:
            hooked = self.query_hook(fd, unit, selector, query, size)
            if hooked is not None:
                return hooked
        if query == evidence.UVC_GET_INFO:
            return b"\x01"
        if query == evidence.UVC_GET_LEN:
            return self.lengths.get((unit, selector), 1).to_bytes(2, "little")
        if query == evidence.UVC_GET_CUR:
            return self.cur_payloads.get((unit, selector), bytes([selector % 251]) * size)
        raise AssertionError(f"unexpected query 0x{query:02x}")

    @staticmethod
    def assert_device(device: str) -> None:
        if device != "/dev/video0":
            raise AssertionError(device)


class FakeLibc:
    def __init__(self, rc: int = 0) -> None:
        self.rc = rc
        self.calls = 0

    def ioctl(self, fd: int, request: int, control: object) -> int:
        del fd, request, control
        self.calls += 1
        return self.rc


class RdkCameraFocusNegativeEvidenceTest(unittest.TestCase):
    def collect(
        self,
        directory: str,
        runtime: FakeRuntime,
        *,
        filename: str = "evidence.json",
    ) -> tuple[dict[str, object], Path]:
        output = Path(directory) / filename
        report = evidence.collect_evidence(
            evidence.CollectionConfig(
                output=output,
                expected_commit=EXPECTED_COMMIT,
                watchdog_seconds=30.0,
            ),
            runtime=runtime,
        )
        return report, output

    def test_matching_identity_executes_each_admitted_query_once(self) -> None:
        runtime = FakeRuntime()
        with tempfile.TemporaryDirectory() as directory:
            report, output = self.collect(directory, runtime)

            persisted = json.loads(output.read_text(encoding="utf-8"))

        self.assertEqual(report["status"], "complete")
        self.assertEqual(report["query_execution"]["executed_count"], 82)
        self.assertEqual(report["query_execution"]["failed_count"], 0)
        self.assertEqual(len(runtime.query_calls), 82)
        self.assertEqual(len(set(runtime.query_calls)), 82)
        self.assertEqual(persisted["status"], "complete")
        self.assertTrue(persisted["protocol_admission"]["admitted"])
        self.assertTrue(persisted["privacy_audit"]["passed"])
        self.assertFalse(persisted["read_only_contract"]["set_cur_performed"])
        self.assertEqual(persisted["read_only_contract"]["unknown_xu_writes_performed"], 0)
        self.assertEqual(
            persisted["pre"]["deployed_fingerprint"]["deployed_commit"],
            EXPECTED_COMMIT,
        )
        self.assertGreater(persisted["checkpointing"]["completed_count"], 82)
        self.assertEqual(runtime.open_calls, 1)
        self.assertEqual(runtime.close_calls, 1)

    def test_descriptor_parser_extracts_exact_guid_and_bmcontrols_fields(self) -> None:
        descriptors = bytearray([5, evidence.UVC_CS_INTERFACE, evidence.UVC_VC_HEADER, 0, 1])
        for expected in evidence.EXPECTED_EXTENSION_UNITS:
            guid = bytes.fromhex(expected["guid_raw_hex"])
            controls = bytes.fromhex(expected["bmControls_hex"])
            extension = bytearray(
                [
                    25 + len(controls),
                    evidence.UVC_CS_INTERFACE,
                    evidence.UVC_EXTENSION_UNIT,
                    expected["unit_id"],
                ]
            )
            extension.extend(guid)
            extension.extend(
                [
                    expected["bNumControls"],
                    1,
                    1,
                    expected["bControlSize"],
                ]
            )
            extension.extend(controls)
            extension.append(0)
            descriptors.extend(extension)

        parsed = evidence.parse_uvc_descriptors(bytes(descriptors))

        self.assertEqual(parsed["parse_errors"], [])
        self.assertEqual(parsed["bcdUVC"], evidence.EXPECTED_BCD_UVC)
        self.assertEqual(
            parsed["extension_units"],
            list(evidence.EXPECTED_EXTENSION_UNITS),
        )

    def test_exact_identity_mismatches_are_rejected_before_open_or_query(self) -> None:
        mutations = {
            "vid": lambda value: value.public["usb"].__setitem__("idVendor", "ffff"),
            "pid": lambda value: value.public["usb"].__setitem__("idProduct", "ffff"),
            "bcd_device": lambda value: value.public["usb"].__setitem__("bcdDevice", "9999"),
            "descriptor": lambda value: value.public["usb"].__setitem__(
                "descriptor_sha256", "0" * 64
            ),
            "bcd_uvc": lambda value: value.public["uvc"].__setitem__("bcdUVC", "0x0150"),
            "guid": lambda value: value.public["uvc"]["extension_units"][0].__setitem__(
                "guid_raw_hex", "0" * 32
            ),
            "bm_controls": lambda value: value.public["uvc"]["extension_units"][1].__setitem__(
                "bmControls_hex", "00000000"
            ),
        }
        with tempfile.TemporaryDirectory() as directory:
            for index, (name, mutate) in enumerate(mutations.items()):
                with self.subTest(name=name):
                    identity = matching_identity()
                    mutate(identity)
                    runtime = FakeRuntime(pre_identity=identity, post_identity=identity)
                    report, _ = self.collect(
                        directory,
                        runtime,
                        filename=f"mismatch-{index}.json",
                    )
                    self.assertEqual(report["status"], "stopped")
                    self.assertEqual(report["stop_reason"]["code"], "exact_identity_mismatch")
                    self.assertEqual(report["query_execution"]["executed_count"], 0)
                    self.assertEqual(runtime.open_calls, 0)
                    self.assertEqual(runtime.query_calls, [])

    def test_non_idle_capture_is_rejected_before_open_or_query(self) -> None:
        for device_state, active in (("recording", True), ("idle", True)):
            with self.subTest(device_state=device_state, active=active):
                health = matching_health()
                health["capture"]["device_state"] = device_state
                health["capture"]["active_recording_present"] = active
                runtime = FakeRuntime(health=health)
                with tempfile.TemporaryDirectory() as directory:
                    report, _ = self.collect(directory, runtime)

                self.assertEqual(report["stop_reason"]["code"], "capture_not_idle")
                self.assertEqual(report["query_execution"]["executed_count"], 0)
                self.assertEqual(runtime.open_calls, 0)
                self.assertEqual(runtime.identity_calls, 0)

    def test_inactive_service_is_rejected_before_open_or_query(self) -> None:
        health = matching_health()
        health["service"]["active_state"] = "inactive"
        runtime = FakeRuntime(health=health)
        with tempfile.TemporaryDirectory() as directory:
            report, _ = self.collect(directory, runtime)

        self.assertEqual(report["stop_reason"]["code"], "rp_ylx_not_active")
        self.assertEqual(runtime.query_calls, [])
        self.assertEqual(runtime.identity_calls, 0)

    def test_wrong_commit_is_rejected_before_open_or_query(self) -> None:
        runtime = FakeRuntime(health=matching_health("b" * 40))
        with tempfile.TemporaryDirectory() as directory:
            report, _ = self.collect(directory, runtime)

        self.assertEqual(report["stop_reason"]["code"], "expected_commit_mismatch")
        self.assertEqual(report["query_execution"]["executed_count"], 0)
        self.assertEqual(runtime.open_calls, 0)
        self.assertEqual(runtime.identity_calls, 0)

    def test_query_gate_rejects_capture_that_became_non_idle_after_identity(self) -> None:
        query_gate_health = matching_health()
        query_gate_health["capture"]["device_state"] = "recording"
        query_gate_health["capture"]["active_recording_present"] = True
        runtime = FakeRuntime(query_gate_health=query_gate_health)
        with tempfile.TemporaryDirectory() as directory:
            report, _ = self.collect(directory, runtime)

        self.assertEqual(report["stop_reason"]["code"], "capture_not_idle")
        self.assertEqual(report["query_execution"]["executed_count"], 0)
        self.assertEqual(runtime.open_calls, 0)
        self.assertEqual(runtime.identity_calls, 1)
        self.assertEqual(
            report["post"]["identity"]["skipped"],
            "unsafe_precondition_stop_reason",
        )

    def test_short_response_stops_after_one_attempt_without_retry(self) -> None:
        runtime = FakeRuntime()
        runtime.query_hook = lambda fd, unit, selector, query, size: b""
        with tempfile.TemporaryDirectory() as directory:
            report, output = self.collect(directory, runtime)
            persisted = json.loads(output.read_text(encoding="utf-8"))

        self.assertEqual(report["stop_reason"]["code"], "uvc_response_length_mismatch")
        self.assertEqual(report["query_execution"]["executed_count"], 1)
        self.assertEqual(report["query_execution"]["failed_count"], 1)
        self.assertEqual(len(runtime.query_calls), 1)
        self.assertEqual(persisted["query_execution"]["executed_count"], 1)

    def test_nonzero_query_failure_stops_without_retry_or_error_text_leak(self) -> None:
        runtime = FakeRuntime()

        def fail_query(fd: int, unit: int, selector: int, query: int, size: int) -> bytes:
            del fd, unit, selector, query, size
            raise RuntimeError("PRIVATE-SERIAL-01.00.00 CUR-FAILURE-BUFFER")

        runtime.query_hook = fail_query
        with tempfile.TemporaryDirectory() as directory:
            report, output = self.collect(directory, runtime)
            encoded = output.read_text(encoding="utf-8")

        self.assertEqual(report["stop_reason"]["code"], "uvc_query_failed")
        self.assertEqual(report["query_execution"]["executed_count"], 1)
        self.assertEqual(len(runtime.query_calls), 1)
        self.assertNotIn("PRIVATE-SERIAL", encoded)
        self.assertNotIn("CUR-FAILURE-BUFFER", encoded)

    def test_oversize_and_denylisted_controls_never_issue_get_cur(self) -> None:
        runtime = FakeRuntime()
        runtime.lengths[(3, 1)] = 257
        with tempfile.TemporaryDirectory() as directory:
            report, _ = self.collect(directory, runtime)

        cur_addresses = {
            (unit, selector)
            for unit, selector, query, size in runtime.query_calls
            if query == evidence.UVC_GET_CUR
        }
        self.assertNotIn((3, 1), cur_addresses)
        self.assertNotIn((4, 10), cur_addresses)
        self.assertNotIn((4, 15), cur_addresses)
        self.assertEqual(report["query_execution"]["executed_count"], 81)
        entries = {(item["unit"], item["selector"]): item for item in report["selectors"]}
        self.assertEqual(
            entries[(3, 1)]["get_cur"]["policy"],
            "skipped_declared_length_over_256",
        )
        self.assertEqual(entries[(4, 10)]["get_cur"]["policy"], "skipped_denylist")
        self.assertEqual(entries[(4, 15)]["get_cur"]["policy"], "skipped_denylist")

    def test_get_not_supported_and_zero_length_are_not_queried(self) -> None:
        runtime = FakeRuntime()
        runtime.lengths[(3, 2)] = 0

        def unsupported(fd: int, unit: int, selector: int, query: int, size: int) -> bytes | None:
            del fd, size
            if (unit, selector, query) == (3, 1, evidence.UVC_GET_INFO):
                return b"\x00"
            return None

        runtime.query_hook = unsupported
        with tempfile.TemporaryDirectory() as directory:
            report, _ = self.collect(directory, runtime)

        cur_addresses = {
            (unit, selector)
            for unit, selector, query, size in runtime.query_calls
            if query == evidence.UVC_GET_CUR
        }
        self.assertNotIn((3, 1), cur_addresses)
        self.assertNotIn((3, 2), cur_addresses)
        entries = {(item["unit"], item["selector"]): item for item in report["selectors"]}
        self.assertEqual(entries[(3, 1)]["get_cur"]["policy"], "skipped_get_not_supported")
        self.assertEqual(
            entries[(3, 2)]["get_cur"]["policy"],
            "skipped_invalid_declared_length",
        )

    def test_unwritable_output_fails_before_any_probe_or_query(self) -> None:
        runtime = FakeRuntime()
        with tempfile.TemporaryDirectory() as directory:
            not_a_directory = Path(directory) / "not-a-directory"
            not_a_directory.write_text("occupied", encoding="utf-8")
            output = not_a_directory / "evidence.json"
            with self.assertRaises(evidence.OutputUnavailable):
                evidence.collect_evidence(
                    evidence.CollectionConfig(output=output, expected_commit=EXPECTED_COMMIT),
                    runtime=runtime,
                )

        self.assertEqual(runtime.identity_calls, 0)
        self.assertEqual(runtime.health_calls, 0)
        self.assertEqual(runtime.open_calls, 0)
        self.assertEqual(runtime.query_calls, [])

    def test_node_change_is_detected_before_first_ioctl(self) -> None:
        runtime = FakeRuntime()
        changed = copy.copy(baseline_binding())
        object.__setattr__(changed, "query_node_rdev", 99)
        runtime.binding_hook = lambda call: baseline_binding() if call == 1 else changed
        with tempfile.TemporaryDirectory() as directory:
            report, _ = self.collect(directory, runtime)

        self.assertEqual(report["stop_reason"]["code"], "node_or_descriptor_changed")
        self.assertEqual(report["query_execution"]["executed_count"], 0)
        self.assertEqual(runtime.query_calls, [])

    def test_descriptor_change_after_one_query_stops_with_partial_count(self) -> None:
        runtime = FakeRuntime()
        changed = copy.copy(baseline_binding())
        object.__setattr__(changed, "descriptor_sha256", "f" * 64)
        runtime.binding_hook = lambda call: baseline_binding() if call <= 2 else changed
        with tempfile.TemporaryDirectory() as directory:
            report, output = self.collect(directory, runtime)
            persisted = json.loads(output.read_text(encoding="utf-8"))

        self.assertEqual(report["stop_reason"]["code"], "node_or_descriptor_changed")
        self.assertEqual(report["query_execution"]["executed_count"], 1)
        self.assertEqual(len(runtime.query_calls), 1)
        self.assertEqual(persisted["query_execution"]["executed_count"], 1)

    def test_post_identity_change_fails_postcondition(self) -> None:
        changed = copy.copy(baseline_binding())
        object.__setattr__(changed, "descriptor_sha256", "e" * 64)
        runtime = FakeRuntime(post_identity=matching_identity(binding=changed))
        with tempfile.TemporaryDirectory() as directory:
            report, _ = self.collect(directory, runtime)

        self.assertEqual(report["status"], "stopped")
        self.assertEqual(report["stop_reason"]["code"], "postcondition_failed")
        failed = [item["name"] for item in report["post"]["checks"] if not item["passed"]]
        self.assertIn("post.identity_stable", failed)

    def test_post_service_restart_fails_health_postcondition(self) -> None:
        post_health = matching_health()
        post_health["service"]["main_pid"] = "456"
        post_health["service"]["restart_count"] = "1"
        runtime = FakeRuntime(post_health=post_health)
        with tempfile.TemporaryDirectory() as directory:
            report, _ = self.collect(directory, runtime)

        self.assertEqual(report["status"], "stopped")
        self.assertEqual(report["stop_reason"]["code"], "postcondition_failed")
        failed = [item["name"] for item in report["post"]["checks"] if not item["passed"]]
        self.assertIn("post.service_main_pid_stable", failed)
        self.assertIn("post.service_restart_count_stable", failed)

    def test_post_non_idle_health_skips_device_identity_enumeration(self) -> None:
        post_health = matching_health()
        post_health["capture"]["device_state"] = "recording"
        post_health["capture"]["active_recording_present"] = True
        runtime = FakeRuntime(post_health=post_health)
        with tempfile.TemporaryDirectory() as directory:
            report, _ = self.collect(directory, runtime)

        self.assertEqual(report["status"], "stopped")
        self.assertEqual(report["stop_reason"]["code"], "postcondition_failed")
        self.assertEqual(runtime.identity_calls, 1)
        self.assertEqual(
            report["post"]["identity"]["skipped"],
            "post_health_not_admitted",
        )

    def test_watchdog_preserves_in_progress_query_count_and_stop_reason(self) -> None:
        runtime = FakeRuntime()

        def expire(fd: int, unit: int, selector: int, query: int, size: int) -> bytes:
            del fd, unit, selector, query, size
            raise evidence.WatchdogExpired()

        runtime.query_hook = expire
        with tempfile.TemporaryDirectory() as directory:
            report, output = self.collect(directory, runtime)
            persisted = json.loads(output.read_text(encoding="utf-8"))

        self.assertEqual(report["stop_reason"]["code"], "watchdog_expired")
        self.assertEqual(report["query_execution"]["executed_count"], 1)
        self.assertIsNotNone(report["query_execution"]["in_progress"])
        self.assertEqual(persisted["stop_reason"]["in_progress_query"]["ordinal"], 1)

    def test_media_topology_is_structured_and_serial_is_hashed(self) -> None:
        topology, private_values = evidence.parse_media_topology(
            """
Media controller API version 6.1.83

Media device information
------------------------
driver          uvcvideo
model           2UQ2:
serial          PRIVATE-MEDIA-SERIAL
bus info        usb-private-bus
hw revision     0x701
driver version  6.1.83

Device topology
- entity 1: 2UQ2:  (1 pad, 1 link)
            device node name /dev/video0
"""
        )

        encoded = json.dumps(topology, sort_keys=True)
        self.assertEqual(topology["information"]["driver"], "uvcvideo")
        self.assertEqual(topology["information"]["driver_version"], "6.1.83")
        self.assertEqual(topology["device_nodes"], ["/dev/video0"])
        self.assertEqual(
            private_values,
            ("2UQ2:", "PRIVATE-MEDIA-SERIAL", "usb-private-bus"),
        )
        self.assertNotIn("2UQ2:", encoded)
        self.assertNotIn("PRIVATE-MEDIA-SERIAL", encoded)
        self.assertNotIn("usb-private-bus", encoded)
        self.assertNotIn("stdout", topology)

    def test_v4l2_controls_are_structured_without_raw_output(self) -> None:
        parsed = evidence.parse_v4l2_controls(
            """
User Controls

brightness 0x00980900 (int) : min=-64 max=64 step=1 default=0 value=3
white_balance_automatic 0x0098090c (bool) : default=1 value=1

Camera Controls

auto_exposure 0x009a0901 (menu) : min=0 max=3 default=3 value=3 (Aperture Priority)
"""
        )

        self.assertEqual(parsed["control_count"], 3)
        self.assertEqual(parsed["unparsed_nonempty_lines"], 0)
        self.assertEqual(parsed["controls"][0]["section"], "User Controls")
        self.assertEqual(parsed["controls"][0]["attributes"]["value"], "3")
        self.assertEqual(parsed["controls"][2]["section"], "Camera Controls")
        self.assertFalse(parsed["raw_output_stored"])
        self.assertNotIn("stdout", parsed)

    def test_report_omits_plaintext_serial_and_raw_cur_encodings(self) -> None:
        runtime = FakeRuntime()
        raw_cur = b"CUR-SECRET"
        runtime.lengths[(3, 1)] = len(raw_cur)
        runtime.cur_payloads[(3, 1)] = raw_cur
        with tempfile.TemporaryDirectory() as directory:
            report, output = self.collect(directory, runtime)
            encoded = output.read_text(encoding="utf-8")

        self.assertTrue(report["privacy_audit"]["passed"])
        for private_identity in (
            PLAINTEXT_SERIAL,
            PLAINTEXT_MANUFACTURER,
            PLAINTEXT_PRODUCT,
            "2UQ2:",
            "usb-xhci-1.2",
        ):
            self.assertNotIn(private_identity, encoded)
        self.assertNotIn(raw_cur.decode("ascii"), encoded)
        self.assertNotIn(raw_cur.hex(), encoded)
        self.assertNotIn(base64.b64encode(raw_cur).decode("ascii"), encoded)
        expected_hash = hashlib.sha256(raw_cur).hexdigest()
        self.assertIn(expected_hash, encoded)

    def test_atomic_output_is_private_and_existing_output_is_not_replaced(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "evidence.json"
            writer = evidence.AtomicCheckpoint(output)
            writer.create({"state": "created"})
            self.assertEqual(stat.S_IMODE(output.stat().st_mode), 0o600)

            second = evidence.AtomicCheckpoint(output)
            with self.assertRaises(evidence.OutputUnavailable):
                second.create({"state": "replacement"})
            self.assertEqual(json.loads(output.read_text(encoding="utf-8")), {"state": "created"})

    def test_low_level_transport_blocks_unsafe_codes_and_risky_cur_before_ioctl(self) -> None:
        runtime = evidence.SystemRuntime()
        fake = FakeLibc()
        with patch.object(evidence, "_LIBC", fake):
            with self.assertRaises(evidence.EvidenceError):
                runtime.query_get(7, 4, 9, 0x01, 1)
            with self.assertRaises(evidence.EvidenceError):
                runtime.query_get(7, 4, 10, evidence.UVC_GET_CUR, 1)
            with self.assertRaises(evidence.EvidenceError):
                runtime.query_get(7, 4, 15, evidence.UVC_GET_CUR, 1)
            with self.assertRaises(evidence.EvidenceError):
                runtime.query_get(7, 4, 9, evidence.UVC_GET_CUR, 257)

        self.assertEqual(fake.calls, 0)

    def test_authenticated_health_probe_rejects_plaintext_http(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            token_path = Path(directory) / "customer.token"
            token_path.write_text("x" * 48 + "\n", encoding="ascii")
            token_path.chmod(0o640)

            with self.assertRaises(evidence.EvidenceError) as raised:
                evidence.SystemRuntime(
                    base_url="http://127.0.0.1:8080",
                    bearer_token_file=token_path,
                )

        self.assertEqual(raised.exception.code, "authenticated_http_forbidden")

    def test_authenticated_https_health_probe_uses_explicit_ca_and_private_token(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            token = "customer-token-" + "x" * 32
            token_path = root / "customer.token"
            token_path.write_text(token + "\n", encoding="ascii")
            token_path.chmod(0o640)
            ca_path = root / "device.crt"
            ca_path.write_text("certificate", encoding="ascii")
            context = MagicMock()
            response = MagicMock()
            response.__enter__.return_value.status = 200
            response.__enter__.return_value.read.return_value = json.dumps(
                {
                    "schema": "ylx.capture-status.v4",
                    "snapshot": {
                        "schema": "ylx.capture-snapshot-event.v4",
                        "device_state": "idle",
                        "active_recording": None,
                    },
                }
            ).encode()
            with (
                patch(
                    "scripts.rdk_camera_focus_negative_evidence.ssl.create_default_context",
                    return_value=context,
                ) as create_context,
                patch(
                    "scripts.rdk_camera_focus_negative_evidence.request.urlopen",
                    return_value=response,
                ) as urlopen,
            ):
                runtime = evidence.SystemRuntime(
                    base_url="https://127.0.0.1:8080",
                    ca_certificate=ca_path,
                    bearer_token_file=token_path,
                )
                status = runtime._capture_status()

            create_context.assert_called_once_with(cafile=str(ca_path))
            probe = urlopen.call_args.args[0]
            self.assertEqual(probe.get_header("Authorization"), f"Bearer {token}")
            self.assertIs(urlopen.call_args.kwargs["context"], context)
            self.assertEqual(status["http_status"], 200)
            self.assertEqual(status["device_state"], "idle")

    def test_evidence_configuration_redacts_health_credentials_and_paths(self) -> None:
        token_path = Path("/run/private/customer-token-sentinel")
        ca_path = Path("/run/private/device-ca-sentinel")
        report = evidence._initial_report(
            evidence.CollectionConfig(
                output=Path("/tmp/evidence.json"),
                expected_commit=EXPECTED_COMMIT,
                base_url="https://127.0.0.1:8080",
                ca_certificate=ca_path,
                bearer_token_file=token_path,
            ),
            "2026-08-23T00:00:00Z",
        )
        rendered = json.dumps(report, sort_keys=True)

        self.assertTrue(report["configuration"]["health_authenticated"])
        self.assertEqual(report["configuration"]["health_transport"], "https")
        self.assertNotIn(str(token_path), rendered)
        self.assertNotIn(str(ca_path), rendered)

    def test_command_allowlist_has_read_only_v4l2_enumeration_only(self) -> None:
        allowed = evidence.SystemRuntime._command_allowed
        self.assertTrue(allowed(["v4l2-ctl", "--list-ctrls", "-d", "/dev/video0"]))
        self.assertFalse(
            allowed(["v4l2-ctl", "--set-ctrl", "focus_absolute=10", "-d", "/dev/video0"])
        )
        self.assertFalse(allowed(["usbreset", "1bcf:0b15"]))
        self.assertFalse(allowed(["systemctl", "restart", "rp-ylx"]))

    def test_low_level_transport_requires_exact_zero_ioctl_result(self) -> None:
        runtime = evidence.SystemRuntime()
        for result in (-1, 1):
            with self.subTest(result=result):
                fake = FakeLibc(result)
                with (
                    patch.object(evidence, "_LIBC", fake),
                    self.assertRaises(evidence.EvidenceError) as raised,
                ):
                    runtime.query_get(7, 3, 1, evidence.UVC_GET_INFO, 1)

                self.assertEqual(raised.exception.code, "uvc_ioctl_nonzero")
                self.assertEqual(fake.calls, 1)


if __name__ == "__main__":
    unittest.main()

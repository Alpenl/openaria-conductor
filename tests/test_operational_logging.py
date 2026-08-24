from __future__ import annotations

import io
import json
import logging
import os
import tempfile
import unittest
from importlib.resources import files
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

from jsonschema import Draft202012Validator, FormatChecker

from rp_ylx.api import AuditEvent
from rp_ylx.daemon import _audit_sink, run_production_service
from rp_ylx.network_control import NetworkController
from rp_ylx.network_credentials import NetworkCredentialStore
from rp_ylx.operational_logging import (
    OPERATIONAL_EVENT_SCHEMA,
    OPERATIONAL_LOGGER_NAME,
    configure_operational_logging,
    operational_logger,
    reset_operational_logging,
)


def _schema(name: str) -> dict[str, object]:
    return json.loads(files("rp_ylx.schemas").joinpath(name).read_text(encoding="utf-8"))


class OperationalLoggingTest(unittest.TestCase):
    def setUp(self) -> None:
        self.stream = io.StringIO()
        configure_operational_logging(stream=self.stream)

    def tearDown(self) -> None:
        reset_operational_logging()

    def events(self) -> list[dict[str, object]]:
        return [json.loads(line) for line in self.stream.getvalue().splitlines()]

    def test_event_is_closed_schema_valid_and_drops_unapproved_values(self) -> None:
        operational_logger("network-control").event(
            "network_transaction_accepted",
            transaction_id="0198d2a0-41a0-7b7a-a751-0e86a39d4db1",
            desired_mode="wifi-client",
            passphrase="do-not-log-this-secret",
            payload={"token": "also-secret"},
            error_code="invalid\nsecret",
            port=8443.5,
        )

        events = self.events()
        self.assertEqual(len(events), 1)
        event = events[0]
        self.assertEqual(event["schema"], OPERATIONAL_EVENT_SCHEMA)
        self.assertEqual(event["component"], "network-control")
        self.assertEqual(event["event"], "network_transaction_accepted")
        self.assertEqual(
            event["context"],
            {
                "desired_mode": "wifi-client",
                "redacted_field_count": 4,
                "transaction_id": "0198d2a0-41a0-7b7a-a751-0e86a39d4db1",
            },
        )
        rendered = self.stream.getvalue()
        self.assertNotIn("do-not-log-this-secret", rendered)
        self.assertNotIn("also-secret", rendered)
        self.assertNotIn("invalid\\nsecret", rendered)
        Draft202012Validator(
            _schema("ylx-operational-event-v1.schema.json"),
            format_checker=FormatChecker(),
        ).validate(event)

    def test_direct_standard_logger_bypass_does_not_render_message(self) -> None:
        logger = logging.getLogger(f"{OPERATIONAL_LOGGER_NAME}.bypass")
        logger.error("passphrase=raw-secret-that-must-not-appear")

        event = self.events()[0]
        self.assertEqual(event["event"], "unstructured_log_rejected")
        self.assertEqual(event["context"], {"redacted_field_count": 1})
        self.assertNotIn("raw-secret-that-must-not-appear", self.stream.getvalue())

    def test_level_filter_and_reconfiguration_are_deterministic(self) -> None:
        self.assertEqual(
            configure_operational_logging(stream=self.stream, level="warning"), "warning"
        )
        logger = operational_logger("runtime")
        logger.event("filtered_info")
        logger.event("retained_warning", level="warning", error_code="expected_warning")
        self.assertEqual([event["event"] for event in self.events()], ["retained_warning"])

        self.stream.seek(0)
        self.stream.truncate()
        self.assertEqual(configure_operational_logging(stream=self.stream, level="verbose"), "info")
        events = self.events()
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["event"], "log_level_defaulted")
        self.assertEqual(events[0]["context"], {"error_code": "invalid_log_level"})

    def test_api_audit_is_schema_valid_private_and_rejects_symlinks(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            audit_path = root / "api-audit.ndjson"
            audit_path.touch(mode=0o644)
            sink = _audit_sink(audit_path)
            sink(
                AuditEvent(
                    request_id="request-1",
                    principal_id="device-owner",
                    operation_id="getDevice",
                    resource_id=None,
                    outcome="allowed",
                )
            )

            event = json.loads(audit_path.read_text(encoding="utf-8"))
            mode = audit_path.stat().st_mode & 0o777
            target = root / "redirected.ndjson"
            target.touch()
            link = root / "audit-link.ndjson"
            link.symlink_to(target)
            with self.assertRaises(OSError):
                _audit_sink(link)(AuditEvent("request-2", None, "getDevice", None, "unauthorized"))

        self.assertEqual(mode, 0o600)
        self.assertEqual(event["schema"], "ylx.api-audit-event.v1")
        self.assertEqual(event["outcome"], "allowed")
        Draft202012Validator(
            _schema("ylx-api-audit-event-v1.schema.json"),
            format_checker=FormatChecker(),
        ).validate(event)

    def test_network_transaction_timeline_never_logs_credential_material(self) -> None:
        secret = "timeline-client-secret"
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            environment = {
                "RP_YLX_NETWORK_STATE_DIR": str(root / "state"),
                "RP_YLX_NETWORK_OPERATION_LOCK_PATH": str(root / "network-operation.lock"),
            }
            credentials = NetworkCredentialStore(token_factory=lambda: "timeline-reference")
            with (
                patch.dict(os.environ, environment, clear=False),
                patch(
                    "rp_ylx.network_control.ensure_rescue_ap",
                    return_value={"ssid": "YLX-TEST", "interface": "wlan0"},
                ),
                patch("rp_ylx.network_control.rescue_network"),
                patch("rp_ylx.network_control.saved_network_is_healthy", return_value=True),
                patch("rp_ylx.network_control.cleanup_orphan_network_candidates"),
                patch("rp_ylx.network_control.forget_network_client_profiles"),
            ):
                controller = NetworkController(
                    device_id="logging-test-device",
                    credential_store=credentials,
                    start_worker=False,
                    require_root=False,
                )
                try:
                    controller.handle(
                        {
                            "schema": "ylx.network-control-request.v1",
                            "operation": "create_credential",
                            "principal_id": "private-principal",
                            "body": {
                                "schema": "ylx.network-credential-request.v1",
                                "passphrase": secret,
                            },
                        }
                    )
                    accepted = controller.handle(
                        {
                            "schema": "ylx.network-control-request.v1",
                            "operation": "forget",
                            "principal_id": "private-principal",
                            "idempotency_key": "private-idempotency-key",
                            "body": {"schema": "ylx.network-forget-request.v1"},
                        }
                    )
                    transaction_id = accepted["body"]["transaction"]["transaction_id"]
                    controller._execute(str(transaction_id))
                finally:
                    controller.close()

        events = self.events()
        stages = [
            event["context"]["stage"]
            for event in events
            if event["event"] == "network_transaction_transition"
        ]
        self.assertEqual(stages, ["forgetting", "forgotten"])
        accepted_event = next(
            event for event in events if event["event"] == "network_transaction_accepted"
        )
        self.assertEqual(accepted_event["context"]["operation"], "forget")
        rendered = self.stream.getvalue()
        self.assertNotIn(secret, rendered)
        self.assertNotIn("timeline-reference", rendered)
        self.assertNotIn("private-principal", rendered)
        self.assertNotIn("private-idempotency-key", rendered)

    def test_monitor_errors_are_rate_limited_without_exception_messages(self) -> None:
        ticks = iter((0, 1_000_000_000, 61_000_000_000))
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with (
                patch.dict(
                    os.environ,
                    {
                        "RP_YLX_NETWORK_STATE_DIR": str(root / "state"),
                        "RP_YLX_NETWORK_OPERATION_LOCK_PATH": str(root / "network-operation.lock"),
                    },
                    clear=False,
                ),
                patch(
                    "rp_ylx.network_control.ensure_rescue_ap",
                    return_value={"ssid": "YLX-TEST", "interface": "wlan0"},
                ),
                patch("rp_ylx.network_control.rescue_network"),
                patch("rp_ylx.network_control.saved_network_is_healthy", return_value=True),
                patch("rp_ylx.network_control.cleanup_orphan_network_candidates"),
            ):
                controller = NetworkController(
                    device_id="monitor-log-test",
                    start_worker=False,
                    require_root=False,
                    monotonic_ns=lambda: next(ticks),
                )
                try:
                    error = RuntimeError("token=monitor-secret")
                    controller._record_monitor_failure(error)
                    controller._record_monitor_failure(error)
                    controller._record_monitor_failure(error)
                    controller._record_monitor_recovered()
                finally:
                    controller.close()

        failures = [
            event for event in self.events() if event["event"] == "network_health_monitor_failed"
        ]
        self.assertEqual(len(failures), 2)
        self.assertEqual(failures[-1]["context"]["suppressed_count"], 1)
        self.assertNotIn("monitor-secret", self.stream.getvalue())

    def test_production_service_lifecycle_is_structured(self) -> None:
        config = SimpleNamespace(security_profile="customer", data_plane="rust", port=8443)
        service = Mock()
        service.server.serve_forever.return_value = None

        with (
            patch("rp_ylx.daemon.load_production_config", return_value=config),
            patch("rp_ylx.daemon.build_production_service", return_value=service),
        ):
            run_production_service("/private/config/path.json")

        self.assertEqual(
            [event["event"] for event in self.events()],
            [
                "production_service_starting",
                "production_service_ready",
                "production_service_stopping",
                "production_service_stopped",
            ],
        )
        rendered = self.stream.getvalue()
        self.assertNotIn("/private/config/path.json", rendered)
        service.close.assert_called_once_with()


if __name__ == "__main__":
    unittest.main()

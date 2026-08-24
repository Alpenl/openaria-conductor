from __future__ import annotations

import io
import json
import os
import socket
import subprocess
import tempfile
import threading
import unittest
from contextlib import redirect_stderr, redirect_stdout
from http import HTTPStatus
from pathlib import Path
from unittest.mock import Mock, patch

import rp_ylx.network as network_module
import rp_ylx.network_state as network_state_module
from rp_ylx.api.gateway import NetworkCommand, NetworkCredentialCommand, ProviderError
from rp_ylx.cli import main
from rp_ylx.network_control import (
    CONTROL_RESPONSE_SCHEMA,
    CONTROL_RESPONSE_TIMEOUT_SECONDS,
    CONTROL_SOCKET_TIMEOUT_SECONDS,
    NetworkControlClientError,
    NetworkController,
    handle_control_payload,
    request_control,
    serve_stdio,
)
from rp_ylx.network_credentials import NetworkCredentialError, NetworkCredentialStore
from rp_ylx.network_state import NetworkStateError, NetworkStateStore
from rp_ylx.operational_logging import reset_operational_logging
from rp_ylx.recording.coordinator import CaptureCoordinator


class NetworkCredentialStoreTest(unittest.TestCase):
    def test_credential_is_opaque_short_lived_and_consumed_once(self) -> None:
        now = 100.0
        store = NetworkCredentialStore(
            ttl_seconds=30,
            clock=lambda: now,
            token_factory=lambda: "opaque-reference-001",
        )

        credential_ref = store.create("client-secret-123")

        self.assertEqual(credential_ref, "cred-opaque-reference-001")
        self.assertNotIn("client-secret-123", credential_ref)
        with store.consume(credential_ref) as secret:
            self.assertEqual(secret, "client-secret-123")
        with (
            self.assertRaises(NetworkCredentialError) as consumed,
            store.consume(credential_ref),
        ):
            pass
        self.assertEqual(consumed.exception.code, "credential_ref_invalid")

        expiring_ref = store.create("second-secret-123")
        now = 131.0
        with (
            self.assertRaises(NetworkCredentialError) as expired,
            store.consume(expiring_ref),
        ):
            pass
        self.assertEqual(expired.exception.code, "credential_ref_expired")

    def test_reservation_is_exclusive_and_consumes_only_on_commit(self) -> None:
        store = NetworkCredentialStore(token_factory=lambda: "reserved-reference")
        credential_ref = store.create("reserved-secret-123")

        with store.reserve(credential_ref) as reservation:
            self.assertEqual(reservation.credential, "reserved-secret-123")
            with (
                self.assertRaises(NetworkCredentialError) as reserved,
                store.consume(credential_ref),
            ):
                pass
            self.assertEqual(reserved.exception.code, "credential_ref_invalid")

        with store.reserve(credential_ref) as reservation:
            reservation.commit()
        with (
            self.assertRaises(NetworkCredentialError) as consumed,
            store.consume(credential_ref),
        ):
            pass
        self.assertEqual(consumed.exception.code, "credential_ref_invalid")


class NetworkStateStoreTest(unittest.TestCase):
    def test_first_authority_imports_existing_secret_free_wifi_lkg(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            root.mkdir(exist_ok=True)
            lkg = root / "lkg-wlan0.json"
            lkg.write_text(
                json.dumps(
                    {
                        "format": "ylx.network-lkg.v0",
                        "mode": "wifi-client",
                        "interface": "wlan0",
                        "profile": "rp-ylx-wifi-client-0123456789ab",
                        "config": {
                            "mode": "wifi-client",
                            "ssid": "Existing LAN",
                            "security": "wpa3-personal",
                        },
                    }
                ),
                encoding="utf-8",
            )
            lkg.chmod(0o600)

            snapshot = NetworkStateStore(root).snapshot()

        self.assertTrue(snapshot["saved"])
        self.assertTrue(snapshot["verified"])
        self.assertEqual(
            snapshot["desired"]["wifi_client"],
            {
                "ssid": "Existing LAN",
                "security": "wpa3-personal",
                "credential_state": "stored",
            },
        )

    def test_accepted_receipt_is_durable_authoritative_and_secret_free(self) -> None:
        transaction_id = "0198d2a0-41a0-7b7a-a751-0e86a39d4db1"
        accepted_at = "2026-08-23T15:00:00+08:00"
        desired = {
            "mode": "wifi-client",
            "wifi_client": {
                "ssid": "Field LAN",
                "security": "wpa2-personal",
                "credential_state": "pending_input",
            },
            "ethernet": None,
        }
        work = {
            "kind": "candidate",
            "profile": "rp-ylx-wifi-client-0123456789ab",
            "config": {
                "mode": "wifi-client",
                "ssid": "Field LAN",
                "security": "wpa2-personal",
            },
        }

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            store = NetworkStateStore(root)
            initial = store.snapshot()
            receipt, replayed = store.accept_transaction(
                operation="apply",
                desired=desired,
                principal_id="customer",
                idempotency_key="idem-1",
                request_fingerprint="a" * 64,
                transaction_id=transaction_id,
                accepted_at=accepted_at,
                work=work,
            )

            self.assertFalse(replayed)
            self.assertEqual(initial["source_revision"], 0)
            self.assertFalse(initial["saved"])
            self.assertFalse(initial["verified"])
            self.assertEqual(receipt["schema"], "ylx.network-transaction-receipt.v1")
            self.assertEqual(receipt["transaction"]["status"], "accepted")
            self.assertEqual(receipt["transaction"]["desired"], desired)
            self.assertEqual(receipt["transaction"]["authority_epoch"], initial["authority_epoch"])
            self.assertEqual(receipt["transaction"]["source_revision"], 1)
            self.assertIsNone(receipt["transaction"]["deadline"])
            self.assertEqual(receipt["transaction"]["recovery_action"], "await_device")

            accepted = store.snapshot()
            self.assertEqual(accepted["source_revision"], 1)
            self.assertTrue(accepted["saved"])
            self.assertFalse(accepted["verified"])
            self.assertEqual(accepted["authority_epoch"], initial["authority_epoch"])
            self.assertEqual(accepted["desired"], desired)
            self.assertEqual(accepted["transaction"]["current"], receipt["transaction"])
            self.assertIsNone(accepted["transaction"]["latest"])

            reopened = NetworkStateStore(root).snapshot()
            self.assertEqual(reopened, accepted)
            state_path = root / "controller-state.json"
            self.assertEqual(state_path.stat().st_mode & 0o777, 0o600)
            rendered = state_path.read_text(encoding="utf-8")
            self.assertNotIn("credential_ref", rendered)
            self.assertNotIn("client-secret", rendered)

            replay, was_replayed = store.accept_transaction(
                operation="apply",
                desired=desired,
                principal_id="customer",
                idempotency_key="idem-1",
                request_fingerprint="a" * 64,
                transaction_id="0198d2a0-41a0-7b7a-a751-0e86a39d4db2",
                accepted_at="2026-08-23T15:01:00+08:00",
                work=work,
            )
            self.assertTrue(was_replayed)
            self.assertEqual(replay, receipt)
            self.assertEqual(store.snapshot()["source_revision"], 1)

            with self.assertRaises(NetworkStateError) as conflict:
                store.accept_transaction(
                    operation="apply",
                    desired=desired,
                    principal_id="customer",
                    idempotency_key="idem-1",
                    request_fingerprint="b" * 64,
                    transaction_id="0198d2a0-41a0-7b7a-a751-0e86a39d4db3",
                    accepted_at="2026-08-23T15:02:00+08:00",
                    work=work,
                )
            self.assertEqual(conflict.exception.code, "idempotency_conflict")

    def test_post_replace_write_error_is_confirmed_from_published_state(self) -> None:
        desired = {"mode": "hotspot", "wifi_client": None, "ethernet": None}
        transaction_id = "0198d2a0-41a0-7b7a-a751-0e86a39d4db4"

        with tempfile.TemporaryDirectory() as directory:
            store = NetworkStateStore(Path(directory))
            store.accept_transaction(
                operation="apply",
                desired=desired,
                principal_id="customer",
                idempotency_key="post-replace",
                request_fingerprint="c" * 64,
                transaction_id=transaction_id,
                accepted_at="2026-08-23T15:03:00+08:00",
                work={"kind": "rescue", "desired": desired},
            )
            real_write_json = network_state_module._write_json

            def publish_then_fail(path: Path, value: object) -> None:
                real_write_json(path, value)
                raise OSError("directory fsync failed after replace")

            with patch.object(network_state_module, "_write_json", side_effect=publish_then_fail):
                terminal = store.transition(
                    transaction_id,
                    status="committed",
                    stage="committed",
                    updated_at="2026-08-23T15:03:01+08:00",
                    ap_validated=True,
                    recovery_action="reconnect_rescue_ap",
                    saved=False,
                    verified=False,
                    retain_work=False,
                )

            snapshot = store.snapshot()

        self.assertEqual(terminal["status"], "committed")
        self.assertIsNone(snapshot["transaction"]["current"])
        self.assertEqual(snapshot["transaction"]["latest"]["transaction_id"], transaction_id)

    def test_legacy_state_migration_rejects_secret_fields_without_rewriting_file(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            NetworkStateStore(root)
            state_path = root / "controller-state.json"
            legacy = json.loads(state_path.read_text(encoding="utf-8"))
            legacy.pop("execution_release")
            legacy.pop("rescue_boot_id")
            legacy["work"] = {"legacy": {"secret": "must-not-migrate"}}
            state_path.write_text(json.dumps(legacy), encoding="utf-8")

            with self.assertRaises(NetworkStateError) as rejected:
                NetworkStateStore(root)

            unchanged = json.loads(state_path.read_text(encoding="utf-8"))

        self.assertEqual(rejected.exception.code, "state_invalid")
        self.assertNotIn("must-not-migrate", str(rejected.exception))
        self.assertNotIn("execution_release", unchanged)
        self.assertNotIn("rescue_boot_id", unchanged)

    def test_old_idempotency_key_survives_the_previous_receipt_limit(self) -> None:
        desired = {"mode": "hotspot", "wifi_client": None, "ethernet": None}
        first_id = "0198d2a0-41a0-7b7a-a751-0e86a39d4db6"
        later_id = "0198d2a0-41a0-7b7a-a751-0e86a39d4db7"

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            store = NetworkStateStore(root)
            with patch.object(NetworkStateStore, "_scope", return_value="0" * 64):
                store.accept_transaction(
                    operation="apply",
                    desired=desired,
                    principal_id="customer",
                    idempotency_key="oldest",
                    request_fingerprint="a" * 64,
                    transaction_id=first_id,
                    accepted_at="2026-08-23T15:05:00+08:00",
                    work={"kind": "rescue", "desired": desired},
                )
            store.transition(
                first_id,
                status="committed",
                stage="committed",
                updated_at="2026-08-23T15:05:01+08:00",
                ap_validated=True,
                recovery_action="reconnect_rescue_ap",
                retain_work=False,
            )

            state_path = root / "controller-state.json"
            state = json.loads(state_path.read_text(encoding="utf-8"))
            oldest = state["receipts"]["0" * 64]
            state["receipts"] = {f"{index:064x}": oldest for index in range(1024)}
            state_path.write_text(json.dumps(state), encoding="utf-8")

            with patch.object(NetworkStateStore, "_scope", return_value="f" * 64):
                store.accept_transaction(
                    operation="apply",
                    desired=desired,
                    principal_id="customer",
                    idempotency_key="newest",
                    request_fingerprint="b" * 64,
                    transaction_id=later_id,
                    accepted_at="2026-08-23T15:05:02+08:00",
                    work={"kind": "rescue", "desired": desired},
                )
            store.transition(
                later_id,
                status="committed",
                stage="committed",
                updated_at="2026-08-23T15:05:03+08:00",
                ap_validated=True,
                recovery_action="reconnect_rescue_ap",
                retain_work=False,
            )

            with (
                patch.object(NetworkStateStore, "_scope", return_value="0" * 64),
                self.assertRaises(NetworkStateError) as conflict,
            ):
                store.accept_transaction(
                    operation="apply",
                    desired=desired,
                    principal_id="customer",
                    idempotency_key="oldest",
                    request_fingerprint="c" * 64,
                    transaction_id="0198d2a0-41a0-7b7a-a751-0e86a39d4db8",
                    accepted_at="2026-08-23T15:05:04+08:00",
                    work={"kind": "rescue", "desired": desired},
                )

        self.assertEqual(conflict.exception.code, "idempotency_conflict")


class NetworkControllerCoreTest(unittest.TestCase):
    def setUp(self) -> None:
        self._operation_directory = tempfile.TemporaryDirectory()
        lock_path = Path(self._operation_directory.name) / "network-operation.lock"
        self._operation_environment = patch.dict(
            os.environ,
            {"RP_YLX_NETWORK_OPERATION_LOCK_PATH": str(lock_path)},
            clear=False,
        )
        self._operation_environment.start()

    def tearDown(self) -> None:
        self._operation_environment.stop()
        self._operation_directory.cleanup()

    @staticmethod
    def _create_credential(controller: NetworkController, passphrase: str) -> str:
        response = controller.handle(
            {
                "schema": "ylx.network-control-request.v1",
                "operation": "create_credential",
                "principal_id": "customer",
                "body": {
                    "schema": "ylx.network-credential-request.v1",
                    "passphrase": passphrase,
                },
            }
        )
        return str(response["body"]["credential_ref"])

    @staticmethod
    def _apply_protected_wifi(
        controller: NetworkController,
        credential_ref: str,
        *,
        idempotency_key: str,
    ) -> dict[str, object]:
        return controller.handle(
            {
                "schema": "ylx.network-control-request.v1",
                "operation": "apply",
                "principal_id": "customer",
                "idempotency_key": idempotency_key,
                "body": {
                    "schema": "ylx.network-apply-request.v1",
                    "desired": {
                        "mode": "wifi-client",
                        "wifi_client": {
                            "ssid": "Field LAN",
                            "security": "wpa2-personal",
                            "credential_ref": credential_ref,
                        },
                        "ethernet": None,
                    },
                },
            }
        )

    @staticmethod
    def _apply_open_wifi(
        controller: NetworkController,
        *,
        idempotency_key: str,
        ssid: str = "Guest",
    ) -> dict[str, object]:
        return controller.handle(
            {
                "schema": "ylx.network-control-request.v1",
                "operation": "apply",
                "principal_id": "customer",
                "idempotency_key": idempotency_key,
                "body": {
                    "schema": "ylx.network-apply-request.v1",
                    "desired": {
                        "mode": "wifi-client",
                        "wifi_client": {"ssid": ssid, "security": "open"},
                        "ethernet": None,
                    },
                },
            }
        )

    def test_open_wifi_omits_credential_reference_and_security_section(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            environment = {
                "RP_YLX_NETWORK_STATE_DIR": str(root / "state"),
                "RP_YLX_NM_PROFILE_DIR": str(root / "profiles"),
                "RP_YLX_AVAHI_SERVICE_DIR": str(root / "avahi"),
            }
            from tests.test_network import FakeNmcli

            nmcli = FakeNmcli(root / "profiles")
            with (
                patch.dict(os.environ, environment, clear=False),
                patch("rp_ylx.network.subprocess.run", side_effect=nmcli),
            ):
                controller = NetworkController(device_id="device-open", require_root=False)
                try:
                    accepted = controller.handle(
                        {
                            "schema": "ylx.network-control-request.v1",
                            "operation": "apply",
                            "principal_id": "customer",
                            "idempotency_key": "open-1",
                            "body": {
                                "schema": "ylx.network-apply-request.v1",
                                "desired": {
                                    "mode": "wifi-client",
                                    "wifi_client": {
                                        "ssid": "Guest",
                                        "security": "open",
                                    },
                                    "ethernet": None,
                                },
                            },
                        }
                    )
                    self.assertTrue(accepted["ok"])
                    self.assertEqual(
                        accepted["body"]["transaction"]["desired"]["wifi_client"],
                        {
                            "ssid": "Guest",
                            "security": "open",
                            "credential_state": "absent",
                        },
                    )
                    self.assertTrue(controller.wait_for_idle(timeout=2))
                    status = controller.handle(
                        {"schema": "ylx.network-control-request.v1", "operation": "status"}
                    )["body"]
                finally:
                    controller.close()

            profile = next((root / "profiles").glob("*wifi-client*.nmconnection"))
            rendered = profile.read_text(encoding="utf-8")
            self.assertNotIn("[wifi-security]", rendered)
            self.assertNotIn("psk=", rendered)
            self.assertEqual(status["transaction"]["latest"]["status"], "committed")

    def test_protected_wifi_requires_credential_reference(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            environment = {
                "RP_YLX_NETWORK_STATE_DIR": str(root / "state"),
                "RP_YLX_NM_PROFILE_DIR": str(root / "profiles"),
                "RP_YLX_AVAHI_SERVICE_DIR": str(root / "avahi"),
            }
            from tests.test_network import FakeNmcli

            with (
                patch.dict(os.environ, environment, clear=False),
                patch(
                    "rp_ylx.network.subprocess.run",
                    side_effect=FakeNmcli(root / "profiles"),
                ),
            ):
                controller = NetworkController(
                    device_id="device-protected",
                    start_worker=False,
                    require_root=False,
                )
                try:
                    response = controller.handle(
                        {
                            "schema": "ylx.network-control-request.v1",
                            "operation": "apply",
                            "principal_id": "customer",
                            "idempotency_key": "protected-1",
                            "body": {
                                "schema": "ylx.network-apply-request.v1",
                                "desired": {
                                    "mode": "wifi-client",
                                    "wifi_client": {
                                        "ssid": "Field LAN",
                                        "security": "wpa2-personal",
                                    },
                                    "ethernet": None,
                                },
                            },
                        }
                    )
                finally:
                    controller.close()

        self.assertFalse(response["ok"])
        self.assertEqual(response["error"]["code"], "request_invalid")

    def test_apply_consumes_credential_after_durable_accept_then_worker_commits(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            environment = {
                "RP_YLX_NETWORK_STATE_DIR": str(root / "state"),
                "RP_YLX_NM_PROFILE_DIR": str(root / "profiles"),
                "RP_YLX_AVAHI_SERVICE_DIR": str(root / "avahi"),
            }
            from tests.test_network import FakeNmcli

            nmcli = FakeNmcli(root / "profiles")
            credentials = NetworkCredentialStore(
                token_factory=lambda: "apply-once-001",
                ttl_seconds=60,
            )
            with (
                patch.dict(os.environ, environment, clear=False),
                patch("rp_ylx.network.subprocess.run", side_effect=nmcli),
            ):
                controller = NetworkController(
                    device_id="device-001",
                    credential_store=credentials,
                    start_worker=False,
                    require_root=False,
                )
                try:
                    created = controller.handle(
                        {
                            "schema": "ylx.network-control-request.v1",
                            "operation": "create_credential",
                            "principal_id": "customer",
                            "body": {
                                "schema": "ylx.network-credential-request.v1",
                                "passphrase": "client-secret-123",
                            },
                        }
                    )
                    credential_receipt = created["body"]
                    credential_ref = credential_receipt["credential_ref"]
                    self.assertTrue(credential_ref.startswith("cred-"))
                    self.assertEqual(
                        set(credential_receipt),
                        {
                            "schema",
                            "credential_ref",
                            "issued_at",
                            "expires_at",
                            "ttl_seconds",
                            "single_use",
                        },
                    )
                    self.assertEqual(
                        credential_receipt["schema"],
                        "ylx.network-credential-receipt.v1",
                    )
                    self.assertTrue(credential_receipt["single_use"])
                    self.assertNotIn("client-secret-123", json.dumps(created))

                    accepted = controller.handle(
                        {
                            "schema": "ylx.network-control-request.v1",
                            "operation": "apply",
                            "principal_id": "customer",
                            "idempotency_key": "apply-1",
                            "body": {
                                "schema": "ylx.network-apply-request.v1",
                                "desired": {
                                    "mode": "wifi-client",
                                    "wifi_client": {
                                        "ssid": "Field LAN",
                                        "security": "wpa2-personal",
                                        "credential_ref": credential_ref,
                                    },
                                    "ethernet": None,
                                },
                            },
                        }
                    )

                    self.assertTrue(accepted["ok"])
                    self.assertEqual(accepted["status"], 202)
                    self.assertFalse(accepted["replayed"])
                    receipt = accepted["body"]
                    self.assertEqual(receipt["transaction"]["status"], "accepted")
                    self.assertEqual(
                        receipt["transaction"]["desired"]["wifi_client"],
                        {
                            "ssid": "Field LAN",
                            "security": "wpa2-personal",
                            "credential_state": "pending_input",
                        },
                    )
                    self.assertEqual(receipt["transaction"]["source_revision"], 1)
                    self.assertIsNone(receipt["transaction"]["deadline"])
                    self.assertEqual(receipt["transaction"]["recovery_action"], "await_device")

                    status_before = controller.handle(
                        {
                            "schema": "ylx.network-control-request.v1",
                            "operation": "status",
                        }
                    )
                    self.assertTrue(status_before["body"]["capability"]["enabled"])
                    self.assertIsNone(status_before["body"]["capability"]["disabled_reason"])
                    self.assertTrue(status_before["body"]["saved"])
                    self.assertFalse(status_before["body"]["verified"])
                    self.assertEqual(
                        status_before["body"]["transaction"]["current"]["status"],
                        "accepted",
                    )
                    state_path = root / "state" / "controller-state.json"
                    state_before = state_path.read_text(encoding="utf-8")
                    self.assertNotIn("client-secret-123", state_before)
                    self.assertNotIn(credential_ref, state_before)

                    client_profile = next((root / "profiles").glob("*wifi-client*.nmconnection"))
                    self.assertEqual(client_profile.stat().st_mode & 0o777, 0o600)
                    self.assertIn("psk=client-secret-123", client_profile.read_text())
                    self.assertIn("autoconnect=false", client_profile.read_text())

                    controller.start()
                    self.assertTrue(controller.wait_for_idle(timeout=2))
                    status_after = controller.handle(
                        {
                            "schema": "ylx.network-control-request.v1",
                            "operation": "status",
                        }
                    )["body"]
                finally:
                    controller.close()

        self.assertIsNone(status_after["transaction"]["current"])
        self.assertEqual(status_after["transaction"]["latest"]["status"], "committed")
        self.assertEqual(status_after["transaction"]["latest"]["stage"], "committed")
        self.assertEqual(
            status_after["transaction"]["latest"]["desired"]["wifi_client"],
            {
                "ssid": "Field LAN",
                "security": "wpa2-personal",
                "credential_state": "stored",
            },
        )
        self.assertIsNone(status_after["transaction"]["latest"]["deadline"])
        self.assertEqual(
            status_after["transaction"]["latest"]["recovery_action"],
            "reconnect_target_lan",
        )
        self.assertTrue(status_after["saved"])
        self.assertTrue(status_after["verified"])
        self.assertGreater(
            status_after["source_revision"],
            status_before["body"]["source_revision"],
        )
        self.assertTrue(nmcli.active["wlan0"].startswith("rp-ylx-wifi-client-"))

    def test_failed_durable_accept_releases_credential_and_removes_candidate(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            environment = {
                "RP_YLX_NETWORK_STATE_DIR": str(root / "state"),
                "RP_YLX_NM_PROFILE_DIR": str(root / "profiles"),
                "RP_YLX_AVAHI_SERVICE_DIR": str(root / "avahi"),
            }
            from tests.test_network import FakeNmcli

            nmcli = FakeNmcli(root / "profiles")
            credentials = NetworkCredentialStore(token_factory=lambda: "durable-accept")
            with (
                patch.dict(os.environ, environment, clear=False),
                patch("rp_ylx.network.subprocess.run", side_effect=nmcli),
            ):
                controller = NetworkController(
                    device_id="device-durable-accept",
                    credential_store=credentials,
                    start_worker=False,
                    require_root=False,
                )
                try:
                    credential_ref = self._create_credential(controller, "durable-accept-secret")
                    with patch.object(
                        controller._state,
                        "accept_transaction",
                        side_effect=OSError("state disk full"),
                    ):
                        failed = self._apply_protected_wifi(
                            controller,
                            credential_ref,
                            idempotency_key="durable-accept-failed",
                        )
                    profiles_after_failure = list(
                        (root / "profiles").glob("*wifi-client*.nmconnection")
                    )
                    accepted = self._apply_protected_wifi(
                        controller,
                        credential_ref,
                        idempotency_key="durable-accept-retry",
                    )
                finally:
                    controller.close()

        self.assertFalse(failed["ok"])
        self.assertEqual(profiles_after_failure, [])
        self.assertTrue(accepted["ok"], accepted)

    def test_restarted_controller_resumes_durable_accepted_candidate(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            environment = {
                "RP_YLX_NETWORK_STATE_DIR": str(root / "state"),
                "RP_YLX_NM_PROFILE_DIR": str(root / "profiles"),
                "RP_YLX_AVAHI_SERVICE_DIR": str(root / "avahi"),
            }
            from tests.test_network import FakeNmcli

            nmcli = FakeNmcli(root / "profiles")
            credentials = NetworkCredentialStore(token_factory=lambda: "restart-once")
            with (
                patch.dict(os.environ, environment, clear=False),
                patch("rp_ylx.network.subprocess.run", side_effect=nmcli),
            ):
                first = NetworkController(
                    device_id="device-restart",
                    credential_store=credentials,
                    start_worker=False,
                    require_root=False,
                )
                credential_ref = self._create_credential(first, "restart-client-secret")
                accepted = self._apply_protected_wifi(
                    first,
                    credential_ref,
                    idempotency_key="restart-apply",
                )
                transaction_id = accepted["body"]["transaction"]["transaction_id"]
                first.close()

                restarted = NetworkController(device_id="device-restart", require_root=False)
                try:
                    self.assertTrue(restarted.wait_for_idle(timeout=2))
                    status = restarted.handle(
                        {"schema": "ylx.network-control-request.v1", "operation": "status"}
                    )["body"]
                finally:
                    restarted.close()

            persisted = (root / "state" / "controller-state.json").read_text(encoding="utf-8")

        self.assertTrue(accepted["ok"])
        self.assertIsNone(status["transaction"]["current"])
        self.assertEqual(status["transaction"]["latest"]["transaction_id"], transaction_id)
        self.assertEqual(status["transaction"]["latest"]["status"], "committed")
        self.assertNotIn("restart-client-secret", persisted)

    def test_restarted_deferred_controller_waits_for_replayed_receipt_release(self) -> None:
        request = {
            "schema": "ylx.network-control-request.v1",
            "operation": "forget",
            "principal_id": "customer",
            "idempotency_key": "deferred-restart-forget",
            "body": {"schema": "ylx.network-forget-request.v1"},
        }
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            environment = {
                "RP_YLX_NETWORK_STATE_DIR": str(root / "state"),
                "RP_YLX_NM_PROFILE_DIR": str(root / "profiles"),
                "RP_YLX_AVAHI_SERVICE_DIR": str(root / "avahi"),
            }
            from tests.test_network import FakeNmcli

            nmcli = FakeNmcli(root / "profiles")
            with (
                patch.dict(os.environ, environment, clear=False),
                patch("rp_ylx.network.subprocess.run", side_effect=nmcli),
            ):
                first = NetworkController(
                    device_id="device-deferred-restart",
                    start_worker=False,
                    require_root=False,
                    defer_execution_until_response=True,
                )
                accepted = first.handle(request)
                first.close()

                restarted = NetworkController(
                    device_id="device-deferred-restart",
                    start_worker=False,
                    require_root=False,
                    defer_execution_until_response=True,
                )
                try:
                    restarted.start()
                    self.assertFalse(restarted.wait_for_idle(timeout=0.05))
                    waiting = restarted.handle(
                        {"schema": "ylx.network-control-request.v1", "operation": "status"}
                    )["body"]
                    replayed = restarted.handle(request)
                    restarted.release_response(replayed)
                    self.assertTrue(restarted.wait_for_idle(timeout=2))
                    completed = restarted.handle(
                        {"schema": "ylx.network-control-request.v1", "operation": "status"}
                    )["body"]
                finally:
                    restarted.close()

        self.assertTrue(accepted["ok"])
        self.assertEqual(waiting["transaction"]["current"]["status"], "accepted")
        self.assertTrue(replayed["replayed"])
        self.assertEqual(completed["transaction"]["latest"]["status"], "committed")

    def test_failed_client_returns_to_rescue_and_retry_reuses_retained_candidate(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            environment = {
                "RP_YLX_NETWORK_STATE_DIR": str(root / "state"),
                "RP_YLX_NM_PROFILE_DIR": str(root / "profiles"),
                "RP_YLX_AVAHI_SERVICE_DIR": str(root / "avahi"),
            }
            from tests.test_network import FakeNmcli

            nmcli = FakeNmcli(root / "profiles")
            nmcli.fail_modes["wifi-client"] = "Secrets were required, but not provided"
            credentials = NetworkCredentialStore(token_factory=lambda: "retry-001")
            with (
                patch.dict(os.environ, environment, clear=False),
                patch("rp_ylx.network.subprocess.run", side_effect=nmcli),
            ):
                controller = NetworkController(
                    device_id="device-001",
                    credential_store=credentials,
                    require_root=False,
                )
                try:
                    credential_ref = controller.handle(
                        {
                            "schema": "ylx.network-control-request.v1",
                            "operation": "create_credential",
                            "principal_id": "customer",
                            "body": {
                                "schema": "ylx.network-credential-request.v1",
                                "passphrase": "wrong-client-secret",
                            },
                        }
                    )["body"]["credential_ref"]
                    accepted = controller.handle(
                        {
                            "schema": "ylx.network-control-request.v1",
                            "operation": "apply",
                            "principal_id": "customer",
                            "idempotency_key": "bad-apply",
                            "body": {
                                "schema": "ylx.network-apply-request.v1",
                                "desired": {
                                    "mode": "wifi-client",
                                    "wifi_client": {
                                        "ssid": "Field LAN",
                                        "security": "wpa2-personal",
                                        "credential_ref": credential_ref,
                                    },
                                    "ethernet": None,
                                },
                            },
                        }
                    )
                    original_id = accepted["body"]["transaction"]["transaction_id"]
                    self.assertTrue(controller.wait_for_idle(timeout=2))

                    failed = controller.handle(
                        {
                            "schema": "ylx.network-control-request.v1",
                            "operation": "status",
                        }
                    )["body"]
                    rescue_profile = json.loads(
                        (root / "state" / "rescue.json").read_text(encoding="utf-8")
                    )["profile"]
                    candidate = next((root / "profiles").glob("*wifi-client*.nmconnection"))
                    self.assertEqual(failed["desired"]["mode"], "wifi-client")
                    self.assertIsNone(failed["transaction"]["current"])
                    self.assertEqual(failed["transaction"]["latest"]["status"], "rescued")
                    self.assertEqual(
                        failed["transaction"]["latest"]["error"]["code"],
                        "credential_rejected",
                    )
                    self.assertEqual(nmcli.active["wlan0"], rescue_profile)
                    self.assertTrue(candidate.exists())

                    nmcli.fail_modes.clear()
                    retried = controller.handle(
                        {
                            "schema": "ylx.network-control-request.v1",
                            "operation": "retry",
                            "principal_id": "customer",
                            "idempotency_key": "retry-1",
                            "body": {
                                "schema": "ylx.network-retry-request.v1",
                                "transaction_id": original_id,
                            },
                        }
                    )
                    self.assertTrue(retried["ok"])
                    self.assertEqual(retried["body"]["transaction"]["operation"], "retry")
                    self.assertTrue(controller.wait_for_idle(timeout=2))
                    committed = controller.handle(
                        {
                            "schema": "ylx.network-control-request.v1",
                            "operation": "status",
                        }
                    )["body"]
                    persisted_state = (root / "state" / "controller-state.json").read_text(
                        encoding="utf-8"
                    )
                finally:
                    controller.close()

        self.assertEqual(committed["transaction"]["latest"]["status"], "committed")
        self.assertEqual(committed["transaction"]["latest"]["operation"], "retry")
        self.assertEqual(nmcli.active["wlan0"], candidate.stem)
        self.assertNotIn("wrong-client-secret", persisted_state)
        self.assertNotIn(credential_ref, persisted_state)

    def test_ten_second_activation_timeout_falls_back_and_retains_candidate(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            environment = {
                "RP_YLX_NETWORK_STATE_DIR": str(root / "state"),
                "RP_YLX_NM_PROFILE_DIR": str(root / "profiles"),
                "RP_YLX_AVAHI_SERVICE_DIR": str(root / "avahi"),
            }
            from tests.test_network import FakeNmcli

            nmcli = FakeNmcli(root / "profiles")
            nmcli.timeout_modes.add("wifi-client")
            credentials = NetworkCredentialStore(token_factory=lambda: "timeout-001")
            with (
                patch.dict(os.environ, environment, clear=False),
                patch("rp_ylx.network.subprocess.run", side_effect=nmcli),
            ):
                controller = NetworkController(
                    device_id="device-timeout",
                    credential_store=credentials,
                    require_root=False,
                )
                nmcli.commands.clear()
                try:
                    credential_ref = controller.handle(
                        {
                            "schema": "ylx.network-control-request.v1",
                            "operation": "create_credential",
                            "principal_id": "customer",
                            "body": {
                                "schema": "ylx.network-credential-request.v1",
                                "passphrase": "timeout-secret-123",
                            },
                        }
                    )["body"]["credential_ref"]
                    accepted = controller.handle(
                        {
                            "schema": "ylx.network-control-request.v1",
                            "operation": "apply",
                            "principal_id": "customer",
                            "idempotency_key": "timeout-apply",
                            "body": {
                                "schema": "ylx.network-apply-request.v1",
                                "desired": {
                                    "mode": "wifi-client",
                                    "wifi_client": {
                                        "ssid": "Slow LAN",
                                        "security": "wpa2-personal",
                                        "credential_ref": credential_ref,
                                    },
                                    "ethernet": None,
                                },
                            },
                        }
                    )
                    self.assertTrue(accepted["ok"])
                    self.assertTrue(controller.wait_for_idle(timeout=2))
                    status = controller.handle(
                        {"schema": "ylx.network-control-request.v1", "operation": "status"}
                    )["body"]
                    rescue_profile = json.loads(
                        (root / "state" / "rescue.json").read_text(encoding="utf-8")
                    )["profile"]
                    candidates = list((root / "profiles").glob("*wifi-client*.nmconnection"))
                finally:
                    controller.close()

        latest = status["transaction"]["latest"]
        self.assertEqual(latest["status"], "rescued")
        self.assertEqual(latest["stage"], "rescued")
        self.assertEqual(latest["error"]["code"], "dhcp_timeout")
        self.assertIsNone(latest["deadline"])
        self.assertEqual(latest["recovery_action"], "reconnect_rescue_ap")
        self.assertEqual(status["desired"]["wifi_client"]["ssid"], "Slow LAN")
        self.assertEqual(len(candidates), 1)
        self.assertEqual(nmcli.active["wlan0"], rescue_profile)
        rescue_activations = [
            command
            for command in nmcli.commands
            if "connection" in command and "up" in command and rescue_profile in command
        ]
        self.assertEqual(len(rescue_activations), 2)

    def test_activation_deadline_includes_snapshot_and_is_strict_at_ten_seconds(self) -> None:
        from tests.test_network import FakeNmcli

        for boundary_ns, expected_status in (
            (9_900_000_000, "committed"),
            (10_000_000_000, "rescued"),
        ):
            with self.subTest(boundary_ns=boundary_ns), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                environment = {
                    "RP_YLX_NETWORK_STATE_DIR": str(root / "state"),
                    "RP_YLX_NM_PROFILE_DIR": str(root / "profiles"),
                    "RP_YLX_AVAHI_SERVICE_DIR": str(root / "avahi"),
                }
                clock = {"now": 0}
                nmcli = FakeNmcli(root / "profiles")

                def timed_nmcli(
                    command: list[str],
                    _nmcli: FakeNmcli = nmcli,
                    _clock: dict[str, int] = clock,
                    _boundary_ns: int = boundary_ns,
                    **kwargs: object,
                ) -> object:
                    result = _nmcli(command, **kwargs)
                    if "connection" in command and "up" in command:
                        profile = command[command.index("id") + 1]
                        if "-wifi-client-" in profile:
                            _clock["now"] = 9_800_000_000
                    elif command[-3:-1] == ["device", "show"]:
                        interface = command[-1]
                        if "-wifi-client-" in _nmcli.active[interface]:
                            _clock["now"] = _boundary_ns
                    return result

                with (
                    patch.dict(os.environ, environment, clear=False),
                    patch("rp_ylx.network.subprocess.run", side_effect=timed_nmcli),
                ):
                    controller = NetworkController(
                        device_id=f"deadline-{boundary_ns}",
                        require_root=False,
                        monotonic_ns=lambda clock=clock: clock["now"],
                    )
                    try:
                        accepted = self._apply_open_wifi(
                            controller,
                            idempotency_key=f"deadline-{boundary_ns}",
                        )
                        self.assertTrue(accepted["ok"], accepted)
                        self.assertTrue(controller.wait_for_idle(timeout=2))
                        latest = controller.handle(
                            {"schema": "ylx.network-control-request.v1", "operation": "status"}
                        )["body"]["transaction"]["latest"]
                    finally:
                        controller.close()

                self.assertEqual(latest["status"], expected_status)
                if boundary_ns == 10_000_000_000:
                    self.assertEqual(latest["error"]["code"], "dhcp_timeout")

    def test_candidate_failure_restores_rescue_strictly_before_fifteen_seconds(self) -> None:
        from tests.test_network import FakeNmcli

        for rescue_finished_ns, expected_status in (
            (14_900_000_000, "rescued"),
            (15_000_000_000, "failed"),
        ):
            with (
                self.subTest(rescue_finished_ns=rescue_finished_ns),
                tempfile.TemporaryDirectory() as directory,
            ):
                root = Path(directory)
                environment = {
                    "RP_YLX_NETWORK_STATE_DIR": str(root / "state"),
                    "RP_YLX_NM_PROFILE_DIR": str(root / "profiles"),
                    "RP_YLX_AVAHI_SERVICE_DIR": str(root / "avahi"),
                }
                clock = {"now": 0}
                candidate_failed = {"value": False}
                rescue_timeouts: list[float] = []
                nmcli = FakeNmcli(root / "profiles")
                nmcli.timeout_modes.add("wifi-client")

                def timed_nmcli(
                    command: list[str],
                    _candidate_failed: dict[str, bool] = candidate_failed,
                    _rescue_timeouts: list[float] = rescue_timeouts,
                    _nmcli: FakeNmcli = nmcli,
                    _clock: dict[str, int] = clock,
                    _rescue_finished_ns: int = rescue_finished_ns,
                    **kwargs: object,
                ) -> object:
                    profile = (
                        command[command.index("id") + 1]
                        if "connection" in command and "up" in command
                        else ""
                    )
                    if _candidate_failed["value"] and "-hotspot-" in profile:
                        _rescue_timeouts.append(float(kwargs["timeout"]))
                        result = _nmcli(command, **kwargs)
                        _clock["now"] = _rescue_finished_ns
                        return result
                    try:
                        return _nmcli(command, **kwargs)
                    except subprocess.TimeoutExpired:
                        if "-wifi-client-" in profile:
                            _candidate_failed["value"] = True
                            _clock["now"] = 10_000_000_000
                        raise

                with (
                    patch.dict(os.environ, environment, clear=False),
                    patch("rp_ylx.network.subprocess.run", side_effect=timed_nmcli),
                ):
                    controller = NetworkController(
                        device_id=f"rescue-deadline-{rescue_finished_ns}",
                        require_root=False,
                        monotonic_ns=lambda clock=clock: clock["now"],
                    )
                    try:
                        accepted = self._apply_open_wifi(
                            controller,
                            idempotency_key=f"rescue-deadline-{rescue_finished_ns}",
                        )
                        self.assertTrue(accepted["ok"], accepted)
                        self.assertTrue(controller.wait_for_idle(timeout=2))
                        latest = controller.handle(
                            {"schema": "ylx.network-control-request.v1", "operation": "status"}
                        )["body"]["transaction"]["latest"]
                    finally:
                        controller.close()

                self.assertEqual(latest["status"], expected_status)
                self.assertEqual(latest["error"]["code"], "dhcp_timeout")
                self.assertEqual(rescue_timeouts, [5.0])

    def test_link_local_dhcp_address_is_not_accepted_as_client_health(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            environment = {
                "RP_YLX_NETWORK_STATE_DIR": str(root / "state"),
                "RP_YLX_NM_PROFILE_DIR": str(root / "profiles"),
                "RP_YLX_AVAHI_SERVICE_DIR": str(root / "avahi"),
            }
            from tests.test_network import FakeNmcli

            nmcli = FakeNmcli(root / "profiles")
            nmcli.dhcp_address_override = "169.254.20.30/16"
            with (
                patch.dict(os.environ, environment, clear=False),
                patch("rp_ylx.network.subprocess.run", side_effect=nmcli),
            ):
                controller = NetworkController(device_id="device-link-local", require_root=False)
                try:
                    accepted = self._apply_open_wifi(
                        controller,
                        idempotency_key="link-local-apply",
                    )
                    self.assertTrue(accepted["ok"], accepted)
                    self.assertTrue(controller.wait_for_idle(timeout=2))
                    latest = controller.handle(
                        {"schema": "ylx.network-control-request.v1", "operation": "status"}
                    )["body"]["transaction"]["latest"]
                finally:
                    controller.close()

        self.assertEqual(latest["status"], "rescued")
        self.assertEqual(latest["error"]["code"], "dhcp_timeout")

    def test_health_fallback_with_missing_profile_is_a_durable_transaction(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            environment = {
                "RP_YLX_NETWORK_STATE_DIR": str(root / "state"),
                "RP_YLX_NM_PROFILE_DIR": str(root / "profiles"),
                "RP_YLX_AVAHI_SERVICE_DIR": str(root / "avahi"),
            }
            from tests.test_network import FakeNmcli

            nmcli = FakeNmcli(root / "profiles")
            clock = {"now": 0}
            with (
                patch.dict(os.environ, environment, clear=False),
                patch("rp_ylx.network.subprocess.run", side_effect=nmcli),
            ):
                controller = NetworkController(
                    device_id="device-health-missing-profile",
                    require_root=False,
                    health_poll_seconds=3600,
                    monotonic_ns=lambda: clock["now"],
                )
                try:
                    applied = self._apply_open_wifi(
                        controller,
                        idempotency_key="health-missing-profile-apply",
                    )
                    self.assertTrue(applied["ok"], applied)
                    self.assertTrue(controller.wait_for_idle(timeout=2))
                    applied_id = applied["body"]["transaction"]["transaction_id"]
                    next((root / "profiles").glob("*wifi-client*.nmconnection")).unlink()
                    nmcli.active["wlan0"] = ""
                    nmcli.addresses["wlan0"] = ""
                    nmcli.routes["wlan0"] = ""

                    clock["now"] = 1_000_000_000
                    controller._monitor_once()
                    clock["now"] = 11_000_000_000
                    controller._monitor_once()
                    self.assertTrue(controller.wait_for_idle(timeout=2))
                    status = controller.handle(
                        {"schema": "ylx.network-control-request.v1", "operation": "status"}
                    )["body"]
                finally:
                    controller.close()

        latest = status["transaction"]["latest"]
        self.assertNotEqual(latest["transaction_id"], applied_id)
        self.assertEqual(latest["operation"], "retry")
        self.assertEqual(latest["status"], "rescued")
        self.assertEqual(latest["error"]["code"], "network_manager_unavailable")

    def test_startup_removes_orphan_candidate_but_keeps_durable_work(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            environment = {
                "RP_YLX_NETWORK_STATE_DIR": str(root / "state"),
                "RP_YLX_NM_PROFILE_DIR": str(root / "profiles"),
                "RP_YLX_AVAHI_SERVICE_DIR": str(root / "avahi"),
            }
            from tests.test_network import FakeNmcli

            nmcli = FakeNmcli(root / "profiles")
            with (
                patch.dict(os.environ, environment, clear=False),
                patch("rp_ylx.network.subprocess.run", side_effect=nmcli),
            ):
                first = NetworkController(
                    device_id="device-orphan-cleanup",
                    start_worker=False,
                    require_root=False,
                )
                first.close()
                orphan = network_module.prepare_network_candidate(
                    "orphan-after-prepare",
                    {"mode": "wifi-client", "ssid": "Orphan LAN", "security": "open"},
                )
                orphan_path = root / "profiles" / f"{orphan['profile']}.nmconnection"
                self.assertTrue(orphan_path.exists())

                restarted = NetworkController(
                    device_id="device-orphan-cleanup",
                    start_worker=False,
                    require_root=False,
                )
                try:
                    self.assertFalse(orphan_path.exists())
                finally:
                    restarted.close()

    def test_close_joins_worker_and_monitor_without_timeout(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            environment = {
                "RP_YLX_NETWORK_STATE_DIR": str(root / "state"),
                "RP_YLX_NM_PROFILE_DIR": str(root / "profiles"),
                "RP_YLX_AVAHI_SERVICE_DIR": str(root / "avahi"),
            }
            from tests.test_network import FakeNmcli

            with (
                patch.dict(os.environ, environment, clear=False),
                patch(
                    "rp_ylx.network.subprocess.run",
                    side_effect=FakeNmcli(root / "profiles"),
                ),
            ):
                controller = NetworkController(
                    device_id="device-close-joins",
                    start_worker=False,
                    require_root=False,
                )
                worker = Mock()
                monitor = Mock()
                controller._thread = worker
                controller._monitor_thread = monitor
                controller.close()

        worker.join.assert_called_once_with()
        monitor.join.assert_called_once_with()

    def test_capture_lease_rejects_mutation_and_pauses_health_monitor(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            environment = {
                "RP_YLX_NETWORK_STATE_DIR": str(root / "state"),
                "RP_YLX_NM_PROFILE_DIR": str(root / "profiles"),
                "RP_YLX_AVAHI_SERVICE_DIR": str(root / "avahi"),
            }
            from tests.test_network import FakeNmcli

            nmcli = FakeNmcli(root / "profiles")
            lease_acquired = threading.Event()
            release_lease = threading.Event()

            def hold_capture_lease() -> None:
                with network_module.network_operation_lease():
                    lease_acquired.set()
                    release_lease.wait(timeout=2)

            with (
                patch.dict(os.environ, environment, clear=False),
                patch("rp_ylx.network.subprocess.run", side_effect=nmcli),
            ):
                controller = NetworkController(
                    device_id="device-capture-lease",
                    require_root=False,
                    health_poll_seconds=3600,
                )
                try:
                    applied = self._apply_open_wifi(
                        controller,
                        idempotency_key="capture-lease-setup",
                    )
                    self.assertTrue(applied["ok"], applied)
                    self.assertTrue(controller.wait_for_idle(timeout=2))
                    nmcli.active["wlan0"] = ""
                    nmcli.addresses["wlan0"] = ""
                    nmcli.routes["wlan0"] = ""
                    controller._health_failure_started_ns = 1

                    holder = threading.Thread(target=hold_capture_lease)
                    holder.start()
                    self.assertTrue(lease_acquired.wait(timeout=1))
                    replayed = self._apply_open_wifi(
                        controller,
                        idempotency_key="capture-lease-setup",
                    )
                    conflict = self._apply_open_wifi(
                        controller,
                        idempotency_key="capture-lease-setup",
                        ssid="Conflicting LAN",
                    )
                    rejected = self._apply_open_wifi(
                        controller,
                        idempotency_key="capture-lease-rejected",
                        ssid="Blocked LAN",
                    )
                    blocked_scan = controller.handle(
                        {"schema": "ylx.network-control-request.v1", "operation": "scan"}
                    )
                    controller._monitor_once(now_ns=20_000_000_000)
                    during_capture = controller.handle(
                        {"schema": "ylx.network-control-request.v1", "operation": "status"}
                    )["body"]
                finally:
                    release_lease.set()
                    if "holder" in locals():
                        holder.join(timeout=2)
                    controller.close()

        self.assertFalse(holder.is_alive())
        self.assertTrue(replayed["ok"])
        self.assertTrue(replayed["replayed"])
        self.assertFalse(conflict["ok"])
        self.assertEqual(conflict["error"]["code"], "idempotency_conflict")
        self.assertFalse(rejected["ok"])
        self.assertEqual(rejected["error"]["code"], "capture_active")
        self.assertFalse(blocked_scan["ok"])
        self.assertEqual(blocked_scan["error"]["code"], "capture_active")
        self.assertIsNone(controller._health_failure_started_ns)
        self.assertEqual(
            during_capture["transaction"]["latest"]["transaction_id"],
            applied["body"]["transaction"]["transaction_id"],
        )

    def test_scan_returns_only_redacted_contract_fields_and_advances_revision(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            environment = {
                "RP_YLX_NETWORK_STATE_DIR": str(root / "state"),
                "RP_YLX_NM_PROFILE_DIR": str(root / "profiles"),
                "RP_YLX_AVAHI_SERVICE_DIR": str(root / "avahi"),
            }
            from tests.test_network import FakeNmcli

            nmcli = FakeNmcli(root / "profiles")
            nmcli.scan_output = "Guest::70\nField LAN:WPA2:50\n"
            with (
                patch.dict(os.environ, environment, clear=False),
                patch("rp_ylx.network.subprocess.run", side_effect=nmcli),
            ):
                controller = NetworkController(
                    device_id="device-scan",
                    start_worker=False,
                    require_root=False,
                )
                try:
                    first = controller.handle(
                        {"schema": "ylx.network-control-request.v1", "operation": "scan"}
                    )
                    second = controller.handle(
                        {"schema": "ylx.network-control-request.v1", "operation": "scan"}
                    )
                finally:
                    controller.close()

        self.assertTrue(first["ok"])
        self.assertEqual(first["status"], 200)
        self.assertEqual(first["body"]["schema"], "ylx.network-scan.v1")
        self.assertEqual(first["body"]["source_revision"], 1)
        self.assertEqual(second["body"]["source_revision"], 2)
        self.assertEqual(first["body"]["authority_epoch"], second["body"]["authority_epoch"])
        for network in first["body"]["networks"]:
            self.assertEqual(
                set(network),
                {"ssid", "hidden", "security", "signal_dbm", "credential_required"},
            )
        rendered = json.dumps(first)
        self.assertNotIn("bssid", rendered.lower())
        self.assertNotIn("passphrase", rendered.lower())

    def test_forget_validates_rescue_then_erases_clients_lkg_work_and_credentials(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            environment = {
                "RP_YLX_NETWORK_STATE_DIR": str(root / "state"),
                "RP_YLX_NM_PROFILE_DIR": str(root / "profiles"),
                "RP_YLX_AVAHI_SERVICE_DIR": str(root / "avahi"),
            }
            from tests.test_network import FakeNmcli

            nmcli = FakeNmcli(root / "profiles")
            first_credentials = NetworkCredentialStore(token_factory=lambda: "forget-apply")
            with (
                patch.dict(os.environ, environment, clear=False),
                patch("rp_ylx.network.subprocess.run", side_effect=nmcli),
            ):
                first_controller = NetworkController(
                    device_id="device-forget",
                    credential_store=first_credentials,
                    require_root=False,
                )
                try:
                    credential_ref = self._create_credential(
                        first_controller, "saved-client-secret"
                    )
                    setup_response = self._apply_protected_wifi(
                        first_controller,
                        credential_ref,
                        idempotency_key="forget-setup",
                    )
                    self.assertTrue(setup_response["ok"], setup_response)
                    self.assertTrue(first_controller.wait_for_idle(timeout=2))
                finally:
                    first_controller.close()

                credentials = NetworkCredentialStore(token_factory=lambda: "forget-unused")
                controller = NetworkController(
                    device_id="device-forget",
                    credential_store=credentials,
                    start_worker=False,
                    require_root=False,
                )
                try:
                    unused_ref = self._create_credential(controller, "unused-client-secret")
                    accepted = controller.handle(
                        {
                            "schema": "ylx.network-control-request.v1",
                            "operation": "forget",
                            "principal_id": "customer",
                            "idempotency_key": "forget-1",
                            "body": {"schema": "ylx.network-forget-request.v1"},
                        }
                    )
                    before = controller.handle(
                        {"schema": "ylx.network-control-request.v1", "operation": "status"}
                    )["body"]
                    self.assertTrue(accepted["ok"])
                    self.assertEqual(before["desired"]["mode"], "wifi-client")
                    self.assertEqual(before["transaction"]["current"]["desired"]["mode"], "hotspot")
                    controller.start()
                    self.assertTrue(controller.wait_for_idle(timeout=2))
                    after = controller.handle(
                        {"schema": "ylx.network-control-request.v1", "operation": "status"}
                    )["body"]
                    with (
                        self.assertRaises(NetworkCredentialError),
                        credentials.consume(unused_ref),
                    ):
                        pass
                    persisted = json.loads(
                        (root / "state" / "controller-state.json").read_text(encoding="utf-8")
                    )
                finally:
                    controller.close()

            rescue = json.loads((root / "state" / "rescue.json").read_text(encoding="utf-8"))
            client_profiles = list((root / "profiles").glob("*wifi-client*.nmconnection"))
            wifi_lkg_exists = (root / "state" / "lkg-wlan0.json").exists()

        self.assertEqual(after["desired"]["mode"], "hotspot")
        self.assertFalse(after["saved"])
        self.assertFalse(after["verified"])
        self.assertEqual(after["transaction"]["latest"]["operation"], "forget")
        self.assertEqual(after["transaction"]["latest"]["status"], "committed")
        self.assertEqual(after["transaction"]["latest"]["stage"], "forgotten")
        self.assertEqual(after["transaction"]["latest"]["recovery_action"], "reconnect_rescue_ap")
        self.assertEqual(client_profiles, [])
        self.assertFalse(wifi_lkg_exists)
        self.assertEqual(nmcli.active["wlan0"], rescue["profile"])
        self.assertEqual(persisted["work"], {})
        self.assertNotIn("unused-client-secret", json.dumps(persisted))

    def test_forget_failure_before_rescue_validation_preserves_client_state(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            environment = {
                "RP_YLX_NETWORK_STATE_DIR": str(root / "state"),
                "RP_YLX_NM_PROFILE_DIR": str(root / "profiles"),
                "RP_YLX_AVAHI_SERVICE_DIR": str(root / "avahi"),
            }
            from tests.test_network import FakeNmcli

            nmcli = FakeNmcli(root / "profiles")
            credentials = NetworkCredentialStore(
                token_factory=iter(["forget-fail-apply", "forget-fail-unused"]).__next__
            )
            with (
                patch.dict(os.environ, environment, clear=False),
                patch("rp_ylx.network.subprocess.run", side_effect=nmcli),
            ):
                controller = NetworkController(
                    device_id="device-forget-fail",
                    credential_store=credentials,
                    require_root=False,
                )
                try:
                    credential_ref = self._create_credential(controller, "saved-client-secret")
                    setup_response = self._apply_protected_wifi(
                        controller,
                        credential_ref,
                        idempotency_key="forget-fail-setup",
                    )
                    self.assertTrue(setup_response["ok"], setup_response)
                    self.assertTrue(controller.wait_for_idle(timeout=2))
                    unused_ref = self._create_credential(controller, "retry-client-secret")
                    nmcli.fail_modes["hotspot"] = "AP activation failed"
                    forgotten = controller.handle(
                        {
                            "schema": "ylx.network-control-request.v1",
                            "operation": "forget",
                            "principal_id": "customer",
                            "idempotency_key": "forget-fail",
                            "body": {"schema": "ylx.network-forget-request.v1"},
                        }
                    )
                    self.assertTrue(forgotten["ok"])
                    self.assertTrue(controller.wait_for_idle(timeout=2))
                    status = controller.handle(
                        {"schema": "ylx.network-control-request.v1", "operation": "status"}
                    )["body"]
                    with credentials.consume(unused_ref) as retained:
                        self.assertEqual(retained, "retry-client-secret")
                    client_profiles = list((root / "profiles").glob("*wifi-client*.nmconnection"))
                    lkg_exists = (root / "state" / "lkg-wlan0.json").exists()
                finally:
                    controller.close()

        self.assertEqual(status["desired"]["mode"], "wifi-client")
        self.assertTrue(status["saved"])
        self.assertTrue(status["verified"])
        self.assertEqual(status["transaction"]["latest"]["status"], "failed")
        self.assertEqual(
            status["transaction"]["latest"]["error"]["code"],
            "rescue_ap_unavailable",
        )
        self.assertEqual(len(client_profiles), 1)
        self.assertTrue(lkg_exists)

    def test_verified_client_health_uses_ten_second_fallback_and_explicit_retry(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            environment = {
                "RP_YLX_NETWORK_STATE_DIR": str(root / "state"),
                "RP_YLX_NM_PROFILE_DIR": str(root / "profiles"),
                "RP_YLX_AVAHI_SERVICE_DIR": str(root / "avahi"),
            }
            from tests.test_network import FakeNmcli

            nmcli = FakeNmcli(root / "profiles")
            clock = {"now": 0}
            credentials = NetworkCredentialStore(token_factory=lambda: "health-credential")
            with (
                patch.dict(os.environ, environment, clear=False),
                patch("rp_ylx.network.subprocess.run", side_effect=nmcli),
            ):
                controller = NetworkController(
                    device_id="device-health-monitor",
                    credential_store=credentials,
                    require_root=False,
                    health_poll_seconds=3600,
                    monotonic_ns=lambda: clock["now"],
                )
                try:
                    credential_ref = controller.handle(
                        {
                            "schema": "ylx.network-control-request.v1",
                            "operation": "create_credential",
                            "principal_id": "customer",
                            "body": {
                                "schema": "ylx.network-credential-request.v1",
                                "passphrase": "health-secret-123",
                            },
                        }
                    )["body"]["credential_ref"]
                    applied = controller.handle(
                        {
                            "schema": "ylx.network-control-request.v1",
                            "operation": "apply",
                            "principal_id": "customer",
                            "idempotency_key": "health-apply",
                            "body": {
                                "schema": "ylx.network-apply-request.v1",
                                "desired": {
                                    "mode": "wifi-client",
                                    "wifi_client": {
                                        "ssid": "Health LAN",
                                        "security": "wpa2-personal",
                                        "credential_ref": credential_ref,
                                    },
                                    "ethernet": None,
                                },
                            },
                        }
                    )
                    self.assertTrue(applied["ok"])
                    self.assertTrue(controller.wait_for_idle(timeout=2))

                    nmcli.active["wlan0"] = ""
                    nmcli.addresses["wlan0"] = ""
                    nmcli.routes["wlan0"] = ""
                    clock["now"] = 1_000_000_000
                    controller._monitor_once()
                    clock["now"] = 10_900_000_000
                    controller._monitor_once()
                    before_deadline = controller.handle(
                        {"schema": "ylx.network-control-request.v1", "operation": "status"}
                    )["body"]
                    self.assertIsNone(before_deadline["transaction"]["current"])

                    clock["now"] = 11_000_000_000
                    controller._monitor_once()
                    self.assertTrue(controller.wait_for_idle(timeout=2))
                    rescued = controller.handle(
                        {"schema": "ylx.network-control-request.v1", "operation": "status"}
                    )["body"]
                    latest = rescued["transaction"]["latest"]
                    self.assertEqual(latest["status"], "rescued")
                    self.assertEqual(latest["error"]["code"], "route_lost")
                    self.assertEqual(rescued["desired"]["mode"], "wifi-client")
                    self.assertTrue(rescued["saved"])
                    rescue_profile = json.loads(
                        (root / "state/rescue.json").read_text(encoding="utf-8")
                    )["profile"]
                    self.assertEqual(nmcli.active["wlan0"], rescue_profile)

                    retried = controller.handle(
                        {
                            "schema": "ylx.network-control-request.v1",
                            "operation": "retry",
                            "principal_id": "customer",
                            "idempotency_key": "health-retry",
                            "body": {
                                "schema": "ylx.network-retry-request.v1",
                                "transaction_id": latest["transaction_id"],
                            },
                        }
                    )
                    self.assertTrue(retried["ok"])
                    self.assertTrue(controller.wait_for_idle(timeout=2))
                    restored = controller.handle(
                        {"schema": "ylx.network-control-request.v1", "operation": "status"}
                    )["body"]
                finally:
                    controller.close()

        self.assertEqual(restored["transaction"]["latest"]["status"], "committed")
        self.assertEqual(restored["transaction"]["latest"]["operation"], "retry")
        self.assertNotIn("health-secret-123", json.dumps(restored))

    def test_boot_starts_rescue_then_restores_saved_client_without_restart_churn(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            boot_id_path = root / "boot-id"
            boot_id_path.write_text("11111111-1111-4111-8111-111111111111\n", encoding="utf-8")
            environment = {
                "RP_YLX_NETWORK_STATE_DIR": str(root / "state"),
                "RP_YLX_NM_PROFILE_DIR": str(root / "profiles"),
                "RP_YLX_AVAHI_SERVICE_DIR": str(root / "avahi"),
                "RP_YLX_BOOT_ID_PATH": str(boot_id_path),
            }
            from tests.test_network import FakeNmcli

            nmcli = FakeNmcli(root / "profiles")
            credentials = NetworkCredentialStore(token_factory=lambda: "boot-credential")
            with (
                patch.dict(os.environ, environment, clear=False),
                patch("rp_ylx.network.subprocess.run", side_effect=nmcli),
            ):
                first = NetworkController(
                    device_id="device-boot-reconcile",
                    credential_store=credentials,
                    require_root=False,
                    health_poll_seconds=3600,
                )
                try:
                    rescue_profile = json.loads(
                        (root / "state/rescue.json").read_text(encoding="utf-8")
                    )["profile"]
                    self.assertEqual(nmcli.active["wlan0"], rescue_profile)
                    credential_ref = first.handle(
                        {
                            "schema": "ylx.network-control-request.v1",
                            "operation": "create_credential",
                            "principal_id": "customer",
                            "body": {
                                "schema": "ylx.network-credential-request.v1",
                                "passphrase": "boot-secret-123",
                            },
                        }
                    )["body"]["credential_ref"]
                    first.handle(
                        {
                            "schema": "ylx.network-control-request.v1",
                            "operation": "apply",
                            "principal_id": "customer",
                            "idempotency_key": "boot-apply",
                            "body": {
                                "schema": "ylx.network-apply-request.v1",
                                "desired": {
                                    "mode": "wifi-client",
                                    "wifi_client": {
                                        "ssid": "Boot LAN",
                                        "security": "wpa2-personal",
                                        "credential_ref": credential_ref,
                                    },
                                    "ethernet": None,
                                },
                            },
                        }
                    )
                    self.assertTrue(first.wait_for_idle(timeout=2))
                    client_profile = nmcli.active["wlan0"]
                finally:
                    first.close()

                nmcli.commands.clear()
                restarted = NetworkController(
                    device_id="device-boot-reconcile",
                    start_worker=False,
                    require_root=False,
                )
                try:
                    self.assertEqual(nmcli.active["wlan0"], client_profile)
                    self.assertFalse(
                        any(
                            "connection" in command and "up" in command
                            for command in nmcli.commands
                        )
                    )
                finally:
                    restarted.close()

                boot_id_path.write_text(
                    "22222222-2222-4222-8222-222222222222\n",
                    encoding="utf-8",
                )
                nmcli.commands.clear()
                rebooted = NetworkController(
                    device_id="device-boot-reconcile",
                    start_worker=False,
                    require_root=False,
                )
                try:
                    status = rebooted.handle(
                        {"schema": "ylx.network-control-request.v1", "operation": "status"}
                    )["body"]
                    reboot_activations = [
                        command[command.index("id") + 1]
                        for command in nmcli.commands
                        if "connection" in command and "up" in command
                    ]
                finally:
                    rebooted.close()

        self.assertEqual(nmcli.active["wlan0"], client_profile)
        self.assertGreaterEqual(len(reboot_activations), 2)
        self.assertEqual(reboot_activations[0], rescue_profile)
        self.assertEqual(reboot_activations[-1], client_profile)
        self.assertEqual(status["transaction"]["latest"]["status"], "committed")
        self.assertEqual(status["transaction"]["latest"]["operation"], "retry")

    def test_reboot_retries_rescued_client_without_same_boot_retry_loop(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            boot_id_path = root / "boot-id"
            boot_id_path.write_text("11111111-1111-4111-8111-111111111111\n", encoding="utf-8")
            environment = {
                "RP_YLX_NETWORK_STATE_DIR": str(root / "state"),
                "RP_YLX_NM_PROFILE_DIR": str(root / "profiles"),
                "RP_YLX_AVAHI_SERVICE_DIR": str(root / "avahi"),
                "RP_YLX_BOOT_ID_PATH": str(boot_id_path),
            }
            from tests.test_network import FakeNmcli

            nmcli = FakeNmcli(root / "profiles")
            nmcli.fail_modes["wifi-client"] = "Secrets were required, but not provided"
            credentials = NetworkCredentialStore(token_factory=lambda: "reboot-credential")
            with (
                patch.dict(os.environ, environment, clear=False),
                patch("rp_ylx.network.subprocess.run", side_effect=nmcli),
            ):
                first = NetworkController(
                    device_id="device-reboot-retry",
                    credential_store=credentials,
                    require_root=False,
                    health_poll_seconds=3600,
                )
                try:
                    credential_ref = self._create_credential(first, "reboot-secret-123")
                    accepted = self._apply_protected_wifi(
                        first,
                        credential_ref,
                        idempotency_key="reboot-retry-apply",
                    )
                    original_id = accepted["body"]["transaction"]["transaction_id"]
                    self.assertTrue(first.wait_for_idle(timeout=2))
                    rescued = first.handle(
                        {"schema": "ylx.network-control-request.v1", "operation": "status"}
                    )["body"]
                    rescue_profile = json.loads(
                        (root / "state/rescue.json").read_text(encoding="utf-8")
                    )["profile"]
                    candidate_profile = next(
                        (root / "profiles").glob("*wifi-client*.nmconnection")
                    ).stem
                finally:
                    first.close()

                self.assertEqual(rescued["transaction"]["latest"]["status"], "rescued")
                self.assertEqual(nmcli.active["wlan0"], rescue_profile)
                nmcli.fail_modes.clear()
                nmcli.commands.clear()

                restarted = NetworkController(
                    device_id="device-reboot-retry",
                    start_worker=False,
                    require_root=False,
                )
                try:
                    same_boot = restarted.handle(
                        {"schema": "ylx.network-control-request.v1", "operation": "status"}
                    )["body"]
                    same_boot_activations = [
                        command[command.index("id") + 1]
                        for command in nmcli.commands
                        if "connection" in command and "up" in command
                    ]
                finally:
                    restarted.close()

                self.assertEqual(same_boot["transaction"]["latest"]["transaction_id"], original_id)
                self.assertNotIn(candidate_profile, same_boot_activations)
                self.assertEqual(nmcli.active["wlan0"], rescue_profile)

                boot_id_path.write_text(
                    "22222222-2222-4222-8222-222222222222\n",
                    encoding="utf-8",
                )
                nmcli.commands.clear()
                rebooted = NetworkController(
                    device_id="device-reboot-retry",
                    start_worker=False,
                    require_root=False,
                )
                try:
                    after_reboot = rebooted.handle(
                        {"schema": "ylx.network-control-request.v1", "operation": "status"}
                    )["body"]
                    reboot_activations = [
                        command[command.index("id") + 1]
                        for command in nmcli.commands
                        if "connection" in command and "up" in command
                    ]
                    with self.assertRaises(NetworkStateError):
                        rebooted._state.work_for(str(original_id))
                finally:
                    rebooted.close()

            persisted = (root / "state/controller-state.json").read_text(encoding="utf-8")

        self.assertGreaterEqual(len(reboot_activations), 2)
        self.assertEqual(reboot_activations[0], rescue_profile)
        self.assertEqual(reboot_activations[-1], candidate_profile)
        self.assertEqual(after_reboot["transaction"]["latest"]["status"], "committed")
        self.assertEqual(after_reboot["transaction"]["latest"]["operation"], "retry")
        self.assertNotEqual(after_reboot["transaction"]["latest"]["transaction_id"], original_id)
        self.assertNotIn("reboot-secret-123", persisted)

    def test_reboot_rebuilds_missing_retry_candidate_from_legacy_lkg(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            boot_id_path = root / "boot-id"
            boot_id_path.write_text("11111111-1111-4111-8111-111111111111\n", encoding="utf-8")
            environment = {
                "RP_YLX_NETWORK_STATE_DIR": str(root / "state"),
                "RP_YLX_NM_PROFILE_DIR": str(root / "profiles"),
                "RP_YLX_AVAHI_SERVICE_DIR": str(root / "avahi"),
                "RP_YLX_BOOT_ID_PATH": str(boot_id_path),
            }
            from tests.test_network import FakeNmcli

            nmcli = FakeNmcli(root / "profiles")
            credentials = NetworkCredentialStore(token_factory=lambda: "legacy-reboot-credential")
            clock = {"now": 0}
            with (
                patch.dict(os.environ, environment, clear=False),
                patch("rp_ylx.network.subprocess.run", side_effect=nmcli),
            ):
                first = NetworkController(
                    device_id="device-legacy-reboot-retry",
                    credential_store=credentials,
                    require_root=False,
                    health_poll_seconds=3600,
                    monotonic_ns=lambda: clock["now"],
                )
                try:
                    credential_ref = self._create_credential(first, "legacy-reboot-secret-123")
                    accepted = self._apply_protected_wifi(
                        first,
                        credential_ref,
                        idempotency_key="legacy-reboot-apply",
                    )
                    self.assertTrue(accepted["ok"])
                    self.assertTrue(first.wait_for_idle(timeout=2))
                    profile_path = next((root / "profiles").glob("*wifi-client*.nmconnection"))
                    profile_bytes = profile_path.read_bytes()

                    profile_path.unlink()
                    nmcli.active["wlan0"] = ""
                    nmcli.addresses["wlan0"] = ""
                    nmcli.routes["wlan0"] = ""
                    clock["now"] = 1_000_000_000
                    first._monitor_once()
                    clock["now"] = 11_000_000_000
                    first._monitor_once()
                    self.assertTrue(first.wait_for_idle(timeout=2))
                    rescued = first.handle(
                        {"schema": "ylx.network-control-request.v1", "operation": "status"}
                    )["body"]
                    failed_id = rescued["transaction"]["latest"]["transaction_id"]
                    failed_work = first._state.work_for(str(failed_id))
                finally:
                    first.close()

                self.assertEqual(rescued["transaction"]["latest"]["status"], "rescued")
                self.assertEqual(
                    rescued["transaction"]["latest"]["error"]["code"],
                    "network_manager_unavailable",
                )
                self.assertNotIn("candidate", failed_work)

                profile_path.write_bytes(profile_bytes)
                profile_path.chmod(0o600)
                lkg_path = root / "state/lkg-wlan0.json"
                lkg = json.loads(lkg_path.read_text(encoding="utf-8"))
                lkg["config"] = {"mode": "wifi-client", "ssid": "Field LAN"}
                lkg_path.write_text(json.dumps(lkg), encoding="utf-8")
                lkg_path.chmod(0o600)
                boot_id_path.write_text(
                    "22222222-2222-4222-8222-222222222222\n",
                    encoding="utf-8",
                )
                nmcli.commands.clear()

                rebooted = NetworkController(
                    device_id="device-legacy-reboot-retry",
                    start_worker=False,
                    require_root=False,
                )
                try:
                    restored = rebooted.handle(
                        {"schema": "ylx.network-control-request.v1", "operation": "status"}
                    )["body"]
                    with self.assertRaises(NetworkStateError):
                        rebooted._state.work_for(str(failed_id))
                finally:
                    rebooted.close()

            persisted = (root / "state/controller-state.json").read_text(encoding="utf-8")

        security_query = next(command for command in nmcli.commands if "--get-values" in command)
        activation = next(
            command
            for command in nmcli.commands
            if "connection" in command
            and "up" in command
            and command[command.index("id") + 1] == profile_path.stem
        )
        self.assertLess(nmcli.commands.index(security_query), nmcli.commands.index(activation))
        self.assertEqual(restored["transaction"]["latest"]["status"], "committed")
        self.assertEqual(restored["transaction"]["latest"]["operation"], "retry")
        self.assertEqual(nmcli.active["wlan0"], profile_path.stem)
        self.assertNotIn("legacy-reboot-secret-123", persisted)


class NetworkControlTest(unittest.TestCase):
    def setUp(self) -> None:
        self._operation_directory = tempfile.TemporaryDirectory()
        lock_path = Path(self._operation_directory.name) / "network-operation.lock"
        self._operation_environment = patch.dict(
            os.environ,
            {"RP_YLX_NETWORK_OPERATION_LOCK_PATH": str(lock_path)},
            clear=False,
        )
        self._operation_environment.start()

    def tearDown(self) -> None:
        self._operation_environment.stop()
        self._operation_directory.cleanup()

    def test_control_response_deadline_covers_root_scan_and_queue_margin(self) -> None:
        self.assertGreaterEqual(
            CONTROL_RESPONSE_TIMEOUT_SECONDS,
            network_module.NETWORK_ACTIVATION_TIMEOUT_SECONDS + 4 * CONTROL_SOCKET_TIMEOUT_SECONDS,
        )

    def _serve_once(self, socket_path: Path, response: bytes | None = None) -> threading.Thread:
        ready = threading.Event()

        def run() -> None:
            with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as server:
                server.bind(str(socket_path))
                server.listen(1)
                ready.set()
                connection, _ = server.accept()
                with connection:
                    payload = b""
                    while not payload.endswith(b"\n"):
                        chunk = connection.recv(4096)
                        if not chunk:
                            break
                        payload += chunk
                    rendered = (
                        json.dumps(handle_control_payload(payload.decode("utf-8"))).encode() + b"\n"
                        if response is None
                        else response
                    )
                    connection.sendall(rendered)

        thread = threading.Thread(target=run, daemon=True)
        thread.start()
        self.assertTrue(ready.wait(timeout=2))
        return thread

    def test_health_reports_mutation_disabled(self) -> None:
        response = handle_control_payload(
            json.dumps(
                {
                    "schema": "ylx.network-control-request.v1",
                    "operation": "health",
                }
            )
        )

        self.assertEqual(response["schema"], CONTROL_RESPONSE_SCHEMA)
        self.assertIs(response["ok"], True)
        self.assertEqual(response["operation"], "health")
        capabilities = response["capabilities"]
        self.assertEqual(capabilities["operations"], ["apply", "forget", "retry"])
        self.assertIs(capabilities["mutation_enabled"], False)
        self.assertEqual(
            capabilities["secret_handling"],
            "opaque_credential_reference_only",
        )

    def test_mutation_request_fails_closed_without_echoing_body(self) -> None:
        response = handle_control_payload(
            json.dumps(
                {
                    "schema": "ylx.network-control-request.v1",
                    "operation": "apply",
                    "principal_id": "customer",
                    "idempotency_key": "idem-1",
                    "body": {
                        "schema": "ylx.network-apply-request.v1",
                        "desired": {
                            "mode": "wifi-client",
                            "wifi_client": {
                                "ssid": "Field LAN",
                                "security": "wpa2-personal",
                                "credential_ref": "cred-network-field-lan",
                            },
                            "ethernet": None,
                        },
                    },
                }
            )
        )

        self.assertIs(response["ok"], False)
        self.assertEqual(response["operation"], "apply")
        self.assertEqual(response["error"]["code"], "network_controller_not_enabled")
        self.assertIs(response["retryable"], True)
        self.assertNotIn("Field LAN", json.dumps(response))
        self.assertNotIn("credential_ref", json.dumps(response))

    def test_inline_secret_is_rejected_before_not_enabled_response(self) -> None:
        response = handle_control_payload(
            json.dumps(
                {
                    "schema": "ylx.network-control-request.v1",
                    "operation": "apply",
                    "principal_id": "customer",
                    "idempotency_key": "idem-1",
                    "body": {
                        "schema": "ylx.network-apply-request.v1",
                        "desired": {
                            "mode": "wifi-client",
                            "wifi_client": {
                                "ssid": "Field LAN",
                                "security": "wpa2-personal",
                                "psk": "candidate-secret",
                            },
                            "ethernet": None,
                        },
                    },
                }
            )
        )

        self.assertIs(response["ok"], False)
        self.assertEqual(response["error"]["code"], "inline_secret_rejected")
        self.assertNotIn("candidate-secret", json.dumps(response))

    def test_invalid_payloads_return_structured_error(self) -> None:
        response = handle_control_payload("{not json")

        self.assertEqual(response["schema"], CONTROL_RESPONSE_SCHEMA)
        self.assertIs(response["ok"], False)
        self.assertEqual(response["error"]["code"], "request_invalid")

    def test_health_rejects_extra_fields(self) -> None:
        response = handle_control_payload(
            json.dumps(
                {
                    "schema": "ylx.network-control-request.v1",
                    "operation": "health",
                    "body": {},
                }
            )
        )

        self.assertIs(response["ok"], False)
        self.assertEqual(response["error"]["code"], "request_invalid")

    def test_secret_bearing_controller_response_is_replaced_fail_closed(self) -> None:
        controller = Mock()
        controller._handle_validated.return_value = {
            "schema": "ylx.network-control-response.v1",
            "ok": True,
            "operation": "create_credential",
            "status": 201,
            "body": {
                "schema": "ylx.network-credential-receipt.v1",
                "credential_ref": "cred-safe-reference",
                "issued_at": "2026-08-23T12:00:00Z",
                "expires_at": "2026-08-23T12:01:00Z",
                "ttl_seconds": 60,
                "single_use": True,
                "passphrase": "must-never-escape",
            },
        }

        response = handle_control_payload(
            json.dumps(
                {
                    "schema": "ylx.network-control-request.v1",
                    "operation": "create_credential",
                    "principal_id": "customer",
                    "body": {
                        "schema": "ylx.network-credential-request.v1",
                        "passphrase": "incoming-secret",
                    },
                }
            ),
            controller=controller,
        )

        self.assertFalse(response["ok"])
        self.assertEqual(response["error"]["code"], "response_invalid")
        self.assertNotIn("must-never-escape", json.dumps(response))
        self.assertNotIn("incoming-secret", json.dumps(response))

    def test_invalid_mutation_shape_fails_before_not_enabled(self) -> None:
        response = handle_control_payload(
            json.dumps(
                {
                    "schema": "ylx.network-control-request.v1",
                    "operation": "apply",
                    "principal_id": "customer",
                    "idempotency_key": "idem-1",
                    "body": {
                        "schema": "ylx.network-apply-request.v1",
                        "desired": {
                            "mode": "wifi-client",
                            "wifi_client": {
                                "ssid": "Field LAN",
                                "security": "wpa2-personal",
                                "credential_ref": "cred/network/field-lan",
                            },
                            "ethernet": None,
                        },
                    },
                }
            )
        )

        self.assertIs(response["ok"], False)
        self.assertEqual(response["error"]["code"], "request_invalid")

    def test_stdio_server_constructs_one_controller_for_the_stream(self) -> None:
        controller = Mock()
        controller._handle_validated.return_value = {
            "schema": "ylx.network-control-response.v1",
            "ok": True,
            "operation": "health",
            "capabilities": {
                "mutation_enabled": True,
                "operations": ["apply", "forget", "retry"],
                "secret_handling": "opaque_credential_reference_only",
            },
        }
        stdout = io.StringIO()
        request = json.dumps(
            {
                "schema": "ylx.network-control-request.v1",
                "operation": "health",
            }
        )
        with patch(
            "rp_ylx.network_control.NetworkController", return_value=controller
        ) as constructor:
            code = serve_stdio(
                stdin=io.StringIO(f"{request}\n{request}\n"),
                stdout=stdout,
            )

        self.assertEqual(code, 0)
        responses = [json.loads(line) for line in stdout.getvalue().splitlines()]
        self.assertEqual(len(responses), 2)
        self.assertTrue(all(response["ok"] for response in responses))
        constructor.assert_called_once_with(defer_execution_until_response=True)
        self.assertEqual(controller._handle_validated.call_count, 2)
        self.assertEqual(controller.release_response.call_count, 2)
        controller.close.assert_called_once_with()

    def test_stdio_write_failure_does_not_release_deferred_transaction(self) -> None:
        class FailingOutput(io.StringIO):
            def write(self, value: str) -> int:
                del value
                raise OSError("response channel closed")

        response = {
            "schema": "ylx.network-control-response.v1",
            "ok": True,
            "operation": "forget",
            "status": 202,
            "body": {
                "schema": "ylx.network-transaction-receipt.v1",
                "accepted_at": "2026-08-23T15:04:00+08:00",
                "transaction": {"transaction_id": "0198d2a0-41a0-7b7a-a751-0e86a39d4db5"},
            },
            "replayed": False,
        }
        controller = Mock()

        with (
            patch("rp_ylx.network_control.handle_control_payload", return_value=response),
            self.assertRaises(OSError),
        ):
            serve_stdio(
                stdin=io.StringIO("request\n"),
                stdout=FailingOutput(),
                controller=controller,
                max_connections=1,
            )

        controller.release_response.assert_not_called()

    def test_stdio_short_write_does_not_release_deferred_transaction(self) -> None:
        class ShortOutput(io.StringIO):
            def write(self, value: str) -> int:
                super().write(value[:-1])
                return len(value) - 1

        response = {
            "schema": "ylx.network-control-response.v1",
            "ok": True,
            "operation": "forget",
            "status": 202,
            "body": {
                "schema": "ylx.network-transaction-receipt.v1",
                "accepted_at": "2026-08-23T15:04:00+08:00",
                "transaction": {"transaction_id": "0198d2a0-41a0-7b7a-a751-0e86a39d4db9"},
            },
            "replayed": False,
        }
        controller = Mock()

        with (
            patch("rp_ylx.network_control.handle_control_payload", return_value=response),
            self.assertRaises(OSError),
        ):
            serve_stdio(
                stdin=io.StringIO("request\n"),
                stdout=ShortOutput(),
                controller=controller,
                max_connections=1,
            )

        controller.release_response.assert_not_called()

    def test_deferred_controller_starts_mutation_only_after_receipt_release(self) -> None:
        with (
            tempfile.TemporaryDirectory() as directory,
            patch.dict(
                os.environ,
                {"RP_YLX_NETWORK_STATE_DIR": str(Path(directory) / "state")},
                clear=False,
            ),
            patch(
                "rp_ylx.network_control.ensure_rescue_ap",
                return_value={"ssid": "YLX-TEST", "interface": "wlan0"},
            ),
            patch("rp_ylx.network_control.rescue_network"),
        ):
            controller = NetworkController(
                device_id="device-deferred",
                require_root=False,
                defer_execution_until_response=True,
            )
            try:
                with patch.object(controller, "_execute") as execute:
                    response = controller.handle(
                        {
                            "schema": "ylx.network-control-request.v1",
                            "operation": "forget",
                            "principal_id": "customer",
                            "idempotency_key": "deferred-forget",
                            "body": {"schema": "ylx.network-forget-request.v1"},
                        }
                    )
                    self.assertTrue(response["ok"])
                    self.assertFalse(controller.wait_for_idle(timeout=0.01))
                    execute.assert_not_called()

                    controller.release_response(response)
                    self.assertTrue(controller.wait_for_idle(timeout=1))
                    execute.assert_called_once_with(
                        response["body"]["transaction"]["transaction_id"]
                    )
            finally:
                controller.close()

    def test_cli_network_control_serve_stdio(self) -> None:
        output = io.StringIO()
        error = io.StringIO()
        self.addCleanup(reset_operational_logging)
        controller = Mock()
        controller._handle_validated.return_value = {
            "schema": "ylx.network-control-response.v1",
            "ok": True,
            "operation": "health",
            "capabilities": {
                "mutation_enabled": True,
                "operations": ["apply", "forget", "retry"],
                "secret_handling": "opaque_credential_reference_only",
            },
        }
        with (
            patch(
                "sys.stdin",
                io.StringIO(
                    json.dumps(
                        {
                            "schema": "ylx.network-control-request.v1",
                            "operation": "health",
                        }
                    )
                ),
            ),
            patch("rp_ylx.network_control.NetworkController", return_value=controller),
            redirect_stderr(error),
            redirect_stdout(output),
        ):
            code = main(["network-control", "serve", "--stdio"])

        self.assertEqual(code, 0)
        response = json.loads(output.getvalue())
        self.assertIs(response["ok"], True)

    def test_socket_activated_stdio_holds_credential_across_connections(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            socket_path = root / "network-control.sock"
            environment = {
                "RP_YLX_NETWORK_STATE_DIR": str(root / "state"),
                "RP_YLX_NM_PROFILE_DIR": str(root / "profiles"),
                "RP_YLX_AVAHI_SERVICE_DIR": str(root / "avahi"),
            }
            from tests.test_network import FakeNmcli

            nmcli = FakeNmcli(root / "profiles")
            credentials = NetworkCredentialStore(token_factory=lambda: "socket-shared")
            with (
                patch.dict(os.environ, environment, clear=False),
                patch("rp_ylx.network.subprocess.run", side_effect=nmcli),
                socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as listener,
            ):
                listener.bind(str(socket_path))
                listener.listen(4)
                controller = NetworkController(
                    device_id="device-socket",
                    credential_store=credentials,
                    start_worker=False,
                    require_root=False,
                )
                input_stream = listener.makefile("r", encoding="utf-8")
                server = threading.Thread(
                    target=serve_stdio,
                    kwargs={
                        "stdin": input_stream,
                        "stdout": io.StringIO(),
                        "controller": controller,
                        "max_connections": 2,
                    },
                    daemon=True,
                )
                server.start()
                try:
                    created = request_control(
                        "create_credential",
                        principal_id="customer",
                        body={
                            "schema": "ylx.network-credential-request.v1",
                            "passphrase": "socket-client-secret",
                        },
                        socket_path=socket_path,
                    )
                    applied = request_control(
                        "apply",
                        principal_id="customer",
                        idempotency_key="socket-apply",
                        body={
                            "schema": "ylx.network-apply-request.v1",
                            "desired": {
                                "mode": "wifi-client",
                                "wifi_client": {
                                    "ssid": "Socket LAN",
                                    "security": "wpa2-personal",
                                    "credential_ref": created["body"]["credential_ref"],
                                },
                                "ethernet": None,
                            },
                        },
                        socket_path=socket_path,
                    )
                    server.join(timeout=2)
                finally:
                    input_stream.close()
                    controller.close()

        self.assertFalse(server.is_alive())
        self.assertTrue(created["ok"])
        self.assertTrue(applied["ok"])
        self.assertEqual(applied["body"]["transaction"]["status"], "accepted")

    def test_client_round_trips_one_socket_request(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            socket_path = Path(directory) / "network-control.sock"
            thread = self._serve_once(socket_path)
            response = request_control(
                "apply",
                principal_id="customer",
                idempotency_key="idem-1",
                body={
                    "schema": "ylx.network-apply-request.v1",
                    "desired": {
                        "mode": "wifi-client",
                        "wifi_client": {
                            "ssid": "Field LAN",
                            "security": "wpa2-personal",
                            "credential_ref": "cred-network-field-lan",
                        },
                        "ethernet": None,
                    },
                },
                socket_path=socket_path,
            )
            thread.join(timeout=2)

        self.assertFalse(response["ok"])
        self.assertEqual(response["operation"], "apply")
        self.assertEqual(response["error"]["code"], "network_controller_not_enabled")

    def test_client_reports_unavailable_socket(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            socket_path = Path(directory) / "missing.sock"
            with self.assertRaises(NetworkControlClientError) as raised:
                request_control(
                    "health",
                    socket_path=socket_path,
                    timeout_seconds=0.1,
                )

        self.assertEqual(raised.exception.code, "network_controller_unavailable")

    def test_client_rejects_invalid_socket_response(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            socket_path = Path(directory) / "network-control.sock"
            thread = self._serve_once(socket_path, response=b"not-json\n")
            with self.assertRaises(NetworkControlClientError) as raised:
                request_control(
                    "health",
                    socket_path=socket_path,
                    timeout_seconds=0.5,
                )
            thread.join(timeout=2)

        self.assertEqual(raised.exception.code, "response_invalid")

    def test_coordinator_forwards_mutation_to_root_controller_and_fails_closed(self) -> None:
        command = NetworkCommand(
            "customer",
            "idem-1",
            {
                "schema": "ylx.network-forget-request.v1",
            },
            b'{"schema":"ylx.network-forget-request.v1"}',
        )
        provider = object.__new__(CaptureCoordinator)

        with (
            patch(
                "rp_ylx.recording.coordinator.request_network_control",
                return_value={
                    "schema": CONTROL_RESPONSE_SCHEMA,
                    "ok": False,
                    "operation": "forget",
                    "error": {
                        "code": "network_controller_not_enabled",
                        "message": "staged but disabled",
                    },
                    "retryable": True,
                },
            ) as request,
            self.assertRaises(ProviderError) as raised,
        ):
            provider.forget_network_client_profile(command)

        self.assertEqual(raised.exception.code, "network_mutation_unavailable")
        self.assertEqual(raised.exception.status, HTTPStatus.SERVICE_UNAVAILABLE)
        self.assertTrue(raised.exception.retryable)
        request.assert_called_once_with(
            "forget",
            principal_id="customer",
            idempotency_key="idem-1",
            body=command.body,
            timeout_seconds=20.0,
        )

    def test_coordinator_maps_root_controller_transport_failure_to_fail_closed(self) -> None:
        command = NetworkCommand(
            "customer",
            "idem-1",
            {
                "schema": "ylx.network-retry-request.v1",
                "transaction_id": "0198d2a0-41a0-7b7a-a751-0e86a39d4db1",
            },
            b"{}",
        )
        provider = object.__new__(CaptureCoordinator)

        with (
            patch(
                "rp_ylx.recording.coordinator.request_network_control",
                side_effect=NetworkControlClientError(
                    "network_controller_unavailable",
                    "network-control socket is unavailable",
                ),
            ),
            self.assertRaises(ProviderError) as raised,
        ):
            provider.retry_network_transaction(command)

        self.assertEqual(raised.exception.code, "network_mutation_unavailable")
        self.assertEqual(raised.exception.status, HTTPStatus.SERVICE_UNAVAILABLE)
        self.assertTrue(raised.exception.retryable)

    def test_coordinator_maps_enabled_root_controller_receipt(self) -> None:
        receipt = {
            "schema": "ylx.network-command-receipt.v1",
            "transaction_id": "0198d2a0-41a0-7b7a-a751-0e86a39d4db1",
            "operation": "apply",
            "accepted_at": "2026-08-23T15:00:00+08:00",
            "status": "accepted",
        }
        command = NetworkCommand(
            "customer",
            "idem-1",
            {
                "schema": "ylx.network-apply-request.v1",
                "desired": {
                    "mode": "hotspot",
                    "wifi_client": None,
                    "ethernet": None,
                },
            },
            b"{}",
        )
        provider = object.__new__(CaptureCoordinator)

        with patch(
            "rp_ylx.recording.coordinator.request_network_control",
            return_value={
                "schema": CONTROL_RESPONSE_SCHEMA,
                "ok": True,
                "operation": "apply",
                "status": 202,
                "body": receipt,
                "replayed": True,
            },
        ):
            result = provider.apply_network_desired_state(command)

        self.assertEqual(result.status, HTTPStatus.ACCEPTED)
        self.assertEqual(result.body, receipt)
        self.assertTrue(result.replayed)

    def test_coordinator_forwards_scan_and_transient_credential(self) -> None:
        provider = object.__new__(CaptureCoordinator)
        scan = {
            "schema": "ylx.network-scan.v1",
            "authority_epoch": "4fa85f64-5717-4562-b3fc-2c963f66afa6",
            "source_revision": 1,
            "scanned_at": "2026-08-23T15:00:00Z",
            "networks": [],
        }
        credential = {
            "schema": "ylx.network-credential-receipt.v1",
            "credential_ref": "cred-short-lived",
            "issued_at": "2026-08-23T15:00:00Z",
            "expires_at": "2026-08-23T15:02:00Z",
            "ttl_seconds": 120,
            "single_use": True,
        }
        with patch(
            "rp_ylx.recording.coordinator.request_network_control",
            side_effect=[
                {
                    "schema": CONTROL_RESPONSE_SCHEMA,
                    "ok": True,
                    "operation": "scan",
                    "status": 200,
                    "body": scan,
                },
                {
                    "schema": CONTROL_RESPONSE_SCHEMA,
                    "ok": True,
                    "operation": "create_credential",
                    "status": 201,
                    "body": credential,
                },
            ],
        ) as request:
            self.assertEqual(provider.scan_networks(), scan)
            self.assertEqual(
                provider.create_network_credential(
                    NetworkCredentialCommand("customer", "transient-secret")
                ),
                credential,
            )

        self.assertEqual(request.call_args_list[0].args, ("scan",))
        self.assertEqual(request.call_args_list[0].kwargs, {"timeout_seconds": 20.0})
        self.assertEqual(request.call_args_list[1].args, ("create_credential",))
        self.assertEqual(request.call_args_list[1].kwargs["principal_id"], "customer")
        self.assertEqual(request.call_args_list[1].kwargs["timeout_seconds"], 20.0)
        self.assertEqual(
            request.call_args_list[1].kwargs["body"],
            {
                "schema": "ylx.network-credential-request.v1",
                "passphrase": "transient-secret",
            },
        )

    def test_coordinator_maps_missing_retry_and_blocks_mutation_during_capture(self) -> None:
        provider = object.__new__(CaptureCoordinator)
        provider._lock = threading.RLock()
        provider._active = None
        provider._network_operation_lease = None
        command = NetworkCommand(
            "customer",
            "retry-missing",
            {
                "schema": "ylx.network-retry-request.v1",
                "transaction_id": "0198d2a0-41a0-7b7a-a751-0e86a39d4db1",
            },
            b"{}",
        )
        missing = {
            "schema": CONTROL_RESPONSE_SCHEMA,
            "ok": False,
            "operation": "retry",
            "error": {"code": "transaction_not_found", "message": "not retained"},
            "retryable": False,
        }
        with (
            patch(
                "rp_ylx.recording.coordinator.request_network_control",
                return_value=missing,
            ),
            self.assertRaises(ProviderError) as raised,
        ):
            provider.retry_network_transaction(command)
        self.assertEqual(raised.exception.code, "network_transaction_not_found")
        self.assertEqual(raised.exception.status, HTTPStatus.NOT_FOUND)

        provider._active = object()
        with (
            patch("rp_ylx.recording.coordinator.request_network_control") as request,
            self.assertRaises(ProviderError) as raised,
        ):
            provider.retry_network_transaction(command)
        self.assertEqual(raised.exception.code, "network_mutation_unavailable")
        self.assertEqual(raised.exception.status, HTTPStatus.SERVICE_UNAVAILABLE)
        self.assertEqual(raised.exception.details, {"reason": "capture_active"})
        request.assert_not_called()

    def test_capture_start_is_rejected_while_network_transaction_is_current(self) -> None:
        provider = object.__new__(CaptureCoordinator)
        provider._active = None
        provider._network_operation_lease = None
        response = {
            "schema": CONTROL_RESPONSE_SCHEMA,
            "ok": True,
            "operation": "status",
            "status": 200,
            "body": {"transaction": {"current": {"status": "running"}, "latest": None}},
        }
        with (
            patch(
                "rp_ylx.recording.coordinator.request_network_control",
                return_value=response,
            ),
            self.assertRaises(ProviderError) as raised,
        ):
            provider._start_capture({})
        self.assertEqual(raised.exception.code, "network_mutation_active")
        self.assertEqual(raised.exception.status, HTTPStatus.CONFLICT)


if __name__ == "__main__":
    unittest.main()

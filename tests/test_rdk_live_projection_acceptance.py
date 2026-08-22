from __future__ import annotations

import argparse
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from scripts import rdk_live_projection_acceptance as harness


class RdkLiveProjectionAcceptanceTest(unittest.TestCase):
    def test_stop_and_wait_uses_configured_timeout_and_accepts_idle_after_timeout(
        self,
    ) -> None:
        with (
            patch.object(
                harness, "_request", side_effect=TimeoutError("response timed out")
            ) as request,
            patch.object(harness, "_wait_for_idle") as wait_for_idle,
        ):
            result = harness._stop_and_wait(
                "http://127.0.0.1:8080",
                timeout_seconds=180.0,
                idempotency_key="stop-test",
            )

        self.assertEqual(request.call_count, 1)
        self.assertEqual(request.call_args.kwargs["timeout"], 180.0)
        wait_for_idle.assert_called_once_with(
            "http://127.0.0.1:8080",
            timeout_seconds=180.0,
        )
        self.assertEqual(result["request_error_type"], "TimeoutError")
        self.assertTrue(result["idle_confirmed"])

    def test_evidence_journal_keeps_status_and_sse_records_as_valid_ndjson(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "acceptance.journal.ndjson"
            with harness._EvidenceJournal(path, fsync_interval_seconds=0.0) as journal:
                journal.append("status_sample", {"source_revision": 7})
                journal.append("sse", {"kind": "event", "id": "8"})

            records = [json.loads(line) for line in path.read_text().splitlines()]

        self.assertEqual([record["sequence"] for record in records], [1, 2])
        self.assertEqual(
            [record["kind"] for record in records],
            ["status_sample", "sse"],
        )
        self.assertEqual(records[0]["payload"], {"source_revision": 7})
        self.assertEqual(records[1]["payload"], {"kind": "event", "id": "8"})

    def test_parse_sse_block_preserves_same_revision_progress_payload(self) -> None:
        parsed = harness._parse_sse_block(
            [
                "id: 2",
                "event: progress",
                'data: {"source_revision":7,"type":"progress"}',
            ]
        )
        self.assertEqual(
            parsed,
            {
                "id": "2",
                "event": "progress",
                "data": {"source_revision": 7, "type": "progress"},
            },
        )

    def test_sse_read_timeout_exceeds_device_heartbeat_window(self) -> None:
        self.assertGreaterEqual(harness.SSE_READ_TIMEOUT_SECONDS, 20.0)

    def test_counter_reconciliation_reports_live_and_final_deltas(self) -> None:
        samples = [
            {
                "http_status": 200,
                "progress": {
                    "elapsed_seconds": 1.0,
                    "captured_frames": 10,
                    "bytes_written": 100,
                },
            },
            {
                "http_status": 200,
                "progress": {
                    "elapsed_seconds": 3.0,
                    "captured_frames": 70,
                    "bytes_written": 900,
                },
            },
        ]

        reconciled = harness._counter_reconciliation(
            samples,
            {
                "duration_seconds": 3.5,
                "frames": 75,
                "artifact_bytes": 1024,
                "imu_samples": 200,
            },
        )

        self.assertEqual(
            reconciled["live_delta"],
            {"elapsed_seconds": 2.0, "captured_frames": 60, "bytes_written": 800},
        )
        self.assertEqual(
            reconciled["final_delta"],
            {"elapsed_seconds": 0.5, "captured_frames": 5, "bytes_written": 124},
        )
        self.assertTrue(harness._final_lag_is_bounded(reconciled))

    def test_final_lag_rejects_overshoot_and_unbounded_active_segment(self) -> None:
        for final_delta in (
            {"elapsed_seconds": -0.1, "captured_frames": 1, "bytes_written": 1},
            {
                "elapsed_seconds": 1,
                "captured_frames": 1,
                "bytes_written": harness.MAX_FINAL_BYTE_LAG + 1,
            },
        ):
            with self.subTest(final_delta=final_delta):
                self.assertFalse(harness._final_lag_is_bounded({"final_delta": final_delta}))

    def test_sse_checks_scope_recording_revision_and_reject_finalizing_regression(self) -> None:
        session_id = "01989f6a-2c00-7a1b-8c2d-3e4f50617283"
        source = ("epoch", 2243)

        def progress(
            delivery_id: int,
            revision: int,
            phase: str,
            elapsed_seconds: float,
            completed_units: int,
        ) -> dict[str, object]:
            return {
                "kind": "event",
                "id": str(delivery_id),
                "event": "progress",
                "data": {
                    "authority_epoch": "epoch",
                    "source_revision": revision,
                    "session_id": session_id,
                    "data": {
                        "phase": phase,
                        "elapsed_seconds": elapsed_seconds,
                        "completed_units": completed_units,
                    },
                },
            }

        events = [
            progress(1, 2243, "recording", 1_799.8, 53_517),
            progress(2, 2243, "recording", 1_800.8, 53_547),
            progress(3, 2244, "finalizing", 1_802.9, 53_555),
            progress(4, 2244, "finalizing", 1_806.9, 53_555),
            progress(5, 2244, "finalizing", 1_807.9, 0),
        ]

        self.assertTrue(
            harness._sse_progress_advances(
                events,
                expected_source=source,
                session_id=session_id,
            )
        )
        self.assertFalse(
            harness._sse_finalizing_progress_non_regressing(events, session_id=session_id)
        )

        events.pop()
        self.assertTrue(
            harness._sse_finalizing_progress_non_regressing(events, session_id=session_id)
        )

    def test_counter_reconciliation_uses_finalizing_boundary_without_hiding_regression(
        self,
    ) -> None:
        session_id = "01989f6a-2c00-7a1b-8c2d-3e4f50617283"
        samples = [
            {
                "http_status": 200,
                "progress": {
                    "elapsed_seconds": 1_800.098491,
                    "captured_frames": 53_524,
                    "bytes_written": 1_046_312_942,
                },
            }
        ]
        events = [
            {
                "kind": "event",
                "id": "1786",
                "event": "progress",
                "data": {
                    "authority_epoch": "epoch",
                    "source_revision": 2244,
                    "session_id": session_id,
                    "data": {
                        "phase": "finalizing",
                        "elapsed_seconds": 1_802.907502,
                        "completed_units": 53_555,
                    },
                },
            },
            {
                "kind": "event",
                "id": "1790",
                "event": "progress",
                "data": {
                    "authority_epoch": "epoch",
                    "source_revision": 2244,
                    "session_id": session_id,
                    "data": {
                        "phase": "finalizing",
                        "elapsed_seconds": 1_806.927320,
                        "completed_units": 53_555,
                    },
                },
            },
            {
                "kind": "event",
                "id": "1791",
                "event": "progress",
                "data": {
                    "authority_epoch": "epoch",
                    "source_revision": 2244,
                    "session_id": session_id,
                    "data": {
                        "phase": "finalizing",
                        "elapsed_seconds": 1_807.932765,
                        "completed_units": 0,
                    },
                },
            },
        ]

        reconciled = harness._counter_reconciliation(
            samples,
            {
                "duration_seconds": 1_807.520448588,
                "frames": 53_555,
                "artifact_bytes": 1_051_752_927,
                "imu_samples": 214_216,
            },
            sse_events=events,
            session_id=session_id,
        )

        self.assertEqual(reconciled["final_projection"]["kind"], "sse_finalizing")
        self.assertEqual(reconciled["final_projection"]["event_id"], "1790")
        self.assertAlmostEqual(
            reconciled["final_delta"]["elapsed_seconds"],
            0.593128588,
            places=6,
        )
        self.assertEqual(reconciled["final_delta"]["captured_frames"], 0)
        self.assertEqual(reconciled["final_delta"]["bytes_written"], 5_439_985)
        self.assertTrue(harness._final_lag_is_bounded(reconciled))

    def test_same_revision_helper_rejects_revision_heartbeat(self) -> None:
        stable = [{"source_revision": 7}, {"source_revision": 7}]
        heartbeat = [{"source_revision": 7}, {"source_revision": 8}]

        self.assertTrue(harness._same_non_null_value(stable, "source_revision"))
        self.assertFalse(harness._same_non_null_value(heartbeat, "source_revision"))

    def test_checks_do_not_accept_revision_heartbeat_as_live_projection(self) -> None:
        samples = [
            {
                "http_status": 200,
                "latency_s": 0.01,
                "authority_epoch": "epoch",
                "source_revision": 7,
                "state_revision": 10,
                "recording_state": "recording",
                "progress": {
                    "elapsed_seconds": 1.0,
                    "captured_frames": 10,
                    "bytes_written": 100,
                },
                "live_imu": {"timestamp_ns": 100, "raw": {"x": 1}},
            },
            {
                "http_status": 200,
                "latency_s": 0.01,
                "authority_epoch": "epoch",
                "source_revision": 8,
                "state_revision": 11,
                "recording_state": "recording",
                "progress": {
                    "elapsed_seconds": 2.0,
                    "captured_frames": 20,
                    "bytes_written": 200,
                },
                "live_imu": {"timestamp_ns": 200, "raw": {"x": 2}},
            },
        ]
        checks = harness._checks(
            expected_commit=None,
            device_commit="a" * 40,
            pre_status=harness.HttpResult(
                200,
                {},
                {"snapshot": {"active_recording": None}},
                b"{}",
                0.01,
            ),
            samples=samples,
            manifest_summary={
                "sealed": True,
                "duration_seconds": 2.1,
                "frames": 21,
                "artifact_bytes": 210,
                "dropped_frames": 0,
                "drop_events": [],
                "fatal_errors": [],
                "roles": sorted(harness.REQUIRED_ARTIFACT_ROLES),
            },
            validation={"returncode": 0, "stdout": {"valid": True}},
            sse_events=[],
            service_before={"NRestarts": 0, "MainPID": 1},
            service_after={"NRestarts": 0, "MainPID": 1},
            duration_requested_s=2.0,
            sample_interval_s=1.0,
            reconciliation={
                "final_delta": {
                    "elapsed_seconds": 0.1,
                    "captured_frames": 1,
                    "bytes_written": 10,
                }
            },
            range_probe={
                "http_status": 206,
                "body_bytes": 1,
                "content_range": "bytes 0-0/10",
            },
            session_id="01989f6a-2c00-7a1b-8c2d-3e4f50617283",
        )

        self.assertFalse(checks["recording_source_revision_stable"])
        self.assertFalse(checks["recording_state_revision_stable"])
        self.assertFalse(checks["same_revision_progress_elapsed_advances"])
        self.assertFalse(checks["same_revision_progress_frames_advance"])
        self.assertFalse(checks["same_revision_progress_bytes_advance"])

    def test_sse_reconnect_requires_event_after_last_event_id_connection(self) -> None:
        incomplete = [
            {"kind": "event", "id": "4"},
            {"kind": "reconnect", "last_event_id": "4"},
            {"kind": "connected", "last_event_id": "4"},
        ]
        converged = [*incomplete, {"kind": "event", "id": "5"}]

        self.assertFalse(harness._sse_reconnect_converged(incomplete))
        self.assertTrue(harness._sse_reconnect_converged(converged))

    def test_artifact_range_probe_requires_real_one_byte_response(self) -> None:
        manifest = {
            "video": {
                "artifact": {
                    "artifact_id": "a" * 64,
                    "role": "video.left",
                    "path": "video/left.mp4",
                    "bytes": 10,
                }
            }
        }
        response = harness.HttpResult(
            206,
            {"content-range": "bytes 0-0/10"},
            "x",
            b"x",
            0.01,
        )
        with patch.object(harness, "_request", return_value=response) as request:
            result = harness._artifact_range_probe(
                "http://127.0.0.1:8080",
                "01989f6a-2c00-7a1b-8c2d-3e4f50617283",
                manifest,
            )

        self.assertEqual(result["http_status"], 206)
        self.assertEqual(result["body_bytes"], 1)
        self.assertEqual(result["content_range"], "bytes 0-0/10")
        self.assertEqual(request.call_args.kwargs["headers"]["Range"], "bytes=0-0")

    def test_collect_stops_with_contract_valid_user_reason_when_sampling_fails(self) -> None:
        calls: list[tuple[str, str, dict[str, object] | None]] = []

        def fake_request(
            base_url: str,
            method: str,
            path: str,
            *,
            payload: dict[str, object] | None = None,
            headers: dict[str, str] | None = None,
            timeout: float = 10.0,
        ) -> harness.HttpResult:
            del base_url, headers, timeout
            calls.append((method, path, payload))
            if path == "/api/v3/device":
                return harness.HttpResult(
                    200,
                    {},
                    {
                        "build": {"commit": "a" * 40},
                        "security_profile": "lab",
                    },
                    b"{}",
                    0.01,
                )
            if path == "/api/v3/capture/status" and method == "GET" and len(calls) == 2:
                return harness.HttpResult(
                    200,
                    {},
                    {
                        "schema": "ylx.capture-status.v2",
                        "authority_epoch": "4fa85f64-5717-4562-b3fc-2c963f66afa6",
                        "source_revision": 7,
                        "snapshot": {
                            "device_state": "idle",
                            "active_recording": None,
                        },
                    },
                    b"{}",
                    0.01,
                )
            if path == "/api/v3/capture/start":
                return harness.HttpResult(
                    202,
                    {},
                    {
                        "snapshot": {
                            "active_recording": {
                                "recording_state": {
                                    "session_id": "01989f6a-2c00-7a1b-8c2d-3e4f50617283"
                                }
                            }
                        }
                    },
                    b"{}",
                    0.01,
                )
            if path == "/api/v3/capture/status" and method == "GET":
                raise RuntimeError("sample failed")
            if path == "/api/v3/capture/stop":
                return harness.HttpResult(202, {}, {}, b"{}", 0.01)
            raise AssertionError((method, path, payload))

        with tempfile.TemporaryDirectory() as directory:
            args = argparse.Namespace(
                base_url="http://127.0.0.1:8080",
                duration=15.0,
                sample_interval=1.0,
                sse_reconnect_after=5.0,
                stop_timeout=1.0,
                recording_root=Path(directory) / "recordings",
                acceptance_dir=Path(directory),
                rp_ylx_bin=Path("/bin/true"),
                service_name="rp-ylx",
                display_name="test",
                expected_commit=None,
                allow_non_loopback_data=True,
                allow_recording_root=True,
            )
            with (
                patch.object(harness, "_request", side_effect=fake_request),
                patch.object(harness, "_service_status", return_value={"NRestarts": 0}),
                patch.object(
                    harness,
                    "_data_mount_info",
                    return_value={"source": "/dev/loop0", "fstype": "ext4"},
                ),
                self.assertRaises(RuntimeError),
            ):
                harness.collect(args)

        self.assertIn(
            (
                "POST",
                "/api/v3/capture/stop",
                {"schema": "ylx.capture-stop.v2", "reason": "user"},
            ),
            calls,
        )

    def test_manifest_summary_counts_nested_artifacts(self) -> None:
        manifest = {
            "sealed": True,
            "time": {"duration_seconds": 2.0},
            "frames": {"count": 60},
            "imu": {
                "sample_count": 240,
                "artifact": {
                    "artifact_id": "i",
                    "role": "imu.samples",
                    "path": "imu.ndjson",
                    "bytes": 5,
                },
            },
            "integrity": {"dropped_frames": 0, "drop_events": [], "fatal_errors": []},
            "video": {
                "segments": [
                    {
                        "artifacts": {
                            "left": {
                                "artifact_id": "l",
                                "role": "video.left",
                                "path": "video/left.mp4",
                                "bytes": 7,
                            },
                            "right": {
                                "artifact_id": "r",
                                "role": "video.right",
                                "path": "video/right.mp4",
                                "bytes": 11,
                            },
                        }
                    }
                ]
            },
        }

        summary = harness._manifest_summary(manifest)

        self.assertEqual(summary["artifact_count"], 3)
        self.assertEqual(summary["artifact_bytes"], 23)
        self.assertEqual(summary["roles"], ["imu.samples", "video.left", "video.right"])


if __name__ == "__main__":
    unittest.main()

# Miro-Ego Device API v1 Migration Guide (Frontend, SDK & Bridge)

This document is the authoritative old-to-new migration reference for migrating the embedded web console (`web/app.js`, `index.html`, `assets.json`), host SDKs, and Bridge/export tooling from the legacy Open Aria / `rp-ylx` Device API (`v2`/`v3`/`v4`) to **Miro-Ego Device API v1** (`0.3.0+`).

> **Clean-Break Policy:** Miro-Ego Device API v1 is a deliberate clean break. Legacy `/api/v2`, `/api/v3`, and `/api/v4` prefixes, legacy `ylx.*` / `openaria.*` wire discriminators, legacy `/data/sessions/` storage, and the legacy 80 GiB ext4 loop-volume layout are **removed completely**. Clients must fail closed on unknown API major versions (`supported_device_api_majors: [1]`, `api_version: "1.0"`).

---

## 1. Contract Artifacts & Integrity Pinning

Consumers (Frontend build checks, SDK codegen, Bridge contract tests) should pin against [contracts/miro-ego-api-support.json](../contracts/miro-ego-api-support.json):

| Artifact | Path | Schema / Version | Bytes | SHA-256 |
| --- | --- | --- | ---: | --- |
| OpenAPI 3.1 Spec | `contracts/miro-ego-device-v1.openapi.yaml` | `1.0.0` (`/api/v1`) | `160114` | `83a7b0caac1982a08d3a27fec56027233b440d75964c03904b0a6f921a379b61` |
| Contract Support Manifest | `contracts/miro-ego-api-support.json` | `miro-ego.device-api-consumer-support.v1` | — | See repo tree |
| Device Session Schema | `contracts/schemas/miro-ego-device-session-v1.schema.json` | `miro-ego.device-session.v1` | `26811` | `892ec94bd9775c51175e80d40f7d702e287ca0084a34795ca0ae00aa44bc3035` |
| Recording State Schema | `contracts/schemas/miro-ego-recording-state-v1.schema.json` | `miro-ego.recording-state.v1` | `11843` | `f6ad048cc9f543da25730c5fe8fcea859726ed7c596d775acdf8d55525d4799b` |
| API Audit Event Schema | `contracts/schemas/miro-ego-api-audit-event-v1.schema.json` | `miro-ego.api-audit-event.v1` | `1372` | `6e42174bf0d683c62ad93458bff820ece36b7203aad1b01a3722bb72e3ad0ece` |
| Operational Event Schema | `contracts/schemas/miro-ego-operational-event-v1.schema.json` | `miro-ego.operational-event.v1` | `4287` | `3c9587acce891c342a3b5229864681ef83e38952f44d73f9ee34f26cb5d0d5bb` |
| Performance Report Schema | `contracts/schemas/miro-ego-performance-report-v1.schema.json` | `miro-ego.performance-report.v1` | `7648` | `b20b5cf9447e35b9e7e966821315c7f3aa3b874aa7f34e0f9993667637c0e81d` |

---

## 2. Complete HTTP Endpoint Mapping

Base URL prefix changes from `https://{device_host}/api/v4` to `https://{device_host}/api/v1`.
All 34 active v4 operations are migrated with identical `operationId` values, plus 3 new Miro-Ego v1 operations (**37 operations across 33 path templates**).

### 2.1 Device, Clock, Voice, Camera & Firmware

| Legacy v4 Endpoint | Miro-Ego v1 Endpoint | Method | `operationId` | Write / Idempotency Policy | Notes / Breaking Changes |
| --- | --- | --- | --- | --- | --- |
| `/api/v4/device` | `/api/v1/device` | `GET` | `getDevice` | Read (`Authorization`) | `schema: miro-ego.device.v1`, `api_version: "1.0"`, adds `capabilities.preview_h264` and `capabilities.diagnostic_logs`. |
| `/api/v4/voice` | `/api/v1/voice` | `GET` | `getVoice` | Read (`Authorization`) | `schema: miro-ego.voice.v1`. |
| `/api/v4/voice` | `/api/v1/voice` | `POST` | `setVoiceVolume` | Origin + `X-CSRF-Token` (no `Idempotency-Key`) | Request `schema: miro-ego.voice-set.v1`. |
| `/api/v4/clock` | `/api/v1/clock` | `GET` | `getDeviceClock` | Read (`Authorization`) | Unified `miro-ego.clock-status.v1` (always includes `boot_id`, `synchronized_at_ms`, `age_seconds`). |
| `/api/v4/clock/check` | `/api/v1/clock/check` | `GET` | `inspectDeviceClock` | Read (`Authorization`) | Returns unified `miro-ego.clock-status.v1` with a fresh 5000 ms challenge. |
| `/api/v4/clock/sync` | `/api/v1/clock/sync` | `POST` | `syncDeviceClock` | Origin + `X-CSRF-Token` (no `Idempotency-Key`) | Single request schema `miro-ego.clock-sync-request.v1` with optional `replace: boolean` (default `false`). |
| `/api/v4/firmware` | `/api/v1/firmware` | `GET` | `firmwareStatus` | Read (`Authorization`) | `schema: miro-ego.firmware-status.v1` (replaces `openaria.firmware-status.v1`). |
| `/api/v4/firmware/check` | `/api/v1/firmware/check` | `GET` | `checkFirmware` | Read (`Authorization`) | Query `?force=true\|false`; `schema: miro-ego.firmware-status.v1`. |
| `/api/v4/firmware/update` | `/api/v1/firmware/update` | `POST` | `firmwareUpdate` | Origin + `X-CSRF-Token` + `Idempotency-Key` | Returns `202` `FirmwareTask`. |
| `/api/v4/firmware/rollback` | `/api/v1/firmware/rollback` | `POST` | `rollbackFirmware` | Origin + `X-CSRF-Token` + `Idempotency-Key` | Returns `202` `FirmwareTask`. |
| `/api/v4/camera/focus` | `/api/v1/camera/focus` | `GET` | `getCameraFocus` | Read (`Authorization`) | `schema: miro-ego.camera-focus.v1`. |
| `/api/v4/camera/focus` | `/api/v1/camera/focus` | `POST` | `setCameraFocus` | Origin + `X-CSRF-Token` + `Idempotency-Key` | Request `schema: miro-ego.camera-focus-set.v1`. |

### 2.2 Network Control

| Legacy v4 Endpoint | Miro-Ego v1 Endpoint | Method | `operationId` | Write / Idempotency Policy | Notes / Breaking Changes |
| --- | --- | --- | --- | --- | --- |
| `/api/v4/network` | `/api/v1/network` | `GET` | `getNetworkStatus` | Read (`Authorization`) | `schema: miro-ego.network-status.v1`. |
| `/api/v4/network/scan` | `/api/v1/network/scan` | `GET` | `scanNetworks` | Read (`Authorization`) | `schema: miro-ego.network-scan.v1`. |
| `/api/v4/network/events` | `/api/v1/network/events` | `GET` | `streamNetworkEvents` | SSE (`Authorization`, `Last-Event-ID`) | `schema: miro-ego.network-event.v1`. |
| `/api/v4/network/credentials` | `/api/v1/network/credentials` | `POST` | `createNetworkCredentialReference` | Origin + `X-CSRF-Token` (no `Idempotency-Key`) | Request `miro-ego.network-credential-request.v1`; `201` `miro-ego.network-credential-receipt.v1`. |
| `/api/v4/network/apply` | `/api/v1/network/apply` | `POST` | `applyNetworkDesiredState` | Origin + `X-CSRF-Token` + `Idempotency-Key` | Request `miro-ego.network-apply-request.v1`; `202` `miro-ego.network-transaction-receipt.v1`. |
| `/api/v4/network/retry` | `/api/v1/network/retry` | `POST` | `retryNetworkTransaction` | Origin + `X-CSRF-Token` + `Idempotency-Key` | Request `miro-ego.network-retry-request.v1`; `202` `miro-ego.network-transaction-receipt.v1`. |
| `/api/v4/network/forget` | `/api/v1/network/forget` | `POST` | `forgetNetworkClientProfile` | Origin + `X-CSRF-Token` + `Idempotency-Key` | Request `miro-ego.network-forget-request.v1`; `202` `miro-ego.network-transaction-receipt.v1`. |

### 2.3 Capture & Live Preview

| Legacy v4 Endpoint | Miro-Ego v1 Endpoint | Method | `operationId` | Write / Idempotency Policy | Notes / Breaking Changes |
| --- | --- | --- | --- | --- | --- |
| `/api/v4/capture/readiness` | `/api/v1/capture/readiness` | `GET` | `getCaptureReadiness` | Read (`Authorization`) | `schema: miro-ego.capture-readiness.v1`; explicitly included in `lab.allowed_operation_ids`. |
| `/api/v4/capture/timer` | `/api/v1/capture/timer` | `GET` | `getCaptureTimer` | Read (`Authorization`) | `schema: miro-ego.capture-timer.v1`. |
| `/api/v4/capture/timer` | `/api/v1/capture/timer` | `POST` | `setCaptureTimer` | Origin + `X-CSRF-Token` (no `Idempotency-Key`) | Request `miro-ego.capture-timer-set.v1`. |
| `/api/v4/capture/status` | `/api/v1/capture/status` | `GET` | `getCaptureStatus` | Read (`Authorization`) | `schema: miro-ego.capture-status.v1`, inner snapshot `miro-ego.capture-snapshot-event.v1`. |
| `/api/v4/capture/start` | `/api/v1/capture/start` | `POST` | `startCapture` | Origin + `X-CSRF-Token` + `Idempotency-Key` | Request `miro-ego.capture-start.v1`; `202` `miro-ego.capture-status.v1`. |
| `/api/v4/capture/stop` | `/api/v1/capture/stop` | `POST` | `stopCapture` | Origin + `X-CSRF-Token` + `Idempotency-Key` | Request `miro-ego.capture-stop.v1`; `202` snapshot or `204` idle no-op. |
| `/api/v4/capture/events` | `/api/v1/capture/events` | `GET` | `streamCaptureEvents` | SSE (`Authorization`, `Last-Event-ID`) | Envelope `miro-ego.capture-event.v1`; payloads `miro-ego.capture-{snapshot,state,progress,diagnostic}-event.v1` and `miro-ego.safe-swap-receipt.v1`. |
| `/api/v4/capture/safe-swap` | `/api/v1/capture/safe-swap` | `GET` | `getCurrentSafeSwapReceipt` | Read (`Authorization`) | `schema: miro-ego.safe-swap-receipt-resource.v1` wrapping minimal `miro-ego.safe-swap-receipt.v1` (legacy v2 handle-audit receipt removed). |
| `/api/v4/preview` | `/api/v1/preview` | `GET` | `getPreview` | Read (`Authorization`) | MJPEG (`multipart/x-mixed-replace; boundary=miro-ego-preview`) or single frame (`image/jpeg`), query `?fps=N`. |
| *(new in v1)* | `/api/v1/preview/h264` | `GET` | `getPreviewH264` | Read (`Authorization`) | **NEW:** Low-bandwidth H.264 fragmented MP4 (`video/mp4`) MSE stream, optional `?fps=N&bitrate_kbps=K`. Returns `404 preview_h264_unsupported` if disabled; clients fall back to `GET /api/v1/preview`. |

### 2.4 Sealed Sessions & Artifact Transfer

| Legacy v4 Endpoint | Miro-Ego v1 Endpoint | Method | `operationId` | Write / Idempotency Policy | Notes / Breaking Changes |
| --- | --- | --- | --- | --- | --- |
| `/api/v4/sessions` | `/api/v1/sessions` | `GET` | `listSessions` | Read (`Authorization`, optional `Prefer: include-recovery`) | Always returns `miro-ego.session-list.v1` with `catalog_revision`. Legacy `ylx.session-list.v2` removed. |
| `/api/v4/sessions/delete` | `/api/v1/sessions/delete` | `POST` | `deleteSessions` | Origin + `X-CSRF-Token` + `Idempotency-Key` | Request `miro-ego.session-delete-request.v1`; response `miro-ego.session-delete-result.v1`. |
| `/api/v4/sessions/{session_id}` | `/api/v1/sessions/{session_id}` | `GET` | `getSession` | Read (`Authorization`) | Exact stored `manifest.json` bytes (`miro-ego.device-session.v1`); header renamed to `Miro-Ego-Manifest-SHA256`. |
| `/api/v4/sessions/{session_id}/unsuccessful-outcome` | `/api/v1/sessions/{session_id}/unsuccessful-outcome` | `GET` | `getRetainedUnsuccessfulSessionOutcome` | Read (`Authorization`) | `schema: miro-ego.retained-unsuccessful-session-resource.v1` wrapping `miro-ego.recording-state.v1`. |
| `/api/v4/sessions/{session_id}/artifacts/{artifact_id}` | `/api/v1/sessions/{session_id}/artifacts/{artifact_id}` | `GET` | `getSessionArtifact` | Read (`Authorization`, `Range`, `If-Range`) | `423 capture_busy` sends `Miro-Ego-Error-Code` + `Miro-Ego-Wait-State: idle`; `409` sends `Miro-Ego-Error-Code: session_not_verified`. |
| `/api/v4/sessions/{session_id}/artifacts/{artifact_id}` | `/api/v1/sessions/{session_id}/artifacts/{artifact_id}` | `HEAD` | `headSessionArtifact` | Read (`Authorization`, `Range`, `If-Range`) | Same admission order and `Miro-Ego-*` error headers without response body. |

### 2.5 Diagnostic Log Listing & Export (New in v1)

| Legacy v4 Endpoint | Miro-Ego v1 Endpoint | Method | `operationId` | Policy | Notes |
| --- | --- | --- | --- | --- | --- |
| *(new in v1)* | `/api/v1/logs` | `GET` | `listLogs` | Read (`Authorization`) | Lists visible diagnostic log files under `/data/logs/` (`schema: miro-ego.log-list.v1`). Optional `?prefix=...&limit=1..500`. Never exposes `/data/.miro-ego/`, dotfiles, or symlinks. |
| *(new in v1)* | `/api/v1/logs/{name}` | `GET` | `getLogFile` | Read (`Authorization`, `Range`, `If-Range`) | Downloads a single diagnostic log file from `/data/logs/` with `200`/`206` single-range support, `ETag`, `Content-Length`, `Content-Disposition`, and `Cache-Control: no-store`. |

### 2.6 Removed Legacy Prefixes

- `/api/v2/*` — **Removed** (returns `404`).
- `/api/v3/*` — **Removed** (returns `404`).
- `/api/v4/*` — **Removed** (returns `404`).

---

## 3. HTTP Headers & Streaming Framing Mapping

| Legacy Header / Token | Miro-Ego v1 Header / Token | Direction | Values / Notes |
| --- | --- | --- | --- |
| `YLX-Audit-Status` | `Miro-Ego-Audit-Status` | Response | `unavailable` when the audit sink cannot persist an audit record. |
| `YLX-Error-Code` | `Miro-Ego-Error-Code` | Response | Mirrors `error.code` on typed error responses (`capture_busy`, `session_not_verified`, `camera_not_connected`, `calibration_unavailable`, `camera_focus_unsupported`, `invalid_camera_focus`, `preview_h264_unsupported`, `storage_unavailable`, `audit_unavailable`, `download_capacity_exhausted`, network error codes). |
| `YLX-Manifest-SHA256` | `Miro-Ego-Manifest-SHA256` | Response | Lowercase 64-hex SHA-256 of exact stored `manifest.json` bytes on `GET /api/v1/sessions/{session_id}`; equals unquoted `ETag`. |
| `YLX-Wait-State` | `Miro-Ego-Wait-State` | Response | `idle` on `423 capture_busy` (`GET`/`HEAD /api/v1/sessions/{session_id}/artifacts/{artifact_id}`). |
| `Access-Control-Expose-Headers` (`YLX-*`) | `Access-Control-Expose-Headers` (`Miro-Ego-*`) | Response | `Accept-Ranges, Content-Range, ETag, Idempotency-Replayed, Retry-After, Miro-Ego-Audit-Status, Miro-Ego-Error-Code, Miro-Ego-Manifest-SHA256, Miro-Ego-Wait-State` |
| `multipart/x-mixed-replace; boundary=ylx-preview` | `multipart/x-mixed-replace; boundary=miro-ego-preview` | Response (`GET /preview`) | Boundary token renamed; standard `Content-Type: image/jpeg` + `Content-Length` per part unchanged. |
| `X-YLX-Sequence`, `X-YLX-Monotonic-Ns` | *(removed)* | Legacy dev server | Legacy standalone Python dev server headers removed with Python runtime cutover. |
| `Authorization`, `X-CSRF-Token`, `Idempotency-Key`, `Idempotency-Replayed`, `Last-Event-ID`, `Prefer`, `ETag`, `Range`, `If-Range`, `Retry-After`, `Cache-Control`, `X-Accel-Buffering` | *(unchanged)* | Standard HTTP | Semantics preserved identically. |

---

## 4. Complete JSON Schema & Wire Identifier Mapping

### 4.1 Standalone JSON Schema Files & `$id` URNs

| Legacy File / `$id` | Miro-Ego v1 File / `$id` | Discriminator Value |
| --- | --- | --- |
| `src/rp_ylx/schemas/ylx-device-session-v{1,2,3,4}.schema.json` (`urn:ylx:schema:device-session:v4`) | `contracts/schemas/miro-ego-device-session-v1.schema.json` (`urn:miro-ego:schema:device-session:v1`) | `schema: "miro-ego.device-session.v1"` |
| `src/rp_ylx/schemas/ylx-recording-state-v1.schema.json` (`urn:ylx:schema:recording-state:v1`) | `contracts/schemas/miro-ego-recording-state-v1.schema.json` (`urn:miro-ego:schema:recording-state:v1`) | `schema: "miro-ego.recording-state.v1"` |
| `src/rp_ylx/schemas/ylx-api-audit-event-v1.schema.json` (`urn:ylx:schema:api-audit-event:v1`) | `contracts/schemas/miro-ego-api-audit-event-v1.schema.json` (`urn:miro-ego:schema:api-audit-event:v1`) | `schema: "miro-ego.api-audit-event.v1"` |
| `src/rp_ylx/schemas/ylx-operational-event-v1.schema.json` (`urn:ylx:schema:operational-event:v1`) | `contracts/schemas/miro-ego-operational-event-v1.schema.json` (`urn:miro-ego:schema:operational-event:v1`) | `schema: "miro-ego.operational-event.v1"` |
| `src/rp_ylx/schemas/ylx-performance-report-v0.schema.json` (`urn:ylx:schema:performance-report:v0`) | `contracts/schemas/miro-ego-performance-report-v1.schema.json` (`urn:miro-ego:schema:performance-report:v1`) | `schema: "miro-ego.performance-report.v1"` *(field renamed from `format` to `schema`)* |

### 4.2 Wire `schema` Discriminator Mapping

| Legacy `schema` Value | Miro-Ego v1 `schema` Value | Scope |
| --- | --- | --- |
| `ylx.device.v4` (and `v2`/`v3`) | `miro-ego.device.v1` | `GET /api/v1/device` |
| `ylx.device-api-consumer-support.v1` | `miro-ego.device-api-consumer-support.v1` | `contracts/miro-ego-api-support.json` |
| `ylx.api-error.v2` | `miro-ego.api-error.v1` | All `application/problem+json` errors |
| `ylx.api-audit-event.v1` | `miro-ego.api-audit-event.v1` | Audit records (`/data/.miro-ego/audit/`) |
| `ylx.operational-event.v1` | `miro-ego.operational-event.v1` | Structured operational logs |
| `ylx.performance-report.v0` | `miro-ego.performance-report.v1` | Benchmark reports |
| `ylx.device-session.v4` (and `v1`/`v2`/`v3`) | `miro-ego.device-session.v1` | Sealed `manifest.json` & `GET /api/v1/sessions/{id}` |
| `ylx.recording-state.v1` | `miro-ego.recording-state.v1` | Active/retained `recording_state` |
| `openaria.recording-encoding.v1` | `miro-ego.recording-encoding.v1` | `manifest.video.encoding.schema` |
| `openaria.audio-clock.v1` / `v2` | `miro-ego.audio-clock.v1` | `manifest.audio.capture_clock.schema` |
| `openaria.capture-audit.v1` | `miro-ego.capture-audit.v1` | `manifest.capture_audit.schema` |
| `openaria.firmware-status.v1` | `miro-ego.firmware-status.v1` | `GET /api/v1/firmware`, `GET /api/v1/firmware/check` |
| `ylx.voice.v1` | `miro-ego.voice.v1` | `GET`/`POST /api/v1/voice` response |
| `ylx.voice-set.v1` | `miro-ego.voice-set.v1` | `POST /api/v1/voice` request |
| `ylx.clock-status.v1` / `ylx.clock-status.v2` | `miro-ego.clock-status.v1` | `GET /api/v1/clock`, `GET /api/v1/clock/check`, `POST /api/v1/clock/sync` response |
| `ylx.clock-sync-request.v1` / `ylx.clock-sync-request.v2` | `miro-ego.clock-sync-request.v1` | `POST /api/v1/clock/sync` request |
| `ylx.camera-connection.v1` | `miro-ego.camera-connection.v1` | `runtime.camera.schema` |
| `ylx.camera-focus.v1` | `miro-ego.camera-focus.v1` | `GET`/`POST /api/v1/camera/focus` & `runtime.camera_focus` |
| `ylx.camera-focus-set.v1` | `miro-ego.camera-focus-set.v1` | `POST /api/v1/camera/focus` request |
| `ylx.capture-readiness.v1` | `miro-ego.capture-readiness.v1` | `GET /api/v1/capture/readiness` |
| `ylx.capture-timer.v1` | `miro-ego.capture-timer.v1` | `GET`/`POST /api/v1/capture/timer` response |
| `ylx.capture-timer-set.v1` | `miro-ego.capture-timer-set.v1` | `POST /api/v1/capture/timer` request |
| `ylx.capture-start.v2` | `miro-ego.capture-start.v1` | `POST /api/v1/capture/start` request |
| `ylx.capture-stop.v2` | `miro-ego.capture-stop.v1` | `POST /api/v1/capture/stop` request |
| `ylx.capture-status.v4` (and `v2`) | `miro-ego.capture-status.v1` | `GET /api/v1/capture/status` & `202` command snapshots |
| `ylx.capture-snapshot-event.v4` (and `v2`) | `miro-ego.capture-snapshot-event.v1` | Snapshot payload inside status & SSE `snapshot` event |
| `ylx.capture-event.v4` | `miro-ego.capture-event.v1` | SSE envelope on `GET /api/v1/capture/events` |
| `ylx.capture-state-event.v2` | `miro-ego.capture-state-event.v1` | SSE `state` event payload |
| `ylx.capture-progress-event.v2` | `miro-ego.capture-progress-event.v1` | SSE `progress` event payload |
| `ylx.capture-diagnostic-event.v2` | `miro-ego.capture-diagnostic-event.v1` | SSE `diagnostic` event payload |
| `ylx.safe-swap-receipt.v3` *(v2 removed)* | `miro-ego.safe-swap-receipt.v1` | SSE `safe_swap` event payload & receipt body |
| `ylx.safe-swap-receipt-resource.v3` *(v2 removed)* | `miro-ego.safe-swap-receipt-resource.v1` | `GET /api/v1/capture/safe-swap` |
| `ylx.retained-unsuccessful-session-resource.v2` | `miro-ego.retained-unsuccessful-session-resource.v1` | `GET /api/v1/sessions/{session_id}/unsuccessful-outcome` |
| `ylx.session-list.v3` *(v2 removed)* | `miro-ego.session-list.v1` | `GET /api/v1/sessions` |
| `ylx.session-delete-request.v1` | `miro-ego.session-delete-request.v1` | `POST /api/v1/sessions/delete` request |
| `ylx.session-delete-result.v1` | `miro-ego.session-delete-result.v1` | `POST /api/v1/sessions/delete` response |
| `ylx.network-status.v1` | `miro-ego.network-status.v1` | `GET /api/v1/network` & SSE `snapshot` |
| `ylx.network-scan.v1` | `miro-ego.network-scan.v1` | `GET /api/v1/network/scan` |
| `ylx.network-credential-request.v1` | `miro-ego.network-credential-request.v1` | `POST /api/v1/network/credentials` request |
| `ylx.network-credential-receipt.v1` | `miro-ego.network-credential-receipt.v1` | `POST /api/v1/network/credentials` `201` response |
| `ylx.network-apply-request.v1` | `miro-ego.network-apply-request.v1` | `POST /api/v1/network/apply` request |
| `ylx.network-retry-request.v1` | `miro-ego.network-retry-request.v1` | `POST /api/v1/network/retry` request |
| `ylx.network-forget-request.v1` | `miro-ego.network-forget-request.v1` | `POST /api/v1/network/forget` request |
| `ylx.network-transaction.v1` | `miro-ego.network-transaction.v1` | Embedded transaction object & SSE `transaction` payload |
| `ylx.network-transaction-receipt.v1` | `miro-ego.network-transaction-receipt.v1` | `202` response from `/network/{apply,retry,forget}` |
| `ylx.network-event.v1` | `miro-ego.network-event.v1` | SSE envelope on `GET /api/v1/network/events` |
| *(new in v1)* | `miro-ego.log-list.v1` | `GET /api/v1/logs` |
| `openaria.echo-web-artifacts.v2` | `miro-ego.web-artifacts.v1` | Embedded frontend `assets.json` (`required_major: 1`) |
| `ylx.production-config.v1` | `miro-ego.device-config.v1` | `/etc/miro-ego/device.json` bootstrap config |

Removed legacy schema discriminators (must be rejected fail-closed):
- `ylx.safe-swap-receipt.v2`, `ylx.safe-swap-receipt-resource.v2`, `ylx.safe-swap-handle-audit.v1`, `ylx.safe-swap-admission-fence.v1`, `ylx.safe-swap-participant-authority.v1`
- `ylx.session-list.v2`
- `ylx.clock-status.v2`, `ylx.clock-sync-request.v2` (consolidated into `miro-ego.clock-status.v1` and `miro-ego.clock-sync-request.v1`)
- `ylx.device-session.v1`, `ylx.device-session.v2`, `ylx.device-session.v3`, `ylx.device-session.v4`

---

## 5. Frontend (`web/app.js`, `index.html`, `assets.json`) Migration Checklist

### 5.1 Browser Storage Keys (`sessionStorage` & `localStorage`)

| Legacy Key | Miro-Ego v1 Key | Storage API | Purpose & Migration Rule |
| --- | --- | --- | --- |
| `rp-ylx-access-token` | `miro-ego-access-token` | `sessionStorage` | Customer bearer token (also echoed as `X-CSRF-Token` on `POST`). Do not read or migrate legacy `rp-ylx-access-token`; clear it on sign-in or startup if present. |
| *(any legacy `rp-ylx-*` / `ylx-*` / `openaria-*`)* | `miro-ego-*` namespace | `localStorage` | Reserved prefix for client-side UI preferences (e.g. `miro-ego-preview-transport` for `"h264"` vs `"mjpeg"`, `miro-ego-focus-peaking`, `miro-ego-inspect-mode`). Never store bearer tokens or Wi-Fi passphrases in `localStorage`. |

### 5.2 Concrete Updates Required in `web/app.js` & `index.html`

1. **API Base & Major Version Guard**
   - Change `ue = "/api/v4"` to `"/api/v1"` (including firmware calls `/api/v4/firmware...` -> `/api/v1/firmware...`).
   - Change `Ht = { supported_device_api_majors: [4] }` to `{ supported_device_api_majors: [1] }`.
   - Validate `device.schema === "miro-ego.device.v1"` and `device.api_version === "1.0"`.
2. **Capabilities Set (`Kt`)**
   - Add `"preview_h264"` (`boolean`) and `"diagnostic_logs"` (`boolean`) to the strict `DeviceDescriptor.capabilities` key set.
3. **Device Label Regex (`jr`) & Brand Copy**
   - Update `device_label` validation regex from `/^YLX-[0-9A-F]{8}$/` to `/^Miro-Ego-[0-9A-F]{8}$/`.
   - Update fallback brand titles in `index.html` and `app.js` from `"Open Aria Echo"` to `"Miro-Ego"`.
   - Update `assets.json`: `"schema": "miro-ego.web-artifacts.v1"`, `"name": "miro-ego-web"`, `"compatibility": { "device_api": { "required_major": 1 } }`.
4. **Header Reads**
   - `getSession`: read `response.headers.get("Miro-Ego-Manifest-SHA256")` instead of `"YLX-Manifest-SHA256"`.
   - Error/status helpers: read `"Miro-Ego-Error-Code"`, `"Miro-Ego-Wait-State"`, and `"Miro-Ego-Audit-Status"` where inspected.
5. **Wire Schema Validators**
   - Update every hardcoded `ylx.*` and `openaria.*` string constant in `app.js` per Section 4.2.
   - Simplify `listSessions` parser: accept only `miro-ego.session-list.v1` (always carrying `catalog_revision`); drop the `ylx.session-list.v2` branch.
   - Simplify clock sync (`Me` / `Gt` / `qn`): `GET /api/v1/clock` and `GET /api/v1/clock/check` both return `miro-ego.clock-status.v1` with `boot_id`, `synchronized_at_ms`, `age_seconds`; `POST /api/v1/clock/sync` always sends `miro-ego.clock-sync-request.v1` (with `replace: true` only for explicit user-confirmed resync).
6. **New v1 Frontend Features**
   - **H.264 fMP4 Preview:** When `device.capabilities.preview_h264 === true` and MSE (`MediaSource.isTypeSupported('video/mp4; codecs="avc1.640028"')`) is available, connect to `GET /api/v1/preview/h264` for low-bandwidth streaming; gracefully fall back to `GET /api/v1/preview` on `404 preview_h264_unsupported` or unsupported browsers.
   - **Diagnostic Logs Export:** When `device.capabilities.diagnostic_logs === true`, expose a diagnostic log drawer calling `GET /api/v1/logs` and linking downloads to `GET /api/v1/logs/{name}` (using `Authorization` fetch + Blob download in `customer` profile so bearer tokens are never placed in query parameters).

---

## 6. mDNS Discovery, Network Identity & System Paths

| Surface | Legacy (`rp-ylx` / Open Aria) | Miro-Ego v1 (`0.3.0+`) |
| --- | --- | --- |
| mDNS Service Type | `_ylx-capture._tcp` (`_ylx-capture._tcp.local.`) | `_miro-ego._tcp` (`_miro-ego._tcp.local.`) |
| mDNS Companion Service | `_http._tcp` (lab) / `_https._tcp` (customer) | `_http._tcp` (lab) / `_https._tcp` (customer) |
| mDNS TXT Records | `scheme=http\|https`, `api=/api/v4/device`, `path=/` | `scheme=http\|https`, `api=/api/v1/device`, `path=/` |
| mDNS Instance Name | `Open Aria on %h` / `Open Aria._ylx-capture._tcp.local.` | `Miro-Ego on %h` / `Miro-Ego._miro-ego._tcp.local.` |
| mDNS Hostname | `rp-ylx.local` | `miro-ego.local` |
| `device_label` (API / Manifests) | `YLX-XXXXXXXX` (`^YLX-[0-9A-F]{8}$`) | `Miro-Ego-XXXXXXXX` (`^Miro-Ego-[0-9A-F]{8}$`) |
| Wi-Fi Rescue AP SSID | `YLX-XXXXXXXX` | `MiroEgo-XXXXXXXX` |
| SSH Admin User | `OpenAria` | `MiroEgo` |
| System Service User / Group | `rp-ylx:rp-ylx` | `miro-ego:miro-ego` |
| On-Device Binary & Paths | `/opt/rp-ylx`, `/etc/rp-ylx`, `/var/lib/rp-ylx`, `/run/rp-ylx` | `/opt/miro-ego`, `/etc/miro-ego`, `/var/lib/miro-ego`, `/run/miro-ego` |

---

## 7. Recording Disk Layout & Bridge / Card-Export Breaking Changes

The recording partition is **exclusively exFAT partition 3** (volume label `MIRO-EGO`, mounted at `/data`). Legacy ext4 loop-volume images and `/data/sessions/` are completely removed.

```text
/data/                                  (exFAT p3, label MIRO-EGO)
├── recordings/                         (user-visible sealed sessions: YYYY-MM-DD/HH-MM-SS_<id>/)
├── logs/                               (user-visible diagnostic logs served via GET /api/v1/logs)
├── updates/                            (reserved directory for offline update bundles)
└── .miro-ego/                          (native exFAT ATTR_HIDDEN — internal device state)
    ├── volume.json                     (volume UUIDv4 identity & format metadata)
    ├── index/
    │   └── catalog.bin                 (replaces legacy catalog.sqlite3)
    ├── staging/                        (in-progress capture workspaces)
    ├── quarantine/                     (malformed / unsealed candidates)
    ├── audit/                          (bounded miro-ego.api-audit-event.v1 logs)
    ├── receipts/                       (safe-swap & idempotency receipts)
    └── telemetry/                      (bounded diagnostic/performance telemetry)
```

### Bridge & Card-Export Impacts
- Scan `/data/recordings/` (never `/data/sessions/`) for sealed `manifest.json` files declaring `"schema": "miro-ego.device-session.v1"`.
- Ignore `/data/.miro-ego/` during user media export; diagnostic export tools may inspect `/data/logs/` directly or via `GET /api/v1/logs`.
- Do not rely on POSIX `st_ino` for file identity on exFAT (Linux 6.1 `fs/exfat` inode numbers are volatile across cache eviction). Verify by manifest SHA-256 and file size.

---

## 8. Summary of Breaking Changes

1. **API Prefix & Fail-Closed Major Version:** `/api/v4/...` -> `/api/v1/...`, `api_version: "1.0"`, `supported_device_api_majors: [1]`. Legacy v2/v3/v4 routes are gone.
2. **All Schema Discriminators Reset to `miro-ego.<name>.v1`:** Every wire JSON object, SSE event, error envelope, and standalone schema file uses the `miro-ego.*.v1` namespace.
3. **All Custom Headers Renamed to `Miro-Ego-*`:** `Miro-Ego-Audit-Status`, `Miro-Ego-Error-Code`, `Miro-Ego-Manifest-SHA256`, `Miro-Ego-Wait-State`.
4. **Unified Clock Schema:** `/clock`, `/clock/check`, and `/clock/sync` no longer split `v1`/`v2` schemas; all use `miro-ego.clock-status.v1` and `miro-ego.clock-sync-request.v1`.
5. **Minimal Safe-Swap Receipt Only:** Legacy v2 `SafeSwapHandleAudit` / `SafeSwapAdmissionFence` / `SafeSwapParticipantAcknowledgement` schemas are removed; only minimal `miro-ego.safe-swap-receipt.v1` exists.
6. **Mandatory `catalog_revision` on `GET /api/v1/sessions`:** Legacy `ylx.session-list.v2` without `catalog_revision` is removed.
7. **`DeviceDescriptor.capabilities` Additions & Lab Consistency:** `preview_h264` and `diagnostic_logs` are required booleans in `DeviceDescriptor.capabilities`; contradictory `lab -> network_mutation: false` schema override is removed (`NetworkStatus.mutation_capability` remains authoritative).
8. **`PerformanceReport` v1 Modernization:** Top-level discriminator renamed from `format` (`ylx.performance-report.v0`) to `schema` (`miro-ego.performance-report.v1`); `identity.wheel_sha256` -> `identity.binary_sha256`; `environment.python` -> `environment.runtime`; `native.adapter` enum updated to `["rust", "c_encoder", "host_tool"]`.
9. **New v1 Endpoints:** `GET /api/v1/preview/h264`, `GET /api/v1/logs`, and `GET /api/v1/logs/{name}`.
10. **OpenAPI External `$ref` Layout:** `contracts/miro-ego-device-v1.openapi.yaml` references `./schemas/miro-ego-device-session-v1.schema.json` and `./schemas/miro-ego-recording-state-v1.schema.json` directly under `contracts/schemas/`.

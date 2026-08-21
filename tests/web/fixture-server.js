// @ts-check

import { createReadStream } from "node:fs";
import { readFile, stat } from "node:fs/promises";
import { createServer } from "node:http";
/** @typedef {import("node:http").IncomingMessage} IncomingMessage */
/** @typedef {import("node:http").ServerResponse} ServerResponse */
/** @typedef {import("../../src/rp_ylx/web/state.js").CaptureStatus} CaptureStatus */
/** @typedef {import("../../src/rp_ylx/web/state.js").CameraFocusStatus} CameraFocusStatus */
/** @typedef {import("../../src/rp_ylx/web/state.js").DeviceDescriptor} DeviceDescriptor */
/** @typedef {import("../../src/rp_ylx/web/state.js").DeviceRuntime} DeviceRuntime */
/** @typedef {import("../../src/rp_ylx/web/state.js").SafeSwapReceipt} SafeSwapReceipt */
import { extname, resolve, sep } from "node:path";
import { fileURLToPath } from "node:url";

const repositoryRoot = resolve(fileURLToPath(new URL("../..", import.meta.url)));
const webRoot = resolve(repositoryRoot, "src/rp_ylx/web");
const fixtureRoot = resolve(repositoryRoot, "tests/web/fixtures");
const canonicalDiagnosticEvent = JSON.parse(
  await readFile(resolve(fixtureRoot, "capture-event.json"), "utf8"),
);
const canonicalFailedRecordingState = JSON.parse(
  await readFile(resolve(fixtureRoot, "recording-state-failed.json"), "utf8"),
);
const canonicalCaptureBusyProblem = JSON.parse(
  await readFile(resolve(fixtureRoot, "error-capture-busy.json"), "utf8"),
);
/** @type {Record<string, string>} */
const contentTypes = {
  ".css": "text/css; charset=utf-8",
  ".html": "text/html; charset=utf-8",
  ".js": "text/javascript; charset=utf-8",
  ".svg": "image/svg+xml",
};

const gibibyte = 1024 ** 3;
const authorityEpoch = "4fa85f64-5717-4562-b3fc-2c963f66afa6";
const deviceId = "a7d9b620-987b-4d35-89c1-5511f8036aed";
const volumeId = "6ba7b810-9dad-41d1-80b4-00c04fd430c8";
const sessionId = "01989f6a-2c00-7a1b-8c2d-3e4f50617283";
const takeId = "01989f6a-2c00-7a1b-8c2d-3e4f50617284";
const generationId = "7d516b70-d8ab-47d1-b2dc-5b1250138789";
const nextAuthorityEpoch = "5fa85f64-5717-4562-b3fc-2c963f66afa6";
const nextSessionId = "01989f6a-2c00-7a1b-8c2d-3e4f50617290";
const nextTakeId = "01989f6a-2c00-7a1b-8c2d-3e4f50617291";
const nextGenerationId = "8d516b70-d8ab-47d1-b2dc-5b1250138789";
const manifestId = "01989f6a-2c00-7a1b-8c2d-3e4f50617285";
const sealedSessionId = "01989f6a-2c00-7a1b-8c2d-3e4f50617286";
const sealedTakeId = "01989f6a-2c00-7a1b-8c2d-3e4f50617287";
const previewJpeg = Buffer.from(
  "/9j/4AAQSkZJRgABAQEAYABgAAD/2wBDAP//////////////////////////////////////////////////////////////////////////////////////2wBDAf//////////////////////////////////////////////////////////////////////////////////////wAARCAABAAEDASIAAhEBAxEB/8QAFQABAQAAAAAAAAAAAAAAAAAAAAX/xAAUEAEAAAAAAAAAAAAAAAAAAAAA/9oADAMBAAIQAxAAAAH/xAAUEAEAAAAAAAAAAAAAAAAAAAAA/9oACAEBAAEFAqf/xAAUEQEAAAAAAAAAAAAAAAAAAAAA/9oACAEDAQE/AX//xAAUEQEAAAAAAAAAAAAAAAAAAAAA/9oACAECAQE/AX//xAAUEAEAAAAAAAAAAAAAAAAAAAAA/9oACAEBAAY/An//xAAUEAEAAAAAAAAAAAAAAAAAAAAA/9oACAEBAAE/Iaf/2gAMAwEAAgADAAAAEP/EABQRAQAAAAAAAAAAAAAAAAAAABD/2gAIAQMBAT8QH//EABQRAQAAAAAAAAAAAAAAAAAAABD/2gAIAQIBAT8QH//EABQQAQAAAAAAAAAAAAAAAAAAABD/2gAIAQEAAT8QH//Z",
  "base64",
);

/**
 * @typedef {object} Fixture
 * @property {DeviceDescriptor & Record<string, unknown>} device
 * @property {CaptureStatus} snapshot
 * @property {{schema: string, items: unknown[], diagnostics: unknown[], next_cursor: string | null}} sessions
 * @property {number} nextDeliveryId
 * @property {number} commandDelayMs
 * @property {boolean} stopReturns204
 * @property {boolean} startProblem
 * @property {boolean} stopProblem
 * @property {boolean} eventsUnavailable
 * @property {boolean} sessionsVolumeUnavailable
 * @property {number} eventSnapshotDelayMs
 * @property {number} previewDelayMs
 * @property {number} previewRequests
 * @property {Set<number>} previewActive
 * @property {Array<Record<string, unknown>>} previewTimeline
 * @property {number} previewMaxConcurrent
 * @property {boolean} requireBearer
 * @property {Array<{path: string, authorization: string | null, lastEventId: string | null, idempotencyKey: string | null}>} apiRequests
 * @property {{schema: "ylx.safe-swap-receipt-resource.v3", receipt: SafeSwapReceipt} | null} safeSwapResource
 * @property {ReturnType<typeof captureEvent> | null} staleSafeSwapEvent
 */

/** @returns {CameraFocusStatus} */
const makeCameraFocusStatus = () => ({
  schema: "ylx.camera-focus.v1",
  value: 42,
  minimum: 0,
  maximum: 255,
  step: 1,
  default: 32,
  auto_supported: true,
  auto_enabled: false,
});

/** @returns {DeviceRuntime} */
const makeRuntime = () => ({
  observed_at: "2026-08-12T02:25:00Z",
  connection_method: "wifi_ap",
  temperature_celsius: 43.5,
  network: {
    ap: {
      state: "active",
      interface: "wlan0",
      addresses: ["10.42.0.1/24"],
      peer_or_ssid: "YLX-A1B2C3D4",
    },
    wifi_client: {
      state: "disabled",
      interface: null,
      addresses: [],
      peer_or_ssid: null,
    },
    wired: {
      state: "disconnected",
      interface: "eth0",
      addresses: [],
      peer_or_ssid: null,
    },
    default_route: "none",
  },
  live_imu: {
    session_id: sessionId,
    clock: {
      time_base: "host_monotonic",
      timestamp_ns: 4_250_000_000,
    },
    raw: {
      units: "raw_int16",
      accelerometer: { x: 12, y: -8, z: 979 },
      gyroscope: { x: 1, y: -2, z: 5 },
    },
    sync: { quality: "good" },
  },
  camera_focus: makeCameraFocusStatus(),
});

/** @returns {DeviceDescriptor & Record<string, unknown>} */
const makeDevice = () => ({
  schema: "ylx.device.v3",
  device: { device_id: deviceId, device_label: "YLX-A1B2C3D4" },
  hardware_fingerprint: `sha256:${"a".repeat(64)}`,
  api_version: "3.0",
  build: {
    package_version: "0.5.0-dev",
    commit: "b".repeat(40),
    build_id: "fixture-rdk-x5",
  },
  security_profile: "lab",
  capabilities: {
    capture: true,
    preview: true,
    range_download: true,
    network_mutation: false,
  },
  storage: {
    volume_id: volumeId,
    total_bytes: 128 * gibibyte,
    available_bytes: 82 * gibibyte,
    writable: true,
  },
  runtime: makeRuntime(),
});

/** @returns {CaptureStatus} */
const makeSnapshot = () => ({
  schema: "ylx.capture-status.v2",
  authority_epoch: authorityEpoch,
  source_revision: 1,
  snapshot: {
    schema: "ylx.capture-snapshot-event.v2",
    device_state: "idle",
    active_recording: null,
    retained_unsuccessful: null,
    runtime: makeRuntime(),
  },
});

/** @returns {Fixture} */
function makeFixture() {
  return {
    device: makeDevice(),
    snapshot: makeSnapshot(),
    sessions: {
      schema: "ylx.session-list.v2",
      items: [
        {
          session_id: sealedSessionId,
          producer_outcome: "sealed",
          take_id: sealedTakeId,
          take_sequence: 1,
          continuation_of: null,
          display_name: "入口标定",
          device: { device_id: deviceId, device_label: "YLX-A1B2C3D4" },
          started_at: "2026-08-12T01:00:00Z",
          ended_at: "2026-08-12T01:02:05Z",
          duration_seconds: 125,
          total_bytes: 3 * gibibyte,
          verification: {
            actor: "gateway",
            validator: {
              name: "rp-ylx-session-validator",
              version: "0.5.0",
              build_sha256: "d".repeat(64),
            },
            manifest_sha256: "e".repeat(64),
            verified_at: "2026-08-12T01:02:10Z",
            verdict: "usable",
            diagnostics: [],
          },
        },
        {
          session_id: sessionId,
          producer_outcome: "sealed",
          take_id: takeId,
          take_sequence: 2,
          continuation_of: sealedSessionId,
          display_name: "长名称用于移动端布局验证-abcdefghijklmnop-第二段采集",
          device: { device_id: deviceId, device_label: "YLX-A1B2C3D4" },
          started_at: "2026-08-12T01:10:00Z",
          ended_at: "2026-08-12T01:11:00Z",
          duration_seconds: 60,
          total_bytes: 2 * gibibyte,
          verification: null,
        },
      ],
      diagnostics: [
        {
          quarantine_id: "e4a4a3ca-568f-4b70-b68d-b36ffbd88602",
          code: "manifest_unreadable",
          observed_at: "2026-08-12T01:20:00Z",
          message: "发现一个无法读取的会话清单，已隔离",
        },
      ],
      next_cursor: null,
    },
    nextDeliveryId: 1,
    commandDelayMs: 0,
    stopReturns204: false,
    startProblem: false,
    stopProblem: false,
    eventsUnavailable: false,
    sessionsVolumeUnavailable: false,
    eventSnapshotDelayMs: 0,
    previewDelayMs: 20,
    previewRequests: 0,
    previewActive: new Set(),
    previewTimeline: [],
    previewMaxConcurrent: 0,
    requireBearer: false,
    apiRequests: [],
    safeSwapResource: null,
    staleSafeSwapEvent: null,
  };
}

let fixture = makeFixture();
/** @type {Set<ServerResponse>} */
let eventResponses = new Set();

function resetFixture() {
  for (const response of eventResponses) {
    response.end();
  }
  eventResponses = new Set();
  fixture = makeFixture();
}

resetFixture();

/** @param {ServerResponse} response @param {number} status @param {unknown} body */
function sendJson(response, status, body) {
  const payload = JSON.stringify(body);
  response.writeHead(status, {
    "Cache-Control": "no-store",
    "Content-Length": Buffer.byteLength(payload),
    "Content-Type": "application/json; charset=utf-8",
  });
  response.end(payload);
}

/** @param {string} type @param {unknown} data @param {string | null} [subjectSessionId] */
function captureEvent(type, data, subjectSessionId = null) {
  const deliveryId = String(fixture.nextDeliveryId++);
  return {
    schema: "ylx.capture-event.v3",
    sse_delivery_id: deliveryId,
    authority_epoch: fixture.snapshot.authority_epoch,
    source_revision: fixture.snapshot.source_revision,
    type,
    occurred_at: "2026-08-12T02:25:01Z",
    session_id: subjectSessionId,
    data,
  };
}

/** @param {ServerResponse} response @param {ReturnType<typeof captureEvent>} event */
function writeEvent(response, event) {
  response.write(`id: ${event.sse_delivery_id}\n`);
  response.write(`event: ${event.type}\n`);
  response.write(`data: ${JSON.stringify(event)}\n\n`);
}

function broadcastSnapshot() {
  const recording =
    fixture.snapshot.snapshot.active_recording ??
    fixture.snapshot.snapshot.retained_unsuccessful;
  const event = captureEvent(
    "snapshot",
    fixture.snapshot.snapshot,
    recording?.recording_state.session_id ?? null,
  );
  for (const response of eventResponses) {
    writeEvent(response, event);
  }
}

/** @param {ReturnType<typeof captureEvent>} event */
function broadcastEvent(event) {
  for (const response of eventResponses) {
    writeEvent(response, event);
  }
}

/** @returns {CameraFocusStatus | null} */
function currentCameraFocus() {
  return fixture.snapshot.snapshot.runtime.camera_focus ?? fixture.device.runtime.camera_focus;
}

/** @param {{value?: unknown, auto_enabled?: unknown}} body @returns {CameraFocusStatus | null} */
function setCameraFocus(body) {
  const current = currentCameraFocus();
  if (!current) {
    return null;
  }
  const next = { ...current };
  if (body.auto_enabled === true) {
    next.auto_enabled = true;
  } else if (typeof body.value === "number" && Number.isInteger(body.value)) {
    next.value = body.value;
    if (next.auto_supported) {
      next.auto_enabled = false;
    }
  }
  fixture.device.runtime = {
    ...fixture.device.runtime,
    camera_focus: { ...next },
  };
  fixture.snapshot.source_revision += 1;
  fixture.snapshot.snapshot = {
    ...fixture.snapshot.snapshot,
    runtime: {
      ...fixture.snapshot.snapshot.runtime,
      camera_focus: { ...next },
    },
  };
  return next;
}

/** @param {IncomingMessage} request @returns {Promise<unknown>} */
async function readJson(request) {
  const chunks = [];
  for await (const chunk of request) {
    chunks.push(chunk);
  }
  return JSON.parse(Buffer.concat(chunks).toString("utf8"));
}

/** @param {string} displayName */
function setRecording(displayName) {
  fixture.snapshot.source_revision += 1;
  const recordingState = {
    schema: "ylx.recording-state.v1",
    state: "recording",
    authority_epoch: fixture.snapshot.authority_epoch,
    state_revision: fixture.snapshot.source_revision,
    updated_at: "2026-08-12T02:25:01Z",
    session_id: sessionId,
    take_id: takeId,
    display_name: displayName,
    device: fixture.device.device,
    storage: {
      volume_id: volumeId,
      status: "mounted",
      writable: true,
      remaining_bytes: fixture.device.storage.available_bytes,
    },
    progress: {
      elapsed_seconds: 0,
      captured_frames: 0,
      bytes_written: 0,
    },
    diagnostics: [],
  };
  fixture.snapshot.snapshot = {
    ...fixture.snapshot.snapshot,
    device_state: "recording",
    active_recording: {
      generation_id: generationId,
      recording_state: recordingState,
    },
    retained_unsuccessful: null,
  };
}

function setFinalizing() {
  const current = fixture.snapshot.snapshot.active_recording?.recording_state;
  if (!current) {
    return;
  }
  fixture.snapshot.source_revision += 1;
  fixture.snapshot.snapshot = {
    ...fixture.snapshot.snapshot,
    device_state: "finalizing",
    active_recording: {
      generation_id: generationId,
      recording_state: {
        ...current,
        state: "finalizing",
        state_revision: fixture.snapshot.source_revision,
        updated_at: "2026-08-12T02:25:02Z",
      },
    },
  };
}

function setIdleAfterUserStop() {
  fixture.snapshot.source_revision += 1;
  fixture.snapshot.snapshot = {
    ...fixture.snapshot.snapshot,
    device_state: "idle",
    active_recording: null,
    retained_unsuccessful: null,
  };
}

function setFailed() {
  fixture.snapshot.source_revision += 1;
  const recordingState = structuredClone(canonicalFailedRecordingState);
  recordingState.authority_epoch = fixture.snapshot.authority_epoch;
  recordingState.state_revision = fixture.snapshot.source_revision;
  fixture.snapshot.snapshot = {
    ...fixture.snapshot.snapshot,
    device_state: "idle",
    active_recording: null,
    retained_unsuccessful: {
      generation_id: generationId,
      recording_state: recordingState,
    },
  };
  broadcastSnapshot();
}

function advanceProgress() {
  const current = fixture.snapshot.snapshot.active_recording?.recording_state;
  if (!current) {
    return;
  }
  fixture.snapshot.snapshot = {
    ...fixture.snapshot.snapshot,
    active_recording: {
      generation_id: generationId,
      recording_state: {
        ...current,
        updated_at: "2026-08-12T02:25:13Z",
        progress: {
          elapsed_seconds: 12.4,
          captured_frames: 744,
          bytes_written: 44_040_192,
        },
      },
    },
  };
  broadcastEvent(
    captureEvent(
      "progress",
      {
        schema: "ylx.capture-progress-event.v2",
        phase: "recording",
        elapsed_seconds: 12.4,
        completed_units: 744,
        total_units: null,
        unit: "frames",
      },
      sessionId,
    ),
  );
}

function completeSafeSwap() {
  /** @type {SafeSwapReceipt} */
  const receipt = {
    schema: "ylx.safe-swap-receipt.v3",
    session_id: sessionId,
    volume_id: volumeId,
    generation_id: generationId,
    manifest_id: manifestId,
    manifest_sha256: "c".repeat(64),
    sealed_at: "2026-08-12T02:25:03Z",
    released_at: "2026-08-12T02:25:04Z",
    release_state: "device-released",
    open_handle_count: 0,
  };
  fixture.safeSwapResource = {
    schema: "ylx.safe-swap-receipt-resource.v3",
    receipt,
  };
  fixture.snapshot.source_revision += 1;
  fixture.snapshot.snapshot = {
    ...fixture.snapshot.snapshot,
    device_state: "idle",
    active_recording: null,
    retained_unsuccessful: null,
  };
  broadcastSnapshot();
  fixture.staleSafeSwapEvent = captureEvent("safe_swap", receipt, sessionId);
  broadcastEvent(fixture.staleSafeSwapEvent);
}

function completeSafeSwapWithGap() {
  /** @type {SafeSwapReceipt} */
  const receipt = {
    schema: "ylx.safe-swap-receipt.v3",
    session_id: sessionId,
    volume_id: volumeId,
    generation_id: generationId,
    manifest_id: manifestId,
    manifest_sha256: "c".repeat(64),
    sealed_at: "2026-08-12T02:25:03Z",
    released_at: "2026-08-12T02:25:04Z",
    release_state: "device-released",
    open_handle_count: 0,
  };
  fixture.safeSwapResource = {
    schema: "ylx.safe-swap-receipt-resource.v3",
    receipt,
  };
  fixture.snapshot.source_revision += 2;
  fixture.snapshot.snapshot = {
    ...fixture.snapshot.snapshot,
    device_state: "idle",
    active_recording: null,
    retained_unsuccessful: null,
  };
  broadcastEvent(captureEvent("safe_swap", receipt, sessionId));
}

/** @param {"mounted" | "open-handles"} unsafeState */
function broadcastUnsafeSafeSwap(unsafeState) {
  const receipt = {
    schema: "ylx.safe-swap-receipt.v3",
    session_id: sessionId,
    volume_id: volumeId,
    generation_id: generationId,
    manifest_id: manifestId,
    manifest_sha256: "c".repeat(64),
    sealed_at: "2026-08-12T02:25:03Z",
    released_at: "2026-08-12T02:25:04Z",
    release_state: unsafeState === "mounted" ? "mounted" : "unmounted",
    open_handle_count: unsafeState === "open-handles" ? 1 : 0,
  };
  broadcastEvent(captureEvent("safe_swap", receipt, sessionId));
}

function startNewAuthorityRecording() {
  fixture.safeSwapResource = null;
  fixture.snapshot = makeSnapshot();
  fixture.snapshot.authority_epoch = nextAuthorityEpoch;
  fixture.snapshot.source_revision = 1;
  setRecording("新 authority 录制");
  const recording = fixture.snapshot.snapshot.active_recording;
  if (!recording) {
    throw new Error("fixture 未建立新录制");
  }
  recording.generation_id = nextGenerationId;
  recording.recording_state = {
    ...recording.recording_state,
    authority_epoch: nextAuthorityEpoch,
    session_id: nextSessionId,
    take_id: nextTakeId,
  };
  broadcastSnapshot();
}

function mutateRelatedResources() {
  fixture.device.storage.available_bytes = 64 * gibibyte;
  fixture.sessions.items.unshift({
    session_id: "01989f6a-2c00-7a1b-8c2d-3e4f50617299",
    producer_outcome: "sealed",
    take_id: "01989f6a-2c00-7a1b-8c2d-3e4f50617300",
    take_sequence: 3,
    continuation_of: null,
    display_name: "外部客户端封存",
    device: fixture.device.device,
    started_at: "2026-08-12T02:30:00Z",
    ended_at: "2026-08-12T02:31:00Z",
    duration_seconds: 60,
    total_bytes: gibibyte,
    verification: null,
  });
  fixture.snapshot.source_revision += 1;
  broadcastSnapshot();
}

/** @param {IncomingMessage} request @param {ServerResponse} response */
function rejectMissingBearer(request, response) {
  if (!fixture.requireBearer || request.headers.authorization === "Bearer customer-token") {
    return false;
  }
  sendJson(response, 401, {
    schema: "ylx.api-error.v2",
    error: {
      code: "authentication_required",
      message: "需要设备访问令牌",
      request_id: "d52bed36-3a92-42f1-b653-22694a231db6",
      retryable: false,
    },
  });
  return true;
}

const server = createServer(async (request, response) => {
  const url = new URL(request.url ?? "/", "http://127.0.0.1:4173");
  if (url.pathname === "/__ready") {
    response.writeHead(204).end();
    return;
  }

  if (url.pathname === "/__fixture/reset" && request.method === "POST") {
    resetFixture();
    response.writeHead(204).end();
    return;
  }

  if (url.pathname === "/__fixture/config" && request.method === "POST") {
    const config = /** @type {{commandDelayMs?: number, previewDelayMs?: number, requireBearer?: boolean, stopReturns204?: boolean, startProblem?: boolean, stopProblem?: boolean, eventsUnavailable?: boolean, sessionsVolumeUnavailable?: boolean, eventSnapshotDelayMs?: number}} */ (
      await readJson(request)
    );
    if (Number.isFinite(config.commandDelayMs)) {
      fixture.commandDelayMs = Number(config.commandDelayMs);
    }
    if (typeof config.requireBearer === "boolean") {
      fixture.requireBearer = config.requireBearer;
      fixture.device.security_profile = config.requireBearer ? "customer" : "lab";
    }
    if (Number.isFinite(config.previewDelayMs)) {
      fixture.previewDelayMs = Number(config.previewDelayMs);
    }
    if (typeof config.stopReturns204 === "boolean") {
      fixture.stopReturns204 = config.stopReturns204;
    }
    if (typeof config.startProblem === "boolean") {
      fixture.startProblem = config.startProblem;
    }
    if (typeof config.stopProblem === "boolean") {
      fixture.stopProblem = config.stopProblem;
    }
    if (typeof config.eventsUnavailable === "boolean") {
      fixture.eventsUnavailable = config.eventsUnavailable;
    }
    if (typeof config.sessionsVolumeUnavailable === "boolean") {
      fixture.sessionsVolumeUnavailable = config.sessionsVolumeUnavailable;
      if (config.sessionsVolumeUnavailable) {
        fixture.device.capabilities.capture = false;
        fixture.device.storage = {
          volume_id: null,
          total_bytes: 0,
          available_bytes: 0,
          writable: false,
        };
      }
    }
    if (Number.isFinite(config.eventSnapshotDelayMs)) {
      fixture.eventSnapshotDelayMs = Number(config.eventSnapshotDelayMs);
    }
    response.writeHead(204).end();
    return;
  }

  if (url.pathname === "/__fixture/state" && request.method === "POST") {
    const body = /** @type {{deviceState?: string, displayName?: string, broadcast?: boolean}} */ (
      await readJson(request)
    );
    if (body.deviceState === "recording") {
      setRecording(body.displayName ?? "其他客户端录制");
    }
    if (body.broadcast) {
      broadcastSnapshot();
    }
    response.writeHead(204).end();
    return;
  }

  if (url.pathname === "/__fixture/disconnect-events" && request.method === "POST") {
    for (const eventResponse of eventResponses) {
      eventResponse.end();
    }
    eventResponses.clear();
    response.writeHead(204).end();
    return;
  }

  if (url.pathname === "/__fixture/safe-swap" && request.method === "POST") {
    completeSafeSwap();
    response.writeHead(204).end();
    return;
  }

  if (url.pathname === "/__fixture/unsafe-safe-swap" && request.method === "POST") {
    const body = /** @type {{state?: "mounted" | "open-handles"}} */ (await readJson(request));
    broadcastUnsafeSafeSwap(body.state ?? "mounted");
    response.writeHead(204).end();
    return;
  }

  if (url.pathname === "/__fixture/safe-swap-gap" && request.method === "POST") {
    completeSafeSwapWithGap();
    response.writeHead(204).end();
    return;
  }

  if (url.pathname === "/__fixture/new-authority-recording" && request.method === "POST") {
    startNewAuthorityRecording();
    response.writeHead(204).end();
    return;
  }

  if (url.pathname === "/__fixture/stale-safe-swap" && request.method === "POST") {
    if (fixture.staleSafeSwapEvent) {
      broadcastEvent(fixture.staleSafeSwapEvent);
    }
    response.writeHead(204).end();
    return;
  }

  if (url.pathname === "/__fixture/related-resources" && request.method === "POST") {
    mutateRelatedResources();
    response.writeHead(204).end();
    return;
  }

  if (url.pathname === "/__fixture/diagnostic" && request.method === "POST") {
    const diagnostic = canonicalDiagnosticEvent.data.diagnostic;
    broadcastEvent(
      captureEvent(
        "diagnostic",
        { schema: "ylx.capture-diagnostic-event.v2", diagnostic },
        sessionId,
      ),
    );
    response.writeHead(204).end();
    return;
  }

  if (url.pathname === "/__fixture/fail-capture" && request.method === "POST") {
    setFailed();
    response.writeHead(204).end();
    return;
  }

  if (url.pathname === "/__fixture/progress" && request.method === "POST") {
    advanceProgress();
    response.writeHead(204).end();
    return;
  }

  if (url.pathname === "/__fixture/requests") {
    sendJson(response, 200, {
      requests: fixture.apiRequests,
      preview: {
        requests: fixture.previewRequests,
        concurrent: fixture.previewActive.size,
        maxConcurrent: fixture.previewMaxConcurrent,
        timeline: fixture.previewTimeline,
      },
    });
    return;
  }

  if (url.pathname.startsWith("/api/v3/")) {
    fixture.apiRequests.push({
      path: url.pathname,
      authorization: request.headers.authorization ?? null,
      lastEventId:
        typeof request.headers["last-event-id"] === "string"
          ? request.headers["last-event-id"]
          : null,
      idempotencyKey:
        typeof request.headers["idempotency-key"] === "string"
          ? request.headers["idempotency-key"]
          : null,
    });
    if (rejectMissingBearer(request, response)) {
      return;
    }
  }

  if (url.pathname === "/api/v3/device") {
    sendJson(response, 200, fixture.device);
    return;
  }

  if (url.pathname === "/api/v3/capture/status") {
    sendJson(response, 200, fixture.snapshot);
    return;
  }

  if (url.pathname === "/api/v3/camera/focus") {
    const current = currentCameraFocus();
    if (!current) {
      sendJson(response, 404, {
        schema: "ylx.api-error.v2",
        error: {
          code: "camera_focus_unsupported",
          message: "当前相机没有可读取的焦距控制",
          request_id: "ff5dd970-a6db-4872-91da-f28c0cd12b70",
          retryable: false,
        },
      });
      return;
    }
    if (request.method === "GET") {
      sendJson(response, 200, current);
      return;
    }
    if (request.method === "POST") {
      const body = /** @type {{schema?: string, value?: unknown, auto_enabled?: unknown}} */ (
        await readJson(request)
      );
      const hasValue = Object.hasOwn(body, "value");
      const hasAuto = Object.hasOwn(body, "auto_enabled");
      if (
        !request.headers["idempotency-key"] ||
        body.schema !== "ylx.camera-focus-set.v1" ||
        (!hasValue && !hasAuto) ||
        (hasValue && (typeof body.value !== "number" || !Number.isInteger(body.value))) ||
        (hasAuto && typeof body.auto_enabled !== "boolean")
      ) {
        sendJson(response, 400, {
          schema: "ylx.api-error.v2",
          error: {
            code: "invalid_camera_focus",
            message: "焦距请求不符合 v3 契约",
            request_id: "f347fe47-1556-4c1c-b855-90f3fa9733bd",
            retryable: false,
          },
        });
        return;
      }
      const next = setCameraFocus(body);
      sendJson(response, 200, next);
      broadcastSnapshot();
      return;
    }
  }

  if (url.pathname === "/api/v3/preview") {
    fixture.previewRequests += 1;
    const requestId = fixture.previewRequests;
    fixture.previewActive.add(requestId);
    fixture.previewTimeline.push({
      requestId,
      phase: "start",
      at: performance.now(),
      method: request.method,
      accept: request.headers.accept ?? null,
      destination: request.headers["sec-fetch-dest"] ?? null,
      referer: request.headers.referer ?? null,
    });
    fixture.previewMaxConcurrent = Math.max(
      fixture.previewMaxConcurrent,
      fixture.previewActive.size,
    );
    await new Promise((resolveDelay) => setTimeout(resolveDelay, fixture.previewDelayMs));
    response.writeHead(200, {
      "Cache-Control": "no-store",
      "Content-Length": previewJpeg.length,
      "Content-Type": "image/jpeg",
      "X-YLX-Preview-Sequence": String(requestId),
    });
    response.end(previewJpeg);
    fixture.previewActive.delete(requestId);
    fixture.previewTimeline.push({ requestId, phase: "end", at: performance.now() });
    return;
  }

  if (url.pathname === "/api/v3/capture/start" && request.method === "POST") {
    const body = /** @type {{schema?: string, mode?: string, take?: {kind?: string}, display_name?: string}} */ (
      await readJson(request)
    );
    if (
      !request.headers["idempotency-key"] ||
      body.schema !== "ylx.capture-start.v2" ||
      body.mode !== "production" ||
      body.take?.kind !== "new"
    ) {
      sendJson(response, 400, {
        schema: "ylx.api-error.v2",
        error: {
          code: "invalid_capture_start",
          message: "录制请求不符合 v3 契约",
          request_id: "f91cd40f-5715-46be-8fa8-cc67b58d1572",
          retryable: false,
        },
      });
      return;
    }
    if (fixture.startProblem) {
      sendJson(response, 409, canonicalCaptureBusyProblem);
      return;
    }
    await new Promise((resolveDelay) => setTimeout(resolveDelay, fixture.commandDelayMs));
    setRecording(body.display_name ?? "未命名录制");
    sendJson(response, 202, fixture.snapshot);
    broadcastSnapshot();
    return;
  }

  if (url.pathname === "/api/v3/capture/stop" && request.method === "POST") {
    const body = /** @type {{schema?: string, reason?: string}} */ (await readJson(request));
    if (
      !request.headers["idempotency-key"] ||
      body.schema !== "ylx.capture-stop.v2" ||
      !["user", "safe_swap"].includes(body.reason ?? "")
    ) {
      sendJson(response, 400, {
        schema: "ylx.api-error.v2",
        error: {
          code: "invalid_capture_stop",
          message: "结束请求不符合 v3 契约",
          request_id: "024d4a33-8d89-43f8-87e6-413fc0c8aaef",
          retryable: false,
        },
      });
      return;
    }
    if (fixture.stopProblem) {
      sendJson(response, 503, {
        schema: "ylx.api-error.v2",
        error: {
          code: "storage_finalize_failed",
          message: "存储封存暂时失败，请重试",
          request_id: "024d4a33-8d89-43f8-87e6-413fc0c8aaef",
          retryable: true,
          details: { volume_id: volumeId },
        },
      });
      return;
    }
    if (!fixture.snapshot.snapshot.active_recording) {
      response.writeHead(204).end();
      return;
    }
    if (fixture.stopReturns204) {
      setIdleAfterUserStop();
      response.writeHead(204).end();
      return;
    }
    setFinalizing();
    sendJson(response, 202, fixture.snapshot);
    broadcastSnapshot();
    if (body.reason === "user") {
      setTimeout(() => {
        setIdleAfterUserStop();
        broadcastSnapshot();
      }, 80);
    }
    return;
  }

  if (url.pathname === "/api/v3/sessions") {
    if (fixture.sessionsVolumeUnavailable) {
      sendJson(response, 409, {
        schema: "ylx.api-error.v2",
        error: {
          code: "volume_not_mounted",
          message: "录制卷不是当前活动挂载点",
          request_id: "962f25f5-c8f1-42cb-a598-4143a806d89f",
          retryable: true,
          details: {},
        },
      });
      return;
    }
    sendJson(response, 200, fixture.sessions);
    return;
  }

  if (url.pathname === "/api/v3/capture/safe-swap") {
    if (fixture.safeSwapResource) {
      sendJson(response, 200, fixture.safeSwapResource);
      return;
    }
    sendJson(response, 404, {
      schema: "ylx.api-error.v2",
      error: {
        code: "safe_swap_receipt_not_found",
        message: "当前没有安全换盘回执",
        request_id: "dd268911-b58f-46ef-8670-ae98239c2a33",
        retryable: false,
      },
    });
    return;
  }

  if (url.pathname === "/api/v3/capture/events") {
    if (fixture.eventsUnavailable) {
      sendJson(response, 503, {
        schema: "ylx.api-error.v2",
        error: {
          code: "event_stream_unavailable",
          message: "设备事件流暂不可用",
          request_id: "0c0e7364-787c-4a97-b7ef-79674089d6c2",
          retryable: true,
          details: {},
        },
      });
      return;
    }
    response.writeHead(200, {
      "Cache-Control": "no-cache",
      "Content-Type": "text/event-stream; charset=utf-8",
      Connection: "keep-alive",
      "X-Accel-Buffering": "no",
    });
    response.flushHeaders();
    eventResponses.add(response);
    if (fixture.eventSnapshotDelayMs > 0) {
      await new Promise((resolveDelay) =>
        setTimeout(resolveDelay, fixture.eventSnapshotDelayMs),
      );
    }
    writeEvent(response, captureEvent("snapshot", fixture.snapshot.snapshot));
    request.on("close", () => eventResponses.delete(response));
    return;
  }

  const requested = url.pathname === "/" ? "index.html" : url.pathname.slice(1);
  const filePath = resolve(webRoot, requested);
  if (!filePath.startsWith(`${webRoot}${sep}`)) {
    response.writeHead(404).end();
    return;
  }

  try {
    const metadata = await stat(filePath);
    if (!metadata.isFile()) {
      throw new Error("not a file");
    }
    response.writeHead(200, {
      "Cache-Control": "no-store",
      "Content-Type": contentTypes[extname(filePath)] ?? "application/octet-stream",
    });
    createReadStream(filePath).pipe(response);
  } catch {
    response.writeHead(404, { "Content-Type": "text/plain; charset=utf-8" });
    response.end("Not found");
  }
});

server.listen(4173, "127.0.0.1");

for (const signal of ["SIGINT", "SIGTERM"]) {
  process.on(signal, () => server.close(() => process.exit(0)));
}

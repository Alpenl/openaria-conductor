// @ts-check

import { DeviceApiError, deviceApi, setAccessToken } from "./api-client.js";
import { followCaptureEvents } from "./event-stream.js";
import { followLatestPreview } from "./preview.js";
import { initialState, reduceState } from "./state.js";

/** @type {Record<string, string>} */
const captureLabels = {
  abandoned: "已放弃",
  blocked: "受阻",
  encoding: "编码中",
  finalizing: "正在结束",
  idle: "待机",
  recording: "录制中",
  recoverable: "可恢复失败",
  failed: "失败",
  verifying: "校验中",
};

/** @type {Record<string, string>} */
const connectionLabels = {
  connected: "已连接",
  connecting: "正在连接",
  disconnected: "连接中断",
};

/** @type {Record<string, string>} */
const connectionMethodLabels = {
  ethernet_direct: "直连网线",
  ethernet_lan: "局域网网线",
  offline: "离线",
  wifi_ap: "设备热点",
  wifi_client: "Wi-Fi",
};

/** @type {import("./state.js").AppState} */
let state = initialState;

/** @type {AbortController | null} */
let eventController = null;
/** @type {AbortController | null} */
let previewController = null;

/** @param {string} selector @returns {Element} */
function element(selector) {
  const match = document.querySelector(selector);
  if (!match) {
    throw new Error(`页面缺少元素 ${selector}`);
  }
  return match;
}

/** @param {number | null | undefined} bytes */
function formatBytes(bytes) {
  if (!Number.isFinite(bytes)) {
    return "--";
  }
  return `${(Number(bytes) / 1024 ** 3).toFixed(1)} GiB`;
}

/** @param {number | null | undefined} value */
function formatNumber(value) {
  return Number.isFinite(value) ? Number(value).toFixed(3) : "--";
}

/** @param {{x?: number, y?: number, z?: number} | null | undefined} vector @param {string} unit */
function formatVector(vector, unit) {
  if (!vector || ![vector.x, vector.y, vector.z].every(Number.isFinite)) {
    return "不可用";
  }
  return `x ${formatNumber(vector.x)}  y ${formatNumber(vector.y)}  z ${formatNumber(vector.z)} ${unit}`;
}

function render() {
  const connection = element(".connection");
  connection.textContent = connectionLabels[state.connection] ?? "状态未知";
  connection.setAttribute("data-state", state.connection);

  if (state.device) {
    element("#device-label").textContent = state.device.device?.device_label ?? "录制设备";
    element('[data-testid="storage-available"]').textContent = formatBytes(
      state.device.storage?.available_bytes,
    );
    element('[data-testid="storage-writable"]').textContent = state.device.storage?.writable
      ? "可写"
      : "不可写";
  }

  const snapshot = state.capture?.snapshot;
  if (!snapshot) {
    renderDiagnostics();
    return;
  }

  const retainedState = snapshot.retained_unsuccessful?.recording_state;
  element('[data-testid="capture-state"]').textContent =
    captureLabels[retainedState?.state ?? snapshot.device_state] ?? "状态未知";
  const command = /** @type {HTMLButtonElement} */ (element("#capture-command"));
  const nameInput = /** @type {HTMLInputElement} */ (element("#capture-name"));
  const canStart =
    state.connection === "connected" &&
    snapshot.device_state === "idle" &&
    state.device?.capabilities?.capture === true &&
    state.device?.storage?.writable === true;
  command.textContent = state.commandPending
    ? "正在发送"
    : snapshot.device_state === "idle"
      ? "开始录制"
      : "录制进行中";
  command.disabled = state.commandPending || !canStart;
  nameInput.disabled =
    state.connection !== "connected" || state.commandPending || snapshot.device_state !== "idle";
  const stopActions = /** @type {HTMLElement} */ (element(".stop-actions"));
  const stopCommand = /** @type {HTMLButtonElement} */ (element("#stop-command"));
  const safeSwapCommand = /** @type {HTMLButtonElement} */ (element("#safe-swap-command"));
  const isActive = ["recording", "finalizing", "encoding", "verifying"].includes(
    snapshot.device_state,
  );
  stopActions.hidden = !isActive;
  stopCommand.disabled =
    state.connection !== "connected" || state.commandPending || snapshot.device_state !== "recording";
  safeSwapCommand.disabled =
    state.connection !== "connected" || state.commandPending || snapshot.device_state !== "recording";

  const recordingState = snapshot.active_recording?.recording_state;
  const currentSession = /** @type {HTMLElement} */ (element(".current-session"));
  currentSession.hidden = !recordingState;
  element('[data-testid="current-session-name"]').textContent = recordingState?.display_name ?? "";
  element('[data-testid="elapsed-seconds"]').textContent = `${Number(
    recordingState?.progress.elapsed_seconds ?? 0,
  ).toFixed(1)} 秒`;
  element('[data-testid="captured-frames"]').textContent = String(
    recordingState?.progress.captured_frames ?? 0,
  );
  element('[data-testid="bytes-written"]').textContent = `${(
    Number(recordingState?.progress.bytes_written ?? 0) /
    1024 ** 2
  ).toFixed(1)} MiB`;
  const runtime = snapshot.runtime;
  element('[data-testid="temperature"]').textContent = Number.isFinite(
    runtime?.temperature_celsius,
  )
    ? `${Number(runtime.temperature_celsius).toFixed(1)} °C`
    : "不可用";
  element('[data-testid="connection-method"]').textContent =
    connectionMethodLabels[runtime?.connection_method] ?? "未知";

  const imu = runtime?.live_imu;
  element('[data-testid="acceleration"]').textContent = formatVector(
    imu?.acceleration_m_s2,
    "m/s²",
  );
  element('[data-testid="angular-velocity"]').textContent = formatVector(
    imu?.angular_velocity_rad_s,
    "rad/s",
  );
  element('[data-testid="orientation"]').textContent = imu?.orientation_quaternion
    ? `w ${formatNumber(imu.orientation_quaternion.w)}  x ${formatNumber(imu.orientation_quaternion.x)}  y ${formatNumber(imu.orientation_quaternion.y)}  z ${formatNumber(imu.orientation_quaternion.z)}`
    : "不可用";

  const safeSwap = /** @type {HTMLElement} */ (element(".safe-swap"));
  safeSwap.hidden = !state.safeSwapReceipt;
  if (state.safeSwapReceipt) {
    const receipt = state.safeSwapReceipt.receipt;
    element('[data-testid="safe-swap-release"]').textContent =
      receipt.release_state === "device-released" ? "设备已释放" : "已卸载";
    element('[data-testid="safe-swap-session"]').textContent = receipt.session_id;
  }

  renderSessions();
  renderDiagnostics();
}

function renderSessions() {
  const container = element("#session-list");
  if (!state.sessions) {
    return;
  }
  container.replaceChildren();
  if (state.sessions.items.length === 0 && state.sessions.diagnostics.length === 0) {
    const empty = document.createElement("p");
    empty.className = "empty-state";
    empty.textContent = "暂无会话";
    container.append(empty);
    return;
  }
  for (const session of state.sessions.items) {
    const item = document.createElement("article");
    item.className = "session-item";
    item.dataset.testid = "session-item";

    const titleRow = document.createElement("div");
    titleRow.className = "session-title-row";
    const title = document.createElement("strong");
    title.textContent = session.display_name;
    const verdict = document.createElement("span");
    verdict.className = "verdict";
    verdict.dataset.verdict = session.verification?.verdict ?? "unknown";
    verdict.textContent =
      session.verification?.verdict === "usable"
        ? "可用"
        : session.verification?.verdict === "unusable"
          ? "不可用"
          : "尚未校验";
    titleRow.append(title, verdict);

    const meta = document.createElement("div");
    meta.className = "session-meta";
    const producer = document.createElement("span");
    producer.textContent = "已封存";
    const identity = document.createElement("code");
    identity.textContent = session.session_id;
    identity.title = session.session_id;
    meta.append(producer, identity);
    item.append(titleRow, meta);
    container.append(item);
  }
  for (const diagnostic of state.sessions.diagnostics) {
    const warning = document.createElement("div");
    warning.className = "discovery-diagnostic";
    warning.textContent = diagnostic.message;
    warning.title = diagnostic.code;
    container.append(warning);
  }
}

function renderDiagnostics() {
  const container = element(".alerts");
  container.replaceChildren();
  const alerts = state.error ? [state.error, ...state.diagnostics] : state.diagnostics;
  for (const diagnostic of alerts) {
    const alert = document.createElement("div");
    alert.className = "alert-item";
    alert.setAttribute("role", "alert");
    const code = document.createElement("code");
    code.textContent = diagnostic.code;
    const message = document.createElement("span");
    message.textContent = diagnostic.message;
    alert.append(code, message);
    if ("at" in diagnostic && typeof diagnostic.at === "string") {
      const occurredAt = document.createElement("time");
      occurredAt.dateTime = diagnostic.at;
      occurredAt.textContent = diagnostic.at;
      alert.append(occurredAt);
    }
    if (diagnostic.details) {
      const details = document.createElement("pre");
      details.textContent = JSON.stringify(diagnostic.details);
      alert.append(details);
    }
    container.append(alert);
  }
}

/** @param {unknown} error */
function visibleError(error) {
  if (error instanceof DeviceApiError) {
    return { code: error.code, message: error.message, details: error.details };
  }
  return {
    code: "command_failed",
    message: error instanceof Error ? error.message : "命令失败",
  };
}

/** @param {import("./state.js").Action} action */
function dispatch(action) {
  state = reduceState(state, action);
  if (action.type === "capture.snapshot") {
    const retainedDiagnostics = action.payload.snapshot.retained_unsuccessful?.recording_state
      .diagnostics;
    if (retainedDiagnostics?.length) {
      const existing = new Set(
        state.diagnostics.map((diagnostic) => `${diagnostic.code}|${diagnostic.at}`),
      );
      for (const diagnostic of retainedDiagnostics) {
        if (!existing.has(`${diagnostic.code}|${diagnostic.at}`)) {
          state = reduceState(state, { type: "diagnostic.received", payload: diagnostic });
        }
      }
    }
  }
  render();
}

async function loadInitialState() {
  try {
    const [device, capture, safeSwap, sessions] = await Promise.all([
      deviceApi.getDevice(),
      deviceApi.getCaptureStatus(),
      deviceApi.getSafeSwap(),
      deviceApi.listSessions(),
    ]);
    dispatch({ type: "device.loaded", payload: device });
    dispatch({ type: "capture.snapshot", payload: capture });
    if (safeSwap?.schema === "ylx.safe-swap-receipt-resource.v3") {
      acceptSafeSwapReceipt(safeSwap.receipt, capture.authority_epoch, capture.source_revision);
    } else {
      dispatch({ type: "safe-swap.cleared" });
    }
    dispatch({ type: "sessions.loaded", payload: sessions });
    dispatch({ type: "error.cleared" });
    /** @type {HTMLElement} */ (element(".credential-prompt")).hidden = true;
    return true;
  } catch (error) {
    if (error instanceof DeviceApiError && error.status === 401) {
      /** @type {HTMLElement} */ (element(".credential-prompt")).hidden = false;
    }
    dispatch({
      type: "connection.failed",
      error:
        error instanceof DeviceApiError
          ? visibleError(error)
          : { code: "connection_failed", message: "连接失败" },
    });
    return false;
  }
}

/**
 * @param {any} receipt
 * @param {string} authorityEpoch
 * @param {number} sourceRevision
 * @param {string | null} [subjectSessionId]
 */
function acceptSafeSwapReceipt(receipt, authorityEpoch, sourceRevision, subjectSessionId = null) {
  const current = state.capture;
  const recording =
    current?.snapshot.active_recording ?? current?.snapshot.retained_unsuccessful;
  const identitiesMatch =
    !recording ||
    (recording.generation_id === receipt?.generation_id &&
      recording.recording_state.session_id === receipt?.session_id &&
      recording.recording_state.storage.volume_id === receipt?.volume_id);
  const isReleased = ["unmounted", "device-released"].includes(receipt?.release_state);
  if (
    receipt?.schema !== "ylx.safe-swap-receipt.v3" ||
    !current ||
    current.authority_epoch !== authorityEpoch ||
    current.source_revision !== sourceRevision ||
    state.device?.storage.volume_id !== receipt.volume_id ||
    (subjectSessionId !== null && subjectSessionId !== receipt.session_id) ||
    !identitiesMatch ||
    !isReleased ||
    receipt.open_handle_count !== 0
  ) {
    return false;
  }
  dispatch({
    type: "safe-swap.received",
    payload: { receipt, authorityEpoch, sourceRevision },
  });
  return true;
}

/** @type {Promise<void> | null} */
let captureRefresh = null;
/** @type {Promise<void> | null} */
let relatedResourcesRefresh = null;

async function refreshCapture() {
  if (!captureRefresh) {
    captureRefresh = deviceApi
      .getCaptureStatus()
      .then((capture) => dispatch({ type: "capture.snapshot", payload: capture }))
      .finally(() => {
        captureRefresh = null;
      });
  }
  await captureRefresh;
}

async function refreshRelatedResources() {
  if (!relatedResourcesRefresh) {
    relatedResourcesRefresh = Promise.all([
      deviceApi.getDevice(),
      deviceApi.getSafeSwap(),
      deviceApi.listSessions(),
    ])
      .then(([device, safeSwap, sessions]) => {
        dispatch({ type: "device.loaded", payload: device });
        if (safeSwap?.schema === "ylx.safe-swap-receipt-resource.v3" && state.capture) {
          acceptSafeSwapReceipt(
            safeSwap.receipt,
            state.capture.authority_epoch,
            state.capture.source_revision,
          );
        } else {
          dispatch({ type: "safe-swap.cleared" });
        }
        dispatch({ type: "sessions.loaded", payload: sessions });
      })
      .catch((error) => console.warn(error))
      .finally(() => {
        relatedResourcesRefresh = null;
      });
  }
  await relatedResourcesRefresh;
}

/** @param {any} event */
async function acceptCaptureEvent(event) {
  if (event.type === "progress") {
    await refreshCapture();
    return;
  }
  const current = state.capture;
  const isNextSnapshot =
    event.type === "snapshot" &&
    current &&
    event.authority_epoch === current.authority_epoch &&
    event.source_revision === current.source_revision + 1;
  if (isNextSnapshot || (event.type === "snapshot" && !current)) {
    dispatch({
      type: "capture.snapshot",
      payload: {
        schema: "ylx.capture-status.v2",
        authority_epoch: event.authority_epoch,
        source_revision: event.source_revision,
        snapshot: event.data,
      },
    });
    await refreshRelatedResources();
    return;
  }
  const matchesCurrent =
    current &&
    event.authority_epoch === current.authority_epoch &&
    event.source_revision === current.source_revision;
  if (matchesCurrent && event.type === "safe_swap") {
    const accepted = acceptSafeSwapReceipt(
      event.data,
      event.authority_epoch,
      event.source_revision,
      event.session_id,
    );
    if (accepted) {
      await refreshRelatedResources();
    }
    return;
  }
  if (
    matchesCurrent &&
    event.type === "diagnostic" &&
    event.data?.schema === "ylx.capture-diagnostic-event.v2"
  ) {
    dispatch({ type: "diagnostic.received", payload: event.data.diagnostic });
    return;
  }
  await refreshCapture();
  if (event.type === "safe_swap" || event.type === "snapshot") {
    await refreshRelatedResources();
  }
}

/** @param {"user" | "safe_swap"} reason */
async function stopCapture(reason) {
  if (state.commandPending || state.connection !== "connected") {
    return;
  }
  dispatch({ type: "command.pending" });
  try {
    const capture = await deviceApi.stopCapture(reason);
    if (capture) {
      dispatch({ type: "capture.snapshot", payload: capture });
    }
    await refreshCapture();
    await refreshRelatedResources();
    dispatch({ type: "command.succeeded" });
  } catch (error) {
    dispatch({
      type: "command.failed",
      error: visibleError(error),
    });
  } finally {
    dispatch({ type: "command.settled" });
  }
}

function startEventStream() {
  if (eventController) {
    return;
  }
  eventController = new AbortController();
  window.addEventListener("pagehide", () => eventController?.abort(), { once: true });
  void followCaptureEvents({
    signal: eventController.signal,
    onEvent: acceptCaptureEvent,
    onConnection: (connection) => dispatch({ type: "connection.changed", connection }),
    onUnauthorized: (error) => {
      eventController = null;
      previewController?.abort();
      previewController = null;
      /** @type {HTMLElement} */ (element(".credential-prompt")).hidden = false;
      dispatch({ type: "connection.failed", error: visibleError(error) });
    },
  });
}

function startPreview() {
  if (previewController) {
    return;
  }
  previewController = new AbortController();
  window.addEventListener("pagehide", () => previewController?.abort(), { once: true });
  const image = /** @type {HTMLImageElement} */ (element('[data-testid="preview-image"]'));
  void followLatestPreview({
    image,
    status: element("#preview-status"),
    signal: previewController.signal,
  });
}

function startLiveConnections() {
  startEventStream();
  if (state.device?.capabilities.preview) {
    startPreview();
  }
}

/** @param {Event} event */
async function submitCredentials(event) {
  event.preventDefault();
  const form = event.currentTarget;
  if (!(form instanceof HTMLFormElement)) {
    return;
  }
  const token = String(new FormData(form).get("access_token") ?? "").trim();
  if (!token) {
    return;
  }
  const button = /** @type {HTMLButtonElement} */ (form.querySelector('button[type="submit"]'));
  button.disabled = true;
  setAccessToken(token);
  const connected = await loadInitialState();
  button.disabled = false;
  if (connected) {
    form.reset();
    startLiveConnections();
  }
}

/** @param {Event} event */
async function submitCapture(event) {
  event.preventDefault();
  const form = event.currentTarget;
  if (
    !(form instanceof HTMLFormElement) ||
    state.commandPending ||
    state.connection !== "connected"
  ) {
    return;
  }
  const displayName = String(new FormData(form).get("display_name") ?? "").trim();
  if (!displayName) {
    return;
  }
  dispatch({ type: "command.pending" });
  try {
    const capture = await deviceApi.startCapture(displayName);
    dispatch({ type: "capture.snapshot", payload: capture });
    await refreshRelatedResources();
    dispatch({ type: "command.succeeded" });
  } catch (error) {
    dispatch({
      type: "command.failed",
      error: visibleError(error),
    });
  } finally {
    dispatch({ type: "command.settled" });
  }
}

render();
/** @type {HTMLFormElement} */ (element("#credential-form")).addEventListener(
  "submit",
  submitCredentials,
);
/** @type {HTMLFormElement} */ (element("#capture-form")).addEventListener("submit", submitCapture);
element("#stop-command").addEventListener("click", () => void stopCapture("user"));
element("#safe-swap-command").addEventListener("click", () => void stopCapture("safe_swap"));
void loadInitialState().then((connected) => {
  if (connected) {
    startLiveConnections();
  }
});

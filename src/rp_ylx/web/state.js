// @ts-check

/** @typedef {{x: number, y: number, z: number}} RawVector3 */
/**
 * @typedef {object} NetworkInterfaceStatus
 * @property {string} state
 * @property {string | null} interface
 * @property {string[]} addresses
 * @property {string | null} peer_or_ssid
 */
/**
 * @typedef {object} LiveImu
 * @property {string} session_id
 * @property {{time_base: "host_monotonic", timestamp_ns: number}} clock
 * @property {{units: "raw_int16", accelerometer: RawVector3, gyroscope: RawVector3}} raw
 * @property {{quality: "insufficient" | "good" | "degraded"}} sync
 */
/**
 * @typedef {object} DeviceRuntime
 * @property {string} observed_at
 * @property {string} connection_method
 * @property {number} temperature_celsius
 * @property {{ap: NetworkInterfaceStatus, wifi_client: NetworkInterfaceStatus, wired: NetworkInterfaceStatus, default_route: string}} network
 * @property {LiveImu | null} live_imu
 */
/**
 * @typedef {object} RecordingState
 * @property {string} schema
 * @property {string} state
 * @property {string} authority_epoch
 * @property {number} state_revision
 * @property {string} updated_at
 * @property {string} session_id
 * @property {string} take_id
 * @property {string} display_name
 * @property {{device_id: string, device_label: string}} device
 * @property {{volume_id: string, status: string, writable: boolean, remaining_bytes: number | null}} storage
 * @property {Array<{code: string, severity: string, message: string, at: string, recoverable: boolean, details?: Record<string, unknown>}>} diagnostics
 * @property {{elapsed_seconds: number, captured_frames: number, bytes_written: number, encoding?: {completed: number, total: number}, verification?: {completed: number, total: number}}} progress
 */
/**
 * @typedef {object} CaptureSnapshot
 * @property {string} schema
 * @property {string} device_state
 * @property {{generation_id: string, recording_state: RecordingState} | null} active_recording
 * @property {{generation_id: string, recording_state: RecordingState} | null} retained_unsuccessful
 * @property {DeviceRuntime} runtime
 */
/**
 * @typedef {object} CaptureStatus
 * @property {string} schema
 * @property {string} authority_epoch
 * @property {number} source_revision
 * @property {CaptureSnapshot} snapshot
 */
/**
 * @typedef {object} DeviceDescriptor
 * @property {{device_id: string, device_label: string}} device
 * @property {{capture: boolean, preview: boolean, range_download: boolean, network_mutation: boolean}} capabilities
 * @property {{volume_id: string | null, total_bytes: number, available_bytes: number, writable: boolean}} storage
 */
/**
 * @typedef {object} AppState
 * @property {"connecting" | "connected" | "disconnected"} connection
 * @property {DeviceDescriptor | null} device
 * @property {CaptureStatus | null} capture
 * @property {SafeSwapState | null} safeSwapReceipt
 * @property {SessionList | null} sessions
 * @property {Diagnostic[]} diagnostics
 * @property {boolean} commandPending
 * @property {{code: string, message: string, details?: Record<string, unknown>} | null} error
 */
/**
 * @typedef {{type: "device.loaded", payload: DeviceDescriptor} |
 *   {type: "capture.snapshot", payload: CaptureStatus} |
 *   {type: "command.pending"} |
 *   {type: "command.settled"} |
 *   {type: "command.succeeded"} |
 *   {type: "command.failed", error: {code: string, message: string, details?: Record<string, unknown>}} |
 *   {type: "error.cleared"} |
 *   {type: "safe-swap.received", payload: SafeSwapState} |
 *   {type: "safe-swap.cleared"} |
 *   {type: "sessions.loaded", payload: SessionList} |
 *   {type: "diagnostic.received", payload: Diagnostic} |
 *   {type: "connection.changed", connection: AppState["connection"]} |
 *   {type: "connection.failed", error: {code: string, message: string, details?: Record<string, unknown>}}} Action
 */

/**
 * @typedef {object} SafeSwapReceipt
 * @property {"ylx.safe-swap-receipt.v3"} schema
 * @property {string} session_id
 * @property {string} volume_id
 * @property {string} generation_id
 * @property {string} manifest_id
 * @property {string} manifest_sha256
 * @property {string} sealed_at
 * @property {string} released_at
 * @property {"unmounted" | "device-released"} release_state
 * @property {0} open_handle_count
 */

/**
 * @typedef {object} SafeSwapState
 * @property {SafeSwapReceipt} receipt
 * @property {string} authorityEpoch
 * @property {number} sourceRevision
 */

/**
 * @typedef {object} Diagnostic
 * @property {string} code
 * @property {string} severity
 * @property {string} message
 * @property {string} at
 * @property {boolean} recoverable
 * @property {Record<string, unknown>} [details]
 */
/**
 * @typedef {object} SessionSummary
 * @property {string} session_id
 * @property {"sealed"} producer_outcome
 * @property {string} display_name
 * @property {number} duration_seconds
 * @property {number} total_bytes
 * @property {{verdict: "usable" | "unusable", diagnostics: string[]} | null} verification
 */
/**
 * @typedef {object} SessionList
 * @property {SessionSummary[]} items
 * @property {Array<{quarantine_id: string, code: string, observed_at: string, message: string}>} diagnostics
 * @property {string | null} next_cursor
 */

/** @type {AppState} */
export const initialState = {
  connection: "connecting",
  device: null,
  capture: null,
  safeSwapReceipt: null,
  sessions: null,
  diagnostics: [],
  commandPending: false,
  error: null,
};

/** @param {SafeSwapState | null} safeSwap @param {CaptureStatus} capture */
function receiptMatchesCapture(safeSwap, capture) {
  if (!safeSwap || safeSwap.authorityEpoch !== capture.authority_epoch) {
    return false;
  }
  const recording =
    capture.snapshot.active_recording ?? capture.snapshot.retained_unsuccessful;
  if (!recording) {
    return true;
  }
  return (
    recording.generation_id === safeSwap.receipt.generation_id &&
    recording.recording_state.session_id === safeSwap.receipt.session_id &&
    recording.recording_state.storage.volume_id === safeSwap.receipt.volume_id
  );
}

/** @param {CaptureStatus | null} current @param {CaptureStatus} incoming */
function isStaleCaptureSnapshot(current, incoming) {
  return (
    current !== null &&
    incoming.authority_epoch === current.authority_epoch &&
    incoming.source_revision < current.source_revision
  );
}

/** @param {AppState} state @param {Action} action @returns {AppState} */
export function reduceState(state, action) {
  if (action.type === "device.loaded") {
    const safeSwapReceipt =
      state.safeSwapReceipt?.receipt.volume_id === action.payload.storage.volume_id
        ? state.safeSwapReceipt
        : null;
    return { ...state, device: action.payload, safeSwapReceipt };
  }
  if (action.type === "capture.snapshot") {
    if (isStaleCaptureSnapshot(state.capture, action.payload)) {
      return state;
    }
    return {
      ...state,
      capture: action.payload,
      safeSwapReceipt: receiptMatchesCapture(state.safeSwapReceipt, action.payload)
        ? state.safeSwapReceipt
        : null,
    };
  }
  if (action.type === "command.pending") {
    return { ...state, commandPending: true, error: null };
  }
  if (action.type === "command.settled") {
    return { ...state, commandPending: false };
  }
  if (action.type === "command.succeeded") {
    return { ...state, error: null };
  }
  if (action.type === "command.failed") {
    return { ...state, error: action.error };
  }
  if (action.type === "error.cleared") {
    return { ...state, error: null };
  }
  if (action.type === "safe-swap.received") {
    return { ...state, safeSwapReceipt: action.payload };
  }
  if (action.type === "safe-swap.cleared") {
    return { ...state, safeSwapReceipt: null };
  }
  if (action.type === "sessions.loaded") {
    return { ...state, sessions: action.payload };
  }
  if (action.type === "diagnostic.received") {
    return { ...state, diagnostics: [...state.diagnostics.slice(-3), action.payload] };
  }
  if (action.type === "connection.changed") {
    return { ...state, connection: action.connection };
  }
  if (action.type === "connection.failed") {
    return { ...state, connection: "disconnected", error: action.error };
  }
  return state;
}

// @ts-check

const API_ROOT = "/api/v3";
const TOKEN_KEY = "rp-ylx-access-token";

export class DeviceApiError extends Error {
  /**
   * @param {string} message
   * @param {number} status
   * @param {string} [code]
   * @param {Record<string, unknown>} [details]
   */
  constructor(message, status, code = `http_${status}`, details = {}) {
    super(message);
    this.name = "DeviceApiError";
    this.status = status;
    this.code = code;
    this.details = details;
  }
}

/** @param {Response} response */
export async function makeApiError(response) {
  let problem = null;
  try {
    problem = await response.json();
  } catch {
    // A non-JSON failure still gets a stable local code and message.
  }
  const error = problem?.schema === "ylx.api-error.v2" ? problem.error : null;
  return new DeviceApiError(
    typeof error?.message === "string" ? error.message : `设备接口返回 ${response.status}`,
    response.status,
    typeof error?.code === "string" ? error.code : `http_${response.status}`,
    error?.details && typeof error.details === "object" ? error.details : {},
  );
}

/** @param {number} milliseconds @param {AbortSignal} signal */
export function waitForAbortableDelay(milliseconds, signal) {
  return new Promise((resolve) => {
    /** @type {number | null} */
    let timeout = null;
    const finish = () => {
      if (timeout !== null) {
        window.clearTimeout(timeout);
      }
      signal.removeEventListener("abort", finish);
      resolve(undefined);
    };
    if (signal.aborted) {
      finish();
      return;
    }
    timeout = window.setTimeout(finish, milliseconds);
    signal.addEventListener("abort", finish, { once: true });
  });
}

export function getAccessToken() {
  return sessionStorage.getItem(TOKEN_KEY)?.trim() || null;
}

/** @param {string} token */
export function setAccessToken(token) {
  sessionStorage.setItem(TOKEN_KEY, token.trim());
}

/** @returns {Record<string, string>} */
export function authorizationHeaders() {
  const token = getAccessToken();
  return token ? { Authorization: `Bearer ${token}` } : {};
}

/** @param {string} accept @param {HeadersInit | undefined} [initial] */
export function requestHeaders(accept, initial) {
  const headers = new Headers(initial);
  headers.set("Accept", accept);
  const token = getAccessToken();
  if (token) {
    headers.set("Authorization", `Bearer ${token}`);
  }
  return headers;
}

/**
 * @param {string} path
 * @param {RequestInit} [options]
 */
async function requestJson(path, options = {}) {
  const response = await fetch(`${API_ROOT}${path}`, {
    ...options,
    cache: "no-store",
    headers: requestHeaders("application/json", options.headers),
  });

  if (!response.ok) {
    throw await makeApiError(response);
  }
  if (response.status === 204) {
    return null;
  }
  return response.json();
}

/**
 * @param {string} path
 * @param {RequestInit} [options]
 */
async function requestOptionalJson(path, options = {}) {
  const response = await fetch(`${API_ROOT}${path}`, {
    ...options,
    cache: "no-store",
    headers: requestHeaders("application/json", options.headers),
  });
  if (response.status === 404) {
    return null;
  }
  if (!response.ok) {
    throw await makeApiError(response);
  }
  return response.json();
}

export const deviceApi = {
  getDevice: () => requestJson("/device"),
  getCaptureStatus: () => requestJson("/capture/status"),
  getSafeSwap: () => requestOptionalJson("/capture/safe-swap"),
  listSessions: () => requestJson("/sessions?limit=50"),
  /** @param {string} displayName */
  startCapture: (displayName) =>
    requestJson("/capture/start", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Idempotency-Key": crypto.randomUUID(),
      },
      body: JSON.stringify({
        schema: "ylx.capture-start.v2",
        mode: "production",
        display_name: displayName,
        take: { kind: "new" },
      }),
    }),
  /** @param {"user" | "safe_swap"} reason */
  stopCapture: (reason) =>
    requestJson("/capture/stop", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Idempotency-Key": crypto.randomUUID(),
      },
      body: JSON.stringify({ schema: "ylx.capture-stop.v2", reason }),
    }),
};

/** @param {AbortSignal} signal */
export async function getLatestPreview(signal) {
  const response = await fetch(`${API_ROOT}/preview`, {
    cache: "no-store",
    headers: requestHeaders("image/jpeg"),
    signal,
  });
  if (!response.ok) {
    throw new DeviceApiError(`设备预览返回 ${response.status}`, response.status);
  }
  const contentType = response.headers.get("Content-Type")?.split(";", 1)[0];
  if (contentType !== "image/jpeg") {
    throw new Error("设备预览不是 JPEG");
  }
  return response.blob();
}

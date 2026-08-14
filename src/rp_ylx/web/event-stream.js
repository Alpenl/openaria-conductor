// @ts-check

import {
  DeviceApiError,
  makeApiError,
  requestHeaders,
  waitForAbortableDelay,
} from "./api-client.js";

const EVENT_URL = "/api/v3/capture/events";

/** @param {string} block */
function parseEvent(block) {
  let id = null;
  let event = "message";
  const data = [];
  for (const line of block.split(/\r?\n/)) {
    if (!line || line.startsWith(":")) {
      continue;
    }
    const separator = line.indexOf(":");
    const field = separator === -1 ? line : line.slice(0, separator);
    const rawValue = separator === -1 ? "" : line.slice(separator + 1);
    const value = rawValue.startsWith(" ") ? rawValue.slice(1) : rawValue;
    if (field === "id") {
      id = value;
    } else if (field === "event") {
      event = value;
    } else if (field === "data") {
      data.push(value);
    }
  }
  if (!id || data.length === 0) {
    return null;
  }
  const payload = JSON.parse(data.join("\n"));
  if (
    !payload ||
    payload.schema !== "ylx.capture-event.v3" ||
    payload.sse_delivery_id !== id ||
    payload.type !== event
  ) {
    throw new Error("设备事件与 SSE envelope 不一致");
  }
  return { id, payload };
}

/**
 * @param {{
 *   signal: AbortSignal,
 *   onEvent: (event: any) => Promise<void> | void,
 *   onConnection: (state: "connected" | "disconnected") => void,
 *   onUnauthorized: (error: DeviceApiError) => void,
 * }} options
 */
export async function followCaptureEvents(options) {
  let lastEventId = null;
  while (!options.signal.aborted) {
    try {
      const headers = requestHeaders("text/event-stream");
      if (lastEventId) {
        headers.set("Last-Event-ID", lastEventId);
      }
      const response = await fetch(EVENT_URL, {
        cache: "no-store",
        headers,
        signal: options.signal,
      });
      if (!response.ok) {
        throw await makeApiError(response);
      }
      if (!response.body) {
        throw new Error("设备事件流没有响应正文");
      }
      const reader = response.body.getReader();
      const decoder = new TextDecoder();
      let buffer = "";
      let connected = false;
      while (!options.signal.aborted) {
        const { done, value } = await reader.read();
        buffer += decoder.decode(value, { stream: !done });
        let boundary = buffer.search(/\r?\n\r?\n/);
        while (boundary !== -1) {
          const block = buffer.slice(0, boundary);
          const separator = buffer.slice(boundary).match(/^\r?\n\r?\n/)?.[0] ?? "\n\n";
          buffer = buffer.slice(boundary + separator.length);
          const parsed = parseEvent(block);
          if (parsed) {
            await options.onEvent(parsed.payload);
            lastEventId = parsed.id;
            if (!connected) {
              options.onConnection("connected");
              connected = true;
            }
          }
          boundary = buffer.search(/\r?\n\r?\n/);
        }
        if (done) {
          break;
        }
      }
    } catch (error) {
      if (options.signal.aborted) {
        return;
      }
      if (error instanceof DeviceApiError && error.status === 401) {
        options.onConnection("disconnected");
        options.onUnauthorized(error);
        return;
      }
      console.warn(error);
    }
    options.onConnection("disconnected");
    await waitForAbortableDelay(1000, options.signal);
  }
}

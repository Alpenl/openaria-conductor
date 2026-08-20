// @ts-check

import { DeviceApiError, getLatestPreview, waitForAbortableDelay } from "./api-client.js";

/**
 * @param {{
 *   image: HTMLImageElement,
 *   status: Element,
 *   signal: AbortSignal,
 * }} options
 */
export async function followLatestPreview(options) {
  let currentUrl = null;
  try {
    while (!options.signal.aborted) {
      try {
        const blob = await getLatestPreview(options.signal);
        const nextUrl = URL.createObjectURL(blob);
        options.image.src = nextUrl;
        options.status.textContent = "实时";
        if (currentUrl) {
          URL.revokeObjectURL(currentUrl);
        }
        currentUrl = nextUrl;
        await waitForAbortableDelay(40, options.signal);
      } catch (error) {
        if (options.signal.aborted) {
          return;
        }
        options.status.textContent = "画面暂不可用";
        const expectedIdle =
          error instanceof DeviceApiError &&
          error.status === 503 &&
          error.code === "preview_unavailable";
        if (!expectedIdle) {
          console.warn(error);
        }
        await waitForAbortableDelay(500, options.signal);
      }
    }
  } finally {
    if (currentUrl) {
      URL.revokeObjectURL(currentUrl);
    }
  }
}

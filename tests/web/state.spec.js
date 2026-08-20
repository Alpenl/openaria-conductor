// @ts-check

import { expect, test } from "@playwright/test";

import { initialState, reduceState } from "../../src/rp_ylx/web/state.js";

const authorityEpoch = "4fa85f64-5717-4562-b3fc-2c963f66afa6";

/** @param {number} sourceRevision @param {string} deviceState */
function captureStatus(sourceRevision, deviceState) {
  return {
    schema: "ylx.capture-status.v2",
    authority_epoch: authorityEpoch,
    source_revision: sourceRevision,
    snapshot: {
      schema: "ylx.capture-snapshot-event.v2",
      device_state: deviceState,
      active_recording: null,
      retained_unsuccessful: null,
      runtime: {
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
        live_imu: null,
      },
    },
  };
}

test("同 authority 下旧 capture snapshot 不能覆盖更高 revision 状态", () => {
  let state = reduceState(
    { ...initialState, connection: "connected" },
    { type: "capture.snapshot", payload: captureStatus(5, "idle") },
  );

  state = reduceState(state, {
    type: "capture.snapshot",
    payload: captureStatus(3, "finalizing"),
  });

  expect(state.capture?.source_revision).toBe(5);
  expect(state.capture?.snapshot.device_state).toBe("idle");
});

// @ts-check

import { defineConfig, devices } from "@playwright/test";

export default defineConfig({
  testDir: "./tests/web",
  outputDir: "test-results",
  fullyParallel: false,
  workers: 1,
  forbidOnly: true,
  retries: 0,
  reporter: "line",
  use: {
    baseURL: "http://127.0.0.1:4173",
    browserName: "chromium",
    channel: "chrome",
    screenshot: "only-on-failure",
    trace: "retain-on-failure",
  },
  projects: [
    {
      name: "桌面",
      use: { viewport: { width: 1440, height: 960 } },
    },
    {
      name: "手机",
      use: { ...devices["Pixel 7"] },
    },
  ],
  webServer: {
    command: "node tests/web/fixture-server.js",
    url: "http://127.0.0.1:4173/__ready",
    reuseExistingServer: false,
    timeout: 10_000,
  },
});

// @ts-check

import { expect, test } from "@playwright/test";

test.beforeEach(async ({ request }) => {
  await request.post("/__fixture/reset");
});

test("设备工作台完全从同源离线加载", async ({ page }) => {
  /** @type {string[]} */
  const externalRequests = [];
  page.on("request", (request) => {
    if (new URL(request.url()).origin !== "http://127.0.0.1:4173") {
      externalRequests.push(request.url());
    }
  });

  await page.goto("/");

  await expect(page).toHaveTitle("RP-YLX 设备工作台");
  await expect(page.getByRole("heading", { name: "RP-YLX" })).toBeVisible();
  expect(externalRequests).toEqual([]);
});

test("权威快照呈现设备、容量和真实 raw IMU", async ({ page }) => {
  await page.goto("/");

  await expect(page.getByText("YLX-A1B2C3D4", { exact: true })).toBeVisible();
  await expect(page.getByTestId("capture-state")).toHaveText("待机");
  await expect(page.getByTestId("storage-available")).toHaveText("82.0 GiB");
  await expect(page.getByTestId("temperature")).toHaveText("43.5 °C");
  await expect(page.getByTestId("acceleration")).toContainText("x 12.000");
  await expect(page.getByTestId("acceleration")).toContainText("raw");
  await expect(page.getByTestId("angular-velocity")).toContainText("x 1.000");
  await expect(page.getByTestId("angular-velocity")).toContainText("raw");
  await expect(page.getByTestId("imu-sync")).toHaveText("good");
});

test("网页可以调整相机焦距并同步到权威快照", async ({ page, request }) => {
  await page.goto("/");

  await expect(page.getByTestId("camera-focus-value")).toHaveText("42");
  await expect(page.locator("#focus-status")).toContainText("手动焦距 42");

  await page.locator("#camera-focus-range").evaluate((node) => {
    const input = /** @type {HTMLInputElement} */ (node);
    input.value = "64";
    input.dispatchEvent(new Event("input", { bubbles: true }));
  });
  await expect(page.getByTestId("camera-focus-value")).toHaveText("64");
  await page.getByRole("button", { name: "应用焦距" }).click();

  await expect(page.getByTestId("camera-focus-value")).toHaveText("64");
  await expect(page.locator("#focus-status")).toContainText("手动焦距 64");

  const response = await request.get("/__fixture/requests");
  const body = /** @type {{requests: Array<{path: string, idempotencyKey: string | null}>}} */ (
    await response.json()
  );
  const focusRequests = body.requests.filter(
    (entry) => entry.path === "/api/v3/camera/focus" && entry.idempotencyKey,
  );
  expect(focusRequests).toHaveLength(1);
});

test("无录制卷时设备保持在线并显示空会话列表", async ({ page, request }) => {
  await request.post("/__fixture/config", { data: { sessionsVolumeUnavailable: true } });

  await page.goto("/");

  await expect(page.locator(".connection")).toHaveText("已连接");
  await expect(page.getByTestId("capture-state")).toHaveText("待机");
  await expect(page.getByTestId("storage-available")).toHaveText("0.0 GiB");
  await expect(page.getByTestId("storage-writable")).toHaveText("不可写");
  await expect(page.getByText("暂无会话", { exact: true })).toBeVisible();
  await expect(page.getByRole("alert")).toHaveCount(0);
  await expect(page.getByRole("button", { name: "开始录制" })).toBeDisabled();
});

test("录制命令在权威快照到达前保持待机", async ({ page, request }) => {
  await request.post("/__fixture/config", { data: { commandDelayMs: 350 } });
  await page.goto("/");
  await expect(page.getByTestId("capture-state")).toHaveText("待机");

  await page.getByLabel("录制名称").fill("走廊采集 01");
  await page.getByRole("button", { name: "开始录制" }).click();

  await expect(page.getByRole("button", { name: "正在发送" })).toBeDisabled();
  await expect(page.getByTestId("capture-state")).toHaveText("待机");
  await expect(page.getByTestId("capture-state")).toHaveText("录制中");
  await expect(page.getByText("走廊采集 01", { exact: true })).toBeVisible();
});

test("缺少 crypto.randomUUID 的 HTTP LAN 浏览器仍能发送录制命令", async ({ page, request }) => {
  await page.addInitScript(() => {
    Object.defineProperty(globalThis.crypto, "randomUUID", {
      configurable: true,
      value: undefined,
    });
  });
  await page.goto("/");
  await page.getByLabel("录制名称").fill("无 randomUUID 兼容");

  await page.getByRole("button", { name: "开始录制" }).click();

  await expect(page.getByTestId("capture-state")).toHaveText("录制中");
  await expect(page.getByRole("alert")).toHaveCount(0);
  const response = await request.get("/__fixture/requests");
  const body = /** @type {{requests: Array<{path: string, idempotencyKey: string | null}>}} */ (
    await response.json()
  );
  const starts = body.requests.filter((entry) => entry.path === "/api/v3/capture/start");
  expect(starts).toHaveLength(1);
  expect(starts[0].idempotencyKey).toMatch(
    /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/,
  );
});

test("录制命令只在本次请求结束后解锁", async ({ page, request }) => {
  await request.post("/__fixture/config", { data: { commandDelayMs: 350 } });
  await page.goto("/");
  await page.getByLabel("录制名称").fill("单次提交");
  await page.getByRole("button", { name: "开始录制" }).click();
  await expect(page.getByRole("button", { name: "正在发送" })).toBeDisabled();

  await request.post("/__fixture/state", { data: { broadcast: true } });
  await expect(page.getByRole("button", { name: "正在发送" })).toBeDisabled();

  const pendingResponse = await request.get("/__fixture/requests");
  const pendingRequests = /** @type {{requests: Array<{path: string}>}} */ (
    await pendingResponse.json()
  ).requests;
  expect(
    pendingRequests.filter((entry) => entry.path === "/api/v3/capture/start"),
  ).toHaveLength(1);

  await expect(page.getByTestId("capture-state")).toHaveText("录制中");
  await expect(page.getByRole("button", { name: "结束录制" })).toBeEnabled();
});

test("customer 事件流携带令牌并在断线后从权威快照收敛", async ({ page, request }) => {
  /** @type {import("@playwright/test").Request[]} */
  const pageEventRequests = [];
  page.on("request", (browserRequest) => {
    if (new URL(browserRequest.url()).pathname === "/api/v3/capture/events") {
      pageEventRequests.push(browserRequest);
    }
  });
  await request.post("/__fixture/config", { data: { requireBearer: true } });
  await page.addInitScript(() => {
    sessionStorage.setItem("rp-ylx-access-token", "customer-token");
  });
  await page.goto("/");
  await expect(page.getByTestId("capture-state")).toHaveText("待机");

  await expect
    .poll(async () => {
      const response = await request.get("/__fixture/requests");
      const body = /** @type {{requests: Array<{path: string}>}} */ (await response.json());
      return body.requests.filter((entry) => entry.path === "/api/v3/capture/events").length;
    })
    .toBe(1);

  await request.post("/__fixture/state", {
    data: { deviceState: "recording", displayName: "另一台手机发起", broadcast: false },
  });
  await request.post("/__fixture/disconnect-events");

  await expect(page.getByTestId("capture-state")).toHaveText("录制中");
  await expect(page.getByText("另一台手机发起", { exact: true })).toBeVisible();

  const response = await request.get("/__fixture/requests");
  const body = /** @type {{requests: Array<{path: string, authorization: string | null, lastEventId: string | null}>}} */ (
    await response.json()
  );
  const events = body.requests.filter(
    (entry) =>
      entry.path === "/api/v3/capture/events" &&
      entry.authorization === "Bearer customer-token",
  );
  expect(events.length).toBeGreaterThanOrEqual(2);
  expect(events[1].lastEventId).toBe("1");
  const requestHeaders = await Promise.all(
    pageEventRequests.map((browserRequest) => browserRequest.allHeaders()),
  );
  expect(requestHeaders.length).toBeGreaterThanOrEqual(2);
  expect(requestHeaders.every((headers) => headers.authorization === "Bearer customer-token")).toBe(
    true,
  );
});

test("事件流持续断线时禁用写操作且限制重连频率", async ({ page, request }) => {
  await page.goto("/");
  await expect(page.locator(".connection")).toHaveText("已连接");
  await request.post("/__fixture/config", { data: { eventsUnavailable: true } });
  await request.post("/__fixture/disconnect-events");

  await expect(page.locator(".connection")).toHaveText("连接中断");
  await expect(page.getByRole("button", { name: "开始录制" })).toBeDisabled();
  await expect(page.getByLabel("录制名称")).toBeDisabled();
  const firstResponse = await request.get("/__fixture/requests");
  const firstBody = /** @type {{requests: Array<{path: string}>}} */ (
    await firstResponse.json()
  );
  const firstCount = firstBody.requests.filter(
    (entry) => entry.path === "/api/v3/capture/events",
  ).length;

  await page.waitForTimeout(650);

  const laterResponse = await request.get("/__fixture/requests");
  const laterBody = /** @type {{requests: Array<{path: string}>}} */ (
    await laterResponse.json()
  );
  const laterCount = laterBody.requests.filter(
    (entry) => entry.path === "/api/v3/capture/events",
  ).length;
  expect(laterCount - firstCount).toBeLessThanOrEqual(1);
});

test("事件流重连处理首个权威事件前保持写操作禁用", async ({ page, request }) => {
  await page.goto("/");
  await expect(page.locator(".connection")).toHaveText("已连接");
  await request.post("/__fixture/config", { data: { eventSnapshotDelayMs: 2000 } });
  await request.post("/__fixture/disconnect-events");
  await expect(page.locator(".connection")).toHaveText("连接中断");

  await expect
    .poll(async () => {
      const response = await request.get("/__fixture/requests");
      const body = /** @type {{requests: Array<{path: string}>}} */ (await response.json());
      return body.requests.filter((entry) => entry.path === "/api/v3/capture/events").length;
    })
    .toBeGreaterThanOrEqual(2);
  await expect(page.locator(".connection")).toHaveText("连接中断");
  await expect(page.getByRole("button", { name: "开始录制" })).toBeDisabled();

  await expect(page.locator(".connection")).toHaveText("已连接");
  await expect(page.getByRole("button", { name: "开始录制" })).toBeEnabled();
});

test("事件流 401 停止重连并回到令牌入口", async ({ page, request }) => {
  await page.goto("/");
  await expect(page.locator(".connection")).toHaveText("已连接");
  await request.post("/__fixture/config", { data: { requireBearer: true } });
  await request.post("/__fixture/disconnect-events");

  await expect(page.getByLabel("设备访问令牌")).toBeVisible();
  await expect(page.locator(".connection")).toHaveText("连接中断");
  await expect(page.getByRole("button", { name: "开始录制" })).toBeDisabled();
  const unauthorizedResponse = await request.get("/__fixture/requests");
  const unauthorizedBody = /** @type {{requests: Array<{path: string}>}} */ (
    await unauthorizedResponse.json()
  );
  const unauthorizedCount = unauthorizedBody.requests.filter(
    (entry) => entry.path === "/api/v3/capture/events",
  ).length;

  await page.waitForTimeout(500);

  const laterResponse = await request.get("/__fixture/requests");
  const laterBody = /** @type {{requests: Array<{path: string}>}} */ (
    await laterResponse.json()
  );
  const laterCount = laterBody.requests.filter(
    (entry) => entry.path === "/api/v3/capture/events",
  ).length;
  expect(laterCount).toBe(unauthorizedCount);
});

test("慢预览响应不排队且录制期间继续更新左眼画面", async ({ page, request }) => {
  let previewInFlight = 0;
  let previewMaxInFlight = 0;
  /** @param {import("@playwright/test").Request} networkRequest */
  const isPagePreview = (networkRequest) =>
    new URL(networkRequest.url()).pathname === "/api/v3/preview";
  page.on("request", (networkRequest) => {
    if (isPagePreview(networkRequest)) {
      previewInFlight += 1;
      previewMaxInFlight = Math.max(previewMaxInFlight, previewInFlight);
    }
  });
  /** @param {import("@playwright/test").Request} networkRequest */
  const finishPreview = (networkRequest) => {
    if (isPagePreview(networkRequest)) {
      previewInFlight -= 1;
    }
  };
  page.on("requestfinished", finishPreview);
  page.on("requestfailed", finishPreview);

  await request.post("/__fixture/config", { data: { previewDelayMs: 180 } });
  await page.goto("/");

  await expect(page.getByTestId("preview-image")).toBeVisible();
  await expect
    .poll(() =>
      page
        .getByTestId("preview-image")
        .evaluate((image) => /** @type {HTMLImageElement} */ (image).naturalWidth),
    )
    .toBeGreaterThan(0);

  await page.getByLabel("录制名称").fill("预览不中断");
  await page.getByRole("button", { name: "开始录制" }).click();
  await expect(page.getByTestId("capture-state")).toHaveText("录制中");

  await expect
    .poll(async () => {
      const response = await request.get("/__fixture/requests");
      return (await response.json()).preview.requests;
    })
    .toBeGreaterThanOrEqual(3);

  expect(previewMaxInFlight).toBe(1);
});

test("预览循环不会累积中止监听器", async ({ page, request }) => {
  await page.addInitScript(() => {
    const metrics = { active: 0, peak: 0 };
    Object.defineProperty(window, "__rpYlxAbortMetrics", { value: metrics });
    const originalAdd = AbortSignal.prototype.addEventListener;
    const originalRemove = AbortSignal.prototype.removeEventListener;
    AbortSignal.prototype.addEventListener = new Proxy(originalAdd, {
      apply(target, thisArgument, argumentsList) {
        if (argumentsList[0] === "abort") {
          metrics.active += 1;
          metrics.peak = Math.max(metrics.peak, metrics.active);
        }
        return Reflect.apply(target, thisArgument, argumentsList);
      },
    });
    AbortSignal.prototype.removeEventListener = new Proxy(originalRemove, {
      apply(target, thisArgument, argumentsList) {
        if (argumentsList[0] === "abort") {
          metrics.active -= 1;
        }
        return Reflect.apply(target, thisArgument, argumentsList);
      },
    });
  });
  await page.goto("/");

  await expect
    .poll(async () => {
      const response = await request.get("/__fixture/requests");
      return (await response.json()).preview.requests;
    })
    .toBeGreaterThanOrEqual(12);

  const activeListeners = await page.evaluate(
    () => /** @type {any} */ (window).__rpYlxAbortMetrics.active,
  );
  expect(activeListeners).toBeLessThanOrEqual(1);
});

test("空闲预览不可用不会持续污染浏览器控制台", async ({ page }) => {
  /** @type {string[]} */
  const warnings = [];
  page.on("console", (message) => {
    if (message.type() === "warning") {
      warnings.push(message.text());
    }
  });
  await page.route("**/api/v3/preview", async (route) => {
    await route.fulfill({
      status: 503,
      contentType: "application/problem+json",
      body: JSON.stringify({
        schema: "ylx.api-error.v2",
        error: {
          code: "preview_unavailable",
          message: "当前没有可用的预览帧",
          request_id: "12c175c7-a794-45f5-b6c7-348c3e73bc22",
          retryable: true,
        },
      }),
    });
  });

  await page.goto("/");

  await expect(page.getByText("画面暂不可用", { exact: true })).toBeVisible();
  await page.waitForTimeout(650);
  expect(warnings).toEqual([]);
});

test("只有 typed safe-swap 回执允许移除存储设备且刷新可恢复", async ({ page, request }) => {
  await page.goto("/");
  await expect(page.getByText("可以移除存储设备")).not.toBeVisible();

  await page.getByLabel("录制名称").fill("换盘测试");
  await page.getByRole("button", { name: "开始录制" }).click();
  await expect(page.getByTestId("capture-state")).toHaveText("录制中");
  await page.getByRole("button", { name: "安全换盘" }).click();

  await expect(page.getByTestId("capture-state")).toHaveText("正在结束");
  await expect(page.getByText("可以移除存储设备")).not.toBeVisible();

  await request.post("/__fixture/safe-swap");
  await expect(page.getByText("可以移除存储设备", { exact: true })).toBeVisible();
  await expect(page.getByTestId("safe-swap-release")).toHaveText("设备已释放");

  await page.reload();
  await expect(page.getByText("可以移除存储设备", { exact: true })).toBeVisible();
});

test("跳号换盘事件只通过权威状态和回执资源收敛", async ({ page, request }) => {
  await page.goto("/");
  await page.getByLabel("录制名称").fill("跳号换盘");
  await page.getByRole("button", { name: "开始录制" }).click();
  await expect(page.getByTestId("capture-state")).toHaveText("录制中");
  await page.getByRole("button", { name: "安全换盘" }).click();

  await request.post("/__fixture/safe-swap-gap");

  await expect(page.getByTestId("capture-state")).toHaveText("待机");
  await expect(page.getByText("可以移除存储设备", { exact: true })).toBeVisible();
});

test("仍挂载或存在打开句柄的换盘事件不能授权移除", async ({ page, request }) => {
  await page.goto("/");
  await page.getByLabel("录制名称").fill("拒绝不安全回执");
  await page.getByRole("button", { name: "开始录制" }).click();
  await expect(page.getByTestId("capture-state")).toHaveText("录制中");

  await request.post("/__fixture/unsafe-safe-swap", { data: { state: "mounted" } });
  await expect(page.getByText("可以移除存储设备", { exact: true })).not.toBeVisible();
  await request.post("/__fixture/unsafe-safe-swap", { data: { state: "open-handles" } });
  await expect(page.getByText("可以移除存储设备", { exact: true })).not.toBeVisible();

  await page.getByRole("button", { name: "安全换盘" }).click();
  await request.post("/__fixture/safe-swap");
  await expect(page.getByText("可以移除存储设备", { exact: true })).toBeVisible();
});

test("结束录制接受空 204 并重新读取权威状态", async ({ page, request }) => {
  await request.post("/__fixture/config", { data: { stopReturns204: true } });
  await page.goto("/");
  await page.getByLabel("录制名称").fill("空响应结束");
  await page.getByRole("button", { name: "开始录制" }).click();
  await expect(page.getByTestId("capture-state")).toHaveText("录制中");

  await page.getByRole("button", { name: "结束录制" }).click();

  await expect(page.getByTestId("capture-state")).toHaveText("待机");
  await expect(page.getByRole("alert")).toHaveCount(0);
});

test("开始录制原样显示 API problem 且服务恢复后可重试", async ({ page, request }) => {
  await request.post("/__fixture/config", { data: { startProblem: true } });
  await page.goto("/");
  await page.getByLabel("录制名称").fill("错误恢复");
  await page.getByRole("button", { name: "开始录制" }).click();

  const alert = page.getByRole("alert");
  await expect(alert).toContainText("capture_busy");
  await expect(alert).toContainText(
    "Artifact transfer is paused until the device returns to idle.",
  );
  const overlapsWorkspace = await alert.evaluate((alertElement) => {
    const alertBounds = alertElement.getBoundingClientRect();
    return Array.from(document.querySelectorAll(".workspace .panel")).some((panel) => {
      const panelBounds = panel.getBoundingClientRect();
      return !(
        alertBounds.right <= panelBounds.left ||
        alertBounds.left >= panelBounds.right ||
        alertBounds.bottom <= panelBounds.top ||
        alertBounds.top >= panelBounds.bottom
      );
    });
  });
  expect(overlapsWorkspace).toBe(false);
  const alertBounds = await alert.boundingBox();
  expect(alertBounds).not.toBeNull();
  expect(alertBounds?.y ?? Number.POSITIVE_INFINITY).toBeLessThan(960);
  await expect(page.getByRole("button", { name: "开始录制" })).toBeEnabled();

  await request.post("/__fixture/config", { data: { startProblem: false } });
  await page.getByRole("button", { name: "开始录制" }).click();
  await expect(page.getByTestId("capture-state")).toHaveText("录制中");
  await expect(page.getByRole("alert")).toHaveCount(0);
});

test("结束录制原样显示 API problem 且服务恢复后可重试", async ({ page, request }) => {
  await page.goto("/");
  await page.getByLabel("录制名称").fill("结束错误恢复");
  await page.getByRole("button", { name: "开始录制" }).click();
  await expect(page.getByTestId("capture-state")).toHaveText("录制中");
  await request.post("/__fixture/config", { data: { stopProblem: true } });

  await page.getByRole("button", { name: "结束录制" }).click();
  const alert = page.getByRole("alert");
  await expect(alert).toContainText("storage_finalize_failed");
  await expect(alert).toContainText("存储封存暂时失败，请重试");
  await expect(page.getByRole("button", { name: "结束录制" })).toBeEnabled();

  await request.post("/__fixture/config", { data: { stopProblem: false } });
  await page.getByRole("button", { name: "结束录制" }).click();
  await expect(page.getByTestId("capture-state")).toHaveText("待机");
  await expect(page.getByRole("alert")).toHaveCount(0);
});

test("新录制清除旧换盘回执且拒绝旧 authority 事件", async ({ page, request }) => {
  await page.goto("/");
  await page.getByLabel("录制名称").fill("第一段换盘");
  await page.getByRole("button", { name: "开始录制" }).click();
  await expect(page.getByTestId("capture-state")).toHaveText("录制中");
  await page.getByRole("button", { name: "安全换盘" }).click();
  await request.post("/__fixture/safe-swap");
  await expect(page.getByText("可以移除存储设备", { exact: true })).toBeVisible();

  await request.post("/__fixture/new-authority-recording");
  await expect(page.getByTestId("capture-state")).toHaveText("录制中");
  await expect(page.getByText("可以移除存储设备", { exact: true })).not.toBeVisible();

  await request.post("/__fixture/stale-safe-swap");
  await expect(page.getByText("可以移除存储设备", { exact: true })).not.toBeVisible();
});

test("会话列表保持生产终态与网关可用性分离并显示发现诊断", async ({ page }) => {
  await page.goto("/");

  const usableSession = page.getByTestId("session-item").filter({ hasText: "入口标定" });
  await expect(usableSession.getByText("已封存", { exact: true })).toBeVisible();
  await expect(usableSession.getByText("可用", { exact: true })).toBeVisible();

  const unknownSession = page.getByTestId("session-item").filter({ hasText: "第二段采集" });
  await expect(unknownSession.getByText("尚未校验", { exact: true })).toBeVisible();
  await expect(page.getByText("发现一个无法读取的会话清单，已隔离")).toBeVisible();
});

test("外部客户端快照触发容量和最近会话有界重拉", async ({ page, request }) => {
  await page.goto("/");
  await expect(page.getByTestId("storage-available")).toHaveText("82.0 GiB");
  await expect(page.getByText("外部客户端封存", { exact: true })).not.toBeVisible();

  await request.post("/__fixture/related-resources");

  await expect(page.getByTestId("storage-available")).toHaveText("64.0 GiB");
  await expect(page.getByText("外部客户端封存", { exact: true })).toBeVisible();
});

test("设备诊断通过 SSE 原样呈现 code message 和 details", async ({ page, request }) => {
  await page.goto("/");
  await expect(page.getByTestId("capture-state")).toHaveText("待机");
  await request.post("/__fixture/diagnostic");

  const alert = page.getByRole("alert");
  await expect(alert).toContainText("media_lost");
  await expect(alert).toContainText(
    "The selected recording volume disappeared before finalization.",
  );
  await expect(alert).toContainText('"action":"reinsert_same_volume"');
  await expect(alert.getByRole("time")).toHaveText("2026-08-08T13:01:02+08:00");
});

test("两个浏览器最终收敛到同一录制状态", async ({ browser }) => {
  const context = await browser.newContext();
  const first = await context.newPage();
  const second = await context.newPage();
  await Promise.all([first.goto("/"), second.goto("/")]);
  await Promise.all([
    expect(first.getByTestId("capture-state")).toHaveText("待机"),
    expect(second.getByTestId("capture-state")).toHaveText("待机"),
  ]);

  await first.getByLabel("录制名称").fill("双客户端测试");
  await first.getByRole("button", { name: "开始录制" }).click();
  await Promise.all([
    expect(first.getByTestId("capture-state")).toHaveText("录制中"),
    expect(second.getByTestId("capture-state")).toHaveText("录制中"),
  ]);

  await second.getByRole("button", { name: "结束录制" }).click();
  await Promise.all([
    expect(first.getByTestId("capture-state")).toHaveText("待机"),
    expect(second.getByTestId("capture-state")).toHaveText("待机"),
  ]);
  await context.close();
});

test("未成功终态保留诊断且不伪装成封存会话", async ({ page, request }) => {
  await page.goto("/");
  await page.getByLabel("录制名称").fill("失败录制");
  await page.getByRole("button", { name: "开始录制" }).click();
  await expect(page.getByTestId("capture-state")).toHaveText("录制中");
  await request.post("/__fixture/fail-capture");

  await expect(page.getByTestId("capture-state")).toHaveText("失败");
  await expect(page.getByText("source_decode_failed", { exact: true })).toBeVisible();
  await expect(
    page.getByText(
      "A captured source frame could not be decoded, so the session was terminated without a manifest.",
      { exact: true },
    ),
  ).toBeVisible();
  await expect(page.getByRole("time")).toHaveText("2026-08-08T13:50:18+08:00");
  await expect(
    page.getByTestId("session-item").filter({ hasText: "YLX-30D5872D" }),
  ).toHaveCount(0);
});

test("progress 事件触发权威快照刷新并显示录制计数", async ({ page, request }) => {
  await page.goto("/");
  await page.getByLabel("录制名称").fill("进度测试");
  await page.getByRole("button", { name: "开始录制" }).click();
  await expect(page.getByTestId("capture-state")).toHaveText("录制中");

  await request.post("/__fixture/progress");
  await expect(page.getByTestId("elapsed-seconds")).toHaveText("12.4 秒");
  await expect(page.getByTestId("captured-frames")).toHaveText("744");
  await expect(page.getByTestId("bytes-written")).toHaveText("42.0 MiB");
});

test("customer 401 后可输入 Bearer 令牌并连接设备", async ({ page, request }) => {
  await request.post("/__fixture/config", { data: { requireBearer: true } });
  await page.goto("/");

  await expect(page.getByLabel("设备访问令牌")).toBeVisible();
  await expect(page.getByRole("alert")).toContainText("authentication_required");
  await expect(page.getByRole("alert")).toContainText("需要设备访问令牌");
  await page.getByLabel("设备访问令牌").fill("customer-token");
  await page.getByRole("button", { name: "连接设备" }).click();

  await expect(page.getByTestId("capture-state")).toHaveText("待机");
  await expect(page.getByLabel("设备访问令牌")).not.toBeVisible();
  await expect(page.getByRole("alert")).toHaveCount(0);
  expect(
    await page.evaluate(() => sessionStorage.getItem("rp-ylx-access-token")),
  ).toBe("customer-token");

  await expect
    .poll(async () => {
      const response = await request.get("/__fixture/requests");
      return (await response.json()).preview.requests;
    })
    .toBeGreaterThanOrEqual(2);
  const response = await request.get("/__fixture/requests");
  const body = /** @type {{requests: Array<{path: string, authorization: string | null}>, preview: {maxConcurrent: number}}} */ (
    await response.json()
  );
  const authenticatedEvents = body.requests.filter(
    (entry) =>
      entry.path === "/api/v3/capture/events" &&
      entry.authorization === "Bearer customer-token",
  );
  expect(authenticatedEvents).toHaveLength(1);
  expect(body.preview.maxConcurrent).toBe(1);
});

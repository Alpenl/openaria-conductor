# `refactor/0.3.0` 与 `main` 双线协同维护规范

本文档记录 `openaria-conductor` 在 `0.3.0` 架构重构期间，`main`（`0.2.x` 稳定主干）与 `refactor/0.3.0`（`0.3.0` 重构演进分支）的定位、单向同步规则、冲突映射表及正式切换（Cutover）门禁。

---

## 一、分支定位与基线关系

| 分支 | 定位与职责 | 版本与渠道约束 | 基线关系 |
| :--- | :--- | :--- | :--- |
| **`main`** | `0.2.x` 线上稳定与现场修复主干。承载 RDK X5 实机问题修复、正式版本 tag（`v0.2.x`）与线上 OTA 渠道交付。 | 受分支保护（PR + `test` 必过、禁强推）；仅允许 `0.2.x` 版本与 `rdk-x5/latest.json` 主 OTA 渠道。 | 与 `origin/main` 保持一致；在 `0.3.0` 正式切换前继续作为唯一线上发布主干。 |
| **`refactor/0.3.0`** | `0.3.0` 架构重构与演进主线。承载模块化拆分、并发/安全加固与测试基础设施重构。 | 切换前暂保持 `0.2.16` 四处版本一致性锁，不向线上 `rdk-x5/latest.json` 发布；仅运行 CI 与 RDK X5 离线测试包构建。 | 自 `v0.2.16`（`60b44a5`）分岔，纯 0.3.0 重构基线 `70f0623` 已先行备份至远端，并通过 `--no-ff` 合入 `origin/main`。 |
| **`release/0.2`（未来切换期创建）** | `0.3.0` 合入 `main` 前，从 `main` 切出的 `0.2.x` 长期冻结维护线。 | 若需继续发包，须使用独立 OSS 前缀（如 `rdk-x5-0.2`），避免触发 `check_channel_promotion` 降级拦截。 | 切换后 `main` 升级为 `0.3.x` 主干。 |

---

## 二、双线协同与修复同步规则

采用 **“单向主同步（`main -> refactor/0.3.0`）+ 分类分流”** 策略：

| 改动类型 | 优先合入目标 | 跨分支同步策略 |
| :--- | :--- | :--- |
| **A. 现场硬件 / 存储安全 / 线上 Bug 修复** | `main`（走 `fix/*` PR） | **必须单向同步至 `refactor/0.3.0`**。 |
| **B. `0.2.x` 发版元数据与发行说明**（版本号递增、`docs/releases/*.md`） | `main`（走 `release/0.2.x` PR） | 仍执行 `--no-ff` 合并进 `refactor/0.3.0` 推进 merge-base，但 4 处版本锁与 `uv.lock`/`Cargo.lock` 保留 `refactor/0.3.0` 侧；**禁止在未同步升版 `pyproject.toml` 时单独采纳新版 `src/rp_ylx/web/assets.json`**。 |
| **C. `0.3.0` 架构重构、内部加固与测试基建** | `refactor/0.3.0` | **禁止反向合入 `main`**，避免在稳定主干引入大面积结构震荡。 |
| **D. 在 `0.3.0` 发现且危及 `0.2.x` 线上安全的严重缺陷** | 双线紧急 PR | 合入 `main` 后再以 `--no-ff` 收口至 `refactor/0.3.0`，防止后续同步重复冲突。 |

### 同步节奏与操作规范
1. **同步节奏**：`origin/main` 每合入修复 PR 后 **24 小时内** 发起同步；打测试包或真机验收前强制对齐最新 `origin/main`。
2. **统一使用 `--no-ff` 合并**（禁止对 `refactor/0.3.0` 执行 `git rebase origin/main`）：
   ```bash
   git fetch origin
   git checkout -b sync/main-into-0.3.0-YYYYMMDD origin/refactor/0.3.0
   git merge --no-ff origin/main -m "chore(sync): merge origin/main (<short-sha>, PR #N) into refactor/0.3.0"
   ```

---

## 三、冲突裁决与模块落位映射

由于 `refactor/0.3.0` 已将多个旧单文件拆分为子模块并保留薄兼容垫片（shim），合并冲突时**严禁机械接受 `main` 文本或将逻辑写回垫片**。

### 裁决优先级（由高到低）
1. **0.3.0 安全、锁序、状态机与纯标准库不变量**（最高优先级，不可回退）。
2. **`main` 修复的真实业务意图**（须在映射模块中完整语义落地）。
3. **0.3.0 模块边界与 `tests/testkit/` 规范**（不污染垫片、不引入循环依赖或跨测试 import）。
4. **版本与契约校验锁**（OpenAPI YAML 合并后必须实时重算字节数与 SHA-256）。

### 模块映射表（`main` -> `refactor/0.3.0`）

| `main` (`0.2.x`) 修改位置 | `refactor/0.3.0` 真实落位 | 结构与安全约束 |
| :--- | :--- | :--- |
| `src/rp_ylx/deployment.py` | `src/rp_ylx/deploy_pkg/{schema,config,systemd,transaction,storage,bundle,network_migration}.py` | `deployment.py` 保持纯导出垫片；`deploy_pkg/` 保持**纯 Python 标准库（零第三方依赖）**。 |
| `src/rp_ylx/api/gateway.py` | `src/rp_ylx/api/{router,http_server,middleware,streams}.py`、`api/controllers/*` | `gateway.py` 保持纯垫片；CORS/Header 放行需同步检查 `router.py`。 |
| `src/rp_ylx/api/events.py` | `src/rp_ylx/api/validators/events.py`、`api/streams.py` | `events.py` 保持纯垫片；区分 API 响应校验与 SQLite 入库校验。 |
| `src/rp_ylx/api/downloads.py` | `api/controllers/sessions.py`、`api/{range,locked_io,manifest_validator}.py` | `downloads.py` 仅保留 `DirectorySessionStore` 与兼容导出。 |
| `src/rp_ylx/recording/{coordinator,catalog,device_session}.py` | `recording/{coordinator,session_catalog,lifecycle,stop_workflow,recovery_orchestrator,state_store,fs_safety}.py` | 严守锁序与 `CapturePhase` 状态机，保持 exFAT 安全写语义。 |
| `src/rp_ylx/daemon.py` / `update.py` | 同路径文件 | 保留 `core.fs` 加固、有界严格 JSON 读取、Ed25519 验签与 `maintenance_lock`。 |
| `native/src/{lib,audio,schema_validation}.rs` | `native/src/{py,audio,hardware,session,sys}/*` | 拒绝恢复已删旧文件；非测试代码保持零 `.unwrap()`/`.expect()`。 |
| `src/rp_ylx/api/ylx-device-v4.openapi.yaml` | 同路径 + `contracts/ylx-device-api-support.json` + `tests/test_gateway_contract.py` | 合并后实时重算字节数与 SHA-256 并同步契约文件与测试。 |
| `tests/test_*.py` | 对应单测 + `tests/testkit/*` | 公共桩留在 `tests/testkit/`，禁止跨测试文件 import 或写死系统锁路径。 |

### 每次同步强制验证门禁
```bash
git diff --check
uv run --extra dev python scripts/release_version.py
CI=true uv run --extra dev python scripts/check.py
```

---

## 四、`0.2.x -> 0.3.0` 正式切换（Cutover）上线门禁

正式将 `refactor/0.3.0` 升版并合入 `main` 前，须集中完成以下 6 项准备（不要在日常同步中零散改动）：

1. **解除 `(0, 2)` 与 `.0` 版本拦截**：同步放开 `scripts/release_version.py`、`scripts/publish_rdk_x5_oss.py` 与 `src/rp_ylx/firmware_control.py` 的版本系列限制，并更新 `tests/test_release_version.py` 与 `tests/test_firmware_control.py`。
2. **原子升级版本号与锁文件**：同步升级 `pyproject.toml`、`native/Cargo.toml`、`src/rp_ylx/__init__.py`、`src/rp_ylx/web/assets.json`，并更新 `uv.lock`、`Cargo.lock` 与 `tests/test_web_resources.py`。
3. **OSS 渠道隔离与升级桥接**：预发阶段使用独立前缀（如 `rdk-x5-next`），避免污染 `rdk-x5/latest.json`；正式升级需提供支持 `(0, 3)` 的过渡版或通过 `sudo openaria-update <manifest-url>` 引导。
4. **Ed25519 签名闭环**：补齐仓库 `release-signing.pub` 并验证 `OPENARIA_RELEASE_SIGNING_KEY` 发布签名与设备端验签链路。
5. **CI 与打包流水线就绪**：确认 `.github/workflows/ci.yml` 与 `.github/workflows/rdk-x5-bundle.yml` 在 `refactor/0.3.0` 全绿。
6. **真机浸泡验收**：参照 Issue #87 标准完成至少一轮 RDK X5 实机安装/OTA、双目长录与重启恢复验证，从 `main` 切出 `release/0.2` 后再将 `refactor/0.3.0` 合入 `main` 并打 `v0.3.0` 附注标签。

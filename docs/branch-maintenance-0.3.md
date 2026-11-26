# `refactor/0.3.0` 与 `main` 双线协同维护规范

本文档记录 **Miro-Ego 0.3.0** 纯原生架构重构期间，`main`（`0.2.x` 历史稳定主干）与 `refactor/0.3.0`（Miro-Ego `0.3.0` 纯原生 Rust + C 主线）的分支定位、单向同步规则、架构落位及最终切换（Cutover）门禁。

---

## 一、分支定位与基线关系

| 分支 | 定位与职责 | 版本与架构状态 | 基线关系 |
| :--- | :--- | :--- | :--- |
| **`main`** | `0.2.x` 线上稳定与现场修复主干。承载旧版 Python + PyO3 运行时在 RDK X5 上的紧急修复与 `v0.2.x` 发布。 | 仅允许 `0.2.x` 补丁版与旧版渠道；合入 `0.3.0` 前切出 `release/0.2` 冻结维护线。 | 与 `origin/main` 保持一致。 |
| **`refactor/0.3.0`** | **Miro-Ego 0.3.0** 纯原生重构主线。单二进制 Rust（`crates/miro-ego-*`）+ 独立 C 硬件编码器（`csrc/miro-ego-encoder/`）+ 纯 exFAT `/data` + 20 GiB 零增长系统盘 + 设备 API v1（`/api/v1`）。 | 已完成纯原生切换（版本 `0.3.0`），移除旧 `src/rp_ylx/`、`native/`、`tests/` 与旧 `contracts/ylx-*`；主机工具迁至 `tools/miro_ego_host/`。 | 经 Wave 1–4 演进并保持 `scripts/check.py` 全绿。 |
| **`release/0.2`（合入 `main` 前创建）** | `refactor/0.3.0` 合入 `main` 前从 `main` 切出的 `0.2.x` 长期冻结维护分支。 | 若需对存量 `0.2.x` 设备发包，须使用独立发布前缀，避免与 `0.3.x` 混用。 | 切换完成后 `main` 升级为 Miro-Ego `0.3.x` 主干。 |

---

## 二、双线协同与修复同步规则

在 `refactor/0.3.0` 尚未正式合入 `main` 之前，仍遵循 **“现场硬件修复单向同步（`main -> refactor/0.3.0`）”** 策略：

| 改动类型 | 优先合入目标 | 跨分支同步策略 |
| :--- | :--- | :--- |
| **A. 现场硬件 / 存储安全 / 线上 Bug 修复** | `main`（走 `fix/*` PR） | 将修复意图等价落地到 `refactor/0.3.0` 对应的 Rust crate / C 编码器 / `deploy/miro-ego/` / `tools/miro_ego_host/` 模块，**严禁恢复已删除的旧 Python 设备端代码**。 |
| **B. `0.2.x` 发版元数据与发行说明** | `main` | 保持 `refactor/0.3.0` 的 `0.3.0` 版本锁（`Cargo.toml` / `pyproject.toml` / `uv.lock` / `Cargo.lock`），不采纳 `0.2.x` 版本元数据。 |
| **C. `0.3.0` 纯原生架构演进、API v1 与 CI/打包基建** | `refactor/0.3.0` | **禁止反向合入 `main`**，直至完成真机浸泡验收与正式切换。 |

---

## 三、旧 `0.2.x` 到 Miro-Ego `0.3.0` 模块落位映射

将 `main` 上发现的硬件或运维问题同步到 `refactor/0.3.0` 时，按以下映射直接修改新架构模块：

| `main` (`0.2.x`) 旧位置 | `refactor/0.3.0` (`Miro-Ego 0.3.0`) 真实落位 | 说明 |
| :--- | :--- | :--- |
| `src/rp_ylx/core/`、`native/src/sys/` | `crates/miro-ego-core/` | 路径常量、配置、锁、`O_NOFOLLOW` 安全 I/O、exFAT `ATTR_HIDDEN` ioctl |
| `src/rp_ylx/capture/`、`native/src/hardware/`、`native/src/audio/` | `crates/miro-ego-capture/` | V4L2 双目采集、IMU、ALSA 音频、`MEGO_SHM` 共享内存环 |
| `src/rp_ylx/hobot/`、`native/hobot/` | `csrc/miro-ego-encoder/` | 独立 C11 硬件双目编码器与 ASan/UBSan 主机测试套 |
| `src/rp_ylx/recording/`、`src/rp_ylx/sqlite*` | `crates/miro-ego-storage/` | 录制状态机、exFAT 会话封存、二进制 `catalog.bin`、断电恢复、诊断日志与审计 |
| `src/rp_ylx/api/`、`ylx-device-v4.openapi.yaml` | `crates/miro-ego-api/` + `contracts/miro-ego-*` | Async Tokio/Axum/Rustls `/api/v1` 网关、Range 下载、SSE、MJPEG/H.264 预览 |
| `src/rp_ylx/network*` | `crates/miro-ego-network/` | `nmcli` 控制、Unix Socket JSON-RPC、救援热点 `MiroEgo-XXXXXXXX`、`_miro-ego._tcp` mDNS |
| `src/rp_ylx/recording_button.py`、`recording_indicator.py`、语音 | `crates/miro-ego-hw/` | 40-pin GPIO 按钮、sysfs LED、语音播报音量、温度/板型读取 |
| `src/rp_ylx/deploy_pkg/`、`src/rp_ylx/deploy/` | `crates/miro-ego-deploy/` + `deploy/miro-ego/` | 纯原生 `miro-ego.rdk-x5-bundle.v1` 验包、A/B 事务发布/回滚、systemd 单元与零增长限额 |
| `scripts/factory_recovery/`、`scripts/card_export/` | `tools/miro_ego_host/{factory_recovery,card_export}/` | 主机侧工厂恢复刷卡器（3 分区 exFAT）与只读 SD 卡导出器 |
| `scripts/check.py`、`scripts/build_rdk_x5_bundle.py` | 同路径（纯原生重写） | 统一静态/契约/C/主机工具/Rust 检查与纯原生 AArch64 bundle 打包 |

### 每次同步强制验证门禁

```bash
git diff --check
uv sync --frozen --extra dev
uv run --extra dev python scripts/release_version.py
CI=true uv run --extra dev python scripts/check.py
```

---

## 四、`0.2.x -> 0.3.0` 正式切换（Cutover）上线门禁清单

1. **[已完成] 解除 `(0, 2)` 与 `.0` 版本限制并统一升级至 `0.3.0`**：`Cargo.toml`、`pyproject.toml`、`uv.lock`、`scripts/release_version.py` 已切换至 `0.3.x` 门禁，旧 `src/rp_ylx/`、`native/`、`tests/`、`contracts/ylx-*` 已彻底清理。
2. **[已完成] CI 与纯原生 RDK X5 打包流水线就绪**：`.github/workflows/ci.yml` 与 `.github/workflows/rdk-x5-bundle.yml` 已切换为纯 Rust + C 原生构建与验证，移除 CPython 运行时与 Python wheel 打包链路。
3. **[已完成] 存储布局与 API v1 契约固化**：详见 [存储与 20 GiB 零增长系统盘规范](miro-ego-storage-layout.md) 与 [Miro-Ego API v1 迁移指南](miro-ego-v1-api-migration.md)。
4. **[已规划至 0.3.0 后 Backlog] 增量 OTA、签名发布渠道与离线录制盘更新包**：`crates/miro-ego-deploy/src/bundle.rs` 已预留 `StoreLayer`（CAS 增量层）与 `OfflinePackageHook`（`/data/updates/` 离线更新包）扩展接口，线上 OSS 渠道与 Ed25519 签名发布在后续版本独立交付。
5. **[待实机执行] RDK X5 实机浸泡验收与主干切换**：
   - 在 RDK X5 V1.0 实机上使用本地/SDK 编译的 `miro-ego-encoder`（链接 `/usr/hobot/lib/libmultimedia.so`）与交叉编译 `miro-ego` 组装纯原生 bundle；
   - 完成离线刷卡首启（验证 p1 `CONFIG` 清理、p2 20 GiB 零增长、p3 exFAT `MIRO-EGO` 挂载与 `ATTR_HIDDEN`）、双目长录、断电恢复及 `miro-ego deploy` A/B 升级回滚验收；
   - 从 `main` 切出 `release/0.2` 冻结分支后，将 `refactor/0.3.0` 合入 `main` 并打附注标签 `v0.3.0`。

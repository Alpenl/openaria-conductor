# 全仓消融实验：删除未使用模式、重复规则和转发层

日期：2026-09-12。分支：`refactor/recent-iterations-ablation-20260912`。

本轮基线是 `24bb5089c60997dcfb75f9043af4a020c0b6374e` 加上
[上一轮消融](../recent-iterations-20260912/README.md)的未提交补丁。
[baseline.patch](baseline.patch) 保存了精确基线，可独立重建。
本轮改动涉及 **12 个实现文件，净减少 256 行**；这个统计包含 Rust 文件中新增的
回归测试，不包含 Python 测试及实验记录。加上上一轮，实现文件累计净减少 296 行。

## 筛查范围

[inventory.json](inventory.json) 列出 104 个实现源文件及 SHA-256：
82 个 Python、18 个 Rust、3 个 C、1 个头文件。范围为 `src/`、`native/`、
`scripts/` 和根目录构建后端。先检查定义、引用、转发函数和状态持有者，再对明确候选
执行删除和行为对照。这是全仓候选筛查与定向消融，不能据此宣称每个函数都经过形式化证明。

| 范围 | 本轮处理与保留依据 |
| --- | --- |
| Rust 队列、相机流 | 删除未使用的生产者等待模式和队列测试计数；保留容量、接收等待、关闭、重启、实际相机丢帧计数。 |
| Rust 录制运行时、资源持有者 | 删除重复的逐帧调度结构；录制资源共享一个整体引用，保持停止和在途帧的生命周期。 |
| Rust FFI、音频、IMU、I/O、预览、指标 | 保留实际设备资源所有者、ABI 契约、可观测指标和资源释放边界；ABI 6 导出回归现在实际执行。 |
| Python HTTP、网络控制、网络状态 | 合并等价的请求规则，删除转发校验；请求、持久化状态和对外状态中语义不同的规则继续各自负责。 |
| 录制、下载、目录、删除 | 删除清单转发层和无效参数；保留内容哈希、路径检查、文件身份、事务提交和删除隔离。目录沿用上一轮简化。 |
| Python 相机、IMU、原生接线 | 删除 IMU 解码的纯转发入口内部层；保留 Python 调试/测试采集路径与 Rust 生产采集路径。 |
| 时间戳、标定 | 沿用上一轮按需构造和流式 CSV；公共整表接口仍有明确语义。 |
| CLI、运行时、性能、硬件探测 | 保留实际命令入口和测量/错误映射；小函数存在参数变换或多处调用时，不仅凭行数删除。 |
| 安装、更新、网络恢复、systemd、CI | 删除未调用的身份生成副本、旧网络激活入口和单独清空 work 的入口；保留发布身份与恢复事务。 |
| C 编码流水线 | 保留分眼、编码、封段线程与缓冲区所有权；本机 C 片段测试覆盖 JSON 和 MP4 等逻辑，整板 SDK 路径未实机消融。 |
| 合约、OpenAPI、Web、历史实验 | 保留兼容性样本及可复核证据；Web 在此仓库是发布产物，实际 UI 源码属于独立 Echo Web 仓库。 |

## 落地的消融

### 1. 有界队列去掉未使用的通用能力

`native/src/bounded.rs` 的生产调用只有 `try_push`。它原先转发到
`push_timeout(..., Duration::ZERO)`，却连带维护等待写入的 Condvar 和超时分支。
`QueueStats` 只有测试读取，而四个统计字段仍在生产路径持续更新。

现在直接执行非阻塞入队，删除超时写入、第二个 Condvar、测试专用统计及其更新。
相机模块自身的拒收/丢帧统计仍参与帧损失核算。该文件从 267 行降至 163 行。

同一份 [queue_probe.rs](queue_probe.rs) 分别编译实际基线和候选模块：

| 指标 | 基线 | 候选 |
| --- | ---: | ---: |
| 200,000 次入队/出队，七次中位数 | 60.61 ms | 36.17 ms |
| 相对耗时降低 | — | 40.3% |
| 三种容量的确定性操作轨迹 | 6,000 个操作 | 结果摘要全部一致 |
| 两个生产者的并发传输 | 80,000 帧 | 无丢失，每个生产者的帧保持顺序 |

这是 Linux x86_64 主机上队列本身的微基准，不代表整机录像吞吐提升。
原始七次采样在 [results.json](results.json)。

### 2. 网络请求规则只有一份实现

新增窄模块 `network_validation.py`，由 HTTP 网关和特权控制进程各自在入口调用。
删除六个重复校验函数，并直接使用已有的状态校验实现，去掉三层单纯转发。
共享模块没有注册表、插件、配置开关或新增依赖。

请求、状态展示与持久化对象存在真实契约差异，没有将三者合成一套过度宽泛的规则。
5,061 组输入覆盖缺字段、额外字段、嵌套类型、SSID 字节长度、IPv4/DNS、
凭据标识、重试 ID，以及固定种子的多步变异：

- HTTP 层全部判定保持一致。
- 控制进程有 76 处预期差异：数组/对象 `mode` 从 `TypeError` 变成正常拒绝。
- 合法输入的通过结果保持一致。新增控制器入口回归确认非法 mode 不会触发实际调度。

结果同时记录原先就存在的 29 个其他 `TypeError` 和 4 个
`UnicodeEncodeError` 探针结果；这些行为保持一致，本次去重不声称已经修复全部畸形输入问题。

### 3. 清单和 IMU 处理去掉纯转发

- 合并 `_manifest_artifacts` 和递归遍历实现，删除传入后立即丢弃的
  `manifest_bytes`、`session_id`、`code`，并清理上游无效传递。
- 合并下载摘要实现和只调用一次的产物列表转发函数。
- 删除始终为 False 的 `path_validated` 开关，路径检查直接执行。
- IMU 的公开 `decode_native_imu_observation` 直接执行解码，删除私有转发层。

2,038 组清单投影、字节汇总和产物描述符对照完全一致，其中包含 196 组成功投影。
这些探针显式使用已经解码的清单；完整 schema、内容校验、封存和下载由现有回归覆盖。
最后的 IMU 简化另通过 38 项 IMU/采集源测试。

### 4. 录制资源不再逐字段复制到第二份结构

删除 `RecordingDispatch`。状态持有 `Arc<SplitSinkRecording>`，
每帧从原先克隆五个 Arc 改为克隆一个整体 Arc；写帧函数借用该所有者和帧对象，
参数从九个减到三个。失败路径按需克隆回调引用。

保持各资源的锁与实际生命期约束。这项没有进行整机性能测量，只报告减少的
引用操作和接口复杂度。新增并发测试在基线与候选都通过：用锁阻塞已接收帧，
发出停止请求，确认入口关闭，随后该帧仍完成编码和索引写入。
Rust 原有测试加这条回归共 82 项通过。

### 5. 删除无调用入口，修复一处失效的验证

删除 daemon 中重复的 `default_device_identity`、
网络模块的 `activate_saved_network`、状态存储的 `clear_work`。
真实安装身份生成、候选网络恢复和事务内清理仍由现行调用链承担。

`test_abi_six_extension_exports_only_deep_and_support_classes` 原先用
`NATIVE_ABI != 5` 跳过当前 ABI 6。现在直接断言 ABI 6 并执行导出边界检查，
避免已删除的浅层接口被重新引入却无人发现。

## 反例消融：这些检查必须保留

反例只修改临时副本或独立探针进程，不进入生产候选：

| 临时删除的内容 | 观测结果 | 决定 |
| --- | --- | --- |
| 网络请求校验 | 3,385 个应拒绝的映射请求变为接受 | 保留两层入口校验，共享规则实现。 |
| 产物路径校验 | 19 个探针结果发生变化 | 保留路径检查，删除绕过开关。 |
| 队列容量限制 | 确定性轨迹立即失败：原本应拒绝的帧被接收 | 保留有界队列。 |

上一轮“跳过整个会话内容校验”的反例同样不成立：另一只眼的文件损坏时会放过下载。
因此本轮继续保留该校验。

## 验证与复现

[validation.json](validation.json) 保存命令、结果、日志摘要及文件身份。

- Python 全仓检查：752 项，成功，1 项因缺少 X5 SDK 头文件跳过。
- Rust：基线 81 项、候选 82 项全部成功；Clippy 所有目标无警告。
- 本轮通过 PEP 517 构建 wheel，并把新 wheel 的 ABI 6 原生扩展用于 Python 回归；
  全仓检查还验证外部安装 wheel。
- IMU/采集接线：38 项单独回归成功；ABI 导出所属原生测试：16 项全部成功。
- Ruff 检查、格式检查、Rust 格式检查、补丁空白检查通过。

重建基线后运行实验，路径可按实际环境调整：

```bash
shnote --what "克隆消融基线" --why "重建上一轮代码" run git clone --shared . /tmp/openaria-repository-baseline
shnote --what "固定基线提交" --why "防止主线变化影响比较" run git -C /tmp/openaria-repository-baseline checkout --detach 24bb5089c60997dcfb75f9043af4a020c0b6374e
shnote --what "恢复上一轮改动" --why "本轮以第一轮候选为基线" run git -C /tmp/openaria-repository-baseline apply /home/alpen/DEV/openaria-conductor/experiments/repository-ablation-20260912/baseline.patch
shnote --what "运行全仓消融对照" --why "复核行为与队列开销" run .venv/bin/python experiments/repository-ablation-20260912/run_ablation.py --baseline /tmp/openaria-repository-baseline
```

需要现有 Python 开发依赖和 Rust 编译器。没有执行 RDK X5 实机录像、
相机/IMU/ALSA/NetworkManager 验收或整板编解码 SDK 验收。

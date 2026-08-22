# Open Aria Conductor

Open Aria Conductor 是运行在 D-Robotics RDK X5 V1.0 上、配套 YLX 2UQ2 的双目视频与 IMU
同步录制软件。本版本只支持这一硬件组合；Raspberry Pi 不属于支持矩阵。0.5 代码仍保留
`rp-ylx` package、CLI、systemd unit 和数据标识，以兼容现有安装与已录制会话。

浏览器控制端由 [Open Aria Echo / Web](https://github.com/Alpenl/openaria-echo-web)
独立构建，Conductor 固定其提交与摘要并在设备本地托管静态制品。

## 项目职责

- 发现并配置受支持的双目相机与 IMU。
- 采集双目视频帧、完整 IMU 样本和明确的时间戳。
- 检测丢帧、传感器停止、存储故障和无效设备状态。
- 管理从准备、录制到结束封存的完整录制会话生命周期。
- 持久写入会话元数据、清单、校验值和诊断记录。
- 自身提供有版本的设备控制 Web API 与浏览器实时预览。
- 报告设备健康状态、剩余容量、录制进度和故障原因。
- 为受支持的 RDK X5 设备提供可重复的安装、升级和回退方法。

## 项目边界

Conductor 负责设备端采集以及写入 RDK X5 存储设备的录制会话。PC 端会话管理、批量数据处理和
最终结果展示不属于本项目。

录制正确性不能依赖预览客户端是否在线。预览、日志和控制通信不得暗中改变采集格式，
也不得降低录制可靠性。

## 对外接口

- 供 Open Aria Bridge / SDK 与 Desktop 使用的录制会话格式。
- Echo / Web 使用的实时预览、控制、状态和诊断 Device API。
- 未来移动端可复用的同一 Device API；移动端不属于当前交付。
- 可在没有真实相机时运行自动化测试的硬件抽象接口。

任何消费者依赖具体实现行为之前，都必须先确定接口版本和兼容规则。

## 初始工作方向

1. 硬件访问与确定性的设备发现。
2. 双目视频帧和 IMU 数据采集。
3. 时间戳、同步和数据丢失检测。
4. 会话生命周期、磁盘布局、封存和故障恢复。
5. 设备控制接口与有资源上限的实时预览传输。
6. RDK X5 端打包、服务管理、运行观测和真机测试。

## 旧代码复用规则

每个复用组件都要记录来源提交、保留行为、依赖、许可证、修改内容和回归测试。缺少可重复
测试的硬件代码，应先固化其外部行为，再放到可测试接口之后重写。

## 开发命令

需要 Python 3.11 或更高版本，推荐使用 `uv`：

```bash
uv sync --extra dev
uv run rp-ylx --version
uv run rp-ylx status
uv run rp-ylx probe
uv run rp-ylx hardware-smoke --help
uv run rp-ylx serve-mock
uv run rp-ylx serve-hardware-preview --device /dev/video0
uv run python scripts/check.py
uv build
```

`status` 在没有相机和 IMU 的电脑上也能正常运行，并明确报告硬件尚未探测。
录制写盘、背压和失败目录语义见 [有界录制管道](docs/recording-pipeline.md)。
RDK X5 的固定硬件事实、短录制步骤和证据边界见 [RDK X5 基线](docs/rdk-x5-baseline.md)。
RDK X5 的热点、客户端、有线、mDNS 和救援行为见 [配网与救援](docs/networking.md)。
测量驱动的 Rust 数据面范围、性能闸门和当前证据边界见
[RDK X5 Rust 性能重写基线](docs/rust-performance-rewrite.md)。

## 当前状态

Rewrite MVP 已建立最小可运行基线。0.5 的差量功能按 GitHub Issue 逐项实现与验收。

公开开发任务见 [GitHub Issues](https://github.com/Alpenl/openaria-conductor/issues)。

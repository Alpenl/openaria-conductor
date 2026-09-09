# IMU 完整包去重修复与实时显示整合

日期：2026-09-09。基线：Conductor `0918356934f63007b58bfb57d2cff0fd7e5ef53e`。

## 结果与验证范围

Python 与正式录制使用的 Rust 采集器均已改为比较完整的 27 字节响应。同一个视频帧号下，只要完整响应变化，就保留包内两组六轴记录。完整响应不变时仍等待新数据，超时仍报告传感器停滞。

本次结果分为两个独立实验：

| 验证 | 原始响应/保留包数 | 六轴记录数 | 窗口 | 记录率 |
| --- | ---: | ---: | ---: | ---: |
| .248 之前保存的原始包，经旧视频帧号规则计数 | 893 | 1,786 | 15.002477 秒 | 119.047 组/秒 |
| 同一批原始包，经修正后的 Python UVC source + collector 回放 | 3,048 | 6,096 | 15.002477 秒 | 406.333 组/秒 |
| 本次 .248 临时运行修正后的 Python UVC source + collector | 2,718 | 5,436 | 15.001887 秒 | 362.354 组/秒 |

回放保留了此前误丢弃的 2,155 个同帧新包。新一轮真机采集保留 1,825 个同帧新包；包序列与样本序列连续，各包的主机时间严格递增。

本次 Python 真机路径平均 GET_CUR 耗时 4.449 ms，查询间主机处理间隙平均 1.071 ms、中位数 0.811 ms，说明这条临时 Python 路径还有主机处理开销。362.354 是本轮实测值，406.333 是历史原始包的回放结果，不能相互替代。Rust 路径已编译并通过测试，本次没有把新 Rust 发布包安装到设备，不能把 Python 实测等同于新正式录制版本验收。

以上均为主机收到的六轴记录率，未验证独立硬件采样频率，也未证明加速度计 800 Hz 全量导出。相同完整响应仍无法区分缓存重复与真实数值完全相同的连续采样。

## 采集与时间处理

- `native/src/imu.rs` 和 `src/rp_ylx/imu/uvc_xu.py`：将仅比较前三字节改为完整包比较；新包成功读取后不额外 sleep，只有重复响应才有等待。
- 24 位视频计数展开允许重复、允许回绕，仍拒绝回退。Python 独立 `TimestampUnwrapper` 的旧默认行为保留，由实时 collector 显式开启重复计数支持。
- 同帧的新响应不作为独立设备时钟锚点，也不复用帧拟合来声称已经确定新样本时刻；该响应输出 `sync.quality=insufficient`、空 offset/residual。不同帧的拟合仍属于视频计数与主机读取之间的证据，不是硬件 IMU 采样时钟。
- 主机时间校验覆盖每次响应，包括没有加入拟合的同帧响应，不能用同帧包掩盖主机时间回退。
- 原始 `ylx.imu.v0` 记录格式保持不变。包内两组数据共享实际 GET_CUR 的读取起止时间与中点，未将推算时间覆盖到原始证据字段。
- 保留全部 signed big-endian int16 数据，不丢弃低 4 位，不更改量程、单位或标定参数。
- `missed_packets_estimate_available=false` 明确说明没有独立 IMU 硬件包计数。既有显式主机/传输丢失仍保留计数和序列缺口；视频帧号跳变不推算 IMU 丢包，`dropped_samples=0` 不表示硬件零丢失。

## 标定导出兼容性

Spectacular 原实现要求 `device_ticks` 对每个包严格递增，并默认按 120 组/秒拟合，修正采集后会拒绝合法的新数据。这条链路已一起调整：

1. Device Session v2/v3 接受视频计数重复，但继续检查计数回退、原始/展开值一致性、包与样本序列连续性。
2. Device Session 默认从主机接收证据计算包吞吐，不再默认强制 120 组/秒。未提供预期速率时，诊断 `expected_rate_hz` 与 `rate_error_ppm` 为 null，避免把实测速率与自身比较而宣称满足标称值。`imu_clock.measured_rate_hz` 仍是包率，六轴记录率需乘每包两组。
3. 包内时间仅在导出的派生时间线上估计：后一组放在读取中点，前一组向前一个估计周期；若前后读取间隔较短，包内间距随之缩小，保证严格递增且不晚于当前主机读取时刻。中断间隙不会被拉伸填充成连续采样。
4. 导出模型增加 `imu_timing`，诊断增加时间来源和估计标记，明确 `host_receive_interpolation`、`sample_times_estimated=true` 与硬件丢包估计不可用。
5. 旧 legacy raw 入口保留原时间重建方式与默认 120 组/秒。旧 Device Session 数据仍可读取，但现在采用主机读取插值，派生时间和模型摘要可能与旧版本不同。

显式指定 `openaria-spectacular-check CAPTURE --imu-rate 400` 时，仍校验收到的记录率是否在既有误差阈值内。该选项不是硬件 ODR 设置命令。本轮临时 Python 实测的 362 组/秒不能通过 400 ± 2% 的速率校验。

## 实时显示 worktree 整合结果

检查了 Conductor、Echo 及相关历史 worktree。所需显示优化已经在当前基线中，无需重复 cherry-pick：

- Conductor `0918356`：空闲 IMU 观察线程、录制前停止并等待空闲观察释放、录制结束后恢复观察、观察过期清理。
- Echo `3a279ddac1f58ffa2516d09c2d955f650f4aee47`：空闲时 `session_id=null` 的 IMU 显示和 500 ms capture status 刷新。当前 Conductor 的内嵌资源已经绑定该提交。
- 实时状态按完整样本标识与主机时间判断更新，不以视频帧号去重；已有同会话 revision 不变时刷新、空闲刷新、过期清理等回归覆盖。
- 页面刷新频率与录制采集频率分别管理。500 ms 刷新显示最新观察，不意味着只保存 2 Hz IMU。
- RP-YLX 旧 worktree 的未提交网络与旧 UI 改动与本次 IMU 显示无关，未移植到 Echo。

## 检查与设备恢复

- Python 全量 unittest：720 项，成功，2 项跳过。
- Rust `cargo test --lib --locked`：81 项通过。测试环境需要 `LD_LIBRARY_PATH=/home/alpen/miniconda3/lib`，否则测试可执行文件无法找到 `libpython3.13.so.1.0`。
- Echo `npm run check`：类型检查、生产构建以及桌面/手机 152 项测试通过，Echo 工作区没有新增修改。
- 所有 Git 跟踪的 126 个 Python 文件：Ruff check 与 format check 通过。
- 全目录 `scripts/check.py` 在所有 unittest 成功后，被既有未提交研究附件 `docs/reports/assets/2026-09-09-imu-rate/analyze.py` 的 16 项格式/lint 问题阻断。本次业务改动没有引入这些问题；该附件未纳入本次提交。
- .248 临时测试只在 `/tmp/openaria-imu-software-fix-20260909/` 装入 Python 模块，利用既有预览开启视频；未替换已安装服务，未改写 XU 控制、相机模式或设备配置。
- 测试结束设备仍 idle，服务 authority epoch 未变，预览 JPEG 可读。配置前后 SHA-256 一致：`7e506d3dca3351a79dbb024ec127d52b7952dd0cbd75d35b621647a06db0b81c`。

## 可复核证据

本地证据目录：`/data2/openaria-imu-rate-20260909/software-fix/`。

- `replay.py`、`replay-result.json`：实际采集类的原始包回放。
- `live_check.py`、`live-result.json`：本轮真机临时测试脚本、模块 SHA-256 和设备前后检查结果。
- `live-imu.jsonl`：本轮 5,436 组完整原始记录。
- 回放源 `../continuous.json` 的 SHA-256：`b1e1a21dc01692d966a166de816feec58353849f14d66395ebd34a41b7be9ff4`。
- 服务端完整检查日志：`/tmp/openaria-imu-fix-check.log`；Echo 检查日志：`/tmp/openaria-imu-echo-check.log`。

更早的硬件 ODR、USB 上限与量化分析见同目录研究报告。本修复不把软件接收率提升表述为芯片 ODR 已恢复，也不为历史上已被丢弃的记录生成替代样本。

# Pi 录制链路消融实验

基线：`e7da4babf1d1f48ebc262b5714497cfabac98801`。日期：2026-10-03。
本轮聚焦最近修改的 Conductor 原生录制、目录和预览路径。
每个候选先独立删减，再比较行为与耗时；没有触及 Android、桌面端或 SDK 产品源码。
全部数字来自 Linux x86_64 主机，不能解释为 RDK X5 实机帧率提升。

## 保留的简化

| 消融项 | 基线 | 候选 | 决定 |
| --- | ---: | ---: | --- |
| 录制进度快照，720 个历史分段 | 260.17 µs/次；639,734 B Python 峰值分配 | 0.740 µs/次；2,069 B | 删除无人读取的历史副本。 |
| 1,000 个缓存会话，仅元数据列表 | 7.89 ms | 3.60 ms | 用目录条目直接判断类型，按需构造 Path。 |
| 1,000 个缓存会话，保留已验证身份检查 | 38.05 ms | 32.79 ms | 同上；清单身份检查仍逐项执行。 |
| 原生写入器，A1 只删除丢弃的逐帧快照 | 37.38 ns/帧；约 2 次分配/帧 | 25.42 ns/帧；约 1 次分配/帧 | 删除无消费者的返回值。 |
| 原生写入器，A2 只改为单个待完成帧 | 37.38 ns/帧 | 31.19 ns/帧 | 集合改为 Option，匹配实际串行写入者。 |
| 原生写入器，A3 只删除测试专用掉帧模型 | 37.38 ns/帧 | 37.16 ns/帧 | 仅结构简化；没有可主张的性能收益。 |
| 原生写入器，A1+A2+A3 | 37.38 ns/帧；104 B 状态对象 | 18.33 ns/帧；72 B 状态对象 | 保留组合。30 fps 下节省的 CPU 时间仍极小。 |

这些实验测量不同局部，收益不能相加。四个实现文件合计净减少 24 行，
该统计包含 Rust 文件里的回归测试，不含 Python 测试和本实验目录。

### 进度快照只携带进度

`TransactionSnapshot.segments` 在 Rust 中克隆完整分段列表，随后 PyO3 再生成 Python 列表。
`device_session.py` 的进度消费者只读 `sink` 和 `encoder_stats`；收集分段另行调用
`transaction.segments()`。因此删除内部快照字典的 `segments` 字段，而专用分段接口、
最终 `finish()` 结果、分段登记和持久检查点保持完整。

这是内部快照字典的字段删减，不声称其字典形状完全不变。公开 HTTP 响应与录制文件格式不变。
使用真实 release 原生扩展、合成编码器事件，分别测量 0、120、720、2,880 个分段；
每组七轮，每轮 1,000 次。内存另以 100 次调用测量 Python 分配，不是进程 RSS。
其他快照字段、显式分段列表、空录制完成及重复完成、非法分段拒绝、abort 资源释放一致。
跨临时目录比较时，只将设备号、inode、mtime 归一化为整数类型；尺寸和摘要仍精确比较。

原始结果：[基线](snapshot-baseline.json)、[候选](snapshot-candidate.json)、
[对照方法与摘要](snapshot-comparison.json)。

### 目录扫描不重复查询类型

用有作用域的 `os.scandir` 替代 `Path.iterdir` 加每项类型查询；缓存命中时不先构造 Path。
符号链接单独沿用 `Path.is_dir`，保留循环和悬空链接的过滤行为。
目录替换后仍重新打开当前路径检查清单身份；单个产物下载不枚举其他会话。

基准用一条真实封存会话生成缓存形状，再播种合成清单身份；不是逐条完整验证过的实际录像。
同一 fixture 比较基线和候选实际 `list_sessions` 方法，v4 响应精确一致。
每组九轮，测试 100、1,000、10,000 个会话；冷启动校验和设备存储性能不在测量范围。
10 项相关行为用例在基线和最终候选均通过，覆盖目录替换、链接、扫描异常关闭和精确下载。
原始结果和最终方法指纹见 [catalog-results.json](catalog-results.json)。

### 写入器只维护实际使用的能力

实际生产消费者 `write_split_sink_frame` 在一个采集线程中顺序 reserve/write/finish。
`finish_frame` 的状态快照始终被丢弃；待写集合从未实际容纳多个帧。
改为返回 `()` 和 `Option<u64>`，保留未完成写入、重复/错误完成、跨会话、溢出和关闭检查。
A2 每次录制只减少一次集合分配，**不是每帧一次**；逐帧分配收益来自 A1。

`reject_frame`/`record_drop` 只在测试中存在。删除该不可达模型后，导出的掉帧字段仍为
原有的 `0`/`[]`；FrameValidator、Metrics 和编码器中的真实损失证据继续保留。

20,000 条确定性轨迹、543,592 个可观察操作在支持的串行路径上一致。
能力收缩明确限定为：同一 transaction 不再接受重叠 reservation。
这两个方法不是 PyO3 导出，但原生引擎接口未全局禁止多个引擎共享同一 transaction；
这种非产品用法的重叠写入现在会失败，不能宣称整个原生接口的所有调用组合都等价。
九轮各百万帧交错测量，分配另行计数，详见 [native-results.json](native-results.json)。

## 拒绝的消融

| 临时删除 | 实验结果 | 决定 |
| --- | --- | --- |
| 预览的第二个编码器 | 模拟 75 ms 编码耗时，25 → 12.5 帧/秒；零延迟 codec 两者均为 25 帧/秒。首帧阻塞时单编码器无法启动第二帧。 | 保留两个有界编码器，支持较大图像的慢缩放路径。 |
| 原生写入器的未完成帧检查 | 有一个待写帧时错误地允许完成录制。 | 保留完成屏障。 |
| 链接的原有类型过滤行为 | 初版 scandir 遇自循环链接抛 ELOOP，旧 Path 实现跳过。 | 已修正候选并增加回归；表中目录结果使用修正后的最终代码。 |

预览实验运行真实调度代码，用合成 60 Hz 输入和带受控延迟的 codec，不测量真实 JPEG 性能。
三轮结果见 [preview-results.json](preview-results.json)。多目录迁移路由、下载期间的资源持有者、
完整性校验和恢复边界仍有实际消费者，本轮保留。

## 复现

从仓库根目录执行，使用开发 Python 环境。输出目录必须放仓库之外。
原生逐帧实验只需 Python 3.11+ 标准库、Git 和 rustc；目录实验还需要项目测试依赖。

```bash
ABLATION_DIR=$(mktemp -d /tmp/openaria-ablation.XXXXXX)
.venv/bin/python experiments/pi-ablation-20261003/run_native.py --output "$ABLATION_DIR/native"
.venv/bin/python experiments/pi-ablation-20261003/benchmark_catalog.py --temp-root "$ABLATION_DIR/catalog" --output "$ABLATION_DIR/catalog.json"
.venv/bin/python experiments/pi-ablation-20261003/preview_probe.py --repo . --output "$ABLATION_DIR/preview.json"
```

快照实验需要分别构建基线和候选 release 扩展（Rust、C 工具链和 Python 开发环境）。
只从 Git 提取基线的原生源码到外部实验目录；不用新增 worktree 或在仓内放构建输出。

```bash
mkdir "$ABLATION_DIR/baseline"
git archive e7da4babf1d1f48ebc262b5714497cfabac98801 Cargo.toml Cargo.lock native | tar -x -C "$ABLATION_DIR/baseline"
CARGO_TARGET_DIR="$ABLATION_DIR/target-baseline" cargo build --locked --release --features extension-module --manifest-path "$ABLATION_DIR/baseline/native/Cargo.toml"
CARGO_TARGET_DIR="$ABLATION_DIR/target-candidate" cargo build --locked --release --features extension-module --manifest-path native/Cargo.toml
.venv/bin/python experiments/pi-ablation-20261003/recording_snapshot.py --extension "$ABLATION_DIR/target-baseline/release/lib_native.so" --variant baseline --output "$ABLATION_DIR/snapshot-baseline.json" --work-dir "$ABLATION_DIR/tmp"
.venv/bin/python experiments/pi-ablation-20261003/recording_snapshot.py --extension "$ABLATION_DIR/target-candidate/release/lib_native.so" --variant candidate --output "$ABLATION_DIR/snapshot-candidate.json" --reference "$ABLATION_DIR/snapshot-baseline.json" --work-dir "$ABLATION_DIR/tmp"
```

回归入口为 `scripts/check.py` 和 `cargo test --workspace`，并运行 Ruff、Clippy 和格式检查。
Python 回归必须加载本轮重建的原生扩展，不能使用旧二进制来验证本轮修改。
完整检查结果随本轮 PR 更新；大型数据、编译输出和详细日志存于仓外。

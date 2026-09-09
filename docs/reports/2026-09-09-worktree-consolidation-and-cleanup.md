# 本地工作树整合与清理

日期：2026-09-09。

## 整合结果

本次整理当前设备与客户端链路的六个仓库：Conductor、Echo Web、Bridge SDK、Bridge Desktop、Echo Mobile、pi-dev。清理了 20 个实体 worktree 和 5 条目录已不存在的 Mobile 登记，每个仓库现在只保留主工作目录。没有删除分支引用，没有推送、发布 OSS 渠道或更新设备服务。

新增内容和历史归并分开提交：

| 仓库/提交 | 处理 |
| --- | --- |
| Conductor `ec1da79` | 正常合并 `feat/one-click-update` 的 4 个提交：一键安装/更新/回退、国内 OSS 发布、暂时性上传失败重试、通用冲突响应下的已有对象验证。保留当前 IMU 与录制优化。 |
| Conductor `af20ea3` | 归并旧远程删除、ABI 重构、采集优化和初始音频中断原型的历史；这些功能已移植到现行代码，旧原型已由后续实现替代，合并前后 Git tree 一致。 |
| Conductor `160d4c4` | 提交此前未跟踪的四份 IMU/视频清晰度报告及十份研究附件，修正分析脚本的 lint 问题。 |
| Bridge SDK `b23b1a8` | 归并早期帧时钟导出原型历史，继续使用 renderer v5 的逐帧时间和音频时钟实现；Git tree 不变。 |
| Bridge Desktop `bf50bfc` | 归并已 cherry-pick 的版本与 capability 修复；补丁等价性经 `git cherry` 确认，Git tree 不变。 |
| Echo Mobile `6d8ad38` | 归并已移植的网络、mDNS、JPEG/focus 优化及旧发布历史；保留当前 0.1.10/versionCode 13，不回退到历史 0.1.9，Git tree 不变。 |
| Echo Web `3a279dd` | 已包含所有旧工作树提交与实时 IMU 显示优化，无需改代码。 |
| pi-dev `ccc9b03` | audio-clock 工作树与主目录同一提交，无需改代码。 |

Conductor 的旧提交对应现行移植包括：产品接口 `4d8c680` → `15435f3`、不可达原生接口清理 `6ce782d` → `3b09b81`、冗余录制抽象清理 `b9a6448` → `a5977d0`、采集拷贝/轮询优化 `b18be3c` → `50d4bdc`，远程删除 `0d9a912` 与现行补丁等价。音频中断原型和 SDK 原型没有覆盖已经验证的后续实现。

上述纯历史归并使用 `ours` merge，并逐个断言合并前后 Git tree 相同；一键安装功能使用正常 `ort` merge，完整引入新增文件与改动。清理前逐一验证了所有目标 HEAD 是各自主目录 HEAD 的祖先。

## 清理数量与空间

| 仓库 | 删除实体 worktree | 清除失效登记 |
| --- | ---: | ---: |
| Conductor | 4 | 0 |
| Echo Web | 3 | 0 |
| Bridge SDK | 3 | 0 |
| Bridge Desktop | 6 | 0 |
| Echo Mobile | 3 | 5 |
| pi-dev | 1 | 0 |
| 合计 | 20 | 5 |

清理前后文件系统可用空间的差值：开发目录所在文件系统约增加 **20.61 GiB**，`/data2` 约增加 **42.45 GiB**。这是操作窗口内观测的可用空间差，可能包含系统同时发生的少量其他写入。

只清理了确认干净、无运行进程工作目录依赖、提交全部可达的检出。检查了保留主目录中的符号链接，没有指向被清理的工作树。保留了旧分支引用，公开下载渠道仍指向此前发布版本。

## 未做架构迁移的历史目录

- `/data2/openaria-finish-20260831/worktrees/console`：与 `/home/alpen/DEV/egoview-console` 没有共同 Git 历史，属于不同架构；不能作为普通工作树合并，不清理。
- `/data2/openaria-product-manual-20260830/source/score-main`：历史云端合同/产品文档参考检出，未纳入本次设备客户端整合，不清理。
- `/home/alpen/DEV/RP-YLX`：旧 v3 网络与内嵌 UI 的 15 个文件修改仍保留，未覆盖 Conductor/Echo 的现行实现。这是独立仓库的主目录，不是本次清理目标。
- `/home/alpen/DEV/openaria-score`：原有 consumer-matrix 本机路径修改仍保留。
- `/home/alpen/pi-dev`：原有 `.codex-work/`、产品介绍材料及两个历史 RDK 运维脚本仍保留为未提交资料；它们不在被删除的 audio-clock 工作树中。旧 bootstrap 绑定历史 commit，未作为当前一键安装入口引入。

这些目录的 Git 引用、已修改文件和非忽略的未跟踪文件均已保存快照。本次没有将“已备份”描述为“功能已完成迁移”。

## 验证

- 合并后的 Python 全量 unittest：737 项，成功，2 项跳过，包含一键更新与现行 IMU/录制/API 回归。
- Ruff check 全目录通过；Ruff format check 全目录通过（151 个文件）。完整检查第一次在研究文档代码块格式处失败，格式修正后单独复查静态检查通过；没有为纯文档空白修改重跑全部测试。
- 一键发布工作流 YAML 可解析，所有 `run` 块通过 `bash -n`。
- SDK/Desktop/Mobile 只做历史归并，已断言 Git tree 不变；Echo 与 pi-dev 无代码变化。
- 每次删除前再次核对 HEAD、`git status --porcelain` 和祖先关系，使用普通 `git worktree remove`，没有强制删除脏工作树。
- 没有运行设备录制、构建新的正式 RDK 安装包或触发云端发布。

## 备份与恢复

备份根目录：`/data2/openaria-worktree-cleanup-20260909-212247/`。

- `before.json`：原始仓库、工作树、分支、HEAD、未提交/忽略文件清单。
- 各仓库 `before.bundle`：所有引用的 Git bundle，逐个运行 `git bundle verify` 并保存 SHA-256。
- `worktree-N-tracked.patch` 和 `worktree-N-local-files.tar.gz`：已修改文件和未跟踪文件快照。
- `worktree-N-outputs.tar.gz`：需要保留的 dist、验收输出、APK/安装包及 local.properties；不随重复缓存一起删除。
- `history-reconciliation.json`：纯历史合并的提交、源分支与 tree 不变校验。
- `cleanup.json`：逐个删除的路径、失效登记及磁盘空间前后值。
- `conductor-check.log`、`conductor-static-final.log`：测试和最终静态检查证据。
- 各核心仓库 `after.bundle` 与 `after.json`：清理后的最终引用和状态。

恢复旧工作树可直接在主仓库执行 `git worktree add /新的路径 原分支名`，原分支均保留。如需在其他机器恢复，可用对应 bundle 克隆，再检出原分支；未提交文件和原构建输出可从对应归档解包。

# Open Aria Conductor Round 2 深度审查与完美化改造台账（2026-11-26）

本报告基于第一轮 67 项改造完成后的全仓库状态，由 6 个专项深度审查代理与静态 AST/卫生扫描联合生成，共定位 **52 项** P0/P1/P2 级隐患、生产回归风险、契约漂移、架构冗余与测试瓶颈，已通过 R2-0 ~ R2-5 六个波次 **52/52 全部清零** 并完成最终严格验收。

---

## 一、Core 与控制面（R2-C01 ~ R2-C10）

- [x] **R2-C01** `[P0]` `src/rp_ylx/core/fs.py:138` — 已新增权威 `is_exfat_or_msdos_fd/path`（Linux `fstatfs` 联合 `fstatvfs.f_files == 0` 兼容 mock 与 exfat-fuse），`write_bytes_atomic` / `write_json_atomic` 支持 `mode=None` 并在 exFAT 上豁免 `EXFAT_UNSUPPORTED_ERRNOS`（含 `EINVAL`/`ENOTSUP`/`EPERM`/`EOPNOTSUPP`），POSIX 严格重抛；`export_atomic_no_overwrite` 补齐 `ENOTSUP`。
- [x] **R2-C02** `[P1]` `src/rp_ylx/persistence.py` / `settings.py` / `database_admin.py` — 已移除有泄漏风险的未使用线程局部 `pool_connections` 路径并在 `connection()` 校验 `_closed`、补全 `__enter__/__exit__`；authority/marker 检查统一改用 `os.path.lexists`；全部 `os.open` 补齐 `O_CLOEXEC`；统一复用 `core.locks.acquire_flock`、`chown_to_parent`、`read_bounded_json`、`persistence.readonly_connection` 与 `core.fs.export_atomic_no_overwrite`（消除 129 行重复 `_export_json`）；删除 `_require_regular_database_file` 并在 `settings.initialize()` 关闭 `db`。
- [x] **R2-C03** `[P1]` `src/rp_ylx/core/rpc.py` — `serve_forever()` 遇 `ConnectionAbortedError`/`InterruptedError` 继续服务、`EMFILE`/`ENFILE` 退避重试；`_handle_connection`/`_reject_busy` 统一捕获 `RpcFramingError` 返回结构化错误响应；新增 `_serve_done` Event 与 `_resources_closed` 锁保护并区分 `self._owns_listener`；提供 `read_timeout_seconds`/`max_connections`/post-write hook。
- [x] **R2-C04** `[P1]` `src/rp_ylx/core/fs.py` & `daemon.py` — `read_bounded_json` 捕获 `RecursionError` 转为 `ValueError`；`read_bearer_token_file` 与 `daemon._validate_tls_file` 补齐 `O_NONBLOCK` 防 FIFO 阻塞，并循环 `os.read()` + 校验 `st_nlink == 1`；`load_production_config` 统一通过 `read_bounded_json` 读取。
- [x] **R2-C05** `[P1]` `src/rp_ylx/core/device_client.py` & `config.py` — 修复 `"::"`/`"::0"` `host` 映射为无方括号 `"::1"`（仅在 `authority`/`url` 加方括号），拦截 `%` scope ID 并校验绝对路径；`config.py` 补齐 `math.isfinite`、严格非 `bool` 整数校验、禁止 `None` 静默转 `"None"` 并复用 `core.ids`。
- [x] **R2-C06** `[P1]` `src/rp_ylx/core/locks.py:46-48` — `acquire_flock` 创建锁父目录后显式 `chmod(0o770)` / `chown` 抵御 `UMask=0077`，补齐 `O_NONBLOCK` 并在 `shared=True` 且锁文件已存在时优先以 `O_RDONLY` 打开。
- [x] **R2-C07** `[P1]` `src/rp_ylx/daemon.py` — 收窄 `start_preview()` `except BaseException` 为 `except Exception`；将 `initialize` / `load_production_config` / `build_production_service` 与信号注册纳入统一 `try/finally` + `suppress(KeyboardInterrupt)`；`CaptureEventPump` 增加 `math.isfinite` 校验，复用 `core.board.RDK_X5_MODEL`，清理 `_read_bearer_token` 并拆分 `build_production_service()` 辅助函数。
- [x] **R2-C08** `[P1]` `src/rp_ylx/recording_indicator.py` — `StatusLed.show()` 在写 sysfs 前重置 `self.pattern = None` 防止部分写入后 LED 卡死在闪烁态；`run_indicator` 捕获 `http.client.HTTPException`；严格解析 `[...]` 触发器令牌。
- [x] **R2-C09** `[P1]` `src/rp_ylx/update.py` — Ed25519 公钥加载改用 `os.path.lexists` + `O_NOFOLLOW | O_NONBLOCK | O_CLOEXEC` 有界读取并限长 env PEM；捕获 `(UnsupportedAlgorithm, TypeError, ValueError)` 且保留精确错误；`exfat_storage_required` 损坏时 fail-closed 并与 `deploy_pkg/storage.py` 对齐 `PARTUUID`；补齐 `HTTPException`/`sqlite3.Error`/`RecursionError` 捕获、重复键/`NaN` 拒绝、`BEGIN IMMEDIATE` + `busy_timeout` + `rowcount` 校验与 `maintenance_lock(shared=True)` 目录创建；去重 `atomic_json` 并加固 `require_idle()`。
- [x] **R2-C10** `[P2]` `src/rp_ylx/cli.py`, `hardware/{probe,smoke}.py`, `core/__init__.py`, `errors.py` — 拆分 `cli.main()` 与 `_record_hardware_smoke()`；`smoke.py` 清理失败 `staging`、规范设备路径匹配并复用 `write_json_atomic`/`utc_now_iso`/`normalize_board_model`；全量补齐 `core/*`、`errors.py` 与控制面模块 `__all__`。

---

## 二、Native Rust 与 Hobot C 数据面（R2-N01 ~ R2-N13）

- [x] **R2-N01** `[P0]` `native/src/py/mod.rs` — `allow_threads_if_gil_held` 声明并校验 `Py_IsInitialized() != 0 && PyGILState_Check() != 0`，杜绝非持锁线程在 `Drop` 中逆向抢锁死锁。
- [x] **R2-N02** `[P0]` `native/src/py/{mod,capture,session,preview}.rs` — 提取 `try_duration_from_secs_f64` 统一替换全部 `Duration::from_secs_f64` 并用 `checked_add`，在释放 GIL 前转为 `PyValueError`。
- [x] **R2-N03** `[P0]` `native/src/audio/alsa_ffi.rs` — `Pcm` 记录 `bytes_per_frame`，`readi`/`clock_anchor` 严格校验非空句柄、缓冲切片字节上限、返回帧数不越界与 checked 时间戳算术。
- [x] **R2-N04** `[P0]` `src/rp_ylx/hobot/ylx_stereo_pipeline.c` — `encoder_output_thread` 校验 `vir_ptr != NULL` 与归还错误并精准上报 `encoder_output_invalid`；`split_eye` / `split_thread_main` 保存 dequeued 容量、校验偶对齐/步长/64 位容量并在失败时归还零大小输入缓冲。
- [x] **R2-N05** `[P0]` `native/src/py/session.rs` — `open_verified_artifact` 立即以 `OwnedFd` 托管裸 fd，仅在 `PyDict` 完整构建成功时 `into_raw_fd()`。
- [x] **R2-N06** `[P0/P1]` `native/src/**/*.rs` — 非测试 Rust 代码 `.unwrap()` / `.expect()` 彻底清零（`Drop` 内 `ArtifactWriter::close` 改用 `unwrap_or_else(|e| e.into_inner())`，并覆盖 `bounded.rs`、`native_camera.rs`、`recorder.rs`、`wav_writer.rs`、`checkpoint.rs`、`v4l2.rs`、`capture_runtime.rs`、`stereo_encoder.rs`、`recording.rs`）。
- [x] **R2-N07** `[P0]` `native/src/session/session_io.rs` — 将路径枚举与按内容 `sha256` 去重拆离，确保不同路径同内容制品可正常通过 `seal_device_session` 校验。
- [x] **R2-N08** `[P1]` `native/src/py/capture.rs` & `session/capture_runtime.rs` — 增加 `lifecycle_lock` 串行化启停关闭并在校验后变更 `failure_sink`，补充 `Drop for PyFailureSink`，使用 `write_unraisable` 上报回调异常，完善 `start_preview`/`start_recording` 线程创建失败回滚。
- [x] **R2-N09** `[P1]` `native/src/session/session_io.rs` & `sys/fs.rs` — `seal_device_session` 在目录发布失败时尽力移除 `partial_root/manifest.json` 并用 `.create_new(true)` + 递增计数写临时 manifest；`sys/fs.rs` 对齐 exFAT 判断（含 `FUSE_SUPER_MAGIC && f_files == 0`）、切换 `libc::renameat2` + `EINTR` 重试、拦截 `O_CREAT|O_TMPFILE` 并复用 `openat_retry`。
- [x] **R2-N10** `[P1]` `native/src/py/preview.rs` — `NativeMultipartPreview::__next__` 在 `preview_unavailable` 时 `continue` 等待并将 multipart 拼装移出 GIL。
- [x] **R2-N11** `[P1]` `native/src/hardware/{imu,v4l2,bounded,native_camera}.rs`, `audio/{recorder,checkpoint}.rs`, `session/{session_store,stereo_encoder,recording}.rs` — 修复 IMU `EINTR`/可重试错误误关 fd/居中残差；V4L2 掩码 `0x0000_e000`、micros 界限、先检焦点范围再改 `CID_FOCUS_AUTO`、曝光独立错误码、判重 `raw == previous`；`bounded::Producer` 引用计数关闭；`native_camera` 控制通道排空；`audio::Recorder::stop` 有界等待；`SessionTransaction::finish` 终态检查；`EncoderProcess::start` 失败即杀；`RecordingSink::flush_and_close` 全关与 `0xFF` 填充。
- [x] **R2-N12** `[P1/P2]` `src/rp_ylx/hobot/*` & `native/hobot/*` — `ledger_record` 先校验序号、释放 `ledger_lock` 后再触发 `on_segment`；`ylx_stereo_encoder.c` 不再在回调内重入 `ylx_pipeline_stats`；`ylx_pipeline_open` 深拷贝字符串并判空 `error`；MP4 64 位时长与跳过 `elst` 空编辑；`ylx_pipeline_finish` 串行等待关闭；`segment_seal` 改用 `fstat`；校验全部 `snprintf` 与 `clock_gettime`；`ylx_packet_queue_push` 将 `malloc+memcpy` 移出锁；抽取 `ylx_cli_common.h` 并拆分 `sbs_live_encode.c` `main()`。
- [x] **R2-N13** `[P2]` `native/src/*` — 优化 `hash_file`/`verify_fd_interruptible` 缓冲、栈 `[iovec; 8]` `writev_all`、预览发布前释放 `shared.state`；统一 PyO3 错误构造与配置结构体减少 `too_many_arguments`；新增 `sys/fs`、`sys/clock`、`native_camera`、`stereo_encoder`、`session_io` 等 Rust 单测（增至 181 项）。

---

## 三、录制核心与生命周期（R2-R01 ~ R2-R09）

- [x] **R2-R01** `[P0]` `src/rp_ylx/recording/{coordinator,device_session}.py` — 缩小 `DeviceSessionRecorder.start()` 持锁区间仅覆盖状态迁移，`coordinator` 在 `recorder.start()` 成功后发布 `_active`，消除 `coordinator._lock <-> recorder._lock` AB-BA 与回滚死锁。
- [x] **R2-R02** `[P0]` `src/rp_ylx/recording/{coordinator,stop_workflow}.py` — 将全部 `_require_admission()` 调用移出 `_lock`/`_stop_condition`，彻底消除 `coordinator._lock <-> _admission_lock` 死锁。
- [x] **R2-R03** `[P0]` `src/rp_ylx/recording/{fs_safety,recovery_orchestrator}.py` — 提取 `copy_regular_atomic` 至 `fs_safety.py`，`recover_partial_directory` 改为先复制控制文件至 `partial/recovery/`、待 `os.rename` + `fsync_directory` 成功后再移除顶层控制文件。
- [x] **R2-R04** `[P0]` `src/rp_ylx/recording/stop_workflow.py` — `finish_stop_capture()` 传递 `root=sealed.path.parent` 至 `remember_sealed_session()`，修复 `.parent.parent.parent` 越级污染 SQLite `session_paths`。
- [x] **R2-R05** `[P1]` `src/rp_ylx/recording/{lifecycle,coordinator,session_catalog}.py` — 新增 `CapturePhase.DELETING`；在 `_starting`/`_deleting` 期间拒收校验；`_catalog_verification_epoch` 自增移入 `_lock`；`delete_sessions` 先落盘幂等回执再清除 `_deleting`；`_network_command` 拒绝 `CLOSING`；`close_capture` 排空后台线程并在正常分支后统一清理激活态。
- [x] **R2-R06** `[P1]` `src/rp_ylx/recording/{session_catalog,catalog}.py` & `recording_layout.py` — 修复同名目录替换污染 `_session_summaries`、新增 `SessionCatalog.forget_path`、校验 `_reverse_paths` 属主、SQLite 提交后再更新内存字典、保留首个重复 `session_id` 主路径、抽取共享 `iter_recording_entries()` 容忍并发删除并严格正则匹配。
- [x] **R2-R07** `[P1]` `src/rp_ylx/recording/{device_session,recovery,recovery_orchestrator,coordinator,session_catalog}.py` — 补齐 `audio/` 与 `video/` 子目录 `fsync_directory`；将音量/定时持久化、网络租约、`_verified` 更新与 `catalog_sessions` 磁盘巡检移出长持锁/写事务。
- [x] **R2-R08** `[P1]` `src/rp_ylx/recording/{device_session,coordinator,stop_workflow,deletion,backing_storage,volume,recovery,fs_safety,manifest_builder}.py` — 统一经 `fs_safety.write_json_atomic(..., mode=0o640)` 兼容 exFAT；`set_camera_focus` 串行化互斥并写后递增 `_focus_revision`；正常封存后迟发源错误只记告警不污染 `_retained`；`_mark_failed` 捕获 SQLite 异常；exFAT 删除前额外核验 `st_mtime_ns` 与 `manifest.json` SHA-256；`backing_storage` 支持 `f_files == 0`；`/proc/self/mountinfo` 取末条叠挂与单调存储缓存；`_publish_safe_swap` 二次校验句柄数且禁止待切换期间起预览；补全 `recovery.py` 清理容错、`verified_artifacts` 与短读循环。
- [x] **R2-R09** `[P2]` `src/rp_ylx/recording/*` — 从 `coordinator.py`（4139 → 2779 行）拆出 `recovery_orchestrator.py`、`stop_workflow.py`、`failure_recovery.py` 并在 `state_store.py` 提取幂等辅助；从 `device_session.py`（2508 → 1970 行）拆出 `session_verifier.py`、`audio_finalizer.py`、`native_bridge.py`；修复 `stereo_encoder`/`sources`/多文件打开资源泄漏、`_lock` 兼容属性及死代码并消除父包自引用环。

---

## 四、API、契约、流式传输与 Spectacular（R2-A01 ~ R2-A09）

- [x] **R2-A01** `[P0]` `src/rp_ylx/api/{security,middleware,http_server}.py` — `hmac.compare_digest` 统一按 UTF-8 编码比较并遍历全部 token；`_dispatch` `except Exception:` 补齐 `_close_if_request_body_is_unread()` 防 HTTP/1.1 失步。
- [x] **R2-A02** `[P0]` `src/rp_ylx/api/http_server.py` — 为 `SSLSocket.do_handshake()` 套用 `request_read` 累计超时并处理 `ssl.SSLError`；覆盖 `send_error()` 返回 JSON Problem Details；新增 POSIX self-pipe `GatewayServer` 唤醒机制彻底消除 `0.5s` `shutdown()` 空转。
- [x] **R2-A03** `[P0]` `src/rp_ylx/api/manifest_validator.py` — 去除缺失可选字段时的早退 `return`，确保视频分段与音频语义校验始终运行；`window_drops` 改为 $O(N \log N)$ 排序双指针滑窗；清理死函数 `_check_rfc3339_datetime`。
- [x] **R2-A04** `[P0]` `src/rp_ylx/api/locked_io.py` — `_exclusive_regular_identity` 复用 `core.fs.is_exfat_or_msdos_fd` 将 exFAT `st_ino` 归零；`_open_relative_regular` 逐级校验 `st_dev == session_dev`。
- [x] **R2-A05** `[P1]` `src/rp_ylx/api/ylx-device-v4.openapi.yaml`, `contracts/ylx-device-api-support.json`, `api/controllers/{device,sessions}.py` — 修复 `/firmware*` v4 OpenAPI 202 `FirmwareTask`、Unix 整数时间戳、`FirmwareAvailableRelease`、`message` 4096、401/403/500 与 `/clock/check` 权限说明并更新契约摘要；移除 `controllers/device.py` 的 `unittest.mock.Mock` 依赖，补齐 `firmware_*` 与 `delete_sessions` 封闭响应校验。
- [x] **R2-A06** `[P1]` `src/rp_ylx/api/security.py` — 将 `_CUSTOMER_MUTATION_OPERATIONS` 更新为真实 13 项写操作集合并校验 `SecurityPolicy.lab()` 的 `csrf_token` 格式。
- [x] **R2-A07** `[P1]` `src/rp_ylx/api/streams.py` — `_NetworkEventReplayBuffer` 使用每客户端 `_resync_anchors` 且锁外调用 `status_provider()`；`_SharedNetworkPoller` 每线程绑定独立停止事件；`EventReplayBuffer` 修复 3 次重试耗尽丢事件与容量边界 off-by-one；`_StreamRegistry.begin` 不覆盖显式 `session_cls`。
- [x] **R2-A08** `[P1/P2]` `src/rp_ylx/api/{middleware,router,preview_thumbnail,schema_validation}.py` & `controllers/*` — 修复预览 `OSError` 断连、`OPTIONS` Host/多头校验、HTTP/1.1 缺 `Host` 拦截、`0.0.0.0`/`::` 拦截、TLS 写字节对账、`/voice` 与 `/capture/timer` CORS 头、严格 `limit` 解析、500 与流中断日志、`PreviewThumbnails` 生命周期与退出竞态及清零 `src/` 内全部 `except Exception: pass`。
- [x] **R2-A09** `[P1/P2]` `src/rp_ylx/spectacular/*` & `api/validators/*` — `spectacular/adapter.py` 校验后立即关闭分眼视频 fd 并按 `media_type` 判 zstd；抽取公共 `DEVICE_SESSION_SCHEMAS` / `percentile` / 原子 CSV 写；清理 `validators/commands.py` 3 个死函数并迁移活体请求校验，对齐 `validators/network.py` 与去重 `validators/events.py`。

---

## 五、网络、部署、Systemd、运维脚本与构建 CI（R2-D01 ~ R2-D10）

- [x] **R2-D01** `[P0]` `src/rp_ylx/deploy/rp-ylx-recover.service` & `deploy_pkg/transaction.py` — 补全系统资产目录 `ReadWritePaths=`（可选路径带 `-` 前缀）并在恢复单元文件后触发 `systemctl daemon-reload`。
- [x] **R2-D02** `[P0]` `src/rp_ylx/deploy/openaria-idle-fstrim.conf` — 增加 `Type=exec`，修复被 systemd 拒绝加载的 `bad-setting`。
- [x] **R2-D03** `[P0]` `deploy_pkg/{bundle,transaction,systemd,config,schema}.py` & `update.py` — `UMask=0077` 下显式授予 `0755`/`0750`/`0644` 权限，为 `_write_atomic` 增加 `mode` 参数并为 `update.py --rollback` 透传 `umask=0o022`。
- [x] **R2-D04** `[P0]` `src/rp_ylx/network_control.py` — `serve_stdio()` 安装 `SIGTERM`/`SIGINT` 优雅退出；新增同启动 `_deferred` 超时自动中止（30s TTL）；stdio `OSError` 路径对称中止未放行延迟响应；修正 `_abort_unreleased_transaction` `recovery_action="none"`。
- [x] **R2-D05** `[P0]` `src/rp_ylx/deploy/rp-ylx-wifi-watchdog` & `.default` — 无论是否设环境变量在 `maintenance.lock` 缺失时一律 fail-closed；重载驱动/重启控制面/重启整机前统一要求独占锁 `flock -x`；`rotate_diagnostics` 加 `set -f` 保护并移除无用配置键。
- [x] **R2-D06** `[P0/P1]` `scripts/{publish_rdk_x5_oss,release_version}.py` & `.github/workflows/rdk-x5-bundle.yml` — 发布工作流接入 `OPENARIA_RELEASE_SIGNING_KEY`、`--publish` 默认强制要求签名（除非 `--allow-unsigned-dev`）；补全 UTF-8 编码声明；本地校验错误清晰展示；单次 `Bundle` 复用避免 4× 重复哈希。
- [x] **R2-D07** `[P0/P1]` `scripts/{_host_disks.py,card_export/export.py,factory_recovery/*}` — `_host_disks.py` 无条件跳过 0 字节读卡器并在 `export.py`/`disks.py` 提供独立分发自包含回退；`card_export` 对 `sudo` 导出目录恢复归属权并强化 `_ExfatFS` `0xE0..0xFF`/`ValidDataLength`/`ActiveFat`/64 MiB 上限校验；`recover.py` 支持 `EXDEV` 与 UTF-8；`device.py` 显式授权并清理临时文件；`prepare_image.py` 先校验后改名。
- [x] **R2-D08** `[P1]` `src/rp_ylx/{network,network_control,network_paths,network_validation,mdns}.py` & systemd units — `_scan()` 引入 per-gen `_ScanJob`；`_monitor_once()` 锁内二次校验 `hotspot`；捕获 `sqlite3.Error`；`network_paths.py` 不再把 `os.open()` `EACCES` 误报 `capture_active` 并统一属组 + `SupplementaryGroups=rp-ylx`、`ReadWritePaths=-/data` + `/data` tmpfiles；有界 `join(timeout=5.0)`；`network_validation.py` 对齐 RFC 3339 与终态不变式并清理 3 处死函数。
- [x] **R2-D09** `[P1]` `src/rp_ylx/deploy_pkg/{transaction,storage,control_shim}.py` & `deploy/{rp-ylx-data-volume,rp-ylx-device-login}` — `_recover_locked()` 先恢复资产配置再删事务日志；`_prepare_release()` 覆盖前先 quiesce；`_uninstall_locked()` 保留 `PRESERVED_DEPLOYMENT_ASSETS`；维护租约维持 `/run/rp-ylx` `0750`；`storage.py` fail-closed 并禁用启发式绕过；`control_shim` 加累计 2.0s 截止期；`rp-ylx-data-volume` 加 `trap` 清理、fail-closed 校验与数字 UID 解析；`rp-ylx-device-login` 升级不再强加 `sudo`。
- [x] **R2-D10** `[P1/P2]` `scripts/*`, `build_backend.py`, `.github/workflows/*`, `README.md`, `docs/*` — 改造 `scripts/check.py` 实现静态预检 + LPT 多进程并行单测 + 串行隔离 `test_distribution` + `--workers`/`--fast`（`--strict`/`CI=true` 禁止 `--fast`）+ 覆盖 `native/hobot/*.c` 与 `bash -n`/`cargo fmt --check`；`build_backend.py` 改为打包后置写入 wheel/sdist 不污染工作区；CI 去重 Cargo 执行并补全 `uv.lock`/`shellcheck`/`--python`；修复 2 个运维脚本 Bug 及 5 处文档误差。

---

## 六、测试速度、隔离、覆盖率与结构卫生（R2-T01 ~ R2-T04）

- [x] **R2-T01** `[P0]` `tests/` 全局隔离 — 已为 `CoordinatorHarness`、`DeploymentHarness`、`CaptureCoordinatorTest`、`CaptureSourcesTest`、`test_restore_*`、`test_readonly_backup`、`test_deployment` 与 `test_regression_fixes_part1.py` 补齐 `OPENARIA_MAINTENANCE_LOCK` 与临时路径隔离，并调宽 WSL2 DrvFs 下并行子进程冷启动超时护栏。
- [x] **R2-T02** `[P0/P1]` `tests/` 提速、消除脆弱断言与契约同步 — `test_distribution.py` 透传 `CARGO_TARGET_DIR` 并合并 venv 验证；消除 9.8s 空等 sleep；抽取 `tests/testkit/tls.py`（`cryptography` 替代慢速 RSA `openssl`）；合并 `test_hobot_*` `setUpClass` 单次编译并用 `scripts/check.py` 共享 stub 启用 `test_hobot_encoding_config.py`；回收 4 处服务线程并全改 `addCleanup`；修复 TLS 弱断言、line-slice、`device_login`/`deployment`/`network_control`/`firmware`/`gateway_contract` 预期。
- [x] **R2-T03** `[P1]` 新增 4 个 Round 2 专项单测与回归集 — 新增 [test_round2_core_and_api.py](file:///y:/openaria-conductor/tests/test_round2_core_and_api.py)（18 项）、[test_round2_recording_units.py](file:///y:/openaria-conductor/tests/test_round2_recording_units.py)（12 项）、[test_round2_deploy_and_network.py](file:///y:/openaria-conductor/tests/test_round2_deploy_and_network.py)（11 项）与 [test_round2_native_gil.py](file:///y:/openaria-conductor/tests/test_round2_native_gil.py)（4 项），合计 **45 项**新增专项单测，重编 `_native` 后全部通过。
- [x] **R2-T04** `[P2]` `tests/` 结构卫生彻底清零 — 已将剩余 16 处跨 `test_*.py` 导入与 ~1,700 行重复测试替身收口至 `tests/testkit/*`（含新增 `testkit/mdns.py` 等），实现跨测试导入 **= 0**、空壳 `__new__` 构造 **= 0**。

---

## 七、Round 2 最终全量验收结果（最后一次编辑后实测）

1. **构建与版本检查**
   - `cargo build --manifest-path native/Cargo.toml --release --features extension-module` & 更新 `src/rp_ylx/_native.abi3.so`：已重编并加载验证。
   - `python scripts/release_version.py`：输出 `0.2.16`（通过）。
   - `git diff --exit-code src/rp_ylx/_build_info.py`：工作区零污染（通过）。

2. **严格全量门禁：`CI=true python scripts/check.py`**
   - **总耗时**：**163.22s**（包含完整 Python + Rust + Clippy + 分发包构建，较 Round 1 仅 Python 串行测试的 **334.389s** 整体缩短超 **2.05 倍**）。
   - **Stage 1（快速静态预检）**：`compileall`（`src`/`scripts`/`tests`）、`ruff check .`、`ruff format --check .`（**447 files already formatted**）、Hobot C `gcc -fsyntax-only -Wall -Wextra -std=gnu11`（同时覆盖 `src/rp_ylx/hobot/*.c` 与 `native/hobot/sbs_live_encode.c`）、deploy 脚本 `bash -n`/`/bin/sh -n`、`cargo fmt --check` — **全部通过**（`shellcheck` 因宿主机未安装按设计跳过）。
   - **Stage 2（LPT 多进程并行 Python 单测）**：**204/204 模块**全部通过，总耗时 **103.20s**（较 Round 1 串行测试提速 **3.24 倍**；发现总测试用例数从 2,254 增至 **2,303 项**）。
   - **Stage 3（串行分发验证与 Rust 全量门禁）**：
     - `tests.test_distribution`：**PASS**（47.47s，wheel + sdist + sdist 二次编译安装校验全过）。
     - `cargo test --manifest-path native/Cargo.toml`：**181 passed; 0 failed; 0 ignored**（2.53s，较 Round 1 新增 15 项 Rust 单元测试）。
     - `cargo clippy --manifest-path native/Cargo.toml --all-targets -- -D warnings`：**0 警告**（0.27s）。

3. **`--fast` 开发提速模式验证**
   - `CI=true python scripts/check.py --fast ...`：稳定以退出码 `2` 拒绝执行（硬护栏生效）。
   - `python scripts/check.py --fast --skip-rust tests.test_round2_*`：**11.82s** 完成全套静态预检 + 4 个并行测试模块（Stage 2 仅 **4.72s**）。

4. **静态 AST、依赖架构与文档完整性校验（`HYGIENE_GATE_ALL_GREEN`）**
   - `SRC_PY_FILES=150` / `TEST_PY_FILES=205`
   - `BARE_EXCEPT=0`
   - `EXCEPT_EXCEPTION_PASS=0`
   - `RUNTIME_CYCLES=0`
   - `CROSS_PACKAGE_PRIVATE_IMPORTS=0`
   - `CROSS_TEST_IMPORTS=0`
   - `HOLLOW_NEW_CALLS=1`（仅保留合法调用 `__init__` 的 `tests/test_worker_start_cancellation.py:24`，空壳 `__new__` 彻底清零）
   - `NON_TEST_RUST_UNWRAP_EXPECT=0`
   - `BAD_DOC_LINKS=0`

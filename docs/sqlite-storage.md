# SQLite 持久化

录制素材、每段采样文件和 `manifest.json` 继续作为独立文件保存。SQLite 不承载视频，
也不会把历史 SHA-256 校验记录当作当前素材仍然完好的证明。

## 数据边界

- `state_root/state.sqlite3`：采集控制状态、焦距设置和幂等回执。回执单独存表，保留既有
  1024 条窗口；检查点只写状态和新增回执，避免反复重写历史请求。
  恢复读取在同一个 SQLite 读事务中取得状态与回执，避免并发检查点提交时把旧状态和
  新回执拼在一起。WAL 写入者仍可在读取期间提交；读取结束或失败都会释放快照。
  同库保存最近 4096 条已结束的素材读取回执，区分 metadata_only、partial、served、failed
  和进程重启后的 interrupted。served 只说明服务端完整读取/发送成功，不表示客户端已落盘或备份。
  下载初始化失败或取消会关闭已打开的目录/文件并归还卷句柄计数；已建立但未交付响应的
  回执尝试记为 failed，回执写入失败会记录日志，不阻止资源回收。
- `state_root/settings.sqlite3`：可修改的相机、音频、录制和空间阈值设置，以及修改历史。
  生效值仍通过完整生产配置校验。修改后应在空闲时重启服务。
- `state_root/catalog.sqlite3`：录像摘要、分段文件索引和历史校验记录。清单仍是目录事实来源，
  文件替换或固件校验版本变化会使缓存失效；重启后的首次访问检查文件类型和大小。
- 网络状态目录的 `controller.sqlite3`：网络控制事务、回执和期望状态。NetworkManager
  凭据、救援网络配置、底层恢复日志以及安装器的启动/回滚文件继续由原有机制管理。
- `/etc/rp-ylx/device.json` 保留设备身份、路径、监听、安全配置和首次导入的默认值。
  SQLite 初始化后，修改 JSON 中已经迁入的动态字段不会覆盖数据库。

初始化和设置 CLI 与服务加载使用同样的生效配置：已有数据库时，先用 SQLite 中的
动态字段覆盖 JSON 的旧默认值，再校验。旧 JSON 中已被覆盖的无效动态值不会阻止启动，
但设备身份、路径、监听及安全配置仍必须有效；首次导入仍先校验完整 JSON，再创建数据库。
数据库只建好表、尚未导入配置时可以继续初始化；已导入的配置行丢失、缺库、损坏或版本
不兼容时仍拒绝启动，不会用 JSON 静默重建权威配置。

数据库使用 WAL、`synchronous=FULL` 和短事务。首次导入保留原 JSON 和 `.pre-sqlite`
副本；`.sqlite3.initialized` 标记阻止数据库丢失后静默恢复旧 JSON。未知版本、损坏或缺失
权威状态应修复或恢复备份，不能删除标记后重新初始化。

已初始化数据库还必须保有完整的基础表结构（包括字段、非空约束和主键）。缺表或
结构不兼容时，启动、数据库检查和恢复预检都会报错，不会重建空表来掩盖丢失的配置、
幂等回执或修改历史。此检查不替代数据内容校验；目录专用的附加表仍受支持。

首次导入时，即使没有旧 JSON，初始状态和导入回执也在同一事务中保存。已有导入回执
但关键状态行丢失时，采集/网络初始化与持久写入会报错，不能重新生成身份、默认配置或
旧 JSON 状态。旧固件留下的“只有导入回执、没有状态行”也需要显式恢复，不能把它自动
当成首次启动；无法区分旧初始化中断和已有权威状态丢失。

启动数据卷时仅修复 UID/GID 不匹配的文件，避免无变化的递归 chown 刷新所有清单 ctime、
导致持久目录索引跨重启失效。ctime 检测保持启用，真实文件变更仍使缓存失效。

素材发布与数据库提交不是同一个原子事务。先持久封存素材和清单，再更新目录；中断后
继续按分段恢复协议处理素材，目录可以重建。运行日志保持轮转文件，方便独立排障。

## 查看和修改

离线安装使用包内解释器；先在设备 shell 定义以下快捷函数（不依赖全局 console script）：

```bash
oa() {
  sudo env PYTHONPATH=/opt/rp-ylx/current/site-packages \
    /opt/rp-ylx/current/runtime/bin/python3 -m rp_ylx "$@"
}
```

```bash
oa settings show --config /etc/rp-ylx/device.json
oa settings set --config /etc/rp-ylx/device.json --input /path/patch.json
oa settings export --config /etc/rp-ylx/device.json --output /path/new-export.json
```

补丁仅包含需修改的字段，例如 `{"storage":{"minimum_available_bytes":2147483648}}`。
设备身份、根路径和安全字段不能通过此接口修改。输出文件必须不存在，且执行用户具有
目标目录权限。焦距沿用现有 API 修改，成功后持久保存并在重新连接服务时恢复。

显式提交 `recording.preset` 会重建整个录制编码配置，旧的码率、GOP、码控等覆盖值
不会继续覆盖新预设。只有同一次补丁中的字段会覆盖该预设，例如：

```json
{"recording":{"preset":"high","gop_frames":60}}
```

不包含 `preset` 的补丁仍只修改指定字段；例如仅设置 `bitrate_kbps` 会保持其他编码参数。
数据库和导出保存经过校验的完整生效值，不保存预设别名，避免后续固件默认值变化悄悄
改变录制规格。无效补丁会回滚全部字段及修改历史；保存后仍需在空闲时重启才能生效。
升级和读取现有设置不会自动重新应用历史预设。

## 备份、恢复和重建

```bash
oa database check --database /var/lib/rp-ylx/state.sqlite3
oa database backup --database /var/lib/rp-ylx/state.sqlite3 --output /secure/new-state.sqlite3
```

对每个数据库分别备份。命令使用 SQLite 在线备份 API，包含已提交 WAL；不要只复制正在
使用的 `.sqlite3` 主文件。跨数据库的整机一致快照应先停止写入服务。备份不等于素材备份，
素材交付仍需独立校验文件 SHA-256。设备端不能仅凭一次下载请求认定客户端已经完成备份。

恢复前正常停止录制，停止 `rp-ylx-recording-button.service`、`rp-ylx.service`、
`rp-ylx-network-control.service` 和 `rp-ylx-recover.service`。恢复入口检查这些服务均已停止。

```bash
oa database restore --database /var/lib/rp-ylx/state.sqlite3 --input /secure/new-state.sqlite3
oa database rebuild-catalog --database /var/lib/rp-ylx/catalog.sqlite3
```

恢复会保留旧数据库和日志为 `.before-restore-*`，不删除素材。只能重建 `catalog.sqlite3`；
采集、配置和网络数据库必须恢复正确备份。目录重建后首次列表重新读取清单，原校验历史
保留在归档数据库中。完成后启动原服务并检查网络、空闲状态、设置和录制列表。

升级到 SQLite 后不得直接运行只读写旧 JSON 的固件。网页、独立升级器和安装器拒绝没有 SQLite 支持的目标；迁移后不提供同版本不同提交的在线替换。
需要回退到旧 JSON 固件时，应停止所有写入者，
恢复迁移前固件及其配套状态备份，再单独核对迁移之后产生的素材和网络配置。

## 空间与内存保护

生产服务每秒检查录制卷剩余字节和 inode。触及配置的预留阈值时正常停止并封存当前录制，
而非等到磁盘写满。预留量仍需覆盖尾段、压缩和封存的临时空间；该机制不能保证介质故障、
突然拔电或其他进程瞬间占满磁盘时不丢失尾段，也不能保证给定容量录满任意时长。

清单校验的 `uniqueItems` 使用与 JSON Schema 等价的集合键，避免分段对象两两比较。
清单缓存同时限制条数和原始 JSON 总字节数（8 MiB）；这个预算不是 Python 总 RSS 上限。

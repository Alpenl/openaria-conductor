# SQLite 持久化

录制素材、每段采样文件和 `manifest.json` 继续作为独立文件保存。SQLite 不承载视频，
也不会把历史 SHA-256 校验记录当作当前素材仍然完好的证明。

## 数据边界

- `state_root/state.sqlite3`：采集控制状态、焦距设置和幂等回执。回执单独存表，保留既有
  1024 条窗口；检查点只写状态和新增回执，避免反复重写历史请求。
- `state_root/settings.sqlite3`：可修改的相机、音频、录制和空间阈值设置，以及修改历史。
  生效值仍通过完整生产配置校验。修改后应在空闲时重启服务。
- `state_root/catalog.sqlite3`：录像摘要、分段文件索引和历史校验记录。清单仍是目录事实来源，
  文件替换或固件校验版本变化会使缓存失效；重启后的首次访问检查文件类型和大小。
- 网络状态目录的 `controller.sqlite3`：网络控制事务、回执和期望状态。NetworkManager
  凭据、救援网络配置、底层恢复日志以及安装器的启动/回滚文件继续由原有机制管理。
- `/etc/rp-ylx/device.json` 保留设备身份、路径、监听、安全配置和首次导入的默认值。
  SQLite 初始化后，修改 JSON 中已经迁入的动态字段不会覆盖数据库。

数据库使用 WAL、`synchronous=FULL` 和短事务。首次导入保留原 JSON 和 `.pre-sqlite`
副本；`.sqlite3.initialized` 标记阻止数据库丢失后静默恢复旧 JSON。未知版本、损坏或缺失
权威状态应修复或恢复备份，不能删除标记后重新初始化。

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

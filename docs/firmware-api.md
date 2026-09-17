# 网页固件更新 API

`0.2.1` 起提供 `/api/v4/firmware` 可选扩展，现有 Device API v4 数据结构保持兼容。
HTTP/匿名 lab 策略沿用现有设备配置；customer 兼容模式仍校验 Bearer、Origin 与 CSRF。
读取需要 `firmwareStatus` 操作权限，升级和回退需要 `firmwareUpdate` 权限。

| 请求 | 结果 |
| --- | --- |
| `GET /api/v4/firmware` | 本机当前/上一版本、缓存的可用版本、最近任务；不访问互联网 |
| `GET /api/v4/firmware/check?force=true` | 从固定 OSS 源刷新；不强制时缓存 20 分钟 |
| `POST /api/v4/firmware/update` | 返回 HTTP 202 任务回执，独立更新服务异步执行 |
| `POST /api/v4/firmware/rollback` | 返回 HTTP 202，本地上一版本回退，无需 OSS |

POST 要求 `Content-Type: application/json`、UUID v4 `Idempotency-Key`、闭集正文
`{"commit":"40位完整Git提交"}`。服务重取并核对渠道身份；确认后渠道已变化返回 409。
重复的请求键与目标返回原回执，同键不同操作/目标返回 409。回执保留 30 天。
已有活动任务、设备非空闲、没有上一版本或版本已变化时不启动新任务。

状态正文 `schema` 为 `openaria.firmware-status.v1`：

- `supported`：是否存在受管应用安装。
- `current` / `previous`：`{version, commit}` 或 null。
- `available`：`{version, commit, release_notes, published_at}` 或 null。
- `has_update`：渠道版本严格高于当前版本；仅接受 `0.2.x`，x >= 1。
- `checked_at`：上次成功检查的 Unix 秒或 null。
- `warning`：检查失败说明或 null。失败可保留旧缓存，但不把检查失败解释成最新。
- `task`：最近回执或 null。

任务包含 `id`、`action`（update/rollback）、`target_commit`、`status`
（accepted/running/succeeded/failed）、`message`、`started_at` 和 `finished_at`。
时间为 Unix 秒，未完成的 `finished_at` 为 null。成功必须同时满足更新器退出成功和安装
身份等于目标；更新器包含服务启动健康检查。失败时读取实际 current，不从目标推断运行版本。

页面每 2.5 秒查询任务；断线只显示等待重连，不自动重发更新命令。手动重试尚未取得回执的
请求沿用同一幂等键。用户刷新浏览器后可从设备状态继续查看任务。

`openaria-update.service` 以独立 systemd cgroup 运行，经权限为 0660 的 Unix socket 接受
Conductor 请求。采集服务仍以受限账号运行。更新器独占 `/run/rp-ylx/maintenance.lock`，
采集持有共享锁直到录制结束；采集服务重启不会删除锁文件。

本地 CLI 与网页共用 `update.py`、安装缓存锁、维护锁和现有部署事务。主应用不可访问或旧固件
缺少 API 时，通过 CLI 恢复；该机制不承诺未验证的物理掉电恢复能力。

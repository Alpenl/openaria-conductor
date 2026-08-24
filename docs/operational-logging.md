# 运行日志

Conductor 的常驻 Python 服务向标准错误输出一行一个 JSON 对象。systemd 将这些对象写入
journald；网络控制服务的标准输出仍专用于 socket 激活协议，不承载日志。

## 运维事件格式

每条记录符合 `ylx.operational-event.v1`：

```json
{"schema":"ylx.operational-event.v1","timestamp":"2026-08-24T01:00:00.000Z","level":"warning","component":"network-control","event":"network_transaction_transition","version":"0.1.0","commit":"0123456789abcdef","context":{"transaction_id":"0198d2a0-41a0-7b7a-a751-0e86a39d4db1","status":"rescued","stage":"rescued","error_code":"dhcp_timeout","recovery_action":"reconnect_rescue_ap"}}
```

顶层字段和 `context` 都是闭合对象。上下文只接受预先登记的标量字段；未知字段、包含换行的
值、嵌套对象和非有限数字会被移除，并以 `redacted_field_count` 计数。日志调用方不得传递
自由格式消息。

运维事件可以包含事务 ID、网络模式、状态、阶段、错误码、恢复动作、组件版本和异常类型。
它不会记录请求体、响应体、SSID、IP 配置、路径、principal、幂等键、credential reference、
passphrase、PSK、token、cookie 或异常消息。

## 查询

查看录制服务实时日志：

```bash
journalctl -u rp-ylx.service -f -o cat
```

查看网络控制事务：

```bash
journalctl -u rp-ylx-network-control.service --since=-30min -o cat \
  | jq -c 'select(.schema == "ylx.operational-event.v1")'
```

只查看失败和回退：

```bash
journalctl -u rp-ylx-network-control.service --since=-30min -o cat \
  | jq -c 'select(.level == "error" or .context.status == "rescued")'
```

systemd 标识符固定为：

- `rp-ylx`
- `rp-ylx-network-control`
- `rp-ylx-wifi-watchdog`
- `rp-ylx-recover`
- `rp-ylx-data-volume`

默认日志级别是 `info`。长期运行时可通过 systemd override 设置
`RP_YLX_LOG_LEVEL=debug|info|warning|error|critical`。非法值会回退到 `info` 并记录
`log_level_defaulted`。

## API 审计

Device API 授权结果写入 `<state_root>/api-audit.ndjson`，每条记录符合
`ylx.api-audit-event.v1`。审计记录包含 UTC 时间、request ID、principal ID、operation ID、
resource ID 和授权结果，不包含 token、cookie 或请求体。

审计文件使用 `O_NOFOLLOW` 打开，只接受普通文件，并在每次写入时强制权限为 `0600`。审计
写入失败会继续保持 fail-closed 行为，不会静默丢弃授权记录。

## 事件范围

网络控制器记录启动与启动协调、健康退化与恢复、事务受理、每次持久状态转换、回退结果、
后台 worker 异常、受控请求拒绝以及关闭。健康监控的同类内部异常每 60 秒最多记录一次，并在
恢复时报告期间抑制的重复数。

生产服务记录启动、就绪、停止请求、关闭成功和启动/关闭失败。mDNS 发布与相机预览使用同一
格式，并只记录稳定错误码和异常类型。

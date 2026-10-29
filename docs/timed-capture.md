# 定时结束采集

网页快门下方的“定时结束”打开设置。默认关闭，预设10分钟；可按分钟、秒设置1秒至24小时，保存后用于之后每次网页、实体按钮、API开始的普通或标定录制。设置只允许在待机时更改，录制期间仍能手动提前结束。

设置保存在设备state.sqlite3的capture_timer文档中，升级与服务重启后保留。旧设备没有该文档时使用关闭/600秒，不迁移媒体或改变现有SQLite schema。设置持久化失败不改变生效值。服务或整机重启不会续录，已开始录像仍走原有中断恢复流程，不恢复旧的单调时钟截止值。

计时从采集源成功启动后开始，设备独立线程每100ms检查一次单调时钟，不依赖浏览器在线，也不受日历时间校准影响。到时调用现有正常停止、保存流程；封存仍需额外耗时，不保证逐帧精确到设定时长。空间不足或录制错误可能更早结束，原有保护保持。

每个计时器绑定具体SessionPlan，停止接口在锁内重核session_id；手动停止、失败、退出清理计时状态。重复开始请求重放原回执，不重新起算。旧计时回调即使与手动停止及下一次开始并发，也不能停止新录像。

## 接口

GET /api/v4/capture/timer：权限getCaptureTimer，返回ylx.capture-timer.v1，其中settings含enabled与duration_seconds；active为null或包含session_id、duration_seconds、remaining_seconds的对象。

POST /api/v4/capture/timer：权限setCaptureTimer，完整替换设置：

```json
{"schema":"ylx.capture-timer-set.v1","enabled":true,"duration_seconds":600}
```

沿用Bearer、Origin和CSRF保护。该设置替换不要求Idempotency-Key；重复相同设置安全。仅接受严格布尔开关和1..86400整数秒，不接受额外字段。录制/保存/关闭中返回409，非法输入400，持久化失败500。成功200表示SQLite事务已提交。若返回结果丢失，可GET确认或待机时重发相同设置。

接口独立于原capture-start.v2、capture-stop.v2和capture-status.v4，旧客户端与媒体格式不变。新网页连接不支持该接口的旧固件时收到404并隐藏入口；其他错误明确提示，不声称保存成功。v4 OpenAPI及前后端固定摘要随本次接口扩展同步更新，v2/v3冻结协议保持。

## 交付验证

需配套Conductor源码和固定的Echo Web构建产物。硬件无关测试覆盖默认/持久化、输入与写盘失败、到期保存、独立后台线程、手动取消、跨会话竞态、重复开始、失败启动、HTTP权限/CSRF及手机界面。发布前仍需在授权测试设备完成定时短录、10分钟实际采集、实体按钮启停和重启后设置验证；本地模拟测试不能代替实机录制验收。

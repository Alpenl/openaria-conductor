# RDK X5 实体录制按钮

三线按钮模块可连接 RDK X5 的 40PIN 排针，通过当前 Device API 控制录制。
短按一次开始，录制时再短按一次停止并保存。按钮服务为可选功能，随安装包提供，
默认关闭；确认接线和按下电平后再启用。

## 接线

先正常关机并断开电源，再连接杜邦线。以下编号全部是 **BOARD 物理引脚编号**。
模块必须支持 3.3V 供电和 3.3V 信号；不要给 GPIO 输入 5V。

| 按钮模块标记 | RDK X5 40PIN 排针 | 功能 |
| --- | --- | --- |
| `+` / `VCC` | **物理 1 脚** | 3.3V |
| `−` / `GND` | **物理 9 脚** | 地（6 脚已由风扇使用） |
| `S` / `OUT` / `SIG` | **物理 37 脚** | GPIO 输入 |

以板上的 PIN1 丝印为起点，两列按 `1/2、3/4、5/6 … 37/38、39/40` 排列。
37 脚在奇数列，紧邻末端的 39 脚。不要按杜邦线颜色猜引脚含义。
9 脚也在奇数列，从 PIN1 起数第 5 针；它与 6 脚同为 GND，可以作为按钮地线。
物理 37 脚在 X5 表中也叫 **GPIO26 / LSIO_GPIO0_22**，芯片 GPIO 编号为 **401**；
程序使用 `Hobot.GPIO.BOARD`，配置里填 `37`。

这张接线表针对带 `S/+/-` 或 `OUT/VCC/GND` 的按钮模块。
`COM/NO/NC` 裸开关或带灯自锁开关需要不同接法，不能套用三线模块供电接线。

物理 37 脚与 SPI2、PWM2 复用。如已启用这些总线，按官方说明使用
`sudo srpi-config` → `Interface Options` → `Peripheral bus config` 关闭占用该脚的功能，
再重启使配置生效。按钮程序不会自动改动总线或重启设备。

官方资料：

- [RDK X3/X5 管脚定义与应用](https://developer.d-robotics.cc/rdk_x_doc/Basic_Application/01_40pin_user_sample/40pin_define)
- [X5 RDK 40Pin 功能对照表](https://rdk-doc.oss-cn-beijing.aliyuncs.com/doc/img/03_Basic_Application/01_40pin_user_sample/image/40pin_user_sample/image-20241217-202319.png)
- [Hobot.GPIO 应用与 BOARD 编号](https://developer.d-robotics.cc/rdk_x_doc/Basic_Application/01_40pin_user_sample/gpio)

## 检查电平并启用

先通过正常固件安装流程安装包含 `rp_ylx.recording_button` 的版本。
安装器提供 `/etc/rp-ylx/recording-button.json` 和 `rp-ylx-recording-button.service`，
升级保留已经配置的引脚和极性。需要系统预置的 `Hobot.GPIO`，无需联网安装 Python 依赖。

接好线并开机后，先运行只读检测。若按钮服务已运行，先停止它，避免两个程序占用同一引脚：

```bash
sudo systemctl stop rp-ylx-recording-button.service
sudo /opt/rp-ylx/current/bin/rp-ylx-recording-button --monitor
```

按下、松开按钮，观察输出的 `level`：

- 松开为 `0`、按下为 `1`：设置 `active_low: false`，这也是同事参考模块的接法。
- 松开为 `1`、按下为 `0`：设置 `active_low: true`。
- 电平不变或不按也跳变：检查接线、供电、模块和引脚复用，不启用录制控制。

检测模式只读取输入并打印变化，不请求开始或停止录制。按 Ctrl+C 退出，会释放 37 脚。

编辑 `/etc/rp-ylx/recording-button.json`，将 `enabled` 改为 `true`，并填写确认过的极性：

```json
{
  "enabled": true,
  "physical_pin": 37,
  "active_low": false,
  "debounce_ms": 80,
  "cooldown_ms": 1000,
  "poll_ms": 20,
  "request_timeout_ms": 5000
}
```

启用开机启动并查看日志：

```bash
sudo systemctl enable --now rp-ylx-recording-button.service
sudo journalctl -u rp-ylx-recording-button.service -f
```

关闭按钮控制：`sudo systemctl disable --now rp-ylx-recording-button.service`。

## 录制行为

- 按键稳定 80ms 后识别，至少间隔 1 秒才接受下一次按下；按住不会反复开关。
- 程序启动时按钮已按住，或请求期间按钮被按住，需要先松开再按，避免意外录制。
- 每次按下先查询当前录制状态：空闲时请求开始；录制时请求停止；保存、阻塞等状态忽略。
- 每次开始前检查 `/api/v4/clock`。已获 NTP 或手机/电脑时间才允许开始。
  离线且尚未校时，应先用日期正确的手机/电脑打开设备控制页面。
  停止录制不受校时状态影响。
- 使用现有 `/api/v4/capture/start` 和 `/api/v4/capture/stop`，因此保留相机、存储、
  录制互斥和网络事务检查，网页也能看到按钮触发的录制状态。
- 请求失败或超时会记入日志，不自动重发切换命令。下一次完整按下会重新查询实际状态。
- 按钮进程以 root 读取官方 GPIO 库所需的 sysfs；采集服务继续以 `rp-ylx` 用户运行。
  Customer 模式沿用现有 Bearer、Origin、CSRF 和设备证书验证，访问不经过环境代理。

仓库 `tmp/` 仅用于保存同事参考压缩包及临时资料，已通过 `.gitignore` 排除。

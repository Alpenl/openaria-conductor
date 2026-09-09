# 录制画质与空闲 IMU 优化

本轮根据代码检查实现改进，不运行设备录制或画质对比实验，不部署设备。
改动涉及 Conductor、Bridge SDK、Bridge Desktop 和 Echo Web 四个本地仓库。

## 录制规格

生产配置文件不指定 `recording` 时，采用高画质 H.264：每眼目标码率
16384 kb/s，QP 下限 18、上限 32、I 帧 QP 20、初始 QP 22，GOP 30，
VBV 3000 ms，保持 I/P、无 B 帧。原来每眼 8192 kb/s，QP 上限 51。
采集尺寸和分眼的逐行复制方式不变：3840×1080 双目输入，每眼 1920×1080。
帧率仍由相机配置决定，未提高相机帧率或取消现有零丢帧质量门槛。

可在生产配置顶层加入以下对象，明确指定规格：

```json
{
  "recording": {
    "preset": "high",
    "codec": "h264",
    "rate_control": "cbr",
    "bitrate_kbps": 16384,
    "min_qp": 18,
    "max_qp": 32,
    "intra_qp": 20,
    "initial_qp": 22,
    "gop_frames": 30,
    "vbv_ms": 3000
  }
}
```

`codec: "hevc"` 选择 H.265；`preset: "standard"` 选择原录制参数基线。
显式字段覆盖 preset 的值。未知字段、非法整数和越界 QP 会被拒绝；
GOP 必须整除每段帧数，防止分段从不可独立解码的帧开始。
标定录制继续使用 H.264，保留所选画质参数。

| 码控 | 码率与 QP 上下限 | 主要配置 |
|---|---|---|
| CBR | 支持 | 目标码率、QP 范围、初始/I 帧 QP、VBV |
| AVBR | 支持 | 同上，自适应可变码率 |
| VBR | SDK 不提供目标码率和 QP 上下限 | I 帧 QP、GOP、帧率 |
| FixQP | 不使用目标码率和 QP 上下限 | I 帧 QP，P 帧使用 initial_qp |

未生效的参数在 manifest 中记为 null；目标码率不代表实测码率或恒定文件大小。
默认高画质仍选 CBR/H.264，H.265 和其他码控作为显式选项开放。
提高码率、限制 QP 会增加存储压力；新默认双眼目标总码率约 32.8 Mbps，
仅视频约 246 MB/分钟，不计容器和音频，也不保证实际码率正好等于该值。

硬件代码分别设置 H.264/HEVC 的 SDK union 成员，检查码控初始化错误。
HEVC 使用 8-bit NV12 对应的 Main profile；编码等级由固件根据规格自动计算。
MP4 封装选择对应编码，识别 HEVC IDR 类型 19/20，并支持 hvc1/hev1 的色彩箱修复。
没有把 CRA 误标成 IDR，没有修改源图像尺寸或增加锐化滤镜。

## 来源契约与客户端

新录制使用 `ylx.device-session.v3`，增加 `video.encoding`，声明 codec、码控、
目标码率、QP 配置、GOP、像素格式和位深。既有 v1/v2 schema 字节保留，旧录制可继续读取。
原生目录读取、设备 API 下载校验、SDK 和桌面端导入/导出同时支持 v3。
原始 manifest 的版本和 SHA-256 保持真实，不把 H.265 伪装为旧 H.264 会话。
API v4 的会话响应扩展为 v2/v3 两种契约；Python/Rust 原生 ABI 升至 6。

需要协调升级 Conductor 与 Bridge 客户端。旧客户端或外部服务若只接受 v1/v2
仍可能拒绝 v3；本轮没有发布客户端、升级外部服务或验证云端接收能力。

## 导出

SDK 默认 `video_quality="high"`，H.264/HEVC 均用 medium / CRF 18。
`standard` 保留原 H.264 veryfast / CRF 20、HEVC medium / CRF 22。
终端设置支持选择；旧收据没有质量字段时按 standard 读取，切换到 high 会重建，
保留先前已验证的导出备份。`retain_sources=True` 可保留逐字节一致的原始素材。

桌面端拼接默认也用 medium / CRF 18，派生配方升级到 6。
旧配方成片仍可读取；需要重新生成时使用新配方。
已有成片与目标编码相同时直接复制文件；只调整音频延迟时视频使用码流复制。
H.264 与 HEVC 之间转换仍需要重新编码。左右眼拼接也仍需要重新编码，
每眼 1920×1080 合成为 3840×1080，不执行缩放。

提高导出质量只减少再次编码的损失，不能恢复镜头、曝光或相机 MJPEG 已损失的细节。

## 空闲 IMU

原生预览启动后，独立的空闲 IMU 线程即开始采集，不需要创建录制会话。
空闲轮询最多 50 Hz，网页可见时每 500 ms 刷新状态；后台页面暂停轮询。
开始录制先停止并 join 空闲线程，再交给录制采集器，避免两个采集器争抢 UVC 控制包。
停止录制后恢复空闲采集；开始录制失败也恢复预览。
设备断开会清空预览数据并有界重试，停止服务会退出并回收线程。

空闲观察的 `live_imu.session_id` 为 null；录制时必须与活动会话相同。
原生缓存超过 2 秒不可用；协调器保留有限的新鲜度窗口，并且重复样本不能刷新缓存年龄。
封存期间不泄漏刚结束会话的旧样本。这里显示的仍是设备原始 IMU 值，未擅自改变单位或标定。

Echo Web 源码修改后由其构建流程生成 dist，再校验各文件 SHA-256 并固定进 Conductor。
没有手工修改设备端生成的 JavaScript。

## 验证范围

验证包括 Python 与 Rust 自动回归、网页桌面/手机用例、旧/新来源契约、
录制参数到原生助手的传递、封存、IMU 新鲜度与停止重启、导出尺寸和同编码复制。
媒体测试使用程序生成的小图像检查功能，不使用设备录制或比较画质。

硬件 C 源码使用官方
[hobot-multimedia-dev](https://github.com/D-Robotics/hobot-multimedia-dev/tree/47f52a54fe6f15a695edde68fa2bec35328972c3)
的 libmm 头文件通过 `-Wall -Wextra -Werror -fsyntax-only`。
额外测试以真实 SDK 结构体覆盖 H.264/HEVC × CBR/VBR/FixQP/AVBR 八种组合，
仅替换 SDK 参数获取函数，不访问相机或 VPU。

这些检查不能代替实机吞吐、温度、长录稳定性、H.265 播放兼容性和主观清晰度验收。
本轮没有给出实测画质提升百分比。


最终回归记录（全量运行及失败项修正后的定向复验）：

- Conductor Python：714 项，其中 1 项按现有条件跳过；最终资源固定断言和安装包复验通过。
- Conductor Rust：80 项通过；Clippy 通过。
- Bridge SDK：262 项及 8 个子用例；来源校验错误文案修正后发布契约 107 项复验通过。
- Bridge Desktop：全 workspace 回归 1142 项通过、9 项按现有设置忽略；新增 HEVC 实际导出/逐字节复制专项 1 项通过。
- Echo Web：桌面/手机共 152 项；重连用例改为等待事件流后，两个相关用例复验通过。
- 官方头文件 C 检查、八种码控配置测试、IDR/MP4 元数据回归通过。
- Python 修改文件 Ruff、两端 Rust Clippy、桌面前端 TypeScript 和网页构建通过。
- 安装包固定的 Echo Web 本地提交：`3a279ddac1f58ffa2516d09c2d955f650f4aee47`。

以上为实现阶段的本地验证记录；后续提交与远程构建以对应 Git 提交和 GitHub Actions 记录为准。
构建期间系统盘满，将 Desktop 的可再生成 target 缓存迁移到
`/data2/openaria-quality-imu-20260909/desktop-target`，原位置保留本机符号链接并加入本机 Git exclude。
回归临时目录使用 `/data2/openaria-quality-imu-20260909/tmp`。

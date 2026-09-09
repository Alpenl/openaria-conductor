# RDK X5 一键安装与更新

在已经启动的 **D-Robotics RDK X5 V1.0 官方 Ubuntu 系统**上运行下面的命令。需要能访问互联网，系统 Python 至少为 3.10。这里的“固件”是 Conductor 应用离线包；首次烧录 RDK 官方操作系统仍使用厂商镜像工具。

```bash
curl --proto '=https' -fSL --retry 3 \
  https://openaria-firmware-cn.oss-cn-beijing.aliyuncs.com/rdk-x5/install.sh \
  -o /tmp/openaria-install.sh && sudo sh /tmp/openaria-install.sh
```

无需克隆 GitHub、手动传包或编辑 `device.json`。安装入口和固件都来自阿里云北京 OSS。脚本完成：

1. 校验板卡、架构、systemd 和权限，下载发布清单及固件。
2. 校验归档大小、SHA-256、闭集文件、内嵌 installer/runtime/wheel 和版本身份。
3. 自动补齐缺少的系统依赖；系统软件源沿用 RDK 镜像已有配置。
4. 首装自动选择固定 `/data` 数据卷大小：最多 80 GiB，至少保留 8 GiB 或可用空间的 20% 给系统；不调整已有数据卷。首次下载、解包后至少还需 12 GiB 可用空间。
5. 调用已有离线部署器，生成设备身份、HTTPS 证书和访问令牌，配置 OpenAria 管理账号、服务与开机启动，等待 Device API 健康检查通过。
6. 安装 `openaria-update` 命令，并报告访问地址和令牌文件位置。

浏览器访问 `https://设备IP:8080/`。首次访问需要信任设备自签名证书，访问令牌可由设备管理员运行 `sudo cat /etc/rp-ylx/customer.token` 查看。OpenAria SSH 管理账号和救援热点使用项目现有默认策略，见 [README](../README.md#设备接入)。安装不会自动加入一个未知 Wi-Fi；初次联网可使用有线网络或 RDK 系统现有网络。

## 日常更新与回退

```bash
openaria-update --check           # 查询可用 commit，无需 sudo
sudo openaria-update             # 更新到发布渠道当前版本
sudo openaria-update --status    # 离线查看当前与上一版本
sudo openaria-update --rollback  # 离线回退到上一版本
sudo openaria-update --reinstall # 修复性重复安装同一版本
```

更新前先停止录制，等待设备回到空闲状态，并在安装期间保持其他客户端空闲。脚本在切换前检查录制状态，无法确认空闲时会停止。更新会保留既有设备配置、凭据和录制文件；版本按完整 Git commit 判断，避免多个开发固件都使用 `0.1.0` 时漏掉更新。普通重复运行同一 commit 不重启服务，同时刷新本地更新器。

默认缓存为 `/var/cache/openaria`。下载错误或校验失败不会执行固件代码；安装失败使用既有部署器的激活失败处理和回退机制。它不扩展当前产品对意外掉电恢复的承诺。两个安装命令不能同时运行。回退到旧版本后，下一次普通更新仍会安装发布渠道当前版本。

如果新机已有单独挂载的 `/data` 或其目录里已有文件，脚本会停止以便核对存储布局。它不会格式化已有磁盘。默认创建的是系统盘上的 ext4 文件数据卷，不是重新分区。

## 只下载和验证

在普通 Linux 电脑上也可验证公开下载链路，无需 RDK 硬件或 sudo：

```bash
sh /tmp/openaria-install.sh --download-only --cache-dir "$HOME/.cache/openaria-test"
```

成功结果包含 `verified: true` 和本地归档路径。此步骤不代表新机安装或录制硬件验收。

## 发布新固件

OSS 参数已经配置在 [`deploy/oss-release.json`](../deploy/oss-release.json)，使用专用桶 `openaria-firmware-cn` 和 `rdk-x5/` 前缀。桶默认私有，发布脚本仅把自己的固件对象设为 `public-read`，匿名用户不能写入，也不能列举桶。

先提交源代码，再用项目 PEP 517 backend 构建 aarch64 wheel，随后用 [`build_rdk_x5_bundle.py`](../scripts/build_rdk_x5_bundle.py) 生成包含 CPython 3.11 和全部依赖的离线 bundle。不能只上传 wheel，`--version` 必须与 wheel 的版本相同。

本地发布器自带 PEP 723 依赖声明，通过 `uv run` 使用隔离环境，不需要安装 ossutil：

```bash
uv run scripts/publish_rdk_x5_oss.py \
  --bundle-dir dist-rdk-x5 \
  --output dist-oss-release \
  --publish
```

默认读取 `OSS_ACCESS_KEY_ID`、`OSS_ACCESS_KEY_SECRET` 和可选 `OSS_SESSION_TOKEN`。已有 Alibaba Cloud CLI 的发布机可追加 `--aliyun-profile <profile名称>`，直接读取 `~/.aliyun/config.json`，不复制密钥。不带 `--publish` 时只生成可审查的本地发布目录；输出目录必须为空。

GitHub Actions 的“RDK X5 离线安装包”工作流已接入发布步骤。手动运行时勾选 `publish_oss` 即可在构建和 bundle 校验成功后上传并切换渠道。仓库使用同名 `OSS_ACCESS_KEY_ID` / `OSS_ACCESS_KEY_SECRET` Secrets，发布账号仅有该桶 `rdk-x5/*` 的对象上传和对象 ACL 设置权限。普通 push/PR 构建不修改公开渠道。CLI 触发方式：

```bash
gh workflow run rdk-x5-bundle.yml --ref <待发布分支> -f publish_oss=true
```

发布器会验证 wheel 构建身份和所有 bundle 文件，再生成确定性 tar.gz。文件按 `releases/<完整commit>/<sha256>.tar.gz` 存放，禁止覆盖不可变对象；更新器也以摘要命名。每个对象上传后都进行匿名完整下载和 SHA-256 核验，全部成功后才最后更新 `latest.json`。渠道清单与 `install.sh` 使用 `Cache-Control: no-cache`，固件使用长期缓存。

`latest.json` 使用 `openaria.rdk-x5-release.v1`，包含 `platform`、`commit`、`version`、`bundle` 和 `updater`；两个下载对象都声明 `url`、`bytes`、`sha256`。信任来源为 HTTPS 发布源和有权写入的发布账号，摘要用于校验传输与内容完整性。客户端不接受 HTTP 降级重定向。

OSS 同时保留不可变发布清单 `releases/<commit>/<清单sha256>.json`，可把它的 HTTPS URL 作为安装器位置参数固定版本。切换公共渠道时，先核验目标清单指向的对象可匿名下载，再用发布凭据将该清单恢复到 `latest.json`；不删除已发布固件。

OSS 防覆盖机制参见 [阿里云官方说明](https://www.alibabacloud.com/help/en/oss/developer-reference/prevent-objects-from-being-overwritten-by-objects-that-have-the-same-names-2)。公开下载使用 OSS 自带 HTTPS 域名即可，无需先配置 CDN 或备案域名。

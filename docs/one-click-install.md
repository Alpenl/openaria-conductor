# RDK X5 一键安装与更新

设备端可直接从 HTTPS 发布清单完成下载、完整性校验、解包和安装：

```bash
sudo python3 scripts/openaria_update.py \
  https://download.example.cn/openaria/latest.json
```

清单固定使用 `openaria.rdk-x5-release.v1`，其中 bundle 必须声明字节数和 SHA-256。下载失败、大小不符、摘要不符或 tar 路径越界都会在执行安装前停止。重复执行会复用本地缓存；安装仍由 `rdk_x5_install.py` 负责，因此保留现有配置、事务恢复和升级回退语义。

发布者先用 `build_rdk_x5_bundle.py` 生成 bundle，再使用已配置阿里云 OSS 凭据的 `ossutil` 发布：

```bash
python3 scripts/publish_rdk_x5_oss.py \
  --bundle dist/openaria-rdk-x5.tar.gz \
  --version 0.5.1 \
  --oss-prefix oss://openaria-release-cn/rdk-x5 \
  --public-base-url https://download.example.cn/rdk-x5
```

脚本上传版本化 bundle 和 `latest.json`，不会把 AccessKey 写入仓库；请通过 `ossutil config` 或实例角色提供凭据。建议 OSS 桶绑定国内 CDN/自定义域名后，将该 HTTPS 地址作为 `--public-base-url`，供设备和用户下载。

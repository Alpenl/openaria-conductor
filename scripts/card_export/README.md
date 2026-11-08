# OpenAria 读卡器录像导出工具（验收候选）

无需改卡的分区、无需安装ext4驱动、不需要设备联网。只读解析系统分区及其中的data-volume.img，
把选中的录像目录复制到电脑，逐文件回读SHA-256校验并输出export-report.json。不复制整个80GB镜像。

## 使用

1. 停止录制，正常关机，再拔卡放入读卡器。Windows提示格式化Linux分区时必须取消。
2. 电脑安装Python 3.10或更新版，将工具ZIP完整解压到电脑。交付包附带离线Python依赖。
3. Windows右键start.cmd“以管理员身份运行”；Mac双击start.command，按终端提示输入本机管理员密码。
   Linux运行 `sh start.command`。若Mac阻止下载的脚本执行，可在终端用 `sh start.command` 启动。
4. 按容量/型号选择内存卡，选择录像编号和电脑上的已有导出目录。不要选内存卡作为目标目录。
5. 等待“导出完成”。新目录包含原始视频分段MP4、音频WAV、manifest及其他采集文件，并保留目录层次。

电脑不具备Python时需要先安装；这不是免安装的.exe/.app。首次运行会在工具目录创建.venv，依赖从随包wheels离线安装。
从源码运行可用 `python -m pip install -r requirements.txt`，再以管理员权限运行export.py。
原始文件不转码、不合并音视频；播放器支持的编码决定能否直接播放，导出成功不代表已做媒体解码验收。

## 命令行

```text
python export.py --source <整盘路径或.img文件> --list
python export.py --source <整盘路径或.img文件> --destination <电脑目录>
python export.py --source <整盘路径> --recording <列表中的ID> --destination <电脑目录>
```

默认导出全部录像目录，包括标记为“未封存/无清单”的目录，便于保留异常停止的数据。“有清单”不等于录像完整性已验证。
仅搜索recordings、sessions及旧data目录，不导出Wi-Fi配置、系统文件或设备凭据。不同录像卷/目录分开保存。
命令退出0表示选中内容已复制校验，失败退出2；失败目录保留，报告状态为incomplete，不覆盖之前的导出。
目前失败后重新运行会建立新的导出目录，不提供断点续传。工具不会修复文件系统或重放ext4日志。

未正常卸载的文件系统默认阻止导出。确需抢救时可显式添加--allow-dirty，报告标记source_dirty；可能缺失最新内容，
不能据复制校验宣称原录像完整。文件系统损坏或读取失败应保留原卡；本工具不会自动写回修复。
只读约束指本工具；操作系统/其他软件仍可能访问卡，所以不要同时挂载写入或运行录制设备。

## 验证范围与设计

主用途为现场人员的导出流程（Workflow、Human-Primary、Interactive CLI、Read-heavy）；命令行参数为次要批处理入口。
无后台会话。唯一持久产物是电脑上的导出目录和校验报告。源卡只以rb方式打开，未调用格式化、挂载或修复命令。

Linux下真实MBR＋外层ext4＋稀疏内层ext4的读取、完整字节导出及源镜像哈希保持已测试。
Windows/macOS实机读卡器访问和真实录像卡尚待验收。正常导出需要读卡器稳定、目标空间足够。

读取库：dissect.extfs 3.15、dissect.volume 3.18（https://docs.dissect.tools/）。

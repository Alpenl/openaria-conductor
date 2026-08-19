"""多个 CLI/守护进程入口共享的窄帮助函数。"""

from __future__ import annotations

import os
from pathlib import Path

from rp_ylx.camera import CameraController, CameraError


def stable_id_for_device(controller: CameraController, device: Path) -> str:
    target = os.path.normpath(str(device))
    resolved_target = device.resolve()
    for descriptor in controller.discover():
        descriptor_path = Path(descriptor.node)
        if (
            os.path.normpath(descriptor.node) == target
            or descriptor_path.resolve() == resolved_target
            or descriptor.stable_id == str(device)
        ):
            return descriptor.stable_id
    raise CameraError("device_not_found", f"未发现指定相机节点：{device}", retryable=True)

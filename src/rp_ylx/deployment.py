"""RDK X5 单机安装、升级恢复与两版本回滚。"""

from __future__ import annotations

import argparse  # noqa: F401 - referenced by standalone deployment bootstrap checks
import fcntl  # noqa: F401 - kept for unittest.mock.patch compatibility on rp_ylx.deployment
import os  # noqa: F401 - kept for unittest.mock.patch compatibility on rp_ylx.deployment
import ssl  # noqa: F401 - kept for unittest.mock.patch compatibility on rp_ylx.deployment
import subprocess  # noqa: F401 - kept for unittest.mock.patch compatibility on rp_ylx.deployment
import sys
import time  # noqa: F401 - kept for unittest.mock.patch compatibility on rp_ylx.deployment
import urllib.request  # noqa: F401 - kept for unittest.mock.patch compatibility on rp_ylx.deployment
from pathlib import Path

if __package__:
    from . import deploy_pkg as _deploy_pkg
else:
    _PACKAGE_PARENT = str(Path(__file__).resolve().parent)
    if _PACKAGE_PARENT not in sys.path:
        sys.path.insert(0, _PACKAGE_PARENT)
    import deploy_pkg as _deploy_pkg  # type: ignore[import-not-found, no-redef]

__all__ = list(_deploy_pkg.__all__)
for _name in __all__:
    globals()[_name] = getattr(_deploy_pkg, _name)
del _name

if __name__ == "__main__":
    raise SystemExit(_deploy_pkg.main())

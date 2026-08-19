"""RP-YLX 设备端录制程序。"""

from importlib.metadata import PackageNotFoundError, version

from rp_ylx._build_info import __commit__

try:
    __version__ = version("rp-ylx")
except PackageNotFoundError:
    __version__ = "0.1.0"

__all__ = ["__commit__", "__version__"]

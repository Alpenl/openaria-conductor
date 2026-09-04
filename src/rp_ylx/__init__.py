"""Open Aria Conductor device recording service."""

from importlib.metadata import PackageNotFoundError, version

from rp_ylx._build_info import __commit__

PRODUCT_NAME = "Open Aria"

try:
    __version__ = version("rp-ylx")
except PackageNotFoundError:
    __version__ = "0.1.0"

__all__ = ["PRODUCT_NAME", "__commit__", "__version__"]

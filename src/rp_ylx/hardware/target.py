"""Canonical identifiers and comparisons for the supported hardware pair."""

from __future__ import annotations

from collections.abc import Mapping

from rp_ylx.core.board import RDK_X5_MODEL, is_rdk_x5_model

RDK_X5_BOARD_ID = "rdk_x5_v1.0"
YLX_2UQ2_USB_ID = ("1bcf", "0b15")
YLX_2UQ2_CAMERA_ID = "ylx_2uq2"

__all__ = [
    "RDK_X5_BOARD_ID",
    "RDK_X5_MODEL",
    "YLX_2UQ2_CAMERA_ID",
    "YLX_2UQ2_USB_ID",
    "is_rdk_x5_model",
    "is_supported_target",
    "is_ylx_2uq2_usb",
]


def is_ylx_2uq2_usb(device: Mapping[str, object]) -> bool:
    vendor = device.get("vendor_id")
    product = device.get("product_id")
    return (
        isinstance(vendor, str)
        and isinstance(product, str)
        and (vendor.casefold(), product.casefold()) == YLX_2UQ2_USB_ID
    )


def is_supported_target(target: object) -> bool:
    return target == {
        "board": RDK_X5_BOARD_ID,
        "camera": YLX_2UQ2_CAMERA_ID,
        "supported": True,
        "reason": "matched",
    }

"""Canonical identifiers and comparisons for the supported hardware pair."""

from __future__ import annotations

from collections.abc import Mapping

RDK_X5_MODEL = "D-Robotics RDK X5 V1.0"
RDK_X5_BOARD_ID = "rdk_x5_v1.0"
YLX_2UQ2_USB_ID = ("1bcf", "0b15")
YLX_2UQ2_CAMERA_ID = "ylx_2uq2"


def is_rdk_x5_model(model: str | None) -> bool:
    if model is None:
        return False
    return " ".join(model.split()).casefold() == RDK_X5_MODEL.casefold()


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

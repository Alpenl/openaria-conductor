"""Shared camera focus status rules for native and API boundaries."""

from collections.abc import Mapping

CAMERA_FOCUS_KEYS = frozenset(
    {"schema", "value", "minimum", "maximum", "step", "default", "auto_supported", "auto_enabled"}
)


def valid_camera_focus_status(value: object, *, allow_integer_subclasses: bool = False) -> bool:
    if not isinstance(value, Mapping) or set(value) != CAMERA_FOCUS_KEYS:
        return False
    return not (
        value["schema"] != "ylx.camera-focus.v1"
        or any(
            not isinstance(value[key], int)
            or isinstance(value[key], bool)
            or (not allow_integer_subclasses and type(value[key]) is not int)
            for key in ("value", "minimum", "maximum", "step", "default")
        )
        or value["minimum"] > value["maximum"]
        or value["step"] <= 0
        or not value["minimum"] <= value["value"] <= value["maximum"]
        or (value["value"] - value["minimum"]) % value["step"] != 0
        or not value["minimum"] <= value["default"] <= value["maximum"]
        or type(value["auto_supported"]) is not bool
        or (value["auto_enabled"] is not None and type(value["auto_enabled"]) is not bool)
        or (not value["auto_supported"] and value["auto_enabled"] is not None)
    )

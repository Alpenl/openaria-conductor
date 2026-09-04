"""双目相机生命周期和采集接口。"""

from rp_ylx.camera.controller import CameraController
from rp_ylx.camera.models import (
    CameraDescriptor,
    CameraError,
    CameraMode,
    FrameObservation,
    StereoFrame,
)
from rp_ylx.camera.synthetic import SyntheticCameraBackend
from rp_ylx.camera.v4l2 import (
    NativeV4L2CameraStream,
    V4L2CameraStream,
    V4L2DiscoveryBackend,
    parse_v4l2_formats,
    split_sbs_mjpeg,
    v4l2_production_stream_factory,
    v4l2_stream_factory,
)

__all__ = [
    "CameraController",
    "CameraDescriptor",
    "CameraError",
    "CameraMode",
    "FrameObservation",
    "StereoFrame",
    "SyntheticCameraBackend",
    "NativeV4L2CameraStream",
    "V4L2CameraStream",
    "V4L2DiscoveryBackend",
    "parse_v4l2_formats",
    "split_sbs_mjpeg",
    "v4l2_production_stream_factory",
    "v4l2_stream_factory",
]

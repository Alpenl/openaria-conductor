"""目标硬件探测。"""

from rp_ylx.hardware.probe import collect_hardware_facts
from rp_ylx.hardware.smoke import HardwareSmokeError, record_hardware_smoke

__all__ = ["HardwareSmokeError", "collect_hardware_facts", "record_hardware_smoke"]

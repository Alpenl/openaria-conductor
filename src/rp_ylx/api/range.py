"""RFC 9110 single byte-range parsing for Device API downloads."""

from __future__ import annotations


class UnsatisfiableRange(ValueError):
    """Range 格式无效、多段或无法满足。"""

    def __init__(self, complete_size: int) -> None:
        self.complete_size = complete_size
        super().__init__(f"无法满足对 {complete_size} 字节表示的 Range 请求")


def _bounded_range_decimal(value: str, upper_bound: int, complete_size: int) -> int:
    if not value.isascii() or not value.isdigit():
        raise UnsatisfiableRange(complete_size)
    normalized = value.lstrip("0") or "0"
    bound = str(upper_bound)
    if len(normalized) > len(bound) or (len(normalized) == len(bound) and normalized > bound):
        return upper_bound + 1
    return int(normalized)


def parse_single_range(value: str | None, complete_size: int) -> tuple[int, int] | None:
    """解析一个 RFC 9110 bytes range，返回首尾均包含的区间。"""
    if complete_size < 0:
        raise ValueError("complete_size 不能为负数")
    if value is None:
        return None
    if not value.startswith("bytes=") or "," in value:
        raise UnsatisfiableRange(complete_size)
    selected = value.removeprefix("bytes=")
    if selected.startswith("-"):
        suffix = selected[1:]
        suffix_size = _bounded_range_decimal(suffix, complete_size, complete_size)
        if suffix_size == 0 or complete_size == 0:
            raise UnsatisfiableRange(complete_size)
        return max(0, complete_size - suffix_size), complete_size - 1

    first_text, separator, last_text = selected.partition("-")
    if (
        not separator
        or not first_text.isascii()
        or not first_text.isdigit()
        or (last_text and (not last_text.isascii() or not last_text.isdigit()))
    ):
        raise UnsatisfiableRange(complete_size)
    first = _bounded_range_decimal(first_text, complete_size, complete_size)
    if first >= complete_size:
        raise UnsatisfiableRange(complete_size)
    if not last_text:
        return first, complete_size - 1
    last = _bounded_range_decimal(last_text, complete_size, complete_size)
    if last < first:
        raise UnsatisfiableRange(complete_size)
    return first, min(last, complete_size - 1)

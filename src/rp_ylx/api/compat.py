"""Helpers for preserving module-level monkeypatch targets across split submodules."""

from __future__ import annotations

import sys
from typing import TypeVar

T = TypeVar("T")


def facade_attr(module_name: str, attr: str, default: T) -> T:
    """Return an overridden attribute on a loaded facade module if patched, else default."""
    module = sys.modules.get(module_name)
    if module is None:
        return default
    return getattr(module, attr, default)

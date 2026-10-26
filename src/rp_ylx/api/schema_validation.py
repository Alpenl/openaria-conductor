"""Optional structural acceleration without changing Python validation semantics."""

from __future__ import annotations

import json
import math

from rp_ylx.native import native_schema_validator_or_none


def _plain_json(value: object) -> bool:
    """Do not turn tuples, non-string keys, custom types or large integers into JSON.

    Restrict numbers to the exact integer range shared by both validators. The
    ordinary Python validator remains authoritative for all other inputs.
    """
    kind = type(value)
    if value is None or kind in (str, bool):
        return True
    if kind is int:
        return abs(value) <= 2**53 - 1
    if kind is float:
        return math.isfinite(value) and abs(value) <= 2**53 - 1
    if kind is list:
        return all(_plain_json(item) for item in value)
    if kind is dict:
        return all(type(key) is str and _plain_json(item) for key, item in value.items())
    return False


def validate_manifest_schema(validator, manifest: object) -> None:
    """Use native success only; rejection/errors retain original errors and behavior.

    The caller must still run the independent session semantic validator. Native
    regex/format handling can be stricter, so a native rejection is not final.
    """
    try:
        if _plain_json(manifest):
            compiled = native_schema_validator_or_none(
                json.dumps(validator.schema, separators=(",", ":"), allow_nan=False)
            )
            if (
                compiled is not None
                and compiled.is_valid(json.dumps(manifest, separators=(",", ":"), allow_nan=False))
                is True
            ):
                return
    except Exception:
        # Unavailable acceleration must never turn a rejected manifest into success.
        pass
    validator.validate(manifest)

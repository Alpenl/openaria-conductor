"""Closed frame-index versions. Raw driver facts are never calibration claims."""


def valid_frame_record(record: dict) -> bool:
    keys = {
        "schema",
        "session_id",
        "frame",
        "source_sequence",
        "host_monotonic_ns",
        "segment_index",
        "segment_frame",
    }
    if record.get("schema") == "ylx.frame-index.v1":
        return set(record) == keys
    if record.get("schema") != "ylx.frame-index.v2" or set(record) != keys | {"timestamp_audit"}:
        return False
    audit = record["timestamp_audit"]
    if not isinstance(audit, dict) or set(audit) != {
        "schema",
        "v4l2_buffer_flags",
        "host_dequeue_monotonic_ns",
        "timestamp_clock",
        "timestamp_source",
        "camera_counter_raw",
        "counter_bits",
    }:
        return False
    counter = audit["camera_counter_raw"]
    return (
        audit["schema"] == "openaria.frame-timestamp-audit.v1"
        and type(audit["v4l2_buffer_flags"]) is int
        and 0 <= audit["v4l2_buffer_flags"] < 2**32
        and type(audit["host_dequeue_monotonic_ns"]) is int
        and audit["host_dequeue_monotonic_ns"] > 0
        and audit["timestamp_clock"] in {"v4l2_monotonic", "host_monotonic_fallback"}
        and audit["timestamp_source"] in {"start_of_exposure", "end_of_frame", "unknown"}
        and audit["counter_bits"] == 24
        and (counter is None or (type(counter) is int and 0 <= counter < 2**24))
    )

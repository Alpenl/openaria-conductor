import collections
import hashlib
import json
import struct
from pathlib import Path

import numpy as np

root = Path(__file__).parent
results = {}
for path in sorted(root.glob("*.json")):
    p = json.loads(path.read_text())
    if not isinstance(p, dict) or "rows" not in p:
        continue
    rows = p["rows"]
    payload = [bytes.fromhex(r[2]) for r in rows]
    seq = np.array([int.from_bytes(b[:3], "big") for b in payload])
    a = np.array([struct.unpack(">12h", b[3:]) for b in payload])
    delta = np.diff(seq) % (1 << 24)
    durations = np.array([(r[1] - r[0]) / 1e6 for r in rows])
    same = np.flatnonzero(delta == 0) + 1
    span = (rows[-1][1] - rows[0][0]) / 1e9
    result = {
        "reads": len(rows),
        "span_seconds": span,
        "reads_per_second": len(rows) / span,
        "group_slots_per_second": 2 * len(rows) / span,
        "counter_changes": int(np.count_nonzero(delta)),
        "counter_changes_per_second": float(np.count_nonzero(delta) / span),
        "counter_delta_histogram": dict(collections.Counter(map(int, delta))),
        "same_counter_reads": len(same),
        "same_counter_payload_changes": sum(payload[i][3:] != payload[i - 1][3:] for i in same),
        "read_duration_ms": dict(
            zip(
                ["min", "p50", "p95", "max"],
                map(float, np.percentile(durations, [0, 50, 95, 100])),
                strict=True,
            )
        ),
        "full_payload_changes": sum(b != c for b, c in zip(payload, payload[1:], strict=False)),
        "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
    }
    for name, sl in [("accel", slice(0, 3)), ("gyro", slice(3, 6))]:
        first = a[:, :6][:, sl]
        second = a[:, 6:][:, sl]
        both = np.stack([first, second], axis=1).reshape(-1, 3)
        result[name] = {
            "within_pair_equal_fraction": float(np.mean(np.all(first == second, axis=1))),
            "previous_second_equals_next_first_fraction": float(
                np.mean(np.all(second[:-1] == first[1:], axis=1))
            ),
            "ordered_value_transitions_per_second_NOT_sample_rate": float(
                np.count_nonzero(np.any(both[1:] != both[:-1], axis=1)) / span
            ),
            "first_slot_changes_per_second": float(
                np.count_nonzero(np.any(first[1:] != first[:-1], axis=1)) / span
            ),
            "second_slot_changes_per_second": float(
                np.count_nonzero(np.any(second[1:] != second[:-1], axis=1)) / span
            ),
        }
    long_indices = [i for i in range(1, len(rows)) if (rows[i][0] - rows[i - 1][1]) > 80_000_000]
    result["pause_recovery"] = [
        {
            "idle_ms": (rows[i][0] - rows[i - 1][1]) / 1e6,
            "counter_step": int(delta[i - 1]),
            "next_steps": list(map(int, delta[i : i + 6])),
        }
        for i in long_indices[:8]
    ]
    results[path.stem] = result
(root / "analysis.json").write_text(json.dumps(results, indent=2))
for name, r in results.items():
    print(name, json.dumps(r))

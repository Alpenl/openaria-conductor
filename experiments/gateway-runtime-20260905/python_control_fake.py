from __future__ import annotations

import json
import sys
import time


def main() -> None:
    for line in sys.stdin:
        command = json.loads(line)
        delay_ms = int(command["delay_ms"])
        if delay_ms < 0 or delay_ms > 5_000:
            raise ValueError("delay_ms is outside the experiment bound")
        time.sleep(delay_ms / 1_000)
        print(
            json.dumps(
                {"delay_ms": delay_ms, "status": "ok"},
                separators=(",", ":"),
                sort_keys=True,
            ),
            flush=True,
        )


if __name__ == "__main__":
    main()

"""Shared bounded metadata limits for recording readers and consumers.

Five hours of five-second segments with complete 100 ms recovery audio clock
anchors can exceed 16 MiB in pretty JSON. Keep all evidence and allow 32 MiB;
this is a metadata bound, not a claim about media capacity or capture duration.
"""

MAX_MANIFEST_BYTES = 32 * 1024 * 1024

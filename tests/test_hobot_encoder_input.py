from __future__ import annotations

import json
import os
import shutil
import struct
import subprocess
import tempfile
import unittest
from pathlib import Path


class HobotEncoderInputTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        compiler = shutil.which("cc")
        if compiler is None:
            raise unittest.SkipTest("C compiler unavailable")
        cls.temporary = tempfile.TemporaryDirectory()
        cls.addClassCleanup(cls.temporary.cleanup)
        cls.root = Path(cls.temporary.name)
        cls.binary = cls.root / "helper"
        source = Path(__file__).resolve().parents[1] / "src/rp_ylx/hobot/ylx_stereo_encoder.c"
        harness = cls.root / "main.c"
        harness.write_text(
            r"""
#include <unistd.h>
#include <errno.h>
#include <stdlib.h>
#include <string.h>
static ssize_t input_read(int fd, void *out, size_t count) {
    static size_t delivered = 0;
    static int interrupted = 0;
    const char *fault = getenv("ENCODER_TEST_READ_FAULT");
    if (fault && strcmp(fault, "interrupt") == 0) {
        if (!interrupted) { interrupted = 1; errno = EINTR; return -1; }
        interrupted = 0;
        if (count > 2) count = 2;
    } else if (fault) {
        size_t limit = (size_t)strtoul(fault, NULL, 10);
        if (delivered >= limit) { errno = EIO; return -1; }
        if (count > limit - delivered) count = limit - delivered;
    }
    ssize_t result = read(fd, out, count);
    if (result > 0) delivered += (size_t)result;
    return result;
}
#define read input_read
"""
            + f'#include "{source}"\n'
            + r"""
#undef read
struct ylx_pipeline { unsigned long long count; };
int ylx_pipeline_open(const ylx_pipeline_config_t *c, ylx_segment_closed_fn cb,
                     void *u, ylx_pipeline_t **out, char *err, size_t n) {
    (void)c; (void)cb; (void)u; (void)err; (void)n;
    static ylx_pipeline_t p; *out = &p; return 0;
}
int ylx_pipeline_submit(ylx_pipeline_t *p, const unsigned char *d, size_t n, int t) {
    (void)d; (void)n; (void)t; p->count++; return 0;
}
int ylx_pipeline_finish(ylx_pipeline_t *p) { (void)p; return 0; }
void ylx_pipeline_close(ylx_pipeline_t *p) { (void)p; }
void ylx_pipeline_stats(const ylx_pipeline_t *p, ylx_pipeline_stats_t *s) {
    memset(s, 0, sizeof(*s)); s->submitted = p->count;
}
const char *ylx_pipeline_error(const ylx_pipeline_t *p) { (void)p; return ""; }
"""
        )
        subprocess.run(
            [
                compiler,
                "-std=gnu11",
                "-Wall",
                "-Wextra",
                "-Werror",
                str(harness),
                "-o",
                str(cls.binary),
            ],
            check=True,
            capture_output=True,
            text=True,
        )

    def run_input(self, payload, fault=None, *, directory_stdin=False):
        environment = os.environ.copy()
        environment.pop("ENCODER_TEST_READ_FAULT", None)
        if fault is not None:
            environment["ENCODER_TEST_READ_FAULT"] = str(fault)
        command = [str(self.binary), "--out-dir", str(self.root)]
        if directory_stdin:
            fd = os.open(self.root, os.O_RDONLY | os.O_DIRECTORY)
            try:
                result = subprocess.run(
                    command, stdin=fd, capture_output=True, timeout=3, env=environment
                )
            finally:
                os.close(fd)
        else:
            result = subprocess.run(
                command, input=payload, capture_output=True, timeout=3, env=environment
            )
        events = [json.loads(line) for line in result.stdout.splitlines()]
        self.assertEqual(events[0]["event"], "ready")
        self.assertEqual(events[-1]["event"], "done")
        return result, events

    def assert_failure(self, payload, code, fault=None, **kwargs):
        result, events = self.run_input(payload, fault, **kwargs)
        self.assertEqual(result.returncode, 1, result.stdout)
        self.assertEqual([event["event"] for event in events], ["ready", "error", "done"])
        self.assertEqual(events[1]["code"], code)
        return events

    def test_partial_header_is_failure_at_every_boundary(self):
        header = struct.pack("<4sI", b"YLXF", 4)
        for offset in range(1, len(header)):
            for prefix in (b"", header + b"jpeg"):
                with self.subTest(offset=offset, prior_frame=bool(prefix)):
                    events = self.assert_failure(prefix + header[:offset], "frame_header_truncated")
                    self.assertEqual(events[-1]["submitted"], int(bool(prefix)))

    def test_partial_payload_remains_failure(self):
        header = struct.pack("<4sI", b"YLXF", 4)
        for offset in range(4):
            with self.subTest(offset=offset):
                events = self.assert_failure(header + b"jpeg"[:offset], "frame_truncated")
                self.assertEqual(events[-1]["submitted"], 0)

    def test_clean_boundary_eof_and_explicit_end_remain_successful(self):
        frame = struct.pack("<4sI", b"YLXF", 4) + b"jpeg"
        end = struct.pack("<4sI", b"YLXF", 0)
        for prefix in (b"", frame):
            for suffix in (b"", end):
                with self.subTest(prior_frame=bool(prefix), explicit_end=bool(suffix)):
                    result, events = self.run_input(prefix + suffix)
                    self.assertEqual(result.returncode, 0)
                    self.assertEqual([e["event"] for e in events], ["ready", "done"])
                    self.assertEqual(events[-1]["submitted"], int(bool(prefix)))

    def test_read_error_before_or_within_header_and_payload_is_failure(self):
        frame = struct.pack("<4sI", b"YLXF", 4) + b"jpeg"
        for limit in (0, 3, 8, 10):
            with self.subTest(limit=limit):
                events = self.assert_failure(frame, "frame_read_failed", limit)
                self.assertIn(os.strerror(5), events[1]["message"])
                self.assertEqual(events[-1]["submitted"], 0)

    def test_real_directory_read_error_is_failure(self):
        self.assert_failure(b"", "frame_read_failed", directory_stdin=True)

    def test_interrupted_fragmented_reads_are_retried(self):
        frame = struct.pack("<4sI", b"YLXF", 4) + b"jpeg"
        result, events = self.run_input(frame * 3 + struct.pack("<4sI", b"YLXF", 0), "interrupt")
        self.assertEqual(result.returncode, 0)
        self.assertEqual([e["event"] for e in events], ["ready", "done"])
        self.assertEqual(events[-1]["submitted"], 3)


if __name__ == "__main__":
    unittest.main()

from __future__ import annotations

import json
import select
import shutil
import struct
import subprocess
import tempfile
import textwrap
import unittest
from pathlib import Path


class HobotEncoderJsonTest(unittest.TestCase):
    _submit_binary: Path
    _harness_binary: Path

    @classmethod
    def setUpClass(cls) -> None:
        compiler = shutil.which("gcc") or shutil.which("cc")
        if compiler is None:
            raise unittest.SkipTest("C compiler unavailable")
        cls._temporary = tempfile.TemporaryDirectory()
        cls.addClassCleanup(cls._temporary.cleanup)
        root = Path(cls._temporary.name)
        repo = Path(__file__).resolve().parents[1]
        source = repo / "src/rp_ylx/hobot/ylx_stereo_encoder.c"
        include_dir = repo / "src/rp_ylx/hobot"

        stubs = r"""
struct ylx_pipeline { int unused; };
int ylx_pipeline_open(const ylx_pipeline_config_t *config,
                      ylx_segment_closed_fn callback, void *user,
                      ylx_pipeline_t **out, char *error, size_t size) {
    (void)config; (void)callback; (void)user; (void)error; (void)size;
    static ylx_pipeline_t p; *out = &p; return 0;
}
int ylx_pipeline_submit(ylx_pipeline_t *p, const unsigned char *data, size_t size, int timeout) {
    (void)p; (void)data; (void)size; (void)timeout; return -1;
}
int ylx_pipeline_finish(ylx_pipeline_t *p) {
    (void)p; char gate;
    return read(STDIN_FILENO, &gate, 1) == 1 ? -1 : -2;
}
void ylx_pipeline_close(ylx_pipeline_t *p) { (void)p; }
void ylx_pipeline_stats(const ylx_pipeline_t *p, ylx_pipeline_stats_t *stats) {
    (void)p; memset(stats, 0, sizeof(*stats)); stats->drop_decoder_input = 1;
}
const char *ylx_pipeline_error(const ylx_pipeline_t *p) {
    (void)p; return "decoder_input_dequeue_failed: ret=-42";
}
"""
        submit_src = root / "submit_main.c"
        cls._submit_binary = root / "submit_main"
        submit_src.write_text(f'#include "{source}"\n' + stubs, encoding="utf-8")
        subprocess.run(
            [
                compiler,
                "-Wall",
                "-Wextra",
                "-Werror",
                str(submit_src),
                "-o",
                str(cls._submit_binary),
            ],
            check=True,
        )

        harness_src = root / "harness.c"
        cls._harness_binary = root / "harness"
        harness_src.write_text(
            textwrap.dedent(
                f'''
                #include <stddef.h>
                #include <string.h>
                #define main ylx_encoder_main
                #include "{source}"
                #undef main

                struct ylx_pipeline {{ int unused; }};

                int ylx_pipeline_open(const ylx_pipeline_config_t *config,
                                      ylx_segment_closed_fn on_segment, void *user,
                                      ylx_pipeline_t **out, char *error, size_t error_len)
                {{
                    (void)config;
                    (void)on_segment;
                    (void)user;
                    (void)out;
                    (void)error;
                    (void)error_len;
                    return -1;
                }}

                int ylx_pipeline_submit(ylx_pipeline_t *pipeline,
                                        const unsigned char *jpeg,
                                        size_t length, int timeout_ms)
                {{
                    (void)pipeline;
                    (void)jpeg;
                    (void)length;
                    (void)timeout_ms;
                    return -1;
                }}

                int ylx_pipeline_finish(ylx_pipeline_t *pipeline)
                {{
                    (void)pipeline;
                    return -1;
                }}

                void ylx_pipeline_close(ylx_pipeline_t *pipeline)
                {{
                    (void)pipeline;
                }}

                void ylx_pipeline_stats(const ylx_pipeline_t *pipeline,
                                        ylx_pipeline_stats_t *out)
                {{
                    (void)pipeline;
                    (void)out;
                }}

                const char *ylx_pipeline_error(const ylx_pipeline_t *pipeline)
                {{
                    (void)pipeline;
                    return "";
                }}

                static int case_escape(void)
                {{
                    on_segment(NULL, 7, 10, 20,
                               "video/left_\\"bad\\\\name.mp4", 123,
                               "video/right_line\\nname.mp4", 456);
                    emit_error_event(
                        "bad\\"code",
                        "message with \\"quote\\" and \\n newline"
                    );
                    return 0;
                }}

                static int case_isolate(void)
                {{
                    if (isolate_event_stream() != 0) {{
                        return 2;
                    }}
                    printf("library stdout noise is not JSON\\n");
                    fflush(stdout);
                    emit("{{\\"event\\":\\"ready\\"}}");
                    on_segment(NULL, 1, 0, 2, "video/left.mp4", 10, "video/right.mp4", 11);
                    return 0;
                }}

                int main(int argc, char **argv)
                {{
                    (void)ylx_encoder_main;
                    if (argc != 2) return 2;
                    if (strcmp(argv[1], "escape") == 0) return case_escape();
                    if (strcmp(argv[1], "isolate") == 0) return case_isolate();
                    return 2;
                }}
                '''
            ),
            encoding="utf-8",
        )
        subprocess.run(
            [
                compiler,
                "-std=gnu11",
                "-Wall",
                "-Wextra",
                f"-I{include_dir}",
                str(harness_src),
                "-o",
                str(cls._harness_binary),
            ],
            check=True,
            cwd=repo,
        )

    def test_submit_error_is_emitted_before_blocked_drain(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            process = subprocess.Popen(
                [str(self._submit_binary), "--out-dir", directory],
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                bufsize=0,
            )
            try:

                def event():
                    self.assertTrue(
                        select.select([process.stdout], [], [], 2)[0], "event blocked by drain"
                    )
                    return json.loads(process.stdout.readline())

                self.assertEqual(event()["event"], "ready")
                process.stdin.write(struct.pack("<4sI", b"YLXF", 4) + b"jpeg")
                failure = event()
                self.assertEqual(failure["event"], "error")
                self.assertEqual(failure["message"], "decoder_input_dequeue_failed: ret=-42")
                self.assertIsNone(process.poll())
                process.stdin.write(b"x")
                self.assertEqual(event()["event"], "done")
                self.assertEqual(process.wait(timeout=2), 1)
                self.assertEqual(process.stdout.read(), b"")
            finally:
                if process.poll() is None:
                    process.kill()
                process.wait(timeout=2)
                for stream in (process.stdin, process.stdout, process.stderr):
                    stream.close()

    def test_helper_stdout_events_escape_json_strings(self) -> None:
        completed = subprocess.run(
            [str(self._harness_binary), "escape"], check=True, text=True, capture_output=True
        )

        events = [json.loads(line) for line in completed.stdout.splitlines()]
        self.assertEqual(events[0]["event"], "segment")
        self.assertEqual(events[0]["left"]["path"], 'video/left_"bad\\name.mp4')
        self.assertEqual(events[0]["right"]["path"], "video/right_line\nname.mp4")
        self.assertEqual(
            events[1],
            {
                "event": "error",
                "code": 'bad"code',
                "message": 'message with "quote" and \n newline',
            },
        )

    def test_helper_stdout_events_are_isolated_from_library_stdout(self) -> None:
        completed = subprocess.run(
            [str(self._harness_binary), "isolate"], check=True, text=True, capture_output=True
        )

        events = [json.loads(line) for line in completed.stdout.splitlines()]
        self.assertEqual([event["event"] for event in events], ["ready", "segment"])
        self.assertIn("library stdout noise is not JSON", completed.stderr)
        self.assertNotIn("library stdout noise", completed.stdout)


if __name__ == "__main__":
    unittest.main()

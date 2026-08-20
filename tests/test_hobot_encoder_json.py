from __future__ import annotations

import json
import shutil
import subprocess
import tempfile
import textwrap
import unittest
from pathlib import Path


class HobotEncoderJsonTest(unittest.TestCase):
    def run_harness(self, body: str) -> subprocess.CompletedProcess[str]:
        compiler = shutil.which("cc") or shutil.which("gcc")
        if compiler is None:
            self.skipTest("C compiler unavailable")
        repo = Path(__file__).resolve().parents[1]
        source = repo / "src/rp_ylx/hobot/ylx_stereo_encoder.c"
        include_dir = repo / "src/rp_ylx/hobot"
        with tempfile.TemporaryDirectory() as directory:
            harness = Path(directory) / "harness.c"
            binary = Path(directory) / "harness"
            harness.write_text(
                textwrap.dedent(
                    f'''
                    #include <stddef.h>
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

                    {body}
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
                    str(harness),
                    "-o",
                    str(binary),
                ],
                check=True,
                cwd=repo,
            )
            return subprocess.run([str(binary)], check=True, text=True, capture_output=True)

    def test_helper_stdout_events_escape_json_strings(self) -> None:
        completed = self.run_harness(
            r"""
            int main(void)
            {
                on_segment(NULL, 7, 10, 20,
                           "video/left_\"bad\\name.mp4", 123,
                           "video/right_line\nname.mp4", 456);
                emit_error_event(
                    "bad\"code",
                    "message with \"quote\" and \n newline"
                );
                return 0;
            }
            """
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
        completed = self.run_harness(
            r"""
            int main(void)
            {
                if (isolate_event_stream() != 0) {
                    return 2;
                }
                printf("library stdout noise is not JSON\n");
                fflush(stdout);
                emit("{\"event\":\"ready\"}");
                on_segment(NULL, 1, 0, 2, "video/left.mp4", 10, "video/right.mp4", 11);
                return 0;
            }
            """
        )

        events = [json.loads(line) for line in completed.stdout.splitlines()]
        self.assertEqual([event["event"] for event in events], ["ready", "segment"])
        self.assertIn("library stdout noise is not JSON", completed.stderr)
        self.assertNotIn("library stdout noise", completed.stdout)


if __name__ == "__main__":
    unittest.main()

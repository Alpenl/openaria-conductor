"""Run the product codec control flow with deterministic SDK failures on the host."""

import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path


class HobotCodecFailuresTest(unittest.TestCase):
    def run_case(self, body: str) -> None:
        compiler = shutil.which("cc")
        if not compiler:
            self.skipTest("C compiler unavailable")
        source = (
            Path(__file__).resolve().parents[1] / "src/rp_ylx/hobot/ylx_stereo_pipeline.c"
        ).read_text()
        submit = source[
            source.index("int ylx_pipeline_submit(") : source.index("int ylx_pipeline_finish(")
        ]
        split = source[
            source.index("static void *split_thread_main(") : source.index(
                "/* ------------------------------------------------------------- lifetime"
            )
        ]
        output = source[
            source.index("static void *encoder_output_thread(") : source.index(
                "static void split_eye("
            )
        ]
        constants = "\n".join(
            line
            for line in source.splitlines()
            if line.startswith(("#define DECODER_", "#define ENCODER_"))
        )
        harness = r"""
#include <assert.h>
#include <stdatomic.h>
#include <stdarg.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#define YLX_EYES 2
#define MC_VIDEO_FRAME_BUFFER 1
#define MC_VIDEO_STREAM_BUFFER 2
#define MC_PIXEL_FORMAT_NV12 3
typedef struct { int id; } media_codec_context_t;
typedef struct {
    int type;
    struct { uint32_t size; int stream_end; unsigned char *vir_ptr; } vstream_buf;
    struct { int width, height, pix_fmt; uint32_t size; } vframe_buf;
} media_codec_buffer_t;
typedef struct { struct { int decode_result; } video_frame_info; } media_codec_output_buffer_info_t;
typedef struct ylx_pipeline ylx_pipeline_t;
typedef struct {
    ylx_pipeline_t *pipeline; int eye, packets;
    media_codec_context_t codec;
} eye_t;
struct ylx_pipeline {
    int eye_width;
    struct { int sbs_width, height, hevc, fps; } config;
    media_codec_context_t decoder;
    eye_t eyes[2];
    atomic_int failed, stopping, split_done;
    atomic_ullong offered, submitted, decoded, fed[2], encoded[2];
    atomic_ullong drop_decoder_input, drop_decoder_output, drop_encoder_input;
    char error[256];
};
#define MUX_QUEUE_BYTES 1024u
#define MUX_QUEUE_SECONDS 16u
static const char *const EYE_NAMES[] = {"left", "right"};
static int dequeue_result[3], queue_result[3], output_calls, output_mode;
static int input_calls[3], queued[3], released, copied;
static unsigned char payload_buffer[32];
static unsigned capacity = sizeof(payload_buffer);
static void fail(ylx_pipeline_t *p, const char *format, ...) {
    if (!p->error[0]) {
        va_list args; va_start(args, format);
        vsnprintf(p->error, sizeof(p->error), format, args); va_end(args);
    }
    atomic_store(&p->failed, 1);
}
static int hb_mm_mc_dequeue_input_buffer(
    media_codec_context_t *c, media_codec_buffer_t *b, int timeout) {
    assert(timeout > 0); input_calls[c->id]++;
    b->vstream_buf.size = capacity; b->vstream_buf.vir_ptr = payload_buffer;
    return dequeue_result[c->id];
}
static int hb_mm_mc_queue_input_buffer(
    media_codec_context_t *c, media_codec_buffer_t *b, int timeout) {
    (void)b; assert(timeout > 0); queued[c->id]++; return queue_result[c->id];
}
static int hb_mm_mc_dequeue_output_buffer(media_codec_context_t *c, media_codec_buffer_t *b,
                                         media_codec_output_buffer_info_t *info, int timeout) {
    (void)c; assert(timeout > 0); output_calls++;
    if (!output_mode || output_calls != 1) return -77;
    b->type = MC_VIDEO_FRAME_BUFFER; b->vframe_buf.size = 12;
    b->vframe_buf.width = 4; b->vframe_buf.height = 2;
    info->video_frame_info.decode_result = output_mode == 1;
    return 0;
}
static int hb_mm_mc_queue_output_buffer(
    media_codec_context_t *c, media_codec_buffer_t *b, int timeout) {
    (void)c; (void)b; (void)timeout; released++; return 0;
}
static void split_eye(const media_codec_buffer_t *s, media_codec_buffer_t *t, int w, int h, int x) {
    (void)s; (void)t; (void)w; (void)h; (void)x; copied++;
}
static int h264_has_idr(const unsigned char *data, size_t length) {
    (void)data; (void)length; return 1;
}
static int h265_has_idr(const unsigned char *data, size_t length) {
    return h264_has_idr(data, length);
}
static int ylx_packet_queue_push(int *q, const void *data, size_t size, int key) {
    (void)q; (void)data; (void)size; (void)key; return 0;
}
static void ylx_packet_queue_close(int *q) { *q = 1; }
static ylx_pipeline_t make_pipeline(void) {
    ylx_pipeline_t p = {0}; p.eye_width = 2; p.config.sbs_width = 4; p.config.height = 2;
    p.eyes[0].codec.id = 1; p.eyes[1].codec.id = 2; return p;
}
"""
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            path, binary = root / "case.c", root / "case"
            used_body = body.replace(
                "int main(void) {",
                "int main(void) { (void)encoder_output_thread; (void)split_thread_main;",
            )
            path.write_text(harness + constants + "\n" + submit + split + output + used_body)
            subprocess.run(
                [
                    compiler,
                    "-std=gnu11",
                    "-Wall",
                    "-Wextra",
                    "-Werror",
                    str(path),
                    "-o",
                    str(binary),
                ],
                check=True,
            )
            subprocess.run([str(binary)], check=True, timeout=5)

    def test_input_failures_preserve_causal_reason_and_reject_following_frames(self):
        self.run_case(r"""
int main(void) {
    (void)split_thread_main;
    unsigned char jpeg[] = {1,2,3,4};
    ylx_pipeline_t p = make_pipeline();
    dequeue_result[0] = -42;
    assert(ylx_pipeline_submit(&p, jpeg, 4, 17) == -1);
    assert(strstr(p.error, "decoder_input_dequeue_failed: ret=-42 timeout_ms=17 bytes=4"));
    assert(p.submitted == 0 && p.drop_decoder_input == 1);
    assert(ylx_pipeline_submit(&p, jpeg, 4, 17) == -1 && input_calls[0] == 1);
    dequeue_result[0] = 0; capacity = 2; p = make_pipeline();
    assert(ylx_pipeline_submit(&p, jpeg, 4, 0) == -1);
    assert(strstr(p.error, "decoder_input_capacity: bytes=4 capacity=2"));
    assert(queued[0] == 1 && p.submitted == 0);
    capacity = 32; queue_result[0] = -43; p = make_pipeline();
    assert(ylx_pipeline_submit(&p, jpeg, 4, 0) == -1);
    assert(strstr(p.error, "decoder_input_queue_failed: ret=-43 bytes=4"));
    assert(p.drop_decoder_input == 1 && p.submitted == 0);
    queue_result[0] = 0; p = make_pipeline();
    assert(ylx_pipeline_submit(&p, jpeg, 4, 0) == 0);
    assert(p.submitted == 1 && memcmp(jpeg, payload_buffer, 4) == 0);
    return 0;
}
""")

    def test_missing_decoder_output_has_bounded_failed_drain(self):
        self.run_case(r"""
int main(void) {
    ylx_pipeline_t p = make_pipeline(); p.submitted = 1; p.stopping = 1;
    split_thread_main(&p);
    assert(output_calls == DECODER_DRAIN_POLLS && p.split_done && p.failed);
    assert(strstr(p.error, "decoder_drain_timeout: ret=-77 submitted=1 decoded=0"));
    // A complete decoder drain is successful and feeds exactly one frame to each eye.
    p = make_pipeline(); p.submitted = 1; p.stopping = 1; output_calls = 0; output_mode = 1;
    split_thread_main(&p);
    assert(!p.failed && p.split_done && p.decoded == 1 && p.fed[0] == 1 && p.fed[1] == 1);
    assert(output_calls == 2 && released == 1 && copied == 2);
    return 0;
}
""")

    def test_missing_encoded_frame_is_failed_instead_of_successful_drain(self):
        self.run_case(r"""
int main(void) {
    ylx_pipeline_t p = make_pipeline();
    p.split_done = 1; p.fed[0] = 1; p.eyes[0].pipeline = &p;
    encoder_output_thread(&p.eyes[0]);
    assert(output_calls == ENCODER_DRAIN_POLLS && p.failed && p.eyes[0].packets);
    assert(strstr(p.error, "encoder_drain_timeout: eye=left ret=-77 fed=1 encoded=0"));
    p = make_pipeline(); p.split_done = 1; p.eyes[0].pipeline = &p; output_calls = 0;
    encoder_output_thread(&p.eyes[0]);
    assert(output_calls == 1 && !p.failed && p.eyes[0].packets);
    return 0;
}
""")

    def test_bad_output_or_one_eye_failure_never_skips_into_next_frame(self):
        self.run_case(r"""
int main(void) {
    ylx_pipeline_t p = make_pipeline(); output_mode = 2;
    split_thread_main(&p);
    assert(p.failed && p.split_done && p.drop_decoder_output == 1);
    assert(strstr(p.error, "decoder_output_invalid") && released == 1 && copied == 0);
    p = make_pipeline(); output_mode = 1; output_calls = 0; dequeue_result[1] = -45;
    split_thread_main(&p);
    assert(p.failed && p.split_done && p.drop_encoder_input == 1);
    assert(strstr(p.error, "encoder_input_dequeue_failed: eye=left ret=-45"));
    assert(!p.fed[0] && !p.fed[1] && output_calls == 1 && released == 2);
    p = make_pipeline(); output_calls = 0; dequeue_result[1] = 0; queue_result[2] = -46;
    split_thread_main(&p);
    assert(strstr(p.error, "encoder_input_queue_failed: eye=right ret=-46"));
    assert(p.fed[0] == 1 && p.fed[1] == 0 && output_calls == 1 && released == 3);
    return 0;
}
""")

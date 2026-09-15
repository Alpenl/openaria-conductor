"""Host regression harness for compressed-packet buffering and stereo commit order."""

import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path


class VideoWriteBackpressureTests(unittest.TestCase):
    def compile_and_run(self, source):
        compiler = shutil.which("cc")
        if not compiler:
            self.skipTest("C compiler unavailable")
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "test.c"
            binary = Path(directory) / "test"
            path.write_text(source)
            subprocess.run(
                [
                    compiler,
                    "-std=gnu11",
                    "-Wall",
                    "-Wextra",
                    "-Werror",
                    "-pthread",
                    str(path),
                    "-o",
                    str(binary),
                ],
                check=True,
            )
            subprocess.run([str(binary)], check=True, timeout=10)

    def test_slow_consumer_keeps_fifo_data_and_enforces_both_memory_limits(self):
        include = Path(__file__).resolve().parents[1] / "src/rp_ylx/hobot/ylx_packet_queue.h"
        self.compile_and_run(
            f'#include "{include}"\n'
            + r"""
#include <assert.h>
#include <stdatomic.h>
#include <unistd.h>
static ylx_packet_queue_t queue;
static atomic_int writing, resume_writer;
static void *consumer(void *unused) {
    (void)unused;
    for (unsigned char n = 0; n < 8; n++) {
        ylx_packet_t *packet = ylx_packet_queue_pop(&queue);
        assert(packet && packet->size == 16 && packet->key_frame == (n == 0));
        for (size_t i = 0; i < packet->size; i++) assert(packet->data[i] == n);
        if (n == 0) {
            atomic_store(&writing, 1);
            while (!atomic_load(&resume_writer)) usleep(1000);
        }
        ylx_packet_queue_done(&queue, packet);
    }
    assert(!ylx_packet_queue_pop(&queue));
    return NULL;
}
int main(void) {
    ylx_packet_queue_init(&queue, 128, 8);
    unsigned char data[16] = {0};
    assert(!ylx_packet_queue_push(&queue, data, sizeof(data), 1));
    pthread_t writer;
    assert(!pthread_create(&writer, NULL, consumer, NULL));
    while (!atomic_load(&writing)) usleep(1000);
    // A blocked disk writer does not block the producer. The outstanding
    // hardware buffer can be returned while this independent copy waits.
    for (unsigned char n = 1; n < 8; n++) {
        memset(data, n, sizeof(data));
        assert(!ylx_packet_queue_push(&queue, data, sizeof(data), 0));
        memset(data, 99, sizeof(data));
    }
    assert(ylx_packet_queue_push(&queue, data, 1, 0) == -1);
    assert(queue.peak_bytes == 128 && queue.peak_frames == 8 && queue.rejected == 1);
    ylx_packet_queue_close(&queue);
    atomic_store(&resume_writer, 1);
    pthread_join(writer, NULL);
    assert(queue.bytes == 0 && queue.frames == 0);
    ylx_packet_queue_destroy(&queue);
    ylx_packet_queue_init(&queue, 32, 100);
    assert(!ylx_packet_queue_push(&queue, data, 16, 0));
    ylx_packet_t *in_flight = ylx_packet_queue_pop(&queue);
    assert(!ylx_packet_queue_push(&queue, data, 16, 0));
    assert(ylx_packet_queue_push(&queue, data, 1, 0) == -1);
    ylx_packet_queue_done(&queue, in_flight);
    ylx_packet_queue_destroy(&queue);
    return 0;
}
"""
        )

    def test_one_eye_can_finish_multiple_segments_before_other_eye(self):
        source = (
            Path(__file__).resolve().parents[1] / "src/rp_ylx/hobot/ylx_stereo_pipeline.c"
        ).read_text()
        ledger = source[
            source.index("static void ledger_record(") : source.index("static void segment_close(")
        ]
        pair = source[
            source.index("typedef struct closed_pair {") : source.index("struct ylx_pipeline {")
        ]
        self.compile_and_run(
            r"""
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#define YLX_EYES 2
#define PATH_LEN 512
"""
            + pair
            + r"""
typedef struct {
    pthread_mutex_t ledger_lock;
    closed_pair_t *closed_pairs;
    int pending_pairs, reported_pairs;
    void (*on_segment)(void *, int, unsigned long long, unsigned long long,
                       const char *, unsigned long long, const char *, unsigned long long);
    void *user;
    int failed;
} ylx_pipeline_t;
static void fail(ylx_pipeline_t *pipeline, const char *format, ...) {
    (void)format; pipeline->failed = 1;
}
static int received;
static void report(void *unused, int index, unsigned long long start, unsigned long long end,
                   const char *left, unsigned long long lb,
                   const char *right, unsigned long long rb) {
    (void)unused; (void)left; (void)right;
    assert(index == received++ && start == (unsigned)index * 900 && end == start + 900);
    assert(lb == 123 && rb == 456);
}
"""
            + ledger
            + r"""
int main(void) {
    ylx_pipeline_t pipeline = {0};
    pthread_mutex_init(&pipeline.ledger_lock, NULL);
    pipeline.on_segment = report;
    for (int n = 0; n < 3; n++) ledger_record(&pipeline, 0, n, "left", 123, n*900, (n+1)*900);
    assert(received == 0);
    for (int n = 0; n < 3; n++) ledger_record(&pipeline, 1, n, "right", 456, n*900, (n+1)*900);
    assert(received == 3 && pipeline.pending_pairs == 0 && !pipeline.failed);
    ledger_record(&pipeline, 0, 3, "left", 123, 2700, 3600);
    ledger_record(&pipeline, 1, 3, "right", 456, 2700, 3599);
    assert(received == 3 && pipeline.failed);
    free(pipeline.closed_pairs);
    pthread_mutex_destroy(&pipeline.ledger_lock);
    return 0;
}
"""
        )

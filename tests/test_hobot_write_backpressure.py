"""Host regression harness for compressed-packet buffering and stereo commit order."""

import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path


class VideoWriteBackpressureTests(unittest.TestCase):
    _binary: Path

    @classmethod
    def setUpClass(cls) -> None:
        compiler = shutil.which("gcc") or shutil.which("cc")
        if not compiler:
            raise unittest.SkipTest("C compiler unavailable")
        cls._temporary = tempfile.TemporaryDirectory()
        cls.addClassCleanup(cls._temporary.cleanup)
        root = Path(__file__).resolve().parents[1] / "src/rp_ylx/hobot"
        pipeline_source = (root / "ylx_stereo_pipeline.c").read_text()
        constants = "\n".join(
            line
            for line in pipeline_source.splitlines()
            if line.startswith(("#define MUX_QUEUE_BYTES", "#define MUX_QUEUE_SECONDS"))
        )
        ledger = pipeline_source[
            pipeline_source.index("static void ledger_record(") : pipeline_source.index(
                "static void queue_segment("
            )
        ]
        pair = pipeline_source[
            pipeline_source.index("typedef struct closed_pair {") : pipeline_source.index(
                "struct ylx_pipeline {"
            )
        ]
        combined = (
            f'#include "{root / "ylx_packet_queue.h"}"\n'
            + constants
            + "\n"
            + r"""
#include <assert.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#define YLX_EYES 2
#define PATH_LEN 512

static ylx_packet_queue_t fifo_queue;
static atomic_int writing, resume_writer;
static void *consumer(void *unused) {
    (void)unused;
    for (unsigned char n = 0; n < 8; n++) {
        ylx_packet_t *packet = ylx_packet_queue_pop(&fifo_queue);
        assert(packet && packet->size == 16 && packet->key_frame == (n == 0));
        for (size_t i = 0; i < packet->size; i++) assert(packet->data[i] == n);
        if (n == 0) {
            atomic_store(&writing, 1);
            while (!atomic_load(&resume_writer)) usleep(1000);
        }
        ylx_packet_queue_done(&fifo_queue, packet);
    }
    assert(!ylx_packet_queue_pop(&fifo_queue));
    return NULL;
}

static int case_fifo(void) {
    ylx_packet_queue_init(&fifo_queue, 128, 8);
    unsigned char data[16] = {0};
    assert(!ylx_packet_queue_push(&fifo_queue, data, sizeof(data), 1));
    pthread_t writer;
    assert(!pthread_create(&writer, NULL, consumer, NULL));
    while (!atomic_load(&writing)) usleep(1000);
    // A blocked disk writer does not block the producer. The outstanding
    // hardware buffer can be returned while this independent copy waits.
    for (unsigned char n = 1; n < 8; n++) {
        memset(data, n, sizeof(data));
        assert(!ylx_packet_queue_push(&fifo_queue, data, sizeof(data), 0));
        memset(data, 99, sizeof(data));
    }
    assert(ylx_packet_queue_push(&fifo_queue, data, 1, 0) == -1);
    assert(fifo_queue.peak_bytes == 128 && fifo_queue.peak_frames == 8 && fifo_queue.rejected == 1);
    ylx_packet_queue_close(&fifo_queue);
    atomic_store(&resume_writer, 1);
    pthread_join(writer, NULL);
    assert(fifo_queue.bytes == 0 && fifo_queue.frames == 0);
    ylx_packet_queue_destroy(&fifo_queue);
    ylx_packet_queue_init(&fifo_queue, 32, 100);
    assert(!ylx_packet_queue_push(&fifo_queue, data, 16, 0));
    ylx_packet_t *in_flight = ylx_packet_queue_pop(&fifo_queue);
    assert(!ylx_packet_queue_push(&fifo_queue, data, 16, 0));
    assert(ylx_packet_queue_push(&fifo_queue, data, 1, 0) == -1);
    ylx_packet_queue_done(&fifo_queue, in_flight);
    ylx_packet_queue_destroy(&fifo_queue);
    return 0;
}

static int case_budget(void) {
    ylx_packet_queue_t queue;
    assert(!ylx_packet_queue_init(&queue, MUX_QUEUE_BYTES, MUX_QUEUE_SECONDS * 30));
    // No consumer runs: simulate a 20-second disk stall at 30fps and 8Mbps.
    unsigned char data[35000];
    for (unsigned n = 0; n < 600; n++) {
        memset(data, n % 251, sizeof(data));
        assert(!ylx_packet_queue_push(&queue, data, sizeof(data), n == 0));
    }
    ylx_packet_queue_close(&queue);
    for (unsigned n = 0; n < 600; n++) {
        ylx_packet_t *packet = ylx_packet_queue_pop(&queue);
        assert(packet && packet->size == sizeof(data));
        for (size_t i = 0; i < packet->size; i++) assert(packet->data[i] == n % 251);
        ylx_packet_queue_done(&queue, packet);
    }
    assert(!ylx_packet_queue_pop(&queue) && queue.bytes == 0 && queue.frames == 0);
    ylx_packet_queue_destroy(&queue);
    return 0;
}
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
static int case_ledger(void) {
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

int main(int argc, char **argv) {
    if (argc != 2) return 2;
    if (strcmp(argv[1], "fifo") == 0) return case_fifo();
    if (strcmp(argv[1], "budget") == 0) return case_budget();
    if (strcmp(argv[1], "ledger") == 0) return case_ledger();
    return 2;
}
"""
        )
        temp_dir = Path(cls._temporary.name)
        path = temp_dir / "test.c"
        cls._binary = temp_dir / "test"
        path.write_text(combined, encoding="utf-8")
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
                str(cls._binary),
            ],
            check=True,
        )

    def test_slow_consumer_keeps_fifo_data_and_enforces_both_memory_limits(self):
        subprocess.run([str(self._binary), "fifo"], check=True, timeout=10)

    def test_production_budget_retains_twenty_seconds_then_drains_in_order(self):
        subprocess.run([str(self._binary), "budget"], check=True, timeout=10)

    def test_one_eye_can_finish_multiple_segments_before_other_eye(self):
        subprocess.run([str(self._binary), "ledger"], check=True, timeout=10)

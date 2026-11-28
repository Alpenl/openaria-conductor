#define _GNU_SOURCE
#include "miro_ego_shm_ring.h"

#include <assert.h>
#include <errno.h>
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/eventfd.h>
#include <sys/mman.h>
#include <time.h>
#include <unistd.h>

static uint64_t now_ns(void)
{
    struct timespec ts = {0};
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
}

static void make_fds(int *shm_fd, int *kick_fd, int *done_fd, int efd_flags)
{
    *shm_fd = memfd_create("miro-ego-shm-test", MFD_CLOEXEC);
    assert(*shm_fd >= 0);
    *kick_fd = eventfd(0, EFD_CLOEXEC | efd_flags);
    assert(*kick_fd >= 0);
    *done_fd = eventfd(0, EFD_CLOEXEC | efd_flags);
    assert(*done_fd >= 0);
}

static void close_fds(int shm_fd, int kick_fd, int done_fd)
{
    close(shm_fd);
    close(kick_fd);
    close(done_fd);
}

static void test_binary_layout_offsets(void)
{
    /* Verify MEGO_SHM v1 byte layout matches Rust shm_ring contract */
    assert(sizeof(miro_ego_shm_header_t) == 4096u);
    assert(offsetof(miro_ego_shm_header_t, magic) == 0u);
    assert(offsetof(miro_ego_shm_header_t, version) == 8u);
    assert(offsetof(miro_ego_shm_header_t, slot_count) == 12u);
    assert(offsetof(miro_ego_shm_header_t, slot_capacity) == 16u);
    assert(offsetof(miro_ego_shm_header_t, flags) == 20u);
    assert(offsetof(miro_ego_shm_header_t, write_idx) == 64u);
    assert(offsetof(miro_ego_shm_header_t, read_idx) == 128u);
    assert(offsetof(miro_ego_shm_header_t, dropped_frames) == 192u);

    assert(sizeof(miro_ego_shm_slot_t) == 64u);
    assert(offsetof(miro_ego_shm_slot_t, frame_seq) == 0u);
    assert(offsetof(miro_ego_shm_slot_t, monotonic_ns) == 8u);
    assert(offsetof(miro_ego_shm_slot_t, payload_len) == 16u);
    assert(offsetof(miro_ego_shm_slot_t, flags) == 20u);
    assert(offsetof(miro_ego_shm_slot_t, data_offset) == 24u);
    assert(offsetof(miro_ego_shm_slot_t, sample_number) == 32u);
    assert(offsetof(miro_ego_shm_slot_t, duration_ticks) == 40u);
    assert(offsetof(miro_ego_shm_slot_t, pcm_offset) == 44u);
    assert(offsetof(miro_ego_shm_slot_t, pcm_len) == 48u);
    assert(offsetof(miro_ego_shm_slot_t, meta_offset) == 52u);
    assert(offsetof(miro_ego_shm_slot_t, meta_len) == 56u);
    assert(offsetof(miro_ego_shm_slot_t, reserved) == 60u);

    /* Data region alignment checks */
    assert(miro_ego_shm_data_offset(1u) == 8192u);
    assert(miro_ego_shm_data_offset(16u) == 8192u);
    assert(miro_ego_shm_data_offset(64u) == 8192u);
    assert(miro_ego_shm_data_offset(128u) == 12288u);
    assert(miro_ego_shm_data_offset(256u) == 20480u);

    assert(miro_ego_shm_required_size(0u, 4096u) == 0u);
    assert(miro_ego_shm_required_size(3u, 4096u) == 0u);
    assert(miro_ego_shm_required_size(512u, 4096u) == 0u);
    assert(miro_ego_shm_required_size(16u, 0u) == 0u);
    assert(miro_ego_shm_required_size(16u, 4096u) == 8192u + 16u * 4096u);
}

typedef struct {
    miro_ego_shm_ring_t *ring;
    uint32_t total_frames;
    uint32_t max_payload;
} wrap_args_t;

static void *wrap_producer(void *arg)
{
    wrap_args_t *a = (wrap_args_t *)arg;
    uint8_t *buf = malloc(a->max_payload);
    assert(buf != NULL);

    for (uint32_t seq = 0; seq < a->total_frames; seq += 1) {
        uint32_t len = 1u + ((seq * 257u + 31u) % a->max_payload);
        for (uint32_t i = 0; i < len; i += 1) {
            buf[i] = (uint8_t)((seq * 13u + i) & 0xffu);
        }
        miro_ego_shm_status_t st = miro_ego_shm_ring_push(
            a->ring, buf, len, (uint64_t)seq, 1000000ULL + (uint64_t)seq * 33333ULL,
            seq & 0x7u, 5000);
        assert(st == MIRO_EGO_SHM_OK);
    }
    assert(miro_ego_shm_ring_set_eof(a->ring) == MIRO_EGO_SHM_OK);
    free(buf);
    return NULL;
}

static void test_mt_wraparound(int efd_flags)
{
    int shm_fd, kick_fd, done_fd;
    make_fds(&shm_fd, &kick_fd, &done_fd, efd_flags);

    miro_ego_shm_ring_t producer, consumer;
    char err[128] = {0};
    assert(miro_ego_shm_ring_init_fd(&producer, shm_fd, kick_fd, done_fd,
                                     8u, 8192u, err, sizeof(err)) == 0);
    assert(miro_ego_shm_ring_attach(&consumer, shm_fd, kick_fd, done_fd,
                                    err, sizeof(err)) == 0);

    wrap_args_t args = {.ring = &producer, .total_frames = 512u, .max_payload = 8192u};
    pthread_t prod_thread;
    assert(pthread_create(&prod_thread, NULL, wrap_producer, &args) == 0);

    uint32_t received = 0;
    for (;;) {
        miro_ego_shm_frame_t frame;
        miro_ego_shm_status_t st = miro_ego_shm_ring_receive(&consumer, &frame, 5000);
        if (st == MIRO_EGO_SHM_EOF) {
            break;
        }
        assert(st == MIRO_EGO_SHM_OK);
        assert(frame.frame_seq == (uint64_t)received);
        assert(frame.monotonic_ns == 1000000ULL + (uint64_t)received * 33333ULL);
        assert(frame.flags == (received & 0x7u));
        const uint32_t expected_len = 1u + ((received * 257u + 31u) % 8192u);
        assert(frame.payload_len == expected_len);
        for (uint32_t i = 0; i < frame.payload_len; i += 1) {
            assert(frame.data[i] == (uint8_t)((received * 13u + i) & 0xffu));
        }
        assert(miro_ego_shm_ring_release(&consumer) == MIRO_EGO_SHM_OK);
        received += 1u;
    }

    assert(received == args.total_frames);
    assert(miro_ego_shm_ring_dropped(&consumer) == 0u);
    pthread_join(prod_thread, NULL);

    miro_ego_shm_ring_detach(&consumer);
    miro_ego_shm_ring_detach(&producer);
    close_fds(shm_fd, kick_fd, done_fd);
}

static void test_eof_drain_and_backpressure(void)
{
    int shm_fd, kick_fd, done_fd;
    make_fds(&shm_fd, &kick_fd, &done_fd, EFD_NONBLOCK);

    miro_ego_shm_ring_t producer, consumer;
    char err[128] = {0};
    assert(miro_ego_shm_ring_init_fd(&producer, shm_fd, kick_fd, done_fd,
                                     4u, 256u, err, sizeof(err)) == 0);
    assert(miro_ego_shm_ring_attach(&consumer, shm_fd, kick_fd, done_fd,
                                    err, sizeof(err)) == 0);

    /* Fill all 4 slots, verify 5th non-blocking push returns FULL and increments dropped_frames */
    uint8_t msg[32];
    for (uint32_t i = 0; i < 4u; i += 1) {
        memset(msg, (int)(0x40u + i), sizeof(msg));
        assert(miro_ego_shm_ring_push(&producer, msg, sizeof(msg), i, i * 10u, 0u, 0) ==
               MIRO_EGO_SHM_OK);
    }
    assert(miro_ego_shm_ring_push(&producer, msg, sizeof(msg), 99u, 99u, 0u, 0) ==
           MIRO_EGO_SHM_FULL);
    assert(miro_ego_shm_ring_dropped(&consumer) == 1u);

    /* Set EOF while 4 frames are still queued: push must now return EOF, and consumer MUST drain all 4 before EOF! */
    assert(miro_ego_shm_ring_set_eof(&producer) == MIRO_EGO_SHM_OK);
    assert(miro_ego_shm_ring_push(&producer, msg, sizeof(msg), 100u, 100u, 0u, 0) ==
           MIRO_EGO_SHM_EOF);

    for (uint32_t i = 0; i < 4u; i += 1) {
        miro_ego_shm_frame_t frame;
        assert(miro_ego_shm_ring_receive(&consumer, &frame, 100) == MIRO_EGO_SHM_OK);
        assert(frame.frame_seq == i);
        assert(frame.payload_len == sizeof(msg));
        assert(frame.data[0] == (uint8_t)(0x40u + i));
        assert(miro_ego_shm_ring_release(&consumer) == MIRO_EGO_SHM_OK);
    }
    miro_ego_shm_frame_t end_frame;
    assert(miro_ego_shm_ring_receive(&consumer, &end_frame, 100) == MIRO_EGO_SHM_EOF);

    miro_ego_shm_ring_detach(&consumer);
    miro_ego_shm_ring_detach(&producer);
    close_fds(shm_fd, kick_fd, done_fd);

    /* Empty immediate EOF test */
    make_fds(&shm_fd, &kick_fd, &done_fd, 0);
    assert(miro_ego_shm_ring_init_fd(&producer, shm_fd, kick_fd, done_fd,
                                     4u, 256u, err, sizeof(err)) == 0);
    assert(miro_ego_shm_ring_attach(&consumer, shm_fd, kick_fd, done_fd,
                                    err, sizeof(err)) == 0);
    assert(miro_ego_shm_ring_set_eof(&producer) == MIRO_EGO_SHM_OK);
    assert(miro_ego_shm_ring_receive(&consumer, &end_frame, 100) == MIRO_EGO_SHM_EOF);
    miro_ego_shm_ring_detach(&consumer);
    miro_ego_shm_ring_detach(&producer);
    close_fds(shm_fd, kick_fd, done_fd);
}

static void test_bounds_and_corruption_rejection(void)
{
    int shm_fd, kick_fd, done_fd;
    make_fds(&shm_fd, &kick_fd, &done_fd, 0);

    miro_ego_shm_ring_t ring;
    char err[128] = {0};

    /* Duplicate / invalid FDs rejected */
    assert(miro_ego_shm_ring_attach(&ring, -1, kick_fd, done_fd, err, sizeof(err)) == -1);
    assert(miro_ego_shm_ring_attach(&ring, shm_fd, shm_fd, done_fd, err, sizeof(err)) == -1);

    /* Truncated memfd (< 4096 bytes) rejected */
    assert(ftruncate(shm_fd, 1024) == 0);
    assert(miro_ego_shm_ring_attach(&ring, shm_fd, kick_fd, done_fd, err, sizeof(err)) == -1);

    /* Initialize a valid 4-slot, 1024-byte ring, then corrupt header fields */
    miro_ego_shm_ring_t prod;
    assert(miro_ego_shm_ring_init_fd(&prod, shm_fd, kick_fd, done_fd,
                                     4u, 1024u, err, sizeof(err)) == 0);

    /* 1. Bad magic */
    prod.header->magic[0] = 'X';
    assert(miro_ego_shm_ring_attach(&ring, shm_fd, kick_fd, done_fd, err, sizeof(err)) == -1);
    prod.header->magic[0] = 'M';

    /* 2. Bad version */
    prod.header->version = 2u;
    assert(miro_ego_shm_ring_attach(&ring, shm_fd, kick_fd, done_fd, err, sizeof(err)) == -1);
    prod.header->version = 1u;

    /* 3. Non-power-of-2 and out-of-range slot_count */
    const uint32_t bad_slots[] = {0u, 3u, 6u, 255u, 512u};
    for (size_t i = 0; i < sizeof(bad_slots) / sizeof(bad_slots[0]); i += 1) {
        prod.header->slot_count = bad_slots[i];
        assert(miro_ego_shm_ring_attach(&ring, shm_fd, kick_fd, done_fd, err, sizeof(err)) == -1);
    }
    prod.header->slot_count = 4u;

    /* 4. Zero / oversized slot_capacity / file smaller than declared capacity */
    prod.header->slot_capacity = 0u;
    assert(miro_ego_shm_ring_attach(&ring, shm_fd, kick_fd, done_fd, err, sizeof(err)) == -1);
    prod.header->slot_capacity = MIRO_EGO_SHM_MAX_SLOT_CAPACITY + 1u;
    assert(miro_ego_shm_ring_attach(&ring, shm_fd, kick_fd, done_fd, err, sizeof(err)) == -1);
    prod.header->slot_capacity = 8192u; /* requires more bytes than fstat(shm_fd) */
    assert(miro_ego_shm_ring_attach(&ring, shm_fd, kick_fd, done_fd, err, sizeof(err)) == -1);
    prod.header->slot_capacity = 1024u;

    /* Attach valid consumer and test runtime slot bounds rejection */
    assert(miro_ego_shm_ring_attach(&ring, shm_fd, kick_fd, done_fd, err, sizeof(err)) == 0);

    /* Corrupt slot: data_offset pointing into header (< data_offset) */
    prod.slots[0].payload_len = 64u;
    prod.slots[0].data_offset = 0u;
    atomic_store_explicit(&prod.header->write_idx, 1ULL, memory_order_release);
    miro_ego_shm_frame_t frame;
    assert(miro_ego_shm_ring_receive(&ring, &frame, 10) == MIRO_EGO_SHM_CORRUPT);

    /* Corrupt slot: data_offset + payload_len > map_size */
    prod.slots[0].data_offset = (uint64_t)prod.map_size - 16u;
    prod.slots[0].payload_len = 32u;
    assert(miro_ego_shm_ring_receive(&ring, &frame, 10) == MIRO_EGO_SHM_CORRUPT);

    /* Corrupt slot: data_offset integer overflow */
    prod.slots[0].data_offset = UINT64_MAX - 8ULL;
    prod.slots[0].payload_len = 32u;
    assert(miro_ego_shm_ring_receive(&ring, &frame, 10) == MIRO_EGO_SHM_CORRUPT);

    /* Corrupt slot: payload_len == 0 */
    prod.slots[0].data_offset = prod.data_offset;
    prod.slots[0].payload_len = 0u;
    assert(miro_ego_shm_ring_receive(&ring, &frame, 10) == MIRO_EGO_SHM_CORRUPT);

    /* Corrupt slot: payload_len > slot_capacity */
    prod.slots[0].payload_len = 1025u;
    assert(miro_ego_shm_ring_receive(&ring, &frame, 10) == MIRO_EGO_SHM_CORRUPT);

    /* Corrupt slot: data_offset pointing to another valid slot (slot 1 instead of slot 0) */
    prod.slots[0].payload_len = 64u;
    prod.slots[0].data_offset = prod.data_offset + 1024u;
    assert(miro_ego_shm_ring_receive(&ring, &frame, 10) == MIRO_EGO_SHM_CORRUPT);
    prod.slots[0].data_offset = prod.data_offset;

    /* Corrupt ring indices: write_idx - read_idx > slot_count */
    prod.slots[0].payload_len = 64u;
    atomic_store_explicit(&prod.header->write_idx, 99ULL, memory_order_release);
    assert(miro_ego_shm_ring_receive(&ring, &frame, 10) == MIRO_EGO_SHM_CORRUPT);

    miro_ego_shm_ring_detach(&ring);
    miro_ego_shm_ring_detach(&prod);
    close_fds(shm_fd, kick_fd, done_fd);
}

static void test_sync_bundle_roundtrip_and_bounds(void)
{
    int shm_fd, kick_fd, done_fd;
    make_fds(&shm_fd, &kick_fd, &done_fd, EFD_NONBLOCK);

    miro_ego_shm_ring_t prod, cons;
    char err[128] = {0};
    assert(miro_ego_shm_ring_init_fd(&prod, shm_fd, kick_fd, done_fd,
                                     4u, 4096u, err, sizeof(err)) == 0);
    assert(miro_ego_shm_ring_attach(&cons, shm_fd, kick_fd, done_fd, err, sizeof(err)) == 0);

    uint8_t jpeg[128];
    uint8_t pcm[640];
    const char *meta = "{\"seq\":7,\"ticks\":1600}";
    memset(jpeg, 0xaa, sizeof(jpeg));
    for (size_t i = 0; i < sizeof(pcm); i += 1u) {
        pcm[i] = (uint8_t)(i & 0xffu);
    }

    assert(miro_ego_shm_ring_push_bundle(
               &prod, jpeg, sizeof(jpeg),
               pcm, (uint32_t)sizeof(pcm),
               (const uint8_t *)meta, (uint32_t)strlen(meta),
               7ULL, 123456789ULL, 11200ULL, 1600u, 1000) == MIRO_EGO_SHM_OK);

    miro_ego_shm_frame_t frame;
    assert(miro_ego_shm_ring_receive(&cons, &frame, 1000) == MIRO_EGO_SHM_OK);
    assert((frame.flags & MIRO_EGO_SHM_SLOT_FLAG_SYNC_BUNDLE) != 0u);
    assert(frame.frame_seq == 7ULL);
    assert(frame.monotonic_ns == 123456789ULL);
    assert(frame.sample_number == 11200ULL);
    assert(frame.duration_ticks == 1600u);
    assert(frame.payload_len == sizeof(jpeg));
    assert(memcmp(frame.data, jpeg, sizeof(jpeg)) == 0);
    assert(frame.pcm_len == sizeof(pcm) && frame.pcm != NULL);
    assert(memcmp(frame.pcm, pcm, sizeof(pcm)) == 0);
    assert(frame.meta_len == strlen(meta) && frame.meta != NULL);
    assert(memcmp(frame.meta, meta, strlen(meta)) == 0);
    assert(miro_ego_shm_ring_release(&cons) == MIRO_EGO_SHM_OK);

    /* Overlapping PCM offset into JPEG payload rejected as CORRUPT */
    prod.slots[1].frame_seq = 8ULL;
    prod.slots[1].monotonic_ns = 1ULL;
    prod.slots[1].data_offset = prod.data_offset + 4096ULL;
    prod.slots[1].payload_len = 128u;
    prod.slots[1].flags = MIRO_EGO_SHM_SLOT_FLAG_SYNC_BUNDLE;
    prod.slots[1].pcm_offset = 64u;
    prod.slots[1].pcm_len = 64u;
    prod.slots[1].meta_offset = 0u;
    prod.slots[1].meta_len = 0u;
    atomic_store_explicit(&prod.header->write_idx, 2ULL, memory_order_release);
    assert(miro_ego_shm_ring_receive(&cons, &frame, 10) == MIRO_EGO_SHM_CORRUPT);

    miro_ego_shm_ring_detach(&cons);
    miro_ego_shm_ring_detach(&prod);
    close_fds(shm_fd, kick_fd, done_fd);
}

typedef struct {
    miro_ego_shm_ring_t *ring;
    uint32_t frames;
    uint32_t frame_bytes;
} bench_args_t;

static void *bench_producer(void *arg)
{
    bench_args_t *b = (bench_args_t *)arg;
    uint8_t *buf = malloc(b->frame_bytes);
    assert(buf != NULL);
    memset(buf, 0xa5, b->frame_bytes);

    for (uint32_t i = 0; i < b->frames; i += 1) {
        buf[0] = (uint8_t)(i & 0xffu);
        buf[b->frame_bytes - 1u] = (uint8_t)((i * 7u) & 0xffu);
        assert(miro_ego_shm_ring_push(b->ring, buf, b->frame_bytes, i, i, 0u, 5000) ==
               MIRO_EGO_SHM_OK);
    }
    assert(miro_ego_shm_ring_set_eof(b->ring) == MIRO_EGO_SHM_OK);
    free(buf);
    return NULL;
}

static void test_throughput_benchmark(void)
{
    int shm_fd, kick_fd, done_fd;
    make_fds(&shm_fd, &kick_fd, &done_fd, EFD_NONBLOCK);

    const uint32_t slot_count = 16u;
    const uint32_t frame_bytes = 256u * 1024u; /* 256 KiB simulated stereo JPEG */
    const uint32_t total_frames = 512u;        /* 128 MiB total */

    miro_ego_shm_ring_t producer, consumer;
    char err[128] = {0};
    assert(miro_ego_shm_ring_init_fd(&producer, shm_fd, kick_fd, done_fd,
                                     slot_count, frame_bytes, err, sizeof(err)) == 0);
    assert(miro_ego_shm_ring_attach(&consumer, shm_fd, kick_fd, done_fd,
                                    err, sizeof(err)) == 0);

    bench_args_t args = {.ring = &producer, .frames = total_frames, .frame_bytes = frame_bytes};
    uint64_t t0 = now_ns();
    pthread_t prod_thread;
    assert(pthread_create(&prod_thread, NULL, bench_producer, &args) == 0);

    uint32_t count = 0;
    uint64_t total_bytes = 0;
    for (;;) {
        miro_ego_shm_frame_t frame;
        miro_ego_shm_status_t st = miro_ego_shm_ring_receive(&consumer, &frame, 5000);
        if (st == MIRO_EGO_SHM_EOF) {
            break;
        }
        assert(st == MIRO_EGO_SHM_OK);
        assert(frame.payload_len == frame_bytes);
        assert(frame.data[0] == (uint8_t)(count & 0xffu));
        assert(frame.data[frame_bytes - 1u] == (uint8_t)((count * 7u) & 0xffu));
        total_bytes += frame.payload_len;
        assert(miro_ego_shm_ring_release(&consumer) == MIRO_EGO_SHM_OK);
        count += 1u;
    }
    pthread_join(prod_thread, NULL);
    uint64_t elapsed_ns = now_ns() - t0;
    assert(count == total_frames);

    double secs = (double)elapsed_ns / 1e9;
    double mib = (double)total_bytes / (1024.0 * 1024.0);
    double mib_s = secs > 0.0 ? mib / secs : 0.0;
    double fps = secs > 0.0 ? (double)count / secs : 0.0;
    printf("  shm_ring benchmark: %u frames (%.1f MiB) in %.3f s -> %.0f fps, %.1f MiB/s\n",
           count, mib, secs, fps, mib_s);

    miro_ego_shm_ring_detach(&consumer);
    miro_ego_shm_ring_detach(&producer);
    close_fds(shm_fd, kick_fd, done_fd);
}

int main(void)
{
    test_binary_layout_offsets();
    test_mt_wraparound(0);
    test_mt_wraparound(EFD_NONBLOCK);
    test_eof_drain_and_backpressure();
    test_bounds_and_corruption_rejection();
    test_sync_bundle_roundtrip_and_bounds();
    test_throughput_benchmark();
    printf("test_shm_ring: PASS (layout + wraparound + backpressure + EOF drain + bounds + bench)\n");
    return 0;
}

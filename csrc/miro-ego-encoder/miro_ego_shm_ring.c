#define _GNU_SOURCE
#include "miro_ego_shm_ring.h"

#include <errno.h>
#include <poll.h>
#include <stdarg.h>
#include <stdio.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

static void set_error(char *error, size_t error_len, const char *fmt, ...)
    __attribute__((format(printf, 3, 4)));

static void set_error(char *error, size_t error_len, const char *fmt, ...)
{
    if (error == NULL || error_len == 0) {
        return;
    }
    va_list args;
    va_start(args, fmt);
    vsnprintf(error, error_len, fmt, args);
    va_end(args);
}

static int is_valid_slot_count(uint32_t slot_count)
{
    return slot_count >= MIRO_EGO_SHM_MIN_SLOTS &&
           slot_count <= MIRO_EGO_SHM_MAX_SLOTS &&
           (slot_count & (slot_count - 1u)) == 0u;
}

uint64_t miro_ego_shm_required_size(uint32_t slot_count, uint32_t slot_capacity)
{
    if (!is_valid_slot_count(slot_count) ||
        slot_capacity == 0u ||
        slot_capacity > MIRO_EGO_SHM_MAX_SLOT_CAPACITY) {
        return 0u;
    }
    const uint64_t data_offset = miro_ego_shm_data_offset(slot_count);
    const uint64_t data_bytes = (uint64_t)slot_count * (uint64_t)slot_capacity;
    if (data_bytes > UINT64_MAX - data_offset) {
        return 0u;
    }
    return data_offset + data_bytes;
}

static int signal_eventfd(int fd)
{
    const uint64_t one = 1ULL;
    for (;;) {
        ssize_t written = write(fd, &one, sizeof(one));
        if (written == (ssize_t)sizeof(one)) {
            return 0;
        }
        if (written < 0) {
            if (errno == EINTR) {
                continue;
            }
            if (errno == EAGAIN) {
                /* Non-blocking eventfd counter already saturated; peer is woken. */
                return 0;
            }
            return -1;
        }
        errno = EIO;
        return -1;
    }
}

static int wait_eventfd(int fd, int timeout_ms)
{
    struct pollfd pfd;
    pfd.fd = fd;
    pfd.events = POLLIN;
    pfd.revents = 0;

    struct timespec deadline = {0};
    if (timeout_ms > 0) {
        if (clock_gettime(CLOCK_MONOTONIC, &deadline) != 0) {
            return -1;
        }
        deadline.tv_sec += timeout_ms / 1000;
        deadline.tv_nsec += (long)(timeout_ms % 1000) * 1000000L;
        if (deadline.tv_nsec >= 1000000000L) {
            deadline.tv_sec += 1;
            deadline.tv_nsec -= 1000000000L;
        }
    }

    int ret;
    for (;;) {
        int wait_ms = timeout_ms;
        if (timeout_ms > 0) {
            struct timespec now = {0};
            if (clock_gettime(CLOCK_MONOTONIC, &now) != 0) {
                return -1;
            }
            long long rem_ns =
                ((long long)deadline.tv_sec - (long long)now.tv_sec) * 1000000000LL +
                ((long long)deadline.tv_nsec - (long long)now.tv_nsec);
            if (rem_ns <= 0) {
                ret = 0;
                break;
            }
            long long rem_ms = (rem_ns + 999999LL) / 1000000LL;
            wait_ms = rem_ms > (long long)INT32_MAX ? INT32_MAX : (int)rem_ms;
        }
        pfd.revents = 0;
        ret = poll(&pfd, 1, wait_ms);
        if (ret >= 0 || errno != EINTR) {
            break;
        }
    }

    if (ret < 0) {
        return -1;
    }
    if (ret == 0) {
        return 0; /* timeout */
    }
    if ((pfd.revents & (POLLERR | POLLHUP | POLLNVAL)) != 0) {
        errno = EIO;
        return -1;
    }

    uint64_t value = 0;
    for (;;) {
        ssize_t got = read(fd, &value, sizeof(value));
        if (got == (ssize_t)sizeof(value)) {
            return 1;
        }
        if (got < 0) {
            if (errno == EINTR) {
                continue;
            }
            if (errno == EAGAIN) {
                /* Already drained on a non-blocking eventfd; re-check indices. */
                return 1;
            }
            return -1;
        }
        errno = EIO;
        return -1;
    }
}

int miro_ego_shm_ring_attach(miro_ego_shm_ring_t *ring, int shm_fd, int kick_fd,
                             int done_fd, char *error, size_t error_len)
{
    if (ring == NULL || shm_fd < 0 || kick_fd < 0 || done_fd < 0) {
        set_error(error, error_len, "invalid_shm_fd");
        return -1;
    }
    if (shm_fd == kick_fd || shm_fd == done_fd || kick_fd == done_fd) {
        set_error(error, error_len, "duplicate_shm_fd");
        return -1;
    }
    memset(ring, 0, sizeof(*ring));
    ring->shm_fd = -1;
    ring->kick_fd = -1;
    ring->done_fd = -1;

    struct stat st;
    memset(&st, 0, sizeof(st));
    if (fstat(shm_fd, &st) != 0) {
        set_error(error, error_len, "shm_fstat_failed: %s", strerror(errno));
        return -1;
    }
    if (st.st_size < (off_t)MIRO_EGO_SHM_HEADER_SIZE) {
        set_error(error, error_len, "shm_too_small: size=%lld", (long long)st.st_size);
        return -1;
    }
    if ((uint64_t)st.st_size > (uint64_t)SIZE_MAX) {
        set_error(error, error_len, "shm_size_overflow");
        return -1;
    }

    const size_t map_size = (size_t)st.st_size;
    void *mapped = mmap(NULL, map_size, PROT_READ | PROT_WRITE, MAP_SHARED, shm_fd, 0);
    if (mapped == MAP_FAILED) {
        set_error(error, error_len, "shm_mmap_failed: %s", strerror(errno));
        return -1;
    }

    miro_ego_shm_header_t *hdr = (miro_ego_shm_header_t *)mapped;
    if (memcmp(hdr->magic, MIRO_EGO_SHM_MAGIC, MIRO_EGO_SHM_MAGIC_LEN) != 0) {
        set_error(error, error_len, "shm_bad_magic");
        munmap(mapped, map_size);
        return -1;
    }
    if (hdr->version != MIRO_EGO_SHM_VERSION) {
        set_error(error, error_len, "shm_unsupported_version: %u", hdr->version);
        munmap(mapped, map_size);
        return -1;
    }

    const uint32_t slot_count = hdr->slot_count;
    const uint32_t slot_capacity = hdr->slot_capacity;
    if (!is_valid_slot_count(slot_count)) {
        set_error(error, error_len, "shm_invalid_slot_count: %u", slot_count);
        munmap(mapped, map_size);
        return -1;
    }
    if (slot_capacity == 0u || slot_capacity > MIRO_EGO_SHM_MAX_SLOT_CAPACITY) {
        set_error(error, error_len, "shm_invalid_slot_capacity: %u", slot_capacity);
        munmap(mapped, map_size);
        return -1;
    }

    const uint64_t required = miro_ego_shm_required_size(slot_count, slot_capacity);
    if (required == 0u || (uint64_t)map_size < required) {
        set_error(error, error_len, "shm_truncated: map_size=%zu required=%llu",
                  map_size, (unsigned long long)required);
        munmap(mapped, map_size);
        return -1;
    }

    const uint64_t read_idx = atomic_load_explicit(&hdr->read_idx, memory_order_acquire);
    const uint64_t write_idx = atomic_load_explicit(&hdr->write_idx, memory_order_acquire);
    if (write_idx < read_idx || (write_idx - read_idx) > (uint64_t)slot_count) {
        set_error(error, error_len, "shm_corrupt_indices: read=%llu write=%llu slots=%u",
                  (unsigned long long)read_idx, (unsigned long long)write_idx, slot_count);
        munmap(mapped, map_size);
        return -1;
    }

    ring->base = (uint8_t *)mapped;
    ring->map_size = map_size;
    ring->header = hdr;
    ring->slots = (miro_ego_shm_slot_t *)(ring->base + MIRO_EGO_SHM_HEADER_SIZE);
    ring->slot_count = slot_count;
    ring->slot_mask = slot_count - 1u;
    ring->slot_capacity = slot_capacity;
    ring->data_offset = miro_ego_shm_data_offset(slot_count);
    ring->shm_fd = shm_fd;
    ring->kick_fd = kick_fd;
    ring->done_fd = done_fd;
    ring->in_flight = 0;
    return 0;
}

void miro_ego_shm_ring_detach(miro_ego_shm_ring_t *ring)
{
    if (ring == NULL) {
        return;
    }
    if (ring->base != NULL && ring->map_size > 0) {
        munmap(ring->base, ring->map_size);
    }
    memset(ring, 0, sizeof(*ring));
    ring->shm_fd = -1;
    ring->kick_fd = -1;
    ring->done_fd = -1;
}

miro_ego_shm_status_t miro_ego_shm_ring_receive(miro_ego_shm_ring_t *ring,
                                                miro_ego_shm_frame_t *frame,
                                                int timeout_ms)
{
    if (ring == NULL || ring->header == NULL || frame == NULL || ring->in_flight) {
        return MIRO_EGO_SHM_INVALID_ARG;
    }

    for (;;) {
        const uint64_t read_idx =
            atomic_load_explicit(&ring->header->read_idx, memory_order_relaxed);
        const uint64_t write_idx =
            atomic_load_explicit(&ring->header->write_idx, memory_order_acquire);

        if (write_idx < read_idx || (write_idx - read_idx) > (uint64_t)ring->slot_count) {
            return MIRO_EGO_SHM_CORRUPT;
        }

        if (read_idx < write_idx) {
            const uint64_t slot_idx = read_idx & (uint64_t)ring->slot_mask;
            const miro_ego_shm_slot_t desc = ring->slots[slot_idx];
            const uint64_t expected_offset =
                ring->data_offset + slot_idx * (uint64_t)ring->slot_capacity;

            if (desc.payload_len == 0u ||
                desc.payload_len > ring->slot_capacity ||
                desc.data_offset != expected_offset ||
                expected_offset > (uint64_t)ring->map_size ||
                (uint64_t)desc.payload_len > (uint64_t)ring->map_size - expected_offset) {
                return MIRO_EGO_SHM_CORRUPT;
            }

            frame->data = ring->base + desc.data_offset;
            frame->payload_len = desc.payload_len;
            frame->flags = desc.flags;
            frame->frame_seq = desc.frame_seq;
            frame->monotonic_ns = desc.monotonic_ns;
            frame->slot_index = slot_idx;
            ring->in_flight = 1;
            return MIRO_EGO_SHM_OK;
        }

        const uint32_t flags =
            atomic_load_explicit(&ring->header->flags, memory_order_acquire);
        if ((flags & MIRO_EGO_SHM_FLAG_EOF) != 0u) {
            /* Re-check write_idx after observing EOF so no trailing frame is missed. */
            const uint64_t final_write =
                atomic_load_explicit(&ring->header->write_idx, memory_order_acquire);
            if (final_write < read_idx ||
                (final_write - read_idx) > (uint64_t)ring->slot_count) {
                return MIRO_EGO_SHM_CORRUPT;
            }
            if (read_idx < final_write) {
                continue;
            }
            return MIRO_EGO_SHM_EOF;
        }

        int wait_ret = wait_eventfd(ring->kick_fd, timeout_ms);
        if (wait_ret < 0) {
            return MIRO_EGO_SHM_IO_ERROR;
        }
        if (wait_ret == 0) {
            /* Final check before reporting timeout in case producer raced poll(). */
            const uint64_t check_write =
                atomic_load_explicit(&ring->header->write_idx, memory_order_acquire);
            const uint32_t check_flags =
                atomic_load_explicit(&ring->header->flags, memory_order_acquire);
            if (check_write != read_idx || (check_flags & MIRO_EGO_SHM_FLAG_EOF) != 0u) {
                continue;
            }
            return MIRO_EGO_SHM_TIMEOUT;
        }
    }
}

miro_ego_shm_status_t miro_ego_shm_ring_release(miro_ego_shm_ring_t *ring)
{
    if (ring == NULL || ring->header == NULL || !ring->in_flight) {
        return MIRO_EGO_SHM_INVALID_ARG;
    }
    const uint64_t read_idx =
        atomic_load_explicit(&ring->header->read_idx, memory_order_relaxed);
    atomic_store_explicit(&ring->header->read_idx, read_idx + 1ULL, memory_order_release);
    ring->in_flight = 0;

    if (signal_eventfd(ring->done_fd) != 0) {
        return MIRO_EGO_SHM_IO_ERROR;
    }
    return MIRO_EGO_SHM_OK;
}

uint64_t miro_ego_shm_ring_dropped(const miro_ego_shm_ring_t *ring)
{
    if (ring == NULL || ring->header == NULL) {
        return 0u;
    }
    return atomic_load_explicit(&ring->header->dropped_frames, memory_order_relaxed);
}

int miro_ego_shm_ring_init_fd(miro_ego_shm_ring_t *ring, int shm_fd, int kick_fd,
                              int done_fd, uint32_t slot_count, uint32_t slot_capacity,
                              char *error, size_t error_len)
{
    if (ring == NULL || shm_fd < 0 || kick_fd < 0 || done_fd < 0) {
        set_error(error, error_len, "invalid_shm_fd");
        return -1;
    }
    const uint64_t total_size = miro_ego_shm_required_size(slot_count, slot_capacity);
    if (total_size == 0u || total_size > (uint64_t)SIZE_MAX) {
        set_error(error, error_len, "invalid_shm_geometry: slots=%u capacity=%u",
                  slot_count, slot_capacity);
        return -1;
    }
    if (ftruncate(shm_fd, (off_t)total_size) != 0) {
        set_error(error, error_len, "shm_ftruncate_failed: %s", strerror(errno));
        return -1;
    }

    void *mapped = mmap(NULL, (size_t)total_size, PROT_READ | PROT_WRITE, MAP_SHARED, shm_fd, 0);
    if (mapped == MAP_FAILED) {
        set_error(error, error_len, "shm_mmap_failed: %s", strerror(errno));
        return -1;
    }
    memset(mapped, 0, MIRO_EGO_SHM_HEADER_SIZE + (size_t)slot_count * MIRO_EGO_SHM_SLOT_SIZE);

    miro_ego_shm_header_t *hdr = (miro_ego_shm_header_t *)mapped;
    memcpy(hdr->magic, MIRO_EGO_SHM_MAGIC, MIRO_EGO_SHM_MAGIC_LEN);
    hdr->version = MIRO_EGO_SHM_VERSION;
    hdr->slot_count = slot_count;
    hdr->slot_capacity = slot_capacity;
    atomic_init(&hdr->flags, 0u);
    atomic_init(&hdr->write_idx, 0ULL);
    atomic_init(&hdr->read_idx, 0ULL);
    atomic_init(&hdr->dropped_frames, 0ULL);

    const uint64_t data_offset = miro_ego_shm_data_offset(slot_count);
    miro_ego_shm_slot_t *slots =
        (miro_ego_shm_slot_t *)((uint8_t *)mapped + MIRO_EGO_SHM_HEADER_SIZE);
    for (uint32_t i = 0; i < slot_count; i += 1) {
        slots[i].data_offset = data_offset + (uint64_t)i * (uint64_t)slot_capacity;
    }

    munmap(mapped, (size_t)total_size);
    return miro_ego_shm_ring_attach(ring, shm_fd, kick_fd, done_fd, error, error_len);
}

miro_ego_shm_status_t miro_ego_shm_ring_push(miro_ego_shm_ring_t *ring,
                                             const uint8_t *payload,
                                             uint32_t payload_len,
                                             uint64_t frame_seq,
                                             uint64_t monotonic_ns,
                                             uint32_t flags,
                                             int timeout_ms)
{
    if (ring == NULL || ring->header == NULL || payload == NULL ||
        payload_len == 0u || payload_len > ring->slot_capacity) {
        return MIRO_EGO_SHM_INVALID_ARG;
    }

    for (;;) {
        const uint32_t hdr_flags =
            atomic_load_explicit(&ring->header->flags, memory_order_acquire);
        if ((hdr_flags & MIRO_EGO_SHM_FLAG_EOF) != 0u) {
            return MIRO_EGO_SHM_EOF;
        }
        const uint64_t write_idx =
            atomic_load_explicit(&ring->header->write_idx, memory_order_relaxed);
        const uint64_t read_idx =
            atomic_load_explicit(&ring->header->read_idx, memory_order_acquire);

        if (write_idx < read_idx || (write_idx - read_idx) > (uint64_t)ring->slot_count) {
            return MIRO_EGO_SHM_CORRUPT;
        }
        if ((write_idx - read_idx) < (uint64_t)ring->slot_count) {
            const uint64_t slot_idx = write_idx & (uint64_t)ring->slot_mask;
            const uint64_t offset =
                ring->data_offset + slot_idx * (uint64_t)ring->slot_capacity;
            memcpy(ring->base + offset, payload, payload_len);

            miro_ego_shm_slot_t *slot = &ring->slots[slot_idx];
            slot->frame_seq = frame_seq;
            slot->monotonic_ns = monotonic_ns;
            slot->payload_len = payload_len;
            slot->flags = flags;
            slot->data_offset = offset;
            memset(slot->reserved, 0, sizeof(slot->reserved));

            atomic_store_explicit(&ring->header->write_idx, write_idx + 1ULL,
                                  memory_order_release);
            if (signal_eventfd(ring->kick_fd) != 0) {
                return MIRO_EGO_SHM_IO_ERROR;
            }
            return MIRO_EGO_SHM_OK;
        }

        if (timeout_ms == 0) {
            atomic_fetch_add_explicit(&ring->header->dropped_frames, 1ULL,
                                      memory_order_relaxed);
            return MIRO_EGO_SHM_FULL;
        }

        int wait_ret = wait_eventfd(ring->done_fd, timeout_ms);
        if (wait_ret < 0) {
            return MIRO_EGO_SHM_IO_ERROR;
        }
        if (wait_ret == 0) {
            const uint64_t check_read =
                atomic_load_explicit(&ring->header->read_idx, memory_order_acquire);
            if ((write_idx - check_read) < (uint64_t)ring->slot_count) {
                continue;
            }
            atomic_fetch_add_explicit(&ring->header->dropped_frames, 1ULL,
                                      memory_order_relaxed);
            return MIRO_EGO_SHM_TIMEOUT;
        }
    }
}

miro_ego_shm_status_t miro_ego_shm_ring_set_eof(miro_ego_shm_ring_t *ring)
{
    if (ring == NULL || ring->header == NULL) {
        return MIRO_EGO_SHM_INVALID_ARG;
    }
    atomic_fetch_or_explicit(&ring->header->flags, MIRO_EGO_SHM_FLAG_EOF,
                             memory_order_release);
    if (signal_eventfd(ring->kick_fd) != 0) {
        return MIRO_EGO_SHM_IO_ERROR;
    }
    return MIRO_EGO_SHM_OK;
}

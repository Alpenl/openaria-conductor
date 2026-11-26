/*
 * Zero-copy memfd + eventfd shared-memory ring buffer (MEGO_SHM v1).
 *
 * Binary layout matches Rust `miro-ego-capture::session::shm_ring` byte-for-byte:
 *   - Header (4096 bytes, little-endian, 64-byte aligned atomics):
 *       0..8   : magic[8] = "MEGO_SHM"
 *       8..12  : uint32_t version = 1
 *       12..16 : uint32_t slot_count (power of 2, 1..=256)
 *       16..20 : uint32_t slot_capacity
 *       20..24 : _Atomic uint32_t flags (bit 0 = EOF/shutdown)
 *       64..72 : _Atomic uint64_t write_idx (producer monotonic)
 *      128..136: _Atomic uint64_t read_idx  (consumer monotonic)
 *      192..200: _Atomic uint64_t dropped_frames
 *   - Slot descriptor table at offset 4096, each 64 bytes:
 *       0..8   : uint64_t frame_seq
 *       8..16  : uint64_t monotonic_ns
 *       16..20 : uint32_t payload_len
 *       20..24 : uint32_t flags
 *       24..32 : uint64_t data_offset
 *       32..64 : uint8_t reserved[32]
 *   - Data region starts right after slot table, 4096-byte aligned,
 *     spanning slot_count * slot_capacity bytes.
 */

#ifndef MIRO_EGO_SHM_RING_H
#define MIRO_EGO_SHM_RING_H

#include <stdatomic.h>
#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define MIRO_EGO_SHM_MAGIC "MEGO_SHM"
#define MIRO_EGO_SHM_MAGIC_LEN 8u
#define MIRO_EGO_SHM_VERSION 1u
#define MIRO_EGO_SHM_HEADER_SIZE 4096u
#define MIRO_EGO_SHM_SLOT_SIZE 64u
#define MIRO_EGO_SHM_PAGE_ALIGN 4096u
#define MIRO_EGO_SHM_MIN_SLOTS 1u
#define MIRO_EGO_SHM_MAX_SLOTS 256u
#define MIRO_EGO_SHM_MAX_SLOT_CAPACITY (64u * 1024u * 1024u)

#define MIRO_EGO_SHM_FLAG_EOF (1u << 0)

typedef struct {
    uint8_t magic[8];
    uint32_t version;
    uint32_t slot_count;
    uint32_t slot_capacity;
    _Atomic uint32_t flags;
    uint8_t _pad0[40];
    _Alignas(64) _Atomic uint64_t write_idx;
    uint8_t _pad1[56];
    _Alignas(64) _Atomic uint64_t read_idx;
    uint8_t _pad2[56];
    _Alignas(64) _Atomic uint64_t dropped_frames;
    uint8_t _pad3[MIRO_EGO_SHM_HEADER_SIZE - 200u];
} miro_ego_shm_header_t;

typedef struct {
    uint64_t frame_seq;
    uint64_t monotonic_ns;
    uint32_t payload_len;
    uint32_t flags;
    uint64_t data_offset;
    uint8_t reserved[32];
} miro_ego_shm_slot_t;

_Static_assert(sizeof(miro_ego_shm_header_t) == 4096u, "MEGO_SHM header must be 4096 bytes");
_Static_assert(offsetof(miro_ego_shm_header_t, magic) == 0u, "magic offset");
_Static_assert(offsetof(miro_ego_shm_header_t, version) == 8u, "version offset");
_Static_assert(offsetof(miro_ego_shm_header_t, slot_count) == 12u, "slot_count offset");
_Static_assert(offsetof(miro_ego_shm_header_t, slot_capacity) == 16u, "slot_capacity offset");
_Static_assert(offsetof(miro_ego_shm_header_t, flags) == 20u, "flags offset");
_Static_assert(offsetof(miro_ego_shm_header_t, write_idx) == 64u, "write_idx offset");
_Static_assert(offsetof(miro_ego_shm_header_t, read_idx) == 128u, "read_idx offset");
_Static_assert(offsetof(miro_ego_shm_header_t, dropped_frames) == 192u, "dropped_frames offset");

_Static_assert(sizeof(miro_ego_shm_slot_t) == 64u, "MEGO_SHM slot must be 64 bytes");
_Static_assert(offsetof(miro_ego_shm_slot_t, frame_seq) == 0u, "frame_seq offset");
_Static_assert(offsetof(miro_ego_shm_slot_t, monotonic_ns) == 8u, "monotonic_ns offset");
_Static_assert(offsetof(miro_ego_shm_slot_t, payload_len) == 16u, "payload_len offset");
_Static_assert(offsetof(miro_ego_shm_slot_t, flags) == 20u, "slot flags offset");
_Static_assert(offsetof(miro_ego_shm_slot_t, data_offset) == 24u, "data_offset offset");
_Static_assert(offsetof(miro_ego_shm_slot_t, reserved) == 32u, "reserved offset");

typedef enum {
    MIRO_EGO_SHM_OK = 0,
    MIRO_EGO_SHM_EOF = 1,
    MIRO_EGO_SHM_TIMEOUT = 2,
    MIRO_EGO_SHM_FULL = 3,
    MIRO_EGO_SHM_CORRUPT = -1,
    MIRO_EGO_SHM_IO_ERROR = -2,
    MIRO_EGO_SHM_INVALID_ARG = -3,
} miro_ego_shm_status_t;

/* Zero-copy borrowed view of the next ready slot payload. */
typedef struct {
    const uint8_t *data;
    uint32_t payload_len;
    uint32_t flags;
    uint64_t frame_seq;
    uint64_t monotonic_ns;
    uint64_t slot_index;
} miro_ego_shm_frame_t;

typedef struct {
    uint8_t *base;
    size_t map_size;
    miro_ego_shm_header_t *header;
    miro_ego_shm_slot_t *slots;
    uint32_t slot_count;
    uint32_t slot_mask;
    uint32_t slot_capacity;
    uint64_t data_offset;
    int shm_fd;
    int kick_fd;
    int done_fd;
    int in_flight; /* consumer currently holds a borrowed frame */
} miro_ego_shm_ring_t;

static inline uint64_t miro_ego_shm_data_offset(uint32_t slot_count)
{
    const uint64_t raw = (uint64_t)MIRO_EGO_SHM_HEADER_SIZE +
                         (uint64_t)slot_count * (uint64_t)MIRO_EGO_SHM_SLOT_SIZE;
    return (raw + (uint64_t)(MIRO_EGO_SHM_PAGE_ALIGN - 1u)) &
           ~(uint64_t)(MIRO_EGO_SHM_PAGE_ALIGN - 1u);
}

/*
 * Computes the minimum required SHM file size in bytes.
 * Returns 0 on invalid parameters or integer overflow.
 */
uint64_t miro_ego_shm_required_size(uint32_t slot_count, uint32_t slot_capacity);

/*
 * Consumer / shared attach to an already-initialized MEGO_SHM v1 memfd.
 * Strictly validates magic, version, power-of-2 slot_count (1..=256),
 * slot_capacity, fstat length, and initial ring indices.
 * Caller retains ownership of shm_fd, kick_fd, and done_fd.
 * Returns 0 on success, -1 on error (writing a diagnostic to error if provided).
 */
int miro_ego_shm_ring_attach(miro_ego_shm_ring_t *ring, int shm_fd, int kick_fd,
                             int done_fd, char *error, size_t error_len);

/* Unmaps the SHM mapping; does NOT close caller-owned file descriptors. */
void miro_ego_shm_ring_detach(miro_ego_shm_ring_t *ring);

/*
 * Consumer receive: waits up to timeout_ms (< 0 means wait indefinitely) for the
 * next frame. On MIRO_EGO_SHM_OK, populates *frame with a zero-copy pointer into
 * the mapped ring; the caller MUST call miro_ego_shm_ring_release() once done
 * reading frame->data before calling receive() again.
 */
miro_ego_shm_status_t miro_ego_shm_ring_receive(miro_ego_shm_ring_t *ring,
                                                miro_ego_shm_frame_t *frame,
                                                int timeout_ms);

/* Advances read_idx with release semantics and signals done_fd. */
miro_ego_shm_status_t miro_ego_shm_ring_release(miro_ego_shm_ring_t *ring);

/* Returns current dropped_frames counter from the shared header. */
uint64_t miro_ego_shm_ring_dropped(const miro_ego_shm_ring_t *ring);

/*
 * Producer helpers (used by host tests and C producers):
 * Initializes an empty memfd (ftruncate + header + slot offsets) and attaches.
 */
int miro_ego_shm_ring_init_fd(miro_ego_shm_ring_t *ring, int shm_fd, int kick_fd,
                              int done_fd, uint32_t slot_count, uint32_t slot_capacity,
                              char *error, size_t error_len);

/*
 * Producer push: waits up to timeout_ms (0 = non-blocking, < 0 = wait forever)
 * for a free slot via done_fd, copies payload into the slot, advances write_idx,
 * and signals kick_fd.
 */
miro_ego_shm_status_t miro_ego_shm_ring_push(miro_ego_shm_ring_t *ring,
                                             const uint8_t *payload,
                                             uint32_t payload_len,
                                             uint64_t frame_seq,
                                             uint64_t monotonic_ns,
                                             uint32_t flags,
                                             int timeout_ms);

/* Sets MIRO_EGO_SHM_FLAG_EOF in header->flags and wakes consumer via kick_fd. */
miro_ego_shm_status_t miro_ego_shm_ring_set_eof(miro_ego_shm_ring_t *ring);

#ifdef __cplusplus
}
#endif

#endif /* MIRO_EGO_SHM_RING_H */

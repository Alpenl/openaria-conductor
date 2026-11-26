/*
 * Self-contained FIPS 180-4 SHA-256 implementation for Miro-Ego encoder.
 *
 * Zero external crypto dependencies. Supports incremental streaming updates
 * and page-aligned 256 KiB file hashing for hot page-cache segment sealing.
 */

#ifndef MIRO_EGO_SHA256_H
#define MIRO_EGO_SHA256_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define MIRO_EGO_SHA256_DIGEST_LEN 32u
#define MIRO_EGO_SHA256_HEX_LEN 65u /* 64 lowercase hex chars + NUL */
#define MIRO_EGO_SHA256_IO_BUF_SIZE (256u * 1024u)
#define MIRO_EGO_SHA256_IO_ALIGN 4096u

typedef struct {
    uint32_t state[8];
    uint64_t total_bytes;
    uint8_t block[64];
    size_t block_len;
} miro_ego_sha256_ctx_t;

void miro_ego_sha256_init(miro_ego_sha256_ctx_t *ctx);
void miro_ego_sha256_update(miro_ego_sha256_ctx_t *ctx, const void *data, size_t len);
void miro_ego_sha256_final(miro_ego_sha256_ctx_t *ctx,
                           uint8_t digest[MIRO_EGO_SHA256_DIGEST_LEN]);
void miro_ego_sha256_to_hex(const uint8_t digest[MIRO_EGO_SHA256_DIGEST_LEN],
                            char hex_out[MIRO_EGO_SHA256_HEX_LEN]);

/*
 * Hash an open file descriptor from offset 0 using pread() and a caller-supplied
 * buffer (typically 256 KiB, 4096-byte aligned). Does not modify the file offset.
 * Returns 0 on success, -1 on error (preserving errno).
 */
int miro_ego_sha256_fd_with_buf(int fd, void *buf, size_t buf_len,
                                char hex_out[MIRO_EGO_SHA256_HEX_LEN],
                                unsigned long long *bytes_out);

/*
 * Convenience wrapper that allocates a 256 KiB 4096-byte-aligned read buffer,
 * hashes fd from offset 0, and frees the buffer.
 * Returns 0 on success, -1 on error (preserving errno).
 */
int miro_ego_sha256_fd(int fd, char hex_out[MIRO_EGO_SHA256_HEX_LEN],
                       unsigned long long *bytes_out);

#ifdef __cplusplus
}
#endif

#endif /* MIRO_EGO_SHA256_H */

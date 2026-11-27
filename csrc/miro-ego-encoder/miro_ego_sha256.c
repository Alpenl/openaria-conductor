#define _GNU_SOURCE
#include "miro_ego_sha256.h"

#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

static const uint32_t SHA256_K[64] = {
    0x428a2f98u, 0x71374491u, 0xb5c0fbcfu, 0xe9b5dba5u,
    0x3956c25bu, 0x59f111f1u, 0x923f82a4u, 0xab1c5ed5u,
    0xd807aa98u, 0x12835b01u, 0x243185beu, 0x550c7dc3u,
    0x72be5d74u, 0x80deb1feu, 0x9bdc06a7u, 0xc19bf174u,
    0xe49b69c1u, 0xefbe4786u, 0x0fc19dc6u, 0x240ca1ccu,
    0x2de92c6fu, 0x4a7484aau, 0x5cb0a9dcu, 0x76f988dau,
    0x983e5152u, 0xa831c66du, 0xb00327c8u, 0xbf597fc7u,
    0xc6e00bf3u, 0xd5a79147u, 0x06ca6351u, 0x14292967u,
    0x27b70a85u, 0x2e1b2138u, 0x4d2c6dfcu, 0x53380d13u,
    0x650a7354u, 0x766a0abbu, 0x81c2c92eu, 0x92722c85u,
    0xa2bfe8a1u, 0xa81a664bu, 0xc24b8b70u, 0xc76c51a3u,
    0xd192e819u, 0xd6990624u, 0xf40e3585u, 0x106aa070u,
    0x19a4c116u, 0x1e376c08u, 0x2748774cu, 0x34b0bcb5u,
    0x391c0cb3u, 0x4ed8aa4au, 0x5b9cca4fu, 0x682e6ff3u,
    0x748f82eeu, 0x78a5636fu, 0x84c87814u, 0x8cc70208u,
    0x90befffau, 0xa4506cebu, 0xbef9a3f7u, 0xc67178f2u,
};

static inline uint32_t rotr32(uint32_t value, unsigned shift)
{
    return (value >> shift) | (value << (32u - shift));
}

static inline uint32_t load_be32(const uint8_t *p)
{
    return ((uint32_t)p[0] << 24) | ((uint32_t)p[1] << 16) |
           ((uint32_t)p[2] << 8) | (uint32_t)p[3];
}

static inline void store_be32(uint8_t *p, uint32_t v)
{
    p[0] = (uint8_t)(v >> 24);
    p[1] = (uint8_t)(v >> 16);
    p[2] = (uint8_t)(v >> 8);
    p[3] = (uint8_t)v;
}

static void sha256_transform(uint32_t state[8], const uint8_t block[64])
{
    uint32_t w[64];
    for (int i = 0; i < 16; i += 1) {
        w[i] = load_be32(block + (size_t)i * 4u);
    }
    for (int i = 16; i < 64; i += 1) {
        const uint32_t s0 = rotr32(w[i - 15], 7) ^ rotr32(w[i - 15], 18) ^ (w[i - 15] >> 3);
        const uint32_t s1 = rotr32(w[i - 2], 17) ^ rotr32(w[i - 2], 19) ^ (w[i - 2] >> 10);
        w[i] = w[i - 16] + s0 + w[i - 7] + s1;
    }

    uint32_t a = state[0];
    uint32_t b = state[1];
    uint32_t c = state[2];
    uint32_t d = state[3];
    uint32_t e = state[4];
    uint32_t f = state[5];
    uint32_t g = state[6];
    uint32_t h = state[7];

    for (int i = 0; i < 64; i += 1) {
        const uint32_t S1 = rotr32(e, 6) ^ rotr32(e, 11) ^ rotr32(e, 25);
        const uint32_t ch = (e & f) ^ ((~e) & g);
        const uint32_t temp1 = h + S1 + ch + SHA256_K[i] + w[i];
        const uint32_t S0 = rotr32(a, 2) ^ rotr32(a, 13) ^ rotr32(a, 22);
        const uint32_t maj = (a & b) ^ (a & c) ^ (b & c);
        const uint32_t temp2 = S0 + maj;

        h = g;
        g = f;
        f = e;
        e = d + temp1;
        d = c;
        c = b;
        b = a;
        a = temp1 + temp2;
    }

    state[0] += a;
    state[1] += b;
    state[2] += c;
    state[3] += d;
    state[4] += e;
    state[5] += f;
    state[6] += g;
    state[7] += h;
}

void miro_ego_sha256_init(miro_ego_sha256_ctx_t *ctx)
{
    ctx->state[0] = 0x6a09e667u;
    ctx->state[1] = 0xbb67ae85u;
    ctx->state[2] = 0x3c6ef372u;
    ctx->state[3] = 0xa54ff53au;
    ctx->state[4] = 0x510e527fu;
    ctx->state[5] = 0x9b05688cu;
    ctx->state[6] = 0x1f83d9abu;
    ctx->state[7] = 0x5be0cd19u;
    ctx->total_bytes = 0;
    ctx->block_len = 0;
}

void miro_ego_sha256_update(miro_ego_sha256_ctx_t *ctx, const void *data, size_t len)
{
    if (len == 0 || data == NULL) {
        return;
    }
    const uint8_t *input = (const uint8_t *)data;
    ctx->total_bytes += (uint64_t)len;

    if (ctx->block_len > 0) {
        const size_t space = 64u - ctx->block_len;
        const size_t take = len < space ? len : space;
        memcpy(ctx->block + ctx->block_len, input, take);
        ctx->block_len += take;
        input += take;
        len -= take;
        if (ctx->block_len == 64u) {
            sha256_transform(ctx->state, ctx->block);
            ctx->block_len = 0;
        }
    }

    while (len >= 64u) {
        sha256_transform(ctx->state, input);
        input += 64u;
        len -= 64u;
    }

    if (len > 0) {
        memcpy(ctx->block, input, len);
        ctx->block_len = len;
    }
}

void miro_ego_sha256_final(miro_ego_sha256_ctx_t *ctx,
                           uint8_t digest[MIRO_EGO_SHA256_DIGEST_LEN])
{
    const uint64_t bit_len = ctx->total_bytes * 8ULL;

    ctx->block[ctx->block_len++] = 0x80u;
    if (ctx->block_len > 56u) {
        memset(ctx->block + ctx->block_len, 0, 64u - ctx->block_len);
        sha256_transform(ctx->state, ctx->block);
        ctx->block_len = 0;
    }
    memset(ctx->block + ctx->block_len, 0, 56u - ctx->block_len);

    store_be32(ctx->block + 56u, (uint32_t)(bit_len >> 32));
    store_be32(ctx->block + 60u, (uint32_t)(bit_len & 0xffffffffu));
    sha256_transform(ctx->state, ctx->block);

    for (int i = 0; i < 8; i += 1) {
        store_be32(digest + (size_t)i * 4u, ctx->state[i]);
    }
}

void miro_ego_sha256_to_hex(const uint8_t digest[MIRO_EGO_SHA256_DIGEST_LEN],
                            char hex_out[MIRO_EGO_SHA256_HEX_LEN])
{
    static const char HEX[] = "0123456789abcdef";
    for (size_t i = 0; i < MIRO_EGO_SHA256_DIGEST_LEN; i += 1) {
        hex_out[i * 2u] = HEX[digest[i] >> 4];
        hex_out[i * 2u + 1u] = HEX[digest[i] & 0x0fu];
    }
    hex_out[64] = '\0';
}

int miro_ego_sha256_update_fd_range(miro_ego_sha256_ctx_t *ctx, int fd,
                                    off_t offset, uint64_t length,
                                    void *buf, size_t buf_len)
{
    if (ctx == NULL || fd < 0 || offset < 0 || buf == NULL || buf_len == 0) {
        errno = EINVAL;
        return -1;
    }
    uint64_t remaining = length;
    while (remaining > 0) {
        size_t want = remaining < (uint64_t)buf_len ? (size_t)remaining : buf_len;
        ssize_t got = pread(fd, buf, want, offset);
        if (got < 0) {
            if (errno == EINTR) {
                continue;
            }
            return -1;
        }
        if (got == 0) {
            errno = EPIPE;
            return -1;
        }
        miro_ego_sha256_update(ctx, buf, (size_t)got);
        offset += got;
        remaining -= (uint64_t)got;
    }
    return 0;
}

int miro_ego_sha256_fd_with_buf(int fd, void *buf, size_t buf_len,
                                char hex_out[MIRO_EGO_SHA256_HEX_LEN],
                                unsigned long long *bytes_out)
{
    if (fd < 0 || buf == NULL || buf_len == 0 || hex_out == NULL) {
        errno = EINVAL;
        return -1;
    }

    miro_ego_sha256_ctx_t ctx;
    miro_ego_sha256_init(&ctx);

    off_t offset = 0;
    unsigned long long total = 0;
    for (;;) {
        ssize_t got = pread(fd, buf, buf_len, offset);
        if (got < 0) {
            if (errno == EINTR) {
                continue;
            }
            return -1;
        }
        if (got == 0) {
            break;
        }
        miro_ego_sha256_update(&ctx, buf, (size_t)got);
        offset += got;
        total += (unsigned long long)got;
    }

    uint8_t digest[MIRO_EGO_SHA256_DIGEST_LEN];
    miro_ego_sha256_final(&ctx, digest);
    miro_ego_sha256_to_hex(digest, hex_out);
    if (bytes_out != NULL) {
        *bytes_out = total;
    }
    return 0;
}

int miro_ego_sha256_fd(int fd, char hex_out[MIRO_EGO_SHA256_HEX_LEN],
                       unsigned long long *bytes_out)
{
    void *buf = NULL;
    int alloc_err = posix_memalign(&buf, MIRO_EGO_SHA256_IO_ALIGN, MIRO_EGO_SHA256_IO_BUF_SIZE);
    if (alloc_err != 0 || buf == NULL) {
        errno = alloc_err != 0 ? alloc_err : ENOMEM;
        return -1;
    }
    int ret = miro_ego_sha256_fd_with_buf(fd, buf, MIRO_EGO_SHA256_IO_BUF_SIZE,
                                          hex_out, bytes_out);
    const int saved_errno = errno;
    free(buf);
    errno = saved_errno;
    return ret;
}

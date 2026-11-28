#include "miro_ego_flac.h"

#include <stdlib.h>
#include <string.h>

typedef struct {
    uint8_t *data;
    size_t len;
    size_t cap;
} flac_buf_t;

static int fbuf_reserve(flac_buf_t *b, size_t extra)
{
    if (b->len + extra <= b->cap) {
        return 0;
    }
    size_t ncap = b->cap ? b->cap * 2u : 4096u;
    while (ncap < b->len + extra) {
        if (ncap > (SIZE_MAX / 2u)) {
            return -1;
        }
        ncap *= 2u;
    }
    uint8_t *next = realloc(b->data, ncap);
    if (next == NULL) {
        return -1;
    }
    b->data = next;
    b->cap = ncap;
    return 0;
}

static int fbuf_u8(flac_buf_t *b, uint8_t v)
{
    if (fbuf_reserve(b, 1u) != 0) {
        return -1;
    }
    b->data[b->len++] = v;
    return 0;
}

static int fbuf_u16(flac_buf_t *b, uint16_t v)
{
    if (fbuf_reserve(b, 2u) != 0) {
        return -1;
    }
    b->data[b->len++] = (uint8_t)(v >> 8);
    b->data[b->len++] = (uint8_t)v;
    return 0;
}

uint8_t miro_ego_flac_crc8(const uint8_t *data, size_t len)
{
    uint8_t crc = 0u;
    for (size_t i = 0; i < len; i += 1u) {
        crc ^= data[i];
        for (int bit = 0; bit < 8; bit += 1) {
            crc = (crc & 0x80u) ? (uint8_t)((crc << 1) ^ 0x07u) : (uint8_t)(crc << 1);
        }
    }
    return crc;
}

uint16_t miro_ego_flac_crc16(const uint8_t *data, size_t len)
{
    uint16_t crc = 0u;
    for (size_t i = 0; i < len; i += 1u) {
        crc ^= (uint16_t)data[i] << 8;
        for (int bit = 0; bit < 8; bit += 1) {
            crc = (crc & 0x8000u) ? (uint16_t)((crc << 1) ^ 0x8005u) : (uint16_t)(crc << 1);
        }
    }
    return crc;
}

typedef struct {
    flac_buf_t *out;
    uint64_t acc;
    int bits;
    int err;
} flac_bitw_t;

static void bw_put(flac_bitw_t *bw, uint32_t val, int nbits)
{
    if (bw->err != 0 || nbits <= 0) {
        return;
    }
    bw->acc = (bw->acc << nbits) | (uint64_t)(val & ((1ULL << nbits) - 1ULL));
    bw->bits += nbits;
    while (bw->bits >= 8) {
        bw->bits -= 8;
        if (fbuf_u8(bw->out, (uint8_t)(bw->acc >> bw->bits)) != 0) {
            bw->err = -1;
            return;
        }
    }
}

static void bw_align(flac_bitw_t *bw)
{
    if (bw->err != 0) {
        return;
    }
    if (bw->bits > 0) {
        const int pad = 8 - bw->bits;
        bw->acc <<= pad;
        if (fbuf_u8(bw->out, (uint8_t)bw->acc) != 0) {
            bw->err = -1;
            return;
        }
        bw->bits = 0;
        bw->acc = 0u;
    }
}

static int fbuf_utf8_u64(flac_buf_t *b, uint64_t v)
{
    if (v <= 0x7FULL) {
        return fbuf_u8(b, (uint8_t)v);
    }
    int bytes = 2;
    for (; bytes <= 7; bytes += 1) {
        const int total_bits = (7 - bytes) + 6 * (bytes - 1);
        if (v < (1ULL << total_bits)) {
            break;
        }
    }
    const int top_bits = 7 - bytes;
    const uint8_t top_mask = (uint8_t)(0xFFu << (8 - bytes));
    const int shift = 6 * (bytes - 1);
    if (fbuf_u8(b, (uint8_t)(top_mask | ((v >> shift) & ((1u << top_bits) - 1u)))) != 0) {
        return -1;
    }
    for (int k = bytes - 2; k >= 0; k -= 1) {
        if (fbuf_u8(b, (uint8_t)(0x80u | ((v >> (6 * k)) & 0x3Fu))) != 0) {
            return -1;
        }
    }
    return 0;
}

static void encode_subframe(flac_bitw_t *bw, const int16_t *ch, uint32_t n)
{
    if (n < 8u) {
        bw_put(bw, 0x02u, 8);
        for (uint32_t i = 0; i < n; i += 1u) {
            bw_put(bw, (uint16_t)ch[i], 16);
        }
        return;
    }

    uint64_t sum_abs = 0u;
    for (uint32_t i = 1; i < n; i += 1u) {
        const int32_t r = (int32_t)ch[i] - (int32_t)ch[i - 1u];
        sum_abs += (uint64_t)(r >= 0 ? r : -r);
    }
    const double mean_abs = (double)sum_abs / (double)(n - 1u);
    int k = 0;
    while (k < 14 && (double)(1U << k) < mean_abs * 1.4427) {
        k += 1;
    }

    uint64_t est_bits = 8ULL + 16ULL + 2ULL + 4ULL + 4ULL;
    int overflow = 0;
    for (uint32_t i = 1; i < n; i += 1u) {
        const int32_t r = (int32_t)ch[i] - (int32_t)ch[i - 1u];
        const uint32_t u = (r >= 0) ? (uint32_t)(r * 2) : (uint32_t)(-r * 2 - 1);
        const uint32_t q = u >> k;
        if (q > 1024u) {
            overflow = 1;
            break;
        }
        est_bits += (uint64_t)q + 1ULL + (uint64_t)k;
    }

    if (overflow || est_bits >= (uint64_t)n * 16ULL) {
        bw_put(bw, 0x02u, 8);
        for (uint32_t i = 0; i < n; i += 1u) {
            bw_put(bw, (uint16_t)ch[i], 16);
        }
        return;
    }

    bw_put(bw, 0x12u, 8); /* 0 | type=001001 (FIXED order 1) | wasted=0 */
    bw_put(bw, (uint16_t)ch[0], 16);
    bw_put(bw, 0u, 2); /* method 00: 4-bit Rice */
    bw_put(bw, 0u, 4); /* partition order 0 */
    bw_put(bw, (uint32_t)k, 4);
    for (uint32_t i = 1; i < n; i += 1u) {
        const int32_t r = (int32_t)ch[i] - (int32_t)ch[i - 1u];
        const uint32_t u = (r >= 0) ? (uint32_t)(r * 2) : (uint32_t)(-r * 2 - 1);
        uint32_t q = u >> k;
        while (q > 16u) {
            bw_put(bw, 0u, 16);
            q -= 16u;
        }
        if (q > 0u) {
            bw_put(bw, 0u, (int)q);
        }
        bw_put(bw, 1u, 1);
        if (k > 0) {
            bw_put(bw, u & ((1U << k) - 1U), k);
        }
    }
}

int miro_ego_flac_encode_frame(uint8_t **out_buf, size_t *out_cap, size_t *out_len,
                               const int16_t *interleaved_stereo, uint32_t blocksize,
                               uint64_t sample_number, int16_t *scratch_l,
                               int16_t *scratch_r)
{
    if (out_buf == NULL || out_cap == NULL || out_len == NULL ||
        interleaved_stereo == NULL || scratch_l == NULL || scratch_r == NULL ||
        blocksize == 0u || blocksize > 65535u || sample_number >= (1ULL << 36)) {
        return -1;
    }

    flac_buf_t b = { .data = *out_buf, .len = 0u, .cap = *out_cap };
    if (fbuf_u8(&b, 0xFFu) != 0 ||
        fbuf_u8(&b, 0xF9u) != 0 ||
        fbuf_u8(&b, 0x7Au) != 0 ||
        fbuf_u8(&b, 0x18u) != 0 ||
        fbuf_utf8_u64(&b, sample_number) != 0 ||
        fbuf_u16(&b, (uint16_t)(blocksize - 1u)) != 0 ||
        fbuf_u8(&b, miro_ego_flac_crc8(b.data, b.len)) != 0) {
        *out_buf = b.data;
        *out_cap = b.cap;
        return -1;
    }

    for (uint32_t i = 0; i < blocksize; i += 1u) {
        scratch_l[i] = interleaved_stereo[i * 2u];
        scratch_r[i] = interleaved_stereo[i * 2u + 1u];
    }

    flac_bitw_t bw = { .out = &b, .acc = 0u, .bits = 0, .err = 0 };
    encode_subframe(&bw, scratch_l, blocksize);
    encode_subframe(&bw, scratch_r, blocksize);
    bw_align(&bw);
    if (bw.err != 0 || fbuf_u16(&b, miro_ego_flac_crc16(b.data, b.len)) != 0) {
        *out_buf = b.data;
        *out_cap = b.cap;
        return -1;
    }

    *out_buf = b.data;
    *out_cap = b.cap;
    *out_len = b.len;
    return 0;
}

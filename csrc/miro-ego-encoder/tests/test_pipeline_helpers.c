#define _GNU_SOURCE
#include "miro_ego_cli_common.h"
#include "miro_ego_mp4_meta.h"
#include "miro_ego_packet_queue.h"
#include "miro_ego_split.h"

#include <assert.h>
#include <fcntl.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

static void put_be16(uint8_t *p, uint16_t v)
{
    p[0] = (uint8_t)(v >> 8);
    p[1] = (uint8_t)v;
}

static void put_be32(uint8_t *p, uint32_t v)
{
    p[0] = (uint8_t)(v >> 24);
    p[1] = (uint8_t)(v >> 16);
    p[2] = (uint8_t)(v >> 8);
    p[3] = (uint8_t)v;
}

static void put_be64(uint8_t *p, uint64_t v)
{
    put_be32(p, (uint32_t)(v >> 32));
    put_be32(p + 4, (uint32_t)(v & 0xffffffffu));
}

static uint32_t get_be32(const uint8_t *p)
{
    return ((uint32_t)p[0] << 24) | ((uint32_t)p[1] << 16) |
           ((uint32_t)p[2] << 8) | (uint32_t)p[3];
}

static uint64_t get_be64(const uint8_t *p)
{
    return ((uint64_t)get_be32(p) << 32) | (uint64_t)get_be32(p + 4);
}

static size_t make_box(uint8_t *out, const char type[4], const uint8_t *body, size_t len)
{
    put_be32(out, (uint32_t)(8u + len));
    memcpy(out + 4, type, 4);
    if (len > 0) {
        memcpy(out + 8, body, len);
    }
    return 8u + len;
}

/* ----------------------------------------------------------- NV12 split --- */

static void verify_nv12_split_case(int sbs_width, int height, int src_stride, int dst_stride)
{
    const int eye_width = sbs_width / 2;
    const size_t src_y_bytes = (size_t)src_stride * (size_t)height;
    const size_t src_uv_bytes = (size_t)src_stride * (size_t)(height / 2);
    const size_t dst_y_bytes = (size_t)dst_stride * (size_t)height;
    const size_t dst_uv_bytes = (size_t)dst_stride * (size_t)(height / 2);

    uint8_t *src_buf = calloc(1, src_y_bytes + src_uv_bytes);
    uint8_t *left_buf = calloc(1, dst_y_bytes + dst_uv_bytes);
    uint8_t *right_buf = calloc(1, dst_y_bytes + dst_uv_bytes);
    assert(src_buf != NULL && left_buf != NULL && right_buf != NULL);

    uint8_t *src_y = src_buf;
    uint8_t *src_uv = src_buf + src_y_bytes;

    for (int y = 0; y < height; y += 1) {
        for (int x = 0; x < sbs_width; x += 1) {
            const int eye = x >= eye_width ? 1 : 0;
            const int ex = eye ? (x - eye_width) : x;
            src_y[(size_t)y * (size_t)src_stride + (size_t)x] =
                (uint8_t)((eye * 113 + ex * 3 + y * 7) & 0xff);
        }
    }
    for (int y = 0; y < height / 2; y += 1) {
        for (int x = 0; x < sbs_width; x += 2) {
            const int eye = x >= eye_width ? 1 : 0;
            const int ex = eye ? (x - eye_width) : x;
            const size_t idx = (size_t)y * (size_t)src_stride + (size_t)x;
            src_uv[idx] = (uint8_t)((eye * 71 + ex + y * 11) & 0xff);
            src_uv[idx + 1] = (uint8_t)((eye * 97 + ex * 5 + y * 13) & 0xff);
        }
    }

    miro_ego_nv12_frame_t src = {
        .y = src_y,
        .uv = src_uv,
        .width = sbs_width,
        .height = height,
        .stride = src_stride,
        .size = (uint32_t)(src_y_bytes + src_uv_bytes),
    };
    miro_ego_nv12_frame_t left = {
        .y = left_buf,
        .uv = left_buf + dst_y_bytes,
        .width = eye_width,
        .height = height,
        .stride = dst_stride,
        .size = (uint32_t)(dst_y_bytes + dst_uv_bytes),
    };
    miro_ego_nv12_frame_t right = {
        .y = right_buf,
        .uv = right_buf + dst_y_bytes,
        .width = eye_width,
        .height = height,
        .stride = dst_stride,
        .size = (uint32_t)(dst_y_bytes + dst_uv_bytes),
    };

    const miro_ego_split_ops_t *ops = miro_ego_split_ops_cpu();
    assert(ops != NULL && ops->split_eye != NULL && ops->split_stereo != NULL);

    assert(ops->split_eye(NULL, &src, &left, eye_width, height, 0) == 0);
    assert(ops->split_eye(NULL, &src, &right, eye_width, height, eye_width) == 0);
    assert(left.size == (uint32_t)(dst_y_bytes + dst_uv_bytes));
    assert(right.size == (uint32_t)(dst_y_bytes + dst_uv_bytes));

    for (int y = 0; y < height; y += 1) {
        for (int x = 0; x < eye_width; x += 1) {
            const size_t d = (size_t)y * (size_t)dst_stride + (size_t)x;
            assert(left.y[d] == (uint8_t)((0 * 113 + x * 3 + y * 7) & 0xff));
            assert(right.y[d] == (uint8_t)((1 * 113 + x * 3 + y * 7) & 0xff));
        }
    }
    for (int y = 0; y < height / 2; y += 1) {
        for (int x = 0; x < eye_width; x += 2) {
            const size_t d = (size_t)y * (size_t)dst_stride + (size_t)x;
            assert(left.uv[d] == (uint8_t)((0 * 71 + x + y * 11) & 0xff));
            assert(left.uv[d + 1] == (uint8_t)((0 * 97 + x * 5 + y * 13) & 0xff));
            assert(right.uv[d] == (uint8_t)((1 * 71 + x + y * 11) & 0xff));
            assert(right.uv[d + 1] == (uint8_t)((1 * 97 + x * 5 + y * 13) & 0xff));
        }
    }

    free(src_buf);
    free(left_buf);
    free(right_buf);
}

static void test_nv12_split_and_preview(void)
{
    /* Full 4K side-by-side 3840x1080 tightly packed */
    verify_nv12_split_case(3840, 1080, 3840, 1920);
    /* Padded strides (typical DMA alignment) */
    verify_nv12_split_case(3840, 1080, 4096, 2048);
    /* Non-64-byte-aligned width (exercises SIMD/scalar tail) */
    verify_nv12_split_case(264, 68, 288, 144);

    /* Validation failures zero dst.size and fail closed */
    uint8_t dummy[128] = {0};
    miro_ego_nv12_frame_t src = {.y = dummy, .uv = dummy + 64, .width = 16, .height = 4, .stride = 16, .size = 96};
    miro_ego_nv12_frame_t dst = {.y = dummy, .uv = dummy + 32, .width = 8, .height = 4, .stride = 8, .size = 48};

    assert(miro_ego_split_eye_nv12(NULL, &dst, 8, 4, 0) == -1 && dst.size == 0);
    dst.size = 48;
    assert(miro_ego_split_eye_nv12(&src, &dst, 7, 4, 0) == -1 && dst.size == 0); /* odd width */
    dst.size = 48;
    assert(miro_ego_split_eye_nv12(&src, &dst, 8, 3, 0) == -1 && dst.size == 0); /* odd height */
    dst.size = 48;
    assert(miro_ego_split_eye_nv12(&src, &dst, 8, 4, 1) == -1 && dst.size == 0); /* odd x_offset */
    dst.size = 48;
    assert(miro_ego_split_eye_nv12(&src, &dst, 8, 4, 10) == -1 && dst.size == 0); /* x_offset + eye_w > stride */
    dst.size = 47;
    assert(miro_ego_split_eye_nv12(&src, &dst, 8, 4, 0) == -1 && dst.size == 0); /* dst too small */

    /* split_stereo + 2x2 NV12 preview downscale verification */
    uint8_t sbs_buf[16 * 8 * 3 / 2];
    uint8_t l_buf[8 * 8 * 3 / 2];
    uint8_t r_buf[8 * 8 * 3 / 2];
    uint8_t p_buf[4 * 4 * 3 / 2];
    memset(sbs_buf, 0, sizeof(sbs_buf));
    /* Fill left eye Y with 100 and UV with (U=40, V=200) */
    for (int y = 0; y < 8; y += 1) {
        memset(sbs_buf + y * 16, 100, 8);
        memset(sbs_buf + y * 16 + 8, 180, 8);
    }
    uint8_t *sbs_uv = sbs_buf + 16 * 8;
    for (int y = 0; y < 4; y += 1) {
        for (int x = 0; x < 8; x += 2) {
            sbs_uv[y * 16 + x] = 40;
            sbs_uv[y * 16 + x + 1] = 200;
        }
    }

    miro_ego_nv12_frame_t s_f = {.y = sbs_buf, .uv = sbs_uv, .width = 16, .height = 8, .stride = 16, .size = sizeof(sbs_buf)};
    miro_ego_nv12_frame_t l_f = {.y = l_buf, .uv = l_buf + 64, .width = 8, .height = 8, .stride = 8, .size = sizeof(l_buf)};
    miro_ego_nv12_frame_t r_f = {.y = r_buf, .uv = r_buf + 64, .width = 8, .height = 8, .stride = 8, .size = sizeof(r_buf)};
    miro_ego_nv12_frame_t p_f = {.y = p_buf, .uv = p_buf + 16, .width = 4, .height = 4, .stride = 4, .size = sizeof(p_buf)};

    assert(miro_ego_split_ops_cpu()->split_stereo(NULL, &s_f, &l_f, &r_f, &p_f) == 0);
    assert(p_f.size == sizeof(p_buf));
    for (int i = 0; i < 16; i += 1) {
        assert(p_f.y[i] == 100);
    }
    for (int i = 0; i < 8; i += 2) {
        assert(p_f.uv[i] == 40);
        assert(p_f.uv[i + 1] == 200);
    }

    /* Contiguous vstride > height padding must be preserved in dst.size */
    uint8_t padded_l[8 * 16 * 3 / 2];
    memset(padded_l, 0, sizeof(padded_l));
    miro_ego_nv12_frame_t l_padded = {
        .y = padded_l,
        .uv = padded_l + 8 * 12, /* vstride=12 > height=8 */
        .width = 8,
        .height = 8,
        .stride = 8,
        .size = sizeof(padded_l),
    };
    assert(miro_ego_split_eye_nv12(&s_f, &l_padded, 8, 8, 0) == 0);
    assert(l_padded.size == (uint32_t)(8 * 12 + 8 * 4));
    assert(l_padded.uv[0] == 40 && l_padded.uv[1] == 200);

    /* Disjoint Y and UV buffers fall back to packed dst_stride * (height + height/2) */
    uint8_t *disjoint_uv = malloc(8 * 4);
    uint8_t *disjoint_y = malloc(8 * 8);
    assert(disjoint_uv != NULL && disjoint_y != NULL);
    miro_ego_nv12_frame_t l_disjoint = {
        .y = disjoint_y,
        .uv = disjoint_uv,
        .width = 8,
        .height = 8,
        .stride = 8,
        .size = 8 * 12,
    };
    assert(miro_ego_split_eye_nv12(&s_f, &l_disjoint, 8, 8, 0) == 0);
    assert(l_disjoint.size == 8 * 12);
    free(disjoint_y);
    free(disjoint_uv);
}

/* ----------------------------------------------------- MP4 metadata fix --- */

static void write_file_bytes(const char *path, const uint8_t *buf, size_t len)
{
    FILE *fp = fopen(path, "wb");
    assert(fp != NULL);
    assert(fwrite(buf, 1, len, fp) == len);
    fclose(fp);
}

static size_t read_file_bytes(const char *path, uint8_t *buf, size_t cap)
{
    FILE *fp = fopen(path, "rb");
    assert(fp != NULL);
    size_t n = fread(buf, 1, cap, fp);
    fclose(fp);
    return n;
}

static void test_mp4_v0_and_colorimetry(void)
{
    const char *sample_entries[] = {"avc1", "hvc1", "hev1"};
    const uint16_t color_cases[2][3] = {{0, 0, 0}, {1, 1, 1}};

    char path[] = "/tmp/miro_ego_mp4_v0_XXXXXX";
    int fd = mkstemp(path);
    assert(fd >= 0);
    close(fd);

    for (size_t s = 0; s < 3; s += 1) {
        for (size_t c = 0; c < 2; c += 1) {
            uint8_t colr_body[11];
            memcpy(colr_body, "nclx", 4);
            put_be16(colr_body + 4, color_cases[c][0]);
            put_be16(colr_body + 6, color_cases[c][1]);
            put_be16(colr_body + 8, color_cases[c][2]);
            colr_body[10] = 0x00;
            uint8_t colr[32];
            size_t colr_len = make_box(colr, "colr", colr_body, sizeof(colr_body));

            uint8_t entry_body[128] = {0};
            memcpy(entry_body + 78, colr, colr_len);
            uint8_t entry[160];
            size_t entry_len = make_box(entry, sample_entries[s], entry_body, 78u + colr_len);

            uint8_t stsd_body[192] = {0};
            put_be32(stsd_body + 4, 1u);
            memcpy(stsd_body + 8, entry, entry_len);
            uint8_t stsd[224];
            size_t stsd_len = make_box(stsd, "stsd", stsd_body, 8u + entry_len);
            uint8_t stbl[256];
            size_t stbl_len = make_box(stbl, "stbl", stsd, stsd_len);
            uint8_t minf[288];
            size_t minf_len = make_box(minf, "minf", stbl, stbl_len);

            uint8_t mvhd_body[28] = {0};
            put_be32(mvhd_body + 12, 1000u);
            put_be32(mvhd_body + 16, 1000u);
            uint8_t mvhd[40];
            size_t mvhd_len = make_box(mvhd, "mvhd", mvhd_body, sizeof(mvhd_body));
            uint8_t mdhd[40];
            size_t mdhd_len = make_box(mdhd, "mdhd", mvhd_body, sizeof(mvhd_body));

            uint8_t mdia_body[384];
            memcpy(mdia_body, mdhd, mdhd_len);
            memcpy(mdia_body + mdhd_len, minf, minf_len);
            uint8_t mdia[400];
            size_t mdia_len = make_box(mdia, "mdia", mdia_body, mdhd_len + minf_len);

            uint8_t tkhd_body[32] = {0};
            put_be32(tkhd_body + 20, 1000u);
            uint8_t tkhd[48];
            size_t tkhd_len = make_box(tkhd, "tkhd", tkhd_body, sizeof(tkhd_body));

            /* elst with leading empty edit (-1) followed by media edit */
            uint8_t elst_body[32] = {0};
            put_be32(elst_body + 4, 2u);
            put_be32(elst_body + 8, 50u);
            put_be32(elst_body + 12, 0xffffffffu); /* empty edit: must not be modified */
            put_be32(elst_body + 16, 0x00010000u);
            put_be32(elst_body + 20, 1000u);       /* media edit: must become 1034 */
            put_be32(elst_body + 24, 0u);
            put_be32(elst_body + 28, 0x00010000u);
            uint8_t elst[48];
            size_t elst_len = make_box(elst, "elst", elst_body, sizeof(elst_body));
            uint8_t edts[64];
            size_t edts_len = make_box(edts, "edts", elst, elst_len);

            uint8_t trak_body[576];
            size_t p = 0;
            memcpy(trak_body + p, tkhd, tkhd_len); p += tkhd_len;
            memcpy(trak_body + p, edts, edts_len); p += edts_len;
            memcpy(trak_body + p, mdia, mdia_len); p += mdia_len;
            uint8_t trak[600];
            size_t trak_len = make_box(trak, "trak", trak_body, p);

            uint8_t moov_body[700];
            memcpy(moov_body, mvhd, mvhd_len);
            memcpy(moov_body + mvhd_len, trak, trak_len);
            uint8_t moov[720];
            size_t moov_len = make_box(moov, "moov", moov_body, mvhd_len + trak_len);

            /* 64-bit largesize mdat box preceding moov */
            const char *payload_txt = "unaltered frame payload colr nclx";
            const size_t payload_len = strlen(payload_txt);
            uint8_t mdat[128];
            put_be32(mdat, 1u); /* size32 == 1 -> 64-bit largesize */
            memcpy(mdat + 4, "mdat", 4);
            put_be64(mdat + 8, (uint64_t)(16u + payload_len));
            memcpy(mdat + 16, payload_txt, payload_len);
            const size_t mdat_len = 16u + payload_len;

            uint8_t file_buf[1024];
            memcpy(file_buf, mdat, mdat_len);
            memcpy(file_buf + mdat_len, moov, moov_len);
            write_file_bytes(path, file_buf, mdat_len + moov_len);

            char reason[128] = {0};
            int hash_fd = open(path, O_RDWR | O_CLOEXEC);
            assert(hash_fd >= 0);
            miro_ego_sha256_ctx_t inc_ctx;
            unsigned long long inc_bytes = 0;
            assert(miro_ego_mp4_extend_durations_and_hash_fd(
                       hash_fd, 30, NULL, 0, &inc_ctx, &inc_bytes, reason, sizeof(reason)) == 0);
            uint8_t inc_digest[MIRO_EGO_SHA256_DIGEST_LEN];
            char inc_hex[MIRO_EGO_SHA256_HEX_LEN] = {0};
            miro_ego_sha256_final(&inc_ctx, inc_digest);
            miro_ego_sha256_to_hex(inc_digest, inc_hex);
            char full_hex[MIRO_EGO_SHA256_HEX_LEN] = {0};
            unsigned long long full_bytes = 0;
            assert(miro_ego_sha256_fd(hash_fd, full_hex, &full_bytes) == 0);
            close(hash_fd);
            assert(inc_bytes == full_bytes);
            assert(strcmp(inc_hex, full_hex) == 0);

            uint8_t actual[1024];
            size_t got = read_file_bytes(path, actual, sizeof(actual));
            assert(got == mdat_len + moov_len);
            /* mdat untouched */
            assert(memcmp(actual, mdat, mdat_len) == 0);

            /* Verify mvhd duration == 1034 */
            const uint8_t *mvhd_out = actual + mdat_len + 8u;
            assert(memcmp(mvhd_out + 4, "mvhd", 4) == 0);
            assert(get_be32(mvhd_out + 24) == 1034u);

            /* Verify elst empty edit stayed 50, media edit became 1034 */
            const uint8_t *elst_pos = memmem(actual + mdat_len, moov_len, "elst", 4);
            assert(elst_pos != NULL);
            const uint8_t *elst_box = elst_pos - 4;
            assert(get_be32(elst_box + 16) == 50u);
            assert(get_be32(elst_box + 28) == 1034u);

            /* Verify colr patch */
            const uint8_t *nclx = memmem(actual + mdat_len, moov_len, "nclx", 4);
            assert(nclx != NULL);
            const uint16_t exp_p = color_cases[c][0] ? color_cases[c][0] : 2u;
            assert(((uint16_t)nclx[4] << 8 | nclx[5]) == exp_p);
            assert(((uint16_t)nclx[6] << 8 | nclx[7]) == exp_p);
            assert(((uint16_t)nclx[8] << 8 | nclx[9]) == exp_p);
            assert(nclx[10] == 0x80u);
        }
    }

    unlink(path);
}

static void test_mp4_v1_64bit_durations(void)
{
    char path[] = "/tmp/miro_ego_mp4_v1_XXXXXX";
    int fd = mkstemp(path);
    assert(fd >= 0);
    close(fd);

    const uint64_t base_dur = 0x0000000100000000ULL + 1000ULL;

    /* Omit colr to also confirm missing colr box is accepted cleanly */
    uint8_t entry_body[80] = {0};
    uint8_t entry[96];
    size_t entry_len = make_box(entry, "avc1", entry_body, 78u);
    uint8_t stsd_body[128] = {0};
    put_be32(stsd_body + 4, 1u);
    memcpy(stsd_body + 8, entry, entry_len);
    uint8_t stsd[144];
    size_t stsd_len = make_box(stsd, "stsd", stsd_body, 8u + entry_len);
    uint8_t stbl[160];
    size_t stbl_len = make_box(stbl, "stbl", stsd, stsd_len);
    uint8_t minf[176];
    size_t minf_len = make_box(minf, "minf", stbl, stbl_len);

    /* mvhd / mdhd v1: 1-byte version=1, 3 flags, 8 create, 8 mod, 4 timescale(@20), 8 dur(@24) -> 32 bytes body */
    uint8_t v1_hdr[32] = {0};
    v1_hdr[0] = 1u;
    put_be32(v1_hdr + 20, 90000u);
    put_be64(v1_hdr + 24, base_dur);
    uint8_t mvhd[48];
    size_t mvhd_len = make_box(mvhd, "mvhd", v1_hdr, sizeof(v1_hdr));
    uint8_t mdhd[48];
    size_t mdhd_len = make_box(mdhd, "mdhd", v1_hdr, sizeof(v1_hdr));

    uint8_t mdia_body[256];
    memcpy(mdia_body, mdhd, mdhd_len);
    memcpy(mdia_body + mdhd_len, minf, minf_len);
    uint8_t mdia[272];
    size_t mdia_len = make_box(mdia, "mdia", mdia_body, mdhd_len + minf_len);

    /* tkhd v1: ver=1, flags, 8 create, 8 mod, 4 track_id, 4 res, 8 dur(@28) -> 36 bytes body */
    uint8_t tkhd_body[36] = {0};
    tkhd_body[0] = 1u;
    put_be64(tkhd_body + 28, base_dur);
    uint8_t tkhd[48];
    size_t tkhd_len = make_box(tkhd, "tkhd", tkhd_body, sizeof(tkhd_body));

    /* elst v1: ver=1, flags, 4 count=2, entry0(empty: media_time=UINT64_MAX), entry1(media) */
    uint8_t elst_body[48] = {0};
    elst_body[0] = 1u;
    put_be32(elst_body + 4, 2u);
    put_be64(elst_body + 8, 900ULL);
    put_be64(elst_body + 16, UINT64_MAX);
    put_be32(elst_body + 24, 0x00010000u);
    put_be64(elst_body + 28, base_dur);
    put_be64(elst_body + 36, 0ULL);
    put_be32(elst_body + 44, 0x00010000u);
    uint8_t elst[64];
    size_t elst_len = make_box(elst, "elst", elst_body, sizeof(elst_body));
    uint8_t edts[80];
    size_t edts_len = make_box(edts, "edts", elst, elst_len);

    uint8_t trak_body[512];
    size_t p = 0;
    memcpy(trak_body + p, tkhd, tkhd_len); p += tkhd_len;
    memcpy(trak_body + p, edts, edts_len); p += edts_len;
    memcpy(trak_body + p, mdia, mdia_len); p += mdia_len;
    uint8_t trak[528];
    size_t trak_len = make_box(trak, "trak", trak_body, p);

    uint8_t moov_body[640];
    memcpy(moov_body, mvhd, mvhd_len);
    memcpy(moov_body + mvhd_len, trak, trak_len);
    uint8_t moov[656];
    size_t moov_len = make_box(moov, "moov", moov_body, mvhd_len + trak_len);

    write_file_bytes(path, moov, moov_len);

    char reason[128] = {0};
    assert(miro_ego_mp4_extend_durations(path, 30, reason, sizeof(reason)) == 0);

    uint8_t actual[656];
    assert(read_file_bytes(path, actual, sizeof(actual)) == moov_len);

    /* 90000 / 30 = 3000 ticks */
    const uint64_t expected_dur = base_dur + 3000ULL;
    const uint8_t *mvhd_out = actual + 8u;
    assert(get_be64(mvhd_out + 32) == expected_dur);

    const uint8_t *tkhd_pos = memmem(actual, moov_len, "tkhd", 4);
    assert(tkhd_pos != NULL);
    assert(get_be64(tkhd_pos - 4 + 36) == expected_dur);

    const uint8_t *mdhd_pos = memmem(actual, moov_len, "mdhd", 4);
    assert(mdhd_pos != NULL);
    assert(get_be64(mdhd_pos - 4 + 32) == expected_dur);

    const uint8_t *elst_pos = memmem(actual, moov_len, "elst", 4);
    assert(elst_pos != NULL);
    assert(get_be64(elst_pos - 4 + 16) == 900ULL);
    assert(get_be64(elst_pos - 4 + 36) == expected_dur);

    /* Failure cases: invalid fd/fps, symlink rejection, corrupt moov */
    assert(miro_ego_mp4_extend_durations_fd(-1, 30, reason, sizeof(reason)) == -1);
    assert(miro_ego_mp4_extend_durations(path, 0, reason, sizeof(reason)) == -1);

    char sym_path[128];
    snprintf(sym_path, sizeof(sym_path), "%s.symlink", path);
    unlink(sym_path);
    assert(symlink(path, sym_path) == 0);
    assert(miro_ego_mp4_extend_durations(sym_path, 30, reason, sizeof(reason)) == -1);
    unlink(sym_path);

    int rw_fd = open(path, O_RDWR | O_CLOEXEC);
    assert(rw_fd >= 0);
    assert(miro_ego_mp4_extend_durations_fd(rw_fd, 30, reason, sizeof(reason)) == 0);
    close(rw_fd);

    uint8_t bad_moov[24];
    uint8_t bad_trak[12] = {0, 0, 0, 12, 't', 'r', 'a', 'k', 0, 0, 0, 0};
    size_t bad_len = make_box(bad_moov, "moov", bad_trak, sizeof(bad_trak));
    write_file_bytes(path, bad_moov, bad_len);
    assert(miro_ego_mp4_extend_durations(path, 30, reason, sizeof(reason)) == -1);

    unlink(path);
}

/* ----------------------------------------------------------- IDR & JSON --- */

static void test_idr_detection(void)
{
    const unsigned char h264_idr[] = {0, 0, 0, 1, 0x67, 9, 0, 0, 1, 0x65, 3};
    const unsigned char h264_nonidr[] = {0, 0, 1, 0x41, 5, 0, 0};
    assert(miro_ego_h264_has_idr(h264_idr, sizeof(h264_idr)) == 1);
    assert(miro_ego_h264_has_idr(h264_nonidr, sizeof(h264_nonidr)) == 0);
    assert(miro_ego_h264_has_idr(NULL, 10) == 0);

    const unsigned char h265_idr_radl[] = {0, 0, 0, 1, 0x26, 1, 3}; /* type 19 */
    const unsigned char h265_idr_lp[] = {0, 0, 1, 0x28, 1, 3};      /* type 20 */
    const unsigned char h265_cra[] = {0, 0, 1, 0x2a, 1, 3};         /* type 21 (not IDR) */
    const unsigned char h265_bad_tid[] = {0, 0, 1, 0x26, 0, 3};     /* temporal_id == 0 invalid */
    assert(miro_ego_h265_has_idr(h265_idr_radl, sizeof(h265_idr_radl)) == 1);
    assert(miro_ego_h265_has_idr(h265_idr_lp, sizeof(h265_idr_lp)) == 1);
    assert(miro_ego_h265_has_idr(h265_cra, sizeof(h265_cra)) == 0);
    assert(miro_ego_h265_has_idr(h265_bad_tid, sizeof(h265_bad_tid)) == 0);
    assert(miro_ego_h265_has_idr(h265_idr_radl, 5) == 0); /* truncated */
    assert(miro_ego_h265_has_idr(NULL, 10) == 0);
}

static void test_json_escaping_and_cli(void)
{
    char *out = NULL;
    size_t out_len = 0;
    FILE *mem = open_memstream(&out, &out_len);
    assert(mem != NULL);
    miro_ego_write_json_string(mem, "a\"b\\c\bd\fe\nf\rg\th\x01\x1f");
    fclose(mem);
    assert(strcmp(out, "\"a\\\"b\\\\c\\bd\\fe\\nf\\rg\\th\\u0001\\u001f\"") == 0);
    free(out);

    out = NULL;
    out_len = 0;
    mem = open_memstream(&out, &out_len);
    assert(mem != NULL);
    miro_ego_write_json_string(mem, NULL);
    fclose(mem);
    assert(strcmp(out, "\"\"") == 0);
    free(out);

    int val = -1;
    assert(miro_ego_parse_int_checked("42", 0, 100, &val) == 0 && val == 42);
    assert(miro_ego_parse_int_checked("-1", 0, 100, &val) == -1);
    assert(miro_ego_parse_int_checked("101", 0, 100, &val) == -1);
    assert(miro_ego_parse_int_checked("12x", 0, 100, &val) == -1);
    assert(miro_ego_parse_int_checked("", 0, 100, &val) == -1);

    int hevc = -1;
    assert(miro_ego_parse_codec("h264", &hevc) == 0 && hevc == 0);
    assert(miro_ego_parse_codec("hevc", &hevc) == 0 && hevc == 1);
    assert(miro_ego_parse_codec("vp9", &hevc) == -1);

    int rc = -1;
    assert(miro_ego_parse_rc_mode("cbr", &rc) == 0 && rc == 0);
    assert(miro_ego_parse_rc_mode("vbr", &rc) == 0 && rc == 1);
    assert(miro_ego_parse_rc_mode("fixqp", &rc) == 0 && rc == 2);
    assert(miro_ego_parse_rc_mode("avbr", &rc) == 0 && rc == 3);
    assert(miro_ego_parse_rc_mode("invalid", &rc) == -1);
}

static void test_packet_queue(void)
{
    miro_ego_packet_queue_t q;
    assert(miro_ego_packet_queue_init(&q, 32u, 2u) == 0);

    uint8_t buf[16];
    memset(buf, 0x11, sizeof(buf));
    assert(miro_ego_packet_queue_push(&q, buf, 16u, 1) == 0);
    memset(buf, 0x22, sizeof(buf));
    assert(miro_ego_packet_queue_push(&q, buf, 16u, 0) == 0);
    /* Both bytes (32) and frames (2) full */
    assert(miro_ego_packet_queue_push(&q, buf, 1u, 0) == -1);
    assert(q.peak_bytes == 32u && q.peak_frames == 2u && q.rejected == 1u);

    miro_ego_packet_t *p0 = miro_ego_packet_queue_pop(&q);
    assert(p0 != NULL && p0->size == 16u && p0->key_frame == 1 && p0->data[0] == 0x11);
    /* Still counted in-flight until done()! */
    assert(miro_ego_packet_queue_push(&q, buf, 1u, 0) == -1);
    miro_ego_packet_queue_done(&q, p0);

    assert(miro_ego_packet_queue_push(&q, buf, 16u, 0) == 0);
    miro_ego_packet_queue_close(&q);
    assert(miro_ego_packet_queue_push(&q, buf, 1u, 0) == -1);

    miro_ego_packet_t *p1 = miro_ego_packet_queue_pop(&q);
    assert(p1 != NULL && p1->data[0] == 0x22);
    miro_ego_packet_queue_done(&q, p1);
    miro_ego_packet_t *p2 = miro_ego_packet_queue_pop(&q);
    assert(p2 != NULL);
    miro_ego_packet_queue_done(&q, p2);
    assert(miro_ego_packet_queue_pop(&q) == NULL);
    assert(q.bytes == 0u && q.frames == 0u);
    miro_ego_packet_queue_destroy(&q);
}

int main(void)
{
    test_nv12_split_and_preview();
    test_mp4_v0_and_colorimetry();
    test_mp4_v1_64bit_durations();
    test_idr_detection();
    test_json_escaping_and_cli();
    test_packet_queue();
    printf("test_pipeline_helpers: PASS (NV12 split + preview + MP4 v0/v1/colr + IDR + JSON + queue)\n");
    return 0;
}

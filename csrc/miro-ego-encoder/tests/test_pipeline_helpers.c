#define _GNU_SOURCE
#include "miro_ego_cli_common.h"
#include "miro_ego_flac.h"
#include "miro_ego_fmp4.h"
#include "miro_ego_packet_queue.h"
#include "miro_ego_split.h"

#include <assert.h>
#include <fcntl.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

static uint32_t get_be32(const uint8_t *p)
{
    return ((uint32_t)p[0] << 24) | ((uint32_t)p[1] << 16) |
           ((uint32_t)p[2] << 8) | (uint32_t)p[3];
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

/* ------------------------------------------------------- FLAC & fMP4 --- */

static size_t read_file_bytes(const char *path, uint8_t *buf, size_t cap)
{
    FILE *fp = fopen(path, "rb");
    assert(fp != NULL);
    size_t n = fread(buf, 1, cap, fp);
    fclose(fp);
    return n;
}

static void test_flac_frame_encoding(void)
{
    uint8_t *buf = NULL;
    size_t cap = 0u;
    size_t len = 0u;
    int16_t *scratch_l = calloc(2048u, sizeof(int16_t));
    int16_t *scratch_r = calloc(2048u, sizeof(int16_t));
    int16_t *pcm = calloc(2048u * 2u, sizeof(int16_t));
    assert(scratch_l != NULL && scratch_r != NULL && pcm != NULL);

    /* Smooth audio signal exercises FIXED order-1 + Rice coding */
    for (uint32_t i = 0; i < 1603u; i += 1u) {
        pcm[i * 2u] = (int16_t)((i * 19u) & 0x1fff);
        pcm[i * 2u + 1u] = (int16_t)(-3000 + (int32_t)((i * 23u) & 0x0fff));
    }
    assert(miro_ego_flac_encode_frame(&buf, &cap, &len, pcm, 1603u, 96000ULL,
                                      scratch_l, scratch_r) == 0);
    assert(len > 12u && len < 1603u * 4u);
    assert(buf[0] == 0xffu && buf[1] == 0xf9u);
    assert(miro_ego_flac_crc16(buf, len - 2u) ==
           (uint16_t)(((uint16_t)buf[len - 2u] << 8) | buf[len - 1u]));

    /* High-entropy noise + small blocksize triggers Verbatim cleanly */
    for (uint32_t i = 0; i < 16u; i += 1u) {
        pcm[i * 2u] = (int16_t)((i & 1u) ? 30000 : -30000);
        pcm[i * 2u + 1u] = (int16_t)((i & 1u) ? -29000 : 29000);
    }
    assert(miro_ego_flac_encode_frame(&buf, &cap, &len, pcm, 16u, 1ULL,
                                      scratch_l, scratch_r) == 0);
    assert(buf[0] == 0xffu && buf[1] == 0xf9u);
    assert(miro_ego_flac_crc16(buf, len - 2u) ==
           (uint16_t)(((uint16_t)buf[len - 2u] << 8) | buf[len - 1u]));

    /* Invalid blocksize rejected */
    assert(miro_ego_flac_encode_frame(&buf, &cap, &len, pcm, 0u, 0ULL,
                                      scratch_l, scratch_r) == -1);
    assert(miro_ego_flac_encode_frame(&buf, &cap, &len, pcm, 65536u, 0ULL,
                                      scratch_l, scratch_r) == -1);

    free(buf);
    free(scratch_l);
    free(scratch_r);
    free(pcm);
}

static void test_fmp4_writer_h264_and_hevc(void)
{
    static const uint8_t h264_idr[] = {
        0, 0, 0, 1, 0x67, 0x64, 0x00, 0x28, 0xac, 0xd9, 0x40,
        0, 0, 0, 1, 0x68, 0xce, 0x38, 0x80,
        0, 0, 0, 1, 0x65, 0x88, 0x84, 0x21,
    };
    static const uint8_t h265_idr[] = {
        0, 0, 0, 1, 0x40, 0x01, 0x0c, 0x01, 0xff, 0xff, 0x01, 0x60,
        0, 0, 0, 1, 0x42, 0x01, 0x01, 0x01, 0x60, 0x00, 0x00, 0x03, 0x00, 0xb0, 0x00, 0x00, 0x03, 0x00, 0x00, 0x03, 0x00, 0x78,
        0, 0, 0, 1, 0x44, 0x01, 0xc1, 0x73,
        0, 0, 0, 1, 0x26, 0x01, 0xaf, 0x09,
    };

    for (int hevc = 0; hevc <= 1; hevc += 1) {
        int has_audio = (hevc == 0) ? 1 : 0;
        char path[] = "/tmp/miro_ego_fmp4_test_XXXXXX";
        int fd = mkstemp(path);
        assert(fd >= 0);

        miro_ego_video_ps_t ps[2] = {{0}, {0}};
        miro_ego_bbuf_t avcc[2] = {{0}, {0}};
        int is_idr[2] = {0, 0};
        const uint8_t *raw = hevc ? h265_idr : h264_idr;
        size_t raw_len = hevc ? sizeof(h265_idr) : sizeof(h264_idr);
        assert(miro_ego_annexb_to_avcc(hevc, raw, raw_len, &ps[0], &avcc[0], &is_idr[0]) == 0);
        assert(miro_ego_annexb_to_avcc(hevc, raw, raw_len, &ps[1], &avcc[1], &is_idr[1]) == 0);
        assert(is_idr[0] == 1 && is_idr[1] == 1);
        assert(ps[0].sps_len > 0u && ps[0].pps_len > 0u);
        if (hevc) {
            assert(ps[0].vps_len > 0u);
        }

        miro_ego_fmp4_t mx;
        assert(miro_ego_fmp4_init(&mx, fd, hevc, has_audio, 64, 32, 48000u) == 0);
        assert(miro_ego_fmp4_write_header(&mx, ps) == 0);

        const uint32_t durs[2] = {1601u, 1599u};
        const uint8_t fake_flac[8] = {0xff, 0xf9, 0x7a, 0x18, 0x00, 0x00, 0x00, 0x00};
        const char *meta = "{\"seq\":0}";
        for (int f = 0; f < 2; f += 1) {
            assert(miro_ego_fmp4_push_sample(&mx, 0, durs[f], avcc[0].data,
                                             (uint32_t)avcc[0].len, 0x02000000u) == 0);
            assert(miro_ego_fmp4_push_sample(&mx, 1, durs[f], avcc[1].data,
                                             (uint32_t)avcc[1].len, 0x02000000u) == 0);
            if (has_audio) {
                assert(miro_ego_fmp4_push_sample(&mx, 2, durs[f], fake_flac,
                                                 sizeof(fake_flac), 0x02000000u) == 0);
                assert(miro_ego_fmp4_push_sample(&mx, 3, durs[f], (const uint8_t *)meta,
                                                 (uint32_t)strlen(meta), 0x02000000u) == 0);
            } else {
                assert(miro_ego_fmp4_push_sample(&mx, 2, durs[f], (const uint8_t *)meta,
                                                 (uint32_t)strlen(meta), 0x02000000u) == 0);
            }
        }

        miro_ego_bbuf_t frag = {0};
        uint64_t frag_off = 0u, frag_dur = 0u;
        assert(miro_ego_fmp4_build_fragment(&mx, &frag, &frag_off, &frag_dur) == 0);
        assert(frag.len > 0u && frag_dur == 3200u);
        assert(write(fd, frag.data, frag.len) == (ssize_t)frag.len);

        miro_ego_bbuf_t mfra = {0};
        assert(miro_ego_fmp4_build_mfra(&mx, &mfra) == 0);
        assert(write(fd, mfra.data, mfra.len) == (ssize_t)mfra.len);
        assert(miro_ego_fmp4_patch_moov_durations(&mx, fd, 3200u) == 0);
        close(fd);

        uint8_t file_buf[4096];
        size_t total = read_file_bytes(path, file_buf, sizeof(file_buf));
        assert(total > 200u);
        assert(memcmp(file_buf + 4u, "ftyp", 4) == 0);
        assert(memmem(file_buf, total, hevc ? "hvcC" : "avcC", 4) != NULL);
        assert(memmem(file_buf, total, "nclx", 4) != NULL);
        assert(memmem(file_buf, total, "mett", 4) != NULL);
        assert((memmem(file_buf, total, "dfLa", 4) != NULL) == (has_audio != 0));
        assert(memmem(file_buf, total, "moof", 4) != NULL);
        assert(memmem(file_buf, total, "mfra", 4) != NULL);
        assert(get_be32(file_buf + mx.mvhd_dur_file_off) == 3200u);
        assert(get_be32(file_buf + mx.mehd_dur_file_off) == 3200u);
        assert(get_be32(file_buf + total - 4u) == (uint32_t)mfra.len);

        miro_ego_bbuf_free(&frag);
        miro_ego_bbuf_free(&mfra);
        miro_ego_bbuf_free(&avcc[0]);
        miro_ego_bbuf_free(&avcc[1]);
        miro_ego_video_ps_free(&ps[0]);
        miro_ego_video_ps_free(&ps[1]);
        miro_ego_fmp4_destroy(&mx);
        unlink(path);
    }
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
    test_flac_frame_encoding();
    test_fmp4_writer_h264_and_hevc();
    test_idr_detection();
    test_json_escaping_and_cli();
    test_packet_queue();
    printf("test_pipeline_helpers: PASS (NV12 split + preview + FLAC + fMP4 H.264/HEVC + IDR + JSON + queue)\n");
    return 0;
}

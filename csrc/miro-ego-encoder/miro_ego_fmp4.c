/*
 * Pure-C ISO-BMFF 3/4-track fragmented MP4 (fMP4) writer + Annex-B helpers.
 */

#define _POSIX_C_SOURCE 200809L

#include "miro_ego_fmp4.h"

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

static int bbuf_reserve(miro_ego_bbuf_t *b, size_t extra)
{
    if (extra > SIZE_MAX - b->len) {
        return -1;
    }
    size_t need = b->len + extra;
    if (need <= b->cap) {
        return 0;
    }
    size_t ncap = b->cap ? b->cap * 2u : 4096u;
    while (ncap < need) {
        if (ncap > SIZE_MAX / 2u) {
            ncap = need;
            break;
        }
        ncap *= 2u;
    }
    uint8_t *p = realloc(b->data, ncap);
    if (p == NULL) {
        return -1;
    }
    b->data = p;
    b->cap = ncap;
    return 0;
}

static int bbuf_u8(miro_ego_bbuf_t *b, uint8_t v)
{
    if (bbuf_reserve(b, 1u) != 0) {
        return -1;
    }
    b->data[b->len++] = v;
    return 0;
}

static int bbuf_u16(miro_ego_bbuf_t *b, uint16_t v)
{
    if (bbuf_reserve(b, 2u) != 0) {
        return -1;
    }
    b->data[b->len++] = (uint8_t)(v >> 8);
    b->data[b->len++] = (uint8_t)v;
    return 0;
}

static int bbuf_u24(miro_ego_bbuf_t *b, uint32_t v)
{
    if (bbuf_reserve(b, 3u) != 0) {
        return -1;
    }
    b->data[b->len++] = (uint8_t)(v >> 16);
    b->data[b->len++] = (uint8_t)(v >> 8);
    b->data[b->len++] = (uint8_t)v;
    return 0;
}

static int bbuf_u32(miro_ego_bbuf_t *b, uint32_t v)
{
    if (bbuf_reserve(b, 4u) != 0) {
        return -1;
    }
    b->data[b->len++] = (uint8_t)(v >> 24);
    b->data[b->len++] = (uint8_t)(v >> 16);
    b->data[b->len++] = (uint8_t)(v >> 8);
    b->data[b->len++] = (uint8_t)v;
    return 0;
}

static int bbuf_u64(miro_ego_bbuf_t *b, uint64_t v)
{
    if (bbuf_u32(b, (uint32_t)(v >> 32)) != 0 ||
        bbuf_u32(b, (uint32_t)(v & 0xffffffffu)) != 0) {
        return -1;
    }
    return 0;
}

static int bbuf_bytes(miro_ego_bbuf_t *b, const void *data, size_t len)
{
    if (len == 0u) {
        return 0;
    }
    if (bbuf_reserve(b, len) != 0) {
        return -1;
    }
    memcpy(b->data + b->len, data, len);
    b->len += len;
    return 0;
}

static int bbuf_zeros(miro_ego_bbuf_t *b, size_t n)
{
    if (n == 0u) {
        return 0;
    }
    if (bbuf_reserve(b, n) != 0) {
        return -1;
    }
    memset(b->data + b->len, 0, n);
    b->len += n;
    return 0;
}

static int bbuf_fourcc(miro_ego_bbuf_t *b, const char tag[4])
{
    return bbuf_bytes(b, tag, 4u);
}

static size_t box_begin(miro_ego_bbuf_t *b, const char tag[4], int *err)
{
    size_t pos = b->len;
    if (bbuf_u32(b, 0u) != 0 || bbuf_fourcc(b, tag) != 0) {
        *err = 1;
    }
    return pos;
}

static void box_end(miro_ego_bbuf_t *b, size_t pos, int *err)
{
    if (*err != 0 || b->len < pos || (b->len - pos) > UINT32_MAX) {
        *err = 1;
        return;
    }
    uint32_t sz = (uint32_t)(b->len - pos);
    b->data[pos] = (uint8_t)(sz >> 24);
    b->data[pos + 1] = (uint8_t)(sz >> 16);
    b->data[pos + 2] = (uint8_t)(sz >> 8);
    b->data[pos + 3] = (uint8_t)sz;
}

static void write_matrix_unity(miro_ego_bbuf_t *b, int *err)
{
    if (bbuf_u32(b, 0x00010000u) != 0 || bbuf_u32(b, 0u) != 0 || bbuf_u32(b, 0u) != 0 ||
        bbuf_u32(b, 0u) != 0 || bbuf_u32(b, 0x00010000u) != 0 || bbuf_u32(b, 0u) != 0 ||
        bbuf_u32(b, 0u) != 0 || bbuf_u32(b, 0u) != 0 || bbuf_u32(b, 0x40000000u) != 0) {
        *err = 1;
    }
}

static int write_all_fd(int fd, const uint8_t *buf, size_t len)
{
    size_t off = 0u;
    while (off < len) {
        ssize_t n = write(fd, buf + off, len - off);
        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }
            return -1;
        }
        if (n == 0) {
            errno = EIO;
            return -1;
        }
        off += (size_t)n;
    }
    return 0;
}

static int pwrite_all_fd(int fd, const uint8_t *buf, size_t len, off_t offset)
{
    size_t done = 0u;
    while (done < len) {
        ssize_t n = pwrite(fd, buf + done, len - done, offset + (off_t)done);
        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }
            return -1;
        }
        if (n == 0) {
            errno = EIO;
            return -1;
        }
        done += (size_t)n;
    }
    return 0;
}

void miro_ego_video_ps_free(miro_ego_video_ps_t *ps)
{
    if (ps == NULL) {
        return;
    }
    free(ps->sps);
    free(ps->pps);
    free(ps->vps);
    memset(ps, 0, sizeof(*ps));
}

void miro_ego_bbuf_free(miro_ego_bbuf_t *buf)
{
    if (buf == NULL) {
        return;
    }
    free(buf->data);
    memset(buf, 0, sizeof(*buf));
}

static size_t annexb_start_code_len(const uint8_t *p, size_t remaining)
{
    if (remaining >= 4u && p[0] == 0u && p[1] == 0u && p[2] == 0u && p[3] == 1u) {
        return 4u;
    }
    if (remaining >= 3u && p[0] == 0u && p[1] == 0u && p[2] == 1u) {
        return 3u;
    }
    return 0u;
}

int miro_ego_h264_has_idr(const unsigned char *data, size_t length)
{
    if (data == NULL || length < 4u) {
        return 0;
    }
    size_t i = 0u;
    while (i + 3u <= length) {
        size_t sc = annexb_start_code_len(data + i, length - i);
        if (sc == 0u) {
            i += 1u;
            continue;
        }
        size_t hdr = i + sc;
        if (hdr < length && (data[hdr] & 0x1fu) == 5u) {
            return 1;
        }
        i = hdr + 1u;
    }
    return 0;
}

int miro_ego_h265_has_idr(const unsigned char *data, size_t length)
{
    if (data == NULL || length < 5u) {
        return 0;
    }
    size_t i = 0u;
    while (i + 4u <= length) {
        size_t sc = annexb_start_code_len(data + i, length - i);
        if (sc == 0u) {
            i += 1u;
            continue;
        }
        size_t hdr = i + sc;
        if (hdr + 1u < length) {
            uint8_t nal_type = (uint8_t)((data[hdr] >> 1) & 0x3fu);
            uint8_t temporal_id = (uint8_t)(data[hdr + 1u] & 0x07u);
            if ((nal_type == 19u || nal_type == 20u) && temporal_id != 0u) {
                return 1;
            }
        }
        i = hdr + 1u;
    }
    return 0;
}

static int save_ps(uint8_t **dst, size_t *dst_len, const uint8_t *src, size_t len)
{
    if (*dst != NULL || len == 0u) {
        return 0;
    }
    uint8_t *copy = malloc(len);
    if (copy == NULL) {
        return -1;
    }
    memcpy(copy, src, len);
    *dst = copy;
    *dst_len = len;
    return 0;
}

int miro_ego_annexb_to_avcc(int hevc, const uint8_t *in, size_t in_len,
                            miro_ego_video_ps_t *ps, miro_ego_bbuf_t *avcc_out,
                            int *out_is_idr)
{
    if (in == NULL || avcc_out == NULL) {
        return -1;
    }
    avcc_out->len = 0u;
    if (out_is_idr != NULL) {
        *out_is_idr = 0;
    }

    size_t i = 0u;
    while (i + 3u <= in_len) {
        size_t sc = annexb_start_code_len(in + i, in_len - i);
        if (sc == 0u) {
            i += 1u;
            continue;
        }
        size_t hdr = i + sc;
        if (hdr >= in_len) {
            break;
        }
        size_t next = hdr + 1u;
        while (next + 3u <= in_len) {
            if (annexb_start_code_len(in + next, in_len - next) != 0u) {
                break;
            }
            next += 1u;
        }
        if (next + 3u > in_len) {
            next = in_len;
        }
        size_t nal_len = next - hdr;
        if (nal_len > 0u) {
            if (!hevc) {
                uint8_t nt = (uint8_t)(in[hdr] & 0x1fu);
                if (nt == 7u && ps != NULL) {
                    if (save_ps(&ps->sps, &ps->sps_len, in + hdr, nal_len) != 0) {
                        return -1;
                    }
                } else if (nt == 8u && ps != NULL) {
                    if (save_ps(&ps->pps, &ps->pps_len, in + hdr, nal_len) != 0) {
                        return -1;
                    }
                } else if (nt != 7u && nt != 8u) {
                    if (nt == 5u && out_is_idr != NULL) {
                        *out_is_idr = 1;
                    }
                    if (nal_len > UINT32_MAX ||
                        bbuf_u32(avcc_out, (uint32_t)nal_len) != 0 ||
                        bbuf_bytes(avcc_out, in + hdr, nal_len) != 0) {
                        return -1;
                    }
                }
            } else {
                uint8_t nt = (uint8_t)((in[hdr] >> 1) & 0x3fu);
                if (nt == 32u && ps != NULL) {
                    if (save_ps(&ps->vps, &ps->vps_len, in + hdr, nal_len) != 0) {
                        return -1;
                    }
                } else if (nt == 33u && ps != NULL) {
                    if (save_ps(&ps->sps, &ps->sps_len, in + hdr, nal_len) != 0) {
                        return -1;
                    }
                } else if (nt == 34u && ps != NULL) {
                    if (save_ps(&ps->pps, &ps->pps_len, in + hdr, nal_len) != 0) {
                        return -1;
                    }
                } else if (nt != 32u && nt != 33u && nt != 34u) {
                    if ((nt == 19u || nt == 20u) && nal_len >= 2u &&
                        (in[hdr + 1u] & 0x07u) != 0u && out_is_idr != NULL) {
                        *out_is_idr = 1;
                    }
                    if (nal_len > UINT32_MAX ||
                        bbuf_u32(avcc_out, (uint32_t)nal_len) != 0 ||
                        bbuf_bytes(avcc_out, in + hdr, nal_len) != 0) {
                        return -1;
                    }
                }
            }
        }
        i = next;
    }
    return 0;
}

static size_t rbsp_strip_emulation(const uint8_t *in, size_t in_len,
                                   uint8_t *out, size_t out_cap)
{
    size_t o = 0u;
    for (size_t i = 0u; i < in_len && o < out_cap; i += 1u) {
        if (i + 2u < in_len && in[i] == 0u && in[i + 1u] == 0u && in[i + 2u] == 3u) {
            out[o++] = 0u;
            if (o < out_cap) {
                out[o++] = 0u;
            }
            i += 2u;
        } else {
            out[o++] = in[i];
        }
    }
    return o;
}

static void write_colr_nclx(miro_ego_bbuf_t *b, int *err)
{
    size_t colr = box_begin(b, "colr", err);
    if (bbuf_fourcc(b, "nclx") != 0 ||
        bbuf_u16(b, 2u) != 0 || /* colour_primaries: unspecified */
        bbuf_u16(b, 2u) != 0 || /* transfer_characteristics: unspecified */
        bbuf_u16(b, 2u) != 0 || /* matrix_coefficients: unspecified */
        bbuf_u8(b, 0x80u) != 0) { /* full_range_flag = 1 */
        *err = 1;
    }
    box_end(b, colr, err);
}

static void write_avc1_sample_entry(miro_ego_bbuf_t *b, int vw, int vh,
                                    const miro_ego_video_ps_t *ps, int *err)
{
    static const uint8_t fallback_sps[4] = {0x67u, 0x42u, 0xc0u, 0x1eu};
    static const uint8_t fallback_pps[2] = {0x68u, 0xceu};
    const uint8_t *sps = (ps->sps != NULL && ps->sps_len >= 4u) ? ps->sps : fallback_sps;
    size_t sps_len = (ps->sps != NULL && ps->sps_len >= 4u) ? ps->sps_len : sizeof(fallback_sps);
    const uint8_t *pps = (ps->pps != NULL && ps->pps_len >= 1u) ? ps->pps : fallback_pps;
    size_t pps_len = (ps->pps != NULL && ps->pps_len >= 1u) ? ps->pps_len : sizeof(fallback_pps);

    size_t avc1 = box_begin(b, "avc1", err);
    if (bbuf_zeros(b, 6u) != 0 || bbuf_u16(b, 1u) != 0 || bbuf_zeros(b, 16u) != 0 ||
        bbuf_u16(b, (uint16_t)vw) != 0 || bbuf_u16(b, (uint16_t)vh) != 0 ||
        bbuf_u32(b, 0x00480000u) != 0 || bbuf_u32(b, 0x00480000u) != 0 ||
        bbuf_u32(b, 0u) != 0 || bbuf_u16(b, 1u) != 0 || bbuf_zeros(b, 32u) != 0 ||
        bbuf_u16(b, 0x0018u) != 0 || bbuf_u16(b, 0xffffu) != 0) {
        *err = 1;
    }

    size_t avcc = box_begin(b, "avcC", err);
    if (bbuf_u8(b, 1u) != 0 || bbuf_u8(b, sps[1]) != 0 ||
        bbuf_u8(b, sps[2]) != 0 || bbuf_u8(b, sps[3]) != 0 ||
        bbuf_u8(b, 0xffu) != 0 || bbuf_u8(b, 0xe1u) != 0 ||
        bbuf_u16(b, (uint16_t)sps_len) != 0 || bbuf_bytes(b, sps, sps_len) != 0 ||
        bbuf_u8(b, 1u) != 0 ||
        bbuf_u16(b, (uint16_t)pps_len) != 0 || bbuf_bytes(b, pps, pps_len) != 0) {
        *err = 1;
    }
    box_end(b, avcc, err);

    write_colr_nclx(b, err);
    box_end(b, avc1, err);
}

static void write_hvc1_sample_entry(miro_ego_bbuf_t *b, int vw, int vh,
                                    const miro_ego_video_ps_t *ps, int *err)
{
    uint8_t ptl_profile = 0x01u; /* Main */
    uint8_t ptl_compat[4] = {0x60u, 0x00u, 0x00u, 0x00u};
    uint8_t ptl_constr[6] = {0xb0u, 0x00u, 0x00u, 0x00u, 0x00u, 0x00u};
    uint8_t ptl_level = 120u; /* Level 4.0 */
    uint8_t num_temporal_layers = 1u;
    uint8_t temporal_nested = 1u;

    if (ps->sps != NULL && ps->sps_len >= 15u) {
        uint8_t rbsp[128] = {0};
        size_t rlen = rbsp_strip_emulation(ps->sps, ps->sps_len, rbsp, sizeof(rbsp));
        if (rlen >= 15u) {
            uint8_t max_sub_minus1 = (uint8_t)((rbsp[2] >> 1) & 0x07u);
            num_temporal_layers = (uint8_t)(max_sub_minus1 + 1u);
            temporal_nested = (uint8_t)(rbsp[2] & 0x01u);
            ptl_profile = rbsp[3];
            memcpy(ptl_compat, rbsp + 4u, 4u);
            memcpy(ptl_constr, rbsp + 8u, 6u);
            ptl_level = rbsp[14];
        }
    }

    size_t hvc1 = box_begin(b, "hvc1", err);
    if (bbuf_zeros(b, 6u) != 0 || bbuf_u16(b, 1u) != 0 || bbuf_zeros(b, 16u) != 0 ||
        bbuf_u16(b, (uint16_t)vw) != 0 || bbuf_u16(b, (uint16_t)vh) != 0 ||
        bbuf_u32(b, 0x00480000u) != 0 || bbuf_u32(b, 0x00480000u) != 0 ||
        bbuf_u32(b, 0u) != 0 || bbuf_u16(b, 1u) != 0 || bbuf_zeros(b, 32u) != 0 ||
        bbuf_u16(b, 0x0018u) != 0 || bbuf_u16(b, 0xffffu) != 0) {
        *err = 1;
    }

    size_t hvcc = box_begin(b, "hvcC", err);
    uint8_t misc_byte = (uint8_t)(((num_temporal_layers & 0x07u) << 3) |
                                  ((temporal_nested & 0x01u) << 2) | 3u);
    uint8_t n_arrays = 0u;
    if (ps->vps != NULL && ps->vps_len > 0u) n_arrays += 1u;
    if (ps->sps != NULL && ps->sps_len > 0u) n_arrays += 1u;
    if (ps->pps != NULL && ps->pps_len > 0u) n_arrays += 1u;

    if (bbuf_u8(b, 1u) != 0 ||
        bbuf_u8(b, ptl_profile) != 0 ||
        bbuf_bytes(b, ptl_compat, 4u) != 0 ||
        bbuf_bytes(b, ptl_constr, 6u) != 0 ||
        bbuf_u8(b, ptl_level) != 0 ||
        bbuf_u16(b, 0xf000u) != 0 || /* min_spatial_segmentation_idc */
        bbuf_u8(b, 0xfcu) != 0 ||    /* parallelismType */
        bbuf_u8(b, 0xfdu) != 0 ||    /* chromaFormat = 1 (4:2:0) */
        bbuf_u8(b, 0xf8u) != 0 ||    /* bitDepthLumaMinus8 = 0 */
        bbuf_u8(b, 0xf8u) != 0 ||    /* bitDepthChromaMinus8 = 0 */
        bbuf_u16(b, 0u) != 0 ||      /* avgFrameRate */
        bbuf_u8(b, misc_byte) != 0 ||
        bbuf_u8(b, n_arrays) != 0) {
        *err = 1;
    }

    struct {
        uint8_t nal_type;
        const uint8_t *data;
        size_t len;
    } arrays[3] = {
        {32u, ps->vps, ps->vps_len},
        {33u, ps->sps, ps->sps_len},
        {34u, ps->pps, ps->pps_len},
    };
    for (int a = 0; a < 3; a += 1) {
        if (arrays[a].data == NULL || arrays[a].len == 0u) {
            continue;
        }
        if (bbuf_u8(b, (uint8_t)(0x80u | arrays[a].nal_type)) != 0 ||
            bbuf_u16(b, 1u) != 0 ||
            bbuf_u16(b, (uint16_t)arrays[a].len) != 0 ||
            bbuf_bytes(b, arrays[a].data, arrays[a].len) != 0) {
            *err = 1;
        }
    }
    box_end(b, hvcc, err);

    write_colr_nclx(b, err);
    box_end(b, hvc1, err);
}

int miro_ego_fmp4_init(miro_ego_fmp4_t *mx, int fd, int hevc, int has_audio,
                       int eye_width, int height, uint32_t timescale)
{
    if (mx == NULL || fd < 0 || eye_width <= 0 || height <= 0 || timescale == 0u) {
        return -1;
    }
    memset(mx, 0, sizeof(*mx));
    mx->fd = fd;
    mx->hevc = hevc ? 1 : 0;
    mx->has_audio = has_audio ? 1 : 0;
    mx->width = eye_width;
    mx->height = height;
    mx->timescale = timescale;
    mx->num_tracks = mx->has_audio ? 4 : 3;
    for (int t = 0; t < mx->num_tracks; t += 1) {
        mx->tracks[t].track_id = (uint32_t)(t + 1);
    }
    return 0;
}

int miro_ego_fmp4_write_header(miro_ego_fmp4_t *mx, const miro_ego_video_ps_t ps[2])
{
    if (mx == NULL || mx->fd < 0 || ps == NULL) {
        return -1;
    }
    miro_ego_bbuf_t b = {0};
    int err = 0;

    size_t ftyp = box_begin(&b, "ftyp", &err);
    if (bbuf_fourcc(&b, "isom") != 0 || bbuf_u32(&b, 512u) != 0 ||
        bbuf_fourcc(&b, "isom") != 0 || bbuf_fourcc(&b, "iso5") != 0 ||
        bbuf_fourcc(&b, "mp41") != 0) {
        err = 1;
    }
    box_end(&b, ftyp, &err);

    size_t moov = box_begin(&b, "moov", &err);
    size_t mvhd = box_begin(&b, "mvhd", &err);
    if (bbuf_u32(&b, 0u) != 0 || bbuf_u32(&b, 0u) != 0 || bbuf_u32(&b, 0u) != 0 ||
        bbuf_u32(&b, mx->timescale) != 0) {
        err = 1;
    }
    mx->mvhd_dur_file_off = (uint64_t)b.len;
    if (bbuf_u32(&b, 0u) != 0 ||
        bbuf_u32(&b, 0x00010000u) != 0 || bbuf_u16(&b, 0x0100u) != 0 ||
        bbuf_zeros(&b, 10u) != 0) {
        err = 1;
    }
    write_matrix_unity(&b, &err);
    if (bbuf_zeros(&b, 24u) != 0 || bbuf_u32(&b, (uint32_t)(mx->num_tracks + 1)) != 0) {
        err = 1;
    }
    box_end(&b, mvhd, &err);

    size_t mvex = box_begin(&b, "mvex", &err);
    size_t mehd = box_begin(&b, "mehd", &err);
    if (bbuf_u32(&b, 0u) != 0) {
        err = 1;
    }
    mx->mehd_dur_file_off = (uint64_t)b.len;
    if (bbuf_u32(&b, 0u) != 0) {
        err = 1;
    }
    box_end(&b, mehd, &err);

    for (int t = 0; t < mx->num_tracks; t += 1) {
        size_t trex = box_begin(&b, "trex", &err);
        if (bbuf_u32(&b, 0u) != 0 || bbuf_u32(&b, mx->tracks[t].track_id) != 0 ||
            bbuf_u32(&b, 1u) != 0 || bbuf_u32(&b, 0u) != 0 ||
            bbuf_u32(&b, 0u) != 0 || bbuf_u32(&b, 0u) != 0) {
            err = 1;
        }
        box_end(&b, trex, &err);
    }
    box_end(&b, mvex, &err);

    for (int t = 0; t < mx->num_tracks; t += 1) {
        int is_v = (t < 2);
        int is_a = (mx->has_audio && t == 2);
        int is_m = !is_v && !is_a;

        size_t trak = box_begin(&b, "trak", &err);
        size_t tkhd = box_begin(&b, "tkhd", &err);
        if (bbuf_u32(&b, is_m ? 1u : 3u) != 0 ||
            bbuf_u32(&b, 0u) != 0 || bbuf_u32(&b, 0u) != 0 ||
            bbuf_u32(&b, mx->tracks[t].track_id) != 0 || bbuf_u32(&b, 0u) != 0) {
            err = 1;
        }
        mx->tracks[t].tkhd_dur_file_off = (uint64_t)b.len;
        if (bbuf_u32(&b, 0u) != 0 || bbuf_zeros(&b, 8u) != 0 ||
            bbuf_u16(&b, 0u) != 0 || bbuf_u16(&b, is_a ? 1u : 0u) != 0 ||
            bbuf_u16(&b, is_a ? 0x0100u : 0u) != 0 || bbuf_u16(&b, 0u) != 0) {
            err = 1;
        }
        write_matrix_unity(&b, &err);
        if (bbuf_u32(&b, is_v ? ((uint32_t)mx->width << 16) : 0u) != 0 ||
            bbuf_u32(&b, is_v ? ((uint32_t)mx->height << 16) : 0u) != 0) {
            err = 1;
        }
        box_end(&b, tkhd, &err);

        size_t mdia = box_begin(&b, "mdia", &err);
        size_t mdhd = box_begin(&b, "mdhd", &err);
        if (bbuf_u32(&b, 0u) != 0 || bbuf_u32(&b, 0u) != 0 || bbuf_u32(&b, 0u) != 0 ||
            bbuf_u32(&b, mx->timescale) != 0 || bbuf_u32(&b, 0u) != 0 ||
            bbuf_u16(&b, 0x55c4u) != 0 || bbuf_u16(&b, 0u) != 0) {
            err = 1;
        }
        box_end(&b, mdhd, &err);

        size_t hdlr = box_begin(&b, "hdlr", &err);
        const char *hn = is_v ? "VideoHandler" : (is_a ? "SoundHandler" : "MetaHandler");
        if (bbuf_u32(&b, 0u) != 0 || bbuf_u32(&b, 0u) != 0 ||
            bbuf_fourcc(&b, is_v ? "vide" : (is_a ? "soun" : "meta")) != 0 ||
            bbuf_zeros(&b, 12u) != 0 ||
            bbuf_bytes(&b, hn, strlen(hn) + 1u) != 0) {
            err = 1;
        }
        box_end(&b, hdlr, &err);

        size_t minf = box_begin(&b, "minf", &err);
        if (is_v) {
            size_t x = box_begin(&b, "vmhd", &err);
            if (bbuf_u32(&b, 1u) != 0 || bbuf_zeros(&b, 8u) != 0) err = 1;
            box_end(&b, x, &err);
        } else if (is_a) {
            size_t x = box_begin(&b, "smhd", &err);
            if (bbuf_u32(&b, 0u) != 0 || bbuf_zeros(&b, 4u) != 0) err = 1;
            box_end(&b, x, &err);
        } else {
            size_t x = box_begin(&b, "nmhd", &err);
            if (bbuf_u32(&b, 0u) != 0) err = 1;
            box_end(&b, x, &err);
        }

        size_t dinf = box_begin(&b, "dinf", &err);
        size_t dref = box_begin(&b, "dref", &err);
        if (bbuf_u32(&b, 0u) != 0 || bbuf_u32(&b, 1u) != 0) err = 1;
        size_t urlb = box_begin(&b, "url ", &err);
        if (bbuf_u32(&b, 1u) != 0) err = 1;
        box_end(&b, urlb, &err);
        box_end(&b, dref, &err);
        box_end(&b, dinf, &err);

        size_t stbl = box_begin(&b, "stbl", &err);
        size_t stsd = box_begin(&b, "stsd", &err);
        if (bbuf_u32(&b, 0u) != 0 || bbuf_u32(&b, 1u) != 0) err = 1;
        if (is_v) {
            if (mx->hevc) {
                write_hvc1_sample_entry(&b, mx->width, mx->height, &ps[t], &err);
            } else {
                write_avc1_sample_entry(&b, mx->width, mx->height, &ps[t], &err);
            }
        } else if (is_a) {
            size_t flac = box_begin(&b, "fLaC", &err);
            if (bbuf_zeros(&b, 6u) != 0 || bbuf_u16(&b, 1u) != 0 ||
                bbuf_zeros(&b, 8u) != 0 || bbuf_u16(&b, 2u) != 0 ||
                bbuf_u16(&b, 16u) != 0 || bbuf_u16(&b, 0u) != 0 ||
                bbuf_u16(&b, 0u) != 0 || bbuf_u32(&b, mx->timescale << 16) != 0) {
                err = 1;
            }
            size_t dfla = box_begin(&b, "dfLa", &err);
            uint64_t packed = ((uint64_t)mx->timescale << 44) |
                              ((uint64_t)1u << 41) | ((uint64_t)15u << 36);
            if (bbuf_u32(&b, 0u) != 0 || bbuf_u8(&b, 0x80u) != 0 ||
                bbuf_u24(&b, 34u) != 0 || bbuf_u16(&b, 16u) != 0 ||
                bbuf_u16(&b, 65535u) != 0 || bbuf_u24(&b, 0u) != 0 ||
                bbuf_u24(&b, 0u) != 0 || bbuf_u64(&b, packed) != 0 ||
                bbuf_zeros(&b, 16u) != 0) {
                err = 1;
            }
            box_end(&b, dfla, &err);
            box_end(&b, flac, &err);
        } else {
            size_t mett = box_begin(&b, "mett", &err);
            if (bbuf_zeros(&b, 6u) != 0 || bbuf_u16(&b, 1u) != 0 ||
                bbuf_u8(&b, 0u) != 0 ||
                bbuf_bytes(&b, MIRO_EGO_FMP4_META_MIME,
                           strlen(MIRO_EGO_FMP4_META_MIME) + 1u) != 0) {
                err = 1;
            }
            box_end(&b, mett, &err);
        }
        box_end(&b, stsd, &err);

        size_t s1 = box_begin(&b, "stts", &err);
        if (bbuf_u32(&b, 0u) != 0 || bbuf_u32(&b, 0u) != 0) err = 1;
        box_end(&b, s1, &err);
        size_t s2 = box_begin(&b, "stsc", &err);
        if (bbuf_u32(&b, 0u) != 0 || bbuf_u32(&b, 0u) != 0) err = 1;
        box_end(&b, s2, &err);
        size_t s3 = box_begin(&b, "stsz", &err);
        if (bbuf_u32(&b, 0u) != 0 || bbuf_u32(&b, 0u) != 0 || bbuf_u32(&b, 0u) != 0) err = 1;
        box_end(&b, s3, &err);
        size_t s4 = box_begin(&b, "stco", &err);
        if (bbuf_u32(&b, 0u) != 0 || bbuf_u32(&b, 0u) != 0) err = 1;
        box_end(&b, s4, &err);

        box_end(&b, stbl, &err);
        box_end(&b, minf, &err);
        box_end(&b, mdia, &err);
        box_end(&b, trak, &err);
    }
    box_end(&b, moov, &err);

    int rc = 0;
    if (err != 0 || write_all_fd(mx->fd, b.data, b.len) != 0) {
        rc = -1;
    } else {
        mx->file_pos = (uint64_t)b.len;
    }
    free(b.data);
    return rc;
}

int miro_ego_fmp4_push_sample(miro_ego_fmp4_t *mx, int track_idx, uint32_t duration,
                              const uint8_t *data, uint32_t size, uint32_t flags)
{
    if (mx == NULL || track_idx < 0 || track_idx >= mx->num_tracks ||
        (size > 0u && data == NULL)) {
        return -1;
    }
    miro_ego_fmp4_track_t *tr = &mx->tracks[track_idx];
    if (tr->n_samples == tr->cap_samples) {
        size_t ncap = tr->cap_samples ? tr->cap_samples * 2u : 256u;
        miro_ego_fmp4_sample_t *p = realloc(tr->samples, ncap * sizeof(*p));
        if (p == NULL) {
            return -1;
        }
        tr->samples = p;
        tr->cap_samples = ncap;
    }
    if (bbuf_bytes(&tr->payload, data, size) != 0) {
        return -1;
    }
    tr->samples[tr->n_samples++] = (miro_ego_fmp4_sample_t){
        .duration = duration,
        .size = size,
        .flags = flags,
    };
    return 0;
}

int miro_ego_fmp4_build_fragment(miro_ego_fmp4_t *mx, miro_ego_bbuf_t *out_frag,
                                 uint64_t *out_frag_off, uint64_t *out_frag_dur_ticks)
{
    if (mx == NULL || out_frag == NULL) {
        return -1;
    }
    out_frag->len = 0u;
    if (out_frag_off != NULL) {
        *out_frag_off = mx->file_pos;
    }
    if (out_frag_dur_ticks != NULL) {
        *out_frag_dur_ticks = 0u;
    }
    if (mx->tracks[0].n_samples == 0u) {
        return 0;
    }

    mx->moof_seq += 1u;
    uint64_t moof_off = mx->file_pos;
    size_t moof_sz = 8u + 16u;
    uint64_t mdat_sz_64 = 8u;
    for (int t = 0; t < mx->num_tracks; t += 1) {
        size_t s_cnt = mx->tracks[t].n_samples;
        if (s_cnt > (SIZE_MAX - moof_sz) / 12u) {
            return -1;
        }
        moof_sz += 8u + 16u + 20u + 20u + 12u * s_cnt;
        mdat_sz_64 += (uint64_t)mx->tracks[t].payload.len;
    }
    if (moof_sz > UINT32_MAX || mdat_sz_64 > UINT32_MAX) {
        return -1;
    }

    int err = 0;
    size_t moof_box = box_begin(out_frag, "moof", &err);
    size_t mfhd = box_begin(out_frag, "mfhd", &err);
    if (bbuf_u32(out_frag, 0u) != 0 || bbuf_u32(out_frag, mx->moof_seq) != 0) {
        err = 1;
    }
    box_end(out_frag, mfhd, &err);

    size_t mdat_cur = 8u;
    uint64_t track0_dur = 0u;
    for (int t = 0; t < mx->num_tracks; t += 1) {
        miro_ego_fmp4_track_t *tr = &mx->tracks[t];
        if (t < 2) {
            if (tr->n_mfra == tr->cap_mfra) {
                size_t ncap = tr->cap_mfra ? tr->cap_mfra * 2u : 128u;
                uint64_t *pt = realloc(tr->mfra_time, ncap * sizeof(uint64_t));
                if (pt != NULL) {
                    tr->mfra_time = pt;
                }
                uint64_t *po = realloc(tr->mfra_moof_off, ncap * sizeof(uint64_t));
                if (po != NULL) {
                    tr->mfra_moof_off = po;
                }
                if (pt == NULL || po == NULL) {
                    return -1;
                }
                tr->cap_mfra = ncap;
            }
            tr->mfra_time[tr->n_mfra] = tr->base_dts;
            tr->mfra_moof_off[tr->n_mfra] = moof_off;
            tr->n_mfra += 1u;
        }

        size_t traf = box_begin(out_frag, "traf", &err);
        size_t tfhd = box_begin(out_frag, "tfhd", &err);
        if (bbuf_u32(out_frag, 0x020000u) != 0 || bbuf_u32(out_frag, tr->track_id) != 0) {
            err = 1;
        }
        box_end(out_frag, tfhd, &err);

        size_t tfdt = box_begin(out_frag, "tfdt", &err);
        if (bbuf_u32(out_frag, 0x01000000u) != 0 || bbuf_u64(out_frag, tr->base_dts) != 0) {
            err = 1;
        }
        box_end(out_frag, tfdt, &err);

        size_t trun = box_begin(out_frag, "trun", &err);
        if (bbuf_u32(out_frag, 0x000701u) != 0 ||
            bbuf_u32(out_frag, (uint32_t)tr->n_samples) != 0 ||
            bbuf_u32(out_frag, (uint32_t)(moof_sz + mdat_cur)) != 0) {
            err = 1;
        }
        uint64_t dur = 0u;
        for (size_t s = 0u; s < tr->n_samples; s += 1u) {
            if (bbuf_u32(out_frag, tr->samples[s].duration) != 0 ||
                bbuf_u32(out_frag, tr->samples[s].size) != 0 ||
                bbuf_u32(out_frag, tr->samples[s].flags) != 0) {
                err = 1;
            }
            dur += (uint64_t)tr->samples[s].duration;
        }
        box_end(out_frag, trun, &err);
        box_end(out_frag, traf, &err);

        if (t == 0) {
            track0_dur = dur;
        }
        tr->base_dts += dur;
        mdat_cur += tr->payload.len;
    }
    box_end(out_frag, moof_box, &err);
    if (err != 0 || out_frag->len != moof_sz) {
        return -1;
    }

    if (bbuf_u32(out_frag, (uint32_t)mdat_sz_64) != 0 ||
        bbuf_fourcc(out_frag, "mdat") != 0) {
        return -1;
    }
    for (int t = 0; t < mx->num_tracks; t += 1) {
        if (bbuf_bytes(out_frag, mx->tracks[t].payload.data,
                       mx->tracks[t].payload.len) != 0) {
            return -1;
        }
        mx->tracks[t].n_samples = 0u;
        mx->tracks[t].payload.len = 0u;
    }

    mx->file_pos += (uint64_t)out_frag->len;
    if (out_frag_off != NULL) {
        *out_frag_off = moof_off;
    }
    if (out_frag_dur_ticks != NULL) {
        *out_frag_dur_ticks = track0_dur;
    }
    return 0;
}

int miro_ego_fmp4_build_mfra(const miro_ego_fmp4_t *mx, miro_ego_bbuf_t *out_mfra)
{
    if (mx == NULL || out_mfra == NULL) {
        return -1;
    }
    out_mfra->len = 0u;
    int err = 0;
    size_t mb = box_begin(out_mfra, "mfra", &err);
    for (int t = 0; t < 2; t += 1) {
        const miro_ego_fmp4_track_t *tr = &mx->tracks[t];
        size_t tfra = box_begin(out_mfra, "tfra", &err);
        if (bbuf_u32(out_mfra, 0x01000000u) != 0 ||
            bbuf_u32(out_mfra, tr->track_id) != 0 ||
            bbuf_u32(out_mfra, 0u) != 0 ||
            bbuf_u32(out_mfra, (uint32_t)tr->n_mfra) != 0) {
            err = 1;
        }
        for (size_t k = 0u; k < tr->n_mfra; k += 1u) {
            if (bbuf_u64(out_mfra, tr->mfra_time[k]) != 0 ||
                bbuf_u64(out_mfra, tr->mfra_moof_off[k]) != 0 ||
                bbuf_u8(out_mfra, 1u) != 0 ||
                bbuf_u8(out_mfra, 1u) != 0 ||
                bbuf_u8(out_mfra, 1u) != 0) {
                err = 1;
            }
        }
        box_end(out_mfra, tfra, &err);
    }
    size_t mfro = box_begin(out_mfra, "mfro", &err);
    if (bbuf_u32(out_mfra, 0u) != 0 || bbuf_u32(out_mfra, 0u) != 0) {
        err = 1;
    }
    box_end(out_mfra, mfro, &err);
    box_end(out_mfra, mb, &err);
    if (err != 0 || out_mfra->len < 4u || out_mfra->len > UINT32_MAX) {
        return -1;
    }
    uint32_t msz = (uint32_t)out_mfra->len;
    out_mfra->data[out_mfra->len - 4u] = (uint8_t)(msz >> 24);
    out_mfra->data[out_mfra->len - 3u] = (uint8_t)(msz >> 16);
    out_mfra->data[out_mfra->len - 2u] = (uint8_t)(msz >> 8);
    out_mfra->data[out_mfra->len - 1u] = (uint8_t)msz;
    return 0;
}

int miro_ego_fmp4_patch_moov_durations(const miro_ego_fmp4_t *mx, int fd,
                                       uint64_t duration_ticks)
{
    if (mx == NULL || fd < 0) {
        return -1;
    }
    uint32_t d32 = duration_ticks > UINT32_MAX ? UINT32_MAX : (uint32_t)duration_ticks;
    uint8_t be[4] = {
        (uint8_t)(d32 >> 24),
        (uint8_t)(d32 >> 16),
        (uint8_t)(d32 >> 8),
        (uint8_t)d32,
    };
    if (pwrite_all_fd(fd, be, 4u, (off_t)mx->mvhd_dur_file_off) != 0 ||
        pwrite_all_fd(fd, be, 4u, (off_t)mx->mehd_dur_file_off) != 0) {
        return -1;
    }
    for (int t = 0; t < mx->num_tracks; t += 1) {
        if (pwrite_all_fd(fd, be, 4u, (off_t)mx->tracks[t].tkhd_dur_file_off) != 0) {
            return -1;
        }
    }
    return 0;
}

void miro_ego_fmp4_destroy(miro_ego_fmp4_t *mx)
{
    if (mx == NULL) {
        return;
    }
    for (int t = 0; t < MIRO_EGO_FMP4_MAX_TRACKS; t += 1) {
        free(mx->tracks[t].samples);
        free(mx->tracks[t].mfra_time);
        free(mx->tracks[t].mfra_moof_off);
        miro_ego_bbuf_free(&mx->tracks[t].payload);
    }
    memset(mx, 0, sizeof(*mx));
}

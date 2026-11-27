#include "miro_ego_split.h"

#include <string.h>

#if defined(__ARM_NEON) || defined(__ARM_NEON__)
#include <arm_neon.h>
#endif

static inline void prefetch_read(const void *addr)
{
#if defined(__GNUC__) || defined(__clang__)
    __builtin_prefetch(addr, 0, 0);
#else
    (void)addr;
#endif
}

static inline void copy_row_fast(uint8_t *dst, const uint8_t *src, size_t width)
{
#if defined(__ARM_NEON) || defined(__ARM_NEON__)
    size_t offset = 0;
    while (width - offset >= 64u) {
        if (offset + 256u < width) {
            prefetch_read(src + offset + 256u);
        }
        uint8x16_t v0 = vld1q_u8(src + offset);
        uint8x16_t v1 = vld1q_u8(src + offset + 16u);
        uint8x16_t v2 = vld1q_u8(src + offset + 32u);
        uint8x16_t v3 = vld1q_u8(src + offset + 48u);
        vst1q_u8(dst + offset, v0);
        vst1q_u8(dst + offset + 16u, v1);
        vst1q_u8(dst + offset + 32u, v2);
        vst1q_u8(dst + offset + 48u, v3);
        offset += 64u;
    }
    while (width - offset >= 16u) {
        uint8x16_t v = vld1q_u8(src + offset);
        vst1q_u8(dst + offset, v);
        offset += 16u;
    }
    if (offset < width) {
        memcpy(dst + offset, src + offset, width - offset);
    }
#else
    memcpy(dst, src, width);
#endif
}

static int compute_nv12_dst_bytes(const miro_ego_nv12_frame_t *dst,
                                  int dst_stride,
                                  int height,
                                  uint32_t target_capacity,
                                  uint64_t *out_bytes)
{
    const uint64_t total_rows = (uint64_t)height + (uint64_t)height / 2ULL;
    if ((uint64_t)dst_stride > UINT64_MAX / total_rows) {
        return -1;
    }
    const uint64_t dst_y_bytes = (uint64_t)dst_stride * (uint64_t)height;
    const uint64_t dst_uv_bytes = (uint64_t)dst_stride * ((uint64_t)height / 2ULL);
    uint64_t dst_bytes = (uint64_t)dst_stride * total_rows;

    const uintptr_t y_addr = (uintptr_t)dst->y;
    const uintptr_t uv_addr = (uintptr_t)dst->uv;
    if (uv_addr >= y_addr && (uint64_t)(uv_addr - y_addr) <= (uint64_t)target_capacity) {
        const uint64_t uv_offset = (uint64_t)(uv_addr - y_addr);
        if (uv_offset < dst_y_bytes || uv_offset > UINT64_MAX - dst_uv_bytes) {
            return -1;
        }
        dst_bytes = uv_offset + dst_uv_bytes;
    }
    if (dst_bytes > (uint64_t)target_capacity || dst_bytes > UINT32_MAX) {
        return -1;
    }
    *out_bytes = dst_bytes;
    return 0;
}

int miro_ego_split_eye_nv12(const miro_ego_nv12_frame_t *src,
                            miro_ego_nv12_frame_t *dst,
                            int eye_width,
                            int height,
                            int x_offset)
{
    if (dst == NULL) {
        return -1;
    }
    const uint32_t target_capacity = dst->size;
    dst->size = 0;

    if (src == NULL ||
        eye_width <= 0 || height <= 0 ||
        (height % 2) != 0 || (eye_width % 2) != 0 ||
        x_offset < 0 || (x_offset % 2) != 0 ||
        src->y == NULL || src->uv == NULL ||
        dst->y == NULL || dst->uv == NULL) {
        return -1;
    }

    const int src_stride = src->stride > 0 ? src->stride : src->width;
    const int dst_stride = dst->stride > 0 ? dst->stride : eye_width;
    if (src_stride <= 0 || dst_stride <= 0 ||
        x_offset > src_stride - eye_width || dst_stride < eye_width) {
        return -1;
    }

    const uint64_t total_rows = (uint64_t)height + (uint64_t)height / 2ULL;
    if ((uint64_t)src_stride > UINT64_MAX / total_rows) {
        return -1;
    }

    const uint64_t src_bytes = (uint64_t)src_stride * total_rows;
    uint64_t dst_bytes = 0;
    if (src_bytes > (uint64_t)src->size ||
        compute_nv12_dst_bytes(dst, dst_stride, height, target_capacity, &dst_bytes) != 0) {
        return -1;
    }

    const uint8_t *src_y = src->y + (size_t)x_offset;
    const uint8_t *src_uv = src->uv + (size_t)x_offset;
    uint8_t *dst_y = dst->y;
    uint8_t *dst_uv = dst->uv;
    const size_t copy_len = (size_t)eye_width;
    const size_t s_stride = (size_t)src_stride;
    const size_t d_stride = (size_t)dst_stride;

    for (int row = 0; row < height; row += 1) {
        const uint8_t *src_row = src_y + (size_t)row * s_stride;
        if (row + 1 < height) {
            prefetch_read(src_row + s_stride);
        }
        copy_row_fast(dst_y + (size_t)row * d_stride, src_row, copy_len);
    }

    const int chroma_rows = height / 2;
    for (int row = 0; row < chroma_rows; row += 1) {
        const uint8_t *src_row = src_uv + (size_t)row * s_stride;
        if (row + 1 < chroma_rows) {
            prefetch_read(src_row + s_stride);
        }
        copy_row_fast(dst_uv + (size_t)row * d_stride, src_row, copy_len);
    }

    dst->width = eye_width;
    dst->height = height;
    dst->size = (uint32_t)dst_bytes;
    return 0;
}

int miro_ego_downscale_nv12_half(const miro_ego_nv12_frame_t *src,
                                 miro_ego_nv12_frame_t *dst)
{
    if (dst == NULL) {
        return -1;
    }
    const uint32_t target_capacity = dst->size;
    dst->size = 0;

    if (src == NULL ||
        src->width <= 0 || src->height <= 0 ||
        (src->width % 4) != 0 || (src->height % 4) != 0 ||
        src->y == NULL || src->uv == NULL ||
        dst->y == NULL || dst->uv == NULL) {
        return -1;
    }

    const int dst_w = src->width / 2;
    const int dst_h = src->height / 2;
    const int src_stride = src->stride > 0 ? src->stride : src->width;
    const int dst_stride = dst->stride > 0 ? dst->stride : dst_w;
    if (src_stride < src->width || dst_stride < dst_w) {
        return -1;
    }

    const uint64_t src_rows = (uint64_t)src->height + (uint64_t)src->height / 2ULL;
    if ((uint64_t)src_stride > UINT64_MAX / src_rows) {
        return -1;
    }
    const uint64_t src_bytes = (uint64_t)src_stride * src_rows;
    uint64_t dst_bytes = 0;
    if (src_bytes > (uint64_t)src->size ||
        compute_nv12_dst_bytes(dst, dst_stride, dst_h, target_capacity, &dst_bytes) != 0) {
        return -1;
    }

    /* 2x2 luma box filter */
    for (int y = 0; y < dst_h; y += 1) {
        const uint8_t *r0 = src->y + (size_t)(y * 2) * (size_t)src_stride;
        const uint8_t *r1 = r0 + (size_t)src_stride;
        uint8_t *out = dst->y + (size_t)y * (size_t)dst_stride;
        for (int x = 0; x < dst_w; x += 1) {
            const unsigned sum = (unsigned)r0[x * 2] + (unsigned)r0[x * 2 + 1] +
                                 (unsigned)r1[x * 2] + (unsigned)r1[x * 2 + 1] + 2u;
            out[x] = (uint8_t)(sum >> 2);
        }
    }

    /* 2x2 interleaved UV box filter (preserving U/V pair lanes) */
    const int dst_uv_h = dst_h / 2;
    const int dst_uv_pairs = dst_w / 2;
    for (int y = 0; y < dst_uv_h; y += 1) {
        const uint8_t *r0 = src->uv + (size_t)(y * 2) * (size_t)src_stride;
        const uint8_t *r1 = r0 + (size_t)src_stride;
        uint8_t *out = dst->uv + (size_t)y * (size_t)dst_stride;
        for (int p = 0; p < dst_uv_pairs; p += 1) {
            const size_t sx = (size_t)p * 4u;
            const unsigned u_sum = (unsigned)r0[sx] + (unsigned)r0[sx + 2u] +
                                   (unsigned)r1[sx] + (unsigned)r1[sx + 2u] + 2u;
            const unsigned v_sum = (unsigned)r0[sx + 1u] + (unsigned)r0[sx + 3u] +
                                   (unsigned)r1[sx + 1u] + (unsigned)r1[sx + 3u] + 2u;
            out[p * 2] = (uint8_t)(u_sum >> 2);
            out[p * 2 + 1] = (uint8_t)(v_sum >> 2);
        }
    }

    dst->width = dst_w;
    dst->height = dst_h;
    dst->size = (uint32_t)dst_bytes;
    return 0;
}

static int cpu_split_eye(void *ctx,
                         const miro_ego_nv12_frame_t *src,
                         miro_ego_nv12_frame_t *dst,
                         int eye_width,
                         int height,
                         int x_offset)
{
    (void)ctx;
    return miro_ego_split_eye_nv12(src, dst, eye_width, height, x_offset);
}

static int cpu_split_stereo(void *ctx,
                            const miro_ego_nv12_frame_t *src,
                            miro_ego_nv12_frame_t *left_dst,
                            miro_ego_nv12_frame_t *right_dst,
                            miro_ego_nv12_frame_t *preview_dst)
{
    (void)ctx;
    if (src == NULL || left_dst == NULL || right_dst == NULL ||
        src->width <= 0 || (src->width % 4) != 0 || src->height <= 0 || (src->height % 2) != 0) {
        return -1;
    }
    const int eye_width = src->width / 2;
    if (miro_ego_split_eye_nv12(src, left_dst, eye_width, src->height, 0) != 0) {
        return -1;
    }
    if (miro_ego_split_eye_nv12(src, right_dst, eye_width, src->height, eye_width) != 0) {
        return -1;
    }
    if (preview_dst != NULL) {
        if (miro_ego_downscale_nv12_half(left_dst, preview_dst) != 0) {
            return -1;
        }
    }
    return 0;
}

static const miro_ego_split_ops_t CPU_SPLIT_OPS = {
#if defined(__ARM_NEON) || defined(__ARM_NEON__)
    .name = "cpu-neon",
#else
    .name = "cpu-scalar",
#endif
    .init = NULL,
    .split_eye = cpu_split_eye,
    .split_stereo = cpu_split_stereo,
    .destroy = NULL,
};

const miro_ego_split_ops_t *miro_ego_split_ops_cpu(void)
{
    return &CPU_SPLIT_OPS;
}

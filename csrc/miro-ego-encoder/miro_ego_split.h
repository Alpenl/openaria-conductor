/*
 * NV12 side-by-side stereo split & preview downscale abstraction.
 *
 * Default CPU backend uses ARM NEON vectorized row copies with software
 * prefetching on aarch64/ARM targets and a portable C fallback on x86_64.
 * The `miro_ego_split_ops_t` vtable provides a clean hook for plugging in a
 * Hobot VSE hardware crop + preview downscale backend on RDK X5.
 */

#ifndef MIRO_EGO_SPLIT_H
#define MIRO_EGO_SPLIT_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct {
    uint8_t *y;      /* Y luma plane pointer */
    uint8_t *uv;     /* interleaved UV chroma plane pointer */
    int width;
    int height;
    int stride;      /* bytes per row (0 => tightly packed width) */
    uint32_t size;   /* capacity in bytes on input; written bytes on output */
} miro_ego_nv12_frame_t;

typedef struct {
    int enabled;
    int width;       /* target preview width (even) */
    int height;      /* target preview height (even) */
    int eye_index;   /* 0 = left eye, 1 = right eye */
} miro_ego_preview_config_t;

typedef struct miro_ego_split_ops {
    const char *name;

    /* Optional backend initialization (may be NULL). */
    int (*init)(void **ctx, int sbs_width, int height,
                const miro_ego_preview_config_t *preview);

    /*
     * Extracts a single eye (eye_width x height) at horizontal byte offset
     * x_offset from a side-by-side NV12 frame into dst.
     * Sets dst->size = 0 and returns -1 on any validation failure.
     */
    int (*split_eye)(void *ctx,
                     const miro_ego_nv12_frame_t *src,
                     miro_ego_nv12_frame_t *dst,
                     int eye_width,
                     int height,
                     int x_offset);

    /*
     * Combined dual-eye split plus optional preview downscale hook
     * (ready for single-pass Hobot VSE hardware crop + downscale).
     * preview_dst may be NULL if preview output is not requested.
     */
    int (*split_stereo)(void *ctx,
                        const miro_ego_nv12_frame_t *src,
                        miro_ego_nv12_frame_t *left_dst,
                        miro_ego_nv12_frame_t *right_dst,
                        miro_ego_nv12_frame_t *preview_dst);

    /* Optional backend teardown (may be NULL). */
    void (*destroy)(void *ctx);
} miro_ego_split_ops_t;

/* Direct NV12 single-eye split helper (used by CPU backend & unit tests). */
int miro_ego_split_eye_nv12(const miro_ego_nv12_frame_t *src,
                            miro_ego_nv12_frame_t *dst,
                            int eye_width,
                            int height,
                            int x_offset);

/* 2x2 NV12 half-resolution preview downscale helper (CPU reference). */
int miro_ego_downscale_nv12_half(const miro_ego_nv12_frame_t *src,
                                 miro_ego_nv12_frame_t *dst);

/* Returns the default CPU split operations vtable (NEON on ARM, scalar C otherwise). */
const miro_ego_split_ops_t *miro_ego_split_ops_cpu(void);

#ifdef __cplusplus
}
#endif

#endif /* MIRO_EGO_SPLIT_H */

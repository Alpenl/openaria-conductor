/*
 * Live side-by-side MJPEG -> dual-eye H.264/H.265 hardware pipeline for RDK X5.
 *
 * The pipeline owns the JPU decoder, the stereo row splitter, both VPU encoders,
 * segmented MP4 muxing, hot page-cache SHA-256 sealing, and page eviction.
 * Callers submit one side-by-side JPEG per encoded frame and receive a callback
 * once a segment index has closed and sealed on both eyes.
 *
 * Fail-closed design: every frame that cannot progress cleanly through JPU,
 * split, VPU, muxer, metadata fixup, and durable seal enters a failed state and
 * is reported immediately, never silently skipped.
 */

#ifndef MIRO_EGO_STEREO_PIPELINE_H
#define MIRO_EGO_STEREO_PIPELINE_H

#include <stddef.h>

#include "miro_ego_split.h"

#ifdef __cplusplus
extern "C" {
#endif

#define MIRO_EGO_EYES 2

typedef struct miro_ego_pipeline miro_ego_pipeline_t;

typedef struct {
    int sbs_width;      /* side-by-side frame width, e.g. 3840 */
    int height;         /* frame height, e.g. 1080 */
    int fps;            /* encoded frame rate after decimation, e.g. 30 */
    int bitrate_kbps;   /* per eye */
    int hevc;           /* 0 = H.264, 1 = H.265 */
    int rate_control;   /* 0 = CBR, 1 = VBR, 2 = fixed QP, 3 = AVBR */
    int intra_period;   /* IDR interval in frames; 0 selects fps */
    int min_qp;
    int max_qp;
    int intra_qp;
    int initial_qp;
    int vbv_ms;
    int segment_frames; /* frames per segment; 0 keeps a single open segment */
    const char *out_dir;     /* directory that receives the segment files */
    const char *path_prefix; /* relative prefix reported back, e.g. "video/" */
    const miro_ego_split_ops_t *split_ops; /* NULL selects default CPU/NEON backend */
    miro_ego_preview_config_t preview;     /* optional preview hook for VSE backend */
} miro_ego_pipeline_config_t;

typedef struct {
    unsigned long long offered;         /* frames handed to submit() */
    unsigned long long submitted;       /* frames the JPU accepted */
    unsigned long long decoded;         /* NV12 frames the JPU produced */
    unsigned long long encoded[MIRO_EGO_EYES];
    unsigned long long bytes[MIRO_EGO_EYES];
    unsigned long long drop_decoder_input;
    unsigned long long drop_encoder_input;
    unsigned long long drop_decoder_output;
    unsigned long long write_queue_peak_bytes[MIRO_EGO_EYES];
    unsigned long long write_queue_peak_frames[MIRO_EGO_EYES];
    unsigned long long write_queue_rejected[MIRO_EGO_EYES];
    unsigned long long max_write_ns[MIRO_EGO_EYES];
    unsigned long long max_seal_ns;
} miro_ego_pipeline_stats_t;

/*
 * Invoked once a segment index has closed, been hashed, fsynced, and evicted
 * from page cache on both eyes. Paths are relative to the session root
 * (path_prefix + file name). SHA-256 strings are 64 lowercase hex characters.
 * The callback runs on an internal thread outside ledger_lock and must not call
 * back into the pipeline.
 */
typedef void (*miro_ego_segment_closed_fn)(void *user, int index,
                                           unsigned long long start_frame,
                                           unsigned long long end_frame,
                                           const char *left_path,
                                           unsigned long long left_bytes,
                                           const char *left_sha256,
                                           const char *right_path,
                                           unsigned long long right_bytes,
                                           const char *right_sha256);

int miro_ego_pipeline_open(const miro_ego_pipeline_config_t *config,
                           miro_ego_segment_closed_fn on_segment, void *user,
                           miro_ego_pipeline_t **out, char *error, size_t error_len);

/* Returns 0 when the frame entered the JPU, -1 when it was dropped or failed. */
int miro_ego_pipeline_submit(miro_ego_pipeline_t *pipeline, const unsigned char *jpeg,
                             size_t length, int timeout_ms);

/* Drains the codecs and closes/seals the trailing segment. */
int miro_ego_pipeline_finish(miro_ego_pipeline_t *pipeline);

void miro_ego_pipeline_close(miro_ego_pipeline_t *pipeline);

void miro_ego_pipeline_stats(const miro_ego_pipeline_t *pipeline,
                             miro_ego_pipeline_stats_t *out);

/* Non-zero once the pipeline has entered a failed state. */
int miro_ego_pipeline_failed(const miro_ego_pipeline_t *pipeline);

/* Non-empty once the pipeline has entered a failed state. */
const char *miro_ego_pipeline_error(const miro_ego_pipeline_t *pipeline);

#ifdef __cplusplus
}
#endif

#endif /* MIRO_EGO_STEREO_PIPELINE_H */

/*
 * Live side-by-side MJPEG -> dual-eye H.264 pipeline for RDK X5 (issue #46).
 *
 * The pipeline owns the JPU decoder, the row split and both VPU H.264 encoders,
 * and writes segmented MP4 straight to disk. Callers push one side-by-side JPEG
 * per encoded frame and are told when a segment index has closed on both eyes.
 *
 * It is fail-closed: every frame that cannot be pushed all the way through is
 * counted as an application drop and reported, never silently skipped.
 */

#ifndef YLX_STEREO_PIPELINE_H
#define YLX_STEREO_PIPELINE_H

#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

#define YLX_EYES 2

typedef struct ylx_pipeline ylx_pipeline_t;

typedef struct {
    int sbs_width;      /* side-by-side frame width, e.g. 3840 */
    int height;         /* frame height, e.g. 1080 */
    int fps;            /* encoded frame rate after decimation, e.g. 30 */
    int bitrate_kbps;   /* per eye */
    int intra_period;   /* IDR interval in frames; 0 selects fps */
    int min_qp;
    int intra_qp;
    int initial_qp;
    int vbv_ms;
    int segment_frames; /* frames per segment; 0 keeps a single open segment */
    const char *out_dir;     /* directory that receives the segment files */
    const char *path_prefix; /* relative prefix reported back, e.g. "video/" */
} ylx_pipeline_config_t;

typedef struct {
    unsigned long long offered;         /* frames handed to submit() */
    unsigned long long submitted;       /* frames the JPU accepted */
    unsigned long long decoded;         /* NV12 frames the JPU produced */
    unsigned long long encoded[YLX_EYES];
    unsigned long long bytes[YLX_EYES];
    unsigned long long drop_decoder_input;
    unsigned long long drop_encoder_input;
    unsigned long long drop_decoder_output;
} ylx_pipeline_stats_t;

/*
 * Invoked once a segment index has closed on both eyes. Paths are relative to
 * the session root (path_prefix + file name). The callback runs on an internal
 * thread and must not call back into the pipeline.
 */
typedef void (*ylx_segment_closed_fn)(void *user, int index,
                                      unsigned long long start_frame,
                                      unsigned long long end_frame,
                                      const char *left_path,
                                      unsigned long long left_bytes,
                                      const char *right_path,
                                      unsigned long long right_bytes);

int ylx_pipeline_open(const ylx_pipeline_config_t *config,
                      ylx_segment_closed_fn on_segment, void *user,
                      ylx_pipeline_t **out, char *error, size_t error_len);

/* Returns 0 when the frame entered the JPU, -1 when it was dropped or failed. */
int ylx_pipeline_submit(ylx_pipeline_t *pipeline, const unsigned char *jpeg,
                        size_t length, int timeout_ms);

/* Drains the codecs and closes the trailing segment. */
int ylx_pipeline_finish(ylx_pipeline_t *pipeline);

void ylx_pipeline_close(ylx_pipeline_t *pipeline);

void ylx_pipeline_stats(const ylx_pipeline_t *pipeline, ylx_pipeline_stats_t *out);

/* Non-empty once the pipeline has entered a failed state. */
const char *ylx_pipeline_error(const ylx_pipeline_t *pipeline);

#ifdef __cplusplus
}
#endif

#endif /* YLX_STEREO_PIPELINE_H */

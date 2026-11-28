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
#include <stdint.h>

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
    int segment_frames; /* legacy alias for fragment_frames */
    int fragment_frames;/* frames per [moof][mdat] fragment; 0 selects intra_period */
    int timescale;      /* unified sample timescale (default 48000 Hz) */
    int audio_tracks;   /* 1 = include lossless FLAC audio track, 0 = video+meta only */
    const char *out_dir;     /* directory that receives output_file */
    const char *path_prefix; /* legacy field retained for ABI compatibility */
    const char *output_file; /* filename inside out_dir, default "session.mp4" */
    const miro_ego_split_ops_t *split_ops; /* NULL selects default CPU/NEON backend */
    miro_ego_preview_config_t preview;     /* optional preview hook for VSE backend */
    int input_stall_timeout_ms;            /* 0 selects default 5000ms backpressure ceiling */
} miro_ego_pipeline_config_t;

typedef struct {
    uint64_t frame_seq;
    uint64_t monotonic_ns;
    uint64_t sample_number;
    uint32_t duration_ticks;
    const uint8_t *pcm;
    uint32_t pcm_len;
    const uint8_t *meta_json;
    uint32_t meta_len;
} miro_ego_sync_bundle_t;

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
    unsigned long long muxed_frames;
    unsigned long long fragments_written;
    unsigned long long total_ticks;
    unsigned long long fdatasync_count;
    unsigned long long sum_fdatasync_ns;
    unsigned long long max_fdatasync_ns;
} miro_ego_pipeline_stats_t;

/*
 * Invoked once a standalone [moof][mdat] fragment has been written and
 * fdatasynced to disk cleanly on the async writer thread.
 */
typedef void (*miro_ego_fragment_fn)(void *user, int index,
                                     unsigned long long start_frame,
                                     unsigned long long end_frame,
                                     unsigned long long duration_ticks,
                                     unsigned long long frag_offset,
                                     unsigned long long frag_bytes);

/*
 * Invoked once at finish() after mfra append, moov duration patching,
 * final fdatasync, full-file SHA-256 verification, and out_dir fsync.
 */
typedef void (*miro_ego_sealed_fn)(void *user,
                                   const char *artifact_path,
                                   unsigned long long bytes,
                                   const char *sha256_hex,
                                   unsigned long long total_frames,
                                   unsigned long long total_ticks,
                                   unsigned long long moof_count,
                                   int has_audio);

int miro_ego_pipeline_open(const miro_ego_pipeline_config_t *config,
                           miro_ego_fragment_fn on_fragment,
                           miro_ego_sealed_fn on_sealed,
                           void *user,
                           miro_ego_pipeline_t **out, char *error, size_t error_len);

/* Returns 0 when the frame entered the JPU, -1 when it was dropped or failed. */
int miro_ego_pipeline_submit(miro_ego_pipeline_t *pipeline, const unsigned char *jpeg,
                             size_t length, int timeout_ms);

/* Submits one MJPEG frame together with its synchronized audio PCM slice & meta JSON. */
int miro_ego_pipeline_submit_bundle(miro_ego_pipeline_t *pipeline,
                                    const unsigned char *jpeg, size_t length,
                                    const miro_ego_sync_bundle_t *bundle,
                                    int timeout_ms);

/* Drains the codecs, writes mfra + moov duration patch, hashes, and seals session.mp4. */
int miro_ego_pipeline_finish(miro_ego_pipeline_t *pipeline);

void miro_ego_pipeline_close(miro_ego_pipeline_t *pipeline);

void miro_ego_pipeline_stats(const miro_ego_pipeline_t *pipeline,
                             miro_ego_pipeline_stats_t *out);

/* Non-zero once the pipeline has entered a failed state. */
int miro_ego_pipeline_failed(const miro_ego_pipeline_t *pipeline);

/* Non-empty once the pipeline has entered a failed state. */
const char *miro_ego_pipeline_error(const miro_ego_pipeline_t *pipeline);

/* Requests immediate stop of bounded input backpressure retry loops (signal-safe). */
void miro_ego_pipeline_request_stop(miro_ego_pipeline_t *pipeline);

#ifdef __cplusplus
}
#endif

#endif /* MIRO_EGO_STEREO_PIPELINE_H */

#define _GNU_SOURCE
#include "miro_ego_stereo_pipeline.h"
#include "miro_ego_mp4_meta.h"
#include "miro_ego_packet_queue.h"
#include "miro_ego_sha256.h"
#include "miro_ego_split.h"

#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdarg.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#include "hb_media_codec.h"
#include "hb_media_muxer.h"

#if defined(HB_SDK_ROOT) && !defined(MIRO_EGO_STUB_HB_MEDIA_CODEC_H)
#include "hb_mem_mgr.h"
#elif !defined(MIRO_EGO_STUB_HB_MEDIA_CODEC_H)
extern int32_t hb_mem_invalidate_buf_with_vaddr(uint64_t virt_addr, uint64_t size)
    __attribute__((weak));
extern int32_t hb_mem_flush_buf_with_vaddr(uint64_t virt_addr, uint64_t size)
    __attribute__((weak));
#endif

#ifndef HB_MEDIA_ERR_WAIT_TIME
#define HB_MEDIA_ERR_WAIT_TIME (-268435443)
#endif

#define DECODER_INPUT_TIMEOUT_MS 200
#define DECODER_OUTPUT_TIMEOUT_MS 200
#define ENCODER_INPUT_TIMEOUT_MS 200
#define ENCODER_OUTPUT_TIMEOUT_MS 200
#define DRAIN_TIMEOUT_NS 5000000000ULL /* 5s ceiling on a stuck JPU/VPU drain */
#define DEFAULT_INPUT_STALL_TIMEOUT_MS 5000 /* 5s continuous input backpressure ceiling */
#define INPUT_RETRY_MIN_BACKOFF_NS 2000000ULL /* 2ms sleep floor when SDK returns immediately */
#define ERROR_LEN 256
#define PATH_LEN 512
#define MUX_QUEUE_BYTES (64u * 1024u * 1024u) /* per eye, compressed packets */
#define MUX_QUEUE_SECONDS 32u
/* Bound active muxers independently of closed files waiting for hash + disk sync.
 * Both limits include their respective in-flight work. */
#define MAX_PENDING_SEGMENTS 32u
#define MAX_PENDING_COMMITS 64u

typedef struct segment {
    struct segment *next;
    media_muxer_context_t muxer;
    int seal_fd;
    int eye;
    int index;
    char absolute[PATH_LEN];
    char relative[PATH_LEN];
    unsigned long long start_frame;
    unsigned long long end_frame;
    unsigned long long finalize_ns;
    miro_ego_sha256_ctx_t sha256_ctx;
    unsigned long long sealed_bytes;
    int sha256_ready;
} segment_t;

typedef struct {
    miro_ego_pipeline_t *pipeline;
    int eye;
    media_codec_context_t codec;
    pthread_t thread;
    pthread_t writer_thread;
    miro_ego_packet_queue_t packets;
    int codec_started;
    int output_started;
    int writer_started;
    int packets_initialized;
    atomic_ullong max_write_ns;
    segment_t *current;
    unsigned long long ordinal; /* encoded frames emitted so far */
} eye_t;

typedef struct closed_pair {
    struct closed_pair *next;
    int index;
    int eyes;
    char paths[MIRO_EGO_EYES][PATH_LEN];
    char sha256[MIRO_EGO_EYES][MIRO_EGO_SHA256_HEX_LEN];
    unsigned long long bytes[MIRO_EGO_EYES];
    unsigned long long start;
    unsigned long long end;
} closed_pair_t;

struct miro_ego_pipeline {
    miro_ego_pipeline_config_t config;
    char *out_dir_owned;
    char *path_prefix_owned;
    int eye_width;
    int segment_frames;

    const miro_ego_split_ops_t *split_ops;
    void *split_ctx;
    int split_ops_initialized;

    /* Reusable 256 KiB page-aligned read buffers for closer and committer threads. */
    void *closer_io_buf;
    void *seal_io_buf;

    media_codec_context_t decoder;
    eye_t eyes[MIRO_EGO_EYES];
    pthread_t split_thread;
    int decoder_started;
    int split_started;
    int closer_started;
    int committer_started;
    int finish_lock_initialized;
    int ledger_initialized;
    int error_initialized;
    int closer_lock_initialized;
    int closer_signal_initialized;
    int commit_signal_initialized;
    pthread_mutex_t finish_lock;
    int finished;

    miro_ego_segment_closed_fn on_segment;
    void *user;

    /* Release the vendor muxer and finish the MP4 index before queuing hash,
     * fsync, fadvise(DONTNEED), and directory sync. Slow durability must not
     * occupy active muxer slots. */
    pthread_t closer_thread;
    pthread_mutex_t closer_lock;
    pthread_cond_t closer_signal;
    segment_t *closer_head;
    segment_t *closer_tail;
    int closer_stop;
    atomic_uint pending_segments;
    pthread_t committer_thread;
    pthread_cond_t commit_signal;
    segment_t *commit_head;
    segment_t *commit_tail;
    int commit_stop;
    unsigned pending_commits;

    pthread_mutex_t ledger_lock;
    /* Eyes can seal at different speeds. Keep all pending pairs until the
     * contiguous prefix is complete instead of overwriting the last eye. */
    closed_pair_t *closed_pairs;
    int pending_pairs;
    int reported_pairs;
    atomic_ullong max_seal_ns;

    atomic_ullong offered;
    atomic_ullong submitted;
    atomic_ullong decoded;
    atomic_ullong fed[MIRO_EGO_EYES];
    atomic_ullong encoded[MIRO_EGO_EYES];
    atomic_ullong bytes[MIRO_EGO_EYES];
    atomic_ullong drop_decoder_input;
    atomic_ullong drop_encoder_input;
    atomic_ullong drop_decoder_output;

    atomic_int stopping;        /* no more frames will be submitted */
    atomic_int abort_requested; /* immediate signal/abort stop */
    atomic_int split_done;      /* the split thread fed the encoders everything */
    atomic_int failed;
    pthread_mutex_t error_lock;
    char error[ERROR_LEN];
};

static const char *const EYE_NAMES[MIRO_EGO_EYES] = {"left", "right"};

static unsigned long long monotonic_time_ns(void);
static void observe_max(atomic_ullong *counter, unsigned long long elapsed);

static void fail(miro_ego_pipeline_t *pipeline, const char *format, ...)
    __attribute__((format(printf, 2, 3)));

static void fail(miro_ego_pipeline_t *pipeline, const char *format, ...)
{
    va_list arguments;
    pthread_mutex_lock(&pipeline->error_lock);
    if (pipeline->error[0] == '\0') {
        va_start(arguments, format);
        vsnprintf(pipeline->error, sizeof(pipeline->error), format, arguments);
        va_end(arguments);
    }
    pthread_mutex_unlock(&pipeline->error_lock);
    atomic_store(&pipeline->failed, 1);
}

/* ---------------------------------------------------------------- codec --- */

static int codec_start(media_codec_context_t *context)
{
    int ret = hb_mm_mc_initialize(context);
    if (ret != 0) {
        return ret;
    }
    ret = hb_mm_mc_configure(context);
    if (ret != 0) {
        hb_mm_mc_release(context);
        return ret;
    }
    mc_av_codec_startup_params_t startup;
    memset(&startup, 0, sizeof(startup));
    ret = hb_mm_mc_start(context, &startup);
    if (ret != 0) {
        hb_mm_mc_release(context);
        return ret;
    }
    return 0;
}

static void codec_stop(media_codec_context_t *context)
{
    hb_mm_mc_pause(context);
    hb_mm_mc_stop(context);
    hb_mm_mc_release(context);
}

static void configure_decoder(media_codec_context_t *context,
                              const miro_ego_pipeline_config_t *config)
{
    memset(context, 0, sizeof(*context));
    context->codec_id = MEDIA_CODEC_ID_MJPEG;
    context->encoder = false;
    mc_video_codec_dec_params_t *params = &context->video_dec_params;
    params->feed_mode = MC_FEEDING_MODE_FRAME_SIZE;
    params->pix_fmt = MC_PIXEL_FORMAT_NV12;
    params->bitstream_buf_size =
        (uint32_t)(((uint64_t)config->sbs_width * (uint64_t)config->height * 3ULL / 2ULL + 0x3ffULL) & ~0x3ffULL);
    params->bitstream_buf_count = 6;
    params->frame_buf_count = 6;
    params->mjpeg_dec_config.rot_degree = MC_CCW_0;
    params->mjpeg_dec_config.mir_direction = MC_DIRECTION_NONE;
    params->mjpeg_dec_config.frame_crop_enable = false;
}

static int configure_encoder(media_codec_context_t *context,
                             const miro_ego_pipeline_config_t *config, int eye_width)
{
    memset(context, 0, sizeof(*context));
    context->codec_id = config->hevc ? MEDIA_CODEC_ID_H265 : MEDIA_CODEC_ID_H264;
    context->encoder = true;
    mc_video_codec_enc_params_t *params = &context->video_enc_params;
    params->width = eye_width;
    params->height = config->height;
    params->pix_fmt = MC_PIXEL_FORMAT_NV12;
    params->bitstream_buf_size =
        (uint32_t)(((uint64_t)eye_width * (uint64_t)config->height * 3ULL / 2ULL + 0x3ffULL) & ~0x3ffULL);
    params->frame_buf_count = 8;
    params->external_frame_buf = false;
    params->bitstream_buf_count = 8;
    /* X5 Wave521CL encodes no B frames and keeps a single reference. */
    params->gop_params.gop_preset_idx = 9;
    params->gop_params.decoding_refresh_type = 2;
    params->rot_degree = MC_CCW_0;
    params->mir_direction = MC_DIRECTION_NONE;
    params->frame_cropping_flag = false;
    params->enable_user_pts = 0;

    const mc_video_rate_control_mode_t modes[2][4] = {
        {MC_AV_RC_MODE_H264CBR, MC_AV_RC_MODE_H264VBR, MC_AV_RC_MODE_H264FIXQP, MC_AV_RC_MODE_H264AVBR},
        {MC_AV_RC_MODE_H265CBR, MC_AV_RC_MODE_H265VBR, MC_AV_RC_MODE_H265FIXQP, MC_AV_RC_MODE_H265AVBR},
    };
    params->rc_params.mode = modes[config->hevc][config->rate_control];
    int ret = hb_mm_mc_get_rate_control_config(context, &params->rc_params);
    if (ret != 0) {
        return ret;
    }
    params->rc_params.mode = modes[config->hevc][config->rate_control];
    const uint32_t period = (uint32_t)(config->intra_period > 0 ? config->intra_period : config->fps);

    /* Never alias the H.264 and HEVC union members. */
#define SET_BUDGET_RC(member, block_field) do { \
    params->rc_params.member.frame_rate = (uint32_t)config->fps; \
    params->rc_params.member.bit_rate = (uint32_t)config->bitrate_kbps; \
    params->rc_params.member.intra_period = period; \
    params->rc_params.member.intra_qp = (uint32_t)config->intra_qp; \
    params->rc_params.member.initial_rc_qp = config->initial_qp; \
    params->rc_params.member.vbv_buffer_size = config->vbv_ms; \
    params->rc_params.member.block_field = 1; \
    params->rc_params.member.min_qp_I = params->rc_params.member.min_qp_P = \
        params->rc_params.member.min_qp_B = (uint32_t)config->min_qp; \
    params->rc_params.member.max_qp_I = params->rc_params.member.max_qp_P = \
        params->rc_params.member.max_qp_B = (uint32_t)config->max_qp; \
    params->rc_params.member.hvs_qp_enable = 1; \
    params->rc_params.member.hvs_qp_scale = 2; \
    params->rc_params.member.max_delta_qp = 10; \
    params->rc_params.member.qp_map_enable = 0; \
} while (0)
#define SET_VBR(member) do { \
    params->rc_params.member.frame_rate = (uint32_t)config->fps; \
    params->rc_params.member.intra_period = period; \
    params->rc_params.member.intra_qp = (uint32_t)config->intra_qp; \
    params->rc_params.member.qp_map_enable = 0; \
} while (0)
#define SET_FIXQP(member) do { \
    params->rc_params.member.frame_rate = (uint32_t)config->fps; \
    params->rc_params.member.intra_period = period; \
    params->rc_params.member.force_qp_I = (uint32_t)config->intra_qp; \
    params->rc_params.member.force_qp_P = params->rc_params.member.force_qp_B = (uint32_t)config->initial_qp; \
} while (0)
    if (config->hevc) {
        params->h265_enc_config.main_still_picture_profile_enable = 0;
        params->h265_enc_config.h265_level = MC_H265_LEVEL_UNSPECIFIED;
        switch (config->rate_control) {
        case 0:
            SET_BUDGET_RC(h265_cbr_params, ctu_level_rc_enalbe);
            break;
        case 1:
            SET_VBR(h265_vbr_params);
            break;
        case 2:
            SET_FIXQP(h265_fixqp_params);
            break;
        case 3:
            SET_BUDGET_RC(h265_avbr_params, ctu_level_rc_enalbe);
            break;
        }
    } else {
        switch (config->rate_control) {
        case 0:
            SET_BUDGET_RC(h264_cbr_params, mb_level_rc_enalbe);
            break;
        case 1:
            SET_VBR(h264_vbr_params);
            break;
        case 2:
            SET_FIXQP(h264_fixqp_params);
            break;
        case 3:
            SET_BUDGET_RC(h264_avbr_params, mb_level_rc_enalbe);
            break;
        }
        params->h264_enc_config.h264_profile = MC_H264_PROFILE_HP;
        params->h264_enc_config.h264_level = MC_H264_LEVEL_UNSPECIFIED;
    }
#undef SET_BUDGET_RC
#undef SET_VBR
#undef SET_FIXQP
    return 0;
}

/* --------------------------------------------------------------- muxer --- */

static void close_seal_fd(segment_t *segment)
{
    if (segment != NULL && segment->seal_fd >= 0) {
        close(segment->seal_fd);
        segment->seal_fd = -1;
    }
}

static int segment_open(miro_ego_pipeline_t *pipeline, eye_t *eye, int index,
                        unsigned long long start_frame)
{
    unsigned pending = atomic_load(&pipeline->pending_segments);
    do {
        if (pending >= MAX_PENDING_SEGMENTS) {
            fail(pipeline, "segment_queue_exhausted: pending=%u limit=%u",
                 pending, MAX_PENDING_SEGMENTS);
            return -1;
        }
    } while (!atomic_compare_exchange_weak(&pipeline->pending_segments, &pending,
                                           pending + 1u));

    segment_t *segment = calloc(1, sizeof(*segment));
    if (segment == NULL) {
        atomic_fetch_sub(&pipeline->pending_segments, 1u);
        fail(pipeline, "out_of_memory: segment %d", index);
        return -1;
    }
    segment->seal_fd = -1;
    segment->eye = eye->eye;
    segment->index = index;
    segment->start_frame = start_frame;
    miro_ego_sha256_init(&segment->sha256_ctx);

    int rel_len = snprintf(segment->relative, sizeof(segment->relative), "%s%s_%05d.mp4",
                           pipeline->config.path_prefix, EYE_NAMES[eye->eye], index);
    int abs_len = snprintf(segment->absolute, sizeof(segment->absolute), "%s/%s_%05d.mp4",
                           pipeline->config.out_dir, EYE_NAMES[eye->eye], index);
    if (rel_len < 0 || (size_t)rel_len >= sizeof(segment->relative) ||
        abs_len < 0 || (size_t)abs_len >= sizeof(segment->absolute)) {
        fail(pipeline, "segment_path_too_long: eye=%s index=%d",
             EYE_NAMES[eye->eye], index);
        free(segment);
        atomic_fetch_sub(&pipeline->pending_segments, 1u);
        return -1;
    }

    /* Open seal_fd before the vendor muxer writes byte 0 so struct file->f_wb_err
     * samples the inode errseq_t cursor prior to any page-cache writeback error. */
    do {
        segment->seal_fd = open(segment->absolute,
                                O_RDWR | O_CREAT | O_TRUNC | O_NOFOLLOW | O_CLOEXEC,
                                0640);
    } while (segment->seal_fd < 0 && errno == EINTR);
    if (segment->seal_fd < 0) {
        fail(pipeline, "segment_open_failed: open %s: %s",
             segment->relative, strerror(errno));
        free(segment);
        atomic_fetch_sub(&pipeline->pending_segments, 1u);
        return -1;
    }

    if (hb_mm_mx_get_default_context(&segment->muxer) != 0) {
        fail(pipeline, "muxer_unavailable: default context for %s", segment->relative);
        close_seal_fd(segment);
        unlink(segment->absolute);
        free(segment);
        atomic_fetch_sub(&pipeline->pending_segments, 1u);
        return -1;
    }
    segment->muxer.output_file_name = segment->absolute;
    segment->muxer.output_format = MEDIA_MUXER_OUTPUT_FORMAT_MP4;
    if (hb_mm_mx_initialize(&segment->muxer) != 0) {
        fail(pipeline, "muxer_unavailable: initialize %s", segment->relative);
        close_seal_fd(segment);
        unlink(segment->absolute);
        free(segment);
        atomic_fetch_sub(&pipeline->pending_segments, 1u);
        return -1;
    }

    mx_stream_params_t params;
    memset(&params, 0, sizeof(params));
    params.codec_id = pipeline->config.hevc ? MEDIA_CODEC_ID_H265 : MEDIA_CODEC_ID_H264;
    params.numerator = 1;
    params.denominator = pipeline->config.fps;
    params.video_params.width = pipeline->eye_width;
    params.video_params.height = pipeline->config.height;
    params.video_params.frame_rate = (uint32_t)pipeline->config.fps;
    params.video_params.bit_rate = (uint32_t)pipeline->config.bitrate_kbps;
    params.video_params.pix_fmt = MC_PIXEL_FORMAT_NV12;
    if (hb_mm_mx_add_stream(&segment->muxer, &params) < 0 ||
        hb_mm_mx_start(&segment->muxer) != 0) {
        fail(pipeline, "muxer_unavailable: start %s", segment->relative);
        hb_mm_mx_stop(&segment->muxer);
        close_seal_fd(segment);
        unlink(segment->absolute);
        free(segment);
        atomic_fetch_sub(&pipeline->pending_segments, 1u);
        return -1;
    }
    eye->current = segment;
    return 0;
}

static void ledger_record(miro_ego_pipeline_t *pipeline, int eye_index, int segment_index,
                          const char *relative, unsigned long long bytes,
                          const char sha256[MIRO_EGO_SHA256_HEX_LEN],
                          unsigned long long start_frame, unsigned long long end_frame)
{
    pthread_mutex_lock(&pipeline->ledger_lock);
    if (segment_index < pipeline->reported_pairs) {
        fail(pipeline, "segment_boundary_mismatch: segment=%d", segment_index);
        pthread_mutex_unlock(&pipeline->ledger_lock);
        return;
    }
    closed_pair_t **position = &pipeline->closed_pairs;
    while (*position != NULL && (*position)->index < segment_index) {
        position = &(*position)->next;
    }
    closed_pair_t *pair = *position;
    if (pair == NULL || pair->index != segment_index) {
        pair = pipeline->pending_pairs < 128 ? calloc(1, sizeof(*pair)) : NULL;
        if (pair == NULL) {
            fail(pipeline, "segment_ledger_exhausted: pending pairs=%d",
                 pipeline->pending_pairs);
            pthread_mutex_unlock(&pipeline->ledger_lock);
            return;
        }
        pair->index = segment_index;
        pair->start = start_frame;
        pair->end = end_frame;
        pair->next = *position;
        *position = pair;
        pipeline->pending_pairs += 1;
    }
    if (pair->start != start_frame || pair->end != end_frame ||
        (pair->eyes & (1 << eye_index)) != 0) {
        fail(pipeline, "segment_boundary_mismatch: segment=%d", segment_index);
        pthread_mutex_unlock(&pipeline->ledger_lock);
        return;
    }
    int path_len = snprintf(pair->paths[eye_index], PATH_LEN, "%s", relative);
    if (path_len < 0 || (size_t)path_len >= PATH_LEN) {
        fail(pipeline, "segment_path_too_long: segment=%d", segment_index);
        pthread_mutex_unlock(&pipeline->ledger_lock);
        return;
    }
    memcpy(pair->sha256[eye_index], sha256, MIRO_EGO_SHA256_HEX_LEN);
    pair->sha256[eye_index][MIRO_EGO_SHA256_HEX_LEN - 1u] = '\0';
    pair->eyes |= 1 << eye_index;
    pair->bytes[eye_index] = bytes;

    closed_pair_t *ready_head = NULL;
    closed_pair_t **ready_tail = &ready_head;
    while ((pair = pipeline->closed_pairs) != NULL && pair->eyes == 3 &&
           pair->index == pipeline->reported_pairs) {
        pipeline->closed_pairs = pair->next;
        pipeline->pending_pairs -= 1;
        pipeline->reported_pairs += 1;
        pair->next = NULL;
        *ready_tail = pair;
        ready_tail = &pair->next;
    }
    pthread_mutex_unlock(&pipeline->ledger_lock);

    while (ready_head != NULL) {
        closed_pair_t *ready = ready_head;
        ready_head = ready->next;
        if (pipeline->on_segment != NULL) {
            pipeline->on_segment(pipeline->user, ready->index, ready->start, ready->end,
                                 ready->paths[0], ready->bytes[0], ready->sha256[0],
                                 ready->paths[1], ready->bytes[1], ready->sha256[1]);
        }
        free(ready);
    }
}

static void queue_segment(segment_t **head, segment_t **tail, segment_t *segment)
{
    segment->next = NULL;
    if (*tail != NULL) {
        (*tail)->next = segment;
    } else {
        *head = segment;
    }
    *tail = segment;
}

/* Hands the finished segment to the closer thread; never blocks the encoder. */
static void segment_close(miro_ego_pipeline_t *pipeline, eye_t *eye)
{
    segment_t *segment = eye->current;
    if (segment == NULL) {
        return;
    }
    eye->current = NULL;
    segment->end_frame = eye->ordinal;
    pthread_mutex_lock(&pipeline->closer_lock);
    queue_segment(&pipeline->closer_head, &pipeline->closer_tail, segment);
    pthread_cond_signal(&pipeline->closer_signal);
    pthread_mutex_unlock(&pipeline->closer_lock);
}

static int segment_finalize(miro_ego_pipeline_t *pipeline, segment_t *segment)
{
    if (segment->end_frame == segment->start_frame) {
        /* A segment with zero frames has no media; release muxer best-effort and unlink. */
        (void)hb_mm_mx_stop(&segment->muxer);
        close_seal_fd(segment);
        unlink(segment->absolute);
        return 0;
    }
    if (hb_mm_mx_stop(&segment->muxer) != 0) {
        close_seal_fd(segment);
        fail(pipeline, "segment_seal_failed: stop %s", segment->relative);
        return 0;
    }
    char reason[128] = {0};
    if (miro_ego_mp4_extend_durations_and_hash_fd(segment->seal_fd, pipeline->config.fps,
                                                  pipeline->closer_io_buf,
                                                  MIRO_EGO_SHA256_IO_BUF_SIZE,
                                                  &segment->sha256_ctx,
                                                  &segment->sealed_bytes,
                                                  reason, sizeof(reason)) != 0) {
        close_seal_fd(segment);
        fail(pipeline, "segment_seal_failed: %s duration fixup: %s",
             segment->relative, reason);
        return 0;
    }
    segment->sha256_ready = 1;
    return 1;
}

static void segment_seal(miro_ego_pipeline_t *pipeline, segment_t *segment)
{
    /*
     * Reuse segment->seal_fd opened before byte 0 so fdatasync() observes any
     * asynchronous page-cache writeback failure via f_wb_err, finalize the
     * incremental SHA-256 populated during MP4 finalize, fdatasync() for
     * durability, and evict clean pages via posix_fadvise(POSIX_FADV_DONTNEED).
     */
    int fd = segment->seal_fd;
    segment->seal_fd = -1;
    if (fd < 0) {
        fail(pipeline, "segment_seal_failed: missing descriptor for %s",
             segment->relative);
        return;
    }
    struct stat metadata;
    memset(&metadata, 0, sizeof(metadata));
    if (fstat(fd, &metadata) != 0 || metadata.st_size < 0) {
        fail(pipeline, "segment_seal_failed: fstat %s: %s",
             segment->relative, strerror(errno));
        close(fd);
        return;
    }
    const unsigned long long bytes = (unsigned long long)metadata.st_size;

    char sha256_hex[MIRO_EGO_SHA256_HEX_LEN] = {0};
    unsigned long long hashed_bytes = 0;
    if (segment->sha256_ready) {
        uint8_t digest[MIRO_EGO_SHA256_DIGEST_LEN];
        miro_ego_sha256_final(&segment->sha256_ctx, digest);
        miro_ego_sha256_to_hex(digest, sha256_hex);
        hashed_bytes = segment->sealed_bytes;
    } else {
        int hash_ret = pipeline->seal_io_buf != NULL
                           ? miro_ego_sha256_fd_with_buf(fd, pipeline->seal_io_buf,
                                                         MIRO_EGO_SHA256_IO_BUF_SIZE,
                                                         sha256_hex, &hashed_bytes)
                           : miro_ego_sha256_fd(fd, sha256_hex, &hashed_bytes);
        if (hash_ret != 0) {
            fail(pipeline, "segment_seal_failed: sha256 %s: %s",
                 segment->relative, strerror(errno));
            close(fd);
            return;
        }
    }
    if (hashed_bytes != bytes) {
        fail(pipeline, "segment_seal_failed: size_mismatch %s: stat=%llu hashed=%llu",
             segment->relative, bytes, hashed_bytes);
        close(fd);
        return;
    }
    int sync_ret;
    do {
        sync_ret = fdatasync(fd);
    } while (sync_ret < 0 && errno == EINTR);
    if (sync_ret < 0 && (errno == EINVAL || errno == ENOSYS || errno == EOPNOTSUPP)) {
        do {
            sync_ret = fsync(fd);
        } while (sync_ret < 0 && errno == EINTR);
    }
    if (sync_ret != 0) {
        fail(pipeline, "segment_seal_failed: sync %s: %s",
             segment->relative, strerror(errno));
        close(fd);
        return;
    }
    /* Evict clean pages after fsync to keep exFAT writeback queues flat. */
    (void)posix_fadvise(fd, 0, 0, POSIX_FADV_DONTNEED);
    close(fd);

    int dir_fd;
    do {
        dir_fd = open(pipeline->config.out_dir,
                      O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC);
    } while (dir_fd < 0 && errno == EINTR);
    if (dir_fd < 0) {
        fail(pipeline, "segment_seal_failed: open video directory: %s", strerror(errno));
        return;
    }
    do {
        sync_ret = fsync(dir_fd);
    } while (sync_ret < 0 && errno == EINTR);
    if (sync_ret != 0 && (errno == EINVAL || errno == ENOSYS || errno == EOPNOTSUPP)) {
        do {
            sync_ret = syncfs(dir_fd);
        } while (sync_ret < 0 && errno == EINTR);
    }
    if (sync_ret != 0 && errno != EINVAL && errno != ENOSYS && errno != EOPNOTSUPP) {
        fail(pipeline, "segment_seal_failed: sync video directory: %s", strerror(errno));
        close(dir_fd);
        return;
    }
    close(dir_fd);

    ledger_record(pipeline, segment->eye, segment->index, segment->relative, bytes,
                  sha256_hex, segment->start_frame, segment->end_frame);
}

static segment_t *wait_segment(miro_ego_pipeline_t *pipeline, pthread_cond_t *signal,
                               segment_t **head, segment_t **tail, const int *stopping)
{
    pthread_mutex_lock(&pipeline->closer_lock);
    while (*head == NULL && !*stopping) {
        pthread_cond_wait(signal, &pipeline->closer_lock);
    }
    segment_t *segment = *head;
    if (segment != NULL) {
        *head = segment->next;
        if (*head == NULL) {
            *tail = NULL;
        }
    }
    pthread_mutex_unlock(&pipeline->closer_lock);
    return segment;
}

static void *closer_thread_main(void *argument)
{
    miro_ego_pipeline_t *pipeline = (miro_ego_pipeline_t *)argument;
    segment_t *segment;
    while ((segment = wait_segment(pipeline, &pipeline->closer_signal,
                                   &pipeline->closer_head, &pipeline->closer_tail,
                                   &pipeline->closer_stop)) != NULL) {
        unsigned long long started = monotonic_time_ns();
        int finalized = segment_finalize(pipeline, segment);
        segment->finalize_ns = monotonic_time_ns() - started;
        atomic_fetch_sub(&pipeline->pending_segments, 1u);
        if (!finalized) {
            observe_max(&pipeline->max_seal_ns, segment->finalize_ns);
            free(segment);
            continue;
        }
        pthread_mutex_lock(&pipeline->closer_lock);
        unsigned pending = pipeline->pending_commits;
        if (pending < MAX_PENDING_COMMITS) {
            pipeline->pending_commits += 1u;
            queue_segment(&pipeline->commit_head, &pipeline->commit_tail, segment);
            pthread_cond_signal(&pipeline->commit_signal);
        }
        pthread_mutex_unlock(&pipeline->closer_lock);
        if (pending >= MAX_PENDING_COMMITS) {
            fail(pipeline, "segment_commit_queue_exhausted: pending=%u limit=%u",
                 pending, MAX_PENDING_COMMITS);
            close_seal_fd(segment);
            free(segment);
        }
    }
    return NULL;
}

static void *committer_thread_main(void *argument)
{
    miro_ego_pipeline_t *pipeline = (miro_ego_pipeline_t *)argument;
    segment_t *segment;
    while ((segment = wait_segment(pipeline, &pipeline->commit_signal,
                                   &pipeline->commit_head, &pipeline->commit_tail,
                                   &pipeline->commit_stop)) != NULL) {
        unsigned long long started = monotonic_time_ns();
        segment_seal(pipeline, segment);
        observe_max(&pipeline->max_seal_ns, segment->finalize_ns + monotonic_time_ns() - started);
        free(segment);
        pthread_mutex_lock(&pipeline->closer_lock);
        pipeline->pending_commits -= 1u;
        pthread_mutex_unlock(&pipeline->closer_lock);
    }
    return NULL;
}

/* -------------------------------------------------------------- threads --- */

static unsigned long long monotonic_time_ns(void)
{
    struct timespec value = {0};
    if (clock_gettime(CLOCK_MONOTONIC, &value) != 0) {
        return 0;
    }
    return (unsigned long long)value.tv_sec * 1000000000ULL + (unsigned long long)value.tv_nsec;
}

static void observe_max(atomic_ullong *counter, unsigned long long elapsed)
{
    unsigned long long previous = atomic_load(counter);
    while (previous < elapsed && !atomic_compare_exchange_weak(counter, &previous, elapsed)) {
    }
}

static int is_transient_dequeue_timeout(int ret)
{
    return ret == HB_MEDIA_ERR_WAIT_TIME ||
           ret == -ETIMEDOUT ||
           ret == -EAGAIN ||
           ret == -EBUSY;
}

static void sleep_ns(unsigned long long ns)
{
    if (ns == 0ULL) {
        return;
    }
    struct timespec req = {
        .tv_sec = (time_t)(ns / 1000000000ULL),
        .tv_nsec = (long)(ns % 1000000000ULL),
    };
    while (nanosleep(&req, &req) == -1 && errno == EINTR) {
    }
}

static void backoff_if_fast_retry(unsigned long long call_started_ns)
{
    const unsigned long long now_ns = monotonic_time_ns();
    if (now_ns >= call_started_ns && (now_ns - call_started_ns) < INPUT_RETRY_MIN_BACKOFF_NS) {
        sleep_ns(INPUT_RETRY_MIN_BACKOFF_NS - (now_ns - call_started_ns));
    }
}

static int drain_deadline_expired(int *draining,
                                  unsigned long long *drain_started_ns,
                                  unsigned long long timeout_ns)
{
    const unsigned long long now_ns = monotonic_time_ns();
    if (!*draining) {
        *draining = 1;
        *drain_started_ns = now_ns;
        return 0;
    }
    return now_ns >= *drain_started_ns && (now_ns - *drain_started_ns) >= timeout_ns;
}

static int dequeue_input_with_retry(miro_ego_pipeline_t *pipeline,
                                    media_codec_context_t *codec,
                                    media_codec_buffer_t *buffer,
                                    int slice_timeout_ms,
                                    int respect_stopping)
{
    const int stall_ms = pipeline->config.input_stall_timeout_ms > 0
                             ? pipeline->config.input_stall_timeout_ms
                             : DEFAULT_INPUT_STALL_TIMEOUT_MS;
    const unsigned long long stall_ceiling_ns = (unsigned long long)stall_ms * 1000000ULL;
    const unsigned long long started_ns = monotonic_time_ns();
    int poll_ms = slice_timeout_ms > 0 ? slice_timeout_ms : DECODER_INPUT_TIMEOUT_MS;
    if (poll_ms > stall_ms) {
        poll_ms = stall_ms;
    }

    for (;;) {
        const unsigned long long call_started_ns = monotonic_time_ns();
        memset(buffer, 0, sizeof(*buffer));
        buffer->type = codec->encoder ? MC_VIDEO_FRAME_BUFFER : MC_VIDEO_STREAM_BUFFER;
        int ret = hb_mm_mc_dequeue_input_buffer(codec, buffer, poll_ms);
        if (ret == 0) {
            return 0;
        }
        if (!is_transient_dequeue_timeout(ret)) {
            return ret;
        }
        if (atomic_load(&pipeline->failed) != 0 ||
            atomic_load(&pipeline->abort_requested) != 0 ||
            (respect_stopping && atomic_load(&pipeline->stopping) != 0)) {
            return ret;
        }
        const unsigned long long now_ns = monotonic_time_ns();
        if (now_ns >= started_ns && (now_ns - started_ns) >= stall_ceiling_ns) {
            return ret;
        }
        backoff_if_fast_retry(call_started_ns);
    }
}

static void ion_invalidate_range(const uint8_t *ptr, uint64_t size)
{
    if (ptr == NULL || size == 0ULL) {
        return;
    }
#if defined(HB_SDK_ROOT)
    (void)hb_mem_invalidate_buf_with_vaddr((uint64_t)(uintptr_t)ptr, size);
#elif !defined(MIRO_EGO_STUB_HB_MEDIA_CODEC_H)
    if (hb_mem_invalidate_buf_with_vaddr != NULL) {
        (void)hb_mem_invalidate_buf_with_vaddr((uint64_t)(uintptr_t)ptr, size);
    }
#else
    (void)ptr;
    (void)size;
#endif
}

static void ion_flush_range(const uint8_t *ptr, uint64_t size)
{
    if (ptr == NULL || size == 0ULL) {
        return;
    }
#if defined(HB_SDK_ROOT)
    (void)hb_mem_flush_buf_with_vaddr((uint64_t)(uintptr_t)ptr, size);
#elif !defined(MIRO_EGO_STUB_HB_MEDIA_CODEC_H)
    if (hb_mem_flush_buf_with_vaddr != NULL) {
        (void)hb_mem_flush_buf_with_vaddr((uint64_t)(uintptr_t)ptr, size);
    }
#else
    (void)ptr;
    (void)size;
#endif
}

static void ion_cache_nv12_frame(const media_codec_buffer_t *frame, int invalidate)
{
    if (frame == NULL || frame->vframe_buf.size == 0u ||
        frame->vframe_buf.vir_ptr[0] == NULL) {
        return;
    }
    void (*op)(const uint8_t *, uint64_t) =
        invalidate ? ion_invalidate_range : ion_flush_range;
    const int stride = frame->vframe_buf.stride > 0
                           ? frame->vframe_buf.stride
                           : frame->vframe_buf.width;
    const int height = frame->vframe_buf.height;
    if (stride <= 0 || height <= 0) {
        op(frame->vframe_buf.vir_ptr[0], frame->vframe_buf.size);
        return;
    }
    const uint64_t y_bytes = (uint64_t)stride * (uint64_t)height;
    const uint64_t uv_bytes = (uint64_t)stride * (uint64_t)(height / 2);
    const uintptr_t y_addr = (uintptr_t)frame->vframe_buf.vir_ptr[0];
    const uintptr_t uv_addr = (uintptr_t)frame->vframe_buf.vir_ptr[1];
    if (frame->vframe_buf.vir_ptr[1] != NULL &&
        uv_addr >= y_addr &&
        (uint64_t)(uv_addr - y_addr) <= (uint64_t)frame->vframe_buf.size) {
        const uint64_t uv_offset = (uint64_t)(uv_addr - y_addr);
        const uint64_t total_bytes = uv_offset + uv_bytes;
        op(frame->vframe_buf.vir_ptr[0],
           total_bytes <= (uint64_t)frame->vframe_buf.size
               ? total_bytes
               : (uint64_t)frame->vframe_buf.size);
    } else {
        op(frame->vframe_buf.vir_ptr[0], y_bytes);
        op(frame->vframe_buf.vir_ptr[1], uv_bytes);
    }
}

static void ion_invalidate_nv12_frame(const media_codec_buffer_t *frame)
{
    ion_cache_nv12_frame(frame, 1);
}

static void ion_flush_nv12_frame(const media_codec_buffer_t *frame)
{
    ion_cache_nv12_frame(frame, 0);
}

static void *mux_writer_thread(void *argument)
{
    eye_t *eye = (eye_t *)argument;
    miro_ego_pipeline_t *pipeline = eye->pipeline;
    int writable = 1;
    miro_ego_packet_t *packet;
    while ((packet = miro_ego_packet_queue_pop(&eye->packets)) != NULL) {
        if (writable && eye->current == NULL) {
            int seg_index = pipeline->segment_frames > 0
                                ? (int)(eye->ordinal / (unsigned long long)pipeline->segment_frames)
                                : 0;
            writable = segment_open(pipeline, eye, seg_index, eye->ordinal) == 0;
        }
        if (writable) {
            mx_stream_t stream;
            memset(&stream, 0, sizeof(stream));
            stream.vir_ptr = packet->data;
            stream.size = (uint32_t)packet->size;
            stream.pts = eye->ordinal - eye->current->start_frame;
            stream.is_key_frame = packet->key_frame;
            unsigned long long started = monotonic_time_ns();
            if (eye->ordinal == eye->current->start_frame && !packet->key_frame) {
                fail(pipeline, "segment_missing_idr: %s", eye->current->relative);
                writable = 0;
            } else if (hb_mm_mx_write_stream(&eye->current->muxer, &stream) != 0) {
                fail(pipeline, "segment_write_failed: %s", eye->current->relative);
                writable = 0;
            }
            observe_max(&eye->max_write_ns, monotonic_time_ns() - started);
            if (writable) {
                atomic_fetch_add(&pipeline->bytes[eye->eye], (unsigned long long)packet->size);
                eye->ordinal += 1ULL;
                if (pipeline->segment_frames > 0 &&
                    eye->ordinal % (unsigned long long)pipeline->segment_frames == 0) {
                    segment_close(pipeline, eye);
                }
            }
        }
        miro_ego_packet_queue_done(&eye->packets, packet);
    }
    /* A failed write can have partially changed the tail MP4. Preserve that
     * file for diagnosis, but never emit it as a completed segment. */
    if (writable) {
        if (eye->current != NULL) {
            segment_close(pipeline, eye);
        }
    } else if (eye->current != NULL) {
        hb_mm_mx_stop(&eye->current->muxer);
        close_seal_fd(eye->current);
        free(eye->current);
        atomic_fetch_sub(&pipeline->pending_segments, 1u);
        eye->current = NULL;
    }
    return NULL;
}

static void *encoder_output_thread(void *argument)
{
    eye_t *eye = (eye_t *)argument;
    miro_ego_pipeline_t *pipeline = eye->pipeline;
    int draining = 0;
    unsigned long long drain_started_ns = 0ULL;
    for (;;) {
        const unsigned long long call_started_ns = monotonic_time_ns();
        media_codec_buffer_t buffer;
        media_codec_output_buffer_info_t info;
        memset(&buffer, 0, sizeof(buffer));
        memset(&info, 0, sizeof(info));
        int ret = hb_mm_mc_dequeue_output_buffer(&eye->codec, &buffer, &info,
                                                 ENCODER_OUTPUT_TIMEOUT_MS);
        if (ret != 0) {
            if (!is_transient_dequeue_timeout(ret)) {
                if (atomic_load(&pipeline->failed) == 0) {
                    fail(pipeline, "encoder_output_dequeue_failed: eye=%s ret=%d",
                         EYE_NAMES[eye->eye], ret);
                }
                break;
            }
            /* Only the split thread knows how many frames this eye owes, so the
             * encoder cannot stop draining before the split thread is done. */
            if (atomic_load(&pipeline->split_done) != 0) {
                if (atomic_load(&pipeline->failed) != 0 ||
                    atomic_load(&pipeline->encoded[eye->eye]) >= atomic_load(&pipeline->fed[eye->eye])) {
                    break;
                }
                if (drain_deadline_expired(&draining, &drain_started_ns, DRAIN_TIMEOUT_NS)) {
                    fail(pipeline, "encoder_drain_timeout: eye=%s ret=%d fed=%llu encoded=%llu",
                         EYE_NAMES[eye->eye], ret, atomic_load(&pipeline->fed[eye->eye]),
                         atomic_load(&pipeline->encoded[eye->eye]));
                    break;
                }
            } else {
                draining = 0;
            }
            backoff_if_fast_retry(call_started_ns);
            continue;
        }
        draining = 0;
        if (buffer.vstream_buf.size == 0 || buffer.vstream_buf.vir_ptr == NULL) {
            fail(pipeline, "encoder_output_invalid: eye=%s bytes=%u",
                 EYE_NAMES[eye->eye], buffer.vstream_buf.size);
            int qret = hb_mm_mc_queue_output_buffer(&eye->codec, &buffer,
                                                    ENCODER_OUTPUT_TIMEOUT_MS);
            if (qret != 0) {
                fail(pipeline, "encoder_output_queue_failed: eye=%s ret=%d",
                     EYE_NAMES[eye->eye], qret);
            }
            break;
        }
        ion_invalidate_range(buffer.vstream_buf.vir_ptr, (uint64_t)buffer.vstream_buf.size);
        int key_frame = (pipeline->config.hevc ? miro_ego_h265_has_idr : miro_ego_h264_has_idr)(
            buffer.vstream_buf.vir_ptr, (size_t)buffer.vstream_buf.size);
        int queued = miro_ego_packet_queue_push(&eye->packets, buffer.vstream_buf.vir_ptr,
                                                (size_t)buffer.vstream_buf.size, key_frame);
        /* Release the VPU buffer before any filesystem operation can block. */
        int qret = hb_mm_mc_queue_output_buffer(&eye->codec, &buffer, ENCODER_OUTPUT_TIMEOUT_MS);
        if (queued != 0) {
            fail(pipeline, "video_write_backpressure: eye=%s capacity_bytes=%u capacity_frames=%u",
                 EYE_NAMES[eye->eye], MUX_QUEUE_BYTES,
                 (unsigned)pipeline->config.fps * MUX_QUEUE_SECONDS);
            break;
        }
        if (qret != 0) {
            fail(pipeline, "encoder_output_queue_failed: eye=%s ret=%d",
                 EYE_NAMES[eye->eye], qret);
            break;
        }
        atomic_fetch_add(&pipeline->encoded[eye->eye], 1ULL);
    }
    miro_ego_packet_queue_close(&eye->packets);
    return NULL;
}

static void split_eye_buffer(const miro_ego_pipeline_t *pipeline,
                             const media_codec_buffer_t *source,
                             media_codec_buffer_t *target,
                             int eye_width, int height, int x_offset)
{
    if (target == NULL) {
        return;
    }
    const uint32_t target_capacity = target->vframe_buf.size;
    target->vframe_buf.size = 0;
    if (source == NULL || source->vframe_buf.pix_fmt != MC_PIXEL_FORMAT_NV12) {
        return;
    }
    miro_ego_nv12_frame_t src_frame = {
        .y = source->vframe_buf.vir_ptr[0],
        .uv = source->vframe_buf.vir_ptr[1],
        .width = source->vframe_buf.width,
        .height = source->vframe_buf.height,
        .stride = source->vframe_buf.stride,
        .size = source->vframe_buf.size,
    };
    miro_ego_nv12_frame_t dst_frame = {
        .y = target->vframe_buf.vir_ptr[0],
        .uv = target->vframe_buf.vir_ptr[1],
        .width = eye_width,
        .height = height,
        .stride = target->vframe_buf.stride,
        .size = target_capacity,
    };
    if (pipeline->split_ops->split_eye(pipeline->split_ctx, &src_frame, &dst_frame,
                                       eye_width, height, x_offset) == 0) {
        uint64_t dst_bytes = dst_frame.size;
        const int dst_stride = dst_frame.stride > 0 ? dst_frame.stride : eye_width;
        if (dst_frame.y != NULL && dst_frame.uv != NULL && dst_stride > 0 && height > 0) {
            const uintptr_t y_addr = (uintptr_t)dst_frame.y;
            const uintptr_t uv_addr = (uintptr_t)dst_frame.uv;
            const uint64_t uv_bytes = (uint64_t)dst_stride * (uint64_t)(height / 2);
            if (uv_addr >= y_addr && (uint64_t)(uv_addr - y_addr) <= (uint64_t)target_capacity) {
                const uint64_t span = (uint64_t)(uv_addr - y_addr) + uv_bytes;
                if (span <= (uint64_t)target_capacity && span > dst_bytes) {
                    dst_bytes = span;
                }
            } else {
                const uint64_t packed =
                    (uint64_t)dst_stride * ((uint64_t)height + (uint64_t)(height / 2));
                if (dst_bytes == 0u && packed <= (uint64_t)target_capacity) {
                    dst_bytes = packed;
                }
            }
        }
        target->vframe_buf.size = dst_bytes <= UINT32_MAX ? (uint32_t)dst_bytes : target_capacity;
    }
}

static void *split_thread_main(void *argument)
{
    miro_ego_pipeline_t *pipeline = (miro_ego_pipeline_t *)argument;
    const int eye_width = pipeline->eye_width;
    const int height = pipeline->config.height;
    int draining = 0;
    unsigned long long drain_started_ns = 0ULL;

    for (;;) {
        if (atomic_load(&pipeline->failed) != 0) {
            break;
        }
        const unsigned long long call_started_ns = monotonic_time_ns();
        media_codec_buffer_t frame;
        media_codec_output_buffer_info_t info;
        memset(&frame, 0, sizeof(frame));
        memset(&info, 0, sizeof(info));
        int ret = hb_mm_mc_dequeue_output_buffer(&pipeline->decoder, &frame, &info,
                                                 DECODER_OUTPUT_TIMEOUT_MS);
        if (ret != 0) {
            if (atomic_load(&pipeline->failed) != 0) {
                break;
            }
            if (!is_transient_dequeue_timeout(ret)) {
                fail(pipeline, "decoder_output_dequeue_failed: ret=%d", ret);
                break;
            }
            if (atomic_load(&pipeline->stopping) != 0) {
                if (atomic_load(&pipeline->decoded) >= atomic_load(&pipeline->submitted)) {
                    break;
                }
                if (drain_deadline_expired(&draining, &drain_started_ns, DRAIN_TIMEOUT_NS)) {
                    fail(pipeline, "decoder_drain_timeout: ret=%d submitted=%llu decoded=%llu",
                         ret, atomic_load(&pipeline->submitted),
                         atomic_load(&pipeline->decoded));
                    break;
                }
            } else {
                draining = 0;
            }
            backoff_if_fast_retry(call_started_ns);
            continue;
        }
        draining = 0;
        if (frame.type != MC_VIDEO_FRAME_BUFFER || frame.vframe_buf.size == 0 ||
            info.video_frame_info.decode_result == 0) {
            atomic_fetch_add(&pipeline->drop_decoder_output, 1ULL);
            fail(pipeline, "decoder_output_invalid: type=%d bytes=%u result=%d",
                 frame.type, frame.vframe_buf.size, info.video_frame_info.decode_result);
            hb_mm_mc_queue_output_buffer(&pipeline->decoder, &frame, 0);
            break;
        }
        if (frame.vframe_buf.width != pipeline->config.sbs_width ||
            frame.vframe_buf.height != height) {
            fail(pipeline, "decoder_geometry_mismatch: got %dx%d expected %dx%d",
                 frame.vframe_buf.width, frame.vframe_buf.height,
                 pipeline->config.sbs_width, height);
            hb_mm_mc_queue_output_buffer(&pipeline->decoder, &frame, 0);
            break;
        }
        ion_invalidate_nv12_frame(&frame);
        atomic_fetch_add(&pipeline->decoded, 1ULL);

        for (int eye = 0; eye < MIRO_EGO_EYES; eye += 1) {
            media_codec_buffer_t input;
            ret = dequeue_input_with_retry(pipeline, &pipeline->eyes[eye].codec, &input,
                                           ENCODER_INPUT_TIMEOUT_MS, 0);
            if (ret != 0) {
                atomic_fetch_add(&pipeline->drop_encoder_input, 1ULL);
                fail(pipeline, "encoder_input_dequeue_failed: eye=%s ret=%d timeout_ms=%d",
                     EYE_NAMES[eye], ret, ENCODER_INPUT_TIMEOUT_MS);
                break;
            }
            input.type = MC_VIDEO_FRAME_BUFFER;
            input.vframe_buf.width = eye_width;
            input.vframe_buf.height = height;
            input.vframe_buf.pix_fmt = MC_PIXEL_FORMAT_NV12;
            split_eye_buffer(pipeline, &frame, &input, eye_width, height, eye * eye_width);
            if (input.vframe_buf.size == 0) {
                atomic_fetch_add(&pipeline->drop_encoder_input, 1ULL);
                fail(pipeline, "encoder_split_failed: eye=%s pix_fmt=%d",
                     EYE_NAMES[eye], frame.vframe_buf.pix_fmt);
                input.vframe_buf.size = 0;
                hb_mm_mc_queue_input_buffer(&pipeline->eyes[eye].codec, &input,
                                            ENCODER_INPUT_TIMEOUT_MS);
                break;
            }
            ion_flush_nv12_frame(&input);
            ret = hb_mm_mc_queue_input_buffer(&pipeline->eyes[eye].codec, &input,
                                              ENCODER_INPUT_TIMEOUT_MS);
            if (ret != 0) {
                atomic_fetch_add(&pipeline->drop_encoder_input, 1ULL);
                fail(pipeline, "encoder_input_queue_failed: eye=%s ret=%d",
                     EYE_NAMES[eye], ret);
                break;
            }
            atomic_fetch_add(&pipeline->fed[eye], 1ULL);
        }
        hb_mm_mc_queue_output_buffer(&pipeline->decoder, &frame, 0);
        if (atomic_load(&pipeline->failed) != 0) {
            break;
        }
    }
    atomic_store(&pipeline->split_done, 1);
    return NULL;
}

/* ------------------------------------------------------------- lifetime --- */

int miro_ego_pipeline_open(const miro_ego_pipeline_config_t *config,
                           miro_ego_segment_closed_fn on_segment, void *user,
                           miro_ego_pipeline_t **out, char *error, size_t error_len)
{
    if (out != NULL) {
        *out = NULL;
    }
    if (config == NULL || out == NULL || config->out_dir == NULL ||
        config->sbs_width <= 0 || config->sbs_width % 4 != 0 || config->sbs_width > 16384 ||
        config->height <= 0 || config->height % 2 != 0 || config->height > 16384 ||
        config->fps <= 0 || config->fps > 60 ||
        config->segment_frames < 0 || config->intra_period < 0 ||
        config->intra_period > 300 || config->hevc < 0 || config->hevc > 1 ||
        config->rate_control < 0 || config->rate_control > 3 ||
        config->bitrate_kbps < 1 || config->bitrate_kbps > 100000 ||
        config->min_qp < 0 || config->max_qp > 51 || config->min_qp > config->max_qp ||
        config->intra_qp < config->min_qp || config->intra_qp > config->max_qp ||
        config->initial_qp < config->min_qp || config->initial_qp > config->max_qp ||
        config->vbv_ms < 1 || config->vbv_ms > 3000 ||
        config->input_stall_timeout_ms < 0 || config->input_stall_timeout_ms > 60000 ||
        (config->segment_frames > 0 && config->segment_frames %
         (config->intra_period > 0 ? config->intra_period : config->fps) != 0)) {
        if (error != NULL && error_len > 0) {
            snprintf(error, error_len, "invalid_configuration");
        }
        return -1;
    }

    const miro_ego_split_ops_t *split_ops =
        config->split_ops != NULL ? config->split_ops : miro_ego_split_ops_cpu();
    if (split_ops == NULL || split_ops->split_eye == NULL) {
        if (error != NULL && error_len > 0) {
            snprintf(error, error_len, "invalid_split_ops");
        }
        return -1;
    }

    miro_ego_pipeline_t *pipeline = calloc(1, sizeof(*pipeline));
    if (pipeline == NULL) {
        if (error != NULL && error_len > 0) {
            snprintf(error, error_len, "out_of_memory");
        }
        return -1;
    }
    pipeline->config = *config;
    pipeline->split_ops = split_ops;
    pipeline->out_dir_owned = strdup(config->out_dir);
    pipeline->path_prefix_owned = strdup(config->path_prefix != NULL ? config->path_prefix : "");
    if (pipeline->out_dir_owned == NULL || pipeline->path_prefix_owned == NULL) {
        if (error != NULL && error_len > 0) {
            snprintf(error, error_len, "out_of_memory");
        }
        goto open_failed;
    }
    if (posix_memalign(&pipeline->closer_io_buf, MIRO_EGO_SHA256_IO_ALIGN,
                       MIRO_EGO_SHA256_IO_BUF_SIZE) != 0 ||
        pipeline->closer_io_buf == NULL ||
        posix_memalign(&pipeline->seal_io_buf, MIRO_EGO_SHA256_IO_ALIGN,
                       MIRO_EGO_SHA256_IO_BUF_SIZE) != 0 ||
        pipeline->seal_io_buf == NULL) {
        if (error != NULL && error_len > 0) {
            snprintf(error, error_len, "out_of_memory");
        }
        goto open_failed;
    }

    pipeline->config.out_dir = pipeline->out_dir_owned;
    pipeline->config.path_prefix = pipeline->path_prefix_owned;
    pipeline->eye_width = config->sbs_width / MIRO_EGO_EYES;
    pipeline->segment_frames = config->segment_frames;
    pipeline->on_segment = on_segment;
    pipeline->user = user;

    const char *resource = "finish lock";
    int ret = pthread_mutex_init(&pipeline->finish_lock, NULL);
    if (ret != 0) goto sync_failed;
    pipeline->finish_lock_initialized = 1;
    resource = "ledger lock";
    ret = pthread_mutex_init(&pipeline->ledger_lock, NULL);
    if (ret != 0) goto sync_failed;
    pipeline->ledger_initialized = 1;
    resource = "error lock";
    ret = pthread_mutex_init(&pipeline->error_lock, NULL);
    if (ret != 0) goto sync_failed;
    pipeline->error_initialized = 1;
    resource = "closer lock";
    ret = pthread_mutex_init(&pipeline->closer_lock, NULL);
    if (ret != 0) goto sync_failed;
    pipeline->closer_lock_initialized = 1;
    resource = "closer condition";
    ret = pthread_cond_init(&pipeline->closer_signal, NULL);
    if (ret != 0) goto sync_failed;
    pipeline->closer_signal_initialized = 1;
    resource = "commit condition";
    ret = pthread_cond_init(&pipeline->commit_signal, NULL);
    if (ret != 0) goto sync_failed;
    pipeline->commit_signal_initialized = 1;

    /* Complete shared queue setup before any worker can observe an eye. */
    for (int eye = 0; eye < MIRO_EGO_EYES; eye += 1) {
        pipeline->eyes[eye].pipeline = pipeline;
        pipeline->eyes[eye].eye = eye;
        resource = eye == 0 ? "left queue" : "right queue";
        ret = miro_ego_packet_queue_init(&pipeline->eyes[eye].packets, MUX_QUEUE_BYTES,
                                         (size_t)config->fps * MUX_QUEUE_SECONDS);
        if (ret != 0) goto sync_failed;
        pipeline->eyes[eye].packets_initialized = 1;
    }

    if (pipeline->split_ops->init != NULL) {
        ret = pipeline->split_ops->init(&pipeline->split_ctx, config->sbs_width,
                                        config->height, &config->preview);
        if (ret != 0) {
            if (error != NULL && error_len > 0) {
                snprintf(error, error_len, "split_init_failed: %s (%d)",
                         pipeline->split_ops->name ? pipeline->split_ops->name : "unknown",
                         ret);
            }
            goto open_failed;
        }
        pipeline->split_ops_initialized = 1;
    }

    configure_decoder(&pipeline->decoder, &pipeline->config);
    ret = codec_start(&pipeline->decoder);
    if (ret != 0) {
        if (error != NULL && error_len > 0) {
            snprintf(error, error_len, "jpu_unavailable: MJPEG decoder start failed (%d)", ret);
        }
        goto open_failed;
    }
    pipeline->decoder_started = 1;

    for (int eye = 0; eye < MIRO_EGO_EYES; eye += 1) {
        ret = configure_encoder(&pipeline->eyes[eye].codec, &pipeline->config,
                                pipeline->eye_width);
        if (ret == 0) {
            ret = codec_start(&pipeline->eyes[eye].codec);
        }
        if (ret != 0) {
            if (error != NULL && error_len > 0) {
                snprintf(error, error_len, "vpu_unavailable: %s video encoder start failed (%d)",
                         EYE_NAMES[eye], ret);
            }
            goto open_failed;
        }
        pipeline->eyes[eye].codec_started = 1;
    }

    const char *worker = "committer";
    ret = pthread_create(&pipeline->committer_thread, NULL, committer_thread_main, pipeline);
    if (ret != 0) goto thread_failed;
    pipeline->committer_started = 1;
    worker = "closer";
    ret = pthread_create(&pipeline->closer_thread, NULL, closer_thread_main, pipeline);
    if (ret != 0) goto thread_failed;
    pipeline->closer_started = 1;
    worker = "splitter";
    ret = pthread_create(&pipeline->split_thread, NULL, split_thread_main, pipeline);
    if (ret != 0) goto thread_failed;
    pipeline->split_started = 1;
    for (int eye = 0; eye < MIRO_EGO_EYES; eye += 1) {
        worker = eye == 0 ? "left writer" : "right writer";
        ret = pthread_create(&pipeline->eyes[eye].writer_thread, NULL, mux_writer_thread,
                             &pipeline->eyes[eye]);
        if (ret != 0) goto thread_failed;
        pipeline->eyes[eye].writer_started = 1;
        worker = eye == 0 ? "left output" : "right output";
        ret = pthread_create(&pipeline->eyes[eye].thread, NULL, encoder_output_thread,
                             &pipeline->eyes[eye]);
        if (ret != 0) goto thread_failed;
        pipeline->eyes[eye].output_started = 1;
    }

    *out = pipeline;
    return 0;

thread_failed:
    if (error != NULL && error_len > 0) {
        snprintf(error, error_len, "thread_start_failed: %s (%d)", worker, ret);
    }
    goto open_failed;
sync_failed:
    if (error != NULL && error_len > 0) {
        snprintf(error, error_len, "sync_init_failed: %s (%d)", resource, ret);
    }
open_failed:
    atomic_store(&pipeline->failed, 1);
    miro_ego_pipeline_close(pipeline);
    return -1;
}

int miro_ego_pipeline_submit(miro_ego_pipeline_t *pipeline, const unsigned char *jpeg,
                             size_t length, int timeout_ms)
{
    if (pipeline == NULL) {
        return -1;
    }
    atomic_fetch_add(&pipeline->offered, 1ULL);
    if (atomic_load(&pipeline->failed) != 0) {
        return -1;
    }
    if (jpeg == NULL || length == 0) {
        atomic_fetch_add(&pipeline->drop_decoder_input, 1ULL);
        fail(pipeline, "decoder_input_invalid: bytes=%zu", length);
        return -1;
    }

    media_codec_buffer_t input;
    const int timeout = timeout_ms > 0 ? timeout_ms : DECODER_INPUT_TIMEOUT_MS;
    int ret = dequeue_input_with_retry(pipeline, &pipeline->decoder, &input, timeout, 1);
    if (ret != 0) {
        atomic_fetch_add(&pipeline->drop_decoder_input, 1ULL);
        fail(pipeline, "decoder_input_dequeue_failed: ret=%d timeout_ms=%d bytes=%zu",
             ret, timeout, length);
        return -1;
    }
    if ((size_t)input.vstream_buf.size < length || input.vstream_buf.vir_ptr == NULL) {
        fail(pipeline, "decoder_input_capacity: bytes=%zu capacity=%u",
             length, input.vstream_buf.size);
        input.vstream_buf.size = 0;
        input.vstream_buf.stream_end = 0;
        hb_mm_mc_queue_input_buffer(&pipeline->decoder, &input, DECODER_INPUT_TIMEOUT_MS);
        atomic_fetch_add(&pipeline->drop_decoder_input, 1ULL);
        return -1;
    }
    input.type = MC_VIDEO_STREAM_BUFFER;
    input.vstream_buf.size = (uint32_t)length;
    input.vstream_buf.stream_end = 0;
    memcpy(input.vstream_buf.vir_ptr, jpeg, length);
    ion_flush_range(input.vstream_buf.vir_ptr, (uint64_t)length);
    ret = hb_mm_mc_queue_input_buffer(&pipeline->decoder, &input, DECODER_INPUT_TIMEOUT_MS);
    if (ret != 0) {
        atomic_fetch_add(&pipeline->drop_decoder_input, 1ULL);
        fail(pipeline, "decoder_input_queue_failed: ret=%d bytes=%zu", ret, length);
        return -1;
    }
    atomic_fetch_add(&pipeline->submitted, 1ULL);
    return 0;
}

int miro_ego_pipeline_finish(miro_ego_pipeline_t *pipeline)
{
    if (pipeline == NULL) {
        return -1;
    }
    atomic_store(&pipeline->stopping, 1);
    if (!pipeline->finish_lock_initialized) {
        return atomic_load(&pipeline->failed) == 0 ? 0 : -1;
    }
    pthread_mutex_lock(&pipeline->finish_lock);
    if (!pipeline->finished) {
        if (pipeline->split_started) {
            pthread_join(pipeline->split_thread, NULL);
        } else {
            atomic_store(&pipeline->split_done, 1);
        }
        for (int eye = 0; eye < MIRO_EGO_EYES; eye += 1) {
            if (pipeline->eyes[eye].output_started) {
                pthread_join(pipeline->eyes[eye].thread, NULL);
            }
            /* Startup can fail after a writer starts but before its producer does. */
            if (pipeline->eyes[eye].packets_initialized) {
                miro_ego_packet_queue_close(&pipeline->eyes[eye].packets);
            }
            if (pipeline->eyes[eye].writer_started) {
                pthread_join(pipeline->eyes[eye].writer_thread, NULL);
            }
        }
        /* Only once both eyes stopped queueing can the closer drain to empty. */
        if (pipeline->closer_started) {
            pthread_mutex_lock(&pipeline->closer_lock);
            pipeline->closer_stop = 1;
            pthread_cond_signal(&pipeline->closer_signal);
            pthread_mutex_unlock(&pipeline->closer_lock);
            pthread_join(pipeline->closer_thread, NULL);
        }
        if (pipeline->committer_started) {
            pthread_mutex_lock(&pipeline->closer_lock);
            pipeline->commit_stop = 1;
            pthread_cond_signal(&pipeline->commit_signal);
            pthread_mutex_unlock(&pipeline->closer_lock);
            pthread_join(pipeline->committer_thread, NULL);
        }
        pipeline->finished = 1;
    }
    pthread_mutex_unlock(&pipeline->finish_lock);
    return atomic_load(&pipeline->failed) == 0 ? 0 : -1;
}

void miro_ego_pipeline_close(miro_ego_pipeline_t *pipeline)
{
    if (pipeline == NULL) {
        return;
    }
    miro_ego_pipeline_finish(pipeline);
    for (int eye = 0; eye < MIRO_EGO_EYES; eye += 1) {
        if (pipeline->eyes[eye].codec_started) {
            codec_stop(&pipeline->eyes[eye].codec);
        }
        if (pipeline->eyes[eye].packets_initialized) {
            miro_ego_packet_queue_destroy(&pipeline->eyes[eye].packets);
        }
    }
    while (pipeline->closed_pairs != NULL) {
        closed_pair_t *pair = pipeline->closed_pairs;
        pipeline->closed_pairs = pair->next;
        free(pair);
    }
    if (pipeline->decoder_started) {
        codec_stop(&pipeline->decoder);
    }
    if (pipeline->split_ops_initialized && pipeline->split_ops != NULL &&
        pipeline->split_ops->destroy != NULL) {
        pipeline->split_ops->destroy(pipeline->split_ctx);
    }
    if (pipeline->ledger_initialized) {
        pthread_mutex_destroy(&pipeline->ledger_lock);
    }
    if (pipeline->error_initialized) {
        pthread_mutex_destroy(&pipeline->error_lock);
    }
    if (pipeline->commit_signal_initialized) {
        pthread_cond_destroy(&pipeline->commit_signal);
    }
    if (pipeline->closer_lock_initialized) {
        pthread_mutex_destroy(&pipeline->closer_lock);
    }
    if (pipeline->closer_signal_initialized) {
        pthread_cond_destroy(&pipeline->closer_signal);
    }
    if (pipeline->finish_lock_initialized) {
        pthread_mutex_destroy(&pipeline->finish_lock);
    }
    free(pipeline->closer_io_buf);
    free(pipeline->seal_io_buf);
    free(pipeline->out_dir_owned);
    free(pipeline->path_prefix_owned);
    free(pipeline);
}

void miro_ego_pipeline_stats(const miro_ego_pipeline_t *pipeline,
                             miro_ego_pipeline_stats_t *out)
{
    if (out == NULL) {
        return;
    }
    memset(out, 0, sizeof(*out));
    if (pipeline == NULL) {
        return;
    }
    miro_ego_pipeline_t *mutable_pipeline = (miro_ego_pipeline_t *)pipeline;
    out->offered = atomic_load(&mutable_pipeline->offered);
    out->submitted = atomic_load(&mutable_pipeline->submitted);
    out->decoded = atomic_load(&mutable_pipeline->decoded);
    out->drop_decoder_input = atomic_load(&mutable_pipeline->drop_decoder_input);
    out->drop_encoder_input = atomic_load(&mutable_pipeline->drop_encoder_input);
    out->drop_decoder_output = atomic_load(&mutable_pipeline->drop_decoder_output);
    out->max_seal_ns = atomic_load(&mutable_pipeline->max_seal_ns);
    for (int eye = 0; eye < MIRO_EGO_EYES; eye += 1) {
        out->encoded[eye] = atomic_load(&mutable_pipeline->encoded[eye]);
        out->bytes[eye] = atomic_load(&mutable_pipeline->bytes[eye]);
        out->max_write_ns[eye] = atomic_load(&mutable_pipeline->eyes[eye].max_write_ns);
        if (mutable_pipeline->eyes[eye].packets_initialized) {
            miro_ego_packet_queue_t *queue = &mutable_pipeline->eyes[eye].packets;
            pthread_mutex_lock(&queue->lock);
            out->write_queue_peak_bytes[eye] = queue->peak_bytes;
            out->write_queue_peak_frames[eye] = queue->peak_frames;
            out->write_queue_rejected[eye] = queue->rejected;
            pthread_mutex_unlock(&queue->lock);
        }
    }
}

int miro_ego_pipeline_failed(const miro_ego_pipeline_t *pipeline)
{
    if (pipeline == NULL) {
        return 1;
    }
    miro_ego_pipeline_t *mutable_pipeline = (miro_ego_pipeline_t *)pipeline;
    return atomic_load(&mutable_pipeline->failed) != 0;
}

const char *miro_ego_pipeline_error(const miro_ego_pipeline_t *pipeline)
{
    if (pipeline == NULL || !pipeline->error_initialized) {
        return "";
    }
    /* An input error is reported before worker drain completes. Copy under the
     * lock so the caller never races the first error from another thread. */
    static _Thread_local char error[ERROR_LEN];
    miro_ego_pipeline_t *mutable_pipeline = (miro_ego_pipeline_t *)pipeline;
    pthread_mutex_lock(&mutable_pipeline->error_lock);
    memcpy(error, pipeline->error, sizeof(error));
    pthread_mutex_unlock(&mutable_pipeline->error_lock);
    return error;
}

void miro_ego_pipeline_request_stop(miro_ego_pipeline_t *pipeline)
{
    if (pipeline == NULL) {
        return;
    }
    atomic_store(&pipeline->stopping, 1);
    atomic_store(&pipeline->abort_requested, 1);
}

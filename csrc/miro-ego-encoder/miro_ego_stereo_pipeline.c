#define _GNU_SOURCE
#include "miro_ego_stereo_pipeline.h"
#include "miro_ego_flac.h"
#include "miro_ego_fmp4.h"
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
#define MAX_PENDING_FRAGMENTS 16u
#define MAX_SYNC_QUEUE_ITEMS 2048u

typedef struct sync_item {
    struct sync_item *next;
    uint64_t frame_seq;
    uint64_t monotonic_ns;
    uint64_t sample_number;
    uint32_t duration_ticks;
    uint8_t *pcm;
    uint32_t pcm_len;
    uint8_t *meta_json;
    uint32_t meta_len;
} sync_item_t;

typedef struct frag_job {
    struct frag_job *next;
    uint8_t *data;
    size_t len;
    int index;
    unsigned long long start_frame;
    unsigned long long end_frame;
    unsigned long long duration_ticks;
    unsigned long long frag_offset;
} frag_job_t;

typedef struct {
    miro_ego_pipeline_t *pipeline;
    int eye;
    media_codec_context_t codec;
    pthread_t thread;
    miro_ego_packet_queue_t packets;
    int codec_started;
    int output_started;
    int packets_initialized;
    atomic_ullong max_write_ns;
} eye_t;

struct miro_ego_pipeline {
    miro_ego_pipeline_config_t config;
    char *out_dir_owned;
    char *path_prefix_owned;
    char *output_file_owned;
    int eye_width;
    int fragment_frames;
    uint32_t timescale;
    int audio_tracks;

    const miro_ego_split_ops_t *split_ops;
    void *split_ctx;
    int split_ops_initialized;

    /* Reusable 256 KiB page-aligned read buffer for finalizing session.mp4 SHA-256. */
    void *seal_io_buf;

    media_codec_context_t decoder;
    eye_t eyes[MIRO_EGO_EYES];
    pthread_t split_thread;
    pthread_t combiner_thread;
    pthread_t writer_thread;
    int decoder_started;
    int split_started;
    int combiner_started;
    int writer_started;
    int finish_lock_initialized;
    int error_initialized;
    int sync_lock_initialized;
    int sync_cond_initialized;
    int writer_lock_initialized;
    int writer_cond_initialized;
    int writer_space_cond_initialized;
    pthread_mutex_t finish_lock;
    int finished;

    miro_ego_fragment_fn on_fragment;
    miro_ego_sealed_fn on_sealed;
    void *user;

    pthread_mutex_t sync_lock;
    pthread_cond_t sync_cond;
    sync_item_t *sync_head;
    sync_item_t *sync_tail;
    size_t sync_count;
    int sync_stop;

    pthread_mutex_t writer_lock;
    pthread_cond_t writer_cond;
    pthread_cond_t writer_space_cond;
    frag_job_t *writer_head;
    frag_job_t *writer_tail;
    size_t pending_frags;
    int writer_stop;
    int writer_fd;

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
    atomic_ullong muxed_frames;
    atomic_ullong fragments_written;
    atomic_ullong total_ticks;
    atomic_ullong fdatasync_count;
    atomic_ullong sum_fdatasync_ns;
    atomic_ullong max_fdatasync_ns;

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
    if (pipeline->sync_cond_initialized) {
        pthread_cond_broadcast(&pipeline->sync_cond);
    }
    if (pipeline->writer_space_cond_initialized) {
        pthread_cond_broadcast(&pipeline->writer_space_cond);
    }
    if (pipeline->writer_cond_initialized) {
        pthread_cond_broadcast(&pipeline->writer_cond);
    }
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

static void sync_item_free(sync_item_t *item)
{
    if (item == NULL) {
        return;
    }
    free(item->pcm);
    free(item->meta_json);
    free(item);
}

static int sync_push_locked(miro_ego_pipeline_t *pipeline, sync_item_t *item)
{
    if (pipeline->sync_count >= MAX_SYNC_QUEUE_ITEMS) {
        return -1;
    }
    item->next = NULL;
    if (pipeline->sync_tail != NULL) {
        pipeline->sync_tail->next = item;
    } else {
        pipeline->sync_head = item;
    }
    pipeline->sync_tail = item;
    pipeline->sync_count += 1u;
    pthread_cond_signal(&pipeline->sync_cond);
    return 0;
}

static void sync_remove_tail_locked(miro_ego_pipeline_t *pipeline, sync_item_t *target)
{
    if (pipeline->sync_tail != target) {
        return;
    }
    sync_item_t *prev = NULL;
    sync_item_t *cur = pipeline->sync_head;
    while (cur != NULL && cur != target) {
        prev = cur;
        cur = cur->next;
    }
    if (cur == target) {
        if (prev != NULL) {
            prev->next = NULL;
        } else {
            pipeline->sync_head = NULL;
        }
        pipeline->sync_tail = prev;
        pipeline->sync_count -= 1u;
    }
}

static sync_item_t *sync_pop(miro_ego_pipeline_t *pipeline)
{
    pthread_mutex_lock(&pipeline->sync_lock);
    while (pipeline->sync_head == NULL && !pipeline->sync_stop &&
           atomic_load(&pipeline->failed) == 0) {
        pthread_cond_wait(&pipeline->sync_cond, &pipeline->sync_lock);
    }
    sync_item_t *item = pipeline->sync_head;
    if (item != NULL) {
        pipeline->sync_head = item->next;
        if (pipeline->sync_head == NULL) {
            pipeline->sync_tail = NULL;
        }
        item->next = NULL;
        pipeline->sync_count -= 1u;
    }
    pthread_mutex_unlock(&pipeline->sync_lock);
    return item;
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

static int durable_sync_fd(int fd)
{
    int sync_ret;
    do {
        sync_ret = fdatasync(fd);
    } while (sync_ret < 0 && errno == EINTR);
    if (sync_ret < 0 && (errno == EINVAL || errno == ENOSYS || errno == EOPNOTSUPP)) {
        do {
            sync_ret = fsync(fd);
        } while (sync_ret < 0 && errno == EINTR);
    }
    return sync_ret;
}

static int durable_sync_dir(const char *dir_path)
{
    int dir_fd;
    do {
        dir_fd = open(dir_path, O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC);
    } while (dir_fd < 0 && errno == EINTR);
    if (dir_fd < 0) {
        return -1;
    }
    int sync_ret;
    do {
        sync_ret = fsync(dir_fd);
    } while (sync_ret < 0 && errno == EINTR);
    if (sync_ret != 0 && (errno == EINVAL || errno == ENOSYS || errno == EOPNOTSUPP)) {
        do {
            sync_ret = syncfs(dir_fd);
        } while (sync_ret < 0 && errno == EINTR);
    }
    int saved_errno = errno;
    close(dir_fd);
    if (sync_ret != 0 && saved_errno != EINVAL && saved_errno != ENOSYS &&
        saved_errno != EOPNOTSUPP) {
        errno = saved_errno;
        return -1;
    }
    return 0;
}

static int writer_enqueue(miro_ego_pipeline_t *pipeline, frag_job_t *job)
{
    pthread_mutex_lock(&pipeline->writer_lock);
    while (pipeline->pending_frags >= MAX_PENDING_FRAGMENTS &&
           !pipeline->writer_stop && atomic_load(&pipeline->failed) == 0) {
        pthread_cond_wait(&pipeline->writer_space_cond, &pipeline->writer_lock);
    }
    if (pipeline->writer_stop || atomic_load(&pipeline->failed) != 0) {
        pthread_mutex_unlock(&pipeline->writer_lock);
        return -1;
    }
    job->next = NULL;
    if (pipeline->writer_tail != NULL) {
        pipeline->writer_tail->next = job;
    } else {
        pipeline->writer_head = job;
    }
    pipeline->writer_tail = job;
    pipeline->pending_frags += 1u;
    pthread_cond_signal(&pipeline->writer_cond);
    pthread_mutex_unlock(&pipeline->writer_lock);
    return 0;
}

static void *writer_thread_main(void *argument)
{
    miro_ego_pipeline_t *pipeline = (miro_ego_pipeline_t *)argument;
    for (;;) {
        pthread_mutex_lock(&pipeline->writer_lock);
        while (pipeline->writer_head == NULL && !pipeline->writer_stop) {
            pthread_cond_wait(&pipeline->writer_cond, &pipeline->writer_lock);
        }
        frag_job_t *job = pipeline->writer_head;
        if (job != NULL) {
            pipeline->writer_head = job->next;
            if (pipeline->writer_head == NULL) {
                pipeline->writer_tail = NULL;
            }
            pipeline->pending_frags -= 1u;
            pthread_cond_signal(&pipeline->writer_space_cond);
        }
        int fd = pipeline->writer_fd;
        pthread_mutex_unlock(&pipeline->writer_lock);

        if (job == NULL) {
            break;
        }
        if (atomic_load(&pipeline->failed) == 0 && fd >= 0) {
            unsigned long long w0 = monotonic_time_ns();
            if (write_all_fd(fd, job->data, job->len) != 0) {
                fail(pipeline, "fragment_write_failed: index=%d: %s",
                     job->index, strerror(errno));
            } else {
                unsigned long long fs0 = monotonic_time_ns();
                if (durable_sync_fd(fd) != 0) {
                    fail(pipeline, "fragment_sync_failed: index=%d: %s",
                         job->index, strerror(errno));
                } else {
                    unsigned long long fs_dt = monotonic_time_ns() - fs0;
                    atomic_fetch_add(&pipeline->fdatasync_count, 1ULL);
                    atomic_fetch_add(&pipeline->sum_fdatasync_ns, fs_dt);
                    observe_max(&pipeline->max_fdatasync_ns, fs_dt);
                    (void)posix_fadvise(fd, (off_t)job->frag_offset,
                                        (off_t)job->len, POSIX_FADV_DONTNEED);
                    observe_max(&pipeline->eyes[0].max_write_ns, monotonic_time_ns() - w0);
                    atomic_fetch_add(&pipeline->fragments_written, 1ULL);
                    if (pipeline->on_fragment != NULL) {
                        pipeline->on_fragment(pipeline->user, job->index,
                                              job->start_frame, job->end_frame,
                                              job->duration_ticks,
                                              job->frag_offset,
                                              (unsigned long long)job->len);
                    }
                }
            }
        }
        free(job->data);
        free(job);
    }
    return NULL;
}

static int resolve_output_paths(const miro_ego_pipeline_t *pipeline,
                                char *rel_out, size_t rel_cap,
                                char *abs_out, size_t abs_cap)
{
    const char *name = (pipeline->output_file_owned != NULL &&
                        pipeline->output_file_owned[0] != '\0')
                           ? pipeline->output_file_owned
                           : "session.mp4";
    int n1 = snprintf(rel_out, rel_cap, "%s", name);
    int n2 = (name[0] == '/')
                 ? snprintf(abs_out, abs_cap, "%s", name)
                 : snprintf(abs_out, abs_cap, "%s/%s",
                            pipeline->out_dir_owned, name);
    if (n1 < 0 || (size_t)n1 >= rel_cap || n2 < 0 || (size_t)n2 >= abs_cap) {
        return -1;
    }
    return 0;
}

static void *mux_combiner_thread_main(void *argument)
{
    miro_ego_pipeline_t *pipeline = (miro_ego_pipeline_t *)argument;
    miro_ego_fmp4_t mx;
    memset(&mx, 0, sizeof(mx));
    miro_ego_video_ps_t ps[MIRO_EGO_EYES] = {{0}, {0}};
    miro_ego_bbuf_t avcc[MIRO_EGO_EYES] = {{0}, {0}};
    miro_ego_bbuf_t mfra_buf = {0};
    uint8_t *flac_buf = NULL;
    size_t flac_cap = 0u;
    size_t flac_len = 0u;
    int16_t *flac_l = NULL;
    int16_t *flac_r = NULL;
    int16_t *silent_pcm = NULL;
    size_t silent_cap_samples = 0u;
    int out_fd = -1;

    if (pipeline->audio_tracks > 0) {
        flac_l = malloc(65536u * sizeof(int16_t));
        flac_r = malloc(65536u * sizeof(int16_t));
        if (flac_l == NULL || flac_r == NULL) {
            fail(pipeline, "out_of_memory: flac scratch");
            goto cleanup;
        }
    }

    char rel_path[PATH_LEN] = {0};
    char abs_path[PATH_LEN] = {0};
    if (resolve_output_paths(pipeline, rel_path, sizeof(rel_path),
                             abs_path, sizeof(abs_path)) != 0) {
        fail(pipeline, "output_path_too_long");
        goto cleanup;
    }

    const uint32_t default_dur =
        pipeline->config.fps > 0
            ? (pipeline->timescale / (uint32_t)pipeline->config.fps)
            : 1600u;

    int mx_opened = 0;
    int writable = 1;
    int frag_index = 0;
    unsigned long long ordinal = 0ULL;
    unsigned long long frag_start_frame = 0ULL;
    unsigned long long total_ticks = 0ULL;

    for (;;) {
        miro_ego_packet_t *pkt_l = miro_ego_packet_queue_pop(&pipeline->eyes[0].packets);
        miro_ego_packet_t *pkt_r = miro_ego_packet_queue_pop(&pipeline->eyes[1].packets);
        if (pkt_l == NULL || pkt_r == NULL) {
            if (pkt_l != NULL) miro_ego_packet_queue_done(&pipeline->eyes[0].packets, pkt_l);
            if (pkt_r != NULL) miro_ego_packet_queue_done(&pipeline->eyes[1].packets, pkt_r);
            break;
        }
        sync_item_t *sb = sync_pop(pipeline);

        if (writable && atomic_load(&pipeline->failed) == 0) {
            int idr_l = 0;
            int idr_r = 0;
            if (miro_ego_annexb_to_avcc(pipeline->config.hevc, pkt_l->data, pkt_l->size,
                                        &ps[0], &avcc[0], &idr_l) != 0 ||
                miro_ego_annexb_to_avcc(pipeline->config.hevc, pkt_r->data, pkt_r->size,
                                        &ps[1], &avcc[1], &idr_r) != 0) {
                fail(pipeline, "video_annexb_parse_failed: frame=%llu", ordinal);
                writable = 0;
            }
            int is_idr_l = pkt_l->key_frame || idr_l;
            int is_idr_r = pkt_r->key_frame || idr_r;
            if (writable && ordinal == frag_start_frame && (!is_idr_l || !is_idr_r)) {
                fail(pipeline, "segment_missing_idr: frame=%llu", ordinal);
                writable = 0;
            }

            if (writable && !mx_opened) {
                do {
                    out_fd = open(abs_path, O_RDWR | O_CREAT | O_TRUNC | O_NOFOLLOW | O_CLOEXEC,
                                  0640);
                } while (out_fd < 0 && errno == EINTR);
                if (out_fd < 0) {
                    fail(pipeline, "segment_open_failed: open %s: %s",
                         rel_path, strerror(errno));
                    writable = 0;
                } else if (miro_ego_fmp4_init(&mx, out_fd, pipeline->config.hevc,
                                              pipeline->audio_tracks, pipeline->eye_width,
                                              pipeline->config.height,
                                              pipeline->timescale) != 0 ||
                           miro_ego_fmp4_write_header(&mx, ps) != 0 ||
                           durable_sync_fd(out_fd) != 0) {
                    fail(pipeline, "muxer_header_failed: %s", rel_path);
                    writable = 0;
                } else {
                    pthread_mutex_lock(&pipeline->writer_lock);
                    pipeline->writer_fd = out_fd;
                    pthread_mutex_unlock(&pipeline->writer_lock);
                    mx_opened = 1;
                }
            }

            if (writable) {
                uint32_t dur = (sb != NULL && sb->duration_ticks > 0u)
                                   ? sb->duration_ticks
                                   : default_dur;
                uint64_t sample_no = (sb != NULL) ? sb->sample_number : total_ticks;

                if (pipeline->audio_tracks > 0) {
                    const int16_t *pcm_ptr = NULL;
                    uint32_t blocksize = dur;
                    if (sb != NULL && sb->pcm_len > 0u) {
                        if (sb->pcm_len % 4u != 0u ||
                            sb->pcm_len / 4u != dur ||
                            dur == 0u || dur > 65535u) {
                            fail(pipeline, "sync_audio_duration_mismatch: frame=%llu pcm=%u dur=%u",
                                 ordinal, sb->pcm_len, dur);
                            writable = 0;
                        } else {
                            pcm_ptr = (const int16_t *)sb->pcm;
                            blocksize = sb->pcm_len / 4u;
                        }
                    } else {
                        if (blocksize == 0u || blocksize > 65535u) {
                            fail(pipeline, "invalid_audio_blocksize: %u", blocksize);
                            writable = 0;
                        } else {
                            if ((size_t)blocksize * 2u > silent_cap_samples) {
                                int16_t *np = realloc(silent_pcm,
                                                      (size_t)blocksize * 2u * sizeof(int16_t));
                                if (np == NULL) {
                                    fail(pipeline, "out_of_memory: silent pcm");
                                    writable = 0;
                                } else {
                                    memset(np, 0, (size_t)blocksize * 2u * sizeof(int16_t));
                                    silent_pcm = np;
                                    silent_cap_samples = (size_t)blocksize * 2u;
                                }
                            }
                            pcm_ptr = silent_pcm;
                        }
                    }
                    if (writable &&
                        miro_ego_flac_encode_frame(&flac_buf, &flac_cap, &flac_len, pcm_ptr,
                                                   blocksize, sample_no, flac_l, flac_r) != 0) {
                        fail(pipeline, "flac_encode_failed: frame=%llu", ordinal);
                        writable = 0;
                    }
                }

                char fallback_meta[128];
                const uint8_t *meta_ptr = NULL;
                uint32_t meta_len = 0u;
                if (sb != NULL && sb->meta_json != NULL && sb->meta_len > 0u) {
                    meta_ptr = sb->meta_json;
                    meta_len = sb->meta_len;
                } else {
                    int ml = snprintf(fallback_meta, sizeof(fallback_meta),
                                      "{\"seq\":%llu,\"ticks\":%u}", ordinal, dur);
                    meta_ptr = (const uint8_t *)fallback_meta;
                    meta_len = ml > 0 ? (uint32_t)ml : 2u;
                }

                if (writable) {
                    uint32_t flags_l = is_idr_l ? 0x02000000u : 0x01010000u;
                    uint32_t flags_r = is_idr_r ? 0x02000000u : 0x01010000u;
                    int meta_track = pipeline->audio_tracks > 0 ? 3 : 2;
                    int push_err = 0;
                    if (miro_ego_fmp4_push_sample(&mx, 0, dur, avcc[0].data,
                                                  (uint32_t)avcc[0].len, flags_l) != 0 ||
                        miro_ego_fmp4_push_sample(&mx, 1, dur, avcc[1].data,
                                                  (uint32_t)avcc[1].len, flags_r) != 0) {
                        push_err = 1;
                    }
                    if (!push_err && pipeline->audio_tracks > 0) {
                        if (miro_ego_fmp4_push_sample(&mx, 2, dur, flac_buf,
                                                      (uint32_t)flac_len, 0x02000000u) != 0) {
                            push_err = 1;
                        }
                    }
                    if (!push_err &&
                        miro_ego_fmp4_push_sample(&mx, meta_track, dur, meta_ptr,
                                                  meta_len, 0x02000000u) != 0) {
                        push_err = 1;
                    }
                    if (push_err) {
                        fail(pipeline, "fmp4_push_sample_failed: frame=%llu", ordinal);
                        writable = 0;
                    }
                }

                if (writable) {
                    atomic_fetch_add(&pipeline->bytes[0], (unsigned long long)pkt_l->size);
                    atomic_fetch_add(&pipeline->bytes[1], (unsigned long long)pkt_r->size);
                    ordinal += 1ULL;
                    total_ticks += (unsigned long long)dur;
                    atomic_store(&pipeline->muxed_frames, ordinal);
                    atomic_store(&pipeline->total_ticks, total_ticks);

                    if (pipeline->fragment_frames > 0 &&
                        (ordinal - frag_start_frame) >= (unsigned long long)pipeline->fragment_frames) {
                        miro_ego_bbuf_t frag_buf = {0};
                        uint64_t frag_off = 0u;
                        uint64_t frag_dur = 0u;
                        if (miro_ego_fmp4_build_fragment(&mx, &frag_buf, &frag_off, &frag_dur) != 0) {
                            miro_ego_bbuf_free(&frag_buf);
                            fail(pipeline, "fmp4_build_fragment_failed: index=%d", frag_index);
                            writable = 0;
                        } else if (frag_buf.len > 0u) {
                            frag_job_t *job = calloc(1, sizeof(*job));
                            if (job == NULL) {
                                miro_ego_bbuf_free(&frag_buf);
                                fail(pipeline, "out_of_memory: frag job");
                                writable = 0;
                            } else {
                                job->data = frag_buf.data;
                                job->len = frag_buf.len;
                                job->index = frag_index;
                                job->start_frame = frag_start_frame;
                                job->end_frame = ordinal;
                                job->duration_ticks = frag_dur;
                                job->frag_offset = frag_off;
                                if (writer_enqueue(pipeline, job) != 0) {
                                    free(job->data);
                                    free(job);
                                    writable = 0;
                                } else {
                                    frag_index += 1;
                                    frag_start_frame = ordinal;
                                }
                            }
                        }
                    }
                }
            }
        }

        sync_item_free(sb);
        miro_ego_packet_queue_done(&pipeline->eyes[0].packets, pkt_l);
        miro_ego_packet_queue_done(&pipeline->eyes[1].packets, pkt_r);
    }

    if (writable && mx_opened && atomic_load(&pipeline->failed) == 0 &&
        mx.tracks[0].n_samples > 0u) {
        miro_ego_bbuf_t frag_buf = {0};
        uint64_t frag_off = 0u;
        uint64_t frag_dur = 0u;
        if (miro_ego_fmp4_build_fragment(&mx, &frag_buf, &frag_off, &frag_dur) != 0) {
            miro_ego_bbuf_free(&frag_buf);
            fail(pipeline, "fmp4_build_fragment_failed: tail index=%d", frag_index);
            writable = 0;
        } else if (frag_buf.len > 0u) {
            frag_job_t *job = calloc(1, sizeof(*job));
            if (job == NULL) {
                miro_ego_bbuf_free(&frag_buf);
                fail(pipeline, "out_of_memory: tail frag job");
                writable = 0;
            } else {
                job->data = frag_buf.data;
                job->len = frag_buf.len;
                job->index = frag_index;
                job->start_frame = frag_start_frame;
                job->end_frame = ordinal;
                job->duration_ticks = frag_dur;
                job->frag_offset = frag_off;
                if (writer_enqueue(pipeline, job) != 0) {
                    free(job->data);
                    free(job);
                    writable = 0;
                } else {
                    frag_index += 1;
                }
            }
        }
    }

    pthread_mutex_lock(&pipeline->writer_lock);
    pipeline->writer_stop = 1;
    pthread_cond_broadcast(&pipeline->writer_cond);
    pthread_cond_broadcast(&pipeline->writer_space_cond);
    pthread_mutex_unlock(&pipeline->writer_lock);
    if (pipeline->writer_started) {
        pthread_join(pipeline->writer_thread, NULL);
        pipeline->writer_started = 0;
    }

    if (writable && mx_opened && atomic_load(&pipeline->failed) == 0) {
        unsigned long long seal_started = monotonic_time_ns();
        if (miro_ego_fmp4_build_mfra(&mx, &mfra_buf) != 0 ||
            write_all_fd(out_fd, mfra_buf.data, mfra_buf.len) != 0 ||
            miro_ego_fmp4_patch_moov_durations(&mx, out_fd, total_ticks) != 0 ||
            durable_sync_fd(out_fd) != 0) {
            fail(pipeline, "segment_seal_failed: finalize %s: %s",
                 rel_path, strerror(errno));
        } else {
            struct stat st;
            memset(&st, 0, sizeof(st));
            char sha256_hex[MIRO_EGO_SHA256_HEX_LEN] = {0};
            unsigned long long hashed_bytes = 0ULL;
            if (fstat(out_fd, &st) != 0 || st.st_size < 0) {
                fail(pipeline, "segment_seal_failed: fstat %s: %s",
                     rel_path, strerror(errno));
            } else {
                const unsigned long long bytes = (unsigned long long)st.st_size;
                int hret = pipeline->seal_io_buf != NULL
                               ? miro_ego_sha256_fd_with_buf(out_fd, pipeline->seal_io_buf,
                                                             MIRO_EGO_SHA256_IO_BUF_SIZE,
                                                             sha256_hex, &hashed_bytes)
                               : miro_ego_sha256_fd(out_fd, sha256_hex, &hashed_bytes);
                if (hret != 0 || hashed_bytes != bytes) {
                    fail(pipeline, "segment_seal_failed: sha256 %s (stat=%llu hashed=%llu)",
                         rel_path, bytes, hashed_bytes);
                } else {
                    (void)posix_fadvise(out_fd, 0, 0, POSIX_FADV_DONTNEED);
                    close(out_fd);
                    out_fd = -1;
                    if (durable_sync_dir(pipeline->out_dir_owned) != 0) {
                        fail(pipeline, "segment_seal_failed: sync directory: %s",
                             strerror(errno));
                    } else {
                        observe_max(&pipeline->max_seal_ns, monotonic_time_ns() - seal_started);
                        if (pipeline->on_sealed != NULL) {
                            pipeline->on_sealed(pipeline->user, rel_path, bytes, sha256_hex,
                                                ordinal, total_ticks,
                                                (unsigned long long)mx.moof_seq,
                                                pipeline->audio_tracks);
                        }
                    }
                }
            }
        }
    }

cleanup:
    if (out_fd >= 0) {
        close(out_fd);
    }
    miro_ego_fmp4_destroy(&mx);
    for (int i = 0; i < MIRO_EGO_EYES; i += 1) {
        miro_ego_video_ps_free(&ps[i]);
        miro_ego_bbuf_free(&avcc[i]);
    }
    miro_ego_bbuf_free(&mfra_buf);
    free(flac_buf);
    free(flac_l);
    free(flac_r);
    free(silent_pcm);
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
                           miro_ego_fragment_fn on_fragment,
                           miro_ego_sealed_fn on_sealed,
                           void *user,
                           miro_ego_pipeline_t **out, char *error, size_t error_len)
{
    if (out != NULL) {
        *out = NULL;
    }
    if (config == NULL || out == NULL || config->out_dir == NULL ||
        config->sbs_width <= 0 || config->sbs_width % 4 != 0 || config->sbs_width > 16384 ||
        config->height <= 0 || config->height % 2 != 0 || config->height > 16384 ||
        config->fps <= 0 || config->fps > 60 ||
        config->segment_frames < 0 || config->fragment_frames < 0 ||
        config->timescale < 0 || config->audio_tracks < 0 || config->audio_tracks > 1 ||
        config->intra_period < 0 || config->intra_period > 300 ||
        config->hevc < 0 || config->hevc > 1 ||
        config->rate_control < 0 || config->rate_control > 3 ||
        config->bitrate_kbps < 1 || config->bitrate_kbps > 100000 ||
        config->min_qp < 0 || config->max_qp > 51 || config->min_qp > config->max_qp ||
        config->intra_qp < config->min_qp || config->intra_qp > config->max_qp ||
        config->initial_qp < config->min_qp || config->initial_qp > config->max_qp ||
        config->vbv_ms < 1 || config->vbv_ms > 3000 ||
        config->input_stall_timeout_ms < 0 || config->input_stall_timeout_ms > 60000) {
        if (error != NULL && error_len > 0) {
            snprintf(error, error_len, "invalid_configuration");
        }
        return -1;
    }
    const int eff_intra = config->intra_period > 0 ? config->intra_period : config->fps;
    const int eff_frag = config->fragment_frames > 0
                             ? config->fragment_frames
                             : (config->segment_frames > 0 ? config->segment_frames : eff_intra);
    if (eff_frag > 0 && eff_frag % eff_intra != 0) {
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
    pipeline->writer_fd = -1;
    pipeline->config = *config;
    pipeline->split_ops = split_ops;
    pipeline->out_dir_owned = strdup(config->out_dir);
    pipeline->path_prefix_owned = strdup(config->path_prefix != NULL ? config->path_prefix : "");
    pipeline->output_file_owned =
        strdup((config->output_file != NULL && config->output_file[0] != '\0')
                   ? config->output_file
                   : "session.mp4");
    if (pipeline->out_dir_owned == NULL || pipeline->path_prefix_owned == NULL ||
        pipeline->output_file_owned == NULL) {
        if (error != NULL && error_len > 0) {
            snprintf(error, error_len, "out_of_memory");
        }
        goto open_failed;
    }
    if (posix_memalign(&pipeline->seal_io_buf, MIRO_EGO_SHA256_IO_ALIGN,
                       MIRO_EGO_SHA256_IO_BUF_SIZE) != 0 ||
        pipeline->seal_io_buf == NULL) {
        if (error != NULL && error_len > 0) {
            snprintf(error, error_len, "out_of_memory");
        }
        goto open_failed;
    }

    pipeline->config.out_dir = pipeline->out_dir_owned;
    pipeline->config.path_prefix = pipeline->path_prefix_owned;
    pipeline->config.output_file = pipeline->output_file_owned;
    pipeline->eye_width = config->sbs_width / MIRO_EGO_EYES;
    pipeline->fragment_frames = eff_frag;
    pipeline->timescale = config->timescale > 0 ? (uint32_t)config->timescale : 48000u;
    pipeline->audio_tracks = config->audio_tracks;
    pipeline->on_fragment = on_fragment;
    pipeline->on_sealed = on_sealed;
    pipeline->user = user;

    const char *resource = "finish lock";
    int ret = pthread_mutex_init(&pipeline->finish_lock, NULL);
    if (ret != 0) goto sync_failed;
    pipeline->finish_lock_initialized = 1;
    resource = "error lock";
    ret = pthread_mutex_init(&pipeline->error_lock, NULL);
    if (ret != 0) goto sync_failed;
    pipeline->error_initialized = 1;
    resource = "sync lock";
    ret = pthread_mutex_init(&pipeline->sync_lock, NULL);
    if (ret != 0) goto sync_failed;
    pipeline->sync_lock_initialized = 1;
    resource = "sync condition";
    ret = pthread_cond_init(&pipeline->sync_cond, NULL);
    if (ret != 0) goto sync_failed;
    pipeline->sync_cond_initialized = 1;
    resource = "writer lock";
    ret = pthread_mutex_init(&pipeline->writer_lock, NULL);
    if (ret != 0) goto sync_failed;
    pipeline->writer_lock_initialized = 1;
    resource = "writer condition";
    ret = pthread_cond_init(&pipeline->writer_cond, NULL);
    if (ret != 0) goto sync_failed;
    pipeline->writer_cond_initialized = 1;
    resource = "writer space condition";
    ret = pthread_cond_init(&pipeline->writer_space_cond, NULL);
    if (ret != 0) goto sync_failed;
    pipeline->writer_space_cond_initialized = 1;

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

    const char *worker = "writer";
    ret = pthread_create(&pipeline->writer_thread, NULL, writer_thread_main, pipeline);
    if (ret != 0) goto thread_failed;
    pipeline->writer_started = 1;
    worker = "combiner";
    ret = pthread_create(&pipeline->combiner_thread, NULL, mux_combiner_thread_main, pipeline);
    if (ret != 0) goto thread_failed;
    pipeline->combiner_started = 1;
    worker = "splitter";
    ret = pthread_create(&pipeline->split_thread, NULL, split_thread_main, pipeline);
    if (ret != 0) goto thread_failed;
    pipeline->split_started = 1;
    for (int eye = 0; eye < MIRO_EGO_EYES; eye += 1) {
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

int miro_ego_pipeline_submit_bundle(miro_ego_pipeline_t *pipeline,
                                    const unsigned char *jpeg, size_t length,
                                    const miro_ego_sync_bundle_t *bundle,
                                    int timeout_ms)
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
    if (bundle != NULL &&
        ((bundle->pcm_len > 0u && bundle->pcm == NULL) ||
         (bundle->meta_len > 0u && bundle->meta_json == NULL))) {
        atomic_fetch_add(&pipeline->drop_decoder_input, 1ULL);
        fail(pipeline, "sync_bundle_invalid");
        return -1;
    }

    sync_item_t *item = calloc(1, sizeof(*item));
    if (item == NULL) {
        atomic_fetch_add(&pipeline->drop_decoder_input, 1ULL);
        fail(pipeline, "out_of_memory: sync_item");
        return -1;
    }
    if (bundle != NULL) {
        item->frame_seq = bundle->frame_seq;
        item->monotonic_ns = bundle->monotonic_ns;
        item->sample_number = bundle->sample_number;
        item->duration_ticks = bundle->duration_ticks;
        if (bundle->pcm_len > 0u) {
            item->pcm = malloc(bundle->pcm_len);
            if (item->pcm == NULL) {
                sync_item_free(item);
                fail(pipeline, "out_of_memory: sync pcm");
                return -1;
            }
            memcpy(item->pcm, bundle->pcm, bundle->pcm_len);
            item->pcm_len = bundle->pcm_len;
        }
        if (bundle->meta_len > 0u) {
            item->meta_json = malloc(bundle->meta_len);
            if (item->meta_json == NULL) {
                sync_item_free(item);
                fail(pipeline, "out_of_memory: sync meta");
                return -1;
            }
            memcpy(item->meta_json, bundle->meta_json, bundle->meta_len);
            item->meta_len = bundle->meta_len;
        }
    }

    media_codec_buffer_t input;
    const int timeout = timeout_ms > 0 ? timeout_ms : DECODER_INPUT_TIMEOUT_MS;
    int ret = dequeue_input_with_retry(pipeline, &pipeline->decoder, &input, timeout, 1);
    if (ret != 0) {
        sync_item_free(item);
        atomic_fetch_add(&pipeline->drop_decoder_input, 1ULL);
        fail(pipeline, "decoder_input_dequeue_failed: ret=%d timeout_ms=%d bytes=%zu",
             ret, timeout, length);
        return -1;
    }
    if ((size_t)input.vstream_buf.size < length || input.vstream_buf.vir_ptr == NULL) {
        sync_item_free(item);
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

    pthread_mutex_lock(&pipeline->sync_lock);
    int sync_pushed = sync_push_locked(pipeline, item);
    pthread_mutex_unlock(&pipeline->sync_lock);
    if (sync_pushed != 0) {
        sync_item_free(item);
        input.vstream_buf.size = 0;
        input.vstream_buf.stream_end = 0;
        hb_mm_mc_queue_input_buffer(&pipeline->decoder, &input, DECODER_INPUT_TIMEOUT_MS);
        atomic_fetch_add(&pipeline->drop_decoder_input, 1ULL);
        fail(pipeline, "sync_queue_exhausted");
        return -1;
    }

    ret = hb_mm_mc_queue_input_buffer(&pipeline->decoder, &input, DECODER_INPUT_TIMEOUT_MS);
    if (ret != 0) {
        pthread_mutex_lock(&pipeline->sync_lock);
        sync_remove_tail_locked(pipeline, item);
        pthread_mutex_unlock(&pipeline->sync_lock);
        sync_item_free(item);
        atomic_fetch_add(&pipeline->drop_decoder_input, 1ULL);
        fail(pipeline, "decoder_input_queue_failed: ret=%d bytes=%zu", ret, length);
        return -1;
    }
    atomic_fetch_add(&pipeline->submitted, 1ULL);
    return 0;
}

int miro_ego_pipeline_submit(miro_ego_pipeline_t *pipeline, const unsigned char *jpeg,
                             size_t length, int timeout_ms)
{
    return miro_ego_pipeline_submit_bundle(pipeline, jpeg, length, NULL, timeout_ms);
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
            if (pipeline->eyes[eye].packets_initialized) {
                miro_ego_packet_queue_close(&pipeline->eyes[eye].packets);
            }
        }
        if (pipeline->sync_lock_initialized) {
            pthread_mutex_lock(&pipeline->sync_lock);
            pipeline->sync_stop = 1;
            if (pipeline->sync_cond_initialized) {
                pthread_cond_broadcast(&pipeline->sync_cond);
            }
            pthread_mutex_unlock(&pipeline->sync_lock);
        }
        if (pipeline->combiner_started) {
            pthread_join(pipeline->combiner_thread, NULL);
            pipeline->combiner_started = 0;
        }
        if (pipeline->writer_lock_initialized) {
            pthread_mutex_lock(&pipeline->writer_lock);
            pipeline->writer_stop = 1;
            if (pipeline->writer_cond_initialized) {
                pthread_cond_broadcast(&pipeline->writer_cond);
            }
            if (pipeline->writer_space_cond_initialized) {
                pthread_cond_broadcast(&pipeline->writer_space_cond);
            }
            pthread_mutex_unlock(&pipeline->writer_lock);
        }
        if (pipeline->writer_started) {
            pthread_join(pipeline->writer_thread, NULL);
            pipeline->writer_started = 0;
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
    while (pipeline->sync_head != NULL) {
        sync_item_t *item = pipeline->sync_head;
        pipeline->sync_head = item->next;
        sync_item_free(item);
    }
    while (pipeline->writer_head != NULL) {
        frag_job_t *job = pipeline->writer_head;
        pipeline->writer_head = job->next;
        free(job->data);
        free(job);
    }
    if (pipeline->decoder_started) {
        codec_stop(&pipeline->decoder);
    }
    if (pipeline->split_ops_initialized && pipeline->split_ops != NULL &&
        pipeline->split_ops->destroy != NULL) {
        pipeline->split_ops->destroy(pipeline->split_ctx);
    }
    if (pipeline->writer_space_cond_initialized) {
        pthread_cond_destroy(&pipeline->writer_space_cond);
    }
    if (pipeline->writer_cond_initialized) {
        pthread_cond_destroy(&pipeline->writer_cond);
    }
    if (pipeline->writer_lock_initialized) {
        pthread_mutex_destroy(&pipeline->writer_lock);
    }
    if (pipeline->sync_cond_initialized) {
        pthread_cond_destroy(&pipeline->sync_cond);
    }
    if (pipeline->sync_lock_initialized) {
        pthread_mutex_destroy(&pipeline->sync_lock);
    }
    if (pipeline->error_initialized) {
        pthread_mutex_destroy(&pipeline->error_lock);
    }
    if (pipeline->finish_lock_initialized) {
        pthread_mutex_destroy(&pipeline->finish_lock);
    }
    free(pipeline->seal_io_buf);
    free(pipeline->out_dir_owned);
    free(pipeline->path_prefix_owned);
    free(pipeline->output_file_owned);
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
    out->muxed_frames = atomic_load(&mutable_pipeline->muxed_frames);
    out->fragments_written = atomic_load(&mutable_pipeline->fragments_written);
    out->total_ticks = atomic_load(&mutable_pipeline->total_ticks);
    out->fdatasync_count = atomic_load(&mutable_pipeline->fdatasync_count);
    out->sum_fdatasync_ns = atomic_load(&mutable_pipeline->sum_fdatasync_ns);
    out->max_fdatasync_ns = atomic_load(&mutable_pipeline->max_fdatasync_ns);
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

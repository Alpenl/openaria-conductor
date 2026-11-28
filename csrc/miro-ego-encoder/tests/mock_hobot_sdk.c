#define _GNU_SOURCE
#include "mock_hobot_sdk.h"

#include "hb_media_codec.h"

#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

static mock_hobot_config_t g_cfg = {
    .sbs_width = 64,
    .height = 32,
    .intra_period = 3,
    .force_missing_idr = 0,
    .force_decoder_capacity = 0,
    .force_bad_decode_result = 0,
    .emit_stdout_noise = 1,
};

static pthread_mutex_t g_mock_lock = PTHREAD_MUTEX_INITIALIZER;
static int g_enc_index = 0;
static mc_video_codec_enc_params_t g_last_enc_params[2];
static int g_jpu_timeouts_left = 0;
static int g_vpu_timeouts_left = 0;
static atomic_ullong g_jpu_timeouts_injected = 0;
static atomic_ullong g_vpu_timeouts_injected = 0;

void mock_hobot_reset(const mock_hobot_config_t *cfg)
{
    pthread_mutex_lock(&g_mock_lock);
    if (cfg != NULL) {
        g_cfg = *cfg;
    }
    g_enc_index = 0;
    memset(g_last_enc_params, 0, sizeof(g_last_enc_params));
    g_jpu_timeouts_left = g_cfg.transient_jpu_input_timeouts;
    g_vpu_timeouts_left = g_cfg.transient_vpu_input_timeouts;
    atomic_store(&g_jpu_timeouts_injected, 0ULL);
    atomic_store(&g_vpu_timeouts_injected, 0ULL);
    pthread_mutex_unlock(&g_mock_lock);
}

void mock_hobot_last_enc_params(int eye, mc_video_codec_enc_params_t *out)
{
    if (out == NULL || eye < 0 || eye >= 2) {
        return;
    }
    pthread_mutex_lock(&g_mock_lock);
    *out = g_last_enc_params[eye];
    pthread_mutex_unlock(&g_mock_lock);
}

unsigned long long mock_hobot_jpu_timeout_injections(void)
{
    return atomic_load(&g_jpu_timeouts_injected);
}

unsigned long long mock_hobot_vpu_timeout_injections(void)
{
    return atomic_load(&g_vpu_timeouts_injected);
}

typedef struct {
    pthread_mutex_t lock;
    pthread_cond_t ready;
    int stopped;
    int is_encoder;
    int codec_id;
    int width;
    int height;
    int intra_period;
    uint32_t pending;
    uint32_t produced;
    uint8_t *in_buf;
    size_t in_buf_size;
    uint8_t *out_buf;
    size_t out_buf_size;
} mock_codec_t;

hb_s32 hb_mm_mc_get_rate_control_config(media_codec_context_t *context,
                                        mc_rate_control_params_t *params)
{
    (void)context;
    if (params != NULL) {
        memset(params, 0, sizeof(*params));
    }
    return 0;
}

hb_s32 hb_mm_mc_initialize(media_codec_context_t *context)
{
    if (g_cfg.emit_stdout_noise) {
        /* Simulate noisy vendor multimedia library printing to stdout. */
        printf("mock-hobot-sdk: initialize codec_id=%d encoder=%d\n",
               context->codec_id, (int)context->encoder);
        fflush(stdout);
    }
    mock_codec_t *mc = calloc(1, sizeof(*mc));
    if (mc == NULL) {
        return -ENOMEM;
    }
    pthread_mutex_init(&mc->lock, NULL);
    pthread_cond_init(&mc->ready, NULL);
    mc->is_encoder = context->encoder ? 1 : 0;
    mc->codec_id = context->codec_id;
    context->instance_handle = mc;
    return 0;
}

hb_s32 hb_mm_mc_configure(media_codec_context_t *context)
{
    mock_codec_t *mc = (mock_codec_t *)context->instance_handle;
    if (mc == NULL) {
        return -EINVAL;
    }
    if (mc->is_encoder) {
        pthread_mutex_lock(&g_mock_lock);
        if (g_enc_index < 2) {
            g_last_enc_params[g_enc_index++] = context->video_enc_params;
        }
        pthread_mutex_unlock(&g_mock_lock);
        mc->width = context->video_enc_params.width;
        mc->height = context->video_enc_params.height;
        mc->intra_period = g_cfg.intra_period > 0 ? g_cfg.intra_period : 3;
        uint32_t cfg_period = 0;
        switch (context->video_enc_params.rc_params.mode) {
        case MC_AV_RC_MODE_H264CBR:
            cfg_period = context->video_enc_params.rc_params.h264_cbr_params.intra_period;
            break;
        case MC_AV_RC_MODE_H264VBR:
            cfg_period = context->video_enc_params.rc_params.h264_vbr_params.intra_period;
            break;
        case MC_AV_RC_MODE_H264FIXQP:
            cfg_period = context->video_enc_params.rc_params.h264_fixqp_params.intra_period;
            break;
        case MC_AV_RC_MODE_H264AVBR:
            cfg_period = context->video_enc_params.rc_params.h264_avbr_params.intra_period;
            break;
        case MC_AV_RC_MODE_H265CBR:
            cfg_period = context->video_enc_params.rc_params.h265_cbr_params.intra_period;
            break;
        case MC_AV_RC_MODE_H265VBR:
            cfg_period = context->video_enc_params.rc_params.h265_vbr_params.intra_period;
            break;
        case MC_AV_RC_MODE_H265FIXQP:
            cfg_period = context->video_enc_params.rc_params.h265_fixqp_params.intra_period;
            break;
        case MC_AV_RC_MODE_H265AVBR:
            cfg_period = context->video_enc_params.rc_params.h265_avbr_params.intra_period;
            break;
        }
        if (cfg_period > 0) {
            mc->intra_period = (int)cfg_period;
        }
        mc->in_buf_size = (size_t)mc->width * (size_t)mc->height * 3u / 2u;
        mc->out_buf_size = 4096u;
    } else {
        mc->width = g_cfg.sbs_width;
        mc->height = g_cfg.height;
        mc->in_buf_size = context->video_dec_params.bitstream_buf_size > 0
                              ? context->video_dec_params.bitstream_buf_size
                              : 65536u;
        mc->out_buf_size = (size_t)mc->width * (size_t)mc->height * 3u / 2u;
    }
    mc->in_buf = calloc(1, mc->in_buf_size > 0 ? mc->in_buf_size : 64u);
    mc->out_buf = calloc(1, mc->out_buf_size > 0 ? mc->out_buf_size : 64u);
    if (mc->in_buf == NULL || mc->out_buf == NULL) {
        return -ENOMEM;
    }
    return 0;
}

hb_s32 hb_mm_mc_start(media_codec_context_t *context, const mc_av_codec_startup_params_t *params)
{
    (void)context;
    (void)params;
    return 0;
}

hb_s32 hb_mm_mc_pause(media_codec_context_t *context)
{
    mock_codec_t *mc = (mock_codec_t *)context->instance_handle;
    if (mc != NULL) {
        pthread_mutex_lock(&mc->lock);
        mc->stopped = 1;
        pthread_cond_broadcast(&mc->ready);
        pthread_mutex_unlock(&mc->lock);
    }
    return 0;
}

hb_s32 hb_mm_mc_stop(media_codec_context_t *context)
{
    return hb_mm_mc_pause(context);
}

hb_s32 hb_mm_mc_release(media_codec_context_t *context)
{
    mock_codec_t *mc = (mock_codec_t *)context->instance_handle;
    if (mc != NULL) {
        pthread_mutex_destroy(&mc->lock);
        pthread_cond_destroy(&mc->ready);
        free(mc->in_buf);
        free(mc->out_buf);
        free(mc);
        context->instance_handle = NULL;
    }
    return 0;
}

hb_s32 hb_mm_mc_dequeue_input_buffer(media_codec_context_t *context,
                                     media_codec_buffer_t *buffer, int timeout_ms)
{
    (void)timeout_ms;
    mock_codec_t *mc = (mock_codec_t *)context->instance_handle;
    if (mc == NULL || buffer == NULL) {
        return -EINVAL;
    }
    pthread_mutex_lock(&g_mock_lock);
    if (g_cfg.permanent_input_timeout) {
        pthread_mutex_unlock(&g_mock_lock);
        return HB_MEDIA_ERR_WAIT_TIME;
    }
    if (!mc->is_encoder && g_jpu_timeouts_left > 0) {
        g_jpu_timeouts_left -= 1;
        pthread_mutex_unlock(&g_mock_lock);
        atomic_fetch_add(&g_jpu_timeouts_injected, 1ULL);
        return (g_jpu_timeouts_left & 1) ? HB_MEDIA_ERR_WAIT_TIME : -ETIMEDOUT;
    }
    if (mc->is_encoder && g_vpu_timeouts_left > 0) {
        g_vpu_timeouts_left -= 1;
        pthread_mutex_unlock(&g_mock_lock);
        atomic_fetch_add(&g_vpu_timeouts_injected, 1ULL);
        return (g_vpu_timeouts_left & 1) ? HB_MEDIA_ERR_WAIT_TIME : -ETIMEDOUT;
    }
    pthread_mutex_unlock(&g_mock_lock);
    if (!mc->is_encoder) {
        buffer->type = MC_VIDEO_STREAM_BUFFER;
        buffer->vstream_buf.vir_ptr = mc->in_buf;
        buffer->vstream_buf.size = g_cfg.force_decoder_capacity > 0
                                       ? (uint32_t)g_cfg.force_decoder_capacity
                                       : (uint32_t)mc->in_buf_size;
    } else {
        const size_t y_size = (size_t)mc->width * (size_t)mc->height;
        buffer->type = MC_VIDEO_FRAME_BUFFER;
        buffer->vframe_buf.vir_ptr[0] = mc->in_buf;
        buffer->vframe_buf.vir_ptr[1] = mc->in_buf + y_size;
        buffer->vframe_buf.width = mc->width;
        buffer->vframe_buf.height = mc->height;
        buffer->vframe_buf.stride = mc->width;
        buffer->vframe_buf.pix_fmt = MC_PIXEL_FORMAT_NV12;
        buffer->vframe_buf.size = (uint32_t)mc->in_buf_size;
    }
    return 0;
}

hb_s32 hb_mm_mc_queue_input_buffer(media_codec_context_t *context,
                                   media_codec_buffer_t *buffer, int timeout_ms)
{
    (void)timeout_ms;
    mock_codec_t *mc = (mock_codec_t *)context->instance_handle;
    if (mc == NULL || buffer == NULL) {
        return -EINVAL;
    }
    uint32_t sz = mc->is_encoder ? buffer->vframe_buf.size : buffer->vstream_buf.size;
    if (sz > 0) {
        pthread_mutex_lock(&mc->lock);
        mc->pending += 1u;
        pthread_cond_signal(&mc->ready);
        pthread_mutex_unlock(&mc->lock);
    }
    return 0;
}

hb_s32 hb_mm_mc_dequeue_output_buffer(media_codec_context_t *context,
                                      media_codec_buffer_t *buffer,
                                      media_codec_output_buffer_info_t *info,
                                      int timeout_ms)
{
    mock_codec_t *mc = (mock_codec_t *)context->instance_handle;
    if (mc == NULL || buffer == NULL || info == NULL) {
        return -EINVAL;
    }
    pthread_mutex_lock(&g_mock_lock);
    const int forced_err = mc->is_encoder ? g_cfg.force_encoder_output_error
                                          : g_cfg.force_decoder_output_error;
    pthread_mutex_unlock(&g_mock_lock);
    if (forced_err != 0) {
        return forced_err;
    }

    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    const int wait_ms = timeout_ms > 0 ? (timeout_ms < 25 ? timeout_ms : 25) : 10;
    ts.tv_nsec += (long)wait_ms * 1000000L;
    while (ts.tv_nsec >= 1000000000L) {
        ts.tv_sec += 1;
        ts.tv_nsec -= 1000000000L;
    }

    pthread_mutex_lock(&mc->lock);
    while (mc->pending == 0u && !mc->stopped) {
        int rc = pthread_cond_timedwait(&mc->ready, &mc->lock, &ts);
        if (rc == ETIMEDOUT) {
            break;
        }
    }
    if (mc->pending == 0u) {
        pthread_mutex_unlock(&mc->lock);
        return -ETIMEDOUT;
    }
    mc->pending -= 1u;
    const uint32_t seq = mc->produced++;
    pthread_mutex_unlock(&mc->lock);

    if (!mc->is_encoder) {
        const size_t y_size = (size_t)mc->width * (size_t)mc->height;
        memset(mc->out_buf, (int)(0x20u + (seq & 0x3fu)), mc->out_buf_size);
        buffer->type = MC_VIDEO_FRAME_BUFFER;
        buffer->vframe_buf.vir_ptr[0] = mc->out_buf;
        buffer->vframe_buf.vir_ptr[1] = mc->out_buf + y_size;
        buffer->vframe_buf.width = mc->width;
        buffer->vframe_buf.height = mc->height;
        buffer->vframe_buf.stride = mc->width;
        buffer->vframe_buf.pix_fmt = MC_PIXEL_FORMAT_NV12;
        buffer->vframe_buf.size = (uint32_t)mc->out_buf_size;
        info->video_frame_info.decode_result = g_cfg.force_bad_decode_result ? 0 : 1;
    } else {
        const int is_idr = (!g_cfg.force_missing_idr) &&
                           (mc->intra_period > 0 && (seq % (uint32_t)mc->intra_period) == 0u);
        uint8_t *p = mc->out_buf;
        size_t len = 0;
        if (mc->codec_id == MEDIA_CODEC_ID_H265) {
            if (is_idr && seq == 0u) {
                /* Minimal Annex-B VPS(32) + SPS(33) + PPS(34) */
                static const uint8_t vps_sps_pps[] = {
                    0, 0, 0, 1, 0x40, 0x01, 0x0c, 0x01, 0xff, 0xff, 0x01, 0x60, 0x00, 0x00, 0x03, 0x00, 0xb0,
                    0, 0, 0, 1, 0x42, 0x01, 0x01, 0x01, 0x60, 0x00, 0x00, 0x03, 0x00, 0xb0, 0x00, 0x00, 0x03, 0x00, 0x00, 0x03, 0x00, 0x78,
                    0, 0, 0, 1, 0x44, 0x01, 0xc1, 0x73,
                };
                memcpy(p, vps_sps_pps, sizeof(vps_sps_pps));
                len = sizeof(vps_sps_pps);
            }
            /* Annex B H.265: type 19 (IDR_W_RADL) => byte3 = (19 << 1) = 0x26, byte4 = 0x01
             *                type 1  (TRAIL_R)    => byte3 = (1 << 1)  = 0x02, byte4 = 0x01 */
            p[len++] = 0; p[len++] = 0; p[len++] = 0; p[len++] = 1;
            p[len++] = is_idr ? 0x26u : 0x02u;
            p[len++] = 0x01u;
            p[len++] = (uint8_t)(seq & 0xffu);
            p[len++] = 0x5au;
            memset(p + len, (int)(seq & 0xffu), 56u);
            len += 56u;
        } else {
            if (is_idr && seq == 0u) {
                static const uint8_t sps_pps[] = {
                    0, 0, 0, 1, 0x67, 0x64, 0x00, 0x28, 0xac, 0xd9, 0x40,
                    0, 0, 0, 1, 0x68, 0xce, 0x38, 0x80,
                };
                memcpy(p, sps_pps, sizeof(sps_pps));
                len = sizeof(sps_pps);
            }
            /* Annex B H.264: IDR (0x65) or non-IDR slice (0x41) */
            p[len++] = 0; p[len++] = 0; p[len++] = 0; p[len++] = 1;
            p[len++] = is_idr ? 0x65u : 0x41u;
            p[len++] = (uint8_t)(seq & 0xffu);
            memset(p + len, (int)(seq & 0xffu), 58u);
            len += 58u;
        }
        buffer->type = MC_VIDEO_STREAM_BUFFER;
        buffer->vstream_buf.vir_ptr = p;
        buffer->vstream_buf.size = (uint32_t)len;
        buffer->vstream_buf.stream_end = 0;
    }
    return 0;
}

hb_s32 hb_mm_mc_queue_output_buffer(media_codec_context_t *context,
                                    media_codec_buffer_t *buffer, int timeout_ms)
{
    (void)context;
    (void)buffer;
    (void)timeout_ms;
    return 0;
}

int32_t hb_mem_invalidate_buf_with_vaddr(uint64_t virt_addr, uint64_t size)
{
    return (virt_addr == 0ULL || size == 0ULL) ? -EINVAL : 0;
}

int32_t hb_mem_flush_buf_with_vaddr(uint64_t virt_addr, uint64_t size)
{
    return (virt_addr == 0ULL || size == 0ULL) ? -EINVAL : 0;
}

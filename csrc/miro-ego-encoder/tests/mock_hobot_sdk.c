#define _GNU_SOURCE
#include "mock_hobot_sdk.h"

#include "hb_media_codec.h"
#include "hb_media_muxer.h"

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

typedef struct {
    char path[512];
    int codec_id;
    int fps;
    int fd;
    uint32_t sample_count;
    uint64_t mdat_bytes;
} mock_muxer_t;

static void write_be32(uint8_t *p, uint32_t v)
{
    p[0] = (uint8_t)(v >> 24);
    p[1] = (uint8_t)(v >> 16);
    p[2] = (uint8_t)(v >> 8);
    p[3] = (uint8_t)v;
}

static void write_be16(uint8_t *p, uint16_t v)
{
    p[0] = (uint8_t)(v >> 8);
    p[1] = (uint8_t)v;
}

static int write_all_fd(int fd, const void *buf, size_t len)
{
    const uint8_t *p = (const uint8_t *)buf;
    while (len > 0) {
        ssize_t n = write(fd, p, len);
        if (n < 0) {
            if (errno == EINTR) continue;
            return -1;
        }
        p += (size_t)n;
        len -= (size_t)n;
    }
    return 0;
}

static size_t wrap_box(uint8_t *out, const char type[4], const uint8_t *body, size_t body_len)
{
    const uint32_t total = (uint32_t)(8u + body_len);
    write_be32(out, total);
    memcpy(out + 4, type, 4);
    if (body_len > 0) {
        memcpy(out + 8, body, body_len);
    }
    return (size_t)total;
}

static int write_minimal_moov(mock_muxer_t *mx)
{
    /* Build a realistic MP4 moov box matching Hobot muxer layout (duration short by 1 frame
     * and reserved (0,0,0) colr nclx values to verify duration + colorimetry fixups). */
    const uint32_t timescale = 1000u;
    const uint32_t fps = mx->fps > 0 ? (uint32_t)mx->fps : 30u;
    const uint32_t frame_ticks = (timescale + fps - 1u) / fps;
    const uint32_t dur = mx->sample_count > 1u ? (mx->sample_count - 1u) * frame_ticks : 0u;

    /* colr box: 8 hdr + 4 "nclx" + 2+2+2 (0,0,0) + 1 (0x00) = 19 bytes */
    uint8_t colr_body[11];
    memcpy(colr_body, "nclx", 4);
    write_be16(colr_body + 4, 0);
    write_be16(colr_body + 6, 0);
    write_be16(colr_body + 8, 0);
    colr_body[10] = 0x00;
    uint8_t colr[32];
    size_t colr_len = wrap_box(colr, "colr", colr_body, sizeof(colr_body));

    /* sample entry (avc1 or hvc1): 78 bytes header + colr */
    uint8_t sample_body[128];
    memset(sample_body, 0, sizeof(sample_body));
    memcpy(sample_body + 78, colr, colr_len);
    uint8_t sample_entry[160];
    const char *fourcc = (mx->codec_id == MEDIA_CODEC_ID_H265) ? "hvc1" : "avc1";
    size_t sample_len = wrap_box(sample_entry, fourcc, sample_body, 78u + colr_len);

    /* stsd: 4 version/flags + 4 entry_count(1) + sample_entry */
    uint8_t stsd_body[192];
    memset(stsd_body, 0, 8);
    write_be32(stsd_body + 4, 1u);
    memcpy(stsd_body + 8, sample_entry, sample_len);
    uint8_t stsd[224];
    size_t stsd_len = wrap_box(stsd, "stsd", stsd_body, 8u + sample_len);

    uint8_t stbl[256];
    size_t stbl_len = wrap_box(stbl, "stbl", stsd, stsd_len);

    uint8_t minf[288];
    size_t minf_len = wrap_box(minf, "minf", stbl, stbl_len);

    /* mvhd / mdhd v0 body: 28 bytes (timescale @12, duration @16 inside body -> @20, @24 in box) */
    uint8_t hdr_body[28];
    memset(hdr_body, 0, sizeof(hdr_body));
    write_be32(hdr_body + 12, timescale);
    write_be32(hdr_body + 16, dur);
    uint8_t mvhd[40];
    size_t mvhd_len = wrap_box(mvhd, "mvhd", hdr_body, sizeof(hdr_body));
    uint8_t mdhd[40];
    size_t mdhd_len = wrap_box(mdhd, "mdhd", hdr_body, sizeof(hdr_body));

    /* mdia = mdhd + minf */
    uint8_t mdia_body[384];
    memcpy(mdia_body, mdhd, mdhd_len);
    memcpy(mdia_body + mdhd_len, minf, minf_len);
    uint8_t mdia[400];
    size_t mdia_len = wrap_box(mdia, "mdia", mdia_body, mdhd_len + minf_len);

    /* tkhd v0 body: 32 bytes (duration @20 inside body -> @28 in box) */
    uint8_t tkhd_body[32];
    memset(tkhd_body, 0, sizeof(tkhd_body));
    write_be32(tkhd_body + 20, dur);
    uint8_t tkhd[48];
    size_t tkhd_len = wrap_box(tkhd, "tkhd", tkhd_body, sizeof(tkhd_body));

    /* elst v0 body: 4 ver/flags + 4 entry_count(1) + 12 entry(dur, media_time=0, rate=1<<16) */
    uint8_t elst_body[20];
    memset(elst_body, 0, sizeof(elst_body));
    write_be32(elst_body + 4, 1u);
    write_be32(elst_body + 8, dur);
    write_be32(elst_body + 12, 0u);
    write_be32(elst_body + 16, 0x00010000u);
    uint8_t elst[32];
    size_t elst_len = wrap_box(elst, "elst", elst_body, sizeof(elst_body));

    uint8_t edts[48];
    size_t edts_len = wrap_box(edts, "edts", elst, elst_len);

    /* trak = tkhd + edts + mdia */
    uint8_t trak_body[512];
    size_t pos = 0;
    memcpy(trak_body + pos, tkhd, tkhd_len);
    pos += tkhd_len;
    memcpy(trak_body + pos, edts, edts_len);
    pos += edts_len;
    memcpy(trak_body + pos, mdia, mdia_len);
    pos += mdia_len;
    uint8_t trak[528];
    size_t trak_len = wrap_box(trak, "trak", trak_body, pos);

    /* moov = mvhd + trak */
    uint8_t moov_body[640];
    memcpy(moov_body, mvhd, mvhd_len);
    memcpy(moov_body + mvhd_len, trak, trak_len);
    uint8_t moov[656];
    size_t moov_len = wrap_box(moov, "moov", moov_body, mvhd_len + trak_len);

    return write_all_fd(mx->fd, moov, moov_len);
}

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
            /* Annex B H.265: type 19 (IDR_W_RADL) => byte3 = (19 << 1) = 0x26, byte4 = 0x01
             *                type 1  (TRAIL_R)    => byte3 = (1 << 1)  = 0x02, byte4 = 0x01 */
            p[0] = 0; p[1] = 0; p[2] = 0; p[3] = 1;
            p[4] = is_idr ? 0x26u : 0x02u;
            p[5] = 0x01u;
            p[6] = (uint8_t)(seq & 0xffu);
            p[7] = 0x5au;
            len = 64u;
            memset(p + 8, (int)(seq & 0xffu), len - 8u);
        } else {
            /* Annex B H.264: IDR (0x65) or non-IDR slice (0x41) */
            p[0] = 0; p[1] = 0; p[2] = 0; p[3] = 1;
            p[4] = is_idr ? 0x65u : 0x41u;
            p[5] = (uint8_t)(seq & 0xffu);
            len = 64u;
            memset(p + 6, (int)(seq & 0xffu), len - 6u);
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

/* --------------------------------------------------------------- muxer --- */

int hb_mm_mx_get_default_context(media_muxer_context_t *context)
{
    if (context == NULL) {
        return -1;
    }
    memset(context, 0, sizeof(*context));
    return 0;
}

int hb_mm_mx_initialize(media_muxer_context_t *context)
{
    if (context == NULL || context->output_file_name == NULL) {
        return -1;
    }
    mock_muxer_t *mx = calloc(1, sizeof(*mx));
    if (mx == NULL) {
        return -1;
    }
    snprintf(mx->path, sizeof(mx->path), "%s", context->output_file_name);
    mx->fd = -1;
    mx->fps = 30;
    context->instance_handle = mx;
    return 0;
}

int hb_mm_mx_add_stream(media_muxer_context_t *context, const mx_stream_params_t *params)
{
    mock_muxer_t *mx = (mock_muxer_t *)context->instance_handle;
    if (mx == NULL || params == NULL) {
        return -1;
    }
    mx->codec_id = params->codec_id;
    mx->fps = params->denominator > 0 ? params->denominator : 30;
    return 0;
}

int hb_mm_mx_start(media_muxer_context_t *context)
{
    mock_muxer_t *mx = (mock_muxer_t *)context->instance_handle;
    if (mx == NULL) {
        return -1;
    }
    mx->fd = open(mx->path, O_CREAT | O_TRUNC | O_RDWR, 0644);
    if (mx->fd < 0) {
        return -1;
    }
    /* Reserve 8-byte mdat header at file offset 0. */
    uint8_t mdat_hdr[8] = {0, 0, 0, 8, 'm', 'd', 'a', 't'};
    if (write_all_fd(mx->fd, mdat_hdr, sizeof(mdat_hdr)) != 0) {
        close(mx->fd);
        mx->fd = -1;
        return -1;
    }
    return 0;
}

int hb_mm_mx_write_stream(media_muxer_context_t *context, const mx_stream_t *stream)
{
    mock_muxer_t *mx = (mock_muxer_t *)context->instance_handle;
    if (mx == NULL || mx->fd < 0 || stream == NULL || stream->vir_ptr == NULL) {
        return -1;
    }
    if (write_all_fd(mx->fd, stream->vir_ptr, stream->size) != 0) {
        return -1;
    }
    mx->mdat_bytes += stream->size;
    mx->sample_count += 1u;
    return 0;
}

int hb_mm_mx_stop(media_muxer_context_t *context)
{
    if (context == NULL) {
        return -1;
    }
    mock_muxer_t *mx = (mock_muxer_t *)context->instance_handle;
    if (mx == NULL) {
        return 0;
    }
    int ret = 0;
    if (mx->fd >= 0) {
        /* Patch mdat box size at offset 0 and append moov box. */
        uint8_t sz[4];
        write_be32(sz, (uint32_t)(8ULL + mx->mdat_bytes));
        if (pwrite(mx->fd, sz, sizeof(sz), 0) != (ssize_t)sizeof(sz)) {
            ret = -1;
        } else if (mx->sample_count > 0u) {
            if (write_minimal_moov(mx) != 0) {
                ret = -1;
            }
        } else if (g_cfg.fail_empty_mux_stop) {
            ret = -1;
        }
        close(mx->fd);
        mx->fd = -1;
    }
    free(mx);
    context->instance_handle = NULL;
    return ret;
}

int32_t hb_mem_invalidate_buf_with_vaddr(uint64_t virt_addr, uint64_t size)
{
    return (virt_addr == 0ULL || size == 0ULL) ? -EINVAL : 0;
}

int32_t hb_mem_flush_buf_with_vaddr(uint64_t virt_addr, uint64_t size)
{
    return (virt_addr == 0ULL || size == 0ULL) ? -EINVAL : 0;
}

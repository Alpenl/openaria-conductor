#define _GNU_SOURCE
#include "ylx_stereo_pipeline.h"

#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stdarg.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#include "hb_media_codec.h"
#include "hb_media_muxer.h"

#define DECODER_INPUT_TIMEOUT_MS 200
#define DECODER_OUTPUT_TIMEOUT_MS 200
#define ENCODER_INPUT_TIMEOUT_MS 1000
#define ENCODER_OUTPUT_TIMEOUT_MS 200
#define ENCODER_DRAIN_POLLS 25 /* 5s ceiling on a stuck VPU drain */
#define ERROR_LEN 256
#define PATH_LEN 512

typedef struct segment {
    struct segment *next;
    media_muxer_context_t muxer;
    int eye;
    int index;
    char absolute[PATH_LEN];
    char relative[PATH_LEN];
    unsigned long long start_frame;
    unsigned long long end_frame;
} segment_t;

typedef struct {
    ylx_pipeline_t *pipeline;
    int eye;
    media_codec_context_t codec;
    pthread_t thread;
    segment_t *current;
    unsigned long long ordinal; /* encoded frames emitted so far */
} eye_t;

struct ylx_pipeline {
    ylx_pipeline_config_t config;
    int eye_width;
    int segment_frames;

    media_codec_context_t decoder;
    eye_t eyes[YLX_EYES];
    pthread_t split_thread;

    ylx_segment_closed_fn on_segment;
    void *user;

    /* Sealing a segment writes the MP4 index and fsyncs several megabytes; on
     * the encode path that stall backs up into dropped frames, so it runs on a
     * dedicated thread instead. */
    pthread_t closer_thread;
    pthread_mutex_t closer_lock;
    pthread_cond_t closer_signal;
    segment_t *closer_head;
    segment_t *closer_tail;
    int closer_stop;

    pthread_mutex_t ledger_lock;
    /* A closed segment is reported once both eyes have finished writing it. */
    int closed_index[YLX_EYES];
    char closed_path[YLX_EYES][PATH_LEN];
    unsigned long long closed_bytes[YLX_EYES];
    unsigned long long closed_start[YLX_EYES];
    unsigned long long closed_end[YLX_EYES];

    atomic_ullong offered;
    atomic_ullong submitted;
    atomic_ullong decoded;
    atomic_ullong fed[YLX_EYES];
    atomic_ullong encoded[YLX_EYES];
    atomic_ullong bytes[YLX_EYES];
    atomic_ullong drop_decoder_input;
    atomic_ullong drop_encoder_input;
    atomic_ullong drop_decoder_output;

    atomic_int stopping;   /* no more frames will be submitted */
    atomic_int split_done; /* the split thread fed the encoders everything */
    atomic_int failed;
    pthread_mutex_t error_lock;
    char error[ERROR_LEN];
};

static const char *const EYE_NAMES[YLX_EYES] = {"left", "right"};

static void fail(ylx_pipeline_t *pipeline, const char *format, ...)
    __attribute__((format(printf, 2, 3)));

static void fail(ylx_pipeline_t *pipeline, const char *format, ...)
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
                              const ylx_pipeline_config_t *config)
{
    memset(context, 0, sizeof(*context));
    context->codec_id = MEDIA_CODEC_ID_MJPEG;
    context->encoder = false;
    mc_video_codec_dec_params_t *params = &context->video_dec_params;
    params->feed_mode = MC_FEEDING_MODE_FRAME_SIZE;
    params->pix_fmt = MC_PIXEL_FORMAT_NV12;
    params->bitstream_buf_size =
        (uint32_t)((config->sbs_width * config->height * 3 / 2 + 0x3ff) & ~0x3ff);
    params->bitstream_buf_count = 6;
    params->frame_buf_count = 6;
    params->mjpeg_dec_config.rot_degree = MC_CCW_0;
    params->mjpeg_dec_config.mir_direction = MC_DIRECTION_NONE;
    params->mjpeg_dec_config.frame_crop_enable = false;
}

static void configure_encoder(media_codec_context_t *context,
                              const ylx_pipeline_config_t *config, int eye_width)
{
    memset(context, 0, sizeof(*context));
    context->codec_id = MEDIA_CODEC_ID_H264;
    context->encoder = true;
    mc_video_codec_enc_params_t *params = &context->video_enc_params;
    params->width = eye_width;
    params->height = config->height;
    params->pix_fmt = MC_PIXEL_FORMAT_NV12;
    params->bitstream_buf_size =
        (uint32_t)((eye_width * config->height * 3 / 2 + 0x3ff) & ~0x3ff);
    params->frame_buf_count = 8;
    params->external_frame_buf = false;
    params->bitstream_buf_count = 8;
    /* x5 wave521cl encodes no B frames and keeps a single reference. */
    params->gop_params.gop_preset_idx = 1;
    params->gop_params.decoding_refresh_type = 2;
    params->rot_degree = MC_CCW_0;
    params->mir_direction = MC_DIRECTION_NONE;
    params->frame_cropping_flag = false;
    params->enable_user_pts = 0;
    params->rc_params.mode = MC_AV_RC_MODE_H264CBR;
    hb_mm_mc_get_rate_control_config(context, &params->rc_params);
    params->rc_params.mode = MC_AV_RC_MODE_H264CBR;
    mc_h264_cbr_params_t *cbr = &params->rc_params.h264_cbr_params;
    /* The driver's CBR loop tracks the QP floor far more closely than the bit
     * budget, so the QP fields are what actually bound the stream size. */
    cbr->frame_rate = (uint32_t)config->fps;
    cbr->bit_rate = (uint32_t)config->bitrate_kbps;
    cbr->intra_period =
        (uint32_t)(config->intra_period > 0 ? config->intra_period : config->fps);
    cbr->intra_qp = (uint32_t)config->intra_qp;
    cbr->initial_rc_qp = config->initial_qp;
    cbr->vbv_buffer_size = (uint32_t)config->vbv_ms;
    cbr->mb_level_rc_enalbe = 1;
    cbr->min_qp_I = (uint32_t)config->min_qp;
    cbr->max_qp_I = 51;
    cbr->min_qp_P = (uint32_t)config->min_qp;
    cbr->max_qp_P = 51;
    cbr->min_qp_B = (uint32_t)config->min_qp;
    cbr->max_qp_B = 51;
    cbr->hvs_qp_enable = 1;
    cbr->hvs_qp_scale = 2;
    cbr->max_delta_qp = 10;
    cbr->qp_map_enable = 0;
    params->h264_enc_config.h264_profile = MC_H264_PROFILE_HP;
    params->h264_enc_config.h264_level = MC_H264_LEVEL4;
}

/* --------------------------------------------------------------- moov --- */

/*
 * The vendor MP4 muxer derives every duration from the presentation time of the
 * last sample, so a segment's declared duration covers one frame less than it
 * actually holds. Players that honour the edit list then drop the final frame.
 * The samples themselves are complete, so the fix is to extend the four
 * duration fields by exactly one frame interval.
 */

static uint32_t read_u32(const unsigned char *cursor)
{
    return ((uint32_t)cursor[0] << 24) | ((uint32_t)cursor[1] << 16) |
           ((uint32_t)cursor[2] << 8) | (uint32_t)cursor[3];
}

static void write_u32(unsigned char *cursor, uint32_t value)
{
    cursor[0] = (unsigned char)(value >> 24);
    cursor[1] = (unsigned char)(value >> 16);
    cursor[2] = (unsigned char)(value >> 8);
    cursor[3] = (unsigned char)value;
}

/* Offset of a version-0 box's duration field, relative to its 4CC. */
typedef struct {
    const char *type;
    size_t timescale_offset; /* 0 when the box carries no timescale */
    size_t duration_offset;
} duration_box_t;

static const duration_box_t DURATION_BOXES[] = {
    {"mvhd", 16, 20},
    {"tkhd", 0, 24},
    {"mdhd", 16, 20},
    {"elst", 0, 12},
};

static unsigned char *find_box(unsigned char *buffer, size_t length, const char *type)
{
    for (size_t offset = 4; offset + 4 <= length; offset += 1) {
        if (memcmp(buffer + offset, type, 4) == 0) {
            return buffer + offset;
        }
    }
    return NULL;
}

static int mp4_extend_durations(const char *path, int fps, char *reason, size_t reason_len)
{
    int fd = open(path, O_RDWR);
    if (fd < 0) {
        snprintf(reason, reason_len, "open: %s", strerror(errno));
        return -1;
    }

    off_t offset = 0;
    uint32_t moov_size = 0;
    off_t moov_offset = -1;
    for (;;) {
        unsigned char header[8];
        if (pread(fd, header, sizeof(header), offset) != (ssize_t)sizeof(header)) {
            break;
        }
        const uint32_t size = read_u32(header);
        if (size < 8) {
            break;
        }
        if (memcmp(header + 4, "moov", 4) == 0) {
            moov_offset = offset;
            moov_size = size;
            break;
        }
        offset += size;
    }
    if (moov_offset < 0 || moov_size > 8u * 1024u * 1024u) {
        snprintf(reason, reason_len, "no usable moov box");
        close(fd);
        return -1;
    }

    unsigned char *moov = malloc(moov_size);
    if (moov == NULL) {
        snprintf(reason, reason_len, "out of memory");
        close(fd);
        return -1;
    }
    if (pread(fd, moov, moov_size, moov_offset) != (ssize_t)moov_size) {
        snprintf(reason, reason_len, "short moov read");
        free(moov);
        close(fd);
        return -1;
    }

    uint32_t movie_timescale = 0;
    int result = 0;
    for (size_t index = 0; index < sizeof(DURATION_BOXES) / sizeof(DURATION_BOXES[0]);
         index += 1) {
        const duration_box_t *box = &DURATION_BOXES[index];
        unsigned char *found = find_box(moov, moov_size, box->type);
        if (found == NULL || (size_t)(found - moov) + box->duration_offset + 4 > moov_size) {
            snprintf(reason, reason_len, "%s box missing", box->type);
            result = -1;
            break;
        }
        if (found[4] != 0) {
            snprintf(reason, reason_len, "%s is not version 0", box->type);
            result = -1;
            break;
        }
        uint32_t timescale = movie_timescale;
        if (box->timescale_offset != 0) {
            timescale = read_u32(found + box->timescale_offset);
            if (index == 0) {
                movie_timescale = timescale;
            }
        }
        if (timescale == 0) {
            snprintf(reason, reason_len, "%s has no usable timescale", box->type);
            result = -1;
            break;
        }
        const uint32_t frame_ticks = (timescale + (uint32_t)fps - 1) / (uint32_t)fps;
        unsigned char *field = found + box->duration_offset;
        write_u32(field, read_u32(field) + frame_ticks);
        if (pwrite(fd, field, 4, moov_offset + (off_t)(field - moov)) != 4) {
            snprintf(reason, reason_len, "%s write failed: %s", box->type, strerror(errno));
            result = -1;
            break;
        }
    }

    free(moov);
    close(fd);
    return result;
}

/* --------------------------------------------------------------- muxer --- */

static int segment_open(ylx_pipeline_t *pipeline, eye_t *eye, int index,
                        unsigned long long start_frame)
{
    segment_t *segment = calloc(1, sizeof(*segment));
    if (segment == NULL) {
        fail(pipeline, "out_of_memory: segment %d", index);
        return -1;
    }
    segment->eye = eye->eye;
    segment->index = index;
    segment->start_frame = start_frame;
    snprintf(segment->relative, sizeof(segment->relative), "%s%s_%05d.mp4",
             pipeline->config.path_prefix, EYE_NAMES[eye->eye], index);
    snprintf(segment->absolute, sizeof(segment->absolute), "%s/%s_%05d.mp4",
             pipeline->config.out_dir, EYE_NAMES[eye->eye], index);

    if (hb_mm_mx_get_default_context(&segment->muxer) != 0) {
        fail(pipeline, "muxer_unavailable: default context for %s", segment->relative);
        free(segment);
        return -1;
    }
    segment->muxer.output_file_name = segment->absolute;
    segment->muxer.output_format = MEDIA_MUXER_OUTPUT_FORMAT_MP4;
    if (hb_mm_mx_initialize(&segment->muxer) != 0) {
        fail(pipeline, "muxer_unavailable: initialize %s", segment->relative);
        free(segment);
        return -1;
    }

    mx_stream_params_t params;
    memset(&params, 0, sizeof(params));
    params.codec_id = MEDIA_CODEC_ID_H264;
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
        free(segment);
        return -1;
    }
    eye->current = segment;
    return 0;
}

static void ledger_record(ylx_pipeline_t *pipeline, int eye_index, int segment_index,
                          const char *relative, unsigned long long bytes,
                          unsigned long long start_frame, unsigned long long end_frame)
{
    const int other = eye_index == 0 ? 1 : 0;
    bool report = false;
    char left_path[PATH_LEN];
    char right_path[PATH_LEN];
    unsigned long long left_bytes = 0;
    unsigned long long right_bytes = 0;
    unsigned long long report_start = 0;
    unsigned long long report_end = 0;

    pthread_mutex_lock(&pipeline->ledger_lock);
    pipeline->closed_index[eye_index] = segment_index;
    snprintf(pipeline->closed_path[eye_index], PATH_LEN, "%s", relative);
    pipeline->closed_bytes[eye_index] = bytes;
    pipeline->closed_start[eye_index] = start_frame;
    pipeline->closed_end[eye_index] = end_frame;
    if (pipeline->closed_index[other] == segment_index) {
        report = true;
        snprintf(left_path, PATH_LEN, "%s", pipeline->closed_path[0]);
        snprintf(right_path, PATH_LEN, "%s", pipeline->closed_path[1]);
        left_bytes = pipeline->closed_bytes[0];
        right_bytes = pipeline->closed_bytes[1];
        report_start = pipeline->closed_start[0];
        report_end = pipeline->closed_end[0];
        if (pipeline->closed_start[1] != report_start ||
            pipeline->closed_end[1] != report_end) {
            report = false;
        }
    }
    pthread_mutex_unlock(&pipeline->ledger_lock);

    if (!report) {
        return;
    }
    if (pipeline->on_segment != NULL) {
        pipeline->on_segment(pipeline->user, segment_index, report_start, report_end,
                             left_path, left_bytes, right_path, right_bytes);
    }
}

/* Hands the finished segment to the closer thread; never blocks the encoder. */
static void segment_close(ylx_pipeline_t *pipeline, eye_t *eye)
{
    segment_t *segment = eye->current;
    if (segment == NULL) {
        return;
    }
    eye->current = NULL;
    segment->end_frame = eye->ordinal;
    segment->next = NULL;
    pthread_mutex_lock(&pipeline->closer_lock);
    if (pipeline->closer_tail == NULL) {
        pipeline->closer_head = segment;
    } else {
        pipeline->closer_tail->next = segment;
    }
    pipeline->closer_tail = segment;
    pthread_cond_signal(&pipeline->closer_signal);
    pthread_mutex_unlock(&pipeline->closer_lock);
}

static void segment_seal(ylx_pipeline_t *pipeline, segment_t *segment)
{
    if (hb_mm_mx_stop(&segment->muxer) != 0) {
        fail(pipeline, "segment_seal_failed: stop %s", segment->relative);
    }
    if (segment->end_frame == segment->start_frame) {
        /* A take whose frame count lands exactly on a boundary opens a segment
         * it never fills; it is not part of the recording. */
        unlink(segment->absolute);
        return;
    }
    char reason[128] = {0};
    if (mp4_extend_durations(segment->absolute, pipeline->config.fps, reason,
                             sizeof(reason)) != 0) {
        fail(pipeline, "segment_seal_failed: %s duration fixup: %s", segment->relative,
             reason);
    }
    /* A closed segment must survive a power cut on its own. */
    int fd = open(segment->absolute, O_RDONLY);
    if (fd >= 0) {
        fsync(fd);
        close(fd);
    }
    struct stat metadata;
    unsigned long long bytes = 0;
    if (stat(segment->absolute, &metadata) == 0) {
        bytes = (unsigned long long)metadata.st_size;
    } else {
        fail(pipeline, "segment_seal_failed: stat %s", segment->relative);
    }
    ledger_record(pipeline, segment->eye, segment->index, segment->relative, bytes,
                  segment->start_frame, segment->end_frame);
}

static void *closer_thread_main(void *argument)
{
    ylx_pipeline_t *pipeline = (ylx_pipeline_t *)argument;
    for (;;) {
        pthread_mutex_lock(&pipeline->closer_lock);
        while (pipeline->closer_head == NULL && pipeline->closer_stop == 0) {
            pthread_cond_wait(&pipeline->closer_signal, &pipeline->closer_lock);
        }
        segment_t *segment = pipeline->closer_head;
        if (segment == NULL) {
            pthread_mutex_unlock(&pipeline->closer_lock);
            return NULL;
        }
        pipeline->closer_head = segment->next;
        if (pipeline->closer_head == NULL) {
            pipeline->closer_tail = NULL;
        }
        pthread_mutex_unlock(&pipeline->closer_lock);

        segment_seal(pipeline, segment);
        free(segment);
    }
}

/* -------------------------------------------------------------- threads --- */

static void *encoder_output_thread(void *argument)
{
    eye_t *eye = (eye_t *)argument;
    ylx_pipeline_t *pipeline = eye->pipeline;
    const int segment_frames = pipeline->segment_frames;

    if (segment_open(pipeline, eye, 0, 0) != 0) {
        return NULL;
    }

    int idle_polls = 0;
    for (;;) {
        media_codec_buffer_t buffer;
        media_codec_output_buffer_info_t info;
        memset(&buffer, 0, sizeof(buffer));
        memset(&info, 0, sizeof(info));
        int ret = hb_mm_mc_dequeue_output_buffer(&eye->codec, &buffer, &info,
                                                 ENCODER_OUTPUT_TIMEOUT_MS);
        if (ret != 0) {
            /* Only the split thread knows how many frames this eye owes, so the
             * encoder cannot stop draining before the split thread is done. */
            if (atomic_load(&pipeline->split_done) != 0 &&
                (eye->ordinal >= atomic_load(&pipeline->fed[eye->eye]) ||
                 ++idle_polls >= ENCODER_DRAIN_POLLS)) {
                break;
            }
            continue;
        }
        idle_polls = 0;
        if (buffer.vstream_buf.size > 0) {
            mx_stream_t stream;
            memset(&stream, 0, sizeof(stream));
            stream.is_audio = 0;
            stream.vir_ptr = buffer.vstream_buf.vir_ptr;
            stream.phy_ptr = buffer.vstream_buf.phy_ptr;
            stream.size = (uint32_t)buffer.vstream_buf.size;
            /* Segment-relative: a segment that starts at a non-zero media time
             * gets an edit list, and the muxer then marks its final sample
             * discardable. Each segment must stand on its own anyway. */
            stream.pts = eye->current != NULL ? eye->ordinal - eye->current->start_frame
                                              : eye->ordinal;
            stream.is_key_frame = info.video_stream_info.nalu_type == 5 ||
                                  (segment_frames > 0 &&
                                   eye->ordinal % (unsigned long long)segment_frames == 0);
            if (eye->current != NULL &&
                hb_mm_mx_write_stream(&eye->current->muxer, &stream) != 0) {
                fail(pipeline, "segment_write_failed: %s", eye->current->relative);
            }
            atomic_fetch_add(&pipeline->bytes[eye->eye],
                             (unsigned long long)buffer.vstream_buf.size);
        }
        hb_mm_mc_queue_output_buffer(&eye->codec, &buffer, ENCODER_OUTPUT_TIMEOUT_MS);
        eye->ordinal += 1;
        atomic_fetch_add(&pipeline->encoded[eye->eye], 1ULL);

        if (segment_frames > 0 &&
            eye->ordinal % (unsigned long long)segment_frames == 0) {
            segment_close(pipeline, eye);
            if (segment_open(pipeline, eye,
                             (int)(eye->ordinal / (unsigned long long)segment_frames),
                             eye->ordinal) != 0) {
                break;
            }
        }
    }

    segment_close(pipeline, eye);
    return NULL;
}

static void split_eye(const media_codec_buffer_t *source, media_codec_buffer_t *target,
                      int eye_width, int height, int x_offset)
{
    const int src_stride = source->vframe_buf.stride > 0 ? source->vframe_buf.stride
                                                         : source->vframe_buf.width;
    const int dst_stride =
        target->vframe_buf.stride > 0 ? target->vframe_buf.stride : eye_width;
    const uint8_t *src_y = source->vframe_buf.vir_ptr[0] + x_offset;
    const uint8_t *src_uv = source->vframe_buf.vir_ptr[1] + x_offset;
    uint8_t *dst_y = target->vframe_buf.vir_ptr[0];
    uint8_t *dst_uv = target->vframe_buf.vir_ptr[1];

    for (int row = 0; row < height; row += 1) {
        memcpy(dst_y + (size_t)row * dst_stride, src_y + (size_t)row * src_stride,
               (size_t)eye_width);
    }
    for (int row = 0; row < height / 2; row += 1) {
        memcpy(dst_uv + (size_t)row * dst_stride, src_uv + (size_t)row * src_stride,
               (size_t)eye_width);
    }
}

static void *split_thread_main(void *argument)
{
    ylx_pipeline_t *pipeline = (ylx_pipeline_t *)argument;
    const int eye_width = pipeline->eye_width;
    const int height = pipeline->config.height;

    for (;;) {
        media_codec_buffer_t frame;
        media_codec_output_buffer_info_t info;
        memset(&frame, 0, sizeof(frame));
        memset(&info, 0, sizeof(info));
        int ret = hb_mm_mc_dequeue_output_buffer(&pipeline->decoder, &frame, &info,
                                                 DECODER_OUTPUT_TIMEOUT_MS);
        if (ret != 0) {
            if (atomic_load(&pipeline->stopping) != 0 &&
                atomic_load(&pipeline->decoded) >= atomic_load(&pipeline->submitted)) {
                break;
            }
            continue;
        }
        if (frame.type != MC_VIDEO_FRAME_BUFFER || frame.vframe_buf.size == 0 ||
            info.video_frame_info.decode_result == 0) {
            atomic_fetch_add(&pipeline->drop_decoder_output, 1ULL);
            hb_mm_mc_queue_output_buffer(&pipeline->decoder, &frame, 0);
            continue;
        }
        if (frame.vframe_buf.width != pipeline->config.sbs_width ||
            frame.vframe_buf.height != height) {
            fail(pipeline, "decoder_geometry_mismatch: got %dx%d expected %dx%d",
                 frame.vframe_buf.width, frame.vframe_buf.height,
                 pipeline->config.sbs_width, height);
            hb_mm_mc_queue_output_buffer(&pipeline->decoder, &frame, 0);
            break;
        }
        atomic_fetch_add(&pipeline->decoded, 1ULL);

        for (int eye = 0; eye < YLX_EYES; eye += 1) {
            media_codec_buffer_t input;
            memset(&input, 0, sizeof(input));
            input.type = MC_VIDEO_FRAME_BUFFER;
            if (hb_mm_mc_dequeue_input_buffer(&pipeline->eyes[eye].codec, &input,
                                              ENCODER_INPUT_TIMEOUT_MS) != 0) {
                atomic_fetch_add(&pipeline->drop_encoder_input, 1ULL);
                continue;
            }
            input.type = MC_VIDEO_FRAME_BUFFER;
            input.vframe_buf.width = eye_width;
            input.vframe_buf.height = height;
            input.vframe_buf.pix_fmt = MC_PIXEL_FORMAT_NV12;
            input.vframe_buf.size = (uint32_t)(eye_width * height * 3 / 2);
            split_eye(&frame, &input, eye_width, height, eye * eye_width);
            if (hb_mm_mc_queue_input_buffer(&pipeline->eyes[eye].codec, &input,
                                            ENCODER_INPUT_TIMEOUT_MS) != 0) {
                atomic_fetch_add(&pipeline->drop_encoder_input, 1ULL);
            } else {
                atomic_fetch_add(&pipeline->fed[eye], 1ULL);
            }
        }
        hb_mm_mc_queue_output_buffer(&pipeline->decoder, &frame, 0);
    }
    atomic_store(&pipeline->split_done, 1);
    return NULL;
}

/* ------------------------------------------------------------- lifetime --- */

int ylx_pipeline_open(const ylx_pipeline_config_t *config,
                      ylx_segment_closed_fn on_segment, void *user,
                      ylx_pipeline_t **out, char *error, size_t error_len)
{
    if (config == NULL || out == NULL || config->out_dir == NULL ||
        config->sbs_width <= 0 || config->sbs_width % 4 != 0 || config->height <= 0 ||
        config->fps <= 0) {
        snprintf(error, error_len, "invalid_configuration");
        return -1;
    }

    ylx_pipeline_t *pipeline = calloc(1, sizeof(*pipeline));
    if (pipeline == NULL) {
        snprintf(error, error_len, "out_of_memory");
        return -1;
    }
    pipeline->config = *config;
    if (pipeline->config.path_prefix == NULL) {
        pipeline->config.path_prefix = "";
    }
    pipeline->eye_width = config->sbs_width / YLX_EYES;
    pipeline->segment_frames = config->segment_frames;
    pipeline->on_segment = on_segment;
    pipeline->user = user;
    pthread_mutex_init(&pipeline->ledger_lock, NULL);
    pthread_mutex_init(&pipeline->error_lock, NULL);
    pthread_mutex_init(&pipeline->closer_lock, NULL);
    pthread_cond_init(&pipeline->closer_signal, NULL);
    for (int eye = 0; eye < YLX_EYES; eye += 1) {
        pipeline->closed_index[eye] = -1;
    }

    configure_decoder(&pipeline->decoder, &pipeline->config);
    int ret = codec_start(&pipeline->decoder);
    if (ret != 0) {
        snprintf(error, error_len, "jpu_unavailable: MJPEG decoder start failed (%d)", ret);
        free(pipeline);
        return -1;
    }

    for (int eye = 0; eye < YLX_EYES; eye += 1) {
        pipeline->eyes[eye].pipeline = pipeline;
        pipeline->eyes[eye].eye = eye;
        configure_encoder(&pipeline->eyes[eye].codec, &pipeline->config,
                          pipeline->eye_width);
        ret = codec_start(&pipeline->eyes[eye].codec);
        if (ret != 0) {
            snprintf(error, error_len, "vpu_unavailable: %s H.264 encoder start failed (%d)",
                     EYE_NAMES[eye], ret);
            for (int done = 0; done < eye; done += 1) {
                codec_stop(&pipeline->eyes[done].codec);
            }
            codec_stop(&pipeline->decoder);
            free(pipeline);
            return -1;
        }
    }

    pthread_create(&pipeline->closer_thread, NULL, closer_thread_main, pipeline);
    pthread_create(&pipeline->split_thread, NULL, split_thread_main, pipeline);
    for (int eye = 0; eye < YLX_EYES; eye += 1) {
        pthread_create(&pipeline->eyes[eye].thread, NULL, encoder_output_thread,
                       &pipeline->eyes[eye]);
    }

    *out = pipeline;
    return 0;
}

int ylx_pipeline_submit(ylx_pipeline_t *pipeline, const unsigned char *jpeg,
                        size_t length, int timeout_ms)
{
    atomic_fetch_add(&pipeline->offered, 1ULL);
    if (atomic_load(&pipeline->failed) != 0) {
        return -1;
    }
    if (jpeg == NULL || length == 0) {
        atomic_fetch_add(&pipeline->drop_decoder_input, 1ULL);
        return -1;
    }

    media_codec_buffer_t input;
    memset(&input, 0, sizeof(input));
    input.type = MC_VIDEO_STREAM_BUFFER;
    if (hb_mm_mc_dequeue_input_buffer(&pipeline->decoder, &input,
                                      timeout_ms > 0 ? timeout_ms
                                                     : DECODER_INPUT_TIMEOUT_MS) != 0) {
        atomic_fetch_add(&pipeline->drop_decoder_input, 1ULL);
        return -1;
    }
    if ((size_t)input.vstream_buf.size < length) {
        hb_mm_mc_queue_input_buffer(&pipeline->decoder, &input, DECODER_INPUT_TIMEOUT_MS);
        atomic_fetch_add(&pipeline->drop_decoder_input, 1ULL);
        return -1;
    }
    input.type = MC_VIDEO_STREAM_BUFFER;
    input.vstream_buf.size = (uint32_t)length;
    input.vstream_buf.stream_end = 0;
    memcpy(input.vstream_buf.vir_ptr, jpeg, length);
    if (hb_mm_mc_queue_input_buffer(&pipeline->decoder, &input,
                                    DECODER_INPUT_TIMEOUT_MS) != 0) {
        atomic_fetch_add(&pipeline->drop_decoder_input, 1ULL);
        return -1;
    }
    atomic_fetch_add(&pipeline->submitted, 1ULL);
    return 0;
}

int ylx_pipeline_finish(ylx_pipeline_t *pipeline)
{
    if (atomic_exchange(&pipeline->stopping, 1) != 0) {
        return atomic_load(&pipeline->failed) == 0 ? 0 : -1;
    }
    pthread_join(pipeline->split_thread, NULL);
    for (int eye = 0; eye < YLX_EYES; eye += 1) {
        pthread_join(pipeline->eyes[eye].thread, NULL);
    }
    /* Only once both eyes stopped queueing can the closer drain to empty. */
    pthread_mutex_lock(&pipeline->closer_lock);
    pipeline->closer_stop = 1;
    pthread_cond_signal(&pipeline->closer_signal);
    pthread_mutex_unlock(&pipeline->closer_lock);
    pthread_join(pipeline->closer_thread, NULL);
    return atomic_load(&pipeline->failed) == 0 ? 0 : -1;
}

void ylx_pipeline_close(ylx_pipeline_t *pipeline)
{
    if (pipeline == NULL) {
        return;
    }
    ylx_pipeline_finish(pipeline);
    for (int eye = 0; eye < YLX_EYES; eye += 1) {
        codec_stop(&pipeline->eyes[eye].codec);
    }
    codec_stop(&pipeline->decoder);
    pthread_mutex_destroy(&pipeline->ledger_lock);
    pthread_mutex_destroy(&pipeline->error_lock);
    pthread_mutex_destroy(&pipeline->closer_lock);
    pthread_cond_destroy(&pipeline->closer_signal);
    free(pipeline);
}

void ylx_pipeline_stats(const ylx_pipeline_t *pipeline, ylx_pipeline_stats_t *out)
{
    ylx_pipeline_t *mutable_pipeline = (ylx_pipeline_t *)pipeline;
    memset(out, 0, sizeof(*out));
    out->offered = atomic_load(&mutable_pipeline->offered);
    out->submitted = atomic_load(&mutable_pipeline->submitted);
    out->decoded = atomic_load(&mutable_pipeline->decoded);
    out->drop_decoder_input = atomic_load(&mutable_pipeline->drop_decoder_input);
    out->drop_encoder_input = atomic_load(&mutable_pipeline->drop_encoder_input);
    out->drop_decoder_output = atomic_load(&mutable_pipeline->drop_decoder_output);
    for (int eye = 0; eye < YLX_EYES; eye += 1) {
        out->encoded[eye] = atomic_load(&mutable_pipeline->encoded[eye]);
        out->bytes[eye] = atomic_load(&mutable_pipeline->bytes[eye]);
    }
}

const char *ylx_pipeline_error(const ylx_pipeline_t *pipeline)
{
    return pipeline->error;
}

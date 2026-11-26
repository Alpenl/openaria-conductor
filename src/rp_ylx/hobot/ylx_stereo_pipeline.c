#define _GNU_SOURCE
#include "ylx_stereo_pipeline.h"
#include "ylx_packet_queue.h"

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
#include <time.h>
#include <unistd.h>

#include "hb_media_codec.h"
#include "hb_media_muxer.h"

#define DECODER_INPUT_TIMEOUT_MS 200
#define DECODER_OUTPUT_TIMEOUT_MS 200
#define DECODER_DRAIN_POLLS 25 /* 5s without output after input stops */
#define ENCODER_INPUT_TIMEOUT_MS 1000
#define ENCODER_OUTPUT_TIMEOUT_MS 200
#define ENCODER_DRAIN_POLLS 25 /* 5s ceiling on a stuck VPU drain */
#define ERROR_LEN 256
#define PATH_LEN 512
#define MUX_QUEUE_BYTES (64u * 1024u * 1024u) /* per eye, compressed packets */
#define MUX_QUEUE_SECONDS 32u
/* Bound active muxers independently of closed files waiting for disk sync.
 * Both limits include their respective in-flight work. */
#define MAX_PENDING_SEGMENTS 32u
#define MAX_PENDING_COMMITS 64u

typedef struct segment {
    struct segment *next;
    media_muxer_context_t muxer;
    int eye;
    int index;
    char absolute[PATH_LEN];
    char relative[PATH_LEN];
    unsigned long long start_frame;
    unsigned long long end_frame;
    unsigned long long finalize_ns;
} segment_t;

typedef struct {
    ylx_pipeline_t *pipeline;
    int eye;
    media_codec_context_t codec;
    pthread_t thread;
    pthread_t writer_thread;
    ylx_packet_queue_t packets;
    int codec_started, output_started, writer_started, packets_initialized;
    atomic_ullong max_write_ns;
    segment_t *current;
    unsigned long long ordinal; /* encoded frames emitted so far */
} eye_t;

typedef struct closed_pair {
    struct closed_pair *next;
    int index, eyes;
    char paths[YLX_EYES][PATH_LEN];
    unsigned long long bytes[YLX_EYES], start, end;
} closed_pair_t;

struct ylx_pipeline {
    ylx_pipeline_config_t config;
    char *out_dir_owned;
    char *path_prefix_owned;
    int eye_width;
    int segment_frames;

    media_codec_context_t decoder;
    eye_t eyes[YLX_EYES];
    pthread_t split_thread;
    int decoder_started, split_started, closer_started, committer_started;
    int finish_lock_initialized, ledger_initialized, error_initialized;
    int closer_lock_initialized, closer_signal_initialized, commit_signal_initialized;
    pthread_mutex_t finish_lock;
    int finished;

    ylx_segment_closed_fn on_segment;
    void *user;

    /* Release the vendor muxer and finish the MP4 index before queuing file
     * and directory sync. Slow durability must not occupy active muxer slots. */
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
    int pending_pairs, reported_pairs;
    atomic_ullong max_seal_ns;

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

static unsigned long long monotonic_time_ns(void);
static void observe_max(atomic_ullong *counter, unsigned long long elapsed);

/* Encoder metadata reports only I/P/B, not IDR. Inspect Annex B NAL headers
 * so stss indexes every true random-access picture, including within a file. */
static int h264_has_idr(const unsigned char *data, size_t length)
{
    for (size_t offset = 0; offset + 3 < length; offset++) {
        if (data[offset] == 0 && data[offset + 1] == 0 && data[offset + 2] == 1 &&
            (data[offset + 3] & 0x1f) == 5) return 1;
    }
    return 0;
}

static int h265_has_idr(const unsigned char *data, size_t length)
{
    for (size_t offset = 0; offset + 4 < length; offset++) {
        if (data[offset] == 0 && data[offset + 1] == 0 && data[offset + 2] == 1) {
            const unsigned type = (data[offset + 3] >> 1) & 0x3f;
            if ((type == 19 || type == 20) && (data[offset + 4] & 7) != 0) return 1;
        }
    }
    return 0;
}

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
        (uint32_t)(((uint64_t)config->sbs_width * (uint64_t)config->height * 3ULL / 2ULL + 0x3ffULL) & ~0x3ffULL);
    params->bitstream_buf_count = 6;
    params->frame_buf_count = 6;
    params->mjpeg_dec_config.rot_degree = MC_CCW_0;
    params->mjpeg_dec_config.mir_direction = MC_DIRECTION_NONE;
    params->mjpeg_dec_config.frame_crop_enable = false;
}

static int configure_encoder(media_codec_context_t *context,
                              const ylx_pipeline_config_t *config, int eye_width)
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
    /* x5 wave521cl encodes no B frames and keeps a single reference. */
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
    if (ret != 0) return ret;
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
            case 0: SET_BUDGET_RC(h265_cbr_params, ctu_level_rc_enalbe); break;
            case 1: SET_VBR(h265_vbr_params); break;
            case 2: SET_FIXQP(h265_fixqp_params); break;
            case 3: SET_BUDGET_RC(h265_avbr_params, ctu_level_rc_enalbe); break;
        }
    } else {
        switch (config->rate_control) {
            case 0: SET_BUDGET_RC(h264_cbr_params, mb_level_rc_enalbe); break;
            case 1: SET_VBR(h264_vbr_params); break;
            case 2: SET_FIXQP(h264_fixqp_params); break;
            case 3: SET_BUDGET_RC(h264_avbr_params, mb_level_rc_enalbe); break;
        }
        params->h264_enc_config.h264_profile = MC_H264_PROFILE_HP;
        params->h264_enc_config.h264_level = MC_H264_LEVEL_UNSPECIFIED;
    }
#undef SET_BUDGET_RC
#undef SET_VBR
#undef SET_FIXQP
    return 0;
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

static uint64_t read_u64(const unsigned char *cursor)
{
    return ((uint64_t)read_u32(cursor) << 32) | (uint64_t)read_u32(cursor + 4);
}

static void write_u32(unsigned char *cursor, uint32_t value)
{
    cursor[0] = (unsigned char)(value >> 24);
    cursor[1] = (unsigned char)(value >> 16);
    cursor[2] = (unsigned char)(value >> 8);
    cursor[3] = (unsigned char)value;
}

static int pread_full(int fd, void *buffer, size_t length, off_t offset)
{
    unsigned char *cursor = (unsigned char *)buffer;
    size_t remaining = length;
    while (remaining > 0) {
        ssize_t got = pread(fd, cursor, remaining, offset);
        if (got < 0) {
            if (errno == EINTR) {
                continue;
            }
            return -1;
        }
        if (got == 0) {
            errno = EPIPE;
            return -1;
        }
        cursor += (size_t)got;
        offset += got;
        remaining -= (size_t)got;
    }
    return 0;
}

static int pwrite_full(int fd, const void *buffer, size_t length, off_t offset)
{
    const unsigned char *cursor = (const unsigned char *)buffer;
    size_t remaining = length;
    while (remaining > 0) {
        ssize_t wrote = pwrite(fd, cursor, remaining, offset);
        if (wrote < 0) {
            if (errno == EINTR) {
                continue;
            }
            return -1;
        }
        if (wrote == 0) {
            errno = EIO;
            return -1;
        }
        cursor += (size_t)wrote;
        offset += wrote;
        remaining -= (size_t)wrote;
    }
    return 0;
}

/* Offset of a version-0 box's duration field, relative to the box header. */
typedef struct {
    const char *type;
    const char *path[4];
    size_t depth;
    size_t timescale_offset; /* 0 when the box carries no timescale */
    size_t duration_offset;
} duration_box_t;

static const duration_box_t DURATION_BOXES[] = {
    {"mvhd", {"mvhd", NULL, NULL, NULL}, 1, 20, 24},
    {"tkhd", {"trak", "tkhd", NULL, NULL}, 2, 0, 28},
    {"mdhd", {"trak", "mdia", "mdhd", NULL}, 3, 20, 24},
    {"elst", {"trak", "edts", "elst", NULL}, 3, 0, 16},
};

static int parse_box_header(const unsigned char *buf, size_t avail,
                            uint64_t *box_size, size_t *header_len)
{
    if (avail < 8) {
        return -1;
    }
    const uint32_t size32 = read_u32(buf);
    if (size32 == 1) {
        if (avail < 16) {
            return -1;
        }
        const uint64_t large = read_u64(buf + 8);
        if (large < 16 || large > avail) {
            return -1;
        }
        *box_size = large;
        *header_len = 16;
        return 0;
    }
    if (size32 == 0) {
        *box_size = avail;
        *header_len = 8;
        return 0;
    }
    if (size32 < 8 || (uint64_t)size32 > avail) {
        return -1;
    }
    *box_size = size32;
    *header_len = 8;
    return 0;
}

static unsigned char *child_box(unsigned char *parent, size_t parent_avail,
                                size_t skip, const char *type, size_t *child_avail)
{
    uint64_t length = 0;
    size_t parent_hdr = 0;
    if (parse_box_header(parent, parent_avail, &length, &parent_hdr) != 0) {
        return NULL;
    }
    const size_t header_delta = parent_hdr - 8;
    if (skip > (size_t)length || header_delta > (size_t)length - skip) {
        return NULL;
    }
    for (size_t offset = skip + header_delta; offset + 8 <= (size_t)length;) {
        uint64_t size = 0;
        size_t child_hdr = 0;
        if (parse_box_header(parent + offset, (size_t)length - offset,
                             &size, &child_hdr) != 0) {
            return NULL;
        }
        if (memcmp(parent + offset + 4, type, 4) == 0) {
            if (child_avail != NULL) {
                *child_avail = (size_t)size;
            }
            return parent + offset;
        }
        offset += (size_t)size;
    }
    return NULL;
}

static unsigned char *find_box_path(unsigned char *moov, size_t moov_size,
                                    const char *const *path, size_t depth,
                                    size_t *out_size)
{
    unsigned char *current = moov;
    size_t current_avail = moov_size;
    for (size_t index = 0; index < depth; index += 1) {
        current = child_box(current, current_avail, 8, path[index], &current_avail);
        if (current == NULL) {
            return NULL;
        }
    }
    if (out_size != NULL) {
        *out_size = current_avail;
    }
    return current;
}

static int mp4_fix_color(unsigned char *moov, size_t moov_size,
                         char *reason, size_t reason_len)
{
    static const struct { const char *type; size_t skip; } path[] = {
        {"trak", 8}, {"mdia", 8}, {"minf", 8}, {"stbl", 8},
        {"stsd", 8}, {"avc1", 16}, {"colr", 86},
    };
    unsigned char *box = moov;
    size_t box_avail = moov_size;
    for (size_t index = 0; index < sizeof(path) / sizeof(path[0]); index++) {
        unsigned char *parent = box;
        size_t parent_avail = box_avail;
        box = child_box(parent, parent_avail, path[index].skip, path[index].type, &box_avail);
        if (box == NULL && strcmp(path[index].type, "avc1") == 0) {
            box = child_box(parent, parent_avail, path[index].skip, "hvc1", &box_avail);
            if (box == NULL) {
                box = child_box(parent, parent_avail, path[index].skip, "hev1", &box_avail);
            }
        }
        if (box == NULL) {
            /* Some vendor versions omit colr and leave the SPS authoritative. */
            if (index == sizeof(path) / sizeof(path[0]) - 1) return 0;
            snprintf(reason, reason_len, "missing color path box %s", path[index].type);
            return -1;
        }
    }
    uint64_t colr_size = 0;
    size_t colr_hdr = 0;
    if (parse_box_header(box, box_avail, &colr_size, &colr_hdr) != 0 || colr_hdr != 8) {
        snprintf(reason, reason_len, "unsupported colr header");
        return -1;
    }
    const size_t size = (size_t)colr_size;
    if (size < 18 || (memcmp(box + 8, "nclx", 4) != 0 &&
                      memcmp(box + 8, "nclc", 4) != 0)) {
        snprintf(reason, reason_len, "unsupported colr box");
        return -1;
    }
    /* Vendor muxer writes reserved primaries/transfer and GBR for NV12.
     * The camera does not declare these; unspecified (2) is truthful. */
    for (size_t field = 12; field < 18; field += 2) {
        if (box[field] == 0 && box[field + 1] == 0) box[field + 1] = 2;
    }
    if (memcmp(box + 8, "nclx", 4) == 0) {
        if (size < 19) {
            snprintf(reason, reason_len, "short nclx box");
            return -1;
        }
        box[18] = 0x80; /* MJPEG -> NV12 row copy preserves full range. */
    }
    return 0;
}

static int mp4_extend_durations(const char *path, int fps, char *reason, size_t reason_len)
{
    if (fps <= 0) {
        snprintf(reason, reason_len, "invalid fps");
        return -1;
    }
    int fd = open(path, O_RDWR);
    if (fd < 0) {
        snprintf(reason, reason_len, "open: %s", strerror(errno));
        return -1;
    }

    off_t offset = 0;
    size_t moov_size = 0;
    off_t moov_offset = -1;
    for (;;) {
        unsigned char header[8];
        if (pread_full(fd, header, sizeof(header), offset) != 0) {
            break;
        }
        const uint32_t size32 = read_u32(header);
        uint64_t box_size = size32;
        if (size32 == 1) {
            unsigned char ext[8];
            if (pread_full(fd, ext, sizeof(ext), offset + 8) != 0) {
                break;
            }
            box_size = read_u64(ext);
            if (box_size < 16) {
                break;
            }
        } else if (size32 == 0) {
            off_t file_end = lseek(fd, 0, SEEK_END);
            if (file_end < 0 || file_end <= offset ||
                (uint64_t)(file_end - offset) < 8) {
                break;
            }
            box_size = (uint64_t)(file_end - offset);
        } else if (size32 < 8) {
            break;
        }
        if (memcmp(header + 4, "moov", 4) == 0) {
            if (box_size > 8u * 1024u * 1024u) {
                break;
            }
            moov_offset = offset;
            moov_size = (size_t)box_size;
            break;
        }
        if (offset < 0 || (uint64_t)offset > (uint64_t)INT64_MAX ||
            box_size > (uint64_t)INT64_MAX - (uint64_t)offset) {
            break;
        }
        offset += (off_t)box_size;
    }
    if (moov_offset < 0 || moov_size < 8 || moov_size > 8u * 1024u * 1024u) {
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
    if (pread_full(fd, moov, moov_size, moov_offset) != 0) {
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
        size_t found_size = 0;
        unsigned char *found = find_box_path(moov, moov_size, box->path, box->depth,
                                             &found_size);
        uint64_t parsed_size = 0;
        size_t found_hdr = 0;
        if (found == NULL ||
            parse_box_header(found, found_size, &parsed_size, &found_hdr) != 0 ||
            found_hdr != 8 || parsed_size < (uint64_t)box->duration_offset + 4u) {
            snprintf(reason, reason_len, "%s box missing", box->type);
            result = -1;
            break;
        }
        if (found[8] != 0) {
            snprintf(reason, reason_len, "%s is not version 0", box->type);
            result = -1;
            break;
        }
        unsigned char *field = found + box->duration_offset;
        if (strcmp(box->type, "elst") == 0) {
            const uint32_t entry_count = read_u32(found + 12);
            if (entry_count == 0) {
                snprintf(reason, reason_len, "elst has no entries");
                result = -1;
                break;
            }
            if ((uint64_t)entry_count > (parsed_size - 16u) / 12u) {
                snprintf(reason, reason_len, "elst entries exceed box size");
                result = -1;
                break;
            }
            field = NULL;
            for (uint32_t entry = 0; entry < entry_count; entry += 1) {
                unsigned char *entry_ptr = found + 16 + (size_t)entry * 12u;
                if (read_u32(entry_ptr + 4) != 0xffffffffu) {
                    field = entry_ptr;
                    break;
                }
            }
            if (field == NULL) {
                snprintf(reason, reason_len, "elst has no media entry");
                result = -1;
                break;
            }
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
        const uint64_t frame_ticks64 =
            ((uint64_t)timescale + (uint64_t)(uint32_t)fps - 1ULL) / (uint64_t)(uint32_t)fps;
        if (frame_ticks64 > UINT32_MAX) {
            snprintf(reason, reason_len, "%s frame ticks overflow", box->type);
            result = -1;
            break;
        }
        const uint32_t frame_ticks = (uint32_t)frame_ticks64;
        const uint32_t current_duration = read_u32(field);
        if (current_duration > UINT32_MAX - frame_ticks) {
            snprintf(reason, reason_len, "%s duration overflow", box->type);
            result = -1;
            break;
        }
        write_u32(field, current_duration + frame_ticks);
    }

    if (result == 0) result = mp4_fix_color(moov, moov_size, reason, reason_len);
    if (result == 0 && pwrite_full(fd, moov, moov_size, moov_offset) != 0) {
        snprintf(reason, reason_len, "moov write failed: %s", strerror(errno));
        result = -1;
    }

    free(moov);
    close(fd);
    return result;
}

/* --------------------------------------------------------------- muxer --- */

static int segment_open(ylx_pipeline_t *pipeline, eye_t *eye, int index,
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
                                           pending + 1));
    segment_t *segment = calloc(1, sizeof(*segment));
    if (segment == NULL) {
        atomic_fetch_sub(&pipeline->pending_segments, 1);
        fail(pipeline, "out_of_memory: segment %d", index);
        return -1;
    }
    segment->eye = eye->eye;
    segment->index = index;
    segment->start_frame = start_frame;
    int rel_len = snprintf(segment->relative, sizeof(segment->relative), "%s%s_%05d.mp4",
                           pipeline->config.path_prefix, EYE_NAMES[eye->eye], index);
    int abs_len = snprintf(segment->absolute, sizeof(segment->absolute), "%s/%s_%05d.mp4",
                           pipeline->config.out_dir, EYE_NAMES[eye->eye], index);
    if (rel_len < 0 || (size_t)rel_len >= sizeof(segment->relative) ||
        abs_len < 0 || (size_t)abs_len >= sizeof(segment->absolute)) {
        fail(pipeline, "segment_path_too_long: eye=%s index=%d",
             EYE_NAMES[eye->eye], index);
        free(segment);
        atomic_fetch_sub(&pipeline->pending_segments, 1);
        return -1;
    }

    if (hb_mm_mx_get_default_context(&segment->muxer) != 0) {
        fail(pipeline, "muxer_unavailable: default context for %s", segment->relative);
        free(segment);
        atomic_fetch_sub(&pipeline->pending_segments, 1);
        return -1;
    }
    segment->muxer.output_file_name = segment->absolute;
    segment->muxer.output_format = MEDIA_MUXER_OUTPUT_FORMAT_MP4;
    if (hb_mm_mx_initialize(&segment->muxer) != 0) {
        fail(pipeline, "muxer_unavailable: initialize %s", segment->relative);
        free(segment);
        atomic_fetch_sub(&pipeline->pending_segments, 1);
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
        unlink(segment->absolute);
        free(segment);
        atomic_fetch_sub(&pipeline->pending_segments, 1);
        return -1;
    }
    eye->current = segment;
    return 0;
}

static void ledger_record(ylx_pipeline_t *pipeline, int eye_index, int segment_index,
                          const char *relative, unsigned long long bytes,
                          unsigned long long start_frame, unsigned long long end_frame)
{
    pthread_mutex_lock(&pipeline->ledger_lock);
    if (segment_index < pipeline->reported_pairs) {
        fail(pipeline, "segment_boundary_mismatch: segment=%d", segment_index);
        pthread_mutex_unlock(&pipeline->ledger_lock);
        return;
    }
    closed_pair_t **position = &pipeline->closed_pairs;
    while (*position && (*position)->index < segment_index) position = &(*position)->next;
    closed_pair_t *pair = *position;
    if (!pair || pair->index != segment_index) {
        pair = pipeline->pending_pairs < 128 ? calloc(1, sizeof(*pair)) : NULL;
        if (!pair) {
            fail(pipeline, "segment_ledger_exhausted: pending pairs=%d", pipeline->pending_pairs);
            pthread_mutex_unlock(&pipeline->ledger_lock);
            return;
        }
        pair->index = segment_index;
        pair->start = start_frame;
        pair->end = end_frame;
        pair->next = *position;
        *position = pair;
        pipeline->pending_pairs++;
    }
    if (pair->start != start_frame || pair->end != end_frame ||
        (pair->eyes & (1 << eye_index))) {
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
    pair->eyes |= 1 << eye_index;
    pair->bytes[eye_index] = bytes;

    closed_pair_t *ready_head = NULL;
    closed_pair_t **ready_tail = &ready_head;
    while ((pair = pipeline->closed_pairs) && pair->eyes == 3 &&
           pair->index == pipeline->reported_pairs) {
        pipeline->closed_pairs = pair->next;
        pipeline->pending_pairs--;
        pipeline->reported_pairs++;
        pair->next = NULL;
        *ready_tail = pair;
        ready_tail = &pair->next;
    }
    pthread_mutex_unlock(&pipeline->ledger_lock);

    while (ready_head != NULL) {
        closed_pair_t *ready = ready_head;
        ready_head = ready->next;
        if (pipeline->on_segment) {
            pipeline->on_segment(pipeline->user, ready->index, ready->start, ready->end,
                                 ready->paths[0], ready->bytes[0],
                                 ready->paths[1], ready->bytes[1]);
        }
        free(ready);
    }
}

static void queue_segment(segment_t **head, segment_t **tail, segment_t *segment)
{
    segment->next = NULL;
    if (*tail != NULL) (*tail)->next = segment;
    else *head = segment;
    *tail = segment;
}

/* Hands the finished segment to the closer thread; never blocks the encoder. */
static void segment_close(ylx_pipeline_t *pipeline, eye_t *eye)
{
    segment_t *segment = eye->current;
    if (segment == NULL) return;
    eye->current = NULL;
    segment->end_frame = eye->ordinal;
    pthread_mutex_lock(&pipeline->closer_lock);
    queue_segment(&pipeline->closer_head, &pipeline->closer_tail, segment);
    pthread_cond_signal(&pipeline->closer_signal);
    pthread_mutex_unlock(&pipeline->closer_lock);
}

static int segment_finalize(ylx_pipeline_t *pipeline, segment_t *segment)
{
    if (hb_mm_mx_stop(&segment->muxer) != 0) {
        fail(pipeline, "segment_seal_failed: stop %s", segment->relative);
        return 0;
    }
    if (segment->end_frame == segment->start_frame) {
        /* A segment opened exactly at the final frame boundary has no media. */
        unlink(segment->absolute);
        return 0;
    }
    char reason[128] = {0};
    if (mp4_extend_durations(segment->absolute, pipeline->config.fps, reason,
                             sizeof(reason)) != 0) {
        fail(pipeline, "segment_seal_failed: %s duration fixup: %s", segment->relative,
             reason);
        return 0;
    }
    return 1;
}

static void segment_seal(ylx_pipeline_t *pipeline, segment_t *segment)
{
    /* A closed segment must survive a power cut on its own. */
    int fd = open(segment->absolute, O_RDONLY);
    if (fd < 0 || fsync(fd) != 0) {
        fail(pipeline, "segment_seal_failed: sync %s: %s", segment->relative, strerror(errno));
        if (fd >= 0) close(fd);
        return;
    }
    struct stat metadata;
    memset(&metadata, 0, sizeof(metadata));
    if (fstat(fd, &metadata) != 0 || metadata.st_size < 0) {
        fail(pipeline, "segment_seal_failed: fstat %s: %s", segment->relative, strerror(errno));
        close(fd);
        return;
    }
    const unsigned long long bytes = (unsigned long long)metadata.st_size;
    close(fd);
    fd = open(pipeline->config.out_dir, O_RDONLY | O_DIRECTORY);
    if (fd < 0 || fsync(fd) != 0) {
        fail(pipeline, "segment_seal_failed: sync video directory: %s", strerror(errno));
        if (fd >= 0) close(fd);
        return;
    }
    close(fd);
    ledger_record(pipeline, segment->eye, segment->index, segment->relative, bytes,
                  segment->start_frame, segment->end_frame);
}

static segment_t *wait_segment(ylx_pipeline_t *pipeline, pthread_cond_t *signal,
                               segment_t **head, segment_t **tail, const int *stopping)
{
    pthread_mutex_lock(&pipeline->closer_lock);
    while (*head == NULL && !*stopping) {
        pthread_cond_wait(signal, &pipeline->closer_lock);
    }
    segment_t *segment = *head;
    if (segment != NULL) {
        *head = segment->next;
        if (*head == NULL) *tail = NULL;
    }
    pthread_mutex_unlock(&pipeline->closer_lock);
    return segment;
}

static void *closer_thread_main(void *argument)
{
    ylx_pipeline_t *pipeline = argument;
    segment_t *segment;
    while ((segment = wait_segment(pipeline, &pipeline->closer_signal,
                    &pipeline->closer_head, &pipeline->closer_tail, &pipeline->closer_stop))) {
        unsigned long long started = monotonic_time_ns();
        int finalized = segment_finalize(pipeline, segment);
        segment->finalize_ns = monotonic_time_ns() - started;
        atomic_fetch_sub(&pipeline->pending_segments, 1);
        if (!finalized) {
            observe_max(&pipeline->max_seal_ns, segment->finalize_ns);
            free(segment);
            continue;
        }
        pthread_mutex_lock(&pipeline->closer_lock);
        unsigned pending = pipeline->pending_commits;
        if (pending < MAX_PENDING_COMMITS) {
            pipeline->pending_commits++;
            queue_segment(&pipeline->commit_head, &pipeline->commit_tail, segment);
            pthread_cond_signal(&pipeline->commit_signal);
        }
        pthread_mutex_unlock(&pipeline->closer_lock);
        if (pending >= MAX_PENDING_COMMITS) {
            fail(pipeline, "segment_commit_queue_exhausted: pending=%u limit=%u",
                 pending, MAX_PENDING_COMMITS);
            free(segment);
        }
    }
    return NULL;
}

static void *committer_thread_main(void *argument)
{
    ylx_pipeline_t *pipeline = argument;
    segment_t *segment;
    while ((segment = wait_segment(pipeline, &pipeline->commit_signal,
                    &pipeline->commit_head, &pipeline->commit_tail, &pipeline->commit_stop))) {
        unsigned long long started = monotonic_time_ns();
        segment_seal(pipeline, segment);
        observe_max(&pipeline->max_seal_ns, segment->finalize_ns + monotonic_time_ns() - started);
        free(segment);
        pthread_mutex_lock(&pipeline->closer_lock);
        pipeline->pending_commits--;
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
    while (previous < elapsed && !atomic_compare_exchange_weak(counter, &previous, elapsed)) {}
}

static void *mux_writer_thread(void *argument)
{
    eye_t *eye = argument;
    ylx_pipeline_t *pipeline = eye->pipeline;
    int writable = segment_open(pipeline, eye, 0, 0) == 0;
    ylx_packet_t *packet;
    while ((packet = ylx_packet_queue_pop(&eye->packets))) {
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
                eye->ordinal++;
                if (pipeline->segment_frames > 0 &&
                    eye->ordinal % (unsigned long long)pipeline->segment_frames == 0) {
                    segment_close(pipeline, eye);
                    writable = segment_open(pipeline, eye,
                        (int)(eye->ordinal / (unsigned long long)pipeline->segment_frames),
                        eye->ordinal) == 0;
                }
            }
        }
        ylx_packet_queue_done(&eye->packets, packet);
    }
    /* A failed write can have partially changed the tail MP4. Preserve that
     * file for diagnosis, but never emit it as a completed segment. */
    if (writable) segment_close(pipeline, eye);
    else if (eye->current) {
        hb_mm_mx_stop(&eye->current->muxer);
        free(eye->current);
        atomic_fetch_sub(&pipeline->pending_segments, 1);
        eye->current = NULL;
    }
    return NULL;
}

static void *encoder_output_thread(void *argument)
{
    eye_t *eye = (eye_t *)argument;
    ylx_pipeline_t *pipeline = eye->pipeline;
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
                (atomic_load(&pipeline->encoded[eye->eye]) >= atomic_load(&pipeline->fed[eye->eye]) ||
                 ++idle_polls >= ENCODER_DRAIN_POLLS)) {
                if (atomic_load(&pipeline->encoded[eye->eye]) < atomic_load(&pipeline->fed[eye->eye])) {
                    fail(pipeline, "encoder_drain_timeout: eye=%s ret=%d fed=%llu encoded=%llu",
                         EYE_NAMES[eye->eye], ret, atomic_load(&pipeline->fed[eye->eye]),
                         atomic_load(&pipeline->encoded[eye->eye]));
                }
                break;
            }
            continue;
        }
        idle_polls = 0;
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
        int key_frame = (pipeline->config.hevc ? h265_has_idr : h264_has_idr)(
            buffer.vstream_buf.vir_ptr, (size_t)buffer.vstream_buf.size);
        int queued = ylx_packet_queue_push(&eye->packets, buffer.vstream_buf.vir_ptr,
                                           (size_t)buffer.vstream_buf.size, key_frame);
        // Release the VPU buffer before any filesystem operation can block.
        int qret = hb_mm_mc_queue_output_buffer(&eye->codec, &buffer, ENCODER_OUTPUT_TIMEOUT_MS);
        if (queued != 0) {
            fail(pipeline, "video_write_backpressure: eye=%s capacity_bytes=%u capacity_frames=%u",
                 EYE_NAMES[eye->eye], MUX_QUEUE_BYTES, pipeline->config.fps * MUX_QUEUE_SECONDS);
            break;
        }
        if (qret != 0) {
            fail(pipeline, "encoder_output_queue_failed: eye=%s ret=%d",
                 EYE_NAMES[eye->eye], qret);
            break;
        }
        atomic_fetch_add(&pipeline->encoded[eye->eye], 1ULL);
    }
    ylx_packet_queue_close(&eye->packets);
    return NULL;
}

static void split_eye(const media_codec_buffer_t *source, media_codec_buffer_t *target,
                      int eye_width, int height, int x_offset)
{
    if (target == NULL) {
        return;
    }
    const uint32_t target_capacity = target->vframe_buf.size;
    target->vframe_buf.size = 0;
    if (source == NULL || eye_width <= 0 || height <= 0 || (height % 2) != 0 ||
        (eye_width % 2) != 0 || x_offset < 0 || (x_offset % 2) != 0 ||
        source->vframe_buf.pix_fmt != MC_PIXEL_FORMAT_NV12 ||
        source->vframe_buf.vir_ptr[0] == NULL || source->vframe_buf.vir_ptr[1] == NULL ||
        target->vframe_buf.vir_ptr[0] == NULL || target->vframe_buf.vir_ptr[1] == NULL) {
        return;
    }
    const int src_stride = source->vframe_buf.stride > 0 ? source->vframe_buf.stride
                                                         : source->vframe_buf.width;
    const int dst_stride =
        target->vframe_buf.stride > 0 ? target->vframe_buf.stride : eye_width;
    if (src_stride <= 0 || dst_stride <= 0 ||
        x_offset > src_stride - eye_width || dst_stride < eye_width) {
        return;
    }
    const uint64_t total_rows = (uint64_t)height + (uint64_t)height / 2ULL;
    if ((uint64_t)src_stride > UINT64_MAX / total_rows ||
        (uint64_t)dst_stride > UINT64_MAX / total_rows) {
        return;
    }
    const uint64_t src_bytes = (uint64_t)src_stride * total_rows;
    const uint64_t dst_bytes = (uint64_t)dst_stride * total_rows;
    if (src_bytes > (uint64_t)source->vframe_buf.size ||
        dst_bytes > (uint64_t)target_capacity || dst_bytes > UINT32_MAX) {
        return;
    }
    const uint8_t *src_y = source->vframe_buf.vir_ptr[0] + x_offset;
    const uint8_t *src_uv = source->vframe_buf.vir_ptr[1] + x_offset;
    uint8_t *dst_y = target->vframe_buf.vir_ptr[0];
    uint8_t *dst_uv = target->vframe_buf.vir_ptr[1];

    for (int row = 0; row < height; row += 1) {
        memcpy(dst_y + (size_t)row * (size_t)dst_stride,
               src_y + (size_t)row * (size_t)src_stride, (size_t)eye_width);
    }
    for (int row = 0; row < height / 2; row += 1) {
        memcpy(dst_uv + (size_t)row * (size_t)dst_stride,
               src_uv + (size_t)row * (size_t)src_stride, (size_t)eye_width);
    }
    target->vframe_buf.size = (uint32_t)dst_bytes;
}

static void *split_thread_main(void *argument)
{
    ylx_pipeline_t *pipeline = (ylx_pipeline_t *)argument;
    const int eye_width = pipeline->eye_width;
    const int height = pipeline->config.height;
    int idle_polls = 0;

    for (;;) {
        media_codec_buffer_t frame;
        media_codec_output_buffer_info_t info;
        memset(&frame, 0, sizeof(frame));
        memset(&info, 0, sizeof(info));
        int ret = hb_mm_mc_dequeue_output_buffer(&pipeline->decoder, &frame, &info,
                                                 DECODER_OUTPUT_TIMEOUT_MS);
        if (ret != 0) {
            if (atomic_load(&pipeline->stopping) != 0) {
                if (atomic_load(&pipeline->decoded) >= atomic_load(&pipeline->submitted)) break;
                if (++idle_polls >= DECODER_DRAIN_POLLS) {
                    fail(pipeline, "decoder_drain_timeout: ret=%d submitted=%llu decoded=%llu",
                         ret, atomic_load(&pipeline->submitted), atomic_load(&pipeline->decoded));
                    break;
                }
            }
            continue;
        }
        idle_polls = 0;
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
        atomic_fetch_add(&pipeline->decoded, 1ULL);

        for (int eye = 0; eye < YLX_EYES; eye += 1) {
            media_codec_buffer_t input;
            memset(&input, 0, sizeof(input));
            input.type = MC_VIDEO_FRAME_BUFFER;
            ret = hb_mm_mc_dequeue_input_buffer(&pipeline->eyes[eye].codec, &input,
                                                ENCODER_INPUT_TIMEOUT_MS);
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
            split_eye(&frame, &input, eye_width, height, eye * eye_width);
            if (input.vframe_buf.size == 0) {
                atomic_fetch_add(&pipeline->drop_encoder_input, 1ULL);
                fail(pipeline, "encoder_split_failed: eye=%s pix_fmt=%d",
                     EYE_NAMES[eye], frame.vframe_buf.pix_fmt);
                input.vframe_buf.size = 0;
                hb_mm_mc_queue_input_buffer(&pipeline->eyes[eye].codec, &input,
                                            ENCODER_INPUT_TIMEOUT_MS);
                break;
            }
            ret = hb_mm_mc_queue_input_buffer(&pipeline->eyes[eye].codec, &input,
                                              ENCODER_INPUT_TIMEOUT_MS);
            if (ret != 0) {
                atomic_fetch_add(&pipeline->drop_encoder_input, 1ULL);
                fail(pipeline, "encoder_input_queue_failed: eye=%s ret=%d",
                     EYE_NAMES[eye], ret);
                break;
            } else {
                atomic_fetch_add(&pipeline->fed[eye], 1ULL);
            }
        }
        hb_mm_mc_queue_output_buffer(&pipeline->decoder, &frame, 0);
        if (atomic_load(&pipeline->failed)) break;
    }
    atomic_store(&pipeline->split_done, 1);
    return NULL;
}

/* ------------------------------------------------------------- lifetime --- */

int ylx_pipeline_open(const ylx_pipeline_config_t *config,
                      ylx_segment_closed_fn on_segment, void *user,
                      ylx_pipeline_t **out, char *error, size_t error_len)
{
    if (out != NULL) *out = NULL;
    if (config == NULL || out == NULL || config->out_dir == NULL ||
        config->sbs_width <= 0 || config->sbs_width % 4 != 0 || config->sbs_width > 16384 ||
        config->height <= 0 || config->height % 2 != 0 || config->height > 16384 ||
        config->fps <= 0 || config->fps > 60 || config->segment_frames < 0 || config->intra_period < 0 ||
        config->intra_period > 300 || config->hevc < 0 || config->hevc > 1 ||
        config->rate_control < 0 || config->rate_control > 3 ||
        config->bitrate_kbps < 1 || config->bitrate_kbps > 100000 ||
        config->min_qp < 0 || config->max_qp > 51 || config->min_qp > config->max_qp ||
        config->intra_qp < config->min_qp || config->intra_qp > config->max_qp ||
        config->initial_qp < config->min_qp || config->initial_qp > config->max_qp ||
        config->vbv_ms < 1 || config->vbv_ms > 3000 ||
        (config->segment_frames > 0 && config->segment_frames %
         (config->intra_period > 0 ? config->intra_period : config->fps) != 0)) {
        if (error != NULL && error_len > 0) snprintf(error, error_len, "invalid_configuration");
        return -1;
    }

    ylx_pipeline_t *pipeline = calloc(1, sizeof(*pipeline));
    if (pipeline == NULL) {
        if (error != NULL && error_len > 0) snprintf(error, error_len, "out_of_memory");
        return -1;
    }
    pipeline->config = *config;
    pipeline->out_dir_owned = strdup(config->out_dir);
    pipeline->path_prefix_owned = strdup(config->path_prefix != NULL ? config->path_prefix : "");
    if (pipeline->out_dir_owned == NULL || pipeline->path_prefix_owned == NULL) {
        if (error != NULL && error_len > 0) snprintf(error, error_len, "out_of_memory");
        goto open_failed;
    }
    pipeline->config.out_dir = pipeline->out_dir_owned;
    pipeline->config.path_prefix = pipeline->path_prefix_owned;
    pipeline->eye_width = config->sbs_width / YLX_EYES;
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
    for (int eye = 0; eye < YLX_EYES; eye += 1) {
        pipeline->eyes[eye].pipeline = pipeline;
        pipeline->eyes[eye].eye = eye;
        resource = eye == 0 ? "left queue" : "right queue";
        ret = ylx_packet_queue_init(&pipeline->eyes[eye].packets, MUX_QUEUE_BYTES,
                                    config->fps * MUX_QUEUE_SECONDS);
        if (ret != 0) goto sync_failed;
        pipeline->eyes[eye].packets_initialized = 1;
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

    for (int eye = 0; eye < YLX_EYES; eye += 1) {
        ret = configure_encoder(&pipeline->eyes[eye].codec, &pipeline->config,
                          pipeline->eye_width);
        if (ret == 0) ret = codec_start(&pipeline->eyes[eye].codec);
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
    for (int eye = 0; eye < YLX_EYES; eye += 1) {
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
    ylx_pipeline_close(pipeline);
    return -1;
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
        fail(pipeline, "decoder_input_invalid: bytes=%zu", length);
        return -1;
    }

    media_codec_buffer_t input;
    memset(&input, 0, sizeof(input));
    input.type = MC_VIDEO_STREAM_BUFFER;
    const int timeout = timeout_ms > 0 ? timeout_ms : DECODER_INPUT_TIMEOUT_MS;
    int ret = hb_mm_mc_dequeue_input_buffer(&pipeline->decoder, &input, timeout);
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
    ret = hb_mm_mc_queue_input_buffer(&pipeline->decoder, &input, DECODER_INPUT_TIMEOUT_MS);
    if (ret != 0) {
        atomic_fetch_add(&pipeline->drop_decoder_input, 1ULL);
        fail(pipeline, "decoder_input_queue_failed: ret=%d bytes=%zu", ret, length);
        return -1;
    }
    atomic_fetch_add(&pipeline->submitted, 1ULL);
    return 0;
}

int ylx_pipeline_finish(ylx_pipeline_t *pipeline)
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
        if (pipeline->split_started) pthread_join(pipeline->split_thread, NULL);
        else atomic_store(&pipeline->split_done, 1);
        for (int eye = 0; eye < YLX_EYES; eye += 1) {
            if (pipeline->eyes[eye].output_started) pthread_join(pipeline->eyes[eye].thread, NULL);
            /* Startup can fail after a writer starts but before its producer does. */
            if (pipeline->eyes[eye].packets_initialized) ylx_packet_queue_close(&pipeline->eyes[eye].packets);
            if (pipeline->eyes[eye].writer_started) pthread_join(pipeline->eyes[eye].writer_thread, NULL);
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

void ylx_pipeline_close(ylx_pipeline_t *pipeline)
{
    if (pipeline == NULL) {
        return;
    }
    ylx_pipeline_finish(pipeline);
    for (int eye = 0; eye < YLX_EYES; eye += 1) {
        if (pipeline->eyes[eye].codec_started) codec_stop(&pipeline->eyes[eye].codec);
        if (pipeline->eyes[eye].packets_initialized) ylx_packet_queue_destroy(&pipeline->eyes[eye].packets);
    }
    while (pipeline->closed_pairs) {
        closed_pair_t *pair = pipeline->closed_pairs;
        pipeline->closed_pairs = pair->next;
        free(pair);
    }
    if (pipeline->decoder_started) codec_stop(&pipeline->decoder);
    if (pipeline->ledger_initialized) pthread_mutex_destroy(&pipeline->ledger_lock);
    if (pipeline->error_initialized) pthread_mutex_destroy(&pipeline->error_lock);
    if (pipeline->commit_signal_initialized) pthread_cond_destroy(&pipeline->commit_signal);
    if (pipeline->closer_lock_initialized) pthread_mutex_destroy(&pipeline->closer_lock);
    if (pipeline->closer_signal_initialized) pthread_cond_destroy(&pipeline->closer_signal);
    if (pipeline->finish_lock_initialized) pthread_mutex_destroy(&pipeline->finish_lock);
    free(pipeline->out_dir_owned);
    free(pipeline->path_prefix_owned);
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
    out->max_seal_ns = atomic_load(&mutable_pipeline->max_seal_ns);
    for (int eye = 0; eye < YLX_EYES; eye += 1) {
        out->encoded[eye] = atomic_load(&mutable_pipeline->encoded[eye]);
        out->bytes[eye] = atomic_load(&mutable_pipeline->bytes[eye]);
        out->max_write_ns[eye] = atomic_load(&mutable_pipeline->eyes[eye].max_write_ns);
        ylx_packet_queue_t *queue = &mutable_pipeline->eyes[eye].packets;
        pthread_mutex_lock(&queue->lock);
        out->write_queue_peak_bytes[eye] = queue->peak_bytes;
        out->write_queue_peak_frames[eye] = queue->peak_frames;
        out->write_queue_rejected[eye] = queue->rejected;
        pthread_mutex_unlock(&queue->lock);
    }
}

const char *ylx_pipeline_error(const ylx_pipeline_t *pipeline)
{
    /* An input error is reported before worker drain completes. Copy under the
     * lock so the caller never races the first error from another thread. */
    static _Thread_local char error[ERROR_LEN];
    ylx_pipeline_t *mutable_pipeline = (ylx_pipeline_t *)pipeline;
    pthread_mutex_lock(&mutable_pipeline->error_lock);
    memcpy(error, pipeline->error, sizeof(error));
    pthread_mutex_unlock(&mutable_pipeline->error_lock);
    return error;
}

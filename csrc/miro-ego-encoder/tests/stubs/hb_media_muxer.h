#ifndef MIRO_EGO_STUB_HB_MEDIA_MUXER_H
#define MIRO_EGO_STUB_HB_MEDIA_MUXER_H

#include <stdint.h>

enum {
    MEDIA_MUXER_OUTPUT_FORMAT_MP4 = 1,
};

typedef struct {
    const char *output_file_name;
    int output_format;
    void *instance_handle; /* internal mock handle */
} media_muxer_context_t;

typedef struct {
    int codec_id;
    int numerator;
    int denominator;
    struct {
        int width;
        int height;
        int pix_fmt;
        uint32_t frame_rate;
        uint32_t bit_rate;
    } video_params;
} mx_stream_params_t;

typedef struct {
    uint8_t *vir_ptr;
    uint32_t size;
    uint64_t pts;
    int is_key_frame;
} mx_stream_t;

int hb_mm_mx_get_default_context(media_muxer_context_t *context);
int hb_mm_mx_initialize(media_muxer_context_t *context);
int hb_mm_mx_add_stream(media_muxer_context_t *context, const mx_stream_params_t *params);
int hb_mm_mx_start(media_muxer_context_t *context);
int hb_mm_mx_stop(media_muxer_context_t *context);
int hb_mm_mx_write_stream(media_muxer_context_t *context, const mx_stream_t *stream);

#endif /* MIRO_EGO_STUB_HB_MEDIA_MUXER_H */

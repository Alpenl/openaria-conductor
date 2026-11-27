#ifndef MIRO_EGO_STUB_HB_MEDIA_CODEC_H
#define MIRO_EGO_STUB_HB_MEDIA_CODEC_H

#include <stdbool.h>
#include <stdint.h>

typedef int32_t hb_s32;

#ifndef HB_MEDIA_ERR_WAIT_TIME
#define HB_MEDIA_ERR_WAIT_TIME (-268435443)
#endif

enum {
    MEDIA_CODEC_ID_MJPEG = 1,
    MEDIA_CODEC_ID_H264 = 2,
    MEDIA_CODEC_ID_H265 = 3,
};

enum {
    MC_FEEDING_MODE_FRAME_SIZE = 1,
    MC_PIXEL_FORMAT_NV12 = 1,
    MC_CCW_0 = 0,
    MC_DIRECTION_NONE = 0,
    MC_H264_PROFILE_HP = 1,
    MC_H264_LEVEL_UNSPECIFIED = 0,
    MC_H265_LEVEL_UNSPECIFIED = 0,
    MC_VIDEO_FRAME_BUFFER = 1,
    MC_VIDEO_STREAM_BUFFER = 2,
};

typedef enum {
    MC_AV_RC_MODE_H264CBR = 0,
    MC_AV_RC_MODE_H264VBR = 1,
    MC_AV_RC_MODE_H264FIXQP = 2,
    MC_AV_RC_MODE_H264AVBR = 3,
    MC_AV_RC_MODE_H265CBR = 4,
    MC_AV_RC_MODE_H265VBR = 5,
    MC_AV_RC_MODE_H265FIXQP = 6,
    MC_AV_RC_MODE_H265AVBR = 7,
} mc_video_rate_control_mode_t;

typedef struct {
    uint32_t frame_rate;
    uint32_t bit_rate;
    uint32_t intra_period;
    uint32_t intra_qp;
    int initial_rc_qp;
    int vbv_buffer_size;
    int mb_level_rc_enalbe;
    int ctu_level_rc_enalbe;
    uint32_t min_qp_I;
    uint32_t min_qp_P;
    uint32_t min_qp_B;
    uint32_t max_qp_I;
    uint32_t max_qp_P;
    uint32_t max_qp_B;
    int hvs_qp_enable;
    int hvs_qp_scale;
    int max_delta_qp;
    int qp_map_enable;
} mc_rate_control_budget_t;

typedef struct {
    uint32_t frame_rate;
    uint32_t intra_period;
    uint32_t intra_qp;
    int qp_map_enable;
} mc_rate_control_vbr_t;

typedef struct {
    uint32_t frame_rate;
    uint32_t intra_period;
    uint32_t force_qp_I;
    uint32_t force_qp_P;
    uint32_t force_qp_B;
} mc_rate_control_fixqp_t;

typedef struct {
    mc_video_rate_control_mode_t mode;
    mc_rate_control_budget_t h264_cbr_params;
    mc_rate_control_vbr_t h264_vbr_params;
    mc_rate_control_fixqp_t h264_fixqp_params;
    mc_rate_control_budget_t h264_avbr_params;
    mc_rate_control_budget_t h265_cbr_params;
    mc_rate_control_vbr_t h265_vbr_params;
    mc_rate_control_fixqp_t h265_fixqp_params;
    mc_rate_control_budget_t h265_avbr_params;
} mc_rate_control_params_t;

typedef struct {
    int feed_mode;
    int pix_fmt;
    uint32_t bitstream_buf_size;
    int bitstream_buf_count;
    int frame_buf_count;
    struct {
        int rot_degree;
        int mir_direction;
        bool frame_crop_enable;
    } mjpeg_dec_config;
} mc_video_codec_dec_params_t;

typedef struct {
    int width;
    int height;
    int pix_fmt;
    uint32_t bitstream_buf_size;
    int frame_buf_count;
    bool external_frame_buf;
    int bitstream_buf_count;
    struct {
        int gop_preset_idx;
        int decoding_refresh_type;
    } gop_params;
    int rot_degree;
    int mir_direction;
    bool frame_cropping_flag;
    int enable_user_pts;
    mc_rate_control_params_t rc_params;
    struct {
        int main_still_picture_profile_enable;
        int h265_level;
    } h265_enc_config;
    struct {
        int h264_profile;
        int h264_level;
    } h264_enc_config;
} mc_video_codec_enc_params_t;

typedef struct {
    int codec_id;
    bool encoder;
    mc_video_codec_dec_params_t video_dec_params;
    mc_video_codec_enc_params_t video_enc_params;
    void *instance_handle; /* internal mock handle */
} media_codec_context_t;

typedef struct {
    int reserved;
} mc_av_codec_startup_params_t;

typedef struct {
    int type;
    struct {
        uint8_t *vir_ptr;
        uint32_t size;
        int stream_end;
    } vstream_buf;
    struct {
        uint8_t *vir_ptr[3];
        int width;
        int height;
        int stride;
        int pix_fmt;
        uint32_t size;
    } vframe_buf;
} media_codec_buffer_t;

typedef struct {
    struct {
        int decode_result;
    } video_frame_info;
} media_codec_output_buffer_info_t;

hb_s32 hb_mm_mc_initialize(media_codec_context_t *context);
hb_s32 hb_mm_mc_configure(media_codec_context_t *context);
hb_s32 hb_mm_mc_start(media_codec_context_t *context, const mc_av_codec_startup_params_t *params);
hb_s32 hb_mm_mc_pause(media_codec_context_t *context);
hb_s32 hb_mm_mc_stop(media_codec_context_t *context);
hb_s32 hb_mm_mc_release(media_codec_context_t *context);
hb_s32 hb_mm_mc_get_rate_control_config(
    media_codec_context_t *context, mc_rate_control_params_t *params
);
hb_s32 hb_mm_mc_dequeue_input_buffer(
    media_codec_context_t *context, media_codec_buffer_t *buffer, int timeout_ms
);
hb_s32 hb_mm_mc_queue_input_buffer(
    media_codec_context_t *context, media_codec_buffer_t *buffer, int timeout_ms
);
hb_s32 hb_mm_mc_dequeue_output_buffer(
    media_codec_context_t *context,
    media_codec_buffer_t *buffer,
    media_codec_output_buffer_info_t *info,
    int timeout_ms
);
hb_s32 hb_mm_mc_queue_output_buffer(
    media_codec_context_t *context, media_codec_buffer_t *buffer, int timeout_ms
);

int32_t hb_mem_invalidate_buf_with_vaddr(uint64_t virt_addr, uint64_t size);
int32_t hb_mem_flush_buf_with_vaddr(uint64_t virt_addr, uint64_t size);

#endif /* MIRO_EGO_STUB_HB_MEDIA_CODEC_H */

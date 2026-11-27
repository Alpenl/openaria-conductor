/* Configurable host mock for Hobot hb_mm_mc / hb_mm_mx APIs. */

#ifndef MIRO_EGO_MOCK_HOBOT_SDK_H
#define MIRO_EGO_MOCK_HOBOT_SDK_H

#include "hb_media_codec.h"

typedef struct {
    int sbs_width;
    int height;
    int intra_period;
    int force_missing_idr;
    int force_decoder_capacity;
    int force_bad_decode_result;
    int emit_stdout_noise;
    int fail_empty_mux_stop;
    int transient_jpu_input_timeouts;
    int transient_vpu_input_timeouts;
    int permanent_input_timeout;
} mock_hobot_config_t;

void mock_hobot_reset(const mock_hobot_config_t *cfg);
void mock_hobot_last_enc_params(int eye, mc_video_codec_enc_params_t *out);
unsigned long long mock_hobot_jpu_timeout_injections(void);
unsigned long long mock_hobot_vpu_timeout_injections(void);

#endif /* MIRO_EGO_MOCK_HOBOT_SDK_H */

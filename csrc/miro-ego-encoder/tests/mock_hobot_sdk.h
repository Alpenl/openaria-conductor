/* Configurable host mock for Hobot hb_mm_mc / hb_mm_mx APIs. */

#ifndef MIRO_EGO_MOCK_HOBOT_SDK_H
#define MIRO_EGO_MOCK_HOBOT_SDK_H

typedef struct {
    int sbs_width;
    int height;
    int intra_period;
    int force_missing_idr;
    int force_decoder_capacity;
    int force_bad_decode_result;
    int emit_stdout_noise;
    int fail_empty_mux_stop;
} mock_hobot_config_t;

void mock_hobot_reset(const mock_hobot_config_t *cfg);

#endif /* MIRO_EGO_MOCK_HOBOT_SDK_H */

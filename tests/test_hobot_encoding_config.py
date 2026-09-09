"""Exercise real configuration code against vendor headers, without opening hardware."""

import os
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path


class HobotEncodingConfigTest(unittest.TestCase):
    def test_all_codec_and_rate_control_combinations(self):
        include = os.environ.get("HOBOT_SDK_INCLUDE")
        compiler = shutil.which("cc")
        if not include or not compiler:
            self.skipTest("set HOBOT_SDK_INCLUDE to the X5 libmm headers")
        root = Path(__file__).resolve().parents[1]
        source = (root / "src/rp_ylx/hobot/ylx_stereo_pipeline.c").read_text()
        config = source[
            source.index("static int configure_encoder(") : source.index(
                "/* --------------------------------------------------------------- moov"
            )
        ]
        body = r"""
#include <assert.h>
#include <stdbool.h>
#include <stdint.h>
#include <string.h>
#include "hb_media_codec.h"
#include "ylx_stereo_pipeline.h"
static int result;
hb_s32 hb_mm_mc_get_rate_control_config(
    media_codec_context_t *context, mc_rate_control_params_t *params) {
    (void)context;
    memset(params, 0, sizeof(*params));
    return result;
}
"""
        body += config
        body += r"""
#define BUDGET(member) do { \
    assert(p->member.bit_rate == 16384); \
    assert(p->member.frame_rate == 30 && p->member.intra_period == 30); \
    assert(p->member.min_qp_P == 18 && p->member.max_qp_P == 32); \
    assert(p->member.intra_qp == 20 && p->member.initial_rc_qp == 22); \
} while (0)
int main(void) {
    ylx_pipeline_config_t config = {.height=1080, .fps=30, .bitrate_kbps=16384,
        .min_qp=18, .max_qp=32, .intra_qp=20, .initial_qp=22, .intra_period=30, .vbv_ms=3000};
    media_codec_context_t context;
    for (int codec=0; codec<2; codec++) for (int mode=0; mode<4; mode++) {
        config.hevc=codec; config.rate_control=mode;
        assert(configure_encoder(&context, &config, 1920)==0);
        assert(context.codec_id == (codec ? MEDIA_CODEC_ID_H265 : MEDIA_CODEC_ID_H264));
        assert(context.video_enc_params.width == 1920 && context.video_enc_params.height == 1080);
        assert(context.video_enc_params.gop_params.gop_preset_idx == 9);
        mc_rate_control_params_t *p=&context.video_enc_params.rc_params;
        if (!codec) switch (mode) {
            case 0: assert(p->mode==MC_AV_RC_MODE_H264CBR); BUDGET(h264_cbr_params); break;
            case 1: assert(p->mode==MC_AV_RC_MODE_H264VBR);
                assert(p->h264_vbr_params.intra_qp==20); break;
            case 2: assert(p->mode==MC_AV_RC_MODE_H264FIXQP);
                assert(p->h264_fixqp_params.force_qp_P==22); break;
            case 3: assert(p->mode==MC_AV_RC_MODE_H264AVBR); BUDGET(h264_avbr_params); break;
        } else switch (mode) {
            case 0: assert(p->mode==MC_AV_RC_MODE_H265CBR); BUDGET(h265_cbr_params); break;
            case 1: assert(p->mode==MC_AV_RC_MODE_H265VBR);
                assert(p->h265_vbr_params.intra_qp==20); break;
            case 2: assert(p->mode==MC_AV_RC_MODE_H265FIXQP);
                assert(p->h265_fixqp_params.force_qp_P==22); break;
            case 3: assert(p->mode==MC_AV_RC_MODE_H265AVBR); BUDGET(h265_avbr_params); break;
        }
    }
    result=-37;
    assert(configure_encoder(&context, &config, 1920)==-37);
    return 0;
}
"""
        with tempfile.TemporaryDirectory() as temporary:
            harness = Path(temporary) / "config.c"
            harness.write_text(body)
            binary = Path(temporary) / "config"
            subprocess.run(
                [
                    compiler,
                    "-std=gnu11",
                    "-Wall",
                    "-Wextra",
                    "-Werror",
                    "-I" + include,
                    "-I" + str(root / "src/rp_ylx/hobot"),
                    str(harness),
                    "-o",
                    str(binary),
                ],
                check=True,
            )
            subprocess.run([str(binary)], check=True)

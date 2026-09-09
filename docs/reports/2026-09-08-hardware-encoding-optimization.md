# RDK X5 hardware encoding experiment

The recording helper previously selected GOP preset 1 (all I slices), despite
the comment promising single-reference I/P. The installed vendor header defines
preset 9 as single-reference consecutive P frames. The new default uses preset 9,
keeps B frames disabled, and keeps the 30-frame IDR interval and 8192 kb/s per-eye
target. QP defaults become minimum 20, intra 22, initial 24.

## Same-input experiment

240 identical 3840×1080 JPEG frames at 30 fps were replayed through the actual
JPU/split/dual-VPU/muxer pipeline. Each eye produced independently decoded
90/90/60-frame segments. All configurations completed with zero application
drops. JPEG inputs were generated from an earlier recording; this measures
encoding loss, not camera optical quality or a simultaneous raw sensor capture.

| GOP / minimum QP | Total bytes | Left SSIM | Right SSIM |
|---|---:|---:|---:|
| All I / 28 | 18,509,887 | 0.987118 | 0.987176 |
| I/P / 28 | 10,015,620 | 0.981373 | 0.981145 |
| I/P / 24 | 13,946,969 | 0.984690 | 0.984422 |
| I/P / 22 | 15,410,918 | 0.986040 | 0.985034 |
| I/P / 20 (selected) | 16,072,833 | 0.986599 | 0.985473 |
| I/P / 18 | 16,532,854 | 0.986907 | 0.985780 |

The selected point saves 13.2% on this clip with a small SSIM reduction. Savings
and distortion depend on content; these numbers are not an equal-quality proof
or a guarantee for every scene. QP 28 was rejected as the default because its
larger size reduction also loses more detail.

## Container correctness

The vendor encoder's output metadata reports only I/P/B, not IDR. The helper now
inspects Annex B NAL headers and writes every actual IDR to MP4 `stss`. Final
hardware verification found entries 1/31/61 in 90-frame segments and 1/31 in
60-frame segments. Segment lengths must be multiples of the configured IDR
period, and a segment starting without IDR fails explicitly.

The vendor muxer writes zero-valued primaries/transfer/matrix, which players
interpret as reserved/reserved/GBR even though the pixels are NV12 YUV. Its API
does not expose color fields. During the existing MP4 duration finalization,
bounded box traversal replaces these invalid zero fields with unspecified (2)
and retains the known full range from MJPEG. It does not assert BT.709 without
evidence. Encoded media bytes remain unchanged. Host C regression covers actual
metadata helper code, valid declarations, damaged boxes, and IDR detection.

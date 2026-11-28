/*
 * Pure-C ISO-BMFF 4-track fragmented MP4 (fMP4) writer + Annex-B helpers.
 *
 * Replaces vendor hb_media_muxer + miro_ego_mp4_meta.c with a single deterministic
 * hardware-sample-timescale container:
 *   Track 1: Left  video (H.264 avc1 or HEVC hvc1)
 *   Track 2: Right video (H.264 avc1 or HEVC hvc1)
 *   Track 3 (if has_audio): Lossless variable-blocksize FLAC (fLaC + dfLa)
 *   Track 3/4: Timed Metadata (mett application/x-miro-ego-sync+json)
 */

#ifndef MIRO_EGO_FMP4_H
#define MIRO_EGO_FMP4_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define MIRO_EGO_FMP4_MAX_TRACKS 4
#define MIRO_EGO_FMP4_META_MIME "application/x-miro-ego-sync+json"

/* Annex-B IDR detectors (kept exported for unit tests & output thread). */
int miro_ego_h264_has_idr(const unsigned char *data, size_t length);
int miro_ego_h265_has_idr(const unsigned char *data, size_t length);

typedef struct {
    uint8_t *data;
    size_t len;
    size_t cap;
} miro_ego_bbuf_t;

typedef struct {
    uint8_t *sps;
    size_t sps_len;
    uint8_t *pps;
    size_t pps_len;
    uint8_t *vps;
    size_t vps_len;
} miro_ego_video_ps_t;

typedef struct {
    uint32_t duration;
    uint32_t size;
    uint32_t flags;
} miro_ego_fmp4_sample_t;

typedef struct {
    uint32_t track_id;
    miro_ego_fmp4_sample_t *samples;
    size_t n_samples;
    size_t cap_samples;
    miro_ego_bbuf_t payload;
    uint64_t base_dts;
    uint64_t tkhd_dur_file_off;
    uint64_t *mfra_time;
    uint64_t *mfra_moof_off;
    size_t n_mfra;
    size_t cap_mfra;
} miro_ego_fmp4_track_t;

typedef struct {
    int fd;
    int hevc;
    int has_audio;
    int width;
    int height;
    uint32_t timescale;
    int num_tracks;
    uint64_t file_pos;
    uint32_t moof_seq;
    uint64_t mvhd_dur_file_off;
    uint64_t mehd_dur_file_off;
    miro_ego_fmp4_track_t tracks[MIRO_EGO_FMP4_MAX_TRACKS];
} miro_ego_fmp4_t;

/*
 * Parses an Annex-B bitstream packet, extracts any VPS/SPS/PPS parameter sets
 * into `*ps`, and converts remaining slice NALs into 4-byte length-prefixed AVCC/HVCC.
 * Returns 0 on success, -1 on error.
 */
int miro_ego_annexb_to_avcc(int hevc, const uint8_t *in, size_t in_len,
                            miro_ego_video_ps_t *ps, miro_ego_bbuf_t *avcc_out,
                            int *out_is_idr);

void miro_ego_video_ps_free(miro_ego_video_ps_t *ps);
void miro_ego_bbuf_free(miro_ego_bbuf_t *buf);

/* Initializes fMP4 writer state on an already-opened fd (`file_pos == 0`). */
int miro_ego_fmp4_init(miro_ego_fmp4_t *mx, int fd, int hevc, int has_audio,
                       int eye_width, int height, uint32_t timescale);

/* Writes ftyp + moov using extracted left/right parameter sets. */
int miro_ego_fmp4_write_header(miro_ego_fmp4_t *mx, const miro_ego_video_ps_t ps[2]);

/* Appends one sample payload + duration + flags to track index `track_idx`. */
int miro_ego_fmp4_push_sample(miro_ego_fmp4_t *mx, int track_idx, uint32_t duration,
                              const uint8_t *data, uint32_t size, uint32_t flags);

/*
 * Serializes accumulated samples into a standalone `[moof][mdat]` byte buffer
 * ready for async disk writing, updating `file_pos`, `base_dts`, and `mfra`
 * table offsets deterministically. Returns 0 on success (`out_frag->len == 0`
 * if nothing queued), -1 on error.
 */
int miro_ego_fmp4_build_fragment(miro_ego_fmp4_t *mx, miro_ego_bbuf_t *out_frag,
                                 uint64_t *out_frag_off, uint64_t *out_frag_dur_ticks);

/*
 * Serializes `mfra` (`tfra` + `mfro`) into `*out_mfra` and computes `dur_ticks`
 * + offsets to patch `mvhd`, `mehd`, and every `tkhd`.
 */
int miro_ego_fmp4_build_mfra(const miro_ego_fmp4_t *mx, miro_ego_bbuf_t *out_mfra);
int miro_ego_fmp4_patch_moov_durations(const miro_ego_fmp4_t *mx, int fd, uint64_t duration_ticks);

void miro_ego_fmp4_destroy(miro_ego_fmp4_t *mx);

#ifdef __cplusplus
}
#endif

#endif /* MIRO_EGO_FMP4_H */

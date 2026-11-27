/*
 * Hardware-independent Annex B IDR detection and MP4 moov metadata fixups.
 *
 * Preserves all battle-tested container safety patches:
 *   - True Annex B IDR NAL scanning for H.264 and H.265 (for accurate stss indexing)
 *   - 64-bit box size (largesize) and both version-0 (32-bit) & version-1 (64-bit)
 *     duration extension across mvhd, tkhd, mdhd, and elst
 *   - Colorimetry unspecified(2) + nclx full-range(0x80) patch for avc1/hvc1/hev1
 */

#ifndef MIRO_EGO_MP4_META_H
#define MIRO_EGO_MP4_META_H

#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Returns 1 if Annex B H.264 stream contains an IDR slice NAL unit (type 5), else 0. */
int miro_ego_h264_has_idr(const unsigned char *data, size_t length);

/* Returns 1 if Annex B H.265 stream contains an IDR NAL unit (type 19 or 20), else 0. */
int miro_ego_h265_has_idr(const unsigned char *data, size_t length);

/*
 * Patches colr box inside an in-memory moov buffer:
 * replaces reserved (0) primaries/transfer/matrix with unspecified (2), and
 * sets nclx full-range flag to 0x80. Missing colr box is allowed (returns 0).
 */
int miro_ego_mp4_fix_color(unsigned char *moov, size_t moov_size,
                           char *reason, size_t reason_len);

/*
 * Extends mvhd, tkhd, mdhd, and elst durations in-place by one frame interval
 * on an already-open read/write file descriptor (without closing fd) and
 * applies the colorimetry fixup.
 * Returns 0 on success, -1 on failure with a diagnostic message in reason.
 */
int miro_ego_mp4_extend_durations_fd(int fd, int fps,
                                     char *reason, size_t reason_len);

/*
 * Path wrapper around miro_ego_mp4_extend_durations_fd opening path with
 * O_RDWR | O_NOFOLLOW | O_CLOEXEC.
 */
int miro_ego_mp4_extend_durations(const char *path, int fps,
                                  char *reason, size_t reason_len);

#ifdef __cplusplus
}
#endif

#endif /* MIRO_EGO_MP4_META_H */

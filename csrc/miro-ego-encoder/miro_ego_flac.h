/*
 * Pure-C RFC 9639 variable-blocksize lossless FLAC frame encoder.
 *
 * Encodes arbitrary blocksize (1..=65535) 48 kHz 2-channel 16-bit S16_LE frames
 * using Fixed LPC order-1 + optimal Rice k=0..14 with automatic Verbatim fallback,
 * CRC-8 header checksum, and CRC-16 frame footer checksum.
 */

#ifndef MIRO_EGO_FLAC_H
#define MIRO_EGO_FLAC_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

uint8_t miro_ego_flac_crc8(const uint8_t *data, size_t len);
uint16_t miro_ego_flac_crc16(const uint8_t *data, size_t len);

/*
 * Encodes `blocksize` interleaved stereo S16_LE PCM frames into a single
 * RFC 9639 variable-blocksize FLAC frame inside `*out_buf` (realloc-grown).
 * `blocksize` must be in [1, 65535]. Returns 0 on success, -1 on error.
 */
int miro_ego_flac_encode_frame(uint8_t **out_buf, size_t *out_cap, size_t *out_len,
                               const int16_t *interleaved_stereo, uint32_t blocksize,
                               uint64_t sample_number, int16_t *scratch_l,
                               int16_t *scratch_r);

#ifdef __cplusplus
}
#endif

#endif /* MIRO_EGO_FLAC_H */

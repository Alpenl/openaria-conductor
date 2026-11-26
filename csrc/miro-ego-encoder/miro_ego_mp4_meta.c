#define _GNU_SOURCE
#include "miro_ego_mp4_meta.h"

#include <errno.h>
#include <fcntl.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

/* Encoder metadata reports only I/P/B, not IDR. Inspect Annex B NAL headers
 * so stss indexes every true random-access picture, including within a file. */
int miro_ego_h264_has_idr(const unsigned char *data, size_t length)
{
    if (data == NULL) {
        return 0;
    }
    for (size_t offset = 0; offset + 3 < length; offset += 1) {
        if (data[offset] == 0 && data[offset + 1] == 0 && data[offset + 2] == 1 &&
            (data[offset + 3] & 0x1fu) == 5u) {
            return 1;
        }
    }
    return 0;
}

int miro_ego_h265_has_idr(const unsigned char *data, size_t length)
{
    if (data == NULL) {
        return 0;
    }
    for (size_t offset = 0; offset + 4 < length; offset += 1) {
        if (data[offset] == 0 && data[offset + 1] == 0 && data[offset + 2] == 1) {
            const unsigned type = (data[offset + 3] >> 1) & 0x3fu;
            if ((type == 19u || type == 20u) && (data[offset + 4] & 7u) != 0u) {
                return 1;
            }
        }
    }
    return 0;
}

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

static void write_u64(unsigned char *cursor, uint64_t value)
{
    write_u32(cursor, (uint32_t)(value >> 32));
    write_u32(cursor + 4, (uint32_t)(value & 0xffffffffu));
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

typedef struct {
    const char *type;
    const char *path[4];
    size_t depth;
    size_t v0_timescale_offset; /* 0 when box carries no timescale */
    size_t v0_duration_offset;
    size_t v1_timescale_offset; /* 0 when box carries no timescale */
    size_t v1_duration_offset;
} duration_box_t;

static const duration_box_t DURATION_BOXES[] = {
    {"mvhd", {"mvhd", NULL, NULL, NULL}, 1, 20, 24, 28, 32},
    {"tkhd", {"trak", "tkhd", NULL, NULL}, 2, 0, 28, 0, 36},
    {"mdhd", {"trak", "mdia", "mdhd", NULL}, 3, 20, 24, 28, 32},
    {"elst", {"trak", "edts", "elst", NULL}, 3, 0, 16, 0, 16},
};

static int parse_box_header(const unsigned char *buf, size_t avail,
                            uint64_t *box_size, size_t *header_len)
{
    if (avail < 8u) {
        return -1;
    }
    const uint32_t size32 = read_u32(buf);
    if (size32 == 1u) {
        if (avail < 16u) {
            return -1;
        }
        const uint64_t large = read_u64(buf + 8);
        if (large < 16ULL || large > (uint64_t)avail) {
            return -1;
        }
        *box_size = large;
        *header_len = 16u;
        return 0;
    }
    if (size32 == 0u) {
        *box_size = (uint64_t)avail;
        *header_len = 8u;
        return 0;
    }
    if (size32 < 8u || (uint64_t)size32 > (uint64_t)avail) {
        return -1;
    }
    *box_size = (uint64_t)size32;
    *header_len = 8u;
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
    const size_t header_delta = parent_hdr - 8u;
    if (skip > (size_t)length || header_delta > (size_t)length - skip) {
        return NULL;
    }
    for (size_t offset = skip + header_delta; offset + 8u <= (size_t)length;) {
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
        current = child_box(current, current_avail, 8u, path[index], &current_avail);
        if (current == NULL) {
            return NULL;
        }
    }
    if (out_size != NULL) {
        *out_size = current_avail;
    }
    return current;
}

int miro_ego_mp4_fix_color(unsigned char *moov, size_t moov_size,
                           char *reason, size_t reason_len)
{
    static const struct {
        const char *type;
        size_t skip;
    } path[] = {
        {"trak", 8}, {"mdia", 8}, {"minf", 8}, {"stbl", 8},
        {"stsd", 8}, {"avc1", 16}, {"colr", 86},
    };
    unsigned char *box = moov;
    size_t box_avail = moov_size;
    for (size_t index = 0; index < sizeof(path) / sizeof(path[0]); index += 1) {
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
            if (index == sizeof(path) / sizeof(path[0]) - 1u) {
                return 0;
            }
            if (reason != NULL && reason_len > 0) {
                snprintf(reason, reason_len, "missing color path box %s", path[index].type);
            }
            return -1;
        }
    }

    uint64_t colr_size = 0;
    size_t colr_hdr = 0;
    if (parse_box_header(box, box_avail, &colr_size, &colr_hdr) != 0 || colr_hdr != 8u) {
        if (reason != NULL && reason_len > 0) {
            snprintf(reason, reason_len, "unsupported colr header");
        }
        return -1;
    }
    const size_t size = (size_t)colr_size;
    if (size < 18u || (memcmp(box + 8, "nclx", 4) != 0 &&
                       memcmp(box + 8, "nclc", 4) != 0)) {
        if (reason != NULL && reason_len > 0) {
            snprintf(reason, reason_len, "unsupported colr box");
        }
        return -1;
    }
    /* Vendor muxer writes reserved primaries/transfer and GBR for NV12.
     * The camera does not declare these; unspecified (2) is truthful. */
    for (size_t field = 12u; field < 18u; field += 2u) {
        if (box[field] == 0 && box[field + 1u] == 0) {
            box[field + 1u] = 2u;
        }
    }
    if (memcmp(box + 8, "nclx", 4) == 0) {
        if (size < 19u) {
            if (reason != NULL && reason_len > 0) {
                snprintf(reason, reason_len, "short nclx box");
            }
            return -1;
        }
        box[18] = 0x80u; /* MJPEG -> NV12 row copy preserves full range. */
    }
    return 0;
}

/*
 * The vendor MP4 muxer derives every duration from the presentation time of the
 * last sample, so a segment's declared duration covers one frame less than it
 * actually holds. Players that honour the edit list then drop the final frame.
 * The samples themselves are complete, so extend the four duration fields by
 * exactly one frame interval.
 */
int miro_ego_mp4_extend_durations(const char *path, int fps,
                                  char *reason, size_t reason_len)
{
    if (path == NULL || fps <= 0) {
        if (reason != NULL && reason_len > 0) {
            snprintf(reason, reason_len, "invalid fps");
        }
        return -1;
    }
    int fd = open(path, O_RDWR);
    if (fd < 0) {
        if (reason != NULL && reason_len > 0) {
            snprintf(reason, reason_len, "open: %s", strerror(errno));
        }
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
        if (size32 == 1u) {
            unsigned char ext[8];
            if (pread_full(fd, ext, sizeof(ext), offset + 8) != 0) {
                break;
            }
            box_size = read_u64(ext);
            if (box_size < 16ULL) {
                break;
            }
        } else if (size32 == 0u) {
            off_t file_end = lseek(fd, 0, SEEK_END);
            if (file_end < 0 || file_end <= offset ||
                (uint64_t)(file_end - offset) < 8ULL) {
                break;
            }
            box_size = (uint64_t)(file_end - offset);
        } else if (size32 < 8u) {
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
    if (moov_offset < 0 || moov_size < 8u || moov_size > 8u * 1024u * 1024u) {
        if (reason != NULL && reason_len > 0) {
            snprintf(reason, reason_len, "no usable moov box");
        }
        close(fd);
        return -1;
    }

    unsigned char *moov = malloc(moov_size);
    if (moov == NULL) {
        if (reason != NULL && reason_len > 0) {
            snprintf(reason, reason_len, "out of memory");
        }
        close(fd);
        return -1;
    }
    if (pread_full(fd, moov, moov_size, moov_offset) != 0) {
        if (reason != NULL && reason_len > 0) {
            snprintf(reason, reason_len, "short moov read");
        }
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
            found_hdr != 8u || parsed_size < 16u) {
            if (reason != NULL && reason_len > 0) {
                snprintf(reason, reason_len, "%s box missing", box->type);
            }
            result = -1;
            break;
        }
        const unsigned version = found[8];
        if (version != 0u && version != 1u) {
            if (reason != NULL && reason_len > 0) {
                snprintf(reason, reason_len, "%s has unsupported version %u", box->type, version);
            }
            result = -1;
            break;
        }

        const size_t dur_width = version == 1u ? 8u : 4u;
        const size_t ts_offset = version == 1u ? box->v1_timescale_offset : box->v0_timescale_offset;
        const size_t dur_offset = version == 1u ? box->v1_duration_offset : box->v0_duration_offset;

        if (parsed_size < (uint64_t)dur_offset + (uint64_t)dur_width) {
            if (reason != NULL && reason_len > 0) {
                snprintf(reason, reason_len, "%s box truncated", box->type);
            }
            result = -1;
            break;
        }

        unsigned char *field = found + dur_offset;
        if (strcmp(box->type, "elst") == 0) {
            const uint32_t entry_count = read_u32(found + 12);
            const uint64_t entry_stride = version == 1u ? 20ULL : 12ULL;
            if (entry_count == 0u) {
                if (reason != NULL && reason_len > 0) {
                    snprintf(reason, reason_len, "elst has no entries");
                }
                result = -1;
                break;
            }
            if ((uint64_t)entry_count > (parsed_size - 16ULL) / entry_stride) {
                if (reason != NULL && reason_len > 0) {
                    snprintf(reason, reason_len, "elst entries exceed box size");
                }
                result = -1;
                break;
            }
            field = NULL;
            for (uint32_t entry = 0; entry < entry_count; entry += 1) {
                unsigned char *entry_ptr = found + 16u + (size_t)((uint64_t)entry * entry_stride);
                if (version == 1u) {
                    if (read_u64(entry_ptr + 8) != UINT64_MAX) {
                        field = entry_ptr;
                        break;
                    }
                } else {
                    if (read_u32(entry_ptr + 4) != 0xffffffffu) {
                        field = entry_ptr;
                        break;
                    }
                }
            }
            if (field == NULL) {
                if (reason != NULL && reason_len > 0) {
                    snprintf(reason, reason_len, "elst has no media entry");
                }
                result = -1;
                break;
            }
        }

        uint32_t timescale = movie_timescale;
        if (ts_offset != 0u) {
            timescale = read_u32(found + ts_offset);
            if (index == 0u) {
                movie_timescale = timescale;
            }
        }
        if (timescale == 0u) {
            if (reason != NULL && reason_len > 0) {
                snprintf(reason, reason_len, "%s has no usable timescale", box->type);
            }
            result = -1;
            break;
        }

        const uint64_t frame_ticks64 =
            ((uint64_t)timescale + (uint64_t)(uint32_t)fps - 1ULL) / (uint64_t)(uint32_t)fps;
        if (version == 1u) {
            const uint64_t current_duration = read_u64(field);
            if (current_duration > UINT64_MAX - frame_ticks64) {
                if (reason != NULL && reason_len > 0) {
                    snprintf(reason, reason_len, "%s duration overflow", box->type);
                }
                result = -1;
                break;
            }
            write_u64(field, current_duration + frame_ticks64);
        } else {
            if (frame_ticks64 > UINT32_MAX) {
                if (reason != NULL && reason_len > 0) {
                    snprintf(reason, reason_len, "%s frame ticks overflow", box->type);
                }
                result = -1;
                break;
            }
            const uint32_t frame_ticks = (uint32_t)frame_ticks64;
            const uint32_t current_duration = read_u32(field);
            if (current_duration > UINT32_MAX - frame_ticks) {
                if (reason != NULL && reason_len > 0) {
                    snprintf(reason, reason_len, "%s duration overflow", box->type);
                }
                result = -1;
                break;
            }
            write_u32(field, current_duration + frame_ticks);
        }
    }

    if (result == 0) {
        result = miro_ego_mp4_fix_color(moov, moov_size, reason, reason_len);
    }
    if (result == 0 && pwrite_full(fd, moov, moov_size, moov_offset) != 0) {
        if (reason != NULL && reason_len > 0) {
            snprintf(reason, reason_len, "moov write failed: %s", strerror(errno));
        }
        result = -1;
    }

    free(moov);
    close(fd);
    return result;
}

/* Shared CLI parsing and JSON string formatting helpers for Miro-Ego encoder tools. */

#ifndef MIRO_EGO_CLI_COMMON_H
#define MIRO_EGO_CLI_COMMON_H

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static inline void miro_ego_write_json_string(FILE *stream, const char *value)
{
    const unsigned char *cursor = (const unsigned char *)(value == NULL ? "" : value);
    fputc('"', stream);
    while (*cursor != '\0') {
        switch (*cursor) {
        case '"':
            fputs("\\\"", stream);
            break;
        case '\\':
            fputs("\\\\", stream);
            break;
        case '\b':
            fputs("\\b", stream);
            break;
        case '\f':
            fputs("\\f", stream);
            break;
        case '\n':
            fputs("\\n", stream);
            break;
        case '\r':
            fputs("\\r", stream);
            break;
        case '\t':
            fputs("\\t", stream);
            break;
        default:
            if (*cursor < 0x20u) {
                fprintf(stream, "\\u%04x", (unsigned)*cursor);
            } else {
                fputc(*cursor, stream);
            }
            break;
        }
        cursor += 1;
    }
    fputc('"', stream);
}

static inline int miro_ego_parse_int_checked(const char *value, int min_val,
                                             int max_val, int *out)
{
    if (value == NULL || *value == '\0' || out == NULL) {
        return -1;
    }
    char *end = NULL;
    errno = 0;
    long parsed = strtol(value, &end, 10);
    if (errno != 0 || end == value || *end != '\0' ||
        parsed < (long)min_val || parsed > (long)max_val) {
        return -1;
    }
    *out = (int)parsed;
    return 0;
}

static inline int miro_ego_parse_integer(const char *value)
{
    int parsed = 0;
    if (miro_ego_parse_int_checked(value, 0, 1000000, &parsed) != 0) {
        fprintf(stderr, "invalid integer: %s\n", value == NULL ? "(null)" : value);
        exit(2);
    }
    return parsed;
}

static inline int miro_ego_parse_codec(const char *codec, int *hevc)
{
    if (codec == NULL || hevc == NULL) {
        return -1;
    }
    if (strcmp(codec, "h264") == 0) {
        *hevc = 0;
        return 0;
    }
    if (strcmp(codec, "hevc") == 0) {
        *hevc = 1;
        return 0;
    }
    return -1;
}

static inline int miro_ego_parse_rc_mode(const char *mode, int *rate_control)
{
    if (mode == NULL || rate_control == NULL) {
        return -1;
    }
    if (strcmp(mode, "cbr") == 0) {
        *rate_control = 0;
        return 0;
    }
    if (strcmp(mode, "vbr") == 0) {
        *rate_control = 1;
        return 0;
    }
    if (strcmp(mode, "fixqp") == 0) {
        *rate_control = 2;
        return 0;
    }
    if (strcmp(mode, "avbr") == 0) {
        *rate_control = 3;
        return 0;
    }
    return -1;
}

#endif /* MIRO_EGO_CLI_COMMON_H */

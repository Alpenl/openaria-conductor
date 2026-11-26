/* Shared CLI parsing and JSON string formatting helpers for Hobot tools. */
#ifndef YLX_CLI_COMMON_H
#define YLX_CLI_COMMON_H

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static inline void ylx_write_json_string(FILE *stream, const char *value)
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
            if (*cursor < 0x20) {
                fprintf(stream, "\\u%04x", *cursor);
            } else {
                fputc(*cursor, stream);
            }
            break;
        }
        cursor += 1;
    }
    fputc('"', stream);
}

static inline int ylx_parse_integer(const char *value)
{
    char *end = NULL;
    errno = 0;
    long parsed = strtol(value, &end, 10);
    if (errno != 0 || end == value || *end != '\0' || parsed < 0 || parsed > 1000000) {
        fprintf(stderr, "invalid integer: %s\n", value);
        exit(2);
    }
    return (int)parsed;
}

static inline int ylx_parse_codec(const char *codec, int *hevc)
{
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

static inline int ylx_parse_rc_mode(const char *mode, int *rate_control)
{
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

#endif /* YLX_CLI_COMMON_H */

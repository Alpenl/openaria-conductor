/*
 * ylx-stereo-encoder: the recording daemon's live dual-eye H.264 helper.
 *
 * The daemon keeps owning capture, frame indexing, IMU and drop accounting; it
 * streams one side-by-side MJPEG frame per encoded frame into this process and
 * reads back one NDJSON line per closed segment. Keeping the codec in its own
 * process means a wedged JPU or VPU fails the recording instead of taking the
 * daemon down with it.
 *
 * stdin frame framing: "YLXF" | u32 little-endian payload length | payload.
 * A zero-length payload means end of stream.
 *
 * stdout: one JSON object per line.
 *   {"event":"ready"}
 *   {"event":"segment","index":0,"start_frame":0,"end_frame":900,
 *    "left":{"path":"video/left_00000.mp4","bytes":123},"right":{...}}
 *   {"event":"done","frames":1234,...}
 *   {"event":"error","code":"...","message":"..."}
 */

#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <stdarg.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#include "ylx_stereo_pipeline.h"

#define FRAME_MAGIC 0x46584c59u /* "YLXF" little-endian */
#define MAX_FRAME_BYTES (32u * 1024u * 1024u)

static FILE *event_stream = NULL;

static FILE *event_output(void)
{
    return event_stream == NULL ? stdout : event_stream;
}

static int isolate_event_stream(void)
{
    /* Vendor multimedia libraries may write logs to stdout; keep NDJSON on a private pipe. */
    fflush(stdout);
    int event_fd = dup(STDOUT_FILENO);
    if (event_fd < 0) {
        return -1;
    }
    FILE *stream = fdopen(event_fd, "w");
    if (stream == NULL) {
        close(event_fd);
        return -1;
    }
    setvbuf(stream, NULL, _IOLBF, 0);
    event_stream = stream;

    if (dup2(STDERR_FILENO, STDOUT_FILENO) < 0) {
        int null_fd = open("/dev/null", O_WRONLY);
        if (null_fd < 0) {
            return -1;
        }
        int redirected = dup2(null_fd, STDOUT_FILENO);
        close(null_fd);
        if (redirected < 0) {
            return -1;
        }
    }
    return 0;
}

static void emit(const char *format, ...) __attribute__((format(printf, 1, 2)));

static void emit(const char *format, ...)
{
    FILE *stream = event_output();
    va_list arguments;
    va_start(arguments, format);
    vfprintf(stream, format, arguments);
    va_end(arguments);
    fputc('\n', stream);
    fflush(stream);
}

static void write_json_string(const char *value)
{
    FILE *stream = event_output();
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

static void emit_error_event(const char *code, const char *message)
{
    FILE *stream = event_output();
    fputs("{\"event\":\"error\",\"code\":", stream);
    write_json_string(code);
    fputs(",\"message\":", stream);
    write_json_string(message);
    fputs("}\n", stream);
    fflush(stream);
}

static void on_segment(void *user, int index, unsigned long long start_frame,
                       unsigned long long end_frame, const char *left_path,
                       unsigned long long left_bytes, const char *right_path,
                       unsigned long long right_bytes)
{
    (void)user;
    FILE *stream = event_output();
    fprintf(stream,
            "{\"event\":\"segment\",\"index\":%d,\"start_frame\":%llu,\"end_frame\":%llu,"
            "\"left\":{\"path\":",
            index, start_frame, end_frame);
    write_json_string(left_path);
    fprintf(stream, ",\"bytes\":%llu},\"right\":{\"path\":", left_bytes);
    write_json_string(right_path);
    fprintf(stream, ",\"bytes\":%llu}}\n", right_bytes);
    fflush(stream);
}

static bool read_exact(void *destination, size_t length)
{
    unsigned char *cursor = destination;
    while (length > 0) {
        ssize_t chunk = read(STDIN_FILENO, cursor, length);
        if (chunk == 0) {
            return false;
        }
        if (chunk < 0) {
            if (errno == EINTR) {
                continue;
            }
            return false;
        }
        cursor += chunk;
        length -= (size_t)chunk;
    }
    return true;
}

static void usage(const char *program)
{
    fprintf(stderr,
            "usage: %s --out-dir DIR [--width 3840] [--height 1080] [--fps 30]\n"
            "          [--bitrate-kbps 8192] [--segment-frames 900]\n"
            "          [--path-prefix video/] [--min-qp 28] [--intra-qp 30]\n"
            "          [--initial-qp 32] [--intra-period 0] [--vbv-ms 3000]\n",
            program);
}

int main(int argc, char **argv)
{
    ylx_pipeline_config_t config = {
        .sbs_width = 3840,
        .height = 1080,
        .fps = 30,
        .bitrate_kbps = 8192,
        .intra_period = 0,
        .min_qp = 28,
        .intra_qp = 30,
        .initial_qp = 32,
        .vbv_ms = 3000,
        .segment_frames = 900,
        .out_dir = NULL,
        .path_prefix = "video/",
    };

    for (int index = 1; index < argc; index += 1) {
        const char *name = argv[index];
        const bool has_value = index + 1 < argc;
        if (strcmp(name, "--out-dir") == 0 && has_value) {
            config.out_dir = argv[++index];
        } else if (strcmp(name, "--path-prefix") == 0 && has_value) {
            config.path_prefix = argv[++index];
        } else if (strcmp(name, "--width") == 0 && has_value) {
            config.sbs_width = atoi(argv[++index]);
        } else if (strcmp(name, "--height") == 0 && has_value) {
            config.height = atoi(argv[++index]);
        } else if (strcmp(name, "--fps") == 0 && has_value) {
            config.fps = atoi(argv[++index]);
        } else if (strcmp(name, "--bitrate-kbps") == 0 && has_value) {
            config.bitrate_kbps = atoi(argv[++index]);
        } else if (strcmp(name, "--segment-frames") == 0 && has_value) {
            config.segment_frames = atoi(argv[++index]);
        } else if (strcmp(name, "--intra-period") == 0 && has_value) {
            config.intra_period = atoi(argv[++index]);
        } else if (strcmp(name, "--min-qp") == 0 && has_value) {
            config.min_qp = atoi(argv[++index]);
        } else if (strcmp(name, "--intra-qp") == 0 && has_value) {
            config.intra_qp = atoi(argv[++index]);
        } else if (strcmp(name, "--initial-qp") == 0 && has_value) {
            config.initial_qp = atoi(argv[++index]);
        } else if (strcmp(name, "--vbv-ms") == 0 && has_value) {
            config.vbv_ms = atoi(argv[++index]);
        } else {
            usage(argv[0]);
            return 2;
        }
    }
    if (config.out_dir == NULL) {
        usage(argv[0]);
        return 2;
    }
    mkdir(config.out_dir, 0750);
    if (isolate_event_stream() != 0) {
        emit_error_event("event_stream_failed", "failed to isolate stdout event stream");
        return 5;
    }

    char error[256] = {0};
    ylx_pipeline_t *pipeline = NULL;
    if (ylx_pipeline_open(&config, on_segment, NULL, &pipeline, error, sizeof(error)) != 0) {
        emit_error_event("pipeline_open_failed", error);
        return 3;
    }
    emit("{\"event\":\"ready\"}");

    unsigned char *payload = malloc(MAX_FRAME_BYTES);
    if (payload == NULL) {
        emit_error_event("out_of_memory", "frame buffer");
        ylx_pipeline_close(pipeline);
        return 4;
    }

    const char *failure_code = NULL;
    for (;;) {
        uint32_t header[2];
        if (!read_exact(header, sizeof(header))) {
            break;
        }
        if (header[0] != FRAME_MAGIC) {
            failure_code = "frame_framing_lost";
            break;
        }
        const uint32_t length = header[1];
        if (length == 0) {
            break;
        }
        if (length > MAX_FRAME_BYTES) {
            failure_code = "frame_too_large";
            break;
        }
        if (!read_exact(payload, length)) {
            failure_code = "frame_truncated";
            break;
        }
        if (ylx_pipeline_submit(pipeline, payload, length, 0) != 0) {
            /* Fail closed: a frame that cannot enter the codec ends the take. */
            failure_code = "frame_rejected";
            break;
        }
    }

    const int finished = ylx_pipeline_finish(pipeline);
    ylx_pipeline_stats_t stats;
    ylx_pipeline_stats(pipeline, &stats);
    const char *pipeline_error = ylx_pipeline_error(pipeline);

    if (failure_code == NULL && finished != 0) {
        failure_code = "pipeline_failed";
    }
    if (failure_code != NULL) {
        emit_error_event(failure_code, pipeline_error[0] != '\0' ? pipeline_error : failure_code);
    }
    emit("{\"event\":\"done\",\"offered\":%llu,\"submitted\":%llu,\"decoded\":%llu,"
         "\"left_frames\":%llu,\"right_frames\":%llu,\"left_bytes\":%llu,"
         "\"right_bytes\":%llu,\"drop_decoder_input\":%llu,\"drop_encoder_input\":%llu,"
         "\"drop_decoder_output\":%llu}",
         stats.offered, stats.submitted, stats.decoded, stats.encoded[0],
         stats.encoded[1], stats.bytes[0], stats.bytes[1], stats.drop_decoder_input,
         stats.drop_encoder_input, stats.drop_decoder_output);

    free(payload);
    ylx_pipeline_close(pipeline);
    return failure_code == NULL ? 0 : 1;
}

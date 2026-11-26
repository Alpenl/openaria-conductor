/*
 * miro-ego-encoder: live dual-eye hardware H.264/H.265 encoder process.
 *
 * Keeps Hobot JPU/VPU hardware codecs isolated in a dedicated process so a
 * wedged codec fails the active recording instead of taking the Miro-Ego daemon
 * down with it.
 *
 * Primary IPC (zero-copy):
 *   --shm-fd <fd> --kick-fd <fd> --done-fd <fd>  (MEGO_SHM v1 memfd + eventfd ring)
 *
 * Debug/fallback stdin IPC:
 *   --stdin   framing: "MGOF" | u32 little-endian payload length | payload
 *             (zero-length payload or clean boundary EOF signals end of stream)
 *
 * stdout: one newline-delimited JSON object per line (isolated from vendor logs):
 *   {"event":"ready"}
 *   {"event":"segment","index":0,"start_frame":0,"end_frame":90,
 *    "left_path":"...","left_bytes":123,"left_sha256":"<64-hex>",
 *    "right_path":"...","right_bytes":456,"right_sha256":"<64-hex>"}
 *   {"event":"stats",...}
 *   {"event":"done",...}
 *   {"event":"error","code":"...","message":"..."}
 */

#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <stdarg.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#include "miro_ego_cli_common.h"
#include "miro_ego_shm_ring.h"
#include "miro_ego_stereo_pipeline.h"

#define STDIN_FRAME_MAGIC 0x464f474du /* "MGOF" little-endian */
#define MAX_FRAME_BYTES (32u * 1024u * 1024u)
#define SHM_POLL_INTERVAL_MS 100

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
    flockfile(stream);
    va_start(arguments, format);
    vfprintf(stream, format, arguments);
    va_end(arguments);
    fputc('\n', stream);
    fflush(stream);
    funlockfile(stream);
}

static void emit_error_event(const char *code, const char *message)
{
    FILE *stream = event_output();
    flockfile(stream);
    fputs("{\"event\":\"error\",\"code\":", stream);
    miro_ego_write_json_string(stream, code);
    fputs(",\"message\":", stream);
    miro_ego_write_json_string(stream, message);
    fputs("}\n", stream);
    fflush(stream);
    funlockfile(stream);
}

static void emit_stats(const char *event, const miro_ego_pipeline_stats_t *stats,
                       unsigned long long shm_dropped_frames)
{
    emit("{\"event\":\"%s\",\"offered\":%llu,\"submitted\":%llu,\"decoded\":%llu,"
         "\"left_frames\":%llu,\"right_frames\":%llu,\"left_bytes\":%llu,\"right_bytes\":%llu,"
         "\"drop_decoder_input\":%llu,\"drop_encoder_input\":%llu,\"drop_decoder_output\":%llu,"
         "\"left_write_queue_peak_bytes\":%llu,\"right_write_queue_peak_bytes\":%llu,"
         "\"left_write_queue_peak_frames\":%llu,\"right_write_queue_peak_frames\":%llu,"
         "\"left_write_queue_rejected\":%llu,\"right_write_queue_rejected\":%llu,"
         "\"left_max_write_ns\":%llu,\"right_max_write_ns\":%llu,\"max_seal_ns\":%llu,"
         "\"shm_dropped_frames\":%llu}",
         event, stats->offered, stats->submitted, stats->decoded,
         stats->encoded[0], stats->encoded[1], stats->bytes[0], stats->bytes[1],
         stats->drop_decoder_input, stats->drop_encoder_input, stats->drop_decoder_output,
         stats->write_queue_peak_bytes[0], stats->write_queue_peak_bytes[1],
         stats->write_queue_peak_frames[0], stats->write_queue_peak_frames[1],
         stats->write_queue_rejected[0], stats->write_queue_rejected[1],
         stats->max_write_ns[0], stats->max_write_ns[1], stats->max_seal_ns,
         shm_dropped_frames);
}

typedef struct {
    atomic_int stats_pending;
} encoder_callback_ctx_t;

static void on_segment(void *user, int index,
                       unsigned long long start_frame, unsigned long long end_frame,
                       const char *left_path, unsigned long long left_bytes,
                       const char *left_sha256,
                       const char *right_path, unsigned long long right_bytes,
                       const char *right_sha256)
{
    FILE *stream = event_output();
    flockfile(stream);
    fprintf(stream,
            "{\"event\":\"segment\",\"index\":%d,\"start_frame\":%llu,\"end_frame\":%llu,"
            "\"left_path\":",
            index, start_frame, end_frame);
    miro_ego_write_json_string(stream, left_path);
    fprintf(stream, ",\"left_bytes\":%llu,\"left_sha256\":", left_bytes);
    miro_ego_write_json_string(stream, left_sha256);
    fputs(",\"right_path\":", stream);
    miro_ego_write_json_string(stream, right_path);
    fprintf(stream, ",\"right_bytes\":%llu,\"right_sha256\":", right_bytes);
    miro_ego_write_json_string(stream, right_sha256);
    fputs("}\n", stream);
    fflush(stream);
    funlockfile(stream);
    if (user != NULL) {
        atomic_store(&((encoder_callback_ctx_t *)user)->stats_pending, 1);
    }
}

typedef enum {
    INPUT_COMPLETE,
    INPUT_EOF,
    INPUT_TRUNCATED,
    INPUT_ERROR
} input_result_t;

static input_result_t read_exact(void *destination, size_t length)
{
    unsigned char *cursor = (unsigned char *)destination;
    const size_t requested = length;
    while (length > 0) {
        ssize_t chunk = read(STDIN_FILENO, cursor, length);
        if (chunk == 0) {
            return length == requested ? INPUT_EOF : INPUT_TRUNCATED;
        }
        if (chunk < 0) {
            if (errno == EINTR) {
                continue;
            }
            return INPUT_ERROR;
        }
        cursor += (size_t)chunk;
        length -= (size_t)chunk;
    }
    return INPUT_COMPLETE;
}

static void usage(const char *program)
{
    fprintf(stderr,
            "usage: %s --out-dir DIR [--shm-fd FD --kick-fd FD --done-fd FD | --stdin]\n"
            "          [--width 3840] [--height 1080] [--fps 30]\n"
            "          [--bitrate-kbps 8192] [--segment-frames 900]\n"
            "          [--path-prefix video/] [--min-qp 20] [--intra-qp 22]\n"
            "          [--initial-qp 24] [--max-qp 51] [--intra-period 0] [--vbv-ms 3000]\n"
            "          [--codec h264|hevc] [--rc-mode cbr|vbr|fixqp|avbr]\n",
            program);
}

int main(int argc, char **argv)
{
    miro_ego_pipeline_config_t config = {
        .sbs_width = 3840,
        .height = 1080,
        .fps = 30,
        .bitrate_kbps = 8192,
        .hevc = 0,
        .rate_control = 0,
        .intra_period = 0,
        .min_qp = 20,
        .max_qp = 51,
        .intra_qp = 22,
        .initial_qp = 24,
        .vbv_ms = 3000,
        .segment_frames = 900,
        .out_dir = NULL,
        .path_prefix = "video/",
        .split_ops = NULL,
        .preview = {0},
    };

    int shm_fd = -1;
    int kick_fd = -1;
    int done_fd = -1;
    bool explicit_stdin = false;

    const struct {
        const char *flag;
        int *target;
    } int_options[] = {
        {"--width", &config.sbs_width},
        {"--height", &config.height},
        {"--fps", &config.fps},
        {"--bitrate-kbps", &config.bitrate_kbps},
        {"--segment-frames", &config.segment_frames},
        {"--intra-period", &config.intra_period},
        {"--min-qp", &config.min_qp},
        {"--max-qp", &config.max_qp},
        {"--intra-qp", &config.intra_qp},
        {"--initial-qp", &config.initial_qp},
        {"--vbv-ms", &config.vbv_ms},
        {"--shm-fd", &shm_fd},
        {"--kick-fd", &kick_fd},
        {"--done-fd", &done_fd},
    };

    for (int index = 1; index < argc; index += 1) {
        const char *name = argv[index];
        const bool has_value = index + 1 < argc;
        if (strcmp(name, "--stdin") == 0) {
            explicit_stdin = true;
        } else if (strcmp(name, "--codec") == 0 && has_value) {
            if (miro_ego_parse_codec(argv[++index], &config.hevc) != 0) {
                return 2;
            }
        } else if (strcmp(name, "--rc-mode") == 0 && has_value) {
            if (miro_ego_parse_rc_mode(argv[++index], &config.rate_control) != 0) {
                return 2;
            }
        } else if (strcmp(name, "--out-dir") == 0 && has_value) {
            config.out_dir = argv[++index];
        } else if (strcmp(name, "--path-prefix") == 0 && has_value) {
            config.path_prefix = argv[++index];
        } else if (has_value) {
            bool matched = false;
            for (size_t opt = 0; opt < sizeof(int_options) / sizeof(int_options[0]); opt += 1) {
                if (strcmp(name, int_options[opt].flag) == 0) {
                    *int_options[opt].target = miro_ego_parse_integer(argv[++index]);
                    matched = true;
                    break;
                }
            }
            if (!matched) {
                usage(argv[0]);
                return 2;
            }
        } else {
            usage(argv[0]);
            return 2;
        }
    }

    const int shm_arg_count = (shm_fd >= 0 ? 1 : 0) +
                              (kick_fd >= 0 ? 1 : 0) +
                              (done_fd >= 0 ? 1 : 0);
    if (config.out_dir == NULL ||
        (shm_arg_count != 0 && shm_arg_count != 3) ||
        (explicit_stdin && shm_arg_count != 0)) {
        usage(argv[0]);
        return 2;
    }
    const bool use_shm = (shm_arg_count == 3);

    mkdir(config.out_dir, 0750);
    if (isolate_event_stream() != 0) {
        emit_error_event("event_stream_failed", "failed to isolate stdout event stream");
        return 5;
    }

    char error[256] = {0};
    miro_ego_shm_ring_t shm_ring;
    memset(&shm_ring, 0, sizeof(shm_ring));
    if (use_shm) {
        if (miro_ego_shm_ring_attach(&shm_ring, shm_fd, kick_fd, done_fd,
                                     error, sizeof(error)) != 0) {
            emit_error_event("shm_attach_failed", error[0] != '\0' ? error : "shm_attach_failed");
            return 3;
        }
    }

    encoder_callback_ctx_t callback_ctx = {0};
    miro_ego_pipeline_t *pipeline = NULL;
    if (miro_ego_pipeline_open(&config, on_segment, &callback_ctx, &pipeline,
                               error, sizeof(error)) != 0) {
        if (use_shm) {
            miro_ego_shm_ring_detach(&shm_ring);
        }
        emit_error_event("pipeline_open_failed", error);
        return 3;
    }
    emit("{\"event\":\"ready\"}");

    const char *failure_code = NULL;
    int input_errno = 0;
    unsigned char *payload = NULL;

    if (use_shm) {
        for (;;) {
            if (miro_ego_pipeline_failed(pipeline)) {
                failure_code = "pipeline_failed";
                break;
            }
            miro_ego_shm_frame_t frame;
            memset(&frame, 0, sizeof(frame));
            miro_ego_shm_status_t status =
                miro_ego_shm_ring_receive(&shm_ring, &frame, SHM_POLL_INTERVAL_MS);
            if (status == MIRO_EGO_SHM_TIMEOUT) {
                continue;
            }
            if (status == MIRO_EGO_SHM_EOF) {
                break;
            }
            if (status != MIRO_EGO_SHM_OK) {
                input_errno = status == MIRO_EGO_SHM_IO_ERROR ? errno : 0;
                failure_code = status == MIRO_EGO_SHM_CORRUPT ? "shm_ring_corrupt"
                                                              : "shm_read_failed";
                break;
            }

            /* Zero-copy submit directly from mapped memfd slot into JPU DMA buffer. */
            const int submit_ret = miro_ego_pipeline_submit(
                pipeline, frame.data, (size_t)frame.payload_len, 0);
            const miro_ego_shm_status_t release_ret = miro_ego_shm_ring_release(&shm_ring);

            if (submit_ret != 0) {
                failure_code = "frame_rejected";
                break;
            }
            if (release_ret != MIRO_EGO_SHM_OK) {
                input_errno = errno;
                failure_code = "shm_release_failed";
                break;
            }
            if (atomic_exchange(&callback_ctx.stats_pending, 0) != 0) {
                miro_ego_pipeline_stats_t snapshot;
                miro_ego_pipeline_stats(pipeline, &snapshot);
                emit_stats("stats", &snapshot,
                           (unsigned long long)miro_ego_shm_ring_dropped(&shm_ring));
            }
        }
    } else {
        payload = malloc(MAX_FRAME_BYTES);
        if (payload == NULL) {
            emit_error_event("out_of_memory", "frame buffer");
            miro_ego_pipeline_close(pipeline);
            return 4;
        }
        for (;;) {
            uint32_t header[2];
            input_result_t input_result = read_exact(header, sizeof(header));
            if (input_result == INPUT_EOF) {
                break;
            }
            if (input_result != INPUT_COMPLETE) {
                input_errno = input_result == INPUT_ERROR ? errno : 0;
                failure_code = input_result == INPUT_ERROR ? "frame_read_failed"
                                                           : "frame_header_truncated";
                break;
            }
            if (header[0] != STDIN_FRAME_MAGIC) {
                failure_code = "frame_framing_lost";
                break;
            }
            const uint32_t length = header[1];
            if (length == 0u) {
                break;
            }
            if (length > MAX_FRAME_BYTES) {
                failure_code = "frame_too_large";
                break;
            }
            input_result = read_exact(payload, length);
            if (input_result != INPUT_COMPLETE) {
                input_errno = input_result == INPUT_ERROR ? errno : 0;
                failure_code = input_result == INPUT_ERROR ? "frame_read_failed"
                                                           : "frame_truncated";
                break;
            }
            if (miro_ego_pipeline_submit(pipeline, payload, length, 0) != 0) {
                /* Fail closed: a frame that cannot enter the codec ends the take. */
                failure_code = "frame_rejected";
                break;
            }
            if (atomic_exchange(&callback_ctx.stats_pending, 0) != 0) {
                miro_ego_pipeline_stats_t snapshot;
                miro_ego_pipeline_stats(pipeline, &snapshot);
                emit_stats("stats", &snapshot, 0ULL);
            }
        }
    }

    /* Preserve the causal error before filesystem drain can take seconds or
     * the owner times out and terminates us. Segment events remain atomic. */
    const bool error_emitted = failure_code != NULL;
    if (error_emitted) {
        char input_reason[256] = {0};
        if (input_errno != 0) {
            snprintf(input_reason, sizeof(input_reason), "%s: %s",
                     failure_code, strerror(input_errno));
        }
        const char *reason = miro_ego_pipeline_error(pipeline);
        emit_error_event(failure_code, reason[0] != '\0' ? reason :
                         input_reason[0] != '\0' ? input_reason : failure_code);
    }

    const int finished = miro_ego_pipeline_finish(pipeline);
    miro_ego_pipeline_stats_t stats;
    miro_ego_pipeline_stats(pipeline, &stats);
    const char *pipeline_error = miro_ego_pipeline_error(pipeline);
    const unsigned long long dropped =
        use_shm ? (unsigned long long)miro_ego_shm_ring_dropped(&shm_ring) : 0ULL;

    if (failure_code == NULL && finished != 0) {
        failure_code = "pipeline_failed";
    }
    if (failure_code != NULL && !error_emitted) {
        emit_error_event(failure_code,
                         pipeline_error[0] != '\0' ? pipeline_error : failure_code);
    }
    emit_stats("done", &stats, dropped);

    free(payload);
    miro_ego_pipeline_close(pipeline);
    if (use_shm) {
        miro_ego_shm_ring_detach(&shm_ring);
    }
    if (event_stream != NULL) {
        fclose(event_stream);
        event_stream = NULL;
    }
    return failure_code == NULL ? 0 : 1;
}

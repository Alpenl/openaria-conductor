/*
 * P0 gate harness for issue #46.
 *
 * Drives the production ylx_stereo_pipeline straight from V4L2 so the gate
 * measures the same JPU decode / row split / dual VPU encode code the recording
 * daemon ships, not a look-alike.
 *
 * Gate assertions (see summary.json): valid 1920x1080 H.264, application drop
 * zero, effective pair fps, RSS trend and peak CPU temperature.
 */

#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <poll.h>
#include <signal.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>
#include <linux/videodev2.h>

#include "ylx_cli_common.h"
#include "ylx_stereo_pipeline.h"

#define CAPTURE_BUFFERS 8

typedef struct {
    void *start;
    size_t length;
} mapped_buffer_t;

typedef struct {
    int fd;
    mapped_buffer_t buffers[CAPTURE_BUFFERS];
    unsigned int buffer_count;
} capture_t;

static volatile sig_atomic_t g_stop = 0;
static atomic_ullong g_segments = 0;

static void handle_signal(int signum)
{
    (void)signum;
    g_stop = 1;
}

static double now_monotonic(void)
{
    struct timespec ts = {0};
    if (clock_gettime(CLOCK_MONOTONIC, &ts) != 0) {
        return 0.0;
    }
    return (double)ts.tv_sec + (double)ts.tv_nsec / 1e9;
}

static long read_rss_kb(void)
{
    FILE *file = fopen("/proc/self/status", "r");
    if (file == NULL) {
        return -1;
    }
    char line[256];
    long value = -1;
    while (fgets(line, sizeof(line), file) != NULL) {
        if (strncmp(line, "VmRSS:", 6) == 0) {
            value = strtol(line + 6, NULL, 10);
            break;
        }
    }
    fclose(file);
    return value;
}

static double read_cpu_temp_c(void)
{
    FILE *file = fopen("/sys/class/thermal/thermal_zone0/temp", "r");
    if (file == NULL) {
        return -1.0;
    }
    long milli = -1;
    if (fscanf(file, "%ld", &milli) != 1) {
        milli = -1;
    }
    fclose(file);
    return milli < 0 ? -1.0 : (double)milli / 1000.0;
}

static void capture_release_resources(capture_t *capture)
{
    for (unsigned int index = 0; index < CAPTURE_BUFFERS; index += 1) {
        if (capture->buffers[index].start != NULL &&
            capture->buffers[index].start != MAP_FAILED) {
            munmap(capture->buffers[index].start, capture->buffers[index].length);
        }
        capture->buffers[index].start = NULL;
        capture->buffers[index].length = 0;
    }
    capture->buffer_count = 0;
    if (capture->fd >= 0) {
        close(capture->fd);
        capture->fd = -1;
    }
}

static int capture_open(capture_t *capture, const char *device, int width, int height,
                        int fps)
{
    memset(capture, 0, sizeof(*capture));
    capture->fd = open(device, O_RDWR | O_NONBLOCK);
    if (capture->fd < 0) {
        fprintf(stderr, "open(%s): %s\n", device, strerror(errno));
        return -1;
    }

    struct v4l2_format format;
    memset(&format, 0, sizeof(format));
    format.type = V4L2_BUF_TYPE_VIDEO_CAPTURE;
    format.fmt.pix.width = (unsigned int)width;
    format.fmt.pix.height = (unsigned int)height;
    format.fmt.pix.pixelformat = V4L2_PIX_FMT_MJPEG;
    format.fmt.pix.field = V4L2_FIELD_NONE;
    if (ioctl(capture->fd, VIDIOC_S_FMT, &format) < 0 ||
        (int)format.fmt.pix.width != width || (int)format.fmt.pix.height != height ||
        format.fmt.pix.pixelformat != V4L2_PIX_FMT_MJPEG) {
        fprintf(stderr, "camera refused %dx%d MJPEG\n", width, height);
        goto fail;
    }

    struct v4l2_streamparm parm;
    memset(&parm, 0, sizeof(parm));
    parm.type = V4L2_BUF_TYPE_VIDEO_CAPTURE;
    parm.parm.capture.timeperframe.numerator = 1;
    parm.parm.capture.timeperframe.denominator = (unsigned int)fps;
    if (ioctl(capture->fd, VIDIOC_S_PARM, &parm) < 0) {
        fprintf(stderr, "VIDIOC_S_PARM: %s\n", strerror(errno));
        goto fail;
    }

    struct v4l2_requestbuffers request;
    memset(&request, 0, sizeof(request));
    request.count = CAPTURE_BUFFERS;
    request.type = V4L2_BUF_TYPE_VIDEO_CAPTURE;
    request.memory = V4L2_MEMORY_MMAP;
    if (ioctl(capture->fd, VIDIOC_REQBUFS, &request) < 0) {
        fprintf(stderr, "VIDIOC_REQBUFS: %s\n", strerror(errno));
        goto fail;
    }
    if (request.count == 0 || request.count > CAPTURE_BUFFERS) {
        fprintf(stderr, "VIDIOC_REQBUFS invalid buffer count: %u (max %u)\n",
                request.count, (unsigned int)CAPTURE_BUFFERS);
        goto fail;
    }
    capture->buffer_count = request.count;

    for (unsigned int index = 0; index < capture->buffer_count; index += 1) {
        struct v4l2_buffer buffer;
        memset(&buffer, 0, sizeof(buffer));
        buffer.type = V4L2_BUF_TYPE_VIDEO_CAPTURE;
        buffer.memory = V4L2_MEMORY_MMAP;
        buffer.index = index;
        if (ioctl(capture->fd, VIDIOC_QUERYBUF, &buffer) < 0) {
            fprintf(stderr, "VIDIOC_QUERYBUF: %s\n", strerror(errno));
            goto fail;
        }
        void *mapped = mmap(NULL, buffer.length, PROT_READ | PROT_WRITE,
                            MAP_SHARED, capture->fd, buffer.m.offset);
        if (mapped == MAP_FAILED) {
            fprintf(stderr, "capture buffer mmap: %s\n", strerror(errno));
            goto fail;
        }
        capture->buffers[index].length = buffer.length;
        capture->buffers[index].start = mapped;
        if (ioctl(capture->fd, VIDIOC_QBUF, &buffer) < 0) {
            fprintf(stderr, "capture buffer setup: %s\n", strerror(errno));
            goto fail;
        }
    }

    enum v4l2_buf_type type = V4L2_BUF_TYPE_VIDEO_CAPTURE;
    if (ioctl(capture->fd, VIDIOC_STREAMON, &type) < 0) {
        fprintf(stderr, "VIDIOC_STREAMON: %s\n", strerror(errno));
        goto fail;
    }
    return 0;

fail:
    capture_release_resources(capture);
    return -1;
}

static void capture_close(capture_t *capture)
{
    if (capture->fd < 0) {
        return;
    }
    enum v4l2_buf_type type = V4L2_BUF_TYPE_VIDEO_CAPTURE;
    ioctl(capture->fd, VIDIOC_STREAMOFF, &type);
    capture_release_resources(capture);
}

static void on_segment(void *user, int index, unsigned long long start_frame,
                       unsigned long long end_frame, const char *left_path,
                       unsigned long long left_bytes, const char *right_path,
                       unsigned long long right_bytes)
{
    (void)user;
    atomic_fetch_add(&g_segments, 1ULL);
    fprintf(stderr, "segment %d frames [%llu,%llu) %s(%llu) %s(%llu)\n", index,
            start_frame, end_frame, left_path, left_bytes, right_path, right_bytes);
}

static double parse_positive_double(const char *value)
{
    char *end = NULL;
    errno = 0;
    double parsed = strtod(value, &end);
    if (errno != 0 || end == value || *end != '\0' || !(parsed > 0.0)) {
        fprintf(stderr, "invalid duration: %s\n", value);
        exit(2);
    }
    return parsed;
}

static void usage(const char *program)
{
    fprintf(stderr,
            "usage: %s --out-dir DIR [--device /dev/video0] [--width 3840]\n"
            "          [--height 1080] [--sensor-fps 60] [--decimation 2]\n"
            "          [--bitrate-kbps 8192] [--segment-frames 900] [--duration 600]\n"
            "          [--min-qp 20] [--intra-qp 22] [--initial-qp 24] [--max-qp 51]\n"
            "          [--intra-period 0] [--vbv-ms 3000]\n"
            "          [--codec h264|hevc] [--rc-mode cbr|vbr|fixqp|avbr]\n",
            program);
}

typedef struct {
    const char *device;
    const char *out_dir;
    int sensor_fps;
    int decimation;
    double duration_s;
    ylx_pipeline_config_t config;
} gate_options_t;

typedef struct {
    double peak_temp;
    long peak_rss;
    long first_rss;
    long last_rss;
} resource_tracker_t;

typedef struct {
    unsigned long long captured;
    unsigned long long capture_gaps;
} run_metrics_t;

static int parse_args(int argc, char **argv, gate_options_t *opts)
{
    *opts = (gate_options_t){
        .device = "/dev/video0",
        .out_dir = NULL,
        .sensor_fps = 60,
        .decimation = 2,
        .duration_s = 600.0,
        .config = {
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
            .path_prefix = "",
        },
    };

    const struct {
        const char *flag;
        int *target;
    } int_options[] = {
        {"--width", &opts->config.sbs_width},
        {"--height", &opts->config.height},
        {"--sensor-fps", &opts->sensor_fps},
        {"--decimation", &opts->decimation},
        {"--bitrate-kbps", &opts->config.bitrate_kbps},
        {"--segment-frames", &opts->config.segment_frames},
        {"--intra-period", &opts->config.intra_period},
        {"--min-qp", &opts->config.min_qp},
        {"--max-qp", &opts->config.max_qp},
        {"--intra-qp", &opts->config.intra_qp},
        {"--initial-qp", &opts->config.initial_qp},
        {"--vbv-ms", &opts->config.vbv_ms},
    };

    for (int index = 1; index < argc; index += 1) {
        const char *name = argv[index];
        const bool has_value = index + 1 < argc;
        if (strcmp(name, "--device") == 0 && has_value) {
            opts->device = argv[++index];
        } else if (strcmp(name, "--out-dir") == 0 && has_value) {
            opts->out_dir = argv[++index];
        } else if (strcmp(name, "--codec") == 0 && has_value) {
            if (ylx_parse_codec(argv[++index], &opts->config.hevc) != 0) {
                usage(argv[0]);
                return 2;
            }
        } else if (strcmp(name, "--rc-mode") == 0 && has_value) {
            if (ylx_parse_rc_mode(argv[++index], &opts->config.rate_control) != 0) {
                usage(argv[0]);
                return 2;
            }
        } else if (strcmp(name, "--duration") == 0 && has_value) {
            opts->duration_s = parse_positive_double(argv[++index]);
        } else if (has_value) {
            bool matched = false;
            for (size_t opt = 0; opt < sizeof(int_options) / sizeof(int_options[0]); opt += 1) {
                if (strcmp(name, int_options[opt].flag) == 0) {
                    *int_options[opt].target = ylx_parse_integer(argv[++index]);
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

    if (opts->out_dir == NULL || opts->decimation < 1 || opts->sensor_fps < 1 ||
        opts->sensor_fps / opts->decimation < 1) {
        usage(argv[0]);
        return 2;
    }
    opts->config.fps = opts->sensor_fps / opts->decimation;
    opts->config.out_dir = opts->out_dir;
    return 0;
}

static void update_resource_peaks(resource_tracker_t *tracker, bool update_last,
                                  double *temp_out, long *rss_out)
{
    const double temp = read_cpu_temp_c();
    const long rss = read_rss_kb();
    if (temp > tracker->peak_temp) {
        tracker->peak_temp = temp;
    }
    if (rss > tracker->peak_rss) {
        tracker->peak_rss = rss;
    }
    if (tracker->first_rss < 0) {
        tracker->first_rss = rss;
    }
    if (update_last || tracker->last_rss < 0) {
        tracker->last_rss = rss;
    }
    if (temp_out != NULL) {
        *temp_out = temp;
    }
    if (rss_out != NULL) {
        *rss_out = rss;
    }
}

static void run_capture_loop(capture_t *capture, ylx_pipeline_t *pipeline,
                             const gate_options_t *opts, double started,
                             run_metrics_t *metrics, resource_tracker_t *tracker)
{
    double next_sample = started + 60.0;
    uint64_t previous_sequence = 0;
    bool have_sequence = false;
    uint64_t capture_index = 0;

    while (g_stop == 0 && (now_monotonic() - started) < opts->duration_s) {
        struct pollfd pfd = {.fd = capture->fd, .events = POLLIN};
        int ready = poll(&pfd, 1, 1000);
        if (ready < 0) {
            if (errno == EINTR) {
                continue;
            }
            fprintf(stderr, "poll: %s\n", strerror(errno));
            break;
        }
        if (ready == 0) {
            continue;
        }

        struct v4l2_buffer buffer;
        memset(&buffer, 0, sizeof(buffer));
        buffer.type = V4L2_BUF_TYPE_VIDEO_CAPTURE;
        buffer.memory = V4L2_MEMORY_MMAP;
        if (ioctl(capture->fd, VIDIOC_DQBUF, &buffer) < 0) {
            if (errno == EAGAIN) {
                continue;
            }
            fprintf(stderr, "VIDIOC_DQBUF: %s\n", strerror(errno));
            break;
        }

        if (buffer.index >= capture->buffer_count) {
            fprintf(stderr, "VIDIOC_DQBUF returned out-of-range index %u (count=%u)\n",
                    buffer.index, capture->buffer_count);
            break;
        }
        if ((buffer.flags & V4L2_BUF_FLAG_ERROR) == 0 && buffer.bytesused > 0) {
            if (buffer.bytesused > capture->buffers[buffer.index].length) {
                fprintf(stderr, "VIDIOC_DQBUF bytesused %u exceeds buffer length %zu\n",
                        buffer.bytesused, capture->buffers[buffer.index].length);
                ioctl(capture->fd, VIDIOC_QBUF, &buffer);
                break;
            }
            metrics->captured += 1;
            if (have_sequence && buffer.sequence > previous_sequence + 1) {
                metrics->capture_gaps += buffer.sequence - previous_sequence - 1;
            }
            previous_sequence = buffer.sequence;
            have_sequence = true;
            if ((capture_index % (uint64_t)opts->decimation) == 0) {
                ylx_pipeline_submit(pipeline, capture->buffers[buffer.index].start,
                                    buffer.bytesused, 0);
            }
            capture_index += 1;
        }
        if (ioctl(capture->fd, VIDIOC_QBUF, &buffer) < 0) {
            fprintf(stderr, "VIDIOC_QBUF: %s\n", strerror(errno));
            break;
        }

        const double now = now_monotonic();
        if (now >= next_sample) {
            double temp = -1.0;
            long rss = -1;
            update_resource_peaks(tracker, true, &temp, &rss);
            ylx_pipeline_stats_t sample;
            ylx_pipeline_stats(pipeline, &sample);
            const unsigned long long segments = atomic_load(&g_segments);
            fprintf(stderr,
                    "[%6.1fs] captured=%llu submitted=%llu decoded=%llu left=%llu "
                    "right=%llu segments=%llu temp=%.1fC rss=%ldkB\n",
                    now - started, metrics->captured, sample.submitted, sample.decoded,
                    sample.encoded[0], sample.encoded[1], segments, temp, rss);
            next_sample = now + 60.0;
        }
    }
}

static void write_summary_json(const char *out_dir, double elapsed,
                               const run_metrics_t *metrics,
                               const ylx_pipeline_stats_t *stats,
                               unsigned long long total_segments,
                               unsigned long long application_drop,
                               double pair_fps, const resource_tracker_t *tracker,
                               const char *pipeline_error)
{
    char summary_path[512];
    int path_len = snprintf(summary_path, sizeof(summary_path), "%s/summary.json", out_dir);
    if (path_len < 0 || (size_t)path_len >= sizeof(summary_path)) {
        fprintf(stderr, "summary path too long: %s/summary.json\n", out_dir);
        return;
    }
    FILE *summary = fopen(summary_path, "w");
    if (summary == NULL) {
        return;
    }
    fprintf(summary,
            "{\n"
            "  \"duration_seconds\": %.3f,\n"
            "  \"captured_frames\": %llu,\n"
            "  \"capture_sequence_gaps\": %llu,\n"
            "  \"submitted_frames\": %llu,\n"
            "  \"decoded_frames\": %llu,\n"
            "  \"left_frames\": %llu,\n"
            "  \"right_frames\": %llu,\n"
            "  \"left_bytes\": %llu,\n"
            "  \"right_bytes\": %llu,\n"
            "  \"segments_closed\": %llu,\n"
            "  \"drop_decoder_input\": %llu,\n"
            "  \"drop_encoder_input\": %llu,\n"
            "  \"drop_decoder_output\": %llu,\n"
            "  \"application_drop\": %llu,\n"
            "  \"effective_pair_fps\": %.3f,\n"
            "  \"peak_cpu_temp_c\": %.1f,\n"
            "  \"first_sample_rss_kb\": %ld,\n"
            "  \"last_sample_rss_kb\": %ld,\n"
            "  \"peak_rss_kb\": %ld,\n"
            "  \"pipeline_error\": ",
            elapsed, metrics->captured, metrics->capture_gaps, stats->submitted,
            stats->decoded, stats->encoded[0], stats->encoded[1], stats->bytes[0],
            stats->bytes[1], total_segments, stats->drop_decoder_input,
            stats->drop_encoder_input, stats->drop_decoder_output, application_drop,
            pair_fps, tracker->peak_temp, tracker->first_rss, tracker->last_rss,
            tracker->peak_rss);
    ylx_write_json_string(summary, pipeline_error);
    fputs("\n}\n", summary);
    fclose(summary);
}

int main(int argc, char **argv)
{
    gate_options_t opts;
    int parse_status = parse_args(argc, argv, &opts);
    if (parse_status != 0) {
        return parse_status;
    }
    mkdir(opts.out_dir, 0755);

    signal(SIGINT, handle_signal);
    signal(SIGTERM, handle_signal);

    char error[256] = {0};
    ylx_pipeline_t *pipeline = NULL;
    if (ylx_pipeline_open(&opts.config, on_segment, NULL, &pipeline, error, sizeof(error)) != 0) {
        fprintf(stderr, "pipeline open failed: %s\n", error);
        return 3;
    }

    capture_t capture;
    if (capture_open(&capture, opts.device, opts.config.sbs_width, opts.config.height,
                     opts.sensor_fps) != 0) {
        ylx_pipeline_close(pipeline);
        return 4;
    }

    const double started = now_monotonic();
    run_metrics_t metrics = {0};
    resource_tracker_t tracker = {
        .peak_temp = -1.0,
        .peak_rss = -1,
        .first_rss = -1,
        .last_rss = -1,
    };
    run_capture_loop(&capture, pipeline, &opts, started, &metrics, &tracker);

    const double elapsed = now_monotonic() - started;
    capture_close(&capture);
    const int finished = ylx_pipeline_finish(pipeline);
    ylx_pipeline_stats_t stats;
    ylx_pipeline_stats(pipeline, &stats);
    const char *pipeline_error = ylx_pipeline_error(pipeline);
    const unsigned long long total_segments = atomic_load(&g_segments);

    update_resource_peaks(&tracker, false, NULL, NULL);

    const unsigned long long left = stats.encoded[0];
    const unsigned long long right = stats.encoded[1];
    const unsigned long long pairs = left < right ? left : right;
    /* Frames rejected downstream already show up as missing encoder output, so
     * only the frames the JPU never accepted are counted separately. */
    const unsigned long long application_drop =
        stats.drop_decoder_input + (stats.submitted > left ? stats.submitted - left : 0) +
        (stats.submitted > right ? stats.submitted - right : 0);
    const double pair_fps = elapsed > 0 ? (double)pairs / elapsed : 0.0;

    write_summary_json(opts.out_dir, elapsed, &metrics, &stats, total_segments,
                       application_drop, pair_fps, &tracker, pipeline_error);

    fprintf(stderr,
            "done in %.1fs: captured=%llu submitted=%llu left=%llu right=%llu "
            "segments=%llu application_drop=%llu pair_fps=%.2f peak_temp=%.1fC "
            "rss %ld->%ld kB\n",
            elapsed, metrics.captured, stats.submitted, left, right, total_segments,
            application_drop, pair_fps, tracker.peak_temp, tracker.first_rss,
            tracker.last_rss);

    ylx_pipeline_close(pipeline);
    return (application_drop == 0 && finished == 0) ? 0 : 1;
}

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
static unsigned long long g_segments = 0;

static void handle_signal(int signum)
{
    (void)signum;
    g_stop = 1;
}

static double now_monotonic(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
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
        return -1;
    }

    struct v4l2_streamparm parm;
    memset(&parm, 0, sizeof(parm));
    parm.type = V4L2_BUF_TYPE_VIDEO_CAPTURE;
    parm.parm.capture.timeperframe.numerator = 1;
    parm.parm.capture.timeperframe.denominator = (unsigned int)fps;
    if (ioctl(capture->fd, VIDIOC_S_PARM, &parm) < 0) {
        fprintf(stderr, "VIDIOC_S_PARM: %s\n", strerror(errno));
        return -1;
    }

    struct v4l2_requestbuffers request;
    memset(&request, 0, sizeof(request));
    request.count = CAPTURE_BUFFERS;
    request.type = V4L2_BUF_TYPE_VIDEO_CAPTURE;
    request.memory = V4L2_MEMORY_MMAP;
    if (ioctl(capture->fd, VIDIOC_REQBUFS, &request) < 0) {
        fprintf(stderr, "VIDIOC_REQBUFS: %s\n", strerror(errno));
        return -1;
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
            return -1;
        }
        capture->buffers[index].length = buffer.length;
        capture->buffers[index].start = mmap(NULL, buffer.length, PROT_READ | PROT_WRITE,
                                             MAP_SHARED, capture->fd, buffer.m.offset);
        if (capture->buffers[index].start == MAP_FAILED ||
            ioctl(capture->fd, VIDIOC_QBUF, &buffer) < 0) {
            fprintf(stderr, "capture buffer setup: %s\n", strerror(errno));
            return -1;
        }
    }

    enum v4l2_buf_type type = V4L2_BUF_TYPE_VIDEO_CAPTURE;
    if (ioctl(capture->fd, VIDIOC_STREAMON, &type) < 0) {
        fprintf(stderr, "VIDIOC_STREAMON: %s\n", strerror(errno));
        return -1;
    }
    return 0;
}

static void capture_close(capture_t *capture)
{
    if (capture->fd < 0) {
        return;
    }
    enum v4l2_buf_type type = V4L2_BUF_TYPE_VIDEO_CAPTURE;
    ioctl(capture->fd, VIDIOC_STREAMOFF, &type);
    for (unsigned int index = 0; index < capture->buffer_count; index += 1) {
        if (capture->buffers[index].start != NULL &&
            capture->buffers[index].start != MAP_FAILED) {
            munmap(capture->buffers[index].start, capture->buffers[index].length);
        }
    }
    close(capture->fd);
    capture->fd = -1;
}

static void on_segment(void *user, int index, unsigned long long start_frame,
                       unsigned long long end_frame, const char *left_path,
                       unsigned long long left_bytes, const char *right_path,
                       unsigned long long right_bytes)
{
    (void)user;
    g_segments += 1;
    fprintf(stderr, "segment %d frames [%llu,%llu) %s(%llu) %s(%llu)\n", index,
            start_frame, end_frame, left_path, left_bytes, right_path, right_bytes);
}

static void usage(const char *program)
{
    fprintf(stderr,
            "usage: %s --out-dir DIR [--device /dev/video0] [--width 3840]\n"
            "          [--height 1080] [--sensor-fps 60] [--decimation 2]\n"
            "          [--bitrate-kbps 8192] [--segment-frames 900] [--duration 600]\n",
            program);
}

int main(int argc, char **argv)
{
    const char *device = "/dev/video0";
    const char *out_dir = NULL;
    int width = 3840;
    int height = 1080;
    int sensor_fps = 60;
    int decimation = 2;
    int bitrate_kbps = 8192;
    int segment_frames = 900;
    double duration_s = 600.0;

    for (int index = 1; index < argc; index += 1) {
        const char *name = argv[index];
        const bool has_value = index + 1 < argc;
        if (strcmp(name, "--device") == 0 && has_value) {
            device = argv[++index];
        } else if (strcmp(name, "--out-dir") == 0 && has_value) {
            out_dir = argv[++index];
        } else if (strcmp(name, "--width") == 0 && has_value) {
            width = atoi(argv[++index]);
        } else if (strcmp(name, "--height") == 0 && has_value) {
            height = atoi(argv[++index]);
        } else if (strcmp(name, "--sensor-fps") == 0 && has_value) {
            sensor_fps = atoi(argv[++index]);
        } else if (strcmp(name, "--decimation") == 0 && has_value) {
            decimation = atoi(argv[++index]);
        } else if (strcmp(name, "--bitrate-kbps") == 0 && has_value) {
            bitrate_kbps = atoi(argv[++index]);
        } else if (strcmp(name, "--segment-frames") == 0 && has_value) {
            segment_frames = atoi(argv[++index]);
        } else if (strcmp(name, "--duration") == 0 && has_value) {
            duration_s = atof(argv[++index]);
        } else {
            usage(argv[0]);
            return 2;
        }
    }
    if (out_dir == NULL || decimation < 1 || sensor_fps < 1) {
        usage(argv[0]);
        return 2;
    }
    mkdir(out_dir, 0755);

    signal(SIGINT, handle_signal);
    signal(SIGTERM, handle_signal);

    ylx_pipeline_config_t config = {
        .sbs_width = width,
        .height = height,
        .fps = sensor_fps / decimation,
        .bitrate_kbps = bitrate_kbps,
        .intra_period = 0,
        .min_qp = 28,
        .intra_qp = 30,
        .initial_qp = 32,
        .vbv_ms = 3000,
        .segment_frames = segment_frames,
        .out_dir = out_dir,
        .path_prefix = "",
    };

    char error[256] = {0};
    ylx_pipeline_t *pipeline = NULL;
    if (ylx_pipeline_open(&config, on_segment, NULL, &pipeline, error, sizeof(error)) != 0) {
        fprintf(stderr, "pipeline open failed: %s\n", error);
        return 3;
    }

    capture_t capture;
    if (capture_open(&capture, device, width, height, sensor_fps) != 0) {
        ylx_pipeline_close(pipeline);
        return 4;
    }

    const double started = now_monotonic();
    double next_sample = started + 60.0;
    double peak_temp = -1.0;
    long peak_rss = -1;
    long first_rss = -1;
    long last_rss = -1;
    unsigned long long captured = 0;
    unsigned long long capture_gaps = 0;
    uint64_t previous_sequence = 0;
    bool have_sequence = false;
    uint64_t capture_index = 0;

    while (g_stop == 0 && (now_monotonic() - started) < duration_s) {
        struct pollfd pfd = {.fd = capture.fd, .events = POLLIN};
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
        if (ioctl(capture.fd, VIDIOC_DQBUF, &buffer) < 0) {
            if (errno == EAGAIN) {
                continue;
            }
            fprintf(stderr, "VIDIOC_DQBUF: %s\n", strerror(errno));
            break;
        }

        if ((buffer.flags & V4L2_BUF_FLAG_ERROR) == 0 && buffer.bytesused > 0) {
            captured += 1;
            if (have_sequence && buffer.sequence > previous_sequence + 1) {
                capture_gaps += buffer.sequence - previous_sequence - 1;
            }
            previous_sequence = buffer.sequence;
            have_sequence = true;
            if ((capture_index % (uint64_t)decimation) == 0) {
                ylx_pipeline_submit(pipeline, capture.buffers[buffer.index].start,
                                    buffer.bytesused, 0);
            }
            capture_index += 1;
        }
        ioctl(capture.fd, VIDIOC_QBUF, &buffer);

        const double now = now_monotonic();
        if (now >= next_sample) {
            const double temp = read_cpu_temp_c();
            const long rss = read_rss_kb();
            if (temp > peak_temp) {
                peak_temp = temp;
            }
            if (rss > peak_rss) {
                peak_rss = rss;
            }
            if (first_rss < 0) {
                first_rss = rss;
            }
            last_rss = rss;
            ylx_pipeline_stats_t sample;
            ylx_pipeline_stats(pipeline, &sample);
            fprintf(stderr,
                    "[%6.1fs] captured=%llu submitted=%llu decoded=%llu left=%llu "
                    "right=%llu segments=%llu temp=%.1fC rss=%ldkB\n",
                    now - started, captured, sample.submitted, sample.decoded,
                    sample.encoded[0], sample.encoded[1], g_segments, temp, rss);
            next_sample = now + 60.0;
        }
    }

    const double elapsed = now_monotonic() - started;
    capture_close(&capture);
    const int finished = ylx_pipeline_finish(pipeline);
    ylx_pipeline_stats_t stats;
    ylx_pipeline_stats(pipeline, &stats);
    const char *pipeline_error = ylx_pipeline_error(pipeline);

    const double final_temp = read_cpu_temp_c();
    const long final_rss = read_rss_kb();
    if (final_temp > peak_temp) {
        peak_temp = final_temp;
    }
    if (final_rss > peak_rss) {
        peak_rss = final_rss;
    }
    if (first_rss < 0) {
        first_rss = final_rss;
    }
    if (last_rss < 0) {
        last_rss = final_rss;
    }

    const unsigned long long left = stats.encoded[0];
    const unsigned long long right = stats.encoded[1];
    const unsigned long long pairs = left < right ? left : right;
    /* Frames rejected downstream already show up as missing encoder output, so
     * only the frames the JPU never accepted are counted separately. */
    const unsigned long long application_drop =
        stats.drop_decoder_input + (stats.submitted > left ? stats.submitted - left : 0) +
        (stats.submitted > right ? stats.submitted - right : 0);
    const double pair_fps = elapsed > 0 ? (double)pairs / elapsed : 0.0;

    char summary_path[512];
    snprintf(summary_path, sizeof(summary_path), "%s/summary.json", out_dir);
    FILE *summary = fopen(summary_path, "w");
    if (summary != NULL) {
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
                "  \"pipeline_error\": \"%s\"\n"
                "}\n",
                elapsed, captured, capture_gaps, stats.submitted, stats.decoded, left,
                right, stats.bytes[0], stats.bytes[1], g_segments,
                stats.drop_decoder_input, stats.drop_encoder_input,
                stats.drop_decoder_output, application_drop, pair_fps, peak_temp,
                first_rss, last_rss, peak_rss, pipeline_error);
        fclose(summary);
    }

    fprintf(stderr,
            "done in %.1fs: captured=%llu submitted=%llu left=%llu right=%llu "
            "segments=%llu application_drop=%llu pair_fps=%.2f peak_temp=%.1fC "
            "rss %ld->%ld kB\n",
            elapsed, captured, stats.submitted, left, right, g_segments,
            application_drop, pair_fps, peak_temp, first_rss, last_rss);

    ylx_pipeline_close(pipeline);
    return (application_drop == 0 && finished == 0) ? 0 : 1;
}

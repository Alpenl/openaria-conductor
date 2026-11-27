#define _GNU_SOURCE
#include "miro_ego_sha256.h"
#include "miro_ego_shm_ring.h"
#include "miro_ego_stereo_pipeline.h"
#include "mock_hobot_sdk.h"

#include <assert.h>
#include <fcntl.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/eventfd.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

typedef struct {
    int index;
    unsigned long long start_frame;
    unsigned long long end_frame;
    char left_path[256];
    unsigned long long left_bytes;
    char left_sha256[MIRO_EGO_SHA256_HEX_LEN];
    char right_path[256];
    unsigned long long right_bytes;
    char right_sha256[MIRO_EGO_SHA256_HEX_LEN];
} recorded_seg_t;

typedef struct {
    recorded_seg_t segs[16];
    int count;
} seg_collector_t;

static void collect_segment(void *user, int index,
                            unsigned long long start_frame, unsigned long long end_frame,
                            const char *left_path, unsigned long long left_bytes,
                            const char *left_sha256,
                            const char *right_path, unsigned long long right_bytes,
                            const char *right_sha256)
{
    seg_collector_t *col = (seg_collector_t *)user;
    assert(col->count < 16);
    recorded_seg_t *s = &col->segs[col->count++];
    s->index = index;
    s->start_frame = start_frame;
    s->end_frame = end_frame;
    snprintf(s->left_path, sizeof(s->left_path), "%s", left_path);
    s->left_bytes = left_bytes;
    snprintf(s->left_sha256, sizeof(s->left_sha256), "%s", left_sha256);
    snprintf(s->right_path, sizeof(s->right_path), "%s", right_path);
    s->right_bytes = right_bytes;
    snprintf(s->right_sha256, sizeof(s->right_sha256), "%s", right_sha256);
}

static void verify_file_sha256(const char *abs_path, unsigned long long expected_bytes,
                               const char *expected_sha256)
{
    int fd = open(abs_path, O_RDONLY);
    assert(fd >= 0);
    char actual_sha[MIRO_EGO_SHA256_HEX_LEN] = {0};
    unsigned long long actual_bytes = 0;
    assert(miro_ego_sha256_fd(fd, actual_sha, &actual_bytes) == 0);
    close(fd);

    assert(actual_bytes == expected_bytes);
    assert(strlen(expected_sha256) == 64u);
    assert(strcmp(actual_sha, expected_sha256) == 0);
}

static void cleanup_dir(const char *dir)
{
    char cmd[512];
    snprintf(cmd, sizeof(cmd), "rm -rf %s", dir);
    int rc = system(cmd);
    (void)rc;
}

/* Custom split_ops hook test state */
static int g_custom_init_calls = 0;
static int g_custom_split_calls = 0;
static int g_custom_destroy_calls = 0;

static int custom_split_init(void **ctx, int sbs_width, int height,
                             const miro_ego_preview_config_t *preview)
{
    (void)sbs_width;
    (void)height;
    (void)preview;
    g_custom_init_calls += 1;
    *ctx = (void *)(uintptr_t)0x1234;
    return 0;
}

static int custom_split_eye(void *ctx, const miro_ego_nv12_frame_t *src,
                            miro_ego_nv12_frame_t *dst, int eye_width,
                            int height, int x_offset)
{
    assert(ctx == (void *)(uintptr_t)0x1234);
    g_custom_split_calls += 1;
    return miro_ego_split_eye_nv12(src, dst, eye_width, height, x_offset);
}

static void custom_split_destroy(void *ctx)
{
    assert(ctx == (void *)(uintptr_t)0x1234);
    g_custom_destroy_calls += 1;
}

static const miro_ego_split_ops_t CUSTOM_SPLIT_OPS = {
    .name = "mock-vse",
    .init = custom_split_init,
    .split_eye = custom_split_eye,
    .split_stereo = NULL,
    .destroy = custom_split_destroy,
};

static void test_pipeline_multi_segment(int hevc, const miro_ego_split_ops_t *ops)
{
    char dir[] = "/tmp/miro_ego_e2e_XXXXXX";
    assert(mkdtemp(dir) != NULL);

    mock_hobot_config_t mock = {
        .sbs_width = 64,
        .height = 32,
        .intra_period = 3,
        .force_missing_idr = 0,
        .force_decoder_capacity = 0,
        .force_bad_decode_result = 0,
        .emit_stdout_noise = 0,
    };
    mock_hobot_reset(&mock);

    miro_ego_pipeline_config_t cfg = {
        .sbs_width = 64,
        .height = 32,
        .fps = 30,
        .bitrate_kbps = 4096,
        .hevc = hevc,
        .rate_control = 0,
        .intra_period = 3,
        .min_qp = 18,
        .max_qp = 40,
        .intra_qp = 22,
        .initial_qp = 24,
        .vbv_ms = 2000,
        .segment_frames = 3,
        .out_dir = dir,
        .path_prefix = "video/",
        .split_ops = ops,
        .preview = {0},
    };

    seg_collector_t col = {0};
    miro_ego_pipeline_t *pipeline = NULL;
    char err[256] = {0};
    assert(miro_ego_pipeline_open(&cfg, collect_segment, &col, &pipeline, err, sizeof(err)) == 0);

    uint8_t fake_jpeg[64];
    memset(fake_jpeg, 0xd8, sizeof(fake_jpeg));
    for (int i = 0; i < 8; i += 1) {
        fake_jpeg[0] = (uint8_t)i;
        assert(miro_ego_pipeline_submit(pipeline, fake_jpeg, sizeof(fake_jpeg), 500) == 0);
    }

    assert(miro_ego_pipeline_finish(pipeline) == 0);
    assert(!miro_ego_pipeline_failed(pipeline));

    miro_ego_pipeline_stats_t st;
    miro_ego_pipeline_stats(pipeline, &st);
    assert(st.offered == 8 && st.submitted == 8 && st.decoded == 8);
    assert(st.encoded[0] == 8 && st.encoded[1] == 8);

    miro_ego_pipeline_close(pipeline);

    /* Expect 3 segments: [0..3), [3..6), [6..8) */
    assert(col.count == 3);
    const unsigned long long expected_starts[3] = {0, 3, 6};
    const unsigned long long expected_ends[3] = {3, 6, 8};
    for (int i = 0; i < 3; i += 1) {
        assert(col.segs[i].index == i);
        assert(col.segs[i].start_frame == expected_starts[i]);
        assert(col.segs[i].end_frame == expected_ends[i]);

        char abs_l[512], abs_r[512];
        snprintf(abs_l, sizeof(abs_l), "%s/left_%05d.mp4", dir, i);
        snprintf(abs_r, sizeof(abs_r), "%s/right_%05d.mp4", dir, i);
        verify_file_sha256(abs_l, col.segs[i].left_bytes, col.segs[i].left_sha256);
        verify_file_sha256(abs_r, col.segs[i].right_bytes, col.segs[i].right_sha256);
    }

    cleanup_dir(dir);
}

static void test_pipeline_failure_modes(void)
{
    char dir[] = "/tmp/miro_ego_fail_XXXXXX";
    assert(mkdtemp(dir) != NULL);

    /* NULL pipeline guards fail closed without crashing */
    miro_ego_pipeline_stats_t null_st;
    memset(&null_st, 0xff, sizeof(null_st));
    miro_ego_pipeline_stats(NULL, &null_st);
    assert(null_st.offered == 0 && null_st.submitted == 0);
    miro_ego_pipeline_stats(NULL, NULL);
    assert(miro_ego_pipeline_submit(NULL, (const unsigned char *)"x", 1, 10) == -1);
    assert(miro_ego_pipeline_failed(NULL) == 1);
    assert(strcmp(miro_ego_pipeline_error(NULL), "") == 0);

    /* Invalid configuration fails cleanly */
    miro_ego_pipeline_config_t bad_cfg = {
        .sbs_width = 3, /* not divisible by 4 */
        .height = 32,
        .fps = 30,
        .bitrate_kbps = 4096,
        .out_dir = dir,
    };
    miro_ego_pipeline_t *pipeline = NULL;
    char err[256] = {0};
    assert(miro_ego_pipeline_open(&bad_cfg, NULL, NULL, &pipeline, err, sizeof(err)) == -1);
    assert(pipeline == NULL && strstr(err, "invalid_configuration") != NULL);

    /* Missing IDR fails closed with segment_missing_idr */
    mock_hobot_config_t mock = {
        .sbs_width = 64,
        .height = 32,
        .intra_period = 3,
        .force_missing_idr = 1,
        .force_decoder_capacity = 0,
        .force_bad_decode_result = 0,
        .emit_stdout_noise = 0,
    };
    mock_hobot_reset(&mock);

    miro_ego_pipeline_config_t cfg = {
        .sbs_width = 64,
        .height = 32,
        .fps = 30,
        .bitrate_kbps = 4096,
        .hevc = 0,
        .rate_control = 0,
        .intra_period = 3,
        .min_qp = 18,
        .max_qp = 40,
        .intra_qp = 22,
        .initial_qp = 24,
        .vbv_ms = 2000,
        .segment_frames = 3,
        .out_dir = dir,
        .path_prefix = "video/",
    };
    seg_collector_t col = {0};
    assert(miro_ego_pipeline_open(&cfg, collect_segment, &col, &pipeline, err, sizeof(err)) == 0);
    uint8_t fake_jpeg[32] = {1, 2, 3, 4};
    assert(miro_ego_pipeline_submit(pipeline, fake_jpeg, sizeof(fake_jpeg), 500) == 0);
    assert(miro_ego_pipeline_finish(pipeline) == -1);
    assert(miro_ego_pipeline_failed(pipeline));
    assert(strstr(miro_ego_pipeline_error(pipeline), "segment_missing_idr") != NULL);
    assert(col.count == 0); /* never publish corrupt segment */
    miro_ego_pipeline_close(pipeline);

    cleanup_dir(dir);
}

static void extract_json_str(const char *line, const char *key, char *out, size_t out_len)
{
    char pat[64];
    snprintf(pat, sizeof(pat), "\"%s\":\"", key);
    const char *p = strstr(line, pat);
    assert(p != NULL);
    p += strlen(pat);
    const char *end = strchr(p, '"');
    assert(end != NULL);
    size_t n = (size_t)(end - p);
    assert(n < out_len);
    memcpy(out, p, n);
    out[n] = '\0';
}

static unsigned long long extract_json_u64(const char *line, const char *key)
{
    char pat[64];
    snprintf(pat, sizeof(pat), "\"%s\":", key);
    const char *p = strstr(line, pat);
    assert(p != NULL);
    p += strlen(pat);
    return strtoull(p, NULL, 10);
}

static void test_cli_shm_end_to_end(void)
{
    char dir[] = "/tmp/miro_ego_cli_shm_XXXXXX";
    assert(mkdtemp(dir) != NULL);

    /* Inheritable FDs (no CLOEXEC) for child miro-ego-encoder-mock */
    int shm_fd = memfd_create("miro-ego-cli-shm", 0);
    int kick_fd = eventfd(0, EFD_NONBLOCK);
    int done_fd = eventfd(0, EFD_NONBLOCK);
    assert(shm_fd >= 0 && kick_fd >= 0 && done_fd >= 0);

    miro_ego_shm_ring_t producer;
    char err[128] = {0};
    assert(miro_ego_shm_ring_init_fd(&producer, shm_fd, kick_fd, done_fd,
                                     8u, 4096u, err, sizeof(err)) == 0);

    int out_pipe[2];
    int err_pipe[2];
    assert(pipe(out_pipe) == 0 && pipe(err_pipe) == 0);

    pid_t pid = fork();
    assert(pid >= 0);
    if (pid == 0) {
        close(out_pipe[0]);
        close(err_pipe[0]);
        dup2(out_pipe[1], STDOUT_FILENO);
        dup2(err_pipe[1], STDERR_FILENO);
        close(out_pipe[1]);
        close(err_pipe[1]);

        char shm_s[16], kick_s[16], done_s[16];
        snprintf(shm_s, sizeof(shm_s), "%d", shm_fd);
        snprintf(kick_s, sizeof(kick_s), "%d", kick_fd);
        snprintf(done_s, sizeof(done_s), "%d", done_fd);

        execl("./tests/build/miro-ego-encoder-mock", "miro-ego-encoder-mock",
              "--out-dir", dir,
              "--path-prefix", "video/",
              "--width", "64",
              "--height", "32",
              "--fps", "30",
              "--bitrate-kbps", "4096",
              "--intra-period", "3",
              "--segment-frames", "3",
              "--shm-fd", shm_s,
              "--kick-fd", kick_s,
              "--done-fd", done_s,
              (char *)NULL);
        _exit(127);
    }

    close(out_pipe[1]);
    close(err_pipe[1]);

    FILE *out_fp = fdopen(out_pipe[0], "r");
    assert(out_fp != NULL);

    /* Wait for {"event":"ready"} */
    char line[2048];
    assert(fgets(line, sizeof(line), out_fp) != NULL);
    assert(strstr(line, "\"event\":\"ready\"") != NULL);

    /* Push 6 frames through MEGO_SHM ring + EOF */
    uint8_t jpeg[128];
    memset(jpeg, 0xaa, sizeof(jpeg));
    for (uint64_t seq = 0; seq < 6u; seq += 1) {
        jpeg[0] = (uint8_t)seq;
        assert(miro_ego_shm_ring_push(&producer, jpeg, sizeof(jpeg),
                                      seq, seq * 33333ULL, 0u, 5000) == MIRO_EGO_SHM_OK);
    }
    assert(miro_ego_shm_ring_set_eof(&producer) == MIRO_EGO_SHM_OK);

    int segments_seen = 0;
    int done_seen = 0;
    while (fgets(line, sizeof(line), out_fp) != NULL) {
        /* Vendor SDK stdout noise must NEVER leak into NDJSON pipe! */
        assert(strstr(line, "mock-hobot-sdk") == NULL);
        if (strstr(line, "\"event\":\"segment\"") != NULL) {
            char l_path[256], r_path[256];
            char l_sha[MIRO_EGO_SHA256_HEX_LEN], r_sha[MIRO_EGO_SHA256_HEX_LEN];
            extract_json_str(line, "left_path", l_path, sizeof(l_path));
            extract_json_str(line, "right_path", r_path, sizeof(r_path));
            extract_json_str(line, "left_sha256", l_sha, sizeof(l_sha));
            extract_json_str(line, "right_sha256", r_sha, sizeof(r_sha));
            unsigned long long l_bytes = extract_json_u64(line, "left_bytes");
            unsigned long long r_bytes = extract_json_u64(line, "right_bytes");
            unsigned long long idx = extract_json_u64(line, "index");
            assert(idx == (unsigned long long)segments_seen);

            char abs_l[512], abs_r[512];
            snprintf(abs_l, sizeof(abs_l), "%s/left_%05llu.mp4", dir, idx);
            snprintf(abs_r, sizeof(abs_r), "%s/right_%05llu.mp4", dir, idx);
            verify_file_sha256(abs_l, l_bytes, l_sha);
            verify_file_sha256(abs_r, r_bytes, r_sha);
            segments_seen += 1;
        } else if (strstr(line, "\"event\":\"done\"") != NULL) {
            assert(extract_json_u64(line, "submitted") == 6ULL);
            assert(extract_json_u64(line, "decoded") == 6ULL);
            assert(extract_json_u64(line, "left_frames") == 6ULL);
            assert(extract_json_u64(line, "right_frames") == 6ULL);
            done_seen += 1;
        }
    }
    fclose(out_fp);

    /* Confirm vendor library stdout noise was redirected to stderr */
    char err_buf[4096] = {0};
    ssize_t err_n = read(err_pipe[0], err_buf, sizeof(err_buf) - 1u);
    close(err_pipe[0]);
    assert(err_n > 0);
    assert(strstr(err_buf, "mock-hobot-sdk") != NULL);

    int status = 0;
    assert(waitpid(pid, &status, 0) == pid);
    assert(WIFEXITED(status) && WEXITSTATUS(status) == 0);
    assert(segments_seen == 2);
    assert(done_seen == 1);

    miro_ego_shm_ring_detach(&producer);
    close(shm_fd);
    close(kick_fd);
    close(done_fd);
    cleanup_dir(dir);
}

static void test_cli_stdin_fallback(void)
{
    char dir[] = "/tmp/miro_ego_cli_stdin_XXXXXX";
    assert(mkdtemp(dir) != NULL);

    int in_pipe[2], out_pipe[2];
    assert(pipe(in_pipe) == 0 && pipe(out_pipe) == 0);

    pid_t pid = fork();
    assert(pid >= 0);
    if (pid == 0) {
        close(in_pipe[1]);
        close(out_pipe[0]);
        dup2(in_pipe[0], STDIN_FILENO);
        dup2(out_pipe[1], STDOUT_FILENO);
        int devnull = open("/dev/null", O_WRONLY);
        if (devnull >= 0) {
            dup2(devnull, STDERR_FILENO);
            close(devnull);
        }
        close(in_pipe[0]);
        close(out_pipe[1]);

        execl("./tests/build/miro-ego-encoder-mock", "miro-ego-encoder-mock",
              "--out-dir", dir,
              "--stdin",
              "--width", "64",
              "--height", "32",
              "--fps", "30",
              "--intra-period", "3",
              "--segment-frames", "3",
              (char *)NULL);
        _exit(127);
    }

    close(in_pipe[0]);
    close(out_pipe[1]);

    /* Write 3 MGOF frames + 0-length EOS */
    uint8_t payload[32] = {0xfe};
    for (int i = 0; i < 3; i += 1) {
        uint32_t hdr[2] = {0x464f474du, (uint32_t)sizeof(payload)};
        assert(write(in_pipe[1], hdr, sizeof(hdr)) == (ssize_t)sizeof(hdr));
        assert(write(in_pipe[1], payload, sizeof(payload)) == (ssize_t)sizeof(payload));
    }
    uint32_t eos[2] = {0x464f474du, 0u};
    assert(write(in_pipe[1], eos, sizeof(eos)) == (ssize_t)sizeof(eos));
    close(in_pipe[1]);

    FILE *out_fp = fdopen(out_pipe[0], "r");
    assert(out_fp != NULL);
    char line[2048];
    int ready_seen = 0, seg_seen = 0, done_seen = 0;
    while (fgets(line, sizeof(line), out_fp) != NULL) {
        if (strstr(line, "\"event\":\"ready\"") != NULL) ready_seen += 1;
        else if (strstr(line, "\"event\":\"segment\"") != NULL) seg_seen += 1;
        else if (strstr(line, "\"event\":\"done\"") != NULL) done_seen += 1;
    }
    fclose(out_fp);

    int status = 0;
    assert(waitpid(pid, &status, 0) == pid);
    assert(WIFEXITED(status) && WEXITSTATUS(status) == 0);
    assert(ready_seen == 1 && seg_seen == 1 && done_seen == 1);

    cleanup_dir(dir);
}

static void test_pipeline_exact_boundary_and_zero_frame_lazy_open(void)
{
    /* 1. Exact multiple of segment_frames (6 frames, segment_frames = 3) with
     *    fail_empty_mux_stop = 1 must succeed cleanly with 2 segments and no orphan left_00002.mp4. */
    char dir[] = "/tmp/miro_ego_lazy_exact_XXXXXX";
    assert(mkdtemp(dir) != NULL);

    mock_hobot_config_t mock = {
        .sbs_width = 64,
        .height = 32,
        .intra_period = 3,
        .force_missing_idr = 0,
        .force_decoder_capacity = 0,
        .force_bad_decode_result = 0,
        .emit_stdout_noise = 0,
        .fail_empty_mux_stop = 1,
    };
    mock_hobot_reset(&mock);

    miro_ego_pipeline_config_t cfg = {
        .sbs_width = 64,
        .height = 32,
        .fps = 30,
        .bitrate_kbps = 4096,
        .hevc = 0,
        .rate_control = 0,
        .intra_period = 3,
        .min_qp = 18,
        .max_qp = 40,
        .intra_qp = 22,
        .initial_qp = 24,
        .vbv_ms = 2000,
        .segment_frames = 3,
        .out_dir = dir,
        .path_prefix = "video/",
    };

    seg_collector_t col = {0};
    miro_ego_pipeline_t *pipeline = NULL;
    char err[256] = {0};
    assert(miro_ego_pipeline_open(&cfg, collect_segment, &col, &pipeline, err, sizeof(err)) == 0);

    uint8_t fake_jpeg[64];
    memset(fake_jpeg, 0xd8, sizeof(fake_jpeg));
    for (int i = 0; i < 6; i += 1) {
        fake_jpeg[0] = (uint8_t)i;
        assert(miro_ego_pipeline_submit(pipeline, fake_jpeg, sizeof(fake_jpeg), 500) == 0);
    }
    assert(miro_ego_pipeline_finish(pipeline) == 0);
    assert(!miro_ego_pipeline_failed(pipeline));
    miro_ego_pipeline_close(pipeline);

    assert(col.count == 2);
    char orphan_l[512], orphan_r[512];
    snprintf(orphan_l, sizeof(orphan_l), "%s/left_00002.mp4", dir);
    snprintf(orphan_r, sizeof(orphan_r), "%s/right_00002.mp4", dir);
    assert(access(orphan_l, F_OK) != 0);
    assert(access(orphan_r, F_OK) != 0);
    cleanup_dir(dir);

    /* 2. Zero-frame session with fail_empty_mux_stop = 1 must finish cleanly
     *    without creating left_00000.mp4 / right_00000.mp4. */
    char zero_dir[] = "/tmp/miro_ego_lazy_zero_XXXXXX";
    assert(mkdtemp(zero_dir) != NULL);
    mock_hobot_reset(&mock);
    cfg.out_dir = zero_dir;
    memset(&col, 0, sizeof(col));
    pipeline = NULL;
    assert(miro_ego_pipeline_open(&cfg, collect_segment, &col, &pipeline, err, sizeof(err)) == 0);
    assert(miro_ego_pipeline_finish(pipeline) == 0);
    assert(!miro_ego_pipeline_failed(pipeline));
    miro_ego_pipeline_close(pipeline);
    assert(col.count == 0);
    snprintf(orphan_l, sizeof(orphan_l), "%s/left_00000.mp4", zero_dir);
    snprintf(orphan_r, sizeof(orphan_r), "%s/right_00000.mp4", zero_dir);
    assert(access(orphan_l, F_OK) != 0);
    assert(access(orphan_r, F_OK) != 0);
    cleanup_dir(zero_dir);
}

int main(void)
{
    test_pipeline_multi_segment(0, NULL);
    test_pipeline_multi_segment(1, NULL);

    g_custom_init_calls = 0;
    g_custom_split_calls = 0;
    g_custom_destroy_calls = 0;
    test_pipeline_multi_segment(0, &CUSTOM_SPLIT_OPS);
    assert(g_custom_init_calls == 1);
    assert(g_custom_split_calls == 16); /* 8 frames * 2 eyes */
    assert(g_custom_destroy_calls == 1);

    test_pipeline_exact_boundary_and_zero_frame_lazy_open();
    test_pipeline_failure_modes();
    test_cli_shm_end_to_end();
    test_cli_stdin_fallback();
    printf("test_pipeline_e2e: PASS (7-thread H.264/HEVC + VSE hook + failure modes + CLI SHM/stdin E2E)\n");
    return 0;
}

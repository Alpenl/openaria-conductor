#define _GNU_SOURCE
#include "miro_ego_sha256.h"
#include "miro_ego_shm_ring.h"
#include "miro_ego_stereo_pipeline.h"
#include "mock_hobot_sdk.h"

#include <assert.h>
#include <errno.h>
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
    unsigned long long duration_ticks;
    unsigned long long frag_offset;
    unsigned long long frag_bytes;
} recorded_frag_t;

typedef struct {
    recorded_frag_t frags[16];
    int frag_count;
    int sealed_count;
    char sealed_path[256];
    unsigned long long sealed_bytes;
    char sealed_sha256[MIRO_EGO_SHA256_HEX_LEN];
    unsigned long long sealed_frames;
    unsigned long long sealed_ticks;
    unsigned long long sealed_moof_count;
    int sealed_has_audio;
} frag_collector_t;

static void collect_fragment(void *user, int index,
                             unsigned long long start_frame, unsigned long long end_frame,
                             unsigned long long duration_ticks,
                             unsigned long long frag_offset, unsigned long long frag_bytes)
{
    frag_collector_t *col = (frag_collector_t *)user;
    assert(col->frag_count < 16);
    recorded_frag_t *f = &col->frags[col->frag_count++];
    f->index = index;
    f->start_frame = start_frame;
    f->end_frame = end_frame;
    f->duration_ticks = duration_ticks;
    f->frag_offset = frag_offset;
    f->frag_bytes = frag_bytes;
}

static void collect_sealed(void *user, const char *artifact_path,
                           unsigned long long bytes, const char *sha256_hex,
                           unsigned long long total_frames, unsigned long long total_ticks,
                           unsigned long long moof_count, int has_audio)
{
    frag_collector_t *col = (frag_collector_t *)user;
    col->sealed_count += 1;
    snprintf(col->sealed_path, sizeof(col->sealed_path), "%s", artifact_path);
    col->sealed_bytes = bytes;
    snprintf(col->sealed_sha256, sizeof(col->sealed_sha256), "%s", sha256_hex);
    col->sealed_frames = total_frames;
    col->sealed_ticks = total_ticks;
    col->sealed_moof_count = moof_count;
    col->sealed_has_audio = has_audio;
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

static void verify_file_boxes(const char *abs_path, int expect_hevc, int expect_audio)
{
    int fd = open(abs_path, O_RDONLY);
    assert(fd >= 0);
    uint8_t *buf = malloc(1024u * 1024u);
    assert(buf != NULL);
    ssize_t n = read(fd, buf, 1024u * 1024u);
    close(fd);
    assert(n > 100);
    size_t len = (size_t)n;
    assert(memcmp(buf + 4u, "ftyp", 4) == 0);
    assert(memmem(buf, len, "moov", 4) != NULL);
    assert(memmem(buf, len, expect_hevc ? "hvc1" : "avc1", 4) != NULL);
    assert(memmem(buf, len, "mett", 4) != NULL);
    assert((memmem(buf, len, "fLaC", 4) != NULL) == (expect_audio != 0));
    assert(memmem(buf, len, "moof", 4) != NULL);
    assert(memmem(buf, len, "mfra", 4) != NULL);
    free(buf);
}

static void test_pipeline_multi_fragment(int hevc, int has_audio, const miro_ego_split_ops_t *ops)
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
        .fragment_frames = 3,
        .timescale = 48000,
        .audio_tracks = has_audio,
        .out_dir = dir,
        .output_file = "session.mp4",
        .split_ops = ops,
        .preview = {0},
    };

    frag_collector_t col = {0};
    miro_ego_pipeline_t *pipeline = NULL;
    char err[256] = {0};
    assert(miro_ego_pipeline_open(&cfg, collect_fragment, collect_sealed, &col,
                                  &pipeline, err, sizeof(err)) == 0);

    uint8_t fake_jpeg[64];
    memset(fake_jpeg, 0xd8, sizeof(fake_jpeg));
    int16_t pcm_buf[1620 * 2];
    for (int s = 0; s < 1620 * 2; s += 1) {
        pcm_buf[s] = (int16_t)((s * 13) & 0x0fff);
    }

    unsigned long long sample_pos = 0ULL;
    for (int i = 0; i < 8; i += 1) {
        fake_jpeg[0] = (uint8_t)i;
        if (has_audio) {
            uint32_t dur = 1590u + (uint32_t)(i * 3u);
            char meta[96];
            snprintf(meta, sizeof(meta), "{\"seq\":%d,\"ticks\":%u}", i, dur);
            miro_ego_sync_bundle_t b = {
                .frame_seq = (uint64_t)i,
                .monotonic_ns = 1000000ULL + (uint64_t)i * 33333000ULL,
                .sample_number = sample_pos,
                .duration_ticks = dur,
                .pcm = (const uint8_t *)pcm_buf,
                .pcm_len = dur * 4u,
                .meta_json = (const uint8_t *)meta,
                .meta_len = (uint32_t)strlen(meta),
            };
            assert(miro_ego_pipeline_submit_bundle(pipeline, fake_jpeg, sizeof(fake_jpeg), &b, 500) == 0);
            sample_pos += dur;
        } else {
            assert(miro_ego_pipeline_submit(pipeline, fake_jpeg, sizeof(fake_jpeg), 500) == 0);
            sample_pos += 1600ULL;
        }
    }

    assert(miro_ego_pipeline_finish(pipeline) == 0);
    assert(!miro_ego_pipeline_failed(pipeline));

    miro_ego_pipeline_stats_t st;
    miro_ego_pipeline_stats(pipeline, &st);
    assert(st.offered == 8 && st.submitted == 8 && st.decoded == 8);
    assert(st.encoded[0] == 8 && st.encoded[1] == 8 && st.muxed_frames == 8);
    assert(st.fragments_written == 3 && st.total_ticks == sample_pos);

    miro_ego_pipeline_close(pipeline);

    assert(col.frag_count == 3);
    assert(col.sealed_count == 1);
    assert(strcmp(col.sealed_path, "session.mp4") == 0);
    assert(col.sealed_frames == 8ULL && col.sealed_ticks == sample_pos);
    assert(col.sealed_moof_count == 3ULL && col.sealed_has_audio == has_audio);

    const unsigned long long expected_starts[3] = {0, 3, 6};
    const unsigned long long expected_ends[3] = {3, 6, 8};
    unsigned long long sum_frag_dur = 0ULL;
    for (int i = 0; i < 3; i += 1) {
        assert(col.frags[i].index == i);
        assert(col.frags[i].start_frame == expected_starts[i]);
        assert(col.frags[i].end_frame == expected_ends[i]);
        assert(col.frags[i].frag_bytes > 0ULL);
        sum_frag_dur += col.frags[i].duration_ticks;
    }
    assert(sum_frag_dur == sample_pos);

    char abs_mp4[512];
    snprintf(abs_mp4, sizeof(abs_mp4), "%s/session.mp4", dir);
    verify_file_sha256(abs_mp4, col.sealed_bytes, col.sealed_sha256);
    verify_file_boxes(abs_mp4, hevc, has_audio);

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
    assert(miro_ego_pipeline_open(&bad_cfg, NULL, NULL, NULL, &pipeline, err, sizeof(err)) == -1);
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
        .fragment_frames = 3,
        .out_dir = dir,
        .output_file = "session.mp4",
    };
    frag_collector_t col = {0};
    assert(miro_ego_pipeline_open(&cfg, collect_fragment, collect_sealed, &col,
                                  &pipeline, err, sizeof(err)) == 0);
    uint8_t fake_jpeg[32] = {1, 2, 3, 4};
    assert(miro_ego_pipeline_submit(pipeline, fake_jpeg, sizeof(fake_jpeg), 500) == 0);
    assert(miro_ego_pipeline_finish(pipeline) == -1);
    assert(miro_ego_pipeline_failed(pipeline));
    assert(strstr(miro_ego_pipeline_error(pipeline), "segment_missing_idr") != NULL);
    assert(col.sealed_count == 0);
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
                                     8u, 16384u, err, sizeof(err)) == 0);

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
              "--output-file", "session.mp4",
              "--width", "64",
              "--height", "32",
              "--fps", "30",
              "--bitrate-kbps", "4096",
              "--intra-period", "3",
              "--fragment-frames", "3",
              "--timescale", "48000",
              "--audio-tracks", "1",
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

    /* Push 6 sync bundles through MEGO_SHM ring + EOF */
    uint8_t jpeg[128];
    uint8_t pcm[1600 * 4];
    memset(jpeg, 0xaa, sizeof(jpeg));
    memset(pcm, 0x11, sizeof(pcm));
    for (uint64_t seq = 0; seq < 6u; seq += 1) {
        jpeg[0] = (uint8_t)seq;
        const char *meta = "{\"seq\":0}";
        assert(miro_ego_shm_ring_push_bundle(
                   &producer, jpeg, sizeof(jpeg),
                   pcm, (uint32_t)sizeof(pcm),
                   (const uint8_t *)meta, (uint32_t)strlen(meta),
                   seq, seq * 33333000ULL, seq * 1600ULL, 1600u, 5000) == MIRO_EGO_SHM_OK);
    }
    assert(miro_ego_shm_ring_set_eof(&producer) == MIRO_EGO_SHM_OK);

    int fragments_seen = 0;
    int sealed_seen = 0;
    int done_seen = 0;
    while (fgets(line, sizeof(line), out_fp) != NULL) {
        /* Vendor SDK stdout noise must NEVER leak into NDJSON pipe! */
        assert(strstr(line, "mock-hobot-sdk") == NULL);
        if (strstr(line, "\"event\":\"fragment\"") != NULL) {
            unsigned long long idx = extract_json_u64(line, "index");
            assert(idx == (unsigned long long)fragments_seen);
            fragments_seen += 1;
        } else if (strstr(line, "\"event\":\"sealed\"") != NULL) {
            char rel[256], sha[MIRO_EGO_SHA256_HEX_LEN];
            extract_json_str(line, "path", rel, sizeof(rel));
            extract_json_str(line, "sha256", sha, sizeof(sha));
            unsigned long long bytes = extract_json_u64(line, "bytes");
            assert(strcmp(rel, "session.mp4") == 0);
            assert(extract_json_u64(line, "frames") == 6ULL);
            assert(extract_json_u64(line, "duration_ticks") == 9600ULL);
            assert(extract_json_u64(line, "moof_count") == 2ULL);
            char abs_mp4[512];
            snprintf(abs_mp4, sizeof(abs_mp4), "%s/session.mp4", dir);
            verify_file_sha256(abs_mp4, bytes, sha);
            sealed_seen += 1;
        } else if (strstr(line, "\"event\":\"done\"") != NULL) {
            assert(extract_json_u64(line, "submitted") == 6ULL);
            assert(extract_json_u64(line, "decoded") == 6ULL);
            assert(extract_json_u64(line, "left_frames") == 6ULL);
            assert(extract_json_u64(line, "right_frames") == 6ULL);
            assert(extract_json_u64(line, "muxed_frames") == 6ULL);
            assert(extract_json_u64(line, "fragments_written") == 2ULL);
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
    assert(fragments_seen == 2);
    assert(sealed_seen == 1);
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
              "--output-file", "session.mp4",
              "--stdin",
              "--width", "64",
              "--height", "32",
              "--fps", "30",
              "--intra-period", "3",
              "--fragment-frames", "3",
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
    int ready_seen = 0, frag_seen = 0, sealed_seen = 0, done_seen = 0;
    while (fgets(line, sizeof(line), out_fp) != NULL) {
        if (strstr(line, "\"event\":\"ready\"") != NULL) ready_seen += 1;
        else if (strstr(line, "\"event\":\"fragment\"") != NULL) frag_seen += 1;
        else if (strstr(line, "\"event\":\"sealed\"") != NULL) sealed_seen += 1;
        else if (strstr(line, "\"event\":\"done\"") != NULL) done_seen += 1;
    }
    fclose(out_fp);

    int status = 0;
    assert(waitpid(pid, &status, 0) == pid);
    assert(WIFEXITED(status) && WEXITSTATUS(status) == 0);
    assert(ready_seen == 1 && frag_seen == 1 && sealed_seen == 1 && done_seen == 1);

    cleanup_dir(dir);
}

static void test_pipeline_exact_boundary_and_zero_frame_lazy_open(void)
{
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
        .fragment_frames = 3,
        .timescale = 48000,
        .audio_tracks = 0,
        .out_dir = dir,
        .output_file = "session.mp4",
    };

    frag_collector_t col = {0};
    miro_ego_pipeline_t *pipeline = NULL;
    char err[256] = {0};
    assert(miro_ego_pipeline_open(&cfg, collect_fragment, collect_sealed, &col,
                                  &pipeline, err, sizeof(err)) == 0);

    uint8_t fake_jpeg[64];
    memset(fake_jpeg, 0xd8, sizeof(fake_jpeg));
    for (int i = 0; i < 6; i += 1) {
        fake_jpeg[0] = (uint8_t)i;
        assert(miro_ego_pipeline_submit(pipeline, fake_jpeg, sizeof(fake_jpeg), 500) == 0);
    }
    assert(miro_ego_pipeline_finish(pipeline) == 0);
    assert(!miro_ego_pipeline_failed(pipeline));
    miro_ego_pipeline_close(pipeline);

    assert(col.frag_count == 2 && col.sealed_count == 1);
    cleanup_dir(dir);

    /* Zero-frame session must finish cleanly without creating session.mp4. */
    char zero_dir[] = "/tmp/miro_ego_lazy_zero_XXXXXX";
    assert(mkdtemp(zero_dir) != NULL);
    mock_hobot_reset(&mock);
    cfg.out_dir = zero_dir;
    memset(&col, 0, sizeof(col));
    pipeline = NULL;
    assert(miro_ego_pipeline_open(&cfg, collect_fragment, collect_sealed, &col,
                                  &pipeline, err, sizeof(err)) == 0);
    assert(miro_ego_pipeline_finish(pipeline) == 0);
    assert(!miro_ego_pipeline_failed(pipeline));
    miro_ego_pipeline_close(pipeline);
    assert(col.frag_count == 0 && col.sealed_count == 0);
    char orphan[512];
    snprintf(orphan, sizeof(orphan), "%s/session.mp4", zero_dir);
    assert(access(orphan, F_OK) != 0);
    cleanup_dir(zero_dir);
}

static void test_pipeline_vbr_config_and_transient_backpressure(void)
{
    for (int hevc = 0; hevc <= 1; hevc += 1) {
        char dir[] = "/tmp/miro_ego_vbr_bp_XXXXXX";
        assert(mkdtemp(dir) != NULL);

        mock_hobot_config_t mock = {
            .sbs_width = 64,
            .height = 32,
            .intra_period = 3,
            .transient_jpu_input_timeouts = 5,
            .transient_vpu_input_timeouts = 7,
        };
        mock_hobot_reset(&mock);

        miro_ego_pipeline_config_t cfg = {
            .sbs_width = 64,
            .height = 32,
            .fps = 30,
            .bitrate_kbps = 12288,
            .hevc = hevc,
            .rate_control = 1, /* VBR */
            .intra_period = 3,
            .min_qp = 18,
            .max_qp = 36,
            .intra_qp = 21,
            .initial_qp = 24,
            .vbv_ms = 2500,
            .fragment_frames = 3,
            .out_dir = dir,
            .output_file = "session.mp4",
        };

        frag_collector_t col = {0};
        miro_ego_pipeline_t *pipeline = NULL;
        char err[256] = {0};
        assert(miro_ego_pipeline_open(&cfg, collect_fragment, collect_sealed, &col,
                                      &pipeline, err, sizeof(err)) == 0);

        for (int eye = 0; eye < 2; eye += 1) {
            mc_video_codec_enc_params_t enc_p;
            mock_hobot_last_enc_params(eye, &enc_p);
            if (!hevc) {
                assert(enc_p.rc_params.mode == MC_AV_RC_MODE_H264VBR);
                const mc_rate_control_vbr_t *v = &enc_p.rc_params.h264_vbr_params;
                assert(v->frame_rate == 30u);
                assert(v->intra_period == 3u);
                assert(v->intra_qp == 21u);
                assert(v->qp_map_enable == 0);
            } else {
                assert(enc_p.rc_params.mode == MC_AV_RC_MODE_H265VBR);
                const mc_rate_control_vbr_t *v = &enc_p.rc_params.h265_vbr_params;
                assert(v->frame_rate == 30u);
                assert(v->intra_period == 3u);
                assert(v->intra_qp == 21u);
                assert(v->qp_map_enable == 0);
            }
        }

        uint8_t fake_jpeg[64];
        memset(fake_jpeg, 0xd8, sizeof(fake_jpeg));
        for (int i = 0; i < 6; i += 1) {
            fake_jpeg[0] = (uint8_t)i;
            assert(miro_ego_pipeline_submit(pipeline, fake_jpeg, sizeof(fake_jpeg), 50) == 0);
        }
        assert(miro_ego_pipeline_finish(pipeline) == 0);
        assert(!miro_ego_pipeline_failed(pipeline));
        assert(mock_hobot_jpu_timeout_injections() == 5ULL);
        assert(mock_hobot_vpu_timeout_injections() == 7ULL);

        miro_ego_pipeline_stats_t st;
        miro_ego_pipeline_stats(pipeline, &st);
        assert(st.submitted == 6ULL && st.decoded == 6ULL);
        assert(st.encoded[0] == 6ULL && st.encoded[1] == 6ULL);
        assert(st.drop_decoder_input == 0ULL && st.drop_encoder_input == 0ULL);
        miro_ego_pipeline_close(pipeline);

        assert(col.frag_count == 2 && col.sealed_count == 1);
        char abs_mp4[512];
        snprintf(abs_mp4, sizeof(abs_mp4), "%s/session.mp4", dir);
        verify_file_sha256(abs_mp4, col.sealed_bytes, col.sealed_sha256);
        cleanup_dir(dir);
    }

    /* 2. Continuous stall exceeding input_stall_timeout_ms fails cleanly without hanging. */
    char stall_dir[] = "/tmp/miro_ego_stall_ceil_XXXXXX";
    assert(mkdtemp(stall_dir) != NULL);
    mock_hobot_config_t perm_mock = {
        .sbs_width = 64,
        .height = 32,
        .intra_period = 3,
        .permanent_input_timeout = 1,
    };
    mock_hobot_reset(&perm_mock);

    miro_ego_pipeline_config_t stall_cfg = {
        .sbs_width = 64,
        .height = 32,
        .fps = 30,
        .bitrate_kbps = 4096,
        .hevc = 0,
        .rate_control = 1,
        .intra_period = 3,
        .min_qp = 18,
        .max_qp = 40,
        .intra_qp = 22,
        .initial_qp = 24,
        .vbv_ms = 2000,
        .fragment_frames = 3,
        .out_dir = stall_dir,
        .output_file = "session.mp4",
        .input_stall_timeout_ms = 25,
    };
    miro_ego_pipeline_t *stall_pipe = NULL;
    char err[256] = {0};
    assert(miro_ego_pipeline_open(&stall_cfg, NULL, NULL, NULL, &stall_pipe, err, sizeof(err)) == 0);
    uint8_t fake_jpeg[32] = {0xff, 0xd8, 0xff, 0xd9};
    assert(miro_ego_pipeline_submit(stall_pipe, fake_jpeg, sizeof(fake_jpeg), 10) == -1);
    assert(miro_ego_pipeline_failed(stall_pipe));
    assert(strstr(miro_ego_pipeline_error(stall_pipe), "decoder_input_dequeue_failed") != NULL);
    miro_ego_pipeline_close(stall_pipe);
    cleanup_dir(stall_dir);

    /* 3. Non-transient output dequeue errors fail immediately without busy-spinning. */
    const int err_cases[2] = {0, 1};
    for (int i = 0; i < 2; i += 1) {
        char err_dir[] = "/tmp/miro_ego_deq_err_XXXXXX";
        assert(mkdtemp(err_dir) != NULL);
        mock_hobot_config_t err_mock = {
            .sbs_width = 64,
            .height = 32,
            .intra_period = 3,
            .force_decoder_output_error = err_cases[i] == 0 ? -EIO : 0,
            .force_encoder_output_error = err_cases[i] == 1 ? -EIO : 0,
        };
        mock_hobot_reset(&err_mock);
        stall_cfg.out_dir = err_dir;
        stall_cfg.input_stall_timeout_ms = 500;
        miro_ego_pipeline_t *err_pipe = NULL;
        assert(miro_ego_pipeline_open(&stall_cfg, NULL, NULL, NULL, &err_pipe, err, sizeof(err)) == 0);
        (void)miro_ego_pipeline_submit(err_pipe, fake_jpeg, sizeof(fake_jpeg), 50);
        assert(miro_ego_pipeline_finish(err_pipe) == -1);
        assert(miro_ego_pipeline_failed(err_pipe));
        const char *expected = err_cases[i] == 0 ? "decoder_output_dequeue_failed"
                                                 : "encoder_output_dequeue_failed";
        assert(strstr(miro_ego_pipeline_error(err_pipe), expected) != NULL);
        miro_ego_pipeline_close(err_pipe);
        cleanup_dir(err_dir);
    }
}

int main(void)
{
    test_pipeline_multi_fragment(0, 0, NULL);
    test_pipeline_multi_fragment(0, 1, NULL);
    test_pipeline_multi_fragment(1, 0, NULL);
    test_pipeline_multi_fragment(1, 1, NULL);

    g_custom_init_calls = 0;
    g_custom_split_calls = 0;
    g_custom_destroy_calls = 0;
    test_pipeline_multi_fragment(0, 0, &CUSTOM_SPLIT_OPS);
    assert(g_custom_init_calls == 1);
    assert(g_custom_split_calls == 16); /* 8 frames * 2 eyes */
    assert(g_custom_destroy_calls == 1);

    test_pipeline_exact_boundary_and_zero_frame_lazy_open();
    test_pipeline_vbr_config_and_transient_backpressure();
    test_pipeline_failure_modes();
    test_cli_shm_end_to_end();
    test_cli_stdin_fallback();
    printf("test_pipeline_e2e: PASS (fMP4 H.264/HEVC 3/4-track + VSE hook + failure modes + CLI SHM/stdin E2E)\n");
    return 0;
}

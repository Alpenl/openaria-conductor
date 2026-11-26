#define _GNU_SOURCE
#include "miro_ego_sha256.h"

#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

static void assert_sha256_str(const char *input, const char *expected_hex)
{
    miro_ego_sha256_ctx_t ctx;
    uint8_t digest[MIRO_EGO_SHA256_DIGEST_LEN];
    char hex[MIRO_EGO_SHA256_HEX_LEN];

    miro_ego_sha256_init(&ctx);
    miro_ego_sha256_update(&ctx, input, strlen(input));
    miro_ego_sha256_final(&ctx, digest);
    miro_ego_sha256_to_hex(digest, hex);

    if (strcmp(hex, expected_hex) != 0) {
        fprintf(stderr, "FAIL sha256(%s): got %s expected %s\n", input, hex, expected_hex);
        abort();
    }
}

static void test_nist_vectors(void)
{
    /* FIPS 180-4 / RFC 6234 standard SHA-256 test vectors */
    assert_sha256_str("",
                      "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855");
    assert_sha256_str("abc",
                      "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad");
    assert_sha256_str("abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq",
                      "248d6a61d20638b8e5c026930c3e6039a33ce45964ff2167f6ecedd419db06c1");
    assert_sha256_str(
        "abcdefghbcdefghicdefghijdefghijkefghijklfghijklmghijklmnhijklmno"
        "ijklmnopjklmnopqklmnopqrlmnopqrsmnopqrstnopqrstu",
        "cf5b16a778af8380036ce59e7b0492370b249b11e8f07a51afac45037afee9d1");

    /* 1,000,000 repetitions of 'a' */
    miro_ego_sha256_ctx_t ctx;
    miro_ego_sha256_init(&ctx);
    uint8_t block[1000];
    memset(block, 'a', sizeof(block));
    for (int i = 0; i < 1000; i += 1) {
        miro_ego_sha256_update(&ctx, block, sizeof(block));
    }
    uint8_t digest[MIRO_EGO_SHA256_DIGEST_LEN];
    char hex[MIRO_EGO_SHA256_HEX_LEN];
    miro_ego_sha256_final(&ctx, digest);
    miro_ego_sha256_to_hex(digest, hex);
    assert(strcmp(hex, "cdc76e5c9914fb9281a1c7e284d73e67f1809a48a497200e046d39ccc7112cd0") == 0);
}

static void test_chunked_boundaries(void)
{
    uint8_t data[517];
    for (size_t i = 0; i < sizeof(data); i += 1) {
        data[i] = (uint8_t)((i * 37u + 13u) & 0xffu);
    }

    miro_ego_sha256_ctx_t ref_ctx;
    uint8_t ref_digest[MIRO_EGO_SHA256_DIGEST_LEN];
    char ref_hex[MIRO_EGO_SHA256_HEX_LEN];
    miro_ego_sha256_init(&ref_ctx);
    miro_ego_sha256_update(&ref_ctx, data, sizeof(data));
    miro_ego_sha256_final(&ref_ctx, ref_digest);
    miro_ego_sha256_to_hex(ref_digest, ref_hex);

    const size_t steps[] = {1, 3, 7, 13, 55, 56, 63, 64, 65, 127, 128, 129};
    for (size_t s = 0; s < sizeof(steps) / sizeof(steps[0]); s += 1) {
        miro_ego_sha256_ctx_t ctx;
        miro_ego_sha256_init(&ctx);
        miro_ego_sha256_update(&ctx, NULL, 0);
        for (size_t off = 0; off < sizeof(data); off += steps[s]) {
            size_t chunk = sizeof(data) - off;
            if (chunk > steps[s]) {
                chunk = steps[s];
            }
            miro_ego_sha256_update(&ctx, data + off, chunk);
        }
        uint8_t digest[MIRO_EGO_SHA256_DIGEST_LEN];
        char hex[MIRO_EGO_SHA256_HEX_LEN];
        miro_ego_sha256_final(&ctx, digest);
        miro_ego_sha256_to_hex(digest, hex);
        assert(strcmp(hex, ref_hex) == 0);
    }
}

static void test_large_file_digest(void)
{
    char tmpl[] = "/tmp/miro_ego_sha256_test_XXXXXX";
    int fd = mkstemp(tmpl);
    assert(fd >= 0);
    unlink(tmpl);

    /* Empty file digest */
    char empty_hex[MIRO_EGO_SHA256_HEX_LEN] = {0};
    unsigned long long empty_bytes = 999;
    assert(miro_ego_sha256_fd(fd, empty_hex, &empty_bytes) == 0);
    assert(empty_bytes == 0);
    assert(strcmp(empty_hex,
                  "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855") == 0);

    /* 1,500,000 bytes (crosses ~6 x 256 KiB aligned read buffers) */
    const size_t total_size = 1500000u;
    uint8_t *payload = malloc(total_size);
    assert(payload != NULL);
    uint32_t rng = 0x12345678u;
    for (size_t i = 0; i < total_size; i += 1) {
        rng = rng * 1664525u + 1013904223u;
        payload[i] = (uint8_t)(rng >> 24);
    }

    size_t written = 0;
    while (written < total_size) {
        ssize_t n = write(fd, payload + written, total_size - written);
        assert(n > 0);
        written += (size_t)n;
    }

    /* Seek to middle of file to verify miro_ego_sha256_fd does not depend on or alter lseek pos. */
    off_t pos_before = lseek(fd, 12345, SEEK_SET);
    assert(pos_before == 12345);

    miro_ego_sha256_ctx_t ref_ctx;
    uint8_t ref_digest[MIRO_EGO_SHA256_DIGEST_LEN];
    char ref_hex[MIRO_EGO_SHA256_HEX_LEN];
    miro_ego_sha256_init(&ref_ctx);
    miro_ego_sha256_update(&ref_ctx, payload, total_size);
    miro_ego_sha256_final(&ref_ctx, ref_digest);
    miro_ego_sha256_to_hex(ref_digest, ref_hex);

    char fd_hex[MIRO_EGO_SHA256_HEX_LEN] = {0};
    unsigned long long fd_bytes = 0;
    assert(miro_ego_sha256_fd(fd, fd_hex, &fd_bytes) == 0);
    assert(fd_bytes == total_size);
    assert(strcmp(fd_hex, ref_hex) == 0);
    assert(lseek(fd, 0, SEEK_CUR) == 12345);

    /* Explicit 256 KiB page-aligned buffer path */
    void *aligned_buf = NULL;
    assert(posix_memalign(&aligned_buf, MIRO_EGO_SHA256_IO_ALIGN,
                          MIRO_EGO_SHA256_IO_BUF_SIZE) == 0);
    char buf_hex[MIRO_EGO_SHA256_HEX_LEN] = {0};
    unsigned long long buf_bytes = 0;
    assert(miro_ego_sha256_fd_with_buf(fd, aligned_buf, MIRO_EGO_SHA256_IO_BUF_SIZE,
                                       buf_hex, &buf_bytes) == 0);
    assert(buf_bytes == total_size);
    assert(strcmp(buf_hex, ref_hex) == 0);

    /* Invalid fd and NULL args fail closed */
    assert(miro_ego_sha256_fd(-1, buf_hex, &buf_bytes) == -1);
    assert(miro_ego_sha256_fd_with_buf(fd, NULL, 1024, buf_hex, &buf_bytes) == -1);
    assert(miro_ego_sha256_fd_with_buf(fd, aligned_buf, 0, buf_hex, &buf_bytes) == -1);

    free(aligned_buf);
    free(payload);
    close(fd);
}

int main(void)
{
    test_nist_vectors();
    test_chunked_boundaries();
    test_large_file_digest();
    printf("test_sha256: PASS (NIST vectors + chunk boundaries + 1.5MB aligned file digest)\n");
    return 0;
}

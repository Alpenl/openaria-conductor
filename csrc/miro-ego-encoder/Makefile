# Builds the Miro-Ego live dual-eye hardware stereo encoder (`miro-ego-encoder`).
#
# Target build (`make all` / `make install`) runs on RDK X5 against the board's
# hobot-multimedia hb_mm_mc / hb_mm_mx libraries.
# Host verification (`make check`) syntax-checks every source file under
# `-Wall -Wextra -Werror -std=gnu11` using stub SDK headers and builds + runs
# the full host C unit, SHM ring, SHA-256, and mock-SDK end-to-end test suite.

CC ?= gcc
CFLAGS ?= -O2 -Wall -Wextra -Werror -std=gnu11 -I/usr/include
LDFLAGS ?= -L/usr/hobot/lib -Wl,-rpath,/usr/hobot/lib
LDLIBS ?= -lmultimedia -lpthread
ifneq ($(strip $(HB_SDK_ROOT)),)
CFLAGS += -DHB_SDK_ROOT -I$(HB_SDK_ROOT)/include
LDFLAGS += -L$(HB_SDK_ROOT)/lib -Wl,-rpath,$(HB_SDK_ROOT)/lib
LDLIBS += -lhbmem
endif
DESTDIR ?= .

HOST_CFLAGS ?= -O2 -g -Wall -Wextra -Werror -std=gnu11
HOST_SANITIZE ?= -fsanitize=address,undefined -fno-omit-frame-pointer
STUB_INCLUDE := tests/stubs
BUILD_DIR := tests/build

TARGET := miro-ego-encoder

SRCS := \
	miro_ego_stereo_encoder.c \
	miro_ego_stereo_pipeline.c \
	miro_ego_shm_ring.c \
	miro_ego_sha256.c \
	miro_ego_split.c \
	miro_ego_mp4_meta.c

HDRS := \
	miro_ego_stereo_pipeline.h \
	miro_ego_shm_ring.h \
	miro_ego_sha256.h \
	miro_ego_split.h \
	miro_ego_mp4_meta.h \
	miro_ego_packet_queue.h \
	miro_ego_cli_common.h

MOCK_ENCODER := $(BUILD_DIR)/miro-ego-encoder-mock

TEST_BINS := \
	$(BUILD_DIR)/test_sha256 \
	$(BUILD_DIR)/test_shm_ring \
	$(BUILD_DIR)/test_pipeline_helpers \
	$(BUILD_DIR)/test_pipeline_e2e

all: $(TARGET)

$(TARGET): $(SRCS) $(HDRS)
	$(CC) $(CFLAGS) -o $@ $(SRCS) $(LDFLAGS) $(LDLIBS)

install: $(TARGET)
	install -D -m 0755 $(TARGET) $(DESTDIR)/$(TARGET)

$(BUILD_DIR):
	mkdir -p $(BUILD_DIR)

syntax-check: $(SRCS) $(HDRS) $(STUB_INCLUDE)/hb_media_codec.h $(STUB_INCLUDE)/hb_media_muxer.h
	@for src in $(SRCS); do \
		echo "  SYNTAX $$src"; \
		$(CC) -fsyntax-only -Wall -Wextra -Werror -std=gnu11 -I$(STUB_INCLUDE) -I. $$src || exit 1; \
	done

$(MOCK_ENCODER): $(SRCS) $(HDRS) tests/mock_hobot_sdk.c tests/mock_hobot_sdk.h $(STUB_INCLUDE)/hb_media_codec.h $(STUB_INCLUDE)/hb_media_muxer.h | $(BUILD_DIR)
	$(CC) $(HOST_CFLAGS) $(HOST_SANITIZE) -I$(STUB_INCLUDE) -I. -Itests -pthread -o $@ \
		$(SRCS) tests/mock_hobot_sdk.c

$(BUILD_DIR)/test_sha256: tests/test_sha256.c miro_ego_sha256.c miro_ego_sha256.h | $(BUILD_DIR)
	$(CC) $(HOST_CFLAGS) $(HOST_SANITIZE) -I. -o $@ tests/test_sha256.c miro_ego_sha256.c

$(BUILD_DIR)/test_shm_ring: tests/test_shm_ring.c miro_ego_shm_ring.c miro_ego_shm_ring.h | $(BUILD_DIR)
	$(CC) $(HOST_CFLAGS) $(HOST_SANITIZE) -I. -pthread -o $@ tests/test_shm_ring.c miro_ego_shm_ring.c

$(BUILD_DIR)/test_pipeline_helpers: tests/test_pipeline_helpers.c miro_ego_split.c miro_ego_mp4_meta.c miro_ego_sha256.c miro_ego_split.h miro_ego_mp4_meta.h miro_ego_sha256.h miro_ego_cli_common.h miro_ego_packet_queue.h | $(BUILD_DIR)
	$(CC) $(HOST_CFLAGS) $(HOST_SANITIZE) -I. -pthread -o $@ tests/test_pipeline_helpers.c miro_ego_split.c miro_ego_mp4_meta.c miro_ego_sha256.c

$(BUILD_DIR)/test_pipeline_e2e: tests/test_pipeline_e2e.c tests/mock_hobot_sdk.c tests/mock_hobot_sdk.h miro_ego_stereo_pipeline.c miro_ego_sha256.c miro_ego_split.c miro_ego_mp4_meta.c miro_ego_shm_ring.c $(HDRS) $(STUB_INCLUDE)/hb_media_codec.h $(STUB_INCLUDE)/hb_media_muxer.h $(MOCK_ENCODER) | $(BUILD_DIR)
	$(CC) $(HOST_CFLAGS) $(HOST_SANITIZE) -I$(STUB_INCLUDE) -I. -Itests -pthread -o $@ \
		tests/test_pipeline_e2e.c tests/mock_hobot_sdk.c miro_ego_stereo_pipeline.c \
		miro_ego_sha256.c miro_ego_split.c miro_ego_mp4_meta.c miro_ego_shm_ring.c

check: syntax-check $(MOCK_ENCODER) $(TEST_BINS)
	@for bin in $(TEST_BINS); do \
		echo "=== RUN $$bin ==="; \
		./$$bin || exit 1; \
	done
	@echo "ALL MIRO-EGO ENCODER CHECKS PASSED"

clean:
	rm -rf $(TARGET) $(BUILD_DIR)

.PHONY: all install syntax-check check clean

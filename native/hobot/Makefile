# P0 gate harness (issue #46). Builds the V4L2 driver on top of the shipped
# pipeline so the gate measures the code the recording daemon actually runs.
# RDK X5 target only.
CC ?= gcc
PIPELINE_DIR ?= ../../src/rp_ylx/hobot
CFLAGS ?= -O2 -Wall -Wextra -std=gnu11 -I/usr/include -I$(PIPELINE_DIR)
LDFLAGS ?= -L/usr/hobot/lib
LDLIBS ?= -lmultimedia -lpthread

TARGET := sbs_live_encode

all: $(TARGET)

$(TARGET): sbs_live_encode.c $(PIPELINE_DIR)/ylx_stereo_pipeline.c
	$(CC) $(CFLAGS) -o $@ sbs_live_encode.c $(PIPELINE_DIR)/ylx_stereo_pipeline.c \
		$(LDFLAGS) $(LDLIBS)

clean:
	rm -f $(TARGET)

.PHONY: all clean

# Builds the recording daemon's live dual-eye H.264 helper. Runs on the RDK X5
# target only: it links against the board's hobot-multimedia hb_mm_mc / hb_mm_mx
# API, which exists nowhere else.
CC ?= gcc
CFLAGS ?= -O2 -Wall -Wextra -std=gnu11 -I/usr/include
LDFLAGS ?= -L/usr/hobot/lib
LDLIBS ?= -lmultimedia -lpthread
DESTDIR ?= .

TARGET := ylx-stereo-encoder

all: $(TARGET)

$(TARGET): ylx_stereo_encoder.c ylx_stereo_pipeline.c ylx_stereo_pipeline.h
	$(CC) $(CFLAGS) -o $@ ylx_stereo_encoder.c ylx_stereo_pipeline.c $(LDFLAGS) $(LDLIBS)

install: $(TARGET)
	install -D -m 0755 $(TARGET) $(DESTDIR)/$(TARGET)

clean:
	rm -f $(TARGET)

.PHONY: all clean install

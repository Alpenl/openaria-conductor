# Audio Capture Clock v1

New recordings use a bounded PCM queue between ALSA and the WAV writer. The
capture thread performs no segment writes or fsync. An XRUN, suspend, unavailable
buffer, or writer error fails the recording; it never restarts ALSA and joins
samples across an unmeasured gap. The writer seals its completed prefix even when
capture fails. Failed recordings retain an audio/capture-clock.json diagnostic.
The native capture engine checks audio health in its IMU loop and reports the
failure through the existing recording failure callback.

The writer queue holds 768 blocks of 1024 PCM frames: 16.384 seconds at 48 kHz,
using 3 MiB of PCM storage for stereo S16_LE, plus one capture block. USB short
reads fill a block before enqueue; normal stop also submits the final partial block.
The diagnostic sidecar uses openaria.audio-capture-diagnostic.v2 and includes
separate open/write/seal/sync latency histograms, failure counts, and up to 64
slow-operation records. It also retains up to 65536 observations at approximately
100 ms intervals containing ALSA sample position, ALSA monotonic timestamp,
observed MONOTONIC/MONOTONIC_RAW timestamps, and read sample count. Omitted
observation counts are explicit. These diagnostics do not replace the original
clock anchors, change the v1 clock contract, or assert physical clock accuracy.

The default input is hw:CARD=D2UQ2,DEV=0, the YLX USB audio interface. Explicit
device configuration remains supported and existing installation configuration
is preserved. In particular, hw:0,0 may select the RDK board's ES8326 input rather
than the head-mounted microphone. The selected ALSA name is recorded in evidence.

Device Session v2 has an optional, versioned audio.capture_clock extension:

- schema is openaria.audio-clock.v1; clock is host_monotonic.
- timestamp_source is alsa_htimestamp_dma. It is a DMA position timestamp, not a
  claim of calibrated microphone ADC or camera exposure timing.
- anchors are [sample_position, monotonic_ns] pairs, collected after reads with
  snd_pcm_htimestamp. The position includes the samples still available in ALSA.
  The first, approximately one-second intermediate, and last anchors are retained.
- sample_start_monotonic_ns and sample_end_monotonic_ns map sample positions zero
  and sample_count using the first-to-last anchor slope. Readers recompute these
  values, anchor residuals, coverage, drift bounds, and the session-relative sync.
- thread_started_monotonic_ns and thread_stopped_monotonic_ns remain the measured
  capture lifetime. They are never overwritten with the sample duration.
- period_frames and buffer_frames are the negotiated hardware parameters.
  queue_capacity_frames, queue_peak_frames, max_write_ns, xrun_count, and
  suspend_count describe the capture/writer path. Successful recordings have no
  XRUN or suspend. Queue exhaustion is a recording failure.

PCM segment times remain relative to sample zero. audio.sync is relative to the
session origin and describes the captured sample interval for clock-bearing
recordings. The sample clock can differ from the nominal sample rate; Bridge uses
the measured rate when rendering audio. Max anchor residual must be at most 20 ms
and measured rate within 1 percent of nominal. These are rejection bounds, not an
external synchronization accuracy guarantee. Short recordings also need at least
two distinct clock anchors.

Historical v2 recordings without capture_clock remain readable as integrity-only
inputs with continuity unknown. Their thread-span/PCM-duration difference is
reported, not repaired or interpreted as an exact gap. They must not be promoted
to sample-clock-aligned. Bridge preserves their source WAV files. Its card and
LAN readers use the same audio_clock_report rules as Conductor; the shared module
must stay byte-identical across the two repositories. Old pinned vendor schemas
remain intact; the current reader explicitly supports this versioned extension.

Bridge renderer/recipe v3 uses frames.ndjson to preserve each captured video
timestamp on a microsecond MP4 time base, without duplicate or dropped frames.
The final frame lasts one measured average interval. Both eyes use the same
capture clock; the original frame index remains the higher-precision evidence.
VFR output uses H.264 without B-frame reordering to keep the final frame's timing
portable across FFmpeg versions. Quality settings stay unchanged, but compression
efficiency can differ. SDK validates every decoded PTS and frame duration;
Desktop compares a streaming digest of all decoded microsecond PTS relative to
the first frame with the capture clock, and independently checks frame count
and stream boundaries. Older FFmpeg MP4 edit lists can quantize the common origin
by less than 1 ms; this is bounded and reported as a boundary residual. Newer
muxers use microsecond precision for both the track and movie time bases.
Existing older renderer outputs are rebuilt from verified sources; the previous
output remains available until the replacement has been verified.

While recording, a cold catalog query inspects only small manifests and reports
unknown byte verification for uncached sessions. Full historical artifact hashing
is deferred until idle; the deferred state never grants download verification.
This avoids triggering large historical reads merely to list sessions during capture.
Historical verification that began while idle also checks capture/shutdown state
between 1 MiB reads. It releases the catalog lock after interruption and remains
unverified until a complete idle retry; capture sealing still verifies in full.

Acceptance covers real USB capture, repeated start/stop, multiple WAV boundaries,
30-minute capture, audio-clock validation, exported frame count and PTS, and
injected ALSA starvation and writer stalls. External optical/acoustic calibration
is a separate accuracy measurement and is not implied by continuity verification.

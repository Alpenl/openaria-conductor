"""Compile actual old/new C functions against controlled codec/muxer adapters."""

# Embedded C adapters retain their source layout for review and reproduction.
# ruff: noqa: E501

import json
import subprocess
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
OUT = Path(__file__).parent
current = (REPO / "src/rp_ylx/hobot/ylx_stereo_pipeline.c").read_text()
baseline = subprocess.check_output(
    ["git", "show", "f89a3b7:src/rp_ylx/hobot/ylx_stereo_pipeline.c"], cwd=REPO, text=True
)


def function(source, signature):
    start = source.index(signature)
    opening = source.index("{", start)
    depth = 1
    end = opening + 1
    while depth:
        depth += (source[end] == "{") - (source[end] == "}")
        end += 1
    return source[start:end] + "\n"


def compile_case(name, source):
    path = OUT / f"{name}.c"
    binary = OUT / name
    path.write_text(source)
    subprocess.run(
        [
            "cc",
            "-std=gnu11",
            "-O2",
            "-Wall",
            "-Wextra",
            "-Werror",
            "-pthread",
            str(path),
            "-o",
            str(binary),
        ],
        check=True,
        timeout=30,
    )
    return binary


common = r"""
#define _GNU_SOURCE
#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdarg.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>
#define YLX_EYES 2
#define PATH_LEN 512
"""
pair_type = current[
    current.index("typedef struct closed_pair {") : current.index("struct ylx_pipeline {")
]
ledger_struct = r"""
typedef struct {
 pthread_mutex_t ledger_lock;
 closed_pair_t *closed_pairs;
 int pending_pairs, reported_pairs;
 int closed_index[2]; char closed_path[2][PATH_LEN];
 unsigned long long closed_bytes[2],closed_start[2],closed_end[2];
 void (*on_segment)(void *,int,unsigned long long,unsigned long long,const char *,unsigned long long,const char *,unsigned long long);
 void *user; int failed;
} ylx_pipeline_t;
"""
# Keep source extraction separate from adapters; generated files show every line.
ledger_source = (
    common
    + pair_type
    + ledger_struct
    + r"""
static void fail(ylx_pipeline_t *p,const char *fmt,...) {(void)fmt;p->failed=1;}
static unsigned reported_mask; static int reports;
static void record(void *u,int n,unsigned long long s,unsigned long long e,const char *l,unsigned long long lb,const char *r,unsigned long long rb) {
 (void)u;(void)l;(void)r; assert(s==(unsigned)n*900 && e==s+900 && lb==123 && rb==456);
 reports++;reported_mask|=1u<<n;
}
"""
    + function(current, "static void ledger_record(").replace("ledger_record(", "ledger_new(")
    + function(baseline, "static void ledger_record(").replace("ledger_record(", "ledger_old(")
    + r"""
int main(void) {
 for(int old=0;old<2;old++) {
  int cases=0,perfect=0,total_reports=0,total_prefix=0;
  for(unsigned mask=0;mask<1024;mask++) {
   if(__builtin_popcount(mask)!=5)continue;
   ylx_pipeline_t p={0};pthread_mutex_init(&p.ledger_lock,NULL);
   p.closed_index[0]=p.closed_index[1]=-1;p.on_segment=record;
   reported_mask=0;reports=0;int ordinal[2]={0};
   for(int i=0;i<10;i++) {
    int eye=(mask>>i)&1,n=ordinal[eye]++;
    (old?ledger_old:ledger_new)(&p,eye,n,eye?"right":"left",eye?456:123,n*900,(n+1)*900);
   }
   int prefix=0;while(prefix<5 && (reported_mask&(1u<<prefix)))prefix++;
   cases++;perfect+=prefix==5;total_prefix+=prefix;total_reports+=reports;
   assert(!p.failed);free(p.closed_pairs);pthread_mutex_destroy(&p.ledger_lock);
  }
  printf("{\"experiment\":\"stereo_ledger\",\"variant\":\"%s\",\"schedules\":%d,\"perfect_schedules\":%d,\"reported_pairs_total\":%d,\"contiguous_pairs_total\":%d,\"possible_pairs_total\":%d}\n",old?"old_single_slot":"full",cases,perfect,total_reports,total_prefix,cases*5);
 }
}
"""
)
ledger = compile_case("ledger", ledger_source)
results = [json.loads(line) for line in subprocess.check_output([ledger], text=True).splitlines()]

packet_header = REPO / "src/rp_ylx/hobot/ylx_packet_queue.h"
writer_adapter = (
    common
    + f'#include "{packet_header}"\n'
    + r"""
#define ENCODER_OUTPUT_TIMEOUT_MS 200
#define ENCODER_DRAIN_POLLS 25
#define MUX_QUEUE_BYTES (32u*1024u*1024u)
#define MUX_QUEUE_SECONDS 16u
#define PACKET_BYTES 65536
#define PACKETS 60
typedef struct {int unused;} media_codec_context_t;
typedef struct {struct {void *vir_ptr;uintptr_t phy_ptr;int size;} vstream_buf;} media_codec_buffer_t;
typedef struct {int unused;} media_codec_output_buffer_info_t;
typedef struct {void *vir_ptr;uintptr_t phy_ptr;uint32_t size;unsigned long long pts;int is_key_frame,is_audio;} mx_stream_t;
typedef struct segment {int muxer;char relative[PATH_LEN];unsigned long long start_frame;} segment_t;
typedef struct ylx_pipeline ylx_pipeline_t;
typedef struct {ylx_pipeline_t *pipeline;int eye;media_codec_context_t codec;segment_t *current;unsigned long long ordinal;ylx_packet_queue_t packets;atomic_ullong max_write_ns;} eye_t;
struct ylx_pipeline {struct {int hevc,fps;} config;int segment_frames;atomic_int split_done,failed;atomic_ullong fed[2],encoded[2],bytes[2];char error[256];};
static const char *EYE_NAMES[2]={"left","right"};
static unsigned char hardware[PACKET_BYTES];
static int emitted,returned,writes,corrupt,stall_ms;
static unsigned long long acquired,max_hold,output_done,begin;
static unsigned long long monotonic_time_ns(void) {struct timespec t;clock_gettime(CLOCK_MONOTONIC,&t);return (unsigned long long)t.tv_sec*1000000000ULL+t.tv_nsec;}
static void observe_max(atomic_ullong *p,unsigned long long v) {if(v>atomic_load(p))atomic_store(p,v);}
static void fail(ylx_pipeline_t *p,const char *fmt,...) {va_list a;va_start(a,fmt);vsnprintf(p->error,sizeof(p->error),fmt,a);va_end(a);atomic_store(&p->failed,1);}
static int segment_open(ylx_pipeline_t *p,eye_t *e,int index,unsigned long long start) {(void)p;(void)index;e->current=calloc(1,sizeof(segment_t));e->current->start_frame=start;return 0;}
static void segment_close(ylx_pipeline_t *p,eye_t *e) {(void)p;free(e->current);e->current=NULL;}
static int hb_mm_mc_dequeue_output_buffer(media_codec_context_t *c,media_codec_buffer_t *b,media_codec_output_buffer_info_t *i,int timeout) {
 (void)c;(void)i;(void)timeout;if(emitted==PACKETS)return -1;
 usleep(1000);memset(hardware,emitted,PACKET_BYTES);hardware[0]=0;hardware[1]=0;hardware[2]=1;hardware[3]=0x65;
 b->vstream_buf.vir_ptr=hardware;b->vstream_buf.size=PACKET_BYTES;emitted++;acquired=monotonic_time_ns();return 0;
}
static int hb_mm_mc_queue_output_buffer(media_codec_context_t *c,media_codec_buffer_t *b,int timeout) {
 (void)c;(void)b;(void)timeout;unsigned long long held=monotonic_time_ns()-acquired;if(held>max_hold)max_hold=held;
 memset(hardware,255,PACKET_BYTES);returned++;return 0;
}
static int hb_mm_mx_write_stream(int *m,mx_stream_t *s) {
 (void)m;if(writes==0 && stall_ms)usleep(stall_ms*1000);
 unsigned char *data=s->vir_ptr;
 for(unsigned i=4;i<s->size;i++)if(data[i]!=(unsigned char)writes){corrupt++;break;}
 writes++;return 0;
}
static int hb_mm_mx_stop(int *m) {(void)m;return 0;}
"""
    + function(current, "static int h264_has_idr(")
    + function(current, "static int h265_has_idr(")
)
writer_functions = (
    function(current, "static void *mux_writer_thread(")
    + function(current, "static void *encoder_output_thread(").replace(
        "encoder_output_thread(", "output_new("
    )
    + function(baseline, "static void *encoder_output_thread(").replace(
        "encoder_output_thread(", "output_old("
    )
)
writer_main = r"""
int main(int argc,char **argv) {
 assert(argc==4);int old=atoi(argv[1]);stall_ms=atoi(argv[2]);size_t capacity=strtoul(argv[3],NULL,10);
 ylx_pipeline_t p={0};p.config.fps=30;atomic_store(&p.split_done,1);atomic_store(&p.fed[0],PACKETS);
 eye_t e={0};e.pipeline=&p;ylx_packet_queue_init(&e.packets,MUX_QUEUE_BYTES,capacity);
 pthread_t thread;begin=monotonic_time_ns();
 if(!old)assert(!pthread_create(&thread,NULL,mux_writer_thread,&e));
 (old?output_old:output_new)(&e);output_done=monotonic_time_ns()-begin;
 if(!old)pthread_join(thread,NULL);
 unsigned long long elapsed=monotonic_time_ns()-begin;
 printf("{\"experiment\":\"writer_isolation\",\"variant\":\"%s\",\"stall_ms\":%d,\"queue_capacity_frames\":%zu,\"frames_returned\":%d,\"frames_written\":%d,\"corrupt_packets\":%d,\"max_hardware_buffer_hold_ms\":%.6f,\"output_thread_ms\":%.6f,\"drain_total_ms\":%.6f,\"queue_peak_bytes\":%zu,\"queue_peak_frames\":%zu,\"rejected\":%zu,\"failed\":%d}\n",old?"old_synchronous":"full",stall_ms,capacity,returned,writes,corrupt,max_hold/1e6,output_done/1e6,elapsed/1e6,e.packets.peak_bytes,e.packets.peak_frames,e.packets.rejected,atomic_load(&p.failed));
 assert(corrupt==0);ylx_packet_queue_destroy(&e.packets);
}
"""
writer = compile_case("writer", writer_adapter + writer_functions + writer_main)
for repeat in range(5):
    cases = [(0, 0, 480), (1, 0, 480), (0, 250, 480), (1, 250, 480), (0, 250, 8)]
    cases = cases[repeat:] + cases[:repeat]
    for old, stall, capacity in cases:
        row = json.loads(
            subprocess.check_output(
                [writer, str(old), str(stall), str(capacity)], text=True, timeout=5
            )
        )
        row["repeat"] = repeat
        results.append(row)

seal_adapter = (
    common
    + r"""
typedef struct {int muxer,eye,index;char absolute[PATH_LEN],relative[PATH_LEN];unsigned long long start_frame,end_frame;} segment_t;
typedef struct {struct {int fps;char out_dir[PATH_LEN];} config;int failed,commits;} ylx_pipeline_t;
static int fault;
static void fail(ylx_pipeline_t *p,const char *fmt,...) {(void)fmt;p->failed=1;}
static int hb_mm_mx_stop(int *m) {(void)m;return fault==1?-1:0;}
static int mp4_extend_durations(const char *p,int fps,char *r,size_t size) {(void)p;(void)fps;(void)r;(void)size;return fault==2?-1:0;}
static int inject_fsync(int fd) {struct stat s;assert(!fstat(fd,&s));if((fault==3 && S_ISREG(s.st_mode))||(fault==4 && S_ISDIR(s.st_mode))){errno=EIO;return -1;}return 0;}
static void ledger_record(ylx_pipeline_t *p,int eye,int index,const char *r,unsigned long long bytes,unsigned long long start,unsigned long long end) {(void)eye;(void)index;(void)r;(void)bytes;(void)start;(void)end;p->commits++;}
#define fsync inject_fsync
"""
    + function(current, "static void segment_seal(").replace("segment_seal(", "seal_new(")
    + function(baseline, "static void segment_seal(").replace("segment_seal(", "seal_old(")
    + r"""
int main(void) {
 char root[]="/tmp/openaria-seal-ablation-XXXXXX";assert(mkdtemp(root));
 segment_t s={0};snprintf(s.absolute,sizeof(s.absolute),"%s/segment.mp4",root);s.end_frame=900;
 int fd=open(s.absolute,O_CREAT|O_WRONLY,0600);assert(fd>=0);assert(write(fd,"data",4)==4);close(fd);
 for(int old=0;old<2;old++)for(fault=0;fault<5;fault++) {
  ylx_pipeline_t p={0};p.config.fps=30;snprintf(p.config.out_dir,PATH_LEN,"%s",root);
  (old?seal_old:seal_new)(&p,&s);
  printf("{\"experiment\":\"seal_errors\",\"variant\":\"%s\",\"fault\":%d,\"declared_complete\":%d,\"failed\":%d}\n",old?"old_unchecked":"full",fault,p.commits,p.failed);
 }
 unlink(s.absolute);rmdir(root);
}
"""
)
seal = compile_case("seal", seal_adapter)
results.extend(json.loads(line) for line in subprocess.check_output([seal], text=True).splitlines())
assert all(
    r["frames_written"] == 60 and not r["failed"]
    for r in results
    if r["experiment"] == "writer_isolation" and r["queue_capacity_frames"] == 480
)
assert all(
    r["rejected"] == 1 and r["failed"]
    for r in results
    if r["experiment"] == "writer_isolation" and r["queue_capacity_frames"] == 8
)
assert (
    next(r for r in results if r["experiment"] == "stereo_ledger" and r["variant"] == "full")[
        "perfect_schedules"
    ]
    == 252
)
(OUT / "c-results.json").write_text(json.dumps(results, indent=2) + "\n")
for row in results:
    print(json.dumps(row), flush=True)

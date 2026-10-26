"""Bound actual muxer lifetime code while a simulated disk sealer is stalled."""

import json
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path


class HobotSegmentCapacityTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        compiler = shutil.which("cc")
        if not compiler:
            raise unittest.SkipTest("C compiler unavailable")
        cls.temporary = tempfile.TemporaryDirectory()
        cls.addClassCleanup(cls.temporary.cleanup)
        root = Path(__file__).resolve().parents[1] / "src/rp_ylx/hobot"
        source = (root / "ylx_stereo_pipeline.c").read_text()
        structures = source[
            source.index("typedef struct segment {") : source.index(
                "static unsigned long long monotonic_time_ns(void);"
            )
        ]
        failure = source[
            source.index("static void fail(") : source.index(
                "/* ---------------------------------------------------------------- codec"
            )
        ]
        opening = source[
            source.index("static int segment_open(") : source.index("static void ledger_record(")
        ]
        closing = source[
            source.index("static void segment_close(") : source.index("static void segment_seal(")
        ]
        closer = source[
            source.index("static void *closer_thread_main(") : source.index(
                "/* -------------------------------------------------------------- threads"
            )
        ]
        writer = source[
            source.index("static void *mux_writer_thread(") : source.index(
                "static void *encoder_output_thread("
            )
        ]
        constants = "\n".join(
            line for line in source.splitlines() if line.startswith("#define MAX_PENDING_SEGMENTS")
        )
        prefix = r"""
#define _GNU_SOURCE
#include <assert.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdarg.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include "ylx_stereo_pipeline.h"
#include "ylx_packet_queue.h"
#define ERROR_LEN 256
#define PATH_LEN 512
#define MEDIA_MUXER_OUTPUT_FORMAT_MP4 1
#define MEDIA_CODEC_ID_H265 1
#define MEDIA_CODEC_ID_H264 2
#define MC_PIXEL_FORMAT_NV12 1
typedef struct {int unused;} media_codec_context_t;
typedef struct {char *output_file_name;int output_format;} media_muxer_context_t;
typedef struct {
    int codec_id,numerator,denominator;
    struct {int width,height;uint32_t frame_rate,bit_rate;int pix_fmt;} video_params;
} mx_stream_params_t;
typedef struct {void *vir_ptr;uint32_t size;unsigned long long pts;int is_key_frame;} mx_stream_t;
static atomic_int live_segments,peak_segments,fail_stage,holding,sealing;
static void *checked_calloc(size_t count,size_t bytes) {
    if(atomic_load(&fail_stage)==1)return NULL;
    void *p=calloc(count,bytes);assert(p);
    int live=atomic_fetch_add(&live_segments,1)+1,old=atomic_load(&peak_segments);
    while(old<live && !atomic_compare_exchange_weak(&peak_segments,&old,live)){}
    return p;
}
static void checked_free(void *p) {
    if(p){assert(atomic_fetch_sub(&live_segments,1)>0);free(p);}
}
#define calloc checked_calloc
#define free checked_free
static int hb_mm_mx_get_default_context(media_muxer_context_t *m) {
    memset(m,0,sizeof(*m));return atomic_load(&fail_stage)==2?-1:0;
}
static int hb_mm_mx_initialize(media_muxer_context_t *m) {
    (void)m;return atomic_load(&fail_stage)==3?-1:0;
}
static int hb_mm_mx_add_stream(media_muxer_context_t *m,mx_stream_params_t *p) {
    (void)m;(void)p;return atomic_load(&fail_stage)==4?-1:0;
}
static int hb_mm_mx_start(media_muxer_context_t *m) {
    (void)m;return atomic_load(&fail_stage)==5?-1:0;
}
static int hb_mm_mx_write_stream(media_muxer_context_t *m,mx_stream_t *s) {
    (void)m;(void)s;return atomic_load(&fail_stage)==6?-1:0;
}
static int hb_mm_mx_stop(media_muxer_context_t *m) {(void)m;return 0;}
"""
        stubs = r"""
static unsigned long long monotonic_time_ns(void){return 1;}
static void observe_max(atomic_ullong *counter,unsigned long long elapsed){
    (void)counter;(void)elapsed;
}
static void segment_seal(ylx_pipeline_t *pipeline,segment_t *segment){
    (void)pipeline;(void)segment;
    atomic_store(&sealing,1);
    while(atomic_load(&holding))usleep(1000);
}
"""
        main = r"""
typedef struct {ylx_pipeline_t *p;eye_t *eye;int count;} producer_t;
static int submit_segment(ylx_pipeline_t *p,eye_t *eye,int index){
    if(segment_open(p,eye,index,(unsigned long long)index*150)!=0)return 0;
    eye->ordinal=(unsigned long long)(index+1)*150;
    segment_close(p,eye);assert(!eye->current);return 1;
}
static void *produce(void *arg){
    producer_t *x=arg;
    for(int i=0;i<1024;i++){if(!submit_segment(x->p,x->eye,i))break;x->count++;}
    return NULL;
}
static void drain(ylx_pipeline_t *p){
    pthread_mutex_lock(&p->closer_lock);p->closer_stop=1;
    pthread_cond_broadcast(&p->closer_signal);pthread_mutex_unlock(&p->closer_lock);
    closer_thread_main(p);assert(!atomic_load(&live_segments));
    p->closer_stop=0;
}
int main(int argc,char **argv){
    assert(argc==2);int mode=atoi(argv[1]),accepted=0;
    ylx_pipeline_t p={0};
    p.config=(ylx_pipeline_config_t){.fps=30,.sbs_width=3840,.height=1080,
        .hevc=1,.bitrate_kbps=12288,.out_dir="unused",.path_prefix="video/"};
    p.eye_width=1920;
    pthread_mutex_init(&p.error_lock,NULL);pthread_mutex_init(&p.closer_lock,NULL);
    pthread_cond_init(&p.closer_signal,NULL);
    for(int i=0;i<2;i++){p.eyes[i].eye=i;p.eyes[i].pipeline=&p;}
    if(mode==0){
        producer_t x={.p=&p,.eye=&p.eyes[0]};produce(&x);accepted=x.count;drain(&p);
    }else if(mode==1){
        // A segment being sealed still owns its muxer even after leaving the queue.
        assert(submit_segment(&p,&p.eyes[0],0));accepted=1;atomic_store(&holding,1);
        pthread_t t;assert(!pthread_create(&t,NULL,closer_thread_main,&p));
        while(!atomic_load(&sealing))usleep(1000);
        producer_t x={.p=&p,.eye=&p.eyes[1]};produce(&x);accepted+=x.count;
        pthread_mutex_lock(&p.closer_lock);p.closer_stop=1;
        pthread_cond_broadcast(&p.closer_signal);pthread_mutex_unlock(&p.closer_lock);
        atomic_store(&holding,0);pthread_join(t,NULL);
    }else if(mode==2){
        producer_t x[2]={{.p=&p,.eye=&p.eyes[0]},{.p=&p,.eye=&p.eyes[1]}};
        pthread_t t[2];
        for(int i=0;i<2;i++)assert(!pthread_create(&t[i],NULL,produce,&x[i]));
        for(int i=0;i<2;i++){pthread_join(t[i],NULL);accepted+=x[i].count;}
        drain(&p);
    }else if(mode==3){
        for(int stage=1;stage<=5;stage++){
            atomic_store(&fail_stage,stage);
            for(int i=0;i<96;i++){
                assert(segment_open(&p,&p.eyes[0],i,0)==-1);
                assert(!p.eyes[0].current && !atomic_load(&live_segments));
            }
        }
        atomic_store(&fail_stage,0);
        for(int i=0;i<32;i++){assert(submit_segment(&p,&p.eyes[0],i));accepted++;}
        drain(&p);
    }else if(mode==4){
        for(int cycle=0;cycle<4;cycle++){
            for(int i=0;i<32;i++){assert(submit_segment(&p,&p.eyes[i%2],i));accepted++;}
            drain(&p);
        }
    }else if(mode==5 || mode==6){
        unsigned char data[4]={0};
        assert(!ylx_packet_queue_init(&p.eyes[0].packets,1024,8));
        assert(!ylx_packet_queue_push(&p.eyes[0].packets,data,sizeof(data),mode==5));
        ylx_packet_queue_close(&p.eyes[0].packets);
        atomic_store(&fail_stage,mode==5?6:0);
        mux_writer_thread(&p.eyes[0]);
        assert(atomic_load(&p.failed) && !p.eyes[0].current && !p.closer_head);
        assert(!atomic_load(&live_segments));
        ylx_packet_queue_destroy(&p.eyes[0].packets);
        atomic_store(&fail_stage,0);
        for(int i=0;i<32;i++){assert(submit_segment(&p,&p.eyes[0],i));accepted++;}
        drain(&p);
    }else abort();
    assert(!atomic_load(&live_segments) && !p.closer_head && !p.closer_tail);
    printf("{\"accepted\":%d,\"peak_segments\":%d,\"failed\":%d,\"error\":\"%s\"}\n",
        accepted,atomic_load(&peak_segments),atomic_load(&p.failed),p.error);
    pthread_cond_destroy(&p.closer_signal);pthread_mutex_destroy(&p.closer_lock);
    pthread_mutex_destroy(&p.error_lock);return 0;
}
"""
        path = Path(cls.temporary.name) / "case.c"
        path.write_text(
            prefix
            + constants
            + "\n"
            + structures
            + failure
            + opening
            + closing
            + stubs
            + closer
            + writer
            + main
        )
        cls.binary = path.with_suffix("")
        subprocess.run(
            [
                compiler,
                "-std=gnu11",
                "-Wall",
                "-Wextra",
                "-Werror",
                "-Wno-unused-function",
                "-pthread",
                "-I",
                str(root),
                str(path),
                "-o",
                str(cls.binary),
            ],
            check=True,
            capture_output=True,
            text=True,
        )

    def run_case(self, mode):
        result = subprocess.run(
            [str(self.binary), str(mode)], capture_output=True, text=True, timeout=10
        )
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        return json.loads(result.stdout)

    def bounded(self, mode):
        result = self.run_case(mode)
        self.assertEqual(result["accepted"], 32, result)
        self.assertEqual(result["peak_segments"], 32, result)
        self.assertEqual(result["failed"], 1, result)
        self.assertIn("segment_queue_exhausted", result["error"])

    def test_stalled_sealer_cannot_accumulate_unbounded_muxers(self):
        self.bounded(0)

    def test_limit_includes_segment_currently_being_sealed(self):
        self.bounded(1)

    def test_both_eye_producers_share_one_atomic_limit(self):
        self.bounded(2)

    def test_failed_allocations_and_initializers_return_capacity(self):
        result = self.run_case(3)
        self.assertEqual(result["accepted"], 32, result)
        self.assertEqual(result["peak_segments"], 32, result)

    def test_drained_segments_return_capacity_for_later_segments(self):
        result = self.run_case(4)
        self.assertEqual(result["accepted"], 128, result)
        self.assertEqual(result["peak_segments"], 32, result)
        self.assertFalse(result["failed"], result)

    def test_failed_writer_returns_capacity_without_publishing_tail(self):
        for mode in (5, 6):
            with self.subTest(mode=mode):
                result = self.run_case(mode)
                self.assertEqual(result["accepted"], 32, result)
                self.assertEqual(result["peak_segments"], 32, result)


if __name__ == "__main__":
    unittest.main(verbosity=2)
